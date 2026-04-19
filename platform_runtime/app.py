from __future__ import annotations

import hashlib
import json
import mimetypes
import secrets
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlencode
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from starlette.datastructures import UploadFile
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from platform_core.models import (
    Account,
    AccountUser,
    Alert,
    CommunicationReview,
    Customer,
    Deal,
    Document,
    Employee,
    Goal,
    GoalTarget,
    InstallationRequest,
    KnowledgeItem,
    Lead,
    Product,
    Purchase,
    Role,
    RuntimeLease,
    Task,
    TaskEvent,
    User,
    Warehouse,
)
from platform_core.runtime_delivery import write_delivery_bundle
from platform_core.runtime_obsidian import export_account_delivery_note, export_portfolio_brief_note
from platform_core.exceptions import AuthorizationError, PlatformCoreError, TenantContextError
from platform_core.runtime_status import read_runtime_status, write_runtime_status
from platform_core.services import (
    AuditLogService,
    CommunicationService,
    EmployeeSnapshot,
    ExecutiveDashboardService,
    GOAL_METRIC_DEFINITIONS,
    GoalService,
    KnowledgeService,
    OperationsService,
    PeopleService,
)
from platform_core.services.accounts import AccountService, MembershipService, UserService
from platform_core.services.user_security import UserSecurityService
from platform_core.services.runtime import (
    AdminQueryService,
    ResolvedRuntimeContext,
    RuntimeAutomationService,
    RuntimeContextService,
    RuntimeIntegrationService,
    SchedulerService,
)
from platform_core.settings import load_platform_settings
from platform_core.tenancy import TenantContext
from platform_runtime.deps import get_db_session, get_runtime_context
from platform_runtime.schemas import (
    CredentialSaveRequest,
    GoalCreateRequest,
    GoalUpdateRequest,
    IntegrationCreateRequest,
    IntegrationUpdateRequest,
    RuleRunRequest,
    SchedulerRunRequest,
    SyncJobCreateRequest,
    TestConnectionRequest,
)


def create_app() -> FastAPI:
    settings = load_platform_settings()
    knowledge_upload_root = (Path(__file__).resolve().parent.parent / "data" / "runtime_knowledge_uploads").resolve()
    app = FastAPI(title="Platform Runtime API", version="0.1.0")
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.secret_key,
        session_cookie="hermes_admin_session",
        same_site="lax",
        https_only=settings.environment == "production",
    )
    templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
    app.mount("/admin-static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="admin-static")

    def ensure_permission(runtime: ResolvedRuntimeContext, permission_code: str) -> None:
        if "*" in runtime.permissions or permission_code in runtime.permissions:
            return
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Missing permission: {permission_code}")

    def resolve_admin_runtime(
        request: Request,
        session: Session,
        *,
        account_slug: str,
        actor_email: str,
    ) -> ResolvedRuntimeContext:
        try:
            return RuntimeContextService(session).resolve(
                account_id=None,
                account_slug=account_slug,
                actor_user_id=None,
                actor_email=actor_email,
                source="admin-ui",
                request_id=request.headers.get("x-request-id"),
            )
        except AuthorizationError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    def _session_actor_email(request: Request) -> str | None:
        value = request.session.get("admin_actor_email")
        if not isinstance(value, str):
            return None
        value = value.strip()
        return value or None

    def _session_account_slug(request: Request) -> str | None:
        value = request.session.get("admin_account_slug")
        if not isinstance(value, str):
            return None
        value = value.strip()
        return value or None

    def _session_auth_version(request: Request) -> int | None:
        value = request.session.get("admin_auth_version")
        if isinstance(value, int):
            return value
        return None

    def _session_csrf_token(request: Request) -> str | None:
        value = request.session.get("admin_csrf_token")
        if not isinstance(value, str):
            return None
        value = value.strip()
        return value or None

    def _ensure_session_csrf_token(request: Request) -> str:
        token = _session_csrf_token(request)
        if token:
            return token
        token = secrets.token_urlsafe(24)
        request.session["admin_csrf_token"] = token
        return token

    def _request_csrf_token(request: Request) -> str | None:
        header = request.headers.get("x-csrf-token")
        if header:
            header = header.strip()
            if header:
                return header
        form_value = request.query_params.get("csrf_token")
        if form_value:
            return form_value.strip()
        return None

    async def _request_form_csrf_token(request: Request) -> str | None:
        payload = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)
        value = str((payload.get("csrf_token") or [""])[0]).strip()
        return value or None

    async def _request_multipart_csrf_token(request: Request) -> str | None:
        form = await request.form()
        value = str(form.get("csrf_token") or "").strip()
        return value or None

    async def _require_csrf(request: Request) -> None:
        expected = _ensure_session_csrf_token(request)
        supplied = _request_csrf_token(request)
        if supplied is None and request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
            supplied = await _request_form_csrf_token(request)
        if supplied is None and request.headers.get("content-type", "").startswith("multipart/form-data"):
            supplied = await _request_multipart_csrf_token(request)
        if supplied != expected:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="CSRF token mismatch.")

    def _current_session_user(session: Session, request: Request) -> User | None:
        actor_email = _session_actor_email(request)
        if actor_email is None:
            return None
        user = UserSecurityService(session).get_by_email(actor_email)
        if user is None:
            return None
        session_auth_version = _session_auth_version(request)
        if session_auth_version is None:
            return None
        if int(user.auth_version or 1) != int(session_auth_version):
            return None
        if user.status != "active":
            return None
        return user

    def _actor_membership_accounts(session: Session, actor_email: str) -> list[AccountUser]:
        return session.execute(
            select(AccountUser)
            .join(User, User.id == AccountUser.user_id)
            .where(User.email == actor_email, AccountUser.status == "active")
            .order_by(AccountUser.account_id.asc(), AccountUser.id.asc())
        ).scalars().all()

    def _audit_admin_access(
        session: Session,
        *,
        actor_email: str,
        action: str,
        details: dict[str, object] | None = None,
    ) -> None:
        memberships = _actor_membership_accounts(session, actor_email)
        user = UserSecurityService(session).get_by_email(actor_email)
        actor_user_id = user.id if user is not None else None
        audit = AuditLogService(session)
        for membership in memberships:
            context = RuntimeContextService(session).resolve(
                account_id=membership.account_id,
                account_slug=None,
                actor_user_id=actor_user_id,
                actor_email=actor_email,
                source="admin-auth",
                request_id=None,
            ).context
            audit.log(
                context,
                action,
                "user",
                str(actor_user_id or actor_email),
                details=details,
            )

    def _push_flash(request: Request, level: str, message: str) -> None:
        flashes = request.session.get("admin_flashes")
        if not isinstance(flashes, list):
            flashes = []
        flashes.append({"level": level, "message": message})
        request.session["admin_flashes"] = flashes[-6:]

    def _pop_flashes(request: Request) -> list[dict[str, str]]:
        flashes = request.session.pop("admin_flashes", [])
        if not isinstance(flashes, list):
            return []
        return [
            {"level": str(item.get("level") or "info"), "message": str(item.get("message") or "").strip()}
            for item in flashes
            if isinstance(item, dict) and str(item.get("message") or "").strip()
        ]

    def _login_redirect(next_path: str | None = None) -> RedirectResponse:
        suffix = ""
        if next_path:
            suffix = f"?{urlencode({'next': next_path})}"
        return RedirectResponse(url=f"/admin/login{suffix}", status_code=status.HTTP_302_FOUND)

    def _admin_page_path(page: str) -> str:
        return {
            "portfolio": "portfolio",
            "platform": "platform",
            "super_admin": "super-admin",
            "accounts": "accounts",
            "users": "users",
            "brief": "brief",
            "delivery": "delivery",
            "dashboard": "dashboard",
            "integrations": "integrations",
            "alerts_tasks": "alerts-tasks",
            "ops_sync": "ops-sync",
            "knowledge": "knowledge",
            "people": "people",
            "goals": "goals",
            "members": "members",
            "settings": "settings",
        }.get(page, "dashboard")

    def _accessible_accounts(session: Session, actor_email: str) -> list[Account]:
        return session.execute(
            select(Account)
            .join(AccountUser, AccountUser.account_id == Account.id)
            .join(User, User.id == AccountUser.user_id)
            .where(User.email == actor_email, AccountUser.status == "active")
            .order_by(Account.name.asc(), Account.slug.asc())
        ).scalars().all()

    def _is_manager_role(role_code: str | None) -> bool:
        return role_code in {"owner", "admin"}

    def _is_owner_role(role_code: str | None) -> bool:
        return role_code == "owner"

    def _actor_can_manage_accounts(session: Session, actor_email: str) -> bool:
        rows = session.execute(
            select(Role.code)
            .join(AccountUser, AccountUser.role_id == Role.id)
            .join(User, User.id == AccountUser.user_id)
            .where(User.email == actor_email, AccountUser.status == "active")
        ).scalars().all()
        return any(_is_manager_role(code) for code in rows)

    def _owner_accounts_with_memberships(session: Session, actor_email: str) -> list[dict[str, object]]:
        rows = session.execute(
            select(Account, AccountUser, Role)
            .join(AccountUser, AccountUser.account_id == Account.id)
            .join(User, User.id == AccountUser.user_id)
            .join(Role, Role.id == AccountUser.role_id)
            .where(
                User.email == actor_email,
                AccountUser.status == "active",
                Role.code == "owner",
            )
            .order_by(Account.name.asc(), Account.slug.asc())
        ).all()
        return [
            {"account": account, "membership": membership, "role": role}
            for account, membership, role in rows
        ]

    def _actor_can_view_portfolio(session: Session, actor_email: str) -> bool:
        return bool(_owner_accounts_for_portfolio(session, actor_email))

    def _require_portfolio_owner(session: Session, actor_email: str) -> None:
        if _actor_can_view_portfolio(session, actor_email):
            return
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner role is required for portfolio view.")

    def _require_account_manager(runtime: ResolvedRuntimeContext) -> None:
        if _is_manager_role(runtime.role_code):
            return
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin role is required.")

    def _require_account_owner(runtime: ResolvedRuntimeContext) -> None:
        if _is_owner_role(runtime.role_code):
            return
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner role is required.")

    def _accessible_accounts_with_memberships(session: Session, actor_email: str) -> list[dict[str, object]]:
        rows = session.execute(
            select(Account, AccountUser, Role)
            .join(AccountUser, AccountUser.account_id == Account.id)
            .join(User, User.id == AccountUser.user_id)
            .join(Role, Role.id == AccountUser.role_id)
            .where(User.email == actor_email, AccountUser.status == "active")
            .order_by(Account.name.asc(), Account.slug.asc())
        ).all()
        return [
            {"account": account, "membership": membership, "role": role}
            for account, membership, role in rows
        ]

    def _accessible_account_ids(session: Session, actor_email: str) -> list[int]:
        return [item.id for item in _accessible_accounts(session, actor_email)]

    def _accessible_users_with_memberships(session: Session, actor_email: str) -> list[dict[str, object]]:
        account_ids = _accessible_account_ids(session, actor_email)
        if not account_ids:
            return []
        users = session.execute(
            select(User)
            .join(AccountUser, AccountUser.user_id == User.id)
            .where(AccountUser.account_id.in_(account_ids))
            .order_by(User.email.asc())
            .distinct()
        ).scalars().all()
        rows: list[dict[str, object]] = []
        for user in users:
            memberships = session.execute(
                select(AccountUser)
                .join(Role, Role.id == AccountUser.role_id)
                .where(AccountUser.user_id == user.id, AccountUser.account_id.in_(account_ids))
                .order_by(AccountUser.account_id.asc(), AccountUser.id.asc())
            ).scalars().all()
            rows.append({"user": user, "memberships": memberships})
        return rows

    def _assert_user_visible_to_actor(session: Session, actor_email: str, user_id: int) -> User:
        account_ids = _accessible_account_ids(session, actor_email)
        user = session.execute(
            select(User)
            .join(AccountUser, AccountUser.user_id == User.id)
            .where(User.id == user_id, AccountUser.account_id.in_(account_ids))
        ).scalars().first()
        if user is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found in accessible accounts.")
        return user

    def _require_admin_email(request: Request) -> str:
        actor_email = _session_actor_email(request)
        if actor_email is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin session is required.")
        return actor_email

    def _require_internal_api_token(request: Request) -> None:
        supplied = (
            request.headers.get("x-internal-api-token")
            or request.headers.get("x-platform-internal-token")
            or ""
        ).strip()
        if not supplied:
            auth_header = (request.headers.get("authorization") or "").strip()
            if auth_header.lower().startswith("bearer "):
                supplied = auth_header[7:].strip()
        if supplied != settings.internal_api_token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Valid internal API token is required.")

    def _require_admin_user(request: Request, session: Session) -> User:
        actor_email = _require_admin_email(request)
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin session is invalid or expired.")
        if user.email != actor_email:
            request.session.clear()
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin session is invalid or expired.")
        return user

    def _admin_nav_items(runtime: ResolvedRuntimeContext) -> list[dict[str, str]]:
        permissions = runtime.permissions
        items: list[dict[str, str]] = []
        feature_access = _feature_access_map(runtime.account)
        if "dashboard.read" in permissions or "*" in permissions:
            if feature_access["owner_briefs"]["allowed"]:
                items.append({"key": "brief", "label": "Brief", "path": "brief"})
                items.append({"key": "delivery", "label": "Delivery", "path": "delivery"})
            items.append({"key": "dashboard", "label": "Dashboard", "path": "dashboard"})
            if feature_access["knowledge_base"]["allowed"]:
                items.append({"key": "knowledge", "label": "Knowledge", "path": "knowledge"})
            if feature_access["people_execution"]["allowed"]:
                items.append({"key": "people", "label": "People", "path": "people"})
            if feature_access["operations_workflows"]["allowed"]:
                items.append({"key": "operations", "label": "Operations", "path": "operations"})
            if feature_access["communication_intelligence"]["allowed"]:
                items.append({"key": "communications", "label": "Communications", "path": "communications"})
            if feature_access["goals_tracking"]["allowed"]:
                items.append({"key": "goals", "label": "Goals", "path": "goals"})
        if "alerts.read" in permissions or "tasks.read" in permissions or "*" in permissions:
            items.append({"key": "alerts_tasks", "label": "Alerts / Tasks", "path": "alerts-tasks"})
        if _is_manager_role(runtime.role_code):
            items.append({"key": "members", "label": "Members", "path": "members"})
        if ("integrations.manage" in permissions or "*" in permissions) and feature_access["integrations_setup"]["allowed"]:
            items.append({"key": "integrations", "label": "Integrations", "path": "integrations"})
        if ({"integrations.manage", "rules.manage", "tasks.read", "alerts.read"}.issubset(permissions) or "*" in permissions) and feature_access["ops_console"]["allowed"]:
            items.append({"key": "ops_sync", "label": "Ops / Sync", "path": "ops-sync"})
        if _is_manager_role(runtime.role_code):
            items.append({"key": "settings", "label": "Settings", "path": "settings"})
        return items

    def _provider_catalog() -> list[dict[str, object]]:
        return [
            {
                "key": "ads:avito",
                "provider_kind": "ads",
                "provider_name": "avito",
                "label": "Avito",
                "description": "Avito ads + leads sync with token-based credentials.",
                "fields": [
                    {"key": "access_token", "label": "Access token", "kind": "secret", "required": True},
                    {"key": "account_external_id", "label": "Account external id", "kind": "text", "required": True},
                    {"key": "base_url", "label": "Base URL", "kind": "text"},
                    {"key": "timeout_seconds", "label": "Timeout seconds", "kind": "number"},
                    {"key": "max_retries", "label": "Max retries", "kind": "number"},
                    {"key": "backoff_seconds", "label": "Backoff seconds", "kind": "number"},
                    {"key": "campaigns_params", "label": "Campaign params JSON", "kind": "json"},
                    {"key": "metrics_params", "label": "Metrics params JSON", "kind": "json"},
                    {"key": "leads_params", "label": "Leads params JSON", "kind": "json"},
                    {"key": "lead_sources", "label": "Lead sources JSON", "kind": "json"},
                    {"key": "fixture_payload", "label": "Fixture payload JSON", "kind": "json"},
                ],
            },
            {
                "key": "erp:moysklad",
                "provider_kind": "erp",
                "provider_name": "moysklad",
                "label": "MoySklad",
                "description": "MoySklad ERP sync for canonical business tables.",
                "fields": [
                    {"key": "login", "label": "Login", "kind": "text", "required": True},
                    {"key": "password", "label": "Password", "kind": "secret", "required": True},
                    {"key": "base_url", "label": "Base URL", "kind": "text"},
                    {"key": "timeout_seconds", "label": "Timeout seconds", "kind": "number"},
                    {"key": "fixture_payload", "label": "Fixture payload JSON", "kind": "json"},
                ],
            },
            {
                "key": "banking:generic_bank",
                "provider_kind": "banking",
                "provider_name": "generic_bank",
                "label": "Generic Bank Feed",
                "description": "Canonical bank feed path. Fixture payload is the primary local setup mode.",
                "fields": [
                    {"key": "fixture_payload", "label": "Fixture payload JSON", "kind": "json"},
                    {"key": "base_url", "label": "Bank feed URL", "kind": "text"},
                    {"key": "access_token", "label": "Access token", "kind": "secret"},
                ],
            },
        ]

    def _provider_spec(provider_kind: str, provider_name: str) -> dict[str, object] | None:
        provider_key = f"{provider_kind}:{provider_name}"
        for item in _provider_catalog():
            if item["key"] == provider_key:
                return item
        return None

    def _admin_context(
        request: Request,
        session: Session,
        runtime: ResolvedRuntimeContext,
        *,
        page: str,
    ) -> dict[str, object]:
        actor_email = runtime.actor_user.email
        account_settings = _account_product_config(runtime.account)
        feature_access = _feature_access_map(runtime.account)
        return {
            "runtime": runtime,
            "page": page,
            "page_path": _admin_page_path(page),
            "accessible_accounts": _accessible_accounts(session, actor_email),
            "nav_items": _admin_nav_items(runtime),
            "can_manage_integrations": "*" in runtime.permissions or "integrations.manage" in runtime.permissions,
            "can_manage_goals": "*" in runtime.permissions or "rules.manage" in runtime.permissions,
            "can_manage_knowledge": "*" in runtime.permissions or "documents.manage" in runtime.permissions,
            "can_view_ops": "*" in runtime.permissions or {"integrations.manage", "rules.manage", "tasks.read", "alerts.read"}.issubset(runtime.permissions),
            "can_manage_members": _is_manager_role(runtime.role_code),
            "can_manage_accounts_global": _actor_can_manage_accounts(session, actor_email),
            "can_manage_users_global": _actor_can_manage_accounts(session, actor_email),
            "can_view_portfolio": _actor_can_view_portfolio(session, actor_email),
            "can_view_platform": _actor_can_manage_accounts(session, actor_email),
            "can_view_super_admin": _actor_can_manage_accounts(session, actor_email),
            "feature_access": feature_access,
            "csrf_token": _ensure_session_csrf_token(request),
            "flashes": _pop_flashes(request),
            "shell_brand_title": account_settings["branding_title"] or "Hermes Admin",
            "shell_brand_subtitle": account_settings["branding_subtitle"] or "Owner / operator console",
        }

    def _status_weight(status_code: str | None) -> int:
        return {"critical": 3, "warning": 2, "healthy": 1, "on_track": 1}.get(str(status_code or "").strip(), 0)

    def _portfolio_sync_health(rows: list[dict[str, object]]) -> dict[str, object]:
        active_rows = [item for item in rows if getattr(item["integration"], "status", None) != "archived"]
        broken: list[dict[str, object]] = []
        stale: list[dict[str, object]] = []
        for item in active_rows:
            integration = item["integration"]
            latest_success = item["latest_success"]
            latest_failure = item["latest_failure"]
            if integration.status == "disabled":
                continue
            success_at = latest_success.finished_at if latest_success is not None else None
            failure_at = latest_failure.finished_at if latest_failure is not None else None
            if latest_failure is not None and (success_at is None or (failure_at is not None and failure_at >= success_at)):
                broken.append(item)
                continue
            if integration.status == "active" and latest_success is None:
                stale.append(item)
        if broken:
            status_code = "critical"
        elif stale:
            status_code = "warning"
        else:
            status_code = "healthy"
        return {
            "status": status_code,
            "active_count": len(active_rows),
            "broken_count": len(broken),
            "stale_count": len(stale),
            "broken_rows": broken,
            "stale_rows": stale,
        }

    def _owner_panel_metric(widgets: dict[str, object], metric_code: str, *, fallback: float | int = 0) -> float | int:
        owner_panel = widgets.get("owner_panel", {}) if isinstance(widgets, dict) else {}
        for item in owner_panel.get("top_numbers", []):
            if item.get("metric_code") == metric_code:
                return item.get("value", fallback)
        return fallback

    def _portfolio_attention_items(
        *,
        account_slug: str,
        goal_snapshots: list[dict[str, object]],
        critical_alerts: list[object],
        overdue_tasks: list[object],
    ) -> list[dict[str, object]]:
        items: list[dict[str, object]] = []
        for snapshot in goal_snapshots:
            for blocker in snapshot.get("blockers", [])[:2]:
                items.append(
                    {
                        "type": "goal_blocker",
                        "status": blocker["metric"]["status"],
                        "title": f"{snapshot['goal'].title}: {blocker['metric']['label']}",
                        "context": f"delta {blocker['metric']['delta']}",
                        "href": blocker["links"]["ops"],
                        "account_slug": account_slug,
                    }
                )
        for alert in critical_alerts[:3]:
            items.append(
                {
                    "type": "alert",
                    "status": alert.severity,
                    "title": alert.title,
                    "context": alert.code,
                    "href": f"/admin/{account_slug}/alerts-tasks?severity=critical",
                    "account_slug": account_slug,
                }
            )
        for task in overdue_tasks[:3]:
            items.append(
                {
                    "type": "task",
                    "status": task.priority,
                    "title": task.title,
                    "context": f"due {task.due_at}",
                    "href": f"/admin/{account_slug}/alerts-tasks?priority=high",
                    "account_slug": account_slug,
                }
            )
        items.sort(key=lambda item: -_status_weight(str(item.get("status") or "")))
        return items[:5]

    def _portfolio_account_row(session: Session, actor_email: str, account: Account) -> dict[str, object]:
        resolved = RuntimeContextService(session).resolve(
            account_id=account.id,
            account_slug=None,
            actor_user_id=None,
            actor_email=actor_email,
            source="admin-portfolio",
            request_id=None,
        )
        dashboard = ExecutiveDashboardService(session).get_dashboard(resolved.context, "today")
        widgets = {item["widget_key"]: item["payload"] for item in dashboard["widgets"]}
        ops = AdminQueryService(session).ops_summary(account.id)
        sync_health = _portfolio_sync_health(ops["integration_sync_status"])
        automation = RuntimeAutomationService(session)
        open_alerts = [item for item in automation.list_alerts(resolved.context) if item.status == "open"]
        open_tasks = [item for item in automation.list_tasks(resolved.context) if item.status == "open"]
        goal_snapshots = [
            _enrich_goal_snapshot(
                account_slug=account.slug,
                actor_email=actor_email,
                snapshot=item,
                open_alerts=open_alerts,
                open_tasks=open_tasks,
                top_problems=widgets.get("owner_panel", {}).get("top_problems", []),
                attention_zones=widgets.get("owner_panel", {}).get("attention_zones", []),
            )
            for item in GoalService(session).get_dashboard_goal_snapshot(resolved.context)
        ]
        goals_at_risk = [item for item in goal_snapshots if item["summary"]["status"] != "on_track"]
        goal_critical_count = sum(int(item["summary"]["critical_count"]) for item in goal_snapshots)
        goal_warning_count = sum(int(item["summary"]["warning_count"]) for item in goal_snapshots)
        critical_alerts = list(ops["active_critical_alerts"])
        overdue_tasks = list(ops["overdue_tasks"])
        failed_sync_jobs = list(ops["recent_failed_sync_jobs"])
        risk_score = (
            goal_critical_count * 20
            + goal_warning_count * 8
            + len(critical_alerts) * 12
            + len(overdue_tasks) * 7
            + len(failed_sync_jobs) * 15
            + sync_health["broken_count"] * 20
            + sync_health["stale_count"] * 8
        )
        if goal_critical_count or sync_health["broken_count"] or failed_sync_jobs:
            health_status = "critical"
        elif goal_warning_count or critical_alerts or overdue_tasks or sync_health["stale_count"]:
            health_status = "warning"
        else:
            health_status = "healthy"
        return {
            "account": account,
            "health_status": health_status,
            "risk_score": risk_score,
            "available_cash": _owner_panel_metric(widgets, "available_cash"),
            "revenue": _owner_panel_metric(widgets, "revenue"),
            "net_profit": _owner_panel_metric(widgets, "net_profit"),
            "incoming_leads": _owner_panel_metric(widgets, "incoming_leads"),
            "critical_alerts_count": len(critical_alerts),
            "overdue_tasks_count": len(overdue_tasks),
            "failed_sync_jobs_count": len(failed_sync_jobs),
            "goals_at_risk_count": len(goals_at_risk),
            "goal_critical_count": goal_critical_count,
            "goal_warning_count": goal_warning_count,
            "sync_health": sync_health,
            "goal_snapshots": goal_snapshots,
            "goals_at_risk": goals_at_risk,
            "top_attention_items": _portfolio_attention_items(
                account_slug=account.slug,
                goal_snapshots=goals_at_risk,
                critical_alerts=critical_alerts,
                overdue_tasks=overdue_tasks,
            ),
            "ops": ops,
        }

    def _portfolio_summary(rows: list[dict[str, object]]) -> dict[str, object]:
        flattened_goal_rows = [
            {
                "account": row["account"],
                "goal": snapshot["goal"],
                "summary": snapshot["summary"],
                "blockers": snapshot.get("blockers", []),
            }
            for row in rows
            for snapshot in row["goals_at_risk"]
        ]
        goal_rollup = {
            "on_track": sum(1 for row in rows for snapshot in row["goal_snapshots"] if snapshot["summary"]["status"] == "on_track"),
            "warning": sum(1 for row in rows for snapshot in row["goal_snapshots"] if snapshot["summary"]["status"] == "warning"),
            "critical": sum(1 for row in rows for snapshot in row["goal_snapshots"] if snapshot["summary"]["status"] == "critical"),
        }
        highest_risk = sorted(rows, key=lambda item: (-int(item["risk_score"]), item["account"].name.lower()))[:5]
        broken_sync = [
            item for item in rows
            if item["sync_health"]["status"] != "healthy"
        ]
        broken_sync.sort(key=lambda item: (-int(item["sync_health"]["broken_count"]), -int(item["sync_health"]["stale_count"]), -int(item["risk_score"])))
        goal_deviations = [item for item in rows if item["goals_at_risk_count"] > 0]
        goal_deviations.sort(key=lambda item: (-int(item["goal_critical_count"]), -int(item["goal_warning_count"]), -int(item["risk_score"])))
        alert_pressure = sorted(
            rows,
            key=lambda item: (
                -(int(item["critical_alerts_count"]) + int(item["overdue_tasks_count"])),
                -int(item["risk_score"]),
            ),
        )[:5]
        failed_sync_jobs = [
            {"account": row["account"], "job": job}
            for row in rows
            for job in row["ops"]["recent_failed_sync_jobs"]
        ]
        failed_sync_jobs.sort(key=lambda item: item["job"].created_at, reverse=True)
        active_critical_alerts = [
            {"account": row["account"], "alert": alert}
            for row in rows
            for alert in row["ops"]["active_critical_alerts"]
        ]
        active_critical_alerts.sort(key=lambda item: item["alert"].last_detected_at, reverse=True)
        overdue_tasks = [
            {"account": row["account"], "task": task}
            for row in rows
            for task in row["ops"]["overdue_tasks"]
        ]
        overdue_tasks.sort(key=lambda item: item["task"].due_at or datetime.max.replace(tzinfo=timezone.utc))
        top_attention_items = sorted(
            [
                {"account": row["account"], "item": item}
                for row in rows
                for item in row["top_attention_items"]
            ],
            key=lambda item: -_status_weight(str(item["item"].get("status") or "")),
        )[:10]
        return {
            "accounts_count": len(rows),
            "healthy_accounts": sum(1 for item in rows if item["health_status"] == "healthy"),
            "warning_accounts": sum(1 for item in rows if item["health_status"] == "warning"),
            "critical_accounts": sum(1 for item in rows if item["health_status"] == "critical"),
            "available_cash_total": sum(float(item["available_cash"] or 0) for item in rows),
            "revenue_total": sum(float(item["revenue"] or 0) for item in rows),
            "net_profit_total": sum(float(item["net_profit"] or 0) for item in rows),
            "incoming_leads_total": sum(float(item["incoming_leads"] or 0) for item in rows),
            "critical_alerts_total": sum(int(item["critical_alerts_count"]) for item in rows),
            "overdue_tasks_total": sum(int(item["overdue_tasks_count"]) for item in rows),
            "broken_sync_accounts": sum(1 for item in rows if item["sync_health"]["status"] != "healthy"),
            "accounts_needing_action": sum(1 for item in rows if item["risk_score"] > 0),
            "goal_rollup": goal_rollup,
            "highest_risk_accounts": highest_risk,
            "broken_sync_accounts_rows": broken_sync[:5],
            "critical_goal_accounts": goal_deviations[:5],
            "alert_pressure_accounts": alert_pressure,
            "failed_sync_jobs": failed_sync_jobs[:10],
            "active_critical_alerts": active_critical_alerts[:10],
            "overdue_tasks": overdue_tasks[:10],
            "goal_rows": flattened_goal_rows[:10],
            "top_attention_items": top_attention_items,
        }

    def _portfolio_brief(portfolio: dict[str, object]) -> dict[str, object]:
        highest_risk_accounts = [
            {
                "account_slug": item["account"].slug,
                "account_name": item["account"].name,
                "health_status": item["health_status"],
                "risk_score": item["risk_score"],
                "goals_at_risk_count": item["goals_at_risk_count"],
                "critical_alerts_count": item["critical_alerts_count"],
                "overdue_tasks_count": item["overdue_tasks_count"],
                "sync_health": item["sync_health"]["status"],
            }
            for item in portfolio["highest_risk_accounts"]
        ]
        critical_alerts = [
            {
                "account_slug": item["account"].slug,
                "account_name": item["account"].name,
                "code": item["alert"].code,
                "title": item["alert"].title,
                "last_detected_at": _serialize_datetime(item["alert"].last_detected_at),
            }
            for item in portfolio["active_critical_alerts"]
        ]
        failed_sync = [
            {
                "account_slug": item["account"].slug,
                "account_name": item["account"].name,
                "job_id": item["job"].id,
                "provider_name": item["job"].provider_name,
                "status": item["job"].status,
                "error": _human_sync_error(item["job"]),
            }
            for item in portfolio["failed_sync_jobs"]
        ]
        goals_at_risk = [
            {
                "account_slug": item["account"].slug,
                "account_name": item["account"].name,
                "goal_id": item["goal"].id,
                "goal_title": item["goal"].title,
                "status": item["summary"]["status"],
                "critical_count": item["summary"]["critical_count"],
                "warning_count": item["summary"]["warning_count"],
            }
            for item in portfolio["goal_rows"]
        ]
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "headline": {
                "accounts_count": portfolio["accounts_count"],
                "accounts_needing_action": portfolio["accounts_needing_action"],
                "critical_accounts": portfolio["critical_accounts"],
                "critical_alerts_total": portfolio["critical_alerts_total"],
                "overdue_tasks_total": portfolio["overdue_tasks_total"],
                "broken_sync_accounts": portfolio["broken_sync_accounts"],
            },
            "daily_brief": highest_risk_accounts,
            "critical_alerts_digest": critical_alerts,
            "failed_sync_digest": failed_sync,
            "goals_at_risk_digest": goals_at_risk,
            "top_attention_items": [
                {
                    "account_slug": item["account"].slug,
                    "account_name": item["account"].name,
                    "type": item["item"]["type"],
                    "title": item["item"]["title"],
                    "status": item["item"]["status"],
                    "context": item["item"]["context"],
                    "href": item["item"]["href"],
                }
                for item in portfolio["top_attention_items"]
            ],
        }

    def _portfolio_brief_markdown(brief: dict[str, object]) -> str:
        lines = [
            "# Portfolio Brief",
            "",
            f"- Generated: {brief['generated_at']}",
            f"- Accounts: {brief['headline']['accounts_count']}",
            f"- Need action: {brief['headline']['accounts_needing_action']}",
            f"- Critical accounts: {brief['headline']['critical_accounts']}",
            "",
            "## Daily Brief",
        ]
        lines.extend(
            f"- {item['account_name']} ({item['account_slug']}): risk {item['risk_score']} · {item['health_status']} · sync {item['sync_health']}"
            for item in brief["daily_brief"]
        )
        lines.append("")
        lines.append("## Critical Alerts Digest")
        if brief["critical_alerts_digest"]:
            lines.extend(
                f"- {item['account_name']}: {item['title']} ({item['code']})"
                for item in brief["critical_alerts_digest"]
            )
        else:
            lines.append("- No critical alerts across portfolio.")
        lines.append("")
        lines.append("## Failed Sync Digest")
        if brief["failed_sync_digest"]:
            lines.extend(
                f"- {item['account_name']}: job #{item['job_id']} · {item['provider_name']} · {item['error'] or item['status']}"
                for item in brief["failed_sync_digest"]
            )
        else:
            lines.append("- No failed sync jobs across portfolio.")
        lines.append("")
        lines.append("## Goals At Risk Digest")
        if brief["goals_at_risk_digest"]:
            lines.extend(
                f"- {item['account_name']}: {item['goal_title']} ({item['status']})"
                for item in brief["goals_at_risk_digest"]
            )
        else:
            lines.append("- No goals at risk across portfolio.")
        return "\n".join(lines).strip() + "\n"

    def _portfolio_brief_text(brief: dict[str, object]) -> str:
        lines = [
            "portfolio brief",
            f"generated: {brief['generated_at']}",
            f"accounts: {brief['headline']['accounts_count']}",
            f"need action: {brief['headline']['accounts_needing_action']}",
            f"critical accounts: {brief['headline']['critical_accounts']}",
            "",
            "daily brief:",
        ]
        lines.extend(
            f"- {item['account_name']} ({item['account_slug']}): risk {item['risk_score']} · {item['health_status']} · sync {item['sync_health']}"
            for item in brief["daily_brief"]
        )
        lines.append("")
        lines.append("critical alerts digest:")
        if brief["critical_alerts_digest"]:
            lines.extend(f"- {item['account_name']}: {item['title']} ({item['code']})" for item in brief["critical_alerts_digest"])
        else:
            lines.append("- none")
        lines.append("")
        lines.append("failed sync digest:")
        if brief["failed_sync_digest"]:
            lines.extend(f"- {item['account_name']}: {item['provider_name']} · {item['error'] or item['status']}" for item in brief["failed_sync_digest"])
        else:
            lines.append("- none")
        lines.append("")
        lines.append("goals at risk digest:")
        if brief["goals_at_risk_digest"]:
            lines.extend(f"- {item['account_name']}: {item['goal_title']} ({item['status']})" for item in brief["goals_at_risk_digest"])
        else:
            lines.append("- none")
        return "\n".join(lines).strip() + "\n"

    def _portfolio_account_runtime(
        request: Request,
        session: Session,
        *,
        account_slug: str,
    ) -> ResolvedRuntimeContext:
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        _require_account_owner(runtime)
        _ensure_account_feature(runtime, "portfolio_console", "Portfolio console")
        return runtime

    def _membership_rows(account: Account, session: Session) -> list[AccountUser]:
        return MembershipService(session).list_memberships(account)

    def _goal_count_for_account(account_id: int, session: Session) -> int:
        return len(session.execute(select(Goal).where(Goal.account_id == account_id, Goal.status != "archived")).scalars().all())

    def _account_onboarding_status(account: Account, session: Session) -> dict[str, object]:
        memberships = _membership_rows(account, session)
        active_memberships = [item for item in memberships if item.status == "active"]
        owners = [item for item in active_memberships if item.role and item.role.code == "owner"]
        admins = [item for item in active_memberships if item.role and item.role.code == "admin"]
        integration_status_rows = AdminQueryService(session).integration_sync_status(account.id)
        goals_count = _goal_count_for_account(account.id, session)
        last_success = None
        for row in integration_status_rows:
            if row["latest_success"] is None:
                continue
            if last_success is None or row["latest_success"].finished_at > last_success.finished_at:
                last_success = row["latest_success"]
        steps = [
            {"key": "account", "label": "Account created", "done": account.status == "active"},
            {"key": "owner", "label": "Owner assigned", "done": len(owners) >= 1},
            {"key": "team", "label": "Admin or operator added", "done": len(active_memberships) >= 2 or len(admins) >= 1},
            {"key": "goals", "label": "Goal configured", "done": goals_count >= 1},
            {"key": "integration", "label": "Integration configured", "done": len(integration_status_rows) >= 1},
            {"key": "sync", "label": "First sync completed", "done": last_success is not None},
        ]
        completed = sum(1 for item in steps if item["done"])
        next_step = next((item["label"] for item in steps if not item["done"]), "Onboarding complete")
        return {
            "account": account,
            "steps": steps,
            "completed_steps": completed,
            "total_steps": len(steps),
            "next_step": next_step,
            "active_memberships": active_memberships,
            "owners_count": len(owners),
            "admins_count": len(admins),
            "goals_count": goals_count,
            "integration_rows": integration_status_rows,
            "last_success": last_success,
            "status": "complete" if completed == len(steps) else "in_progress" if completed > 0 else "not_started",
        }

    def _account_status_options() -> list[str]:
        return ["active", "disabled", "archived"]

    def _account_plan_options() -> list[str]:
        return ["internal", "pilot", "growth", "enterprise"]

    def _feature_flag_definitions() -> list[dict[str, str]]:
        return [
            {"key": "portfolio_console", "label": "Portfolio console", "description": "Owner-level portfolio visibility."},
            {"key": "owner_briefs", "label": "Owner briefs", "description": "Portfolio briefs and digest blocks."},
            {"key": "knowledge_base", "label": "Knowledge base", "description": "Operational memory, files and SOP knowledge."},
            {"key": "people_execution", "label": "People execution", "description": "Employee registry, workload and KPI view."},
            {"key": "operations_workflows", "label": "Operations workflows", "description": "Purchases, receiving, logistics requests and documents."},
            {"key": "communication_intelligence", "label": "Communication intelligence", "description": "Transcript reviews, quality signals and owner guidance."},
            {"key": "goals_tracking", "label": "Goals tracking", "description": "Plan vs fact and goal workflows."},
            {"key": "integrations_setup", "label": "Integrations setup", "description": "Admin UI for integration setup and sync."},
            {"key": "ops_console", "label": "Ops console", "description": "Ops / Sync visibility and actions."},
        ]

    def _plan_profiles() -> dict[str, dict[str, object]]:
        return {
            "internal": {
                "label": "Internal",
                "summary": "Internal operator deployment with full platform surface for live business use.",
                "recommended_features": {"portfolio_console", "owner_briefs", "knowledge_base", "people_execution", "operations_workflows", "communication_intelligence", "goals_tracking", "integrations_setup", "ops_console"},
                "usage_note": "Best fit for active internal operations and product validation.",
            },
            "pilot": {
                "label": "Pilot",
                "summary": "Small rollout for one operating team with core execution and onboarding flows.",
                "recommended_features": {"owner_briefs", "knowledge_base", "people_execution", "operations_workflows", "communication_intelligence", "goals_tracking", "integrations_setup", "ops_console"},
                "usage_note": "Good for proving value before broad rollout.",
            },
            "growth": {
                "label": "Growth",
                "summary": "Multi-account owner workflow with portfolio visibility and delivery flows.",
                "recommended_features": {"portfolio_console", "owner_briefs", "knowledge_base", "people_execution", "operations_workflows", "communication_intelligence", "goals_tracking", "integrations_setup", "ops_console"},
                "usage_note": "Designed for wider operational rollout.",
            },
            "enterprise": {
                "label": "Enterprise",
                "summary": "Highest readiness profile with all current product surfaces enabled.",
                "recommended_features": {"portfolio_console", "owner_briefs", "knowledge_base", "people_execution", "operations_workflows", "communication_intelligence", "goals_tracking", "integrations_setup", "ops_console"},
                "usage_note": "Suitable for fully managed multi-account environments.",
            },
        }

    def _default_feature_flags() -> dict[str, bool]:
        return {item["key"]: True for item in _feature_flag_definitions()}

    def _soft_limit_definitions() -> list[dict[str, str]]:
        return [
            {"key": "active_memberships", "label": "Active members", "description": "Soft limit for active account memberships."},
            {"key": "active_integrations", "label": "Active integrations", "description": "Soft limit for non-archived integrations."},
            {"key": "active_goals", "label": "Active goals", "description": "Soft limit for active goals."},
            {"key": "active_knowledge_items", "label": "Knowledge items", "description": "Soft limit for active knowledge entries."},
            {"key": "active_employees", "label": "Active employees", "description": "Soft limit for active employee records."},
            {"key": "active_documents", "label": "Active documents", "description": "Soft limit for active document records."},
            {"key": "open_installation_requests", "label": "Open installation requests", "description": "Soft limit for open logistics / installation requests."},
            {"key": "open_purchase_requests", "label": "Open purchase requests", "description": "Soft limit for requested purchases awaiting receiving."},
            {"key": "communication_reviews", "label": "Communication reviews", "description": "Soft limit for stored transcript reviews."},
        ]

    def _default_account_settings() -> dict[str, object]:
        return {
            "branding_title": "",
            "branding_subtitle": "",
            "default_dashboard_period": "today",
            "show_owner_brief": True,
            "show_portfolio_on_login": False,
            "default_owner_user_id": None,
            "default_operator_user_id": None,
        }

    def _account_product_config(account: Account) -> dict[str, object]:
        settings_payload = dict(_default_account_settings())
        raw = account.settings_json if isinstance(account.settings_json, dict) else {}
        for key, value in raw.items():
            settings_payload[key] = value
        return settings_payload

    def _account_feature_flags(account: Account) -> dict[str, bool]:
        flags = _default_feature_flags()
        raw = account.feature_flags_json if isinstance(account.feature_flags_json, dict) else {}
        for item in _feature_flag_definitions():
            key = item["key"]
            if key in raw:
                flags[key] = bool(raw[key])
        return flags

    def _account_feature_rows(account: Account) -> list[dict[str, object]]:
        flags = _account_feature_flags(account)
        plan_profiles = _plan_profiles()
        recommended = set(plan_profiles.get(account.plan_type, plan_profiles["internal"])["recommended_features"])
        rows: list[dict[str, object]] = []
        for item in _feature_flag_definitions():
            key = item["key"]
            enabled = bool(flags.get(key))
            included = key in recommended
            if enabled and included:
                status_code = "healthy"
            elif enabled and not included:
                status_code = "warning"
            elif included and not enabled:
                status_code = "critical"
            else:
                status_code = "warning"
            rows.append(
                {
                    "key": key,
                    "label": item["label"],
                    "description": item["description"],
                    "enabled": enabled,
                    "included_by_plan": included,
                    "status": status_code,
                }
            )
        return rows

    def _account_feature_access(account: Account, feature_key: str) -> dict[str, object]:
        flags = _account_feature_flags(account)
        plan_profiles = _plan_profiles()
        profile = plan_profiles.get(account.plan_type, plan_profiles["internal"])
        included = feature_key in set(profile["recommended_features"])
        enabled = bool(flags.get(feature_key, False))
        allowed = included and enabled and account.status == "active"
        if account.status != "active":
            reason = f"Feature is unavailable while account status is `{account.status}`."
        elif not included:
            reason = f"Feature is not included in the current `{account.plan_type}` plan."
        elif not enabled:
            reason = "Feature is disabled for this account."
        else:
            reason = None
        return {
            "key": feature_key,
            "included_by_plan": included,
            "enabled": enabled,
            "allowed": allowed,
            "reason": reason,
        }

    def _feature_access_map(account: Account) -> dict[str, dict[str, object]]:
        return {item["key"]: _account_feature_access(account, item["key"]) for item in _feature_flag_definitions()}

    def _ensure_account_feature(runtime: ResolvedRuntimeContext, feature_key: str, section_label: str) -> None:
        access = _account_feature_access(runtime.account, feature_key)
        if access["allowed"]:
            return
        detail = f"{section_label} is unavailable. {access['reason']}"
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail)

    def _owner_accounts_for_portfolio(session: Session, actor_email: str) -> list[dict[str, object]]:
        rows = _owner_accounts_with_memberships(session, actor_email)
        return [item for item in rows if _account_feature_access(item["account"], "portfolio_console")["allowed"]]

    def _account_membership_visibility(account: Account, session: Session) -> dict[str, object]:
        active_memberships = [item for item in _membership_rows(account, session) if item.status == "active"]
        owners = [item for item in active_memberships if item.role and item.role.code == "owner"]
        admins = [item for item in active_memberships if item.role and item.role.code == "admin"]
        return {
            "owners_count": len(owners),
            "admins_count": len(admins),
            "owner_labels": [item.user.full_name or item.user.email for item in owners[:3] if item.user is not None],
            "admin_labels": [item.user.full_name or item.user.email for item in admins[:3] if item.user is not None],
        }

    def _account_soft_limits(account: Account) -> dict[str, int | None]:
        limits: dict[str, int | None] = {item["key"]: None for item in _soft_limit_definitions()}
        raw = account.soft_limits_json if isinstance(account.soft_limits_json, dict) else {}
        for item in _soft_limit_definitions():
            key = item["key"]
            value = raw.get(key)
            if value in {None, ""}:
                limits[key] = None
                continue
            limits[key] = max(0, int(value))
        return limits

    def _account_usage_snapshot(account: Account, session: Session) -> dict[str, int]:
        memberships = _membership_rows(account, session)
        integrations = RuntimeIntegrationService(session).list_integrations(
            TenantContext(account_id=account.id, actor_user_id=None, source="settings", is_system=True)
        )
        goals = GoalService(session).list_goals(TenantContext(account_id=account.id, actor_user_id=None, source="settings", is_system=True))
        knowledge_count = KnowledgeService(session).count_active_items(account.id)
        active_employees = PeopleService(session).count_active_employees(account.id)
        documents_count = session.execute(
            select(func.count()).select_from(Document).where(Document.account_id == account.id, Document.status != "archived")
        ).scalar_one()
        open_installations = session.execute(
            select(func.count()).select_from(InstallationRequest).where(
                InstallationRequest.account_id == account.id,
                InstallationRequest.status.in_(["open", "scheduled"]),
            )
        ).scalar_one()
        open_purchase_requests = session.execute(
            select(func.count()).select_from(Purchase).where(
                Purchase.account_id == account.id,
                Purchase.status.in_(["draft", "requested", "ordered"]),
            )
        ).scalar_one()
        communication_reviews = session.execute(
            select(func.count()).select_from(CommunicationReview).where(CommunicationReview.account_id == account.id)
        ).scalar_one()
        return {
            "active_memberships": sum(1 for item in memberships if item.status == "active"),
            "active_integrations": sum(1 for item in integrations if item.status != "archived"),
            "active_goals": sum(1 for item in goals if item.status != "archived"),
            "active_knowledge_items": knowledge_count,
            "active_employees": active_employees,
            "active_documents": int(documents_count or 0),
            "open_installation_requests": int(open_installations or 0),
            "open_purchase_requests": int(open_purchase_requests or 0),
            "communication_reviews": int(communication_reviews or 0),
        }

    def _account_usage_rows(account: Account, session: Session) -> list[dict[str, object]]:
        usage = _account_usage_snapshot(account, session)
        limits = _account_soft_limits(account)
        rows: list[dict[str, object]] = []
        for item in _soft_limit_definitions():
            key = item["key"]
            current = int(usage.get(key, 0))
            limit = limits.get(key)
            if limit is None:
                status_code = "healthy"
                remaining = None
            else:
                remaining = limit - current
                if current > limit:
                    status_code = "critical"
                elif remaining <= 1:
                    status_code = "warning"
                else:
                    status_code = "healthy"
            rows.append(
                {
                    "key": key,
                    "label": item["label"],
                    "description": item["description"],
                    "current": current,
                    "limit": limit,
                    "remaining": remaining,
                    "status": status_code,
                }
            )
        return rows

    def _account_product_readiness(runtime: ResolvedRuntimeContext, session: Session) -> dict[str, object]:
        onboarding = _account_onboarding_status(runtime.account, session)
        ops = AdminQueryService(session).ops_summary(runtime.account.id)
        sync_health = _portfolio_sync_health(ops["integration_sync_status"])
        goal_snapshots = GoalService(session).get_dashboard_goal_snapshot(runtime.context)
        goals_at_risk = [item for item in goal_snapshots if item["summary"]["status"] != "on_track"]
        settings_payload = _account_product_config(runtime.account)
        next_steps: list[dict[str, str]] = []
        if onboarding["goals_count"] == 0:
            next_steps.append({"label": "Create the first goal", "href": f"/admin/{runtime.account.slug}/goals"})
        if KnowledgeService(session).count_active_items(runtime.account.id) == 0:
            next_steps.append({"label": "Load the first SOP or knowledge note", "href": f"/admin/{runtime.account.slug}/knowledge"})
        if PeopleService(session).count_active_employees(runtime.account.id) == 0:
            next_steps.append({"label": "Add the first employee and assign ownership", "href": f"/admin/{runtime.account.slug}/people"})
        product_count = session.execute(
            select(func.count()).select_from(Product).where(Product.account_id == runtime.account.id)
        ).scalar_one()
        warehouse_count = session.execute(
            select(func.count()).select_from(Warehouse).where(Warehouse.account_id == runtime.account.id)
        ).scalar_one()
        if product_count == 0 or warehouse_count == 0:
            next_steps.append({"label": "Set up the first product and warehouse", "href": f"/admin/{runtime.account.slug}/operations"})
        review_count = session.execute(
            select(func.count()).select_from(CommunicationReview).where(CommunicationReview.account_id == runtime.account.id)
        ).scalar_one()
        if review_count == 0:
            next_steps.append({"label": "Review the first message or call transcript", "href": f"/admin/{runtime.account.slug}/communications"})
        if len(onboarding["integration_rows"]) == 0:
            next_steps.append({"label": "Connect the first integration", "href": f"/admin/{runtime.account.slug}/integrations"})
        if onboarding["last_success"] is None and len(onboarding["integration_rows"]) > 0:
            next_steps.append({"label": "Run the first sync", "href": f"/admin/{runtime.account.slug}/integrations"})
        if not settings_payload.get("default_owner_user_id"):
            next_steps.append({"label": "Set default owner", "href": f"/admin/{runtime.account.slug}/settings"})
        if not settings_payload.get("default_operator_user_id") and onboarding["active_memberships"]:
            next_steps.append({"label": "Set default operator", "href": f"/admin/{runtime.account.slug}/settings"})
        if sync_health["status"] != "healthy":
            next_steps.append({"label": "Review sync health", "href": f"/admin/{runtime.account.slug}/ops-sync"})
        if goals_at_risk:
            next_steps.append({"label": "Review goal deviations", "href": f"/admin/{runtime.account.slug}/goals?risk_only=1"})
        if ops["active_critical_alerts"]:
            next_steps.append({"label": "Work through critical alerts", "href": f"/admin/{runtime.account.slug}/alerts-tasks?severity=critical"})
        if not next_steps:
            next_steps.append({"label": "Account is product-ready for daily use", "href": f"/admin/{runtime.account.slug}/dashboard"})
        if onboarding["completed_steps"] < onboarding["total_steps"]:
            status_code = "setup_required"
        elif sync_health["status"] == "critical" or goals_at_risk or ops["active_critical_alerts"]:
            status_code = "attention"
        else:
            status_code = "ready"
        return {
            "status": status_code,
            "onboarding": onboarding,
            "sync_health": sync_health,
            "critical_alerts_count": len(ops["active_critical_alerts"]),
            "overdue_tasks_count": len(ops["overdue_tasks"]),
            "goals_at_risk_count": len(goals_at_risk),
            "failed_sync_jobs_count": len(ops["recent_failed_sync_jobs"]),
            "usage_rows": _account_usage_rows(runtime.account, session),
            "next_steps": next_steps[:6],
        }

    def _platform_account_readiness_rows(session: Session, actor_email: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        account_rows = _accessible_accounts_with_memberships(session, actor_email)
        for item in account_rows:
            account = item["account"]
            resolved = RuntimeContextService(session).resolve(
                account_id=account.id,
                account_slug=None,
                actor_user_id=None,
                actor_email=actor_email,
                source="admin-platform",
                request_id=None,
            )
            readiness = _account_product_readiness(resolved, session)
            feature_rows = _account_feature_rows(account)
            usage_rows = _account_usage_rows(account, session)
            rows.append(
                {
                    "account": account,
                    "role": item["role"],
                    "readiness": readiness,
                    "features_enabled": sum(1 for feature in feature_rows if feature["enabled"]),
                    "features_included": sum(1 for feature in feature_rows if feature["included_by_plan"]),
                    "feature_rows": feature_rows,
                    "usage_rows": usage_rows,
                    "usage_pressure_count": sum(1 for usage in usage_rows if usage["status"] != "healthy"),
                }
            )
        rows.sort(
            key=lambda item: (
                -_status_weight(item["readiness"]["status"]),
                -int(item["readiness"]["critical_alerts_count"] + item["readiness"]["goals_at_risk_count"] + item["readiness"]["failed_sync_jobs_count"]),
                item["account"].name.lower(),
            )
        )
        return rows

    def _platform_health_summary(session: Session, actor_email: str) -> dict[str, object]:
        runtime_visibility = _runtime_visibility(session)
        account_rows = _platform_account_readiness_rows(session, actor_email)
        accounts_requiring_attention = [
            item for item in account_rows
            if item["readiness"]["status"] != "ready"
            or item["usage_pressure_count"] > 0
        ]
        if runtime_visibility["database_status"] != "ok" or runtime_visibility["worker_health"] == "critical":
            status_code = "critical"
        elif runtime_visibility["warnings"] or accounts_requiring_attention:
            status_code = "warning"
        else:
            status_code = "healthy"
        return {
            "status": status_code,
            "accessible_accounts": len(account_rows),
            "accounts_requiring_attention": len(accounts_requiring_attention),
            "healthy_accounts": sum(1 for item in account_rows if item["readiness"]["status"] == "ready" and item["usage_pressure_count"] == 0),
            "runtime_warnings_count": len(runtime_visibility["warnings"]),
            "delivery_recorded": runtime_visibility["delivery_status"] is not None,
            "database_status": runtime_visibility["database_status"],
            "worker_health": runtime_visibility["worker_health"],
            "top_attention_accounts": accounts_requiring_attention[:5],
            "plan_mix": {
                plan_type: sum(1 for item in account_rows if item["account"].plan_type == plan_type)
                for plan_type in _account_plan_options()
            },
        }

    def _super_admin_account_rows(session: Session, actor_email: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for item in _platform_account_readiness_rows(session, actor_email):
            account = item["account"]
            membership_visibility = _account_membership_visibility(account, session)
            feature_rows = item["feature_rows"]
            top_issues: list[str] = []
            if item["readiness"]["critical_alerts_count"]:
                top_issues.append(f"critical alerts {item['readiness']['critical_alerts_count']}")
            if item["readiness"]["failed_sync_jobs_count"]:
                top_issues.append(f"failed sync {item['readiness']['failed_sync_jobs_count']}")
            if item["readiness"]["goals_at_risk_count"]:
                top_issues.append(f"goals at risk {item['readiness']['goals_at_risk_count']}")
            if item["readiness"]["overdue_tasks_count"]:
                top_issues.append(f"overdue tasks {item['readiness']['overdue_tasks_count']}")
            if not top_issues and item["readiness"]["next_steps"]:
                top_issues.append(item["readiness"]["next_steps"][0]["label"])
            rows.append(
                {
                    **item,
                    "membership": membership_visibility,
                    "membership_visibility": membership_visibility,
                    "feature_access": _feature_access_map(account),
                    "feature_summary": {
                        "enabled": sum(1 for feature in feature_rows if feature["enabled"]),
                        "included": sum(1 for feature in feature_rows if feature["included_by_plan"]),
                        "disabled": sum(1 for feature in feature_rows if not feature["enabled"]),
                    },
                    "top_issues": top_issues[:3],
                }
            )
        return rows

    def _default_account_users(runtime: ResolvedRuntimeContext, session: Session) -> dict[str, User | None]:
        settings_payload = _account_product_config(runtime.account)
        active_memberships = [item for item in _membership_rows(runtime.account, session) if item.status == "active"]
        user_by_id = {item.user.id: item.user for item in active_memberships if item.user is not None}

        def _first_by_roles(*role_codes: str) -> User | None:
            for role_code in role_codes:
                for membership in active_memberships:
                    if membership.role is not None and membership.role.code == role_code and membership.user is not None:
                        return membership.user
            return None

        owner_user = user_by_id.get(settings_payload.get("default_owner_user_id")) if settings_payload.get("default_owner_user_id") else None
        if owner_user is None:
            owner_user = _first_by_roles("owner", "admin", "operator")
        operator_user = user_by_id.get(settings_payload.get("default_operator_user_id")) if settings_payload.get("default_operator_user_id") else None
        if operator_user is None:
            operator_user = _first_by_roles("operator", "admin", "owner")
        return {
            "owner": owner_user or runtime.actor_user,
            "operator": operator_user or owner_user or runtime.actor_user,
        }

    def _account_brief_digest(runtime: ResolvedRuntimeContext, session: Session) -> dict[str, object]:
        dashboard = ExecutiveDashboardService(session).get_dashboard(runtime.context, "today")
        widgets = {item["widget_key"]: item["payload"] for item in dashboard["widgets"]}
        automation = RuntimeAutomationService(session)
        open_alerts = [item for item in automation.list_alerts(runtime.context) if item.status == "open"]
        open_tasks = [item for item in automation.list_tasks(runtime.context) if item.status == "open"]
        critical_alerts = [item for item in open_alerts if item.severity == "critical"]
        overdue_tasks = [
            item for item in open_tasks
            if item.due_at is not None and item.due_at <= datetime.now(timezone.utc)
        ]
        goal_snapshots = [
            _enrich_goal_snapshot(
                account_slug=runtime.account.slug,
                actor_email=runtime.actor_user.email,
                snapshot=item,
                open_alerts=open_alerts,
                open_tasks=open_tasks,
                top_problems=widgets.get("owner_panel", {}).get("top_problems", []),
                attention_zones=widgets.get("owner_panel", {}).get("attention_zones", []),
            )
            for item in GoalService(session).get_dashboard_goal_snapshot(runtime.context)
        ]
        goals_at_risk = [item for item in goal_snapshots if item["summary"]["status"] != "on_track"]
        ops = AdminQueryService(session).ops_summary(runtime.account.id)
        defaults = _default_account_users(runtime, session)
        must_do_now: list[dict[str, object]] = []
        for alert in critical_alerts[:4]:
            must_do_now.append(
                {
                    "type": "alert",
                    "status": alert.severity,
                    "title": alert.title,
                    "context": alert.code,
                    "href": f"/admin/{runtime.account.slug}/alerts-tasks?severity=critical",
                }
            )
        for task in overdue_tasks[:4]:
            must_do_now.append(
                {
                    "type": "task",
                    "status": task.priority,
                    "title": task.title,
                    "context": f"due {task.due_at}",
                    "href": f"/admin/{runtime.account.slug}/alerts-tasks?overdue=1",
                }
            )
        for row in ops["integration_sync_status"]:
            state = _portfolio_sync_health([row])
            if state["status"] == "critical":
                must_do_now.append(
                    {
                        "type": "sync",
                        "status": "critical",
                        "title": row["integration"].external_ref or row["integration"].display_name,
                        "context": _human_sync_error(row["latest_failure"]) or "Broken sync",
                        "href": f"/admin/{runtime.account.slug}/ops-sync?sync_state=critical",
                    }
                )
        for snapshot in goals_at_risk[:4]:
            if snapshot["blockers"]:
                blocker = snapshot["blockers"][0]
                must_do_now.append(
                    {
                        "type": "goal",
                        "status": snapshot["summary"]["status"],
                        "title": snapshot["goal"].title,
                        "context": f"{blocker['metric']['label']} · delta {blocker['metric']['delta']}",
                        "href": f"/admin/{runtime.account.slug}/goals?goal_id={snapshot['goal'].id}",
                    }
                )
        must_do_now.sort(key=lambda item: -_status_weight(str(item.get("status") or "")))
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "defaults": defaults,
            "dashboard": dashboard,
            "widgets": widgets,
            "critical_alerts": critical_alerts[:8],
            "overdue_tasks": overdue_tasks[:8],
            "failed_sync_jobs": list(ops["recent_failed_sync_jobs"])[:8],
            "goals_at_risk": goals_at_risk[:8],
            "must_do_now": must_do_now[:10],
            "sync_health": _portfolio_sync_health(ops["integration_sync_status"]),
            "top_problems": widgets.get("owner_panel", {}).get("top_problems", []),
            "attention_zones": widgets.get("owner_panel", {}).get("attention_zones", []),
        }

    def _account_delivery_pack(runtime: ResolvedRuntimeContext, session: Session) -> dict[str, object]:
        settings_payload = _account_product_config(runtime.account)
        readiness = _account_product_readiness(runtime, session)
        brief = _account_brief_digest(runtime, session)
        onboarding = readiness["onboarding"]
        sync_health = readiness["sync_health"]
        integrations_needing_attention: list[dict[str, object]] = []
        for row in onboarding["integration_rows"]:
            state = _portfolio_sync_health([row])
            if state["status"] == "healthy":
                continue
            integrations_needing_attention.append(
                {
                    "integration": row["integration"],
                    "sync_state": state["status"],
                    "latest_success": row["latest_success"],
                    "latest_failure": row["latest_failure"],
                    "last_error": _human_sync_error(row["latest_failure"]),
                }
            )
        configured_now = [
            {"label": "Account status", "value": runtime.account.status},
            {"label": "Plan type", "value": runtime.account.plan_type},
            {"label": "Timezone", "value": runtime.account.default_timezone},
            {"label": "Currency", "value": runtime.account.default_currency},
            {"label": "Default owner", "value": (brief["defaults"]["owner"].full_name or brief["defaults"]["owner"].email) if brief["defaults"]["owner"] else "not set"},
            {"label": "Default operator", "value": (brief["defaults"]["operator"].full_name or brief["defaults"]["operator"].email) if brief["defaults"]["operator"] else "not set"},
            {"label": "Integrations", "value": str(len(onboarding["integration_rows"]))},
            {"label": "Goals", "value": str(onboarding["goals_count"])},
            {"label": "Knowledge items", "value": str(KnowledgeService(session).count_active_items(runtime.account.id))},
            {"label": "Last successful sync", "value": _serialize_datetime(onboarding["last_success"].finished_at) if onboarding["last_success"] else "not run yet"},
            {"label": "Sync health", "value": sync_health["status"]},
        ]
        setup_gaps = [
            {"label": item["label"], "href": item["href"]}
            for item in readiness["next_steps"]
            if "product-ready" not in item["label"].lower()
        ]
        if not setup_gaps:
            setup_gaps.append({"label": "No blocking setup gaps. Account is ready for daily execution.", "href": f"/admin/{runtime.account.slug}/dashboard"})
        health_problems: list[dict[str, object]] = []
        for item in brief["critical_alerts"][:4]:
            health_problems.append(
                {
                    "type": "alert",
                    "title": item.title,
                    "detail": f"{item.code} · {item.severity}",
                    "href": f"/admin/{runtime.account.slug}/alerts-tasks?severity=critical",
                }
            )
        for item in brief["failed_sync_jobs"][:4]:
            health_problems.append(
                {
                    "type": "sync",
                    "title": f"Sync job #{item.id}",
                    "detail": _human_sync_error(item) or item.provider_name,
                    "href": f"/admin/{runtime.account.slug}/ops-sync?sync_state=critical",
                }
            )
        for item in brief["goals_at_risk"][:3]:
            blocker = item["blockers"][0]["metric"]["label"] if item["blockers"] else "goal deviation"
            health_problems.append(
                {
                    "type": "goal",
                    "title": item["goal"].title,
                    "detail": f"{item['summary']['status']} · {blocker}",
                    "href": f"/admin/{runtime.account.slug}/goals?goal_id={item['goal'].id}",
                }
            )
        owner_actions = [
            {"title": item["title"], "context": item["context"], "href": item["href"], "status": item["status"], "type": item["type"]}
            for item in brief["must_do_now"][:6]
        ]
        operator_checklist: list[dict[str, object]] = []
        for item in brief["overdue_tasks"][:4]:
            operator_checklist.append(
                {
                    "title": item.title,
                    "context": f"Task · due {item.due_at or '—'}",
                    "href": f"/admin/{runtime.account.slug}/alerts-tasks?overdue=1",
                }
            )
        for item in integrations_needing_attention[:3]:
            operator_checklist.append(
                {
                    "title": item["integration"].external_ref or item["integration"].display_name,
                    "context": f"Integration · {item['sync_state']}",
                    "href": f"/admin/{runtime.account.slug}/ops-sync?sync_state={item['sync_state']}",
                }
            )
        if not operator_checklist:
            operator_checklist.append(
                {
                    "title": "No operator blockers detected",
                    "context": "Current account state looks healthy for routine execution.",
                    "href": f"/admin/{runtime.account.slug}/dashboard",
                }
            )
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "account": runtime.account,
            "settings": settings_payload,
            "readiness": readiness,
            "brief": brief,
            "configured_now": configured_now,
            "setup_gaps": setup_gaps,
            "health_problems": health_problems[:8],
            "integrations_needing_attention": integrations_needing_attention[:6],
            "owner_actions": owner_actions,
            "operator_checklist": operator_checklist[:8],
            "handoff_summary": {
                "status": readiness["status"],
                "onboarding": f"{onboarding['completed_steps']}/{onboarding['total_steps']}",
                "account_status": runtime.account.status,
                "plan_type": runtime.account.plan_type,
                "sync_health": sync_health["status"],
                "goals_at_risk": readiness["goals_at_risk_count"],
                "critical_alerts": readiness["critical_alerts_count"],
                "overdue_tasks": readiness["overdue_tasks_count"],
                "failed_sync_jobs": readiness["failed_sync_jobs_count"],
            },
        }

    def _knowledge_item_type_options() -> list[str]:
        return ["note", "sop", "policy", "customer_note", "file", "reference"]

    def _knowledge_status_options() -> list[str]:
        return ["active", "archived"]

    def _knowledge_tags(raw: str | None) -> list[str]:
        if not raw:
            return []
        return [item.strip() for item in raw.split(",") if item.strip()]

    def _knowledge_item_linked_customer(item: KnowledgeItem, customer_lookup: dict[int, Customer]) -> Customer | None:
        if item.customer_id is None:
            return None
        return customer_lookup.get(item.customer_id)

    def _knowledge_item_linked_deal(item: KnowledgeItem, deal_lookup: dict[int, Deal]) -> Deal | None:
        if item.deal_id is None:
            return None
        return deal_lookup.get(item.deal_id)

    def _knowledge_storage_path(account_slug: str, upload: UploadFile, content_sha256: str) -> tuple[Path, str]:
        suffix = Path(upload.filename or "attachment.bin").suffix
        safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in Path(upload.filename or "attachment").stem)
        safe_name = safe_name.strip("-_.") or "attachment"
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        relative = Path(account_slug) / datetime.now(timezone.utc).strftime("%Y/%m/%d") / f"{stamp}-{content_sha256[:10]}-{safe_name}{suffix}"
        absolute = knowledge_upload_root / relative
        return absolute, relative.as_posix()

    def _employee_status_options() -> list[str]:
        return ["active", "disabled"]

    def _employee_snapshot_map(runtime: ResolvedRuntimeContext, session: Session) -> dict[int, EmployeeSnapshot]:
        return {item.employee.id: item for item in PeopleService(session).employee_snapshots(runtime.context)}

    def _document_type_options() -> list[str]:
        return ["invoice", "claim", "purchase_order", "receipt", "internal_note"]

    def _document_status_options() -> list[str]:
        return ["draft", "issued", "sent", "paid", "archived"]

    def _installation_status_options() -> list[str]:
        return ["open", "scheduled", "done", "cancelled"]

    def _communication_channel_options() -> list[str]:
        return ["message", "call", "chat", "email"]

    def _communication_direction_options() -> list[str]:
        return ["inbound", "outbound"]

    def _account_delivery_markdown(pack: dict[str, object]) -> str:
        account = pack["account"]
        lines = [
            f"# {account.name} Delivery Brief",
            "",
            f"- Generated: {pack['generated_at']}",
            f"- Account: {account.slug}",
            f"- Status: {pack['handoff_summary']['status']}",
            f"- Plan: {pack['handoff_summary']['plan_type']}",
            f"- Sync health: {pack['handoff_summary']['sync_health']}",
            "",
            "## Configured Now",
        ]
        lines.extend(f"- {item['label']}: {item['value']}" for item in pack["configured_now"])
        lines.append("")
        lines.append("## What Needs Setup")
        lines.extend(f"- {item['label']}" for item in pack["setup_gaps"])
        lines.append("")
        lines.append("## Health Problems")
        if pack["health_problems"]:
            lines.extend(f"- {item['type']}: {item['title']} ({item['detail']})" for item in pack["health_problems"])
        else:
            lines.append("- No critical health problems right now.")
        lines.append("")
        lines.append("## Owner Actions")
        lines.extend(f"- {item['title']} ({item['type']} · {item['context']})" for item in pack["owner_actions"])
        lines.append("")
        lines.append("## Operator Checklist")
        lines.extend(f"- {item['title']} ({item['context']})" for item in pack["operator_checklist"])
        return "\n".join(lines).strip() + "\n"

    def _account_delivery_text(pack: dict[str, object]) -> str:
        account = pack["account"]
        lines = [
            f"{account.name} delivery brief",
            f"generated: {pack['generated_at']}",
            f"account: {account.slug}",
            f"status: {pack['handoff_summary']['status']}",
            f"sync health: {pack['handoff_summary']['sync_health']}",
            "",
            "configured now:",
        ]
        lines.extend(f"- {item['label']}: {item['value']}" for item in pack["configured_now"])
        lines.append("")
        lines.append("what to do first:")
        lines.extend(f"- {item['label']}" for item in pack["setup_gaps"])
        lines.append("")
        lines.append("owner actions:")
        lines.extend(f"- {item['title']} ({item['context']})" for item in pack["owner_actions"])
        lines.append("")
        lines.append("operator checklist:")
        lines.extend(f"- {item['title']} ({item['context']})" for item in pack["operator_checklist"])
        return "\n".join(lines).strip() + "\n"

    def _runtime_visibility(session: Session) -> dict[str, object]:
        db_ok = True
        db_error = None
        revision = None
        try:
            session.execute(text("select 1"))
            revision = session.execute(text("select version_num from alembic_version")).scalar_one_or_none()
        except Exception as exc:
            db_ok = False
            db_error = str(exc)
        worker_status = read_runtime_status("worker_status")
        backup_status = read_runtime_status("backup_status")
        smoke_status = read_runtime_status("smoke_status")
        verify_status = read_runtime_status("verify_status")
        delivery_status = read_runtime_status("delivery_status")
        obsidian_status = read_runtime_status("obsidian_status")
        now = datetime.now(timezone.utc)
        worker_health = "unknown"
        worker_age_seconds = None
        if worker_status is not None:
            timestamp = worker_status.get("finished_at") or worker_status.get("written_at")
            if isinstance(timestamp, str):
                parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                worker_age_seconds = int((now - parsed.astimezone(timezone.utc)).total_seconds())
            if worker_status.get("status") == "error":
                worker_health = "critical"
            elif worker_age_seconds is not None and worker_age_seconds <= settings.worker_poll_interval_seconds * 3:
                worker_health = "healthy"
            else:
                worker_health = "warning"
        lease_rows = session.execute(select(Account.id, RuntimeLease.lease_key, RuntimeLease.owner, RuntimeLease.heartbeat_at)).all()
        warnings: list[str] = []
        if not db_ok:
            warnings.append("Database connectivity check failed.")
        if worker_status is None:
            warnings.append("Worker status has not been recorded yet.")
        elif worker_health != "healthy":
            warnings.append("Worker heartbeat looks stale or unhealthy.")
        if backup_status is None:
            warnings.append("Backup status has not been recorded yet.")
        if smoke_status is None:
            warnings.append("Smoke runtime check has not been recorded yet.")
        if verify_status is None:
            warnings.append("DB verification status has not been recorded yet.")
        if delivery_status is None:
            warnings.append("Delivery status has not been recorded yet.")
        return {
            "app_version": settings.app_version,
            "environment": settings.environment,
            "database_url": settings.database_url,
            "database_backend": settings.database_url.split(":", 1)[0],
            "database_status": "ok" if db_ok else "error",
            "database_error": db_error,
            "current_revision": revision,
            "worker_status": worker_status,
            "worker_health": worker_health,
            "worker_age_seconds": worker_age_seconds,
            "worker_id": settings.worker_id,
            "runtime_leases": [
                {
                    "account_id": account_id,
                    "lease_key": lease_key,
                    "owner": owner,
                    "heartbeat_at": _serialize_datetime(heartbeat_at),
                }
                for account_id, lease_key, owner, heartbeat_at in lease_rows
            ],
            "backup_status": backup_status,
            "smoke_status": smoke_status,
            "verify_status": verify_status,
            "delivery_status": delivery_status,
            "obsidian_status": obsidian_status,
            "warnings": warnings,
        }

    def _write_account_obsidian_export(
        *,
        actor_email: str,
        account_slug: str,
        account_name: str,
        generated_at: str,
        markdown_text: str,
    ) -> dict[str, str]:
        try:
            paths = export_account_delivery_note(
                account_slug=account_slug,
                account_name=account_name,
                generated_at=generated_at,
                markdown_text=markdown_text,
                settings=settings,
            )
        except Exception as exc:
            write_runtime_status(
                "obsidian_status",
                {
                    "status": "error",
                    "scope": "account",
                    "account_slug": account_slug,
                    "actor_email": actor_email,
                    "error": str(exc),
                },
            )
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Obsidian export failed: {exc}") from exc
        write_runtime_status(
            "obsidian_status",
            {
                "status": "ok",
                "scope": "account",
                "account_slug": account_slug,
                "actor_email": actor_email,
                "paths": paths,
            },
        )
        return paths

    def _write_portfolio_obsidian_export(
        *,
        actor_email: str,
        generated_at: str,
        markdown_text: str,
    ) -> dict[str, str]:
        try:
            paths = export_portfolio_brief_note(
                generated_at=generated_at,
                markdown_text=markdown_text,
                settings=settings,
            )
        except Exception as exc:
            write_runtime_status(
                "obsidian_status",
                {
                    "status": "error",
                    "scope": "portfolio",
                    "actor_email": actor_email,
                    "error": str(exc),
                },
            )
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Obsidian export failed: {exc}") from exc
        write_runtime_status(
            "obsidian_status",
            {
                "status": "ok",
                "scope": "portfolio",
                "actor_email": actor_email,
                "paths": paths,
            },
        )
        return paths

    def _goal_period_defaults(period_kind: str, runtime: ResolvedRuntimeContext) -> tuple[date, date]:
        try:
            zone = ZoneInfo(runtime.account.default_timezone)
        except Exception:
            zone = timezone.utc
        today = datetime.now(zone).date()
        if period_kind == "week":
            start = today - timedelta(days=today.weekday())
            return start, start + timedelta(days=6)
        if period_kind == "month":
            start = today.replace(day=1)
            if start.month == 12:
                next_month = start.replace(year=start.year + 1, month=1, day=1)
            else:
                next_month = start.replace(month=start.month + 1, day=1)
            return start, next_month - timedelta(days=1)
        return today, today

    def _goal_period_from_payload(
        runtime: ResolvedRuntimeContext,
        period_kind: str,
        period_start: date | None,
        period_end: date | None,
    ) -> tuple[date, date]:
        default_start, default_end = _goal_period_defaults(period_kind, runtime)
        return period_start or default_start, period_end or default_end

    def _alert_sla_map() -> dict[str, str]:
        return {
            "bank.balance_below_safe_threshold": "same business hour",
            "inventory.stock_below_threshold": "same day",
            "lead.no_first_response": "30 min max",
            "marketing.cpl_above_threshold": "same day",
            "leads.lost_above_threshold": "same day",
            "task.overdue_escalation": "same day",
        }

    def _human_sync_error(job) -> str | None:
        if job is None:
            return None
        if job.error_message:
            return job.error_message
        if job.error_code:
            return f"{job.error_code}: sync failed."
        if job.status in {"failed", "retry"}:
            return f"Last sync ended with status {job.status}."
        return None

    def _goal_metric_alert_map() -> dict[str, list[str]]:
        return {
            "available_cash": ["bank.balance_below_safe_threshold"],
            "low_stock_items": ["inventory.stock_below_threshold"],
            "cpl": ["marketing.cpl_above_threshold"],
            "lost_leads": ["leads.lost_above_threshold"],
            "incoming_leads": ["leads.lost_above_threshold", "lead.no_first_response"],
            "first_response_breaches": ["lead.no_first_response"],
            "revenue": ["marketing.cpl_above_threshold", "leads.lost_above_threshold"],
            "net_profit": ["marketing.cpl_above_threshold", "bank.balance_below_safe_threshold"],
        }

    def _goal_metric_links(account_slug: str, actor_email: str, metric_code: str) -> dict[str, str]:
        del actor_email
        severity = "critical"
        priority = "high"
        if metric_code == "cpl":
            severity = "warning"
        return {
            "alerts": f"/admin/{account_slug}/alerts-tasks?severity={severity}",
            "tasks": f"/admin/{account_slug}/alerts-tasks?priority={priority}",
            "ops": f"/admin/{account_slug}/ops-sync",
        }

    def _goal_blocker_rows(
        *,
        account_slug: str,
        actor_email: str,
        metrics: list[dict[str, object]],
        open_alerts: list[object],
        open_tasks: list[object],
        top_problems: list[dict[str, object]],
        attention_zones: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        blockers: list[dict[str, object]] = []
        alert_map = _goal_metric_alert_map()
        tasks_by_id = {getattr(item, "id", None): item for item in open_tasks}
        for metric in metrics:
            if metric["status"] == "on_track":
                continue
            metric_code = str(metric["metric_code"])
            related_codes = alert_map.get(metric_code, [])
            related_alerts = [item for item in open_alerts if item.code in related_codes][:3]
            related_problems = [item for item in top_problems if item["code"] in related_codes][:3]
            problem_task_ids = [item["task_id"] for item in related_problems if item.get("task_id") is not None]
            related_tasks = [tasks_by_id[item_id] for item_id in problem_task_ids if item_id in tasks_by_id][:3]
            if not related_tasks:
                related_tasks = [
                    item for item in open_tasks
                    if item.priority in {"critical", "high"}
                ][:3]
            related_actions = [
                item for item in attention_zones
                if item.get("action_type") == "task" and item.get("task_id") in problem_task_ids
            ][:3]
            if not related_actions:
                related_actions = attention_zones[:3]
            blockers.append(
                {
                    "metric": metric,
                    "alert_codes": related_codes,
                    "related_alerts": related_alerts,
                    "related_tasks": related_tasks,
                    "related_problems": related_problems,
                    "attention_actions": related_actions,
                    "links": _goal_metric_links(account_slug, actor_email, metric_code),
                }
            )
        return blockers

    def _enrich_goal_snapshot(
        *,
        account_slug: str,
        actor_email: str,
        snapshot: dict[str, object],
        open_alerts: list[object],
        open_tasks: list[object],
        top_problems: list[dict[str, object]],
        attention_zones: list[dict[str, object]],
    ) -> dict[str, object]:
        enriched = dict(snapshot)
        enriched["blockers"] = _goal_blocker_rows(
            account_slug=account_slug,
            actor_email=actor_email,
            metrics=list(snapshot["metrics"]),
            open_alerts=open_alerts,
            open_tasks=open_tasks,
            top_problems=top_problems,
            attention_zones=attention_zones,
        )
        return enriched

    def _parse_admin_payload(request_payload: dict[str, object], key: str) -> dict[str, object]:
        value = request_payload.get(key) or {}
        if not isinstance(value, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{key} must be an object.")
        return value

    @app.get("/health")
    def health(session: Session = Depends(get_db_session)) -> dict[str, object]:
        session.execute(text("select 1"))
        return {
            "status": "ok",
            "service": "platform-runtime",
            "database": "ok",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @app.get("/api/auth/context")
    def auth_context(runtime: ResolvedRuntimeContext = Depends(get_runtime_context)) -> dict[str, object]:
        return {
            "account": {"id": runtime.account.id, "slug": runtime.account.slug, "name": runtime.account.name},
            "actor_user": {"id": runtime.actor_user.id, "email": runtime.actor_user.email, "full_name": runtime.actor_user.full_name},
            "permissions": sorted(runtime.permissions),
        }

    @app.get("/api/dashboard/summary")
    def dashboard_summary(
        period: str = Query(default="today", pattern="^(today|yesterday|week|month)$"),
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "dashboard.read")
        return ExecutiveDashboardService(session).get_dashboard(runtime.context, period)

    @app.get("/api/dashboard/widgets/{widget_key}")
    def dashboard_widget(
        widget_key: str,
        period: str = Query(default="today", pattern="^(today|yesterday|week|month)$"),
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "dashboard.read")
        payload = ExecutiveDashboardService(session).get_dashboard(runtime.context, period)
        for widget in payload["widgets"]:
            if widget["widget_key"] == widget_key:
                return widget
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Widget not found.")

    @app.get("/api/dashboard/owner-panel")
    def owner_panel(
        period: str = Query(default="today", pattern="^(today|yesterday|week|month)$"),
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "dashboard.read")
        payload = ExecutiveDashboardService(session).get_dashboard(runtime.context, period)
        for widget in payload["widgets"]:
            if widget["widget_key"] == "owner_panel":
                return widget["payload"]
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Owner panel not found.")

    @app.post("/api/automation/rules/run")
    def run_rules(
        body: RuleRunRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "rules.manage")
        results = RuntimeAutomationService(session).run_all_rules(runtime.context, now=body.now)
        return {"count": len(results), "results": results}

    @app.post("/api/automation/rules/run/{rule_code}")
    def run_rule(
        rule_code: str,
        body: RuleRunRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "rules.manage")
        results = RuntimeAutomationService(session).run_rule(runtime.context, rule_code, now=body.now)
        return {"count": len(results), "results": results}

    @app.get("/api/automation/alerts")
    def list_alerts(
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> list[dict[str, object]]:
        ensure_permission(runtime, "alerts.read")
        return [_serialize_alert(item) for item in RuntimeAutomationService(session).list_alerts(runtime.context)]

    @app.get("/api/automation/tasks")
    def list_tasks(
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> list[dict[str, object]]:
        ensure_permission(runtime, "tasks.read")
        return [_serialize_task(item) for item in RuntimeAutomationService(session).list_tasks(runtime.context)]

    @app.get("/api/automation/recommendations")
    def list_recommendations(
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> list[dict[str, object]]:
        ensure_permission(runtime, "rules.manage")
        return [
            _serialize_recommendation(item)
            for item in RuntimeAutomationService(session).list_recommendations(runtime.context)
        ]

    @app.get("/api/integrations")
    def list_integrations(
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> list[dict[str, object]]:
        ensure_permission(runtime, "integrations.manage")
        return [_serialize_integration(item) for item in RuntimeIntegrationService(session).list_integrations(runtime.context)]

    @app.post("/api/integrations")
    def create_integration_api(
        body: IntegrationCreateRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "integrations.manage")
        service = RuntimeIntegrationService(session)
        try:
            integration = service.create_integration(
                runtime.context,
                provider_kind=body.provider_kind,
                provider_name=body.provider_name,
                display_name=body.display_name,
                external_ref=body.external_ref,
                status=body.status,
                connection_mode=body.connection_mode,
                sync_mode=body.sync_mode,
                settings_json=body.settings,
            )
        except (PlatformCoreError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {"integration": _serialize_integration(integration)}

    @app.patch("/api/integrations/{integration_id}")
    def update_integration_api(
        integration_id: int,
        body: IntegrationUpdateRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "integrations.manage")
        service = RuntimeIntegrationService(session)
        try:
            integration = service.update_integration(
                runtime.context,
                integration_id=integration_id,
                display_name=body.display_name,
                external_ref=body.external_ref,
                status=body.status,
                connection_mode=body.connection_mode,
                sync_mode=body.sync_mode,
                settings_json=body.settings,
            )
        except (TenantContextError, PlatformCoreError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {"integration": _serialize_integration(integration)}

    @app.get("/api/integrations/{integration_id}/setup")
    def integration_setup_api(
        integration_id: int,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "integrations.manage")
        service = RuntimeIntegrationService(session)
        try:
            payload = service.integration_setup_payload(runtime.context, integration_id=integration_id)
        except TenantContextError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return {
            "integration": _serialize_integration(payload["integration"]),
            "masked_credentials": payload["masked_credentials"],
            "credential_version": payload["credential_version"],
            "credential_last_rotated_at": _serialize_datetime(payload["credential_last_rotated_at"]),
            "latest_jobs": [_serialize_sync_job(item) for item in payload["latest_jobs"]],
        }

    @app.post("/api/integrations/{integration_id}/credentials")
    def save_credentials_api(
        integration_id: int,
        body: CredentialSaveRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "integrations.manage")
        service = RuntimeIntegrationService(session)
        if body.replace_mode == "replace" and not body.credentials:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Replace mode requires a new credential payload.")
        try:
            credential = service.save_credentials(
                runtime.context,
                integration_id=integration_id,
                secret_payload=body.credentials,
                credential_type=body.credential_type,
                replace_mode=body.replace_mode,
            )
            payload = service.integration_setup_payload(runtime.context, integration_id=integration_id)
        except (TenantContextError, PlatformCoreError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {
            "credential_version": credential.version,
            "masked_credentials": payload["masked_credentials"],
            "credential_last_rotated_at": _serialize_datetime(payload["credential_last_rotated_at"]),
        }

    @app.post("/api/integrations/{integration_id}/test-connection")
    def test_connection_api(
        integration_id: int,
        body: TestConnectionRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "integrations.manage")
        service = RuntimeIntegrationService(session)
        try:
            return service.test_connection(
                runtime.context,
                integration_id=integration_id,
                override_payload=body.credentials,
            )
        except (TenantContextError, PlatformCoreError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    @app.post("/api/integrations/{integration_id}/sync")
    def enqueue_integration_sync(
        integration_id: int,
        body: SyncJobCreateRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "integrations.manage")
        service = RuntimeIntegrationService(session)
        job, created = service.enqueue_sync_job(
            runtime.context,
            integration_id=integration_id,
            job_type=body.job_type,
            trigger_mode="manual",
            idempotency_key=body.idempotency_key,
            scope_json=body.scope,
        )
        execution = None
        if body.execute_now:
            execution = service.execute_job(job.id, owner=settings.worker_id, ttl_seconds=settings.runtime_lease_ttl_seconds)
        return {
            "created": created,
            "job": _serialize_sync_job(job),
            "execution": _serialize_job_execution(execution) if execution is not None else None,
        }

    @app.get("/api/integrations/sync-jobs")
    def list_sync_jobs(
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> list[dict[str, object]]:
        ensure_permission(runtime, "integrations.manage")
        return [_serialize_sync_job(item) for item in RuntimeIntegrationService(session).list_sync_jobs(runtime.context)]

    @app.get("/api/integrations/logs")
    def list_integration_logs(
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> list[dict[str, object]]:
        ensure_permission(runtime, "integrations.manage")
        return [_serialize_integration_log(item) for item in RuntimeIntegrationService(session).list_logs(runtime.context)]

    @app.post("/api/admin/runtime/scheduler/run-once")
    def scheduler_run_once(
        body: SchedulerRunRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "rules.manage")
        ensure_permission(runtime, "integrations.manage")
        scheduler = SchedulerService(
            session,
            worker_id=settings.worker_id,
            lease_ttl_seconds=settings.runtime_lease_ttl_seconds,
        )
        result = scheduler.run_once(run_rules=body.run_rules, run_sync_jobs=body.run_sync_jobs)
        return {"requested": body.model_dump(), "result": result}

    @app.get("/api/admin/accounts/{account_id}/audit-logs")
    def audit_logs(
        account_id: int,
        limit: int = Query(default=50, ge=1, le=500),
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> list[dict[str, object]]:
        ensure_permission(runtime, "audit.read")
        if runtime.context.account_id != account_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cross-account audit access denied.")
        return [_serialize_audit_log(item) for item in AdminQueryService(session).list_audit_logs(account_id, limit)]

    @app.get("/api/admin/accounts/{account_id}/ops-summary")
    def ops_summary(
        account_id: int,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "integrations.manage")
        ensure_permission(runtime, "rules.manage")
        ensure_permission(runtime, "tasks.read")
        ensure_permission(runtime, "alerts.read")
        if runtime.context.account_id != account_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cross-account ops access denied.")
        payload = AdminQueryService(session).ops_summary(account_id)
        return {
            "generated_at": payload["generated_at"],
            "recent_failed_sync_jobs": [_serialize_sync_job(item) for item in payload["recent_failed_sync_jobs"]],
            "recent_failed_rule_runs": [_serialize_rule_execution(item) for item in payload["recent_failed_rule_runs"]],
            "overdue_tasks": [_serialize_task(item) for item in payload["overdue_tasks"]],
            "active_critical_alerts": [_serialize_alert(item) for item in payload["active_critical_alerts"]],
            "integration_sync_status": [
                {
                    "integration": _serialize_integration(item["integration"]),
                    "latest_success": _serialize_sync_job(item["latest_success"]) if item["latest_success"] is not None else None,
                    "latest_failure": _serialize_sync_job(item["latest_failure"]) if item["latest_failure"] is not None else None,
                }
                for item in payload["integration_sync_status"]
            ],
        }

    @app.get("/api/goals")
    def list_goals_api(
        status_filter: str | None = Query(default=None, alias="status"),
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> list[dict[str, object]]:
        ensure_permission(runtime, "dashboard.read")
        service = GoalService(session)
        payload = []
        for goal in service.list_goals(runtime.context, status=status_filter):
            metrics = service.get_goal_metrics(runtime.context, goal.id)
            payload.append(_serialize_goal(goal, summary=metrics["summary"]))
        return payload

    @app.post("/api/goals")
    def create_goal_api(
        body: GoalCreateRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "rules.manage")
        service = GoalService(session)
        period_start, period_end = _goal_period_from_payload(runtime, body.period_kind, body.period_start, body.period_end)
        try:
            goal = service.create_goal(
                runtime.context,
                title=body.title,
                description=body.description,
                period_kind=body.period_kind,
                period_start=period_start,
                period_end=period_end,
                owner_user_id=body.owner_user_id,
                is_primary=body.is_primary,
                status=body.status,
                targets=[item.model_dump(exclude_none=True) for item in body.targets],
            )
        except PlatformCoreError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        metrics = service.get_goal_metrics(runtime.context, goal.id)
        return {
            "goal": _serialize_goal(goal, summary=metrics["summary"]),
            "targets": [_serialize_goal_target(item) for item in metrics["targets"]],
        }

    @app.get("/api/goals/{goal_id}")
    def get_goal_api(
        goal_id: int,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "dashboard.read")
        service = GoalService(session)
        try:
            metrics = service.get_goal_metrics(runtime.context, goal_id)
        except TenantContextError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return {
            "goal": _serialize_goal(metrics["goal"], summary=metrics["summary"]),
            "targets": [_serialize_goal_target(item) for item in metrics["targets"]],
        }

    @app.patch("/api/goals/{goal_id}")
    def update_goal_api(
        goal_id: int,
        body: GoalUpdateRequest,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "rules.manage")
        service = GoalService(session)
        payload = body.model_dump(exclude_unset=True, exclude_none=False)
        if "period_kind" in payload:
            period_start, period_end = _goal_period_from_payload(
                runtime,
                payload["period_kind"],
                payload.get("period_start"),
                payload.get("period_end"),
            )
            payload["period_start"] = period_start
            payload["period_end"] = period_end
        try:
            goal = service.update_goal(
                runtime.context,
                goal_id,
                title=payload.get("title"),
                description=payload.get("description"),
                period_kind=payload.get("period_kind"),
                period_start=payload.get("period_start"),
                period_end=payload.get("period_end"),
                owner_user_id=payload.get("owner_user_id"),
                is_primary=payload.get("is_primary"),
                status=payload.get("status"),
                targets=[item.model_dump(exclude_none=True) for item in body.targets] if body.targets is not None else None,
            )
        except TenantContextError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except PlatformCoreError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        metrics = service.get_goal_metrics(runtime.context, goal.id)
        return {
            "goal": _serialize_goal(goal, summary=metrics["summary"]),
            "targets": [_serialize_goal_target(item) for item in metrics["targets"]],
        }

    @app.get("/api/goals/{goal_id}/metrics")
    def get_goal_metrics_api(
        goal_id: int,
        session: Session = Depends(get_db_session),
        runtime: ResolvedRuntimeContext = Depends(get_runtime_context),
    ) -> dict[str, object]:
        ensure_permission(runtime, "dashboard.read")
        service = GoalService(session)
        try:
            metrics = service.get_goal_metrics(runtime.context, goal_id)
        except TenantContextError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return {
            "goal": _serialize_goal(metrics["goal"], summary=metrics["summary"]),
            "metrics": metrics["metrics"],
            "summary": metrics["summary"],
        }

    @app.get("/admin/login", response_class=HTMLResponse)
    def admin_login_page(
        request: Request,
        session: Session = Depends(get_db_session),
        next: str | None = Query(default=None),
        reset: bool = Query(default=False),
    ) -> HTMLResponse:
        if _current_session_user(session, request) is not None:
            return RedirectResponse(url=next or "/admin", status_code=status.HTTP_302_FOUND)
        csrf_token = _ensure_session_csrf_token(request)
        return templates.TemplateResponse(
            request,
            "admin/access.html",
            {
                "next_path": next,
                "error_message": None,
                "prefill_email": "",
                "bootstrap_error": None,
                "bootstrap_email": "",
                "bootstrap_claim_url": None,
                "csrf_token": csrf_token,
                "reset_notice": reset,
            },
        )

    @app.post("/admin/login", response_class=HTMLResponse)
    async def admin_login_submit(
        request: Request,
        session: Session = Depends(get_db_session),
    ):
        await _require_csrf(request)
        payload = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)
        email = str((payload.get("email") or [""])[0]).strip().lower()
        password = str((payload.get("password") or [""])[0]).strip()
        next_path = str((payload.get("next") or [""])[0]).strip() or None
        csrf_token = _ensure_session_csrf_token(request)
        if not email or not password:
            return templates.TemplateResponse(
                request,
                "admin/access.html",
                {
                    "next_path": next_path,
                    "error_message": "Email and password are required.",
                    "prefill_email": email,
                    "bootstrap_error": None,
                    "bootstrap_email": "",
                    "bootstrap_claim_url": None,
                    "csrf_token": csrf_token,
                    "reset_notice": False,
                },
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        auth = UserSecurityService(session).authenticate(email, password)
        if not auth.ok or auth.user is None:
            if auth.user is not None:
                _audit_admin_access(session, actor_email=email, action="admin.auth.login_failed", details={"reason": auth.reason})
            return templates.TemplateResponse(
                request,
                "admin/access.html",
                {
                    "next_path": next_path,
                    "error_message": auth.reason,
                    "prefill_email": email,
                    "bootstrap_error": None,
                    "bootstrap_email": "",
                    "bootstrap_claim_url": None,
                    "csrf_token": csrf_token,
                    "reset_notice": False,
                },
                status_code=status.HTTP_401_UNAUTHORIZED,
            )
        memberships = _accessible_accounts(session, email)
        if not memberships:
            return templates.TemplateResponse(
                request,
                "admin/access.html",
                {
                    "next_path": next_path,
                    "error_message": "No active account memberships found for this email.",
                    "prefill_email": email,
                    "bootstrap_error": None,
                    "bootstrap_email": "",
                    "bootstrap_claim_url": None,
                    "csrf_token": csrf_token,
                    "reset_notice": False,
                },
                status_code=status.HTTP_403_FORBIDDEN,
            )
        current_account = _session_account_slug(request)
        request.session.clear()
        request.session["admin_actor_email"] = email
        request.session["admin_auth_version"] = int(auth.user.auth_version or 1)
        allowed_slugs = {item.slug for item in memberships}
        request.session["admin_account_slug"] = current_account if current_account in allowed_slugs else memberships[0].slug
        request.session["admin_csrf_token"] = secrets.token_urlsafe(24)
        _audit_admin_access(session, actor_email=email, action="admin.auth.login", details={"mode": "password"})
        return RedirectResponse(url=next_path or "/admin", status_code=status.HTTP_302_FOUND)

    @app.post("/admin/bootstrap-access", response_class=HTMLResponse)
    async def admin_bootstrap_access(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        await _require_csrf(request)
        payload = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)
        email = str((payload.get("bootstrap_email") or [""])[0]).strip().lower()
        access_code = str((payload.get("access_code") or [""])[0]).strip()
        next_path = str((payload.get("next") or [""])[0]).strip() or None
        csrf_token = _ensure_session_csrf_token(request)
        if not email or not access_code:
            return templates.TemplateResponse(
                request,
                "admin/access.html",
                {
                    "next_path": next_path,
                    "error_message": None,
                    "prefill_email": "",
                    "bootstrap_error": "Email and bootstrap access code are required.",
                    "bootstrap_email": email,
                    "bootstrap_claim_url": None,
                    "csrf_token": csrf_token,
                    "reset_notice": False,
                },
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if access_code != settings.admin_access_code:
            return templates.TemplateResponse(
                request,
                "admin/access.html",
                {
                    "next_path": next_path,
                    "error_message": None,
                    "prefill_email": "",
                    "bootstrap_error": "Invalid bootstrap access code.",
                    "bootstrap_email": email,
                    "bootstrap_claim_url": None,
                    "csrf_token": csrf_token,
                    "reset_notice": False,
                },
                status_code=status.HTTP_401_UNAUTHORIZED,
            )
        user = UserSecurityService(session).get_by_email(email)
        if user is None or user.status == "disabled":
            return templates.TemplateResponse(
                request,
                "admin/access.html",
                {
                    "next_path": next_path,
                    "error_message": None,
                    "prefill_email": "",
                    "bootstrap_error": "User not found or disabled.",
                    "bootstrap_email": email,
                    "bootstrap_claim_url": None,
                    "csrf_token": csrf_token,
                    "reset_notice": False,
                },
                status_code=status.HTTP_404_NOT_FOUND,
            )
        memberships = _accessible_accounts(session, email)
        if not memberships:
            return templates.TemplateResponse(
                request,
                "admin/access.html",
                {
                    "next_path": next_path,
                    "error_message": None,
                    "prefill_email": "",
                    "bootstrap_error": "No active account memberships found for this email.",
                    "bootstrap_email": email,
                    "bootstrap_claim_url": None,
                    "csrf_token": csrf_token,
                    "reset_notice": False,
                },
                status_code=status.HTTP_403_FORBIDDEN,
            )
        token = UserSecurityService(session).issue_password_reset(user)
        claim_url = f"/admin/password/claim?token={token}"
        _audit_admin_access(session, actor_email=email, action="admin.auth.bootstrap_reset_issued", details={"mode": "access_code"})
        return templates.TemplateResponse(
            request,
            "admin/access.html",
            {
                "next_path": next_path,
                "error_message": None,
                "prefill_email": "",
                "bootstrap_error": None,
                "bootstrap_email": email,
                "bootstrap_claim_url": claim_url,
                "csrf_token": csrf_token,
                "reset_notice": False,
            },
        )

    @app.get("/admin/password/claim", response_class=HTMLResponse)
    def admin_password_claim_page(
        request: Request,
        token: str = Query(..., min_length=20),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        csrf_token = _ensure_session_csrf_token(request)
        try:
            user, token_type = UserSecurityService(session).token_claim_preview(token)
            error_message = None
        except AuthorizationError as exc:
            user = None
            token_type = None
            error_message = str(exc)
        return templates.TemplateResponse(
            request,
            "admin/password_claim.html",
            {
                "csrf_token": csrf_token,
                "token": token,
                "user": user,
                "token_type": token_type,
                "error_message": error_message,
            },
            status_code=status.HTTP_200_OK if error_message is None else status.HTTP_400_BAD_REQUEST,
        )

    @app.post("/admin/password/claim", response_class=HTMLResponse)
    async def admin_password_claim_submit(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        await _require_csrf(request)
        payload = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)
        token = str((payload.get("token") or [""])[0]).strip()
        password = str((payload.get("password") or [""])[0]).strip()
        password_confirm = str((payload.get("password_confirm") or [""])[0]).strip()
        csrf_token = _ensure_session_csrf_token(request)
        if not token or not password:
            return templates.TemplateResponse(
                request,
                "admin/password_claim.html",
                {"csrf_token": csrf_token, "token": token, "user": None, "token_type": None, "error_message": "Token and password are required."},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if password != password_confirm:
            return templates.TemplateResponse(
                request,
                "admin/password_claim.html",
                {"csrf_token": csrf_token, "token": token, "user": None, "token_type": None, "error_message": "Password confirmation does not match."},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        try:
            user = UserSecurityService(session).claim_password(token, password)
        except (AuthorizationError, PlatformCoreError) as exc:
            return templates.TemplateResponse(
                request,
                "admin/password_claim.html",
                {"csrf_token": csrf_token, "token": token, "user": None, "token_type": None, "error_message": str(exc)},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        _audit_admin_access(session, actor_email=user.email, action="admin.auth.password_claim", details={"mode": "token"})
        return RedirectResponse(url="/admin/login?reset=1", status_code=status.HTTP_302_FOUND)

    @app.post("/admin/logout")
    async def admin_logout(request: Request, session: Session = Depends(get_db_session)) -> RedirectResponse:
        await _require_csrf(request)
        actor_email = _session_actor_email(request)
        if actor_email:
            _audit_admin_access(session, actor_email=actor_email, action="admin.auth.logout", details={"mode": "session"})
        request.session.clear()
        return RedirectResponse(url="/admin/login", status_code=status.HTTP_302_FOUND)

    @app.get("/admin")
    def admin_home(
        request: Request,
        choose: bool = Query(default=False),
        session: Session = Depends(get_db_session),
    ):
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect("/admin")
        actor_email = user.email
        accounts = _accessible_accounts(session, actor_email)
        if not accounts:
            request.session.clear()
            return _login_redirect("/admin")
        active_slug = _session_account_slug(request)
        if not choose and active_slug and any(item.slug == active_slug for item in accounts):
            return RedirectResponse(url=f"/admin/{active_slug}/dashboard", status_code=status.HTTP_302_FOUND)
        if len(accounts) == 1:
            request.session["admin_account_slug"] = accounts[0].slug
            return RedirectResponse(url=f"/admin/{accounts[0].slug}/dashboard", status_code=status.HTTP_302_FOUND)
        return templates.TemplateResponse(
            request,
            "admin/account_select.html",
            {"accounts": accounts, "actor_email": actor_email, "csrf_token": _ensure_session_csrf_token(request)},
        )

    @app.get("/admin/switch-account/{account_slug}")
    def admin_switch_account(
        request: Request,
        account_slug: str,
        next: str | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> RedirectResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/switch-account/{account_slug}")
        actor_email = user.email
        allowed = {item.slug for item in _accessible_accounts(session, actor_email)}
        if account_slug not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cross-account switch denied.")
        request.session["admin_account_slug"] = account_slug
        return RedirectResponse(url=next or f"/admin/{account_slug}/dashboard", status_code=status.HTTP_302_FOUND)

    @app.get("/admin/portfolio", response_class=HTMLResponse)
    def admin_portfolio(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect("/admin/portfolio")
        actor_email = user.email
        _require_portfolio_owner(session, actor_email)
        owner_rows = _owner_accounts_for_portfolio(session, actor_email)
        portfolio_rows = [_portfolio_account_row(session, actor_email, item["account"]) for item in owner_rows]
        portfolio_rows.sort(key=lambda item: (-int(item["risk_score"]), item["account"].name.lower()))
        portfolio = _portfolio_summary(portfolio_rows)
        brief = _portfolio_brief(portfolio)
        return templates.TemplateResponse(
            request,
            "admin/portfolio.html",
            {
                "page": "portfolio",
                "page_path": "portfolio",
                "actor_email": actor_email,
                "csrf_token": _ensure_session_csrf_token(request),
                "flashes": _pop_flashes(request),
                "can_manage_accounts_global": _actor_can_manage_accounts(session, actor_email),
                "can_manage_users_global": _actor_can_manage_accounts(session, actor_email),
                "can_view_portfolio": True,
                "can_view_platform": _actor_can_manage_accounts(session, actor_email),
                "can_view_super_admin": _actor_can_manage_accounts(session, actor_email),
                "shell_brand_title": "Hermes Admin",
                "shell_brand_subtitle": "Cross-account owner console",
                "portfolio_rows": portfolio_rows,
                "portfolio": portfolio,
                "brief": brief,
                "human_sync_error": _human_sync_error,
            },
        )

    @app.get("/admin/platform", response_class=HTMLResponse)
    def admin_platform_page(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect("/admin/platform")
        actor_email = user.email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        account_readiness_rows = _platform_account_readiness_rows(session, actor_email)
        return templates.TemplateResponse(
            request,
            "admin/platform.html",
            {
                "page": "platform",
                "page_path": "platform",
                "actor_email": actor_email,
                "csrf_token": _ensure_session_csrf_token(request),
                "flashes": _pop_flashes(request),
                "can_manage_accounts_global": True,
                "can_manage_users_global": True,
                "can_view_portfolio": _actor_can_view_portfolio(session, actor_email),
                "can_view_platform": True,
                "can_view_super_admin": True,
                "shell_brand_title": "Hermes Admin",
                "shell_brand_subtitle": "Runtime and environment visibility",
                "runtime_visibility": _runtime_visibility(session),
                "platform_health": _platform_health_summary(session, actor_email),
                "account_readiness_rows": account_readiness_rows,
                "plan_profiles": _plan_profiles(),
            },
        )

    @app.get("/admin/super-admin", response_class=HTMLResponse)
    def admin_super_admin_page(
        request: Request,
        selected: str | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect("/admin/super-admin")
        actor_email = user.email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        account_rows = _super_admin_account_rows(session, actor_email)
        selected_slug = selected or _session_account_slug(request) or (account_rows[0]["account"].slug if account_rows else None)
        selected_row = next((row for row in account_rows if row["account"].slug == selected_slug), None)
        return templates.TemplateResponse(
            request,
            "admin/super_admin.html",
            {
                "page": "super_admin",
                "page_path": "super-admin",
                "actor_email": actor_email,
                "csrf_token": _ensure_session_csrf_token(request),
                "flashes": _pop_flashes(request),
                "can_manage_accounts_global": True,
                "can_manage_users_global": True,
                "can_view_portfolio": _actor_can_view_portfolio(session, actor_email),
                "can_view_platform": True,
                "can_view_super_admin": True,
                "shell_brand_title": "Hermes Admin",
                "shell_brand_subtitle": "Platform owner control surface",
                "account_rows": account_rows,
                "selected_row": selected_row,
                "account_status_options": _account_status_options(),
                "account_plan_options": _account_plan_options(),
            },
        )

    @app.post("/admin/super-admin/accounts/{account_slug}/update")
    async def admin_super_admin_update_account(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor = _require_admin_user(request, session)
        actor_email = actor.email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        account = AccountService(session).get_by_slug(account_slug)
        if account is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found.")
        if account.id not in _accessible_account_ids(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is not accessible.")
        payload = await request.json()
        next_status = str(payload.get("status") or account.status).strip()
        next_plan_type = str(payload.get("plan_type") or account.plan_type).strip()
        if next_status not in set(_account_status_options()):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported account status.")
        if next_plan_type not in set(_account_plan_options()):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported plan type.")
        incoming_flags = payload.get("feature_flags")
        incoming_limits = payload.get("soft_limits")
        feature_flags = _account_feature_flags(account)
        if isinstance(incoming_flags, dict):
            for item in _feature_flag_definitions():
                key = item["key"]
                if key in incoming_flags:
                    feature_flags[key] = bool(incoming_flags[key])
        soft_limits = _account_soft_limits(account)
        if isinstance(incoming_limits, dict):
            for item in _soft_limit_definitions():
                key = item["key"]
                if key in incoming_limits:
                    value = incoming_limits[key]
                    soft_limits[key] = None if value in {None, ""} else max(0, int(value))
        before = {
            "status": account.status,
            "plan_type": account.plan_type,
            "feature_flags": _account_feature_flags(account),
            "soft_limits": _account_soft_limits(account),
        }
        AccountService(session).update_account(
            account,
            status=next_status,
            plan_type=next_plan_type,
            feature_flags_json=feature_flags,
            soft_limits_json=soft_limits,
        )
        audit_context = TenantContext(account_id=account.id, actor_user_id=actor.id, source="super-admin", role_code="admin")
        audit = AuditLogService(session)
        if before["status"] != account.status:
            audit.log(
                audit_context,
                "platform.account.status_changed",
                "account",
                str(account.id),
                details={"before": before["status"], "after": account.status},
            )
        if before["plan_type"] != account.plan_type:
            audit.log(
                audit_context,
                "platform.account.plan_changed",
                "account",
                str(account.id),
                details={"before": before["plan_type"], "after": account.plan_type},
            )
        if before["feature_flags"] != feature_flags:
            audit.log(
                audit_context,
                "platform.account.feature_flags_changed",
                "account",
                str(account.id),
                details={"before": before["feature_flags"], "after": feature_flags},
            )
        if before["soft_limits"] != soft_limits:
            audit.log(
                audit_context,
                "platform.account.soft_limits_changed",
                "account",
                str(account.id),
                details={"before": before["soft_limits"], "after": soft_limits},
            )
        readiness = _account_product_readiness(
            RuntimeContextService(session).resolve(
                account_id=account.id,
                account_slug=None,
                actor_user_id=None,
                actor_email=actor_email,
                source="super-admin",
                request_id=request.headers.get("x-request-id"),
            ),
            session,
        )
        return JSONResponse(
            {
                "account": _serialize_account(account),
                "feature_flags": feature_flags,
                "soft_limits": soft_limits,
                "readiness": {
                    "status": readiness["status"],
                    "onboarding": _serialize_onboarding(readiness["onboarding"]),
                    "sync_health": {
                        "status": readiness["sync_health"]["status"],
                        "active_count": readiness["sync_health"]["active_count"],
                        "broken_count": readiness["sync_health"]["broken_count"],
                        "stale_count": readiness["sync_health"]["stale_count"],
                    },
                    "critical_alerts_count": readiness["critical_alerts_count"],
                    "overdue_tasks_count": readiness["overdue_tasks_count"],
                    "goals_at_risk_count": readiness["goals_at_risk_count"],
                    "failed_sync_jobs_count": readiness["failed_sync_jobs_count"],
                    "usage_rows": list(readiness["usage_rows"]),
                    "next_steps": list(readiness["next_steps"]),
                },
            }
        )

    @app.get("/admin/portfolio/brief")
    def admin_portfolio_brief(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> dict[str, object]:
        actor = _require_admin_user(request, session)
        actor_email = actor.email
        _require_portfolio_owner(session, actor_email)
        owner_rows = _owner_accounts_for_portfolio(session, actor_email)
        portfolio_rows = [_portfolio_account_row(session, actor_email, item["account"]) for item in owner_rows]
        portfolio_rows.sort(key=lambda item: (-int(item["risk_score"]), item["account"].name.lower()))
        portfolio = _portfolio_summary(portfolio_rows)
        return _portfolio_brief(portfolio)

    @app.get("/admin/portfolio/brief.md", response_class=PlainTextResponse)
    def admin_portfolio_brief_markdown(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> PlainTextResponse:
        actor = _require_admin_user(request, session)
        actor_email = actor.email
        _require_portfolio_owner(session, actor_email)
        owner_rows = _owner_accounts_for_portfolio(session, actor_email)
        portfolio_rows = [_portfolio_account_row(session, actor_email, item["account"]) for item in owner_rows]
        portfolio_rows.sort(key=lambda item: (-int(item["risk_score"]), item["account"].name.lower()))
        portfolio = _portfolio_summary(portfolio_rows)
        brief = _portfolio_brief(portfolio)
        return PlainTextResponse(_portfolio_brief_markdown(brief), media_type="text/markdown; charset=utf-8")

    @app.get("/admin/portfolio/brief.txt", response_class=PlainTextResponse)
    def admin_portfolio_brief_text(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> PlainTextResponse:
        actor = _require_admin_user(request, session)
        actor_email = actor.email
        _require_portfolio_owner(session, actor_email)
        owner_rows = _owner_accounts_for_portfolio(session, actor_email)
        portfolio_rows = [_portfolio_account_row(session, actor_email, item["account"]) for item in owner_rows]
        portfolio_rows.sort(key=lambda item: (-int(item["risk_score"]), item["account"].name.lower()))
        portfolio = _portfolio_summary(portfolio_rows)
        brief = _portfolio_brief(portfolio)
        return PlainTextResponse(_portfolio_brief_text(brief), media_type="text/plain; charset=utf-8")

    @app.post("/admin/portfolio/brief/generate")
    async def admin_generate_portfolio_brief(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor = _require_admin_user(request, session)
        actor_email = actor.email
        payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
        export_obsidian = bool(payload.get("export_obsidian"))
        _require_portfolio_owner(session, actor_email)
        owner_rows = _owner_accounts_for_portfolio(session, actor_email)
        portfolio_rows = [_portfolio_account_row(session, actor_email, item["account"]) for item in owner_rows]
        portfolio_rows.sort(key=lambda item: (-int(item["risk_score"]), item["account"].name.lower()))
        portfolio = _portfolio_summary(portfolio_rows)
        brief = _portfolio_brief(portfolio)
        paths = write_delivery_bundle(
            scope="portfolio",
            name="portfolio_brief",
            json_payload=brief,
            markdown_text=_portfolio_brief_markdown(brief),
            text_text=_portfolio_brief_text(brief),
        )
        status_payload = {
            "status": "ok",
            "scope": "portfolio",
            "actor_email": actor_email,
            "accounts_count": brief["headline"]["accounts_count"],
            "paths": paths,
        }
        write_runtime_status("delivery_status", status_payload)
        obsidian_paths = None
        if export_obsidian:
            obsidian_paths = _write_portfolio_obsidian_export(
                actor_email=actor_email,
                generated_at=brief["generated_at"],
                markdown_text=_portfolio_brief_markdown(brief),
            )
        return JSONResponse({"brief": brief, "paths": paths, "status": status_payload, "obsidian": obsidian_paths})

    @app.get("/admin/accounts", response_class=HTMLResponse)
    def admin_accounts_page(
        request: Request,
        selected: str | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect("/admin/accounts")
        actor_email = user.email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        account_rows = _accessible_accounts_with_memberships(session, actor_email)
        for row in account_rows:
            row["onboarding"] = _account_onboarding_status(row["account"], session)
        selected_slug = selected or _session_account_slug(request) or (account_rows[0]["account"].slug if account_rows else None)
        selected_onboarding = next(
            (row["onboarding"] for row in account_rows if row["account"].slug == selected_slug),
            None,
        )
        return templates.TemplateResponse(
            request,
            "admin/accounts.html",
            {
                "account_rows": account_rows,
                "selected_onboarding": selected_onboarding,
                "selected_slug": selected_slug,
                "page": "accounts",
                "page_path": "accounts",
                "actor_email": actor_email,
                "can_manage_accounts_global": True,
                "can_manage_users_global": True,
                "can_view_portfolio": _actor_can_view_portfolio(session, actor_email),
                "can_view_platform": True,
                "can_view_super_admin": True,
                "csrf_token": _ensure_session_csrf_token(request),
                "flashes": _pop_flashes(request),
                "shell_brand_title": "Hermes Admin",
                "shell_brand_subtitle": "Global onboarding and account setup",
            },
        )

    @app.post("/admin/accounts/create")
    async def admin_create_account(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        payload = await request.json()
        account_body = _parse_admin_payload(payload, "account")
        owner_body = _parse_admin_payload(payload, "owner")
        admin_body = payload.get("admin") or {}
        if admin_body and not isinstance(admin_body, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="admin must be an object.")
        slug = str(account_body.get("slug") or "").strip().lower()
        name = str(account_body.get("name") or "").strip()
        default_timezone = str(account_body.get("default_timezone") or "Etc/UTC").strip()
        owner_email = str(owner_body.get("email") or "").strip().lower()
        owner_full_name = str(owner_body.get("full_name") or owner_email).strip()
        admin_email = str(admin_body.get("email") or "").strip().lower()
        admin_full_name = str(admin_body.get("full_name") or admin_email).strip()
        if not slug or not name or not owner_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Account slug, name and owner email are required.")
        account_service = AccountService(session)
        user_service = UserService(session)
        membership_service = MembershipService(session)
        try:
            if account_service.get_by_slug(slug) is not None:
                raise PlatformCoreError("Account slug already exists.")
            account, _ = account_service.ensure_account(slug=slug, name=name, default_timezone=default_timezone)
            owner_user, _ = user_service.ensure_user(owner_email, owner_full_name or owner_email)
            membership_service.ensure_membership(account, owner_user, "owner", status="active")
            if admin_email:
                admin_user, _ = user_service.ensure_user(admin_email, admin_full_name or admin_email)
                membership_service.ensure_membership(account, admin_user, "admin", status="active")
            if actor_email not in {owner_email, admin_email}:
                actor_user = user_service.get_by_email(actor_email)
                if actor_user is not None:
                    membership_service.ensure_membership(account, actor_user, "admin", status="active")
            session.flush()
        except (PlatformCoreError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        request.session["admin_account_slug"] = account.slug
        onboarding = _account_onboarding_status(account, session)
        return JSONResponse({"account": _serialize_account(account), "onboarding": _serialize_onboarding(onboarding)})

    @app.get("/admin/users", response_class=HTMLResponse)
    def admin_users_page(
        request: Request,
        user_id: int | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect("/admin/users")
        actor_email = user.email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        user_rows = _accessible_users_with_memberships(session, actor_email)
        selected_user = None
        if user_id is not None:
            selected_user = next((row for row in user_rows if row["user"].id == user_id), None)
            if selected_user is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found in accessible accounts.")
        accounts = _accessible_accounts(session, actor_email)
        roles = session.execute(select(Role).order_by(Role.id.asc())).scalars().all()
        return templates.TemplateResponse(
            request,
            "admin/users.html",
            {
                "page": "users",
                "page_path": "users",
                "actor_email": actor_email,
                "can_manage_accounts_global": True,
                "can_manage_users_global": True,
                "can_view_portfolio": _actor_can_view_portfolio(session, actor_email),
                "can_view_platform": True,
                "can_view_super_admin": True,
                "csrf_token": _ensure_session_csrf_token(request),
                "flashes": _pop_flashes(request),
                "shell_brand_title": "Hermes Admin",
                "shell_brand_subtitle": "Global user lifecycle and access management",
                "user_rows": user_rows,
                "selected_user": selected_user,
                "accounts": accounts,
                "roles": roles,
            },
        )

    @app.post("/admin/users/save")
    async def admin_save_user(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor = _require_admin_user(request, session)
        actor_email = actor.email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        payload = await request.json()
        body = _parse_admin_payload(payload, "user")
        membership_body = payload.get("membership") or {}
        if membership_body and not isinstance(membership_body, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="membership must be an object.")
        service = UserSecurityService(session)
        membership_service = MembershipService(session)
        account_service = AccountService(session)
        accessible_account_slugs = {account.slug for account in _accessible_accounts(session, actor_email)}
        user_id = payload.get("user_id")
        try:
            candidate_email = str(body.get("email") or "").strip().lower()
            existing_user = service.get_by_email(candidate_email) if candidate_email else None
            if user_id is None and existing_user is not None:
                _assert_user_visible_to_actor(session, actor_email, existing_user.id)
            target_user, _ = service.create_or_update_user(
                email=candidate_email,
                full_name=str(body.get("full_name") or "").strip(),
                status=str(body.get("status") or "invited").strip(),
                user_id=int(user_id) if user_id is not None else None,
            )
            membership_summary = None
            account_slug = str(membership_body.get("account_slug") or "").strip()
            role_code = str(membership_body.get("role_code") or "").strip()
            if account_slug and role_code:
                if account_slug not in accessible_account_slugs:
                    raise PlatformCoreError("Cannot assign membership in an inaccessible account.")
                account = account_service.get_by_slug(account_slug)
                if account is None:
                    raise PlatformCoreError("Initial account not found.")
                membership, _ = membership_service.ensure_membership(account, target_user, role_code, status="active")
                membership_summary = _serialize_membership(membership)
            issue_invite = bool(payload.get("issue_invite"))
            invite_link = None
            if issue_invite:
                token = service.issue_invite(target_user)
                invite_link = f"/admin/password/claim?token={token}"
            session.flush()
        except (PlatformCoreError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse(
            {
                "user": _serialize_user(target_user),
                "membership": membership_summary,
                "invite_link": invite_link,
            }
        )

    @app.post("/admin/users/{user_id}/invite")
    async def admin_issue_invite(
        request: Request,
        user_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        service = UserSecurityService(session)
        user = _assert_user_visible_to_actor(session, actor_email, user_id)
        token = service.issue_invite(user)
        return JSONResponse({"user": _serialize_user(user), "invite_link": f"/admin/password/claim?token={token}"})

    @app.post("/admin/users/{user_id}/reset-password")
    async def admin_issue_password_reset(
        request: Request,
        user_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        service = UserSecurityService(session)
        try:
            user = _assert_user_visible_to_actor(session, actor_email, user_id)
            token = service.issue_password_reset(user)
        except PlatformCoreError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"user": _serialize_user(user), "reset_link": f"/admin/password/claim?token={token}"})

    @app.post("/admin/users/{user_id}/status")
    async def admin_set_user_status(
        request: Request,
        user_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        if not _actor_can_manage_accounts(session, actor_email):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin access is required.")
        payload = await request.json()
        next_status = str(payload.get("status") or "").strip()
        service = UserSecurityService(session)
        try:
            user = _assert_user_visible_to_actor(session, actor_email, user_id)
            service.set_user_status(user, next_status)
        except PlatformCoreError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"user": _serialize_user(user)})

    @app.get("/admin/{account_slug}/brief", response_class=HTMLResponse)
    def admin_brief(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/brief")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "owner_briefs", "Execution brief")
        brief = _account_brief_digest(runtime, session)
        return templates.TemplateResponse(
            request,
            "admin/brief.html",
            {
                **_admin_context(request, session, runtime, page="brief"),
                "brief": brief,
                "human_sync_error": _human_sync_error,
                "can_assign_defaults": _is_manager_role(runtime.role_code),
            },
        )

    @app.get("/admin/{account_slug}/brief.json")
    def admin_brief_json(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> dict[str, object]:
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "owner_briefs", "Execution brief")
        brief = _account_brief_digest(runtime, session)
        return {
            "generated_at": brief["generated_at"],
            "account": _serialize_account(runtime.account),
            "defaults": {
                "owner": _serialize_user(brief["defaults"]["owner"]) if brief["defaults"]["owner"] is not None else None,
                "operator": _serialize_user(brief["defaults"]["operator"]) if brief["defaults"]["operator"] is not None else None,
            },
            "critical_alerts": [_serialize_alert(item) for item in brief["critical_alerts"]],
            "overdue_tasks": [_serialize_task(item) for item in brief["overdue_tasks"]],
            "failed_sync_jobs": [_serialize_sync_job(item) for item in brief["failed_sync_jobs"]],
            "goals_at_risk": [
                {
                    "goal": _serialize_goal(item["goal"], summary=item["summary"]),
                    "blockers": [
                        {
                            "metric": blocker["metric"],
                            "alert_codes": blocker["alert_codes"],
                            "related_alerts": [_serialize_alert(alert) for alert in blocker["related_alerts"]],
                            "related_tasks": [_serialize_task(task) for task in blocker["related_tasks"]],
                            "related_problems": blocker["related_problems"],
                            "attention_actions": blocker["attention_actions"],
                            "links": blocker["links"],
                        }
                        for blocker in item["blockers"]
                    ],
                }
                for item in brief["goals_at_risk"]
            ],
            "must_do_now": brief["must_do_now"],
        }

    @app.get("/admin/{account_slug}/delivery", response_class=HTMLResponse)
    def admin_delivery(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/delivery")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "owner_briefs", "Delivery pack")
        pack = _account_delivery_pack(runtime, session)
        return templates.TemplateResponse(
            request,
            "admin/delivery.html",
            {
                **_admin_context(request, session, runtime, page="delivery"),
                "delivery_pack": pack,
                "human_sync_error": _human_sync_error,
            },
        )

    @app.get("/admin/{account_slug}/delivery.json")
    def admin_delivery_json(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> dict[str, object]:
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "owner_briefs", "Delivery pack")
        pack = _account_delivery_pack(runtime, session)
        return {
            "generated_at": pack["generated_at"],
            "account": _serialize_account(runtime.account),
            "handoff_summary": pack["handoff_summary"],
            "configured_now": pack["configured_now"],
            "setup_gaps": pack["setup_gaps"],
            "health_problems": pack["health_problems"],
            "owner_actions": pack["owner_actions"],
            "operator_checklist": pack["operator_checklist"],
            "integrations_needing_attention": [
                {
                    "integration": {
                        "id": item["integration"].id,
                        "external_ref": item["integration"].external_ref,
                        "display_name": item["integration"].display_name,
                        "provider_name": item["integration"].provider_name,
                        "status": item["integration"].status,
                    },
                    "sync_state": item["sync_state"],
                    "last_error": item["last_error"],
                    "latest_success": _serialize_sync_job(item["latest_success"]) if item["latest_success"] is not None else None,
                    "latest_failure": _serialize_sync_job(item["latest_failure"]) if item["latest_failure"] is not None else None,
                }
                for item in pack["integrations_needing_attention"]
            ],
        }

    @app.get("/admin/{account_slug}/delivery.md", response_class=PlainTextResponse)
    def admin_delivery_markdown(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> PlainTextResponse:
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "owner_briefs", "Delivery pack")
        pack = _account_delivery_pack(runtime, session)
        return PlainTextResponse(_account_delivery_markdown(pack), media_type="text/markdown; charset=utf-8")

    @app.get("/admin/{account_slug}/delivery.txt", response_class=PlainTextResponse)
    def admin_delivery_text(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> PlainTextResponse:
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "owner_briefs", "Delivery pack")
        pack = _account_delivery_pack(runtime, session)
        return PlainTextResponse(_account_delivery_text(pack), media_type="text/plain; charset=utf-8")

    @app.post("/admin/{account_slug}/delivery/generate")
    async def admin_generate_delivery(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor = _require_admin_user(request, session)
        payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
        export_obsidian = bool(payload.get("export_obsidian"))
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "owner_briefs", "Delivery pack")
        pack = _account_delivery_pack(runtime, session)
        paths = write_delivery_bundle(
            scope="accounts",
            name=runtime.account.slug,
            json_payload={
                "generated_at": pack["generated_at"],
                "account": _serialize_account(runtime.account),
                "handoff_summary": pack["handoff_summary"],
                "configured_now": pack["configured_now"],
                "setup_gaps": pack["setup_gaps"],
                "health_problems": pack["health_problems"],
                "owner_actions": pack["owner_actions"],
                "operator_checklist": pack["operator_checklist"],
                "integrations_needing_attention": [
                    {
                        "integration_id": item["integration"].id,
                        "external_ref": item["integration"].external_ref,
                        "display_name": item["integration"].display_name,
                        "provider_name": item["integration"].provider_name,
                        "status": item["integration"].status,
                        "sync_state": item["sync_state"],
                        "last_error": item["last_error"],
                    }
                    for item in pack["integrations_needing_attention"]
                ],
            },
            markdown_text=_account_delivery_markdown(pack),
            text_text=_account_delivery_text(pack),
        )
        status_payload = {
            "status": "ok",
            "scope": "account",
            "account_slug": runtime.account.slug,
            "actor_email": actor.email,
            "paths": paths,
            "handoff_summary": pack["handoff_summary"],
        }
        write_runtime_status("delivery_status", status_payload)
        obsidian_paths = None
        if export_obsidian:
            obsidian_paths = _write_account_obsidian_export(
                actor_email=actor.email,
                account_slug=runtime.account.slug,
                account_name=runtime.account.name,
                generated_at=pack["generated_at"],
                markdown_text=_account_delivery_markdown(pack),
            )
        return JSONResponse({"paths": paths, "status": status_payload, "obsidian": obsidian_paths})

    @app.post("/internal/reports/accounts/{account_slug}/delivery")
    async def internal_generate_account_delivery(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        _require_internal_api_token(request)
        payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
        actor_email = str(payload.get("actor_email") or "").strip().lower()
        export_obsidian = bool(payload.get("export_obsidian"))
        if not actor_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="actor_email is required.")
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "owner_briefs", "Delivery pack")
        pack = _account_delivery_pack(runtime, session)
        paths = write_delivery_bundle(
            scope="accounts",
            name=runtime.account.slug,
            json_payload={
                "generated_at": pack["generated_at"],
                "account": _serialize_account(runtime.account),
                "handoff_summary": pack["handoff_summary"],
                "configured_now": pack["configured_now"],
                "setup_gaps": pack["setup_gaps"],
                "health_problems": pack["health_problems"],
                "owner_actions": pack["owner_actions"],
                "operator_checklist": pack["operator_checklist"],
            },
            markdown_text=_account_delivery_markdown(pack),
            text_text=_account_delivery_text(pack),
        )
        status_payload = {
            "status": "ok",
            "scope": "account",
            "account_slug": runtime.account.slug,
            "actor_email": actor_email,
            "paths": paths,
            "handoff_summary": pack["handoff_summary"],
        }
        write_runtime_status("delivery_status", status_payload)
        obsidian_paths = None
        if export_obsidian:
            obsidian_paths = _write_account_obsidian_export(
                actor_email=actor_email,
                account_slug=runtime.account.slug,
                account_name=runtime.account.name,
                generated_at=pack["generated_at"],
                markdown_text=_account_delivery_markdown(pack),
            )
        return JSONResponse({"paths": paths, "status": status_payload, "obsidian": obsidian_paths})

    @app.post("/internal/reports/portfolio/brief")
    async def internal_generate_portfolio_brief(
        request: Request,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        _require_internal_api_token(request)
        payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
        actor_email = str(payload.get("actor_email") or "").strip().lower()
        export_obsidian = bool(payload.get("export_obsidian"))
        if not actor_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="actor_email is required.")
        _require_portfolio_owner(session, actor_email)
        owner_rows = _owner_accounts_for_portfolio(session, actor_email)
        portfolio_rows = [_portfolio_account_row(session, actor_email, item["account"]) for item in owner_rows]
        portfolio_rows.sort(key=lambda item: (-int(item["risk_score"]), item["account"].name.lower()))
        portfolio = _portfolio_summary(portfolio_rows)
        brief = _portfolio_brief(portfolio)
        paths = write_delivery_bundle(
            scope="portfolio",
            name="portfolio_brief",
            json_payload=brief,
            markdown_text=_portfolio_brief_markdown(brief),
            text_text=_portfolio_brief_text(brief),
        )
        status_payload = {
            "status": "ok",
            "scope": "portfolio",
            "actor_email": actor_email,
            "accounts_count": brief["headline"]["accounts_count"],
            "paths": paths,
        }
        write_runtime_status("delivery_status", status_payload)
        obsidian_paths = None
        if export_obsidian:
            obsidian_paths = _write_portfolio_obsidian_export(
                actor_email=actor_email,
                generated_at=brief["generated_at"],
                markdown_text=_portfolio_brief_markdown(brief),
            )
        return JSONResponse({"brief": brief, "paths": paths, "status": status_payload, "obsidian": obsidian_paths})

    @app.get("/admin/{account_slug}/dashboard", response_class=HTMLResponse)
    def admin_dashboard(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/dashboard")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "dashboard.read")
        dashboard = ExecutiveDashboardService(session).get_dashboard(runtime.context, "today")
        widgets = {item["widget_key"]: item["payload"] for item in dashboard["widgets"]}
        goals = GoalService(session).get_dashboard_goal_snapshot(runtime.context)
        account_settings = _account_product_config(runtime.account)
        automation = RuntimeAutomationService(session)
        alerts = [
            item for item in RuntimeAutomationService(session).list_alerts(runtime.context)
            if item.status == "open"
        ][:8]
        open_alerts = [item for item in automation.list_alerts(runtime.context) if item.status == "open"]
        tasks = automation.list_tasks(runtime.context)
        overdue_tasks = [item for item in tasks if item.status == "open" and item.due_at is not None and item.due_at <= datetime.now(timezone.utc)][:8]
        goal_snapshots = [
            _enrich_goal_snapshot(
                account_slug=account_slug,
                actor_email=actor_email,
                snapshot=item,
                open_alerts=open_alerts,
                open_tasks=[task for task in tasks if task.status == "open"],
                top_problems=widgets.get("owner_panel", {}).get("top_problems", []),
                attention_zones=widgets.get("owner_panel", {}).get("attention_zones", []),
            )
            for item in goals
        ]
        readiness = _account_product_readiness(runtime, session)
        return templates.TemplateResponse(
            request,
            "admin/dashboard.html",
            {
                **_admin_context(request, session, runtime, page="dashboard"),
                "dashboard": dashboard,
                "widgets": widgets,
                "goal_snapshots": goal_snapshots,
                "critical_alerts": alerts,
                "overdue_tasks": overdue_tasks,
                "readiness": readiness,
                "show_owner_brief": bool(account_settings.get("show_owner_brief", True)),
            },
        )

    @app.get("/admin/{account_slug}/integrations", response_class=HTMLResponse)
    def admin_integrations(
        request: Request,
        account_slug: str,
        integration_id: int | None = Query(default=None),
        provider: str | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/integrations")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "integrations.manage")
        _ensure_account_feature(runtime, "integrations_setup", "Integrations setup")
        service = RuntimeIntegrationService(session)
        integrations = service.list_integrations(runtime.context)
        sync_status = {
            item["integration"].id: item
            for item in AdminQueryService(session).integration_sync_status(runtime.account.id)
        }
        selected_setup = None
        selected_provider_key = provider or "ads:avito"
        if integration_id is not None:
            try:
                selected_setup = service.integration_setup_payload(runtime.context, integration_id=integration_id)
            except TenantContextError as exc:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
            selected_provider_key = f"{selected_setup['integration'].provider_kind}:{selected_setup['integration'].provider_name}"
        provider_catalog = _provider_catalog()
        return templates.TemplateResponse(
            request,
            "admin/integrations.html",
            {
                **_admin_context(request, session, runtime, page="integrations"),
                "integrations": integrations,
                "sync_status": sync_status,
                "human_sync_error": _human_sync_error,
                "provider_catalog": provider_catalog,
                "selected_provider_key": selected_provider_key,
                "selected_setup": selected_setup,
                "selected_provider_spec": next((item for item in provider_catalog if item["key"] == selected_provider_key), provider_catalog[0]),
            },
        )

    @app.get("/admin/{account_slug}/members", response_class=HTMLResponse)
    def admin_members(
        request: Request,
        account_slug: str,
        membership_id: int | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/members")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        _require_account_manager(runtime)
        membership_service = MembershipService(session)
        memberships = membership_service.list_memberships(runtime.account)
        selected_membership = None
        if membership_id is not None:
            try:
                selected_membership = membership_service.get_membership(runtime.account, membership_id)
            except PlatformCoreError as exc:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        roles = session.execute(select(Role).order_by(Role.id.asc())).scalars().all()
        return templates.TemplateResponse(
            request,
            "admin/members.html",
            {
                **_admin_context(request, session, runtime, page="members"),
                "memberships": memberships,
                "selected_membership": selected_membership,
                "roles": roles,
                "onboarding": _account_onboarding_status(runtime.account, session),
            },
        )

    @app.get("/admin/{account_slug}/settings", response_class=HTMLResponse)
    def admin_account_settings(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/settings")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        _require_account_manager(runtime)
        active_memberships = [item for item in _membership_rows(runtime.account, session) if item.status == "active"]
        account_settings = _account_product_config(runtime.account)
        feature_rows = _account_feature_rows(runtime.account)
        plan_profiles = _plan_profiles()
        return templates.TemplateResponse(
            request,
            "admin/settings.html",
            {
                **_admin_context(request, session, runtime, page="settings"),
                "account_settings": account_settings,
                "feature_flags": _account_feature_flags(runtime.account),
                "feature_rows": feature_rows,
                "feature_definitions": _feature_flag_definitions(),
                "soft_limit_rows": _account_usage_rows(runtime.account, session),
                "soft_limit_definitions": _soft_limit_definitions(),
                "readiness": _account_product_readiness(runtime, session),
                "active_memberships": active_memberships,
                "account_status_options": _account_status_options(),
                "account_plan_options": _account_plan_options(),
                "current_plan_profile": plan_profiles.get(runtime.account.plan_type, plan_profiles["internal"]),
            },
        )

    @app.post("/admin/{account_slug}/settings/save")
    async def admin_save_account_settings(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> RedirectResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        _require_account_manager(runtime)
        payload = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)

        def _value(name: str, default: str = "") -> str:
            return str((payload.get(name) or [default])[0]).strip()

        feature_flags_selected = {str(item).strip() for item in payload.get("feature_flags", []) if str(item).strip()}
        feature_flags = {
            item["key"]: item["key"] in feature_flags_selected
            for item in _feature_flag_definitions()
        }
        soft_limits: dict[str, int | None] = {}
        for item in _soft_limit_definitions():
            raw = _value(f"soft_limit_{item['key']}")
            soft_limits[item["key"]] = None if raw == "" else max(0, int(raw))
        updated_settings = {
            "branding_title": _value("branding_title"),
            "branding_subtitle": _value("branding_subtitle"),
            "default_dashboard_period": _value("default_dashboard_period", "today") or "today",
            "show_owner_brief": bool(payload.get("show_owner_brief")),
            "show_portfolio_on_login": bool(payload.get("show_portfolio_on_login")),
            "default_owner_user_id": int(_value("default_owner_user_id")) if _value("default_owner_user_id") else None,
            "default_operator_user_id": int(_value("default_operator_user_id")) if _value("default_operator_user_id") else None,
        }
        try:
            AccountService(session).update_account(
                runtime.account,
                name=_value("account_name") or runtime.account.name,
                default_timezone=_value("default_timezone") or runtime.account.default_timezone,
                default_currency=_value("default_currency") or runtime.account.default_currency,
                status=_value("account_status") or runtime.account.status,
                plan_type=_value("plan_type") or runtime.account.plan_type,
                settings_json=updated_settings,
                feature_flags_json=feature_flags,
                soft_limits_json=soft_limits,
            )
            _push_flash(request, "success", "Account settings updated.")
        except (PlatformCoreError, IntegrityError, ValueError) as exc:
            _push_flash(request, "error", f"Account settings update failed: {exc}")
        return RedirectResponse(url=f"/admin/{runtime.account.slug}/settings", status_code=status.HTTP_302_FOUND)

    @app.get("/admin/{account_slug}/knowledge", response_class=HTMLResponse)
    def admin_knowledge(
        request: Request,
        account_slug: str,
        q: str | None = Query(default=None),
        item_type: str | None = Query(default=None),
        status_filter: str = Query(default="active"),
        item_id: int | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/knowledge")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "business.read")
        _ensure_account_feature(runtime, "knowledge_base", "Knowledge base")
        knowledge_service = KnowledgeService(session)
        items = knowledge_service.list_items(
            runtime.context,
            q=q,
            item_type=item_type or None,
            status=status_filter or None,
        )
        selected_item = None
        if item_id is not None:
            try:
                selected_item = knowledge_service.get_item(runtime.context, item_id)
            except TenantContextError as exc:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        elif items:
            selected_item = items[0]
        customer_ids = {item.customer_id for item in items if item.customer_id is not None}
        deal_ids = {item.deal_id for item in items if item.deal_id is not None}
        if selected_item is not None:
            if selected_item.customer_id is not None:
                customer_ids.add(selected_item.customer_id)
            if selected_item.deal_id is not None:
                deal_ids.add(selected_item.deal_id)
        customer_lookup = {
            item.id: item
            for item in session.execute(
                select(Customer).where(Customer.account_id == runtime.account.id, Customer.id.in_(customer_ids))
            ).scalars().all()
        } if customer_ids else {}
        deal_lookup = {
            item.id: item
            for item in session.execute(
                select(Deal).where(Deal.account_id == runtime.account.id, Deal.id.in_(deal_ids))
            ).scalars().all()
        } if deal_ids else {}
        recent_customers = session.execute(
            select(Customer)
            .where(Customer.account_id == runtime.account.id)
            .order_by(Customer.updated_at.desc(), Customer.id.desc())
            .limit(50)
        ).scalars().all()
        recent_deals = session.execute(
            select(Deal)
            .where(Deal.account_id == runtime.account.id)
            .order_by(Deal.updated_at.desc(), Deal.id.desc())
            .limit(50)
        ).scalars().all()
        return templates.TemplateResponse(
            request,
            "admin/knowledge.html",
            {
                **_admin_context(request, session, runtime, page="knowledge"),
                "knowledge_items": items,
                "selected_item": selected_item,
                "customer_lookup": customer_lookup,
                "deal_lookup": deal_lookup,
                "recent_customers": recent_customers,
                "recent_deals": recent_deals,
                "knowledge_query": q or "",
                "knowledge_item_type": item_type or "",
                "knowledge_status_filter": status_filter or "active",
                "knowledge_item_type_options": _knowledge_item_type_options(),
                "knowledge_status_options": _knowledge_status_options(),
            },
        )

    @app.post("/admin/{account_slug}/knowledge/save")
    async def admin_save_knowledge(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> RedirectResponse:
        await _require_csrf(request)
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "documents.manage")
        _ensure_account_feature(runtime, "knowledge_base", "Knowledge base")
        form = await request.form()
        title = str(form.get("title") or "").strip()
        summary = str(form.get("summary") or "").strip() or None
        body_text = str(form.get("body_text") or "").strip() or None
        item_type = str(form.get("item_type") or "note").strip() or "note"
        tags = _knowledge_tags(str(form.get("tags") or ""))
        customer_id_raw = str(form.get("customer_id") or "").strip()
        deal_id_raw = str(form.get("deal_id") or "").strip()
        upload = form.get("upload")
        customer_id = int(customer_id_raw) if customer_id_raw else None
        deal_id = int(deal_id_raw) if deal_id_raw else None
        if item_type not in set(_knowledge_item_type_options()):
            _push_flash(request, "error", "Unsupported knowledge item type.")
            return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge", status_code=status.HTTP_302_FOUND)
        if customer_id is not None:
            customer = session.execute(
                select(Customer).where(Customer.account_id == runtime.account.id, Customer.id == customer_id)
            ).scalar_one_or_none()
            if customer is None:
                _push_flash(request, "error", "Linked customer not found in selected account.")
                return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge", status_code=status.HTTP_302_FOUND)
        if deal_id is not None:
            deal = session.execute(
                select(Deal).where(Deal.account_id == runtime.account.id, Deal.id == deal_id)
            ).scalar_one_or_none()
            if deal is None:
                _push_flash(request, "error", "Linked deal not found in selected account.")
                return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge", status_code=status.HTTP_302_FOUND)

        file_name = None
        file_path = None
        mime_type = None
        content_size_bytes = None
        content_sha256 = None
        source_kind = "manual"
        metadata: dict[str, object] = {}
        if isinstance(upload, UploadFile) and upload.filename:
            content = await upload.read()
            if not content:
                _push_flash(request, "error", "Uploaded file is empty.")
                return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge", status_code=status.HTTP_302_FOUND)
            if len(content) > 10 * 1024 * 1024:
                _push_flash(request, "error", "Uploaded file is too large. Limit is 10 MB.")
                return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge", status_code=status.HTTP_302_FOUND)
            content_sha256 = hashlib.sha256(content).hexdigest()
            absolute_path, relative_path = _knowledge_storage_path(runtime.account.slug, upload, content_sha256)
            absolute_path.parent.mkdir(parents=True, exist_ok=True)
            absolute_path.write_bytes(content)
            file_name = upload.filename
            file_path = relative_path
            mime_type = upload.content_type or mimetypes.guess_type(upload.filename)[0] or "application/octet-stream"
            content_size_bytes = len(content)
            source_kind = "upload"
            metadata = {"uploaded_at": datetime.now(timezone.utc).isoformat()}
            if not title:
                title = Path(upload.filename).stem or upload.filename
            if item_type == "note":
                item_type = "file"

        if not title:
            _push_flash(request, "error", "Knowledge item title is required.")
            return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge", status_code=status.HTTP_302_FOUND)
        if not body_text and not summary and file_path is None:
            _push_flash(request, "error", "Provide note text, summary or upload a file.")
            return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge", status_code=status.HTTP_302_FOUND)

        try:
            item = KnowledgeService(session).create_item(
                runtime.context,
                title=title,
                summary=summary,
                body_text=body_text,
                item_type=item_type,
                source_kind=source_kind,
                customer_id=customer_id,
                deal_id=deal_id,
                file_name=file_name,
                file_path=file_path,
                mime_type=mime_type,
                content_size_bytes=content_size_bytes,
                content_sha256=content_sha256,
                tags=tags,
                metadata=metadata,
            )
            AuditLogService(session).log(
                runtime.context,
                "knowledge.item.created",
                "knowledge_item",
                str(item.id),
                details={"item_type": item.item_type, "source_kind": item.source_kind, "file_name": item.file_name},
            )
            _push_flash(request, "success", f"Knowledge item #{item.id} saved.")
            return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge?item_id={item.id}", status_code=status.HTTP_302_FOUND)
        except PlatformCoreError as exc:
            _push_flash(request, "error", f"Knowledge item save failed: {exc}")
            return RedirectResponse(url=f"/admin/{runtime.account.slug}/knowledge", status_code=status.HTTP_302_FOUND)

    @app.post("/admin/{account_slug}/knowledge/{item_id}/status")
    async def admin_knowledge_status(
        request: Request,
        account_slug: str,
        item_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        ensure_permission(runtime, "documents.manage")
        _ensure_account_feature(runtime, "knowledge_base", "Knowledge base")
        payload = await request.json()
        next_status = str(payload.get("status") or "").strip()
        item = KnowledgeService(session).update_status(runtime.context, item_id, status=next_status)
        AuditLogService(session).log(
            runtime.context,
            "knowledge.item.status",
            "knowledge_item",
            str(item.id),
            details={"status": item.status},
        )
        return JSONResponse({"item": _serialize_knowledge_item(item)})

    @app.get("/admin/{account_slug}/knowledge/{item_id}/download")
    def admin_download_knowledge_file(
        request: Request,
        account_slug: str,
        item_id: int,
        session: Session = Depends(get_db_session),
    ) -> FileResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/knowledge")
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=user.email)
        ensure_permission(runtime, "business.read")
        _ensure_account_feature(runtime, "knowledge_base", "Knowledge base")
        item = KnowledgeService(session).get_item(runtime.context, item_id)
        if not item.file_path:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge file is not available.")
        absolute_path = (knowledge_upload_root / item.file_path).resolve()
        if not absolute_path.is_file() or knowledge_upload_root not in absolute_path.parents:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge file is missing.")
        return FileResponse(path=str(absolute_path), media_type=item.mime_type or "application/octet-stream", filename=item.file_name or absolute_path.name)

    @app.get("/admin/{account_slug}/people", response_class=HTMLResponse)
    def admin_people(
        request: Request,
        account_slug: str,
        employee_id: int | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/people")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "business.read")
        _ensure_account_feature(runtime, "people_execution", "People execution")
        people_service = PeopleService(session)
        employees = people_service.list_employees(runtime.context)
        employee_snapshots = _employee_snapshot_map(runtime, session)
        selected_employee = None
        if employee_id is not None:
            try:
                selected_employee = people_service.get_employee(runtime.context, employee_id)
            except TenantContextError as exc:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        elif employees:
            selected_employee = employees[0]
        membership_rows = [item for item in _membership_rows(runtime.account, session) if item.status == "active" and item.user is not None]
        selectable_users = [item.user for item in membership_rows]
        selected_tasks = []
        if selected_employee is not None:
            selected_tasks = [
                item for item in RuntimeAutomationService(session).list_tasks(runtime.context)
                if item.assignee_employee_id == selected_employee.id or (selected_employee.user_id is not None and item.assignee_user_id == selected_employee.user_id)
            ][:12]
        return templates.TemplateResponse(
            request,
            "admin/people.html",
            {
                **_admin_context(request, session, runtime, page="people"),
                "employees": employees,
                "employee_snapshots": employee_snapshots,
                "selected_employee": selected_employee,
                "selected_tasks": selected_tasks,
                "selectable_users": selectable_users,
                "employee_status_options": _employee_status_options(),
                "default_users": _default_account_users(runtime, session),
                "can_manage_people": _is_manager_role(runtime.role_code),
                "can_create_tasks": "*" in runtime.permissions or "tasks.manage" in runtime.permissions,
            },
        )

    @app.post("/admin/{account_slug}/people/employee/save")
    async def admin_save_employee(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        _require_account_manager(runtime)
        ensure_permission(runtime, "business.write")
        _ensure_account_feature(runtime, "people_execution", "People execution")
        body = payload.get("employee") or {}
        try:
            employee = PeopleService(session).upsert_employee(
                runtime.context,
                employee_id=int(payload["employee_id"]) if payload.get("employee_id") else None,
                user_id=int(body["user_id"]) if body.get("user_id") else None,
                employee_code=str(body.get("employee_code") or "").strip() or None,
                full_name=str(body.get("full_name") or "").strip(),
                role_title=str(body.get("role_title") or "").strip() or None,
                department=str(body.get("department") or "").strip() or None,
                email=str(body.get("email") or "").strip() or None,
                phone=str(body.get("phone") or "").strip() or None,
                status=str(body.get("status") or "active").strip(),
            )
            AuditLogService(session).log(
                runtime.context,
                "people.employee.saved",
                "employee",
                str(employee.id),
                details={"status": employee.status, "user_id": employee.user_id},
            )
        except (PlatformCoreError, TenantContextError, ValueError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"employee": _serialize_employee(employee)})

    @app.post("/admin/{account_slug}/people/tasks/create")
    async def admin_create_people_task(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "tasks.manage")
        _ensure_account_feature(runtime, "people_execution", "People execution")
        body = payload.get("task") or {}
        title = str(body.get("title") or "").strip()
        if not title:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Task title is required.")
        employee_id = int(body["assignee_employee_id"]) if body.get("assignee_employee_id") else None
        assignee_user_id = int(body["assignee_user_id"]) if body.get("assignee_user_id") else None
        if employee_id is None and assignee_user_id is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Assignee is required.")
        if employee_id is not None:
            employee = PeopleService(session).get_employee(runtime.context, employee_id)
            if assignee_user_id is None and employee.user_id is not None:
                assignee_user_id = employee.user_id
        due_at_raw = str(body.get("due_at") or "").strip()
        due_at = datetime.fromisoformat(due_at_raw) if due_at_raw else None
        if due_at is not None and due_at.tzinfo is None:
            due_at = due_at.replace(tzinfo=timezone.utc)
        task = Task(
            account_id=runtime.account.id,
            assignee_user_id=assignee_user_id,
            assignee_employee_id=employee_id,
            created_by_user_id=runtime.actor_user.id,
            source="manual",
            title=title,
            description=str(body.get("description") or "").strip() or None,
            status="open",
            priority=str(body.get("priority") or "normal").strip() or "normal",
            due_at=due_at,
            related_entity_type="employee",
            related_entity_id=str(employee_id) if employee_id is not None else None,
        )
        session.add(task)
        session.flush()
        session.add(
            TaskEvent(
                account_id=runtime.account.id,
                task_id=task.id,
                actor_user_id=runtime.actor_user.id,
                event_type="task.created_from_people_ui",
                event_at=datetime.now(timezone.utc),
                payload_json={"assignee_employee_id": employee_id, "assignee_user_id": assignee_user_id},
            )
        )
        AuditLogService(session).log(
            runtime.context,
            "people.task.created",
            "task",
            str(task.id),
            details={"assignee_employee_id": employee_id, "assignee_user_id": assignee_user_id},
        )
        session.flush()
        return JSONResponse({"task": _serialize_task(task)})

    @app.get("/admin/{account_slug}/operations", response_class=HTMLResponse)
    def admin_operations(
        request: Request,
        account_slug: str,
        threshold_days: int = Query(default=30, ge=0),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/operations")
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=user.email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "business.read")
        _ensure_account_feature(runtime, "operations_workflows", "Operations")
        operations_service = OperationsService(session)
        products = operations_service.list_products(runtime.context)
        warehouses = operations_service.list_warehouses(runtime.context)
        purchases = operations_service.list_purchases(runtime.context)
        documents = operations_service.list_documents(runtime.context)
        installation_requests = operations_service.list_installation_requests(runtime.context)
        stagnant_threshold = threshold_days
        stagnant_stock = operations_service.stagnant_stock(runtime.context, threshold_days=stagnant_threshold)
        recent_customers = session.execute(
            select(Customer)
            .where(Customer.account_id == runtime.account.id)
            .order_by(Customer.updated_at.desc(), Customer.id.desc())
            .limit(50)
        ).scalars().all()
        recent_deals = session.execute(
            select(Deal)
            .where(Deal.account_id == runtime.account.id)
            .order_by(Deal.updated_at.desc(), Deal.id.desc())
            .limit(50)
        ).scalars().all()
        active_employees = session.execute(
            select(Employee)
            .where(Employee.account_id == runtime.account.id, Employee.status == "active")
            .order_by(Employee.full_name.asc(), Employee.id.asc())
        ).scalars().all()
        customer_ids = {
            item
            for item in (
                [purchase.supplier_customer_id for purchase in purchases]
                + [document.customer_id for document in documents]
                + [installation.customer_id for installation in installation_requests]
            )
            if item is not None
        }
        deal_ids = {
            item
            for item in (
                [document.deal_id for document in documents]
                + [installation.deal_id for installation in installation_requests]
            )
            if item is not None
        }
        employee_ids = {item.assigned_employee_id for item in installation_requests if item.assigned_employee_id is not None}
        customer_lookup = {
            item.id: item
            for item in session.execute(
                select(Customer).where(Customer.account_id == runtime.account.id, Customer.id.in_(customer_ids))
            ).scalars().all()
        } if customer_ids else {}
        deal_lookup = {
            item.id: item
            for item in session.execute(
                select(Deal).where(Deal.account_id == runtime.account.id, Deal.id.in_(deal_ids))
            ).scalars().all()
        } if deal_ids else {}
        employee_lookup = {
            item.id: item
            for item in session.execute(
                select(Employee).where(Employee.account_id == runtime.account.id, Employee.id.in_(employee_ids))
            ).scalars().all()
        } if employee_ids else {}
        return templates.TemplateResponse(
            request,
            "admin/operations.html",
            {
                **_admin_context(request, session, runtime, page="operations"),
                "products": products,
                "warehouses": warehouses,
                "purchases": purchases,
                "documents": documents,
                "installation_requests": installation_requests,
                "stagnant_stock": stagnant_stock,
                "stagnant_threshold_days": stagnant_threshold,
                "recent_customers": recent_customers,
                "recent_deals": recent_deals,
                "active_employees": active_employees,
                "customer_lookup": customer_lookup,
                "deal_lookup": deal_lookup,
                "employee_lookup": employee_lookup,
                "document_type_options": _document_type_options(),
                "document_status_options": _document_status_options(),
                "can_manage_operations": _is_manager_role(runtime.role_code) or "*" in runtime.permissions or "business.write" in runtime.permissions,
            },
        )

    @app.post("/admin/{account_slug}/operations/warehouse/save")
    async def admin_save_warehouse(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        _require_account_manager(runtime)
        ensure_permission(runtime, "business.write")
        _ensure_account_feature(runtime, "operations_workflows", "Operations")
        body = payload.get("warehouse") or {}
        try:
            warehouse = OperationsService(session).create_warehouse(
                runtime.context,
                code=str(body.get("code") or "").strip(),
                name=str(body.get("name") or "").strip(),
                location=str(body.get("location") or "").strip() or None,
            )
            AuditLogService(session).log(
                runtime.context,
                "operations.warehouse.created",
                "warehouse",
                str(warehouse.id),
                details={"code": warehouse.code, "name": warehouse.name},
            )
        except (PlatformCoreError, TenantContextError, ValueError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"warehouse": _serialize_warehouse(warehouse)})

    @app.post("/admin/{account_slug}/operations/product/save")
    async def admin_save_product(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        _require_account_manager(runtime)
        ensure_permission(runtime, "business.write")
        _ensure_account_feature(runtime, "operations_workflows", "Operations")
        body = payload.get("product") or {}
        try:
            product = OperationsService(session).create_product(
                runtime.context,
                sku=str(body.get("sku") or "").strip() or None,
                name=str(body.get("name") or "").strip(),
                unit=str(body.get("unit") or "").strip() or "pcs",
                list_price=Decimal(str(body.get("list_price") or "0")),
                cost_price=Decimal(str(body.get("cost_price") or "0")),
                min_stock_level=Decimal(str(body.get("min_stock_level") or "0")),
            )
            AuditLogService(session).log(
                runtime.context,
                "operations.product.created",
                "product",
                str(product.id),
                details={"sku": product.sku, "name": product.name},
            )
        except (PlatformCoreError, TenantContextError, ValueError, IntegrityError, ArithmeticError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"product": _serialize_product(product)})

    @app.post("/admin/{account_slug}/operations/purchases/save")
    async def admin_save_purchase(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "business.write")
        _ensure_account_feature(runtime, "operations_workflows", "Operations")
        body = payload.get("purchase") or {}
        try:
            purchase = OperationsService(session).create_purchase_request(
                runtime.context,
                supplier_customer_id=int(body["supplier_customer_id"]) if body.get("supplier_customer_id") else None,
                warehouse_id=int(body["warehouse_id"]) if body.get("warehouse_id") else None,
                product_id=int(body["product_id"]),
                quantity=Decimal(str(body.get("quantity") or "0")),
                unit_cost=Decimal(str(body.get("unit_cost") or "0")),
                notes=str(body.get("notes") or "").strip() or None,
            )
            AuditLogService(session).log(
                runtime.context,
                "operations.purchase.created",
                "purchase",
                str(purchase.id),
                details={"purchase_number": purchase.purchase_number, "status": purchase.status},
            )
        except (PlatformCoreError, TenantContextError, ValueError, IntegrityError, ArithmeticError, KeyError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"purchase": _serialize_purchase(purchase)})

    @app.post("/admin/{account_slug}/operations/purchases/{purchase_id}/receive")
    async def admin_receive_purchase(
        request: Request,
        account_slug: str,
        purchase_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "business.write")
        _ensure_account_feature(runtime, "operations_workflows", "Operations")
        try:
            purchase = OperationsService(session).receive_purchase(runtime.context, purchase_id)
            AuditLogService(session).log(
                runtime.context,
                "operations.purchase.received",
                "purchase",
                str(purchase.id),
                details={"purchase_number": purchase.purchase_number, "status": purchase.status},
            )
        except (PlatformCoreError, TenantContextError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"purchase": _serialize_purchase(purchase)})

    @app.post("/admin/{account_slug}/operations/documents/save")
    async def admin_save_document(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "documents.manage")
        _ensure_account_feature(runtime, "operations_workflows", "Operations")
        body = payload.get("document") or {}
        issued_at_raw = str(body.get("issued_at") or "").strip()
        issued_at = datetime.fromisoformat(issued_at_raw) if issued_at_raw else None
        if issued_at is not None and issued_at.tzinfo is None:
            issued_at = issued_at.replace(tzinfo=timezone.utc)
        try:
            document = OperationsService(session).create_document(
                runtime.context,
                document_type=str(body.get("document_type") or "invoice").strip() or "invoice",
                document_number=str(body.get("document_number") or "").strip() or None,
                customer_id=int(body["customer_id"]) if body.get("customer_id") else None,
                deal_id=int(body["deal_id"]) if body.get("deal_id") else None,
                status=str(body.get("status") or "draft").strip() or "draft",
                issued_at=issued_at,
                total_amount=Decimal(str(body.get("total_amount") or "0")),
                summary=str(body.get("summary") or "").strip() or None,
            )
            AuditLogService(session).log(
                runtime.context,
                "operations.document.created",
                "document",
                str(document.id),
                details={"document_type": document.document_type, "status": document.status},
            )
        except (PlatformCoreError, TenantContextError, ValueError, IntegrityError, ArithmeticError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"document": _serialize_document(document)})

    @app.post("/admin/{account_slug}/operations/installations/save")
    async def admin_save_installation(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "business.write")
        _ensure_account_feature(runtime, "operations_workflows", "Operations")
        body = payload.get("installation") or {}
        scheduled_for_raw = str(body.get("scheduled_for") or "").strip()
        scheduled_for = datetime.fromisoformat(scheduled_for_raw) if scheduled_for_raw else None
        if scheduled_for is not None and scheduled_for.tzinfo is None:
            scheduled_for = scheduled_for.replace(tzinfo=timezone.utc)
        try:
            installation = OperationsService(session).create_installation_request(
                runtime.context,
                customer_id=int(body["customer_id"]) if body.get("customer_id") else None,
                deal_id=int(body["deal_id"]) if body.get("deal_id") else None,
                assigned_employee_id=int(body["assigned_employee_id"]) if body.get("assigned_employee_id") else None,
                title=str(body.get("title") or "").strip(),
                address=str(body.get("address") or "").strip() or None,
                scheduled_for=scheduled_for,
                notes=str(body.get("notes") or "").strip() or None,
            )
            created_task = None
            if installation.assigned_employee_id is not None:
                employee = PeopleService(session).get_employee(runtime.context, installation.assigned_employee_id)
                created_task = Task(
                    account_id=runtime.account.id,
                    assignee_user_id=employee.user_id,
                    assignee_employee_id=employee.id,
                    created_by_user_id=runtime.actor_user.id,
                    source="operations",
                    title=f"Installation: {installation.title}",
                    description=installation.address or str((installation.notes_json or {}).get("notes") or "") or None,
                    status="open",
                    priority="high",
                    due_at=installation.scheduled_for,
                    related_entity_type="installation_request",
                    related_entity_id=str(installation.id),
                )
                session.add(created_task)
                session.flush()
                session.add(
                    TaskEvent(
                        account_id=runtime.account.id,
                        task_id=created_task.id,
                        actor_user_id=runtime.actor_user.id,
                        event_type="task.created_from_operations_ui",
                        event_at=datetime.now(timezone.utc),
                        payload_json={"installation_request_id": installation.id, "assigned_employee_id": employee.id},
                    )
                )
            AuditLogService(session).log(
                runtime.context,
                "operations.installation.created",
                "installation_request",
                str(installation.id),
                details={"status": installation.status, "assigned_employee_id": installation.assigned_employee_id, "task_id": created_task.id if created_task else None},
            )
        except (PlatformCoreError, TenantContextError, ValueError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"installation_request": _serialize_installation_request(installation), "task": _serialize_task(created_task) if created_task else None})

    @app.get("/admin/{account_slug}/communications", response_class=HTMLResponse)
    def admin_communications(
        request: Request,
        account_slug: str,
        review_id: int | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/communications")
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=user.email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "business.read")
        _ensure_account_feature(runtime, "communication_intelligence", "Communications")
        service = CommunicationService(session)
        reviews = service.list_reviews(runtime.context)
        selected_review = None
        if review_id is not None:
            try:
                selected_review = service.get_review(runtime.context, review_id)
            except TenantContextError as exc:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        elif reviews:
            selected_review = reviews[0]
        recent_customers = session.execute(
            select(Customer)
            .where(Customer.account_id == runtime.account.id)
            .order_by(Customer.updated_at.desc(), Customer.id.desc())
            .limit(50)
        ).scalars().all()
        recent_leads = session.execute(
            select(Lead)
            .where(Lead.account_id == runtime.account.id)
            .order_by(Lead.updated_at.desc(), Lead.id.desc())
            .limit(50)
        ).scalars().all()
        active_employees = session.execute(
            select(Employee)
            .where(Employee.account_id == runtime.account.id, Employee.status == "active")
            .order_by(Employee.full_name.asc(), Employee.id.asc())
        ).scalars().all()
        linked_task = None
        if selected_review is not None:
            linked_task = session.execute(
                select(Task).where(
                    Task.account_id == runtime.account.id,
                    Task.related_entity_type == "communication_review",
                    Task.related_entity_id == str(selected_review.id),
                )
            ).scalar_one_or_none()
        return templates.TemplateResponse(
            request,
            "admin/communications.html",
            {
                **_admin_context(request, session, runtime, page="communications"),
                "reviews": reviews,
                "selected_review": selected_review,
                "recent_customers": recent_customers,
                "recent_leads": recent_leads,
                "active_employees": active_employees,
                "linked_task": linked_task,
                "communication_channel_options": _communication_channel_options(),
                "communication_direction_options": _communication_direction_options(),
                "default_users": _default_account_users(runtime, session),
                "can_manage_communications": _is_manager_role(runtime.role_code) or "*" in runtime.permissions or "business.write" in runtime.permissions,
            },
        )

    @app.post("/admin/{account_slug}/communications/reviews/save")
    async def admin_save_communication_review(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        ensure_permission(runtime, "business.write")
        _ensure_account_feature(runtime, "communication_intelligence", "Communications")
        body = payload.get("review") or {}
        try:
            review = CommunicationService(session).create_review(
                runtime.context,
                created_by_user_id=runtime.actor_user.id,
                customer_id=int(body["customer_id"]) if body.get("customer_id") else None,
                lead_id=int(body["lead_id"]) if body.get("lead_id") else None,
                employee_id=int(body["employee_id"]) if body.get("employee_id") else None,
                channel=str(body.get("channel") or "message").strip() or "message",
                direction=str(body.get("direction") or "inbound").strip() or "inbound",
                title=str(body.get("title") or "").strip(),
                transcript_text=str(body.get("transcript_text") or "").strip(),
                response_delay_minutes=int(body["response_delay_minutes"]) if body.get("response_delay_minutes") not in {None, ""} else None,
            )
            AuditLogService(session).log(
                runtime.context,
                "communications.review.created",
                "communication_review",
                str(review.id),
                details={"quality_status": review.quality_status, "sentiment": review.sentiment},
            )
        except (PlatformCoreError, TenantContextError, ValueError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"review": _serialize_communication_review(review)})

    @app.post("/admin/{account_slug}/communications/reviews/{review_id}/task")
    async def admin_create_communication_task(
        request: Request,
        account_slug: str,
        review_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor = _require_admin_user(request, session)
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor.email)
        ensure_permission(runtime, "tasks.manage")
        _ensure_account_feature(runtime, "communication_intelligence", "Communications")
        body = payload.get("task") or {}
        due_at_raw = str(body.get("due_at") or "").strip()
        due_at = datetime.fromisoformat(due_at_raw) if due_at_raw else None
        if due_at is not None and due_at.tzinfo is None:
            due_at = due_at.replace(tzinfo=timezone.utc)
        assignee_employee_id = int(body["assignee_employee_id"]) if body.get("assignee_employee_id") else None
        assignee_user_id = int(body["assignee_user_id"]) if body.get("assignee_user_id") else None
        try:
            if assignee_employee_id is not None and assignee_user_id is None:
                employee = PeopleService(session).get_employee(runtime.context, assignee_employee_id)
                assignee_user_id = employee.user_id
            task = CommunicationService(session).create_follow_up_task(
                runtime.context,
                review_id=review_id,
                created_by_user_id=runtime.actor_user.id,
                assignee_user_id=assignee_user_id,
                assignee_employee_id=assignee_employee_id,
                due_at=due_at,
            )
            AuditLogService(session).log(
                runtime.context,
                "communications.task.created",
                "task",
                str(task.id),
                details={"review_id": review_id, "assignee_employee_id": assignee_employee_id, "assignee_user_id": assignee_user_id},
            )
        except (PlatformCoreError, TenantContextError, ValueError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"task": _serialize_task(task)})

    @app.get("/admin/{account_slug}/alerts-tasks", response_class=HTMLResponse)
    def admin_alerts_tasks(
        request: Request,
        account_slug: str,
        severity: str | None = Query(default=None),
        priority: str | None = Query(default=None),
        alert_code: str | None = Query(default=None),
        overdue: bool = Query(default=False),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/alerts-tasks")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "alerts.read")
        ensure_permission(runtime, "tasks.read")
        automation = RuntimeAutomationService(session)
        alerts = [item for item in automation.list_alerts(runtime.context) if item.status == "open"]
        tasks = [item for item in automation.list_tasks(runtime.context) if item.status == "open"]
        if severity:
            alerts = [item for item in alerts if item.severity == severity]
        if alert_code:
            alerts = [item for item in alerts if item.code == alert_code]
        if priority:
            tasks = [item for item in tasks if item.priority == priority]
        if overdue:
            tasks = [
                item for item in tasks
                if item.due_at is not None and item.due_at <= datetime.now(timezone.utc)
            ]
        user_ids = {item.assigned_user_id for item in alerts if item.assigned_user_id is not None}
        user_ids.update(item.assignee_user_id for item in tasks if item.assignee_user_id is not None)
        employee_ids = {item.assignee_employee_id for item in tasks if item.assignee_employee_id is not None}
        users = {
            item.id: item
            for item in session.execute(select(User).where(User.id.in_(user_ids))).scalars().all()
        } if user_ids else {}
        employees = {
            item.id: item
            for item in session.execute(select(Employee).where(Employee.id.in_(employee_ids))).scalars().all()
        } if employee_ids else {}
        overdue_tasks = [item for item in tasks if item.due_at is not None and item.due_at <= datetime.now(timezone.utc)]
        return templates.TemplateResponse(
            request,
            "admin/alerts_tasks.html",
            {
                **_admin_context(request, session, runtime, page="alerts_tasks"),
                "alerts": alerts,
                "tasks": tasks,
                "overdue_tasks": overdue_tasks,
                "severity_filter": severity,
                "priority_filter": priority,
                "alert_code_filter": alert_code,
                "overdue_filter": overdue,
                "users": users,
                "employees": employees,
                "alert_slas": _alert_sla_map(),
                "default_users": _default_account_users(runtime, session),
                "can_assign_defaults": _is_manager_role(runtime.role_code),
            },
        )

    @app.post("/admin/{account_slug}/alerts/{alert_id}/status")
    async def admin_alert_status(
        request: Request,
        account_slug: str,
        alert_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "alerts.read")
        next_status = str(payload.get("status") or "").strip()
        note = str(payload.get("note") or "").strip() or None
        if next_status not in {"open", "acknowledged", "dismissed"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported alert status.")
        alert = session.execute(
            select(Alert).where(Alert.account_id == runtime.account.id, Alert.id == alert_id)
        ).scalar_one_or_none()
        if alert is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found.")
        alert.status = next_status
        AuditLogService(session).log(
            runtime.context,
            "account.alert.status",
            "alert",
            str(alert.id),
            details={"status": next_status, "note": note, "source": "alerts-tasks"},
        )
        session.flush()
        return JSONResponse({"alert": _serialize_alert(alert)})

    @app.post("/admin/{account_slug}/alerts/{alert_id}/assign-default")
    async def admin_alert_assign_default(
        request: Request,
        account_slug: str,
        alert_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        _require_account_manager(runtime)
        ensure_permission(runtime, "alerts.read")
        assignee_kind = str(payload.get("assignee_kind") or "operator").strip()
        defaults = _default_account_users(runtime, session)
        assigned_user = defaults["owner"] if assignee_kind == "owner" else defaults["operator"]
        if assigned_user is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Default assignee is not configured.")
        alert = session.execute(
            select(Alert).where(Alert.account_id == runtime.account.id, Alert.id == alert_id)
        ).scalar_one_or_none()
        if alert is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found.")
        alert.assigned_user_id = assigned_user.id
        AuditLogService(session).log(
            runtime.context,
            "account.alert.assign_default",
            "alert",
            str(alert.id),
            details={"assigned_user_id": assigned_user.id, "assignee_kind": assignee_kind, "source": "alerts-tasks"},
        )
        session.flush()
        return JSONResponse({"alert": _serialize_alert(alert), "assigned_user": _serialize_user(assigned_user)})

    @app.post("/admin/{account_slug}/tasks/{task_id}/status")
    async def admin_task_status(
        request: Request,
        account_slug: str,
        task_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "tasks.read")
        next_status = str(payload.get("status") or "").strip()
        note = str(payload.get("note") or "").strip() or None
        if next_status not in {"open", "done"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported task status.")
        task = session.execute(
            select(Task).where(Task.account_id == runtime.account.id, Task.id == task_id)
        ).scalar_one_or_none()
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        task.status = next_status
        task.completed_at = datetime.now(timezone.utc) if next_status == "done" else None
        session.add(
            TaskEvent(
                account_id=runtime.account.id,
                task_id=task.id,
                actor_user_id=runtime.actor_user.id,
                event_type="task.updated_from_account_ui",
                event_at=datetime.now(timezone.utc),
                payload_json={"status": next_status, "note": note, "source": "alerts-tasks"},
            )
        )
        AuditLogService(session).log(
            runtime.context,
            "account.task.status",
            "task",
            str(task.id),
            details={"status": next_status, "note": note, "source": "alerts-tasks"},
        )
        session.flush()
        return JSONResponse({"task": _serialize_task(task)})

    @app.post("/admin/{account_slug}/tasks/{task_id}/assign-default")
    async def admin_task_assign_default(
        request: Request,
        account_slug: str,
        task_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        _require_account_manager(runtime)
        ensure_permission(runtime, "tasks.read")
        assignee_kind = str(payload.get("assignee_kind") or "operator").strip()
        defaults = _default_account_users(runtime, session)
        assigned_user = defaults["owner"] if assignee_kind == "owner" else defaults["operator"]
        if assigned_user is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Default assignee is not configured.")
        task = session.execute(
            select(Task).where(Task.account_id == runtime.account.id, Task.id == task_id)
        ).scalar_one_or_none()
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        task.assignee_user_id = assigned_user.id
        session.add(
            TaskEvent(
                account_id=runtime.account.id,
                task_id=task.id,
                actor_user_id=runtime.actor_user.id,
                event_type="task.assigned_default",
                event_at=datetime.now(timezone.utc),
                payload_json={"assigned_user_id": assigned_user.id, "assignee_kind": assignee_kind, "source": "alerts-tasks"},
            )
        )
        AuditLogService(session).log(
            runtime.context,
            "account.task.assign_default",
            "task",
            str(task.id),
            details={"assigned_user_id": assigned_user.id, "assignee_kind": assignee_kind, "source": "alerts-tasks"},
        )
        session.flush()
        return JSONResponse({"task": _serialize_task(task), "assigned_user": _serialize_user(assigned_user)})

    @app.get("/admin/{account_slug}/ops-sync", response_class=HTMLResponse)
    def admin_ops_sync(
        request: Request,
        account_slug: str,
        sync_state: str | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/ops-sync")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "integrations.manage")
        ensure_permission(runtime, "rules.manage")
        ensure_permission(runtime, "tasks.read")
        ensure_permission(runtime, "alerts.read")
        _ensure_account_feature(runtime, "ops_console", "Ops / Sync")
        ops = AdminQueryService(session).ops_summary(runtime.account.id)
        if sync_state:
            filtered_rows = []
            for item in ops["integration_sync_status"]:
                status_code = _portfolio_sync_health([item])["status"]
                if sync_state == status_code:
                    filtered_rows.append(item)
            ops = dict(ops)
            ops["integration_sync_status"] = filtered_rows
        return templates.TemplateResponse(
            request,
            "admin/ops_sync.html",
            {
                **_admin_context(request, session, runtime, page="ops_sync"),
                "ops": ops,
                "sync_state_filter": sync_state,
                "human_sync_error": _human_sync_error,
            },
        )

    @app.get("/admin/{account_slug}/goals", response_class=HTMLResponse)
    def admin_goals(
        request: Request,
        account_slug: str,
        goal_id: int | None = Query(default=None),
        risk_only: bool = Query(default=False),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        user = _current_session_user(session, request)
        if user is None:
            request.session.clear()
            return _login_redirect(f"/admin/{account_slug}/goals")
        actor_email = user.email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        request.session["admin_account_slug"] = runtime.account.slug
        ensure_permission(runtime, "dashboard.read")
        _ensure_account_feature(runtime, "goals_tracking", "Goals tracking")
        service = GoalService(session)
        goals = service.list_goals(runtime.context)
        goal_summaries: dict[int, dict[str, object]] = {}
        if risk_only:
            risky_goals = []
            for goal in goals:
                metrics_payload = service.get_goal_metrics(runtime.context, goal.id)
                goal_summaries[goal.id] = metrics_payload["summary"]
                if metrics_payload["summary"]["status"] != "on_track":
                    risky_goals.append(goal)
            goals = risky_goals
            if goal_id is None and goals:
                goal_id = goals[0].id
        try:
            selected_goal_payload = service.get_goal_metrics(runtime.context, goal_id) if goal_id is not None else None
        except TenantContextError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        selected_targets_by_code = {
            item.metric_code: item
            for item in (selected_goal_payload["targets"] if selected_goal_payload is not None else [])
        }
        owners = session.execute(
            select(User)
            .join(AccountUser, AccountUser.user_id == User.id)
            .where(AccountUser.account_id == runtime.account.id, AccountUser.status == "active")
            .order_by(User.full_name.asc(), User.email.asc())
        ).scalars().all()
        period_start, period_end = _goal_period_defaults("month", runtime)
        dashboard = ExecutiveDashboardService(session).get_dashboard(runtime.context, "today")
        widgets = {item["widget_key"]: item["payload"] for item in dashboard["widgets"]}
        automation = RuntimeAutomationService(session)
        open_alerts = [item for item in automation.list_alerts(runtime.context) if item.status == "open"]
        open_tasks = [item for item in automation.list_tasks(runtime.context) if item.status == "open"]
        selected_goal_enriched = None
        if selected_goal_payload is not None:
            selected_goal_enriched = dict(selected_goal_payload)
            selected_goal_enriched["blockers"] = _goal_blocker_rows(
                account_slug=account_slug,
                actor_email=actor_email,
                metrics=list(selected_goal_payload["metrics"]),
                open_alerts=open_alerts,
                open_tasks=open_tasks,
                top_problems=widgets.get("owner_panel", {}).get("top_problems", []),
                attention_zones=widgets.get("owner_panel", {}).get("attention_zones", []),
            )
        return templates.TemplateResponse(
            request,
            "admin/goals.html",
            {
                **_admin_context(request, session, runtime, page="goals"),
                "goals": goals,
                "selected_goal": selected_goal_enriched,
                "selected_targets_by_code": selected_targets_by_code,
                "metric_definitions": list(GOAL_METRIC_DEFINITIONS.values()),
                "owners": owners,
                "goal_summaries": goal_summaries,
                "risk_only": risk_only,
                "default_period_start": period_start.isoformat(),
                "default_period_end": period_end.isoformat(),
            },
        )

    @app.post("/admin/{account_slug}/integrations/{integration_id}/sync")
    async def admin_run_sync(
        request: Request,
        account_slug: str,
        integration_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "integrations.manage")
        _ensure_account_feature(runtime, "integrations_setup", "Integrations setup")
        service = RuntimeIntegrationService(session)
        idempotency_key = f"admin-ui-sync:{integration_id}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
        job, _ = service.enqueue_sync_job(
            runtime.context,
            integration_id=integration_id,
            job_type="full_sync",
            trigger_mode="manual",
            idempotency_key=idempotency_key,
            scope_json={"source": "admin-ui"},
        )
        execution = service.execute_job(job.id, owner=settings.worker_id, ttl_seconds=settings.runtime_lease_ttl_seconds)
        session.flush()
        return JSONResponse(
            {
                "job": _serialize_sync_job(job),
                "execution": _serialize_job_execution(execution),
            }
        )

    @app.post("/admin/portfolio/accounts/{account_slug}/sync")
    async def admin_portfolio_sync_account(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        runtime = _portfolio_account_runtime(request, session, account_slug=account_slug)
        ensure_permission(runtime, "integrations.manage")
        integration_id = payload.get("integration_id")
        execute_now = bool(payload.get("execute_now", True))
        service = RuntimeIntegrationService(session)
        integrations = [item for item in service.list_integrations(runtime.context) if item.status == "active"]
        if integration_id is not None:
            integrations = [item for item in integrations if item.id == int(integration_id)]
        if not integrations:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No active integrations available for sync.")
        results: list[dict[str, object]] = []
        for integration in integrations:
            idempotency_key = f"portfolio-sync:{runtime.account.slug}:{integration.id}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
            job, created = service.enqueue_sync_job(
                runtime.context,
                integration_id=integration.id,
                job_type="full_sync",
                trigger_mode="manual",
                idempotency_key=idempotency_key,
                scope_json={"source": "portfolio"},
            )
            execution = service.execute_job(job.id, owner=settings.worker_id, ttl_seconds=settings.runtime_lease_ttl_seconds) if execute_now else None
            results.append(
                {
                    "integration_id": integration.id,
                    "integration_ref": integration.external_ref or integration.display_name,
                    "created": created,
                    "job": _serialize_sync_job(job),
                    "execution": _serialize_job_execution(execution) if execution is not None else None,
                }
            )
        return JSONResponse({"account_slug": runtime.account.slug, "count": len(results), "results": results})

    @app.post("/admin/portfolio/accounts/{account_slug}/alerts/{alert_id}/status")
    async def admin_portfolio_alert_status(
        request: Request,
        account_slug: str,
        alert_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        runtime = _portfolio_account_runtime(request, session, account_slug=account_slug)
        ensure_permission(runtime, "alerts.read")
        next_status = str(payload.get("status") or "").strip()
        if next_status not in {"open", "acknowledged", "dismissed"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported alert status.")
        alert = session.execute(
            select(Alert).where(Alert.account_id == runtime.account.id, Alert.id == alert_id)
        ).scalar_one_or_none()
        if alert is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found.")
        alert.status = next_status
        AuditLogService(session).log(
            runtime.context,
            "owner.alert.status",
            "alert",
            str(alert.id),
            details={"status": next_status, "source": "portfolio"},
        )
        session.flush()
        return JSONResponse({"alert": _serialize_alert(alert)})

    @app.post("/admin/portfolio/accounts/{account_slug}/tasks/{task_id}/status")
    async def admin_portfolio_task_status(
        request: Request,
        account_slug: str,
        task_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        runtime = _portfolio_account_runtime(request, session, account_slug=account_slug)
        ensure_permission(runtime, "tasks.read")
        next_status = str(payload.get("status") or "").strip()
        if next_status not in {"open", "done"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported task status.")
        task = session.execute(
            select(Task).where(Task.account_id == runtime.account.id, Task.id == task_id)
        ).scalar_one_or_none()
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        task.status = next_status
        task.completed_at = datetime.now(timezone.utc) if next_status == "done" else None
        session.add(
            TaskEvent(
                account_id=runtime.account.id,
                task_id=task.id,
                actor_user_id=runtime.actor_user.id,
                event_type="task.updated_by_owner",
                event_at=datetime.now(timezone.utc),
                payload_json={"status": next_status, "source": "portfolio"},
            )
        )
        AuditLogService(session).log(
            runtime.context,
            "owner.task.status",
            "task",
            str(task.id),
            details={"status": next_status, "source": "portfolio"},
        )
        session.flush()
        return JSONResponse({"task": _serialize_task(task)})

    @app.post("/admin/{account_slug}/integrations/{integration_id}/status")
    async def admin_update_integration_status(
        request: Request,
        account_slug: str,
        integration_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        payload = await request.json()
        next_status = str(payload.get("status") or "").strip()
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "integrations.manage")
        _ensure_account_feature(runtime, "integrations_setup", "Integrations setup")
        service = RuntimeIntegrationService(session)
        try:
            integration = service.set_integration_status(runtime.context, integration_id=integration_id, status=next_status)
        except (PlatformCoreError, TenantContextError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"integration": _serialize_integration(integration)})

    @app.post("/admin/{account_slug}/integrations/save")
    async def admin_save_integration(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "integrations.manage")
        _ensure_account_feature(runtime, "integrations_setup", "Integrations setup")
        body = _parse_admin_payload(payload, "integration")
        credentials = _parse_admin_payload(payload, "credentials")
        replace_mode = str(payload.get("replace_mode") or "merge")
        service = RuntimeIntegrationService(session)
        integration_id = payload.get("integration_id")
        if replace_mode == "replace" and not credentials:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Replace mode requires a new credential payload.")
        try:
            if integration_id:
                integration = service.update_integration(
                    runtime.context,
                    integration_id=int(integration_id),
                    display_name=str(body.get("display_name") or "").strip() or None,
                    external_ref=body.get("external_ref"),
                    status=str(body.get("status") or "active"),
                    connection_mode=str(body.get("connection_mode") or "polling"),
                    sync_mode=str(body.get("sync_mode") or "manual"),
                    settings_json=body.get("settings") if isinstance(body.get("settings"), dict) else None,
                )
            else:
                integration = service.create_integration(
                    runtime.context,
                    provider_kind=str(body.get("provider_kind") or "").strip(),
                    provider_name=str(body.get("provider_name") or "").strip(),
                    display_name=str(body.get("display_name") or "").strip(),
                    external_ref=body.get("external_ref"),
                    status=str(body.get("status") or "active"),
                    connection_mode=str(body.get("connection_mode") or "polling"),
                    sync_mode=str(body.get("sync_mode") or "manual"),
                    settings_json=body.get("settings") if isinstance(body.get("settings"), dict) else None,
                )
            if credentials:
                service.save_credentials(
                    runtime.context,
                    integration_id=integration.id,
                    secret_payload=credentials,
                    replace_mode=replace_mode,
                )
            setup = service.integration_setup_payload(runtime.context, integration_id=integration.id)
        except (PlatformCoreError, TenantContextError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse(
            {
                "integration": _serialize_integration(integration),
                "setup": {
                    "masked_credentials": setup["masked_credentials"],
                    "credential_version": setup["credential_version"],
                    "credential_last_rotated_at": _serialize_datetime(setup["credential_last_rotated_at"]),
                    "latest_jobs": [_serialize_sync_job(item) for item in setup["latest_jobs"]],
                },
            }
        )

    @app.post("/admin/{account_slug}/integrations/{integration_id}/test")
    async def admin_test_integration(
        request: Request,
        account_slug: str,
        integration_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "integrations.manage")
        _ensure_account_feature(runtime, "integrations_setup", "Integrations setup")
        credentials = _parse_admin_payload(payload, "credentials")
        try:
            result = RuntimeIntegrationService(session).test_connection(
                runtime.context,
                integration_id=integration_id,
                override_payload=credentials,
            )
        except (PlatformCoreError, TenantContextError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse(result)

    @app.post("/admin/{account_slug}/members/save")
    async def admin_save_member(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        payload = await request.json()
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        _require_account_manager(runtime)
        body = _parse_admin_payload(payload, "member")
        membership_id = payload.get("membership_id")
        role_code = str(body.get("role_code") or "").strip()
        status_code = str(body.get("status") or "active").strip()
        membership_service = MembershipService(session)
        user_security = UserSecurityService(session)
        try:
            if membership_id:
                membership = membership_service.get_membership(runtime.account, int(membership_id))
                submitted_email = str(body.get("email") or "").strip().lower()
                if submitted_email and submitted_email != membership.user.email:
                    raise PlatformCoreError("Existing membership email cannot be changed. Remove and add a new member instead.")
                full_name = str(body.get("full_name") or membership.user.full_name or membership.user.email).strip()
                membership.user.full_name = full_name or membership.user.email
                membership = membership_service.update_membership(
                    runtime.account,
                    membership.id,
                    role_code=role_code or None,
                    status=status_code or None,
                )
            else:
                email = str(body.get("email") or "").strip().lower()
                full_name = str(body.get("full_name") or email).strip()
                if not email or not role_code:
                    raise PlatformCoreError("Email and role are required for new membership.")
                user, _ = user_security.create_or_update_user(
                    email=email,
                    full_name=full_name or email,
                    status="invited",
                )
                membership, _ = membership_service.ensure_membership(
                    runtime.account,
                    user,
                    role_code,
                    status=status_code or "active",
                )
            session.flush()
        except (PlatformCoreError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"membership": _serialize_membership(membership)})

    @app.post("/admin/{account_slug}/members/{membership_id}/disable")
    async def admin_disable_member(
        request: Request,
        account_slug: str,
        membership_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        _require_account_manager(runtime)
        try:
            membership = MembershipService(session).disable_membership(runtime.account, membership_id)
        except PlatformCoreError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"membership": _serialize_membership(membership)})

    @app.post("/admin/{account_slug}/members/{membership_id}/remove")
    async def admin_remove_member(
        request: Request,
        account_slug: str,
        membership_id: int,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        _require_account_manager(runtime)
        try:
            MembershipService(session).remove_membership(runtime.account, membership_id)
        except PlatformCoreError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse({"removed": True, "membership_id": membership_id})

    @app.post("/admin/{account_slug}/goals/save")
    async def admin_save_goal(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        await _require_csrf(request)
        payload = await request.json()
        actor_email = _require_admin_user(request, session).email
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "rules.manage")
        _ensure_account_feature(runtime, "goals_tracking", "Goals tracking")
        body = payload.get("goal") or {}
        targets = payload.get("targets") or []
        period_kind = str(body.get("period_kind") or "month")
        period_start_raw = body.get("period_start")
        period_end_raw = body.get("period_end")
        period_start = date.fromisoformat(period_start_raw) if period_start_raw else None
        period_end = date.fromisoformat(period_end_raw) if period_end_raw else None
        period_start, period_end = _goal_period_from_payload(runtime, period_kind, period_start, period_end)
        service = GoalService(session)
        goal_id = payload.get("goal_id")
        if goal_id:
            try:
                goal = service.update_goal(
                    runtime.context,
                    int(goal_id),
                    title=body.get("title"),
                    description=body.get("description"),
                    period_kind=period_kind,
                    period_start=period_start,
                    period_end=period_end,
                    owner_user_id=int(body["owner_user_id"]) if body.get("owner_user_id") else None,
                    is_primary=bool(body.get("is_primary")),
                    status=str(body.get("status") or "active"),
                    targets=targets,
                )
            except TenantContextError as exc:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
            except PlatformCoreError as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        else:
            try:
                goal = service.create_goal(
                    runtime.context,
                    title=str(body.get("title") or "").strip(),
                    description=body.get("description"),
                    period_kind=period_kind,
                    period_start=period_start,
                    period_end=period_end,
                    owner_user_id=int(body["owner_user_id"]) if body.get("owner_user_id") else None,
                    is_primary=bool(body.get("is_primary")),
                    status=str(body.get("status") or "active"),
                    targets=targets,
                )
            except PlatformCoreError as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        metrics = service.get_goal_metrics(runtime.context, goal.id)
        return JSONResponse(
            {
                "goal": _serialize_goal(goal, summary=metrics["summary"]),
                "metrics": metrics["metrics"],
            }
        )

    return app


def _serialize_decimal(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _serialize_account(account: Account) -> dict[str, object]:
    return {
        "id": account.id,
        "slug": account.slug,
        "name": account.name,
        "status": account.status,
        "plan_type": account.plan_type,
        "default_timezone": account.default_timezone,
        "default_currency": account.default_currency,
        "settings": account.settings_json if isinstance(account.settings_json, dict) else {},
        "feature_flags": account.feature_flags_json if isinstance(account.feature_flags_json, dict) else {},
        "soft_limits": account.soft_limits_json if isinstance(account.soft_limits_json, dict) else {},
        "created_at": _serialize_datetime(account.created_at),
        "updated_at": _serialize_datetime(account.updated_at),
    }


def _serialize_user(user: User) -> dict[str, object]:
    return {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name,
        "status": user.status,
        "password_set_at": _serialize_datetime(user.password_set_at),
        "last_login_at": _serialize_datetime(user.last_login_at),
        "failed_login_attempts": int(user.failed_login_attempts or 0),
        "locked_until": _serialize_datetime(user.locked_until),
        "invite_sent_at": _serialize_datetime(user.invite_sent_at),
        "invite_accepted_at": _serialize_datetime(user.invite_accepted_at),
        "reset_requested_at": _serialize_datetime(user.reset_requested_at),
        "auth_version": int(user.auth_version or 1),
        "created_at": _serialize_datetime(user.created_at),
        "updated_at": _serialize_datetime(user.updated_at),
    }


def _serialize_membership(membership: AccountUser) -> dict[str, object]:
    return {
        "id": membership.id,
        "account_id": membership.account_id,
        "user_id": membership.user_id,
        "role_code": membership.role.code if membership.role is not None else None,
        "role_name": membership.role.name if membership.role is not None else None,
        "status": membership.status,
        "joined_at": _serialize_datetime(membership.joined_at),
        "user": {
            "id": membership.user.id,
            "email": membership.user.email,
            "full_name": membership.user.full_name,
            "status": membership.user.status,
            "password_set_at": _serialize_datetime(membership.user.password_set_at),
        },
    }


def _serialize_employee(employee: Employee) -> dict[str, object]:
    return {
        "id": employee.id,
        "account_id": employee.account_id,
        "user_id": employee.user_id,
        "employee_code": employee.employee_code,
        "full_name": employee.full_name,
        "role_title": employee.role_title,
        "department": employee.department,
        "email": employee.email,
        "phone": employee.phone,
        "status": employee.status,
        "hired_at": employee.hired_at.isoformat() if employee.hired_at else None,
        "created_at": _serialize_datetime(employee.created_at),
        "updated_at": _serialize_datetime(employee.updated_at),
    }


def _serialize_onboarding(onboarding: dict[str, object]) -> dict[str, object]:
    last_success = onboarding["last_success"]
    return {
        "account": _serialize_account(onboarding["account"]),
        "status": onboarding["status"],
        "completed_steps": onboarding["completed_steps"],
        "total_steps": onboarding["total_steps"],
        "next_step": onboarding["next_step"],
        "owners_count": onboarding["owners_count"],
        "admins_count": onboarding["admins_count"],
        "goals_count": onboarding["goals_count"],
        "last_success": _serialize_datetime(last_success.finished_at if last_success is not None else None),
        "steps": list(onboarding["steps"]),
    }


def _serialize_task(task) -> dict[str, object]:
    return {
        "id": task.id,
        "account_id": task.account_id,
        "assignee_user_id": task.assignee_user_id,
        "assignee_employee_id": task.assignee_employee_id,
        "title": task.title,
        "description": task.description,
        "status": task.status,
        "priority": task.priority,
        "due_at": _serialize_datetime(task.due_at),
        "completed_at": _serialize_datetime(task.completed_at),
        "source": task.source,
        "source_rule_id": task.source_rule_id,
        "dedupe_key": task.dedupe_key,
        "escalation_level": task.escalation_level,
        "related_entity_type": task.related_entity_type,
        "related_entity_id": task.related_entity_id,
    }


def _serialize_alert(alert) -> dict[str, object]:
    return {
        "id": alert.id,
        "account_id": alert.account_id,
        "assigned_user_id": alert.assigned_user_id,
        "code": alert.code,
        "title": alert.title,
        "description": alert.description,
        "severity": alert.severity,
        "status": alert.status,
        "source_rule_id": alert.source_rule_id,
        "dedupe_key": alert.dedupe_key,
        "related_entity_type": alert.related_entity_type,
        "related_entity_id": alert.related_entity_id,
        "last_detected_at": _serialize_datetime(alert.last_detected_at),
    }


def _serialize_warehouse(warehouse: Warehouse) -> dict[str, object]:
    return {
        "id": warehouse.id,
        "account_id": warehouse.account_id,
        "code": warehouse.code,
        "name": warehouse.name,
        "status": warehouse.status,
        "location": warehouse.location,
        "created_at": _serialize_datetime(warehouse.created_at),
        "updated_at": _serialize_datetime(warehouse.updated_at),
    }


def _serialize_product(product: Product) -> dict[str, object]:
    return {
        "id": product.id,
        "account_id": product.account_id,
        "sku": product.sku,
        "name": product.name,
        "unit": product.unit,
        "status": product.status,
        "list_price": str(product.list_price),
        "cost_price": str(product.cost_price),
        "min_stock_level": str(product.min_stock_level),
        "created_at": _serialize_datetime(product.created_at),
        "updated_at": _serialize_datetime(product.updated_at),
    }


def _serialize_purchase(purchase: Purchase) -> dict[str, object]:
    return {
        "id": purchase.id,
        "account_id": purchase.account_id,
        "supplier_customer_id": purchase.supplier_customer_id,
        "warehouse_id": purchase.warehouse_id,
        "purchase_number": purchase.purchase_number,
        "status": purchase.status,
        "ordered_at": _serialize_datetime(purchase.ordered_at),
        "received_at": _serialize_datetime(purchase.received_at),
        "currency": purchase.currency,
        "total_amount": str(purchase.total_amount),
        "notes_json": purchase.notes_json or {},
        "created_at": _serialize_datetime(purchase.created_at),
        "updated_at": _serialize_datetime(purchase.updated_at),
    }


def _serialize_document(document: Document) -> dict[str, object]:
    return {
        "id": document.id,
        "account_id": document.account_id,
        "customer_id": document.customer_id,
        "deal_id": document.deal_id,
        "document_type": document.document_type,
        "document_number": document.document_number,
        "status": document.status,
        "issued_at": _serialize_datetime(document.issued_at),
        "total_amount": str(document.total_amount),
        "currency": document.currency,
        "snapshot_json": document.snapshot_json or {},
        "created_at": _serialize_datetime(document.created_at),
        "updated_at": _serialize_datetime(document.updated_at),
    }


def _serialize_installation_request(request_item: InstallationRequest) -> dict[str, object]:
    return {
        "id": request_item.id,
        "account_id": request_item.account_id,
        "customer_id": request_item.customer_id,
        "deal_id": request_item.deal_id,
        "assigned_employee_id": request_item.assigned_employee_id,
        "request_number": request_item.request_number,
        "title": request_item.title,
        "status": request_item.status,
        "address": request_item.address,
        "scheduled_for": _serialize_datetime(request_item.scheduled_for),
        "notes_json": request_item.notes_json or {},
        "created_at": _serialize_datetime(request_item.created_at),
        "updated_at": _serialize_datetime(request_item.updated_at),
    }


def _serialize_communication_review(review: CommunicationReview) -> dict[str, object]:
    return {
        "id": review.id,
        "account_id": review.account_id,
        "created_by_user_id": review.created_by_user_id,
        "customer_id": review.customer_id,
        "lead_id": review.lead_id,
        "employee_id": review.employee_id,
        "channel": review.channel,
        "direction": review.direction,
        "title": review.title,
        "transcript_text": review.transcript_text,
        "source_kind": review.source_kind,
        "quality_status": review.quality_status,
        "sentiment": review.sentiment,
        "response_delay_minutes": review.response_delay_minutes,
        "next_step_present": review.next_step_present,
        "follow_up_status": review.follow_up_status,
        "summary_json": review.summary_json or {},
        "created_at": _serialize_datetime(review.created_at),
        "updated_at": _serialize_datetime(review.updated_at),
    }


def _serialize_knowledge_item(item: KnowledgeItem) -> dict[str, object]:
    return {
        "id": item.id,
        "account_id": item.account_id,
        "created_by_user_id": item.created_by_user_id,
        "customer_id": item.customer_id,
        "deal_id": item.deal_id,
        "document_id": item.document_id,
        "item_type": item.item_type,
        "source_kind": item.source_kind,
        "title": item.title,
        "summary": item.summary,
        "body_text": item.body_text,
        "status": item.status,
        "visibility": item.visibility,
        "file_name": item.file_name,
        "file_path": item.file_path,
        "mime_type": item.mime_type,
        "content_size_bytes": item.content_size_bytes,
        "content_sha256": item.content_sha256,
        "tags": list(item.tags_json or []),
        "metadata": item.metadata_json if isinstance(item.metadata_json, dict) else {},
        "created_at": _serialize_datetime(item.created_at),
        "updated_at": _serialize_datetime(item.updated_at),
    }


def _serialize_recommendation(recommendation) -> dict[str, object]:
    return {
        "id": recommendation.id,
        "account_id": recommendation.account_id,
        "code": recommendation.code,
        "title": recommendation.title,
        "status": recommendation.status,
        "source_rule_id": recommendation.source_rule_id,
        "dedupe_key": recommendation.dedupe_key,
        "related_entity_type": recommendation.related_entity_type,
        "related_entity_id": recommendation.related_entity_id,
    }


def _serialize_integration(integration) -> dict[str, object]:
    return {
        "id": integration.id,
        "account_id": integration.account_id,
        "external_ref": integration.external_ref,
        "provider_kind": integration.provider_kind,
        "provider_name": integration.provider_name,
        "display_name": integration.display_name,
        "status": integration.status,
        "sync_mode": integration.sync_mode,
        "connection_mode": integration.connection_mode,
        "last_sync_at": _serialize_datetime(integration.last_sync_at),
        "settings": integration.settings_json,
    }


def _serialize_sync_job(job) -> dict[str, object]:
    return {
        "id": job.id,
        "account_id": job.account_id,
        "integration_id": job.integration_id,
        "provider_kind": job.provider_kind,
        "provider_name": job.provider_name,
        "job_type": job.job_type,
        "status": job.status,
        "idempotency_key": job.idempotency_key,
        "attempts_count": job.attempts_count,
        "max_attempts": job.max_attempts,
        "locked_by": job.locked_by,
        "scheduled_at": _serialize_datetime(job.scheduled_at),
        "started_at": _serialize_datetime(job.started_at),
        "finished_at": _serialize_datetime(job.finished_at),
        "error_code": job.error_code,
        "error_message": job.error_message,
        "scope": job.scope_json,
        "cursor": job.cursor_json,
    }


def _serialize_integration_log(log) -> dict[str, object]:
    return {
        "id": log.id,
        "account_id": log.account_id,
        "integration_id": log.integration_id,
        "sync_job_id": log.sync_job_id,
        "level": log.level,
        "event_type": log.event_type,
        "status": log.status,
        "provider_kind": log.provider_kind,
        "provider_name": log.provider_name,
        "message": log.message,
        "payload": log.payload_json,
        "created_at": _serialize_datetime(log.created_at),
    }


def _serialize_rule_execution(execution) -> dict[str, object]:
    return {
        "id": execution.id,
        "account_id": execution.account_id,
        "rule_id": execution.rule_id,
        "rule_version_id": execution.rule_version_id,
        "execution_key": execution.execution_key,
        "status": execution.status,
        "evaluated_entity_type": execution.evaluated_entity_type,
        "evaluated_entity_id": execution.evaluated_entity_id,
        "window_key": execution.window_key,
        "run_count": execution.run_count,
        "alert_id": execution.alert_id,
        "task_id": execution.task_id,
        "recommendation_id": execution.recommendation_id,
        "details": execution.details_json,
        "error_message": execution.error_message,
        "last_evaluated_at": _serialize_datetime(execution.last_evaluated_at),
        "last_triggered_at": _serialize_datetime(execution.last_triggered_at),
        "updated_at": _serialize_datetime(execution.updated_at),
    }


def _serialize_goal(goal: Goal, *, summary: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "id": goal.id,
        "account_id": goal.account_id,
        "owner_user_id": goal.owner_user_id,
        "title": goal.title,
        "description": goal.description,
        "status": goal.status,
        "period_kind": goal.period_kind,
        "period_start": goal.period_start.isoformat(),
        "period_end": goal.period_end.isoformat(),
        "is_primary": goal.is_primary,
        "settings": goal.settings_json,
        "created_at": _serialize_datetime(goal.created_at),
        "updated_at": _serialize_datetime(goal.updated_at),
        "summary": summary,
    }


def _serialize_goal_target(target: GoalTarget) -> dict[str, object]:
    return {
        "id": target.id,
        "goal_id": target.goal_id,
        "metric_code": target.metric_code,
        "direction": target.direction,
        "target_value": _serialize_decimal(target.target_value),
        "settings": target.settings_json,
    }


def _serialize_audit_log(log) -> dict[str, object]:
    return {
        "id": log.id,
        "account_id": log.account_id,
        "actor_user_id": log.actor_user_id,
        "source": log.source,
        "action": log.action,
        "entity_type": log.entity_type,
        "entity_id": log.entity_id,
        "status": log.status,
        "request_id": log.request_id,
        "details": log.details_json,
        "created_at": _serialize_datetime(log.created_at),
    }


def _serialize_job_execution(result) -> dict[str, object] | None:
    if result is None:
        return None
    return {
        "job_id": result.job_id,
        "status": result.status,
        "lease_acquired": result.lease_acquired,
        "message": result.message,
    }
