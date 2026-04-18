from __future__ import annotations

import json
import secrets
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlencode
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from platform_core.models import Account, AccountUser, Employee, Goal, GoalTarget, Role, User
from platform_core.exceptions import AuthorizationError, PlatformCoreError, TenantContextError
from platform_core.services import AuditLogService, ExecutiveDashboardService, GOAL_METRIC_DEFINITIONS, GoalService
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

    async def _require_csrf(request: Request) -> None:
        expected = _ensure_session_csrf_token(request)
        supplied = _request_csrf_token(request)
        if supplied is None and request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
            supplied = await _request_form_csrf_token(request)
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

    def _login_redirect(next_path: str | None = None) -> RedirectResponse:
        suffix = ""
        if next_path:
            suffix = f"?{urlencode({'next': next_path})}"
        return RedirectResponse(url=f"/admin/login{suffix}", status_code=status.HTTP_302_FOUND)

    def _admin_page_path(page: str) -> str:
        return {
            "portfolio": "portfolio",
            "accounts": "accounts",
            "users": "users",
            "dashboard": "dashboard",
            "integrations": "integrations",
            "alerts_tasks": "alerts-tasks",
            "ops_sync": "ops-sync",
            "goals": "goals",
            "members": "members",
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
        return bool(_owner_accounts_with_memberships(session, actor_email))

    def _require_portfolio_owner(session: Session, actor_email: str) -> None:
        if _actor_can_view_portfolio(session, actor_email):
            return
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner role is required for portfolio view.")

    def _require_account_manager(runtime: ResolvedRuntimeContext) -> None:
        if _is_manager_role(runtime.role_code):
            return
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Owner or admin role is required.")

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
        if "dashboard.read" in permissions or "*" in permissions:
            items.append({"key": "dashboard", "label": "Dashboard", "path": "dashboard"})
            items.append({"key": "goals", "label": "Goals", "path": "goals"})
        if "alerts.read" in permissions or "tasks.read" in permissions or "*" in permissions:
            items.append({"key": "alerts_tasks", "label": "Alerts / Tasks", "path": "alerts-tasks"})
        if _is_manager_role(runtime.role_code):
            items.append({"key": "members", "label": "Members", "path": "members"})
        if "integrations.manage" in permissions or "*" in permissions:
            items.append({"key": "integrations", "label": "Integrations", "path": "integrations"})
        if {"integrations.manage", "rules.manage", "tasks.read", "alerts.read"}.issubset(permissions) or "*" in permissions:
            items.append({"key": "ops_sync", "label": "Ops / Sync", "path": "ops-sync"})
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
        return {
            "runtime": runtime,
            "page": page,
            "page_path": _admin_page_path(page),
            "accessible_accounts": _accessible_accounts(session, actor_email),
            "nav_items": _admin_nav_items(runtime),
            "can_manage_integrations": "*" in runtime.permissions or "integrations.manage" in runtime.permissions,
            "can_manage_goals": "*" in runtime.permissions or "rules.manage" in runtime.permissions,
            "can_view_ops": "*" in runtime.permissions or {"integrations.manage", "rules.manage", "tasks.read", "alerts.read"}.issubset(runtime.permissions),
            "can_manage_members": _is_manager_role(runtime.role_code),
            "can_manage_accounts_global": _actor_can_manage_accounts(session, actor_email),
            "can_manage_users_global": _actor_can_manage_accounts(session, actor_email),
            "can_view_portfolio": _actor_can_view_portfolio(session, actor_email),
            "csrf_token": _ensure_session_csrf_token(request),
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
        owner_rows = _owner_accounts_with_memberships(session, actor_email)
        portfolio_rows = [_portfolio_account_row(session, actor_email, item["account"]) for item in owner_rows]
        portfolio_rows.sort(key=lambda item: (-int(item["risk_score"]), item["account"].name.lower()))
        portfolio = _portfolio_summary(portfolio_rows)
        return templates.TemplateResponse(
            request,
            "admin/portfolio.html",
            {
                "page": "portfolio",
                "page_path": "portfolio",
                "actor_email": actor_email,
                "csrf_token": _ensure_session_csrf_token(request),
                "can_manage_accounts_global": _actor_can_manage_accounts(session, actor_email),
                "can_manage_users_global": _actor_can_manage_accounts(session, actor_email),
                "can_view_portfolio": True,
                "portfolio_rows": portfolio_rows,
                "portfolio": portfolio,
                "human_sync_error": _human_sync_error,
            },
        )

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
                "csrf_token": _ensure_session_csrf_token(request),
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
                "csrf_token": _ensure_session_csrf_token(request),
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

    @app.get("/admin/{account_slug}/alerts-tasks", response_class=HTMLResponse)
    def admin_alerts_tasks(
        request: Request,
        account_slug: str,
        severity: str | None = Query(default=None),
        priority: str | None = Query(default=None),
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
        if priority:
            tasks = [item for item in tasks if item.priority == priority]
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
                "users": users,
                "employees": employees,
                "alert_slas": _alert_sla_map(),
            },
        )

    @app.get("/admin/{account_slug}/ops-sync", response_class=HTMLResponse)
    def admin_ops_sync(
        request: Request,
        account_slug: str,
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
        ops = AdminQueryService(session).ops_summary(runtime.account.id)
        return templates.TemplateResponse(
            request,
            "admin/ops_sync.html",
            {
                **_admin_context(request, session, runtime, page="ops_sync"),
                "ops": ops,
                "human_sync_error": _human_sync_error,
            },
        )

    @app.get("/admin/{account_slug}/goals", response_class=HTMLResponse)
    def admin_goals(
        request: Request,
        account_slug: str,
        goal_id: int | None = Query(default=None),
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
        service = GoalService(session)
        goals = service.list_goals(runtime.context)
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
        "default_timezone": account.default_timezone,
        "default_currency": account.default_currency,
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
        "title": task.title,
        "status": task.status,
        "priority": task.priority,
        "due_at": _serialize_datetime(task.due_at),
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
        "code": alert.code,
        "title": alert.title,
        "severity": alert.severity,
        "status": alert.status,
        "source_rule_id": alert.source_rule_id,
        "dedupe_key": alert.dedupe_key,
        "related_entity_type": alert.related_entity_type,
        "related_entity_id": alert.related_entity_id,
        "last_detected_at": _serialize_datetime(alert.last_detected_at),
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
