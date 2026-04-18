from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from platform_core.models import Account, AccountUser, Employee, Goal, GoalTarget, User
from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.services import ExecutiveDashboardService, GOAL_METRIC_DEFINITIONS, GoalService
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
        return RuntimeContextService(session).resolve(
            account_id=None,
            account_slug=account_slug,
            actor_user_id=None,
            actor_email=actor_email,
            source="admin-ui",
            request_id=request.headers.get("x-request-id"),
        )

    def admin_query_string(actor_email: str) -> str:
        return urlencode({"actor_email": actor_email})

    def _admin_page_path(page: str) -> str:
        return {
            "dashboard": "dashboard",
            "integrations": "integrations",
            "alerts_tasks": "alerts-tasks",
            "ops_sync": "ops-sync",
            "goals": "goals",
        }.get(page, "dashboard")

    def _accessible_accounts(session: Session, actor_email: str) -> list[Account]:
        return session.execute(
            select(Account)
            .join(AccountUser, AccountUser.account_id == Account.id)
            .join(User, User.id == AccountUser.user_id)
            .where(User.email == actor_email, AccountUser.status == "active")
            .order_by(Account.name.asc(), Account.slug.asc())
        ).scalars().all()

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
            "page_query": admin_query_string(actor_email),
            "accessible_accounts": _accessible_accounts(session, actor_email),
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
        query = admin_query_string(actor_email)
        severity = "critical"
        priority = "high"
        if metric_code == "cpl":
            severity = "warning"
        return {
            "alerts": f"/admin/{account_slug}/alerts-tasks?{query}&severity={severity}",
            "tasks": f"/admin/{account_slug}/alerts-tasks?{query}&priority={priority}",
            "ops": f"/admin/{account_slug}/ops-sync?{query}",
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
        try:
            credential = service.save_credentials(
                runtime.context,
                integration_id=integration_id,
                secret_payload=body.credentials,
                credential_type=body.credential_type,
            )
            payload = service.integration_setup_payload(runtime.context, integration_id=integration_id)
        except (TenantContextError, PlatformCoreError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {
            "credential_version": credential.version,
            "masked_credentials": payload["masked_credentials"],
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

    @app.get("/admin", response_class=HTMLResponse)
    def admin_home(
        request: Request,
        account_slug: str | None = Query(default=None),
        actor_email: str | None = Query(default=None),
    ):
        if account_slug and actor_email:
            return RedirectResponse(
                url=f"/admin/{account_slug}/dashboard?{admin_query_string(actor_email)}",
                status_code=status.HTTP_302_FOUND,
            )
        return templates.TemplateResponse(request, "admin/access.html", {})

    @app.get("/admin/{account_slug}/dashboard", response_class=HTMLResponse)
    def admin_dashboard(
        request: Request,
        account_slug: str,
        actor_email: str = Query(...),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
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
                **_admin_context(session, runtime, page="dashboard"),
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
        actor_email: str = Query(...),
        integration_id: int | None = Query(default=None),
        provider: str | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
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
                **_admin_context(session, runtime, page="integrations"),
                "integrations": integrations,
                "sync_status": sync_status,
                "human_sync_error": _human_sync_error,
                "provider_catalog": provider_catalog,
                "selected_provider_key": selected_provider_key,
                "selected_setup": selected_setup,
                "selected_provider_spec": next((item for item in provider_catalog if item["key"] == selected_provider_key), provider_catalog[0]),
            },
        )

    @app.get("/admin/{account_slug}/alerts-tasks", response_class=HTMLResponse)
    def admin_alerts_tasks(
        request: Request,
        account_slug: str,
        actor_email: str = Query(...),
        severity: str | None = Query(default=None),
        priority: str | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
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
                **_admin_context(session, runtime, page="alerts_tasks"),
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
        actor_email: str = Query(...),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "integrations.manage")
        ensure_permission(runtime, "rules.manage")
        ensure_permission(runtime, "tasks.read")
        ensure_permission(runtime, "alerts.read")
        ops = AdminQueryService(session).ops_summary(runtime.account.id)
        return templates.TemplateResponse(
            request,
            "admin/ops_sync.html",
            {
                **_admin_context(session, runtime, page="ops_sync"),
                "ops": ops,
                "human_sync_error": _human_sync_error,
            },
        )

    @app.get("/admin/{account_slug}/goals", response_class=HTMLResponse)
    def admin_goals(
        request: Request,
        account_slug: str,
        actor_email: str = Query(...),
        goal_id: int | None = Query(default=None),
        session: Session = Depends(get_db_session),
    ) -> HTMLResponse:
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
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
                **_admin_context(session, runtime, page="goals"),
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
        payload = await request.json()
        actor_email = str(payload.get("actor_email") or "").strip()
        if not actor_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="actor_email is required.")
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

    @app.post("/admin/{account_slug}/integrations/save")
    async def admin_save_integration(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        payload = await request.json()
        actor_email = str(payload.get("actor_email") or "").strip()
        if not actor_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="actor_email is required.")
        runtime = resolve_admin_runtime(request, session, account_slug=account_slug, actor_email=actor_email)
        ensure_permission(runtime, "integrations.manage")
        body = _parse_admin_payload(payload, "integration")
        credentials = _parse_admin_payload(payload, "credentials")
        service = RuntimeIntegrationService(session)
        integration_id = payload.get("integration_id")
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
                service.save_credentials(runtime.context, integration_id=integration.id, secret_payload=credentials)
            setup = service.integration_setup_payload(runtime.context, integration_id=integration.id)
        except (PlatformCoreError, TenantContextError, IntegrityError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse(
            {
                "integration": _serialize_integration(integration),
                "setup": {
                    "masked_credentials": setup["masked_credentials"],
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
        payload = await request.json()
        actor_email = str(payload.get("actor_email") or "").strip()
        if not actor_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="actor_email is required.")
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

    @app.post("/admin/{account_slug}/goals/save")
    async def admin_save_goal(
        request: Request,
        account_slug: str,
        session: Session = Depends(get_db_session),
    ) -> JSONResponse:
        payload = await request.json()
        actor_email = str(payload.get("actor_email") or "").strip()
        if not actor_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="actor_email is required.")
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
