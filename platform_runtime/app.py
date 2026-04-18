from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from fastapi import Depends, FastAPI, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from platform_core.services import ExecutiveDashboardService
from platform_core.services.runtime import (
    AdminQueryService,
    ResolvedRuntimeContext,
    RuntimeAutomationService,
    RuntimeIntegrationService,
    SchedulerService,
)
from platform_core.settings import load_platform_settings
from platform_runtime.deps import get_db_session, get_runtime_context
from platform_runtime.schemas import RuleRunRequest, SchedulerRunRequest, SyncJobCreateRequest


def create_app() -> FastAPI:
    settings = load_platform_settings()
    app = FastAPI(title="Platform Runtime API", version="0.1.0")

    def ensure_permission(runtime: ResolvedRuntimeContext, permission_code: str) -> None:
        if "*" in runtime.permissions or permission_code in runtime.permissions:
            return
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Missing permission: {permission_code}")

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
        "provider_kind": integration.provider_kind,
        "provider_name": integration.provider_name,
        "display_name": integration.display_name,
        "status": integration.status,
        "sync_mode": integration.sync_mode,
        "connection_mode": integration.connection_mode,
        "last_sync_at": _serialize_datetime(integration.last_sync_at),
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
