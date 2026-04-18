from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from platform_core.exceptions import AuthorizationError, PlatformCoreError, TenantContextError
from platform_core.models import (
    Account,
    AccountUser,
    Alert,
    AuditLog,
    Integration,
    IntegrationCredential,
    IntegrationLog,
    Recommendation,
    Rule,
    RuleExecution,
    RuntimeLease,
    SyncJob,
    Task,
    User,
)
from platform_core.providers.adapters import (
    AvitoAdsProviderAdapter,
    GenericBankProviderAdapter,
    GoogleSheetsSpreadsheetProviderAdapter,
    MoySkladERPProviderAdapter,
    TelegramMessagingProviderAdapter,
    WhatsAppMessagingProviderAdapter,
)
from platform_core.providers.contracts import AdsLeadRecord, ProviderRegistry, SyncCursor
from platform_core.services.audit import AuditLogService
from platform_core.services.authz import AuthorizationService
from platform_core.services.automation import RuleEngineService
from platform_core.services.credentials import CredentialCrypto
from platform_core.services.provider_sync import AdsSyncService, BankSyncService, ERPSyncService
from platform_core.tenancy import TenantContext, require_account_id


def build_provider_registry() -> ProviderRegistry:
    return ProviderRegistry(
        [
            GenericBankProviderAdapter(),
            AvitoAdsProviderAdapter(),
            MoySkladERPProviderAdapter(),
            TelegramMessagingProviderAdapter(),
            WhatsAppMessagingProviderAdapter(),
            GoogleSheetsSpreadsheetProviderAdapter(),
        ]
    )


@dataclass(frozen=True)
class ResolvedRuntimeContext:
    context: TenantContext
    account: Account
    actor_user: User
    permissions: set[str]


@dataclass(frozen=True)
class JobExecutionResult:
    job_id: int
    status: str
    lease_acquired: bool
    message: str


class RuntimeContextService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._authz = AuthorizationService(session)

    def resolve(
        self,
        *,
        account_id: int | None,
        account_slug: str | None,
        actor_user_id: int | None,
        actor_email: str | None,
        source: str,
        request_id: str | None,
    ) -> ResolvedRuntimeContext:
        account = None
        if account_id is not None:
            account = self.session.execute(select(Account).where(Account.id == account_id)).scalar_one_or_none()
        elif account_slug:
            account = self.session.execute(select(Account).where(Account.slug == account_slug)).scalar_one_or_none()
        if account is None:
            raise TenantContextError("Account could not be resolved from runtime request.")

        actor = None
        if actor_user_id is not None:
            actor = self.session.execute(select(User).where(User.id == actor_user_id)).scalar_one_or_none()
        elif actor_email:
            actor = self.session.execute(select(User).where(User.email == actor_email)).scalar_one_or_none()
        if actor is None:
            raise AuthorizationError("Actor user could not be resolved from runtime request.")

        membership = self.session.execute(
            select(AccountUser).where(
                AccountUser.account_id == account.id,
                AccountUser.user_id == actor.id,
                AccountUser.status == "active",
            )
        ).scalar_one_or_none()
        if membership is None:
            raise AuthorizationError("Actor user is not a member of the selected account.")

        context = TenantContext(
            account_id=account.id,
            actor_user_id=actor.id,
            source=source,
            request_id=request_id,
            role_code=None,
            is_system=False,
        )
        permissions = self._authz.list_permissions(context)
        return ResolvedRuntimeContext(context=context, account=account, actor_user=actor, permissions=permissions)


class RuntimeLeaseService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def acquire(
        self,
        *,
        account_id: int,
        lease_key: str,
        owner: str,
        ttl_seconds: int,
        metadata: dict[str, object] | None = None,
    ) -> bool:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl_seconds)
        lease = self.session.execute(
            select(RuntimeLease).where(RuntimeLease.account_id == account_id, RuntimeLease.lease_key == lease_key)
        ).scalar_one_or_none()
        if lease is None:
            lease = RuntimeLease(
                account_id=account_id,
                lease_key=lease_key,
                owner=owner,
                expires_at=expires_at,
                metadata_json=metadata or {},
                heartbeat_at=now,
            )
            self.session.add(lease)
            try:
                self.session.flush()
                return True
            except IntegrityError:
                self.session.rollback()
                lease = self.session.execute(
                    select(RuntimeLease).where(RuntimeLease.account_id == account_id, RuntimeLease.lease_key == lease_key)
                ).scalar_one_or_none()
                if lease is None:
                    return False
        if self._dt(lease.expires_at) > now and lease.owner != owner:
            return False
        lease.owner = owner
        lease.expires_at = expires_at
        lease.metadata_json = metadata or lease.metadata_json
        lease.heartbeat_at = now
        self.session.flush()
        return True

    def release(self, *, account_id: int, lease_key: str, owner: str) -> None:
        lease = self.session.execute(
            select(RuntimeLease).where(RuntimeLease.account_id == account_id, RuntimeLease.lease_key == lease_key)
        ).scalar_one_or_none()
        if lease is None or lease.owner != owner:
            return
        self.session.delete(lease)
        self.session.flush()

    def _dt(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class RuntimeAutomationService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._audit = AuditLogService(session)

    def run_all_rules(self, context: TenantContext, *, now: datetime | None = None) -> list[dict[str, Any]]:
        results = RuleEngineService(self.session).evaluate_account(context, now=now)
        self._audit.log(
            context,
            "runtime.automation.run_all",
            "account",
            str(context.account_id),
            details={"result_count": len(results)},
        )
        return [result.__dict__ for result in results]

    def run_rule(self, context: TenantContext, rule_code: str, *, now: datetime | None = None) -> list[dict[str, Any]]:
        results = [
            item.__dict__
            for item in RuleEngineService(self.session).evaluate_account(context, now=now, rule_codes={rule_code})
        ]
        self._audit.log(
            context,
            "runtime.automation.run_rule",
            "rule",
            rule_code,
            details={"result_count": len(results)},
        )
        return results

    def list_alerts(self, context: TenantContext) -> list[Alert]:
        require_account_id(context)
        return self.session.execute(
            select(Alert).where(Alert.account_id == context.account_id).order_by(Alert.last_detected_at.desc(), Alert.id.desc())
        ).scalars().all()

    def list_tasks(self, context: TenantContext) -> list[Task]:
        require_account_id(context)
        return self.session.execute(
            select(Task).where(Task.account_id == context.account_id).order_by(Task.due_at.asc(), Task.id.desc())
        ).scalars().all()

    def list_recommendations(self, context: TenantContext) -> list[Recommendation]:
        require_account_id(context)
        return self.session.execute(
            select(Recommendation)
            .where(Recommendation.account_id == context.account_id)
            .order_by(Recommendation.created_at.desc(), Recommendation.id.desc())
        ).scalars().all()


class RuntimeIntegrationService:
    def __init__(self, session: Session, *, credentials_crypto: CredentialCrypto | None = None) -> None:
        self.session = session
        self._crypto = credentials_crypto or CredentialCrypto.from_settings()
        self._registry = build_provider_registry()
        self._audit = AuditLogService(session)

    def list_integrations(self, context: TenantContext) -> list[Integration]:
        require_account_id(context)
        return self.session.execute(
            select(Integration).where(Integration.account_id == context.account_id).order_by(Integration.id.asc())
        ).scalars().all()

    def enqueue_sync_job(
        self,
        context: TenantContext,
        *,
        integration_id: int,
        job_type: str = "full_sync",
        trigger_mode: str = "manual",
        idempotency_key: str,
        scope_json: dict[str, object] | None = None,
        scheduled_at: datetime | None = None,
    ) -> tuple[SyncJob, bool]:
        integration = self._get_integration(context.account_id, integration_id)
        existing = self.session.execute(
            select(SyncJob).where(SyncJob.account_id == context.account_id, SyncJob.idempotency_key == idempotency_key)
        ).scalar_one_or_none()
        if existing is not None:
            return existing, False
        job = SyncJob(
            account_id=context.account_id,
            integration_id=integration.id,
            provider_kind=integration.provider_kind,
            provider_name=integration.provider_name,
            job_type=job_type,
            trigger_mode=trigger_mode,
            status="pending",
            idempotency_key=idempotency_key,
            max_attempts=5,
            scheduled_at=scheduled_at or datetime.now(timezone.utc),
            scope_json=scope_json or {},
        )
        self.session.add(job)
        self.session.flush()
        self._log(
            context.account_id,
            integration.id,
            job.id,
            level="info",
            event_type="sync.enqueued",
            status="queued",
            message=f"Sync job {job.id} enqueued.",
            payload_json={"job_type": job_type, "idempotency_key": idempotency_key},
            request_id=context.request_id,
            provider_kind=integration.provider_kind,
            provider_name=integration.provider_name,
        )
        self._audit.log(
            context,
            "runtime.integrations.enqueue_sync",
            "sync_job",
            str(job.id),
            details={"integration_id": integration.id, "job_type": job_type, "idempotency_key": idempotency_key},
        )
        return job, True

    def list_sync_jobs(self, context: TenantContext) -> list[SyncJob]:
        return self.session.execute(
            select(SyncJob).where(SyncJob.account_id == context.account_id).order_by(SyncJob.id.desc())
        ).scalars().all()

    def list_logs(self, context: TenantContext) -> list[IntegrationLog]:
        return self.session.execute(
            select(IntegrationLog).where(IntegrationLog.account_id == context.account_id).order_by(IntegrationLog.id.desc())
        ).scalars().all()

    def claim_due_jobs(self, *, owner: str, ttl_seconds: int, limit: int = 10) -> list[SyncJob]:
        now = datetime.now(timezone.utc)
        pending_jobs = self.session.execute(
            select(SyncJob).where(
                SyncJob.status.in_(("pending", "retry")),
                SyncJob.scheduled_at <= now,
                SyncJob.attempts_count < SyncJob.max_attempts,
            ).order_by(SyncJob.scheduled_at.asc(), SyncJob.id.asc()).limit(limit)
        ).scalars().all()
        lease_service = RuntimeLeaseService(self.session)
        claimed: list[SyncJob] = []
        for job in pending_jobs:
            lease_key = f"sync_job:{job.id}"
            acquired = lease_service.acquire(
                account_id=job.account_id,
                lease_key=lease_key,
                owner=owner,
                ttl_seconds=ttl_seconds,
                metadata={"job_id": job.id},
            )
            if not acquired:
                continue
            if job.status == "running":
                continue
            job.status = "running"
            job.locked_by = owner
            job.started_at = now
            job.attempts_count += 1
            job.error_code = None
            job.error_message = None
            claimed.append(job)
        self.session.flush()
        return claimed

    def execute_job(self, job_id: int, *, owner: str, ttl_seconds: int) -> JobExecutionResult:
        job = self.session.execute(select(SyncJob).where(SyncJob.id == job_id)).scalar_one()
        lease_service = RuntimeLeaseService(self.session)
        lease_key = f"sync_job:{job.id}"
        acquired = lease_service.acquire(
            account_id=job.account_id,
            lease_key=lease_key,
            owner=owner,
            ttl_seconds=ttl_seconds,
            metadata={"job_id": job.id},
        )
        if not acquired:
            return JobExecutionResult(job_id=job.id, status=job.status, lease_acquired=False, message="Lease not acquired.")

        integration = self.session.execute(select(Integration).where(Integration.id == job.integration_id)).scalar_one()
        context = TenantContext(account_id=job.account_id, actor_user_id=None, source="worker", is_system=True)
        try:
            if job.status not in {"running", "pending", "retry"}:
                return JobExecutionResult(job_id=job.id, status=job.status, lease_acquired=True, message="Job already terminal.")

            previous_status = job.status
            job.status = "running"
            job.locked_by = owner
            job.started_at = job.started_at or datetime.now(timezone.utc)
            if job.attempts_count <= 0 or previous_status in {"pending", "retry"}:
                job.attempts_count += 1
            self._log(
                context.account_id,
                integration.id,
                job.id,
                level="info",
                event_type="sync.started",
                status="running",
                message=f"Sync job {job.id} started.",
                payload_json={"job_type": job.job_type},
                request_id=context.request_id,
                provider_kind=integration.provider_kind,
                provider_name=integration.provider_name,
            )

            result_payload = self._run_provider(integration, job)
            job.status = "completed"
            job.finished_at = datetime.now(timezone.utc)
            job.cursor_json = result_payload.get("cursor", {})
            integration.last_sync_at = job.finished_at
            self._log(
                context.account_id,
                integration.id,
                job.id,
                level="info",
                event_type="sync.completed",
                status="ok",
                message=f"Sync job {job.id} completed.",
                payload_json=result_payload,
                request_id=context.request_id,
                provider_kind=integration.provider_kind,
                provider_name=integration.provider_name,
            )
            self.session.flush()
            return JobExecutionResult(job_id=job.id, status="completed", lease_acquired=True, message="Job completed.")
        except Exception as exc:
            job.status = "retry" if job.attempts_count < job.max_attempts else "failed"
            job.finished_at = datetime.now(timezone.utc)
            if job.status == "retry":
                job.scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=min(300, 15 * max(1, job.attempts_count)))
            job.error_code = exc.__class__.__name__
            job.error_message = str(exc)
            self._log(
                context.account_id,
                integration.id,
                job.id,
                level="error",
                event_type="sync.failed",
                status=job.status,
                message=f"Sync job {job.id} failed: {exc}",
                payload_json={"error": str(exc), "job_type": job.job_type},
                request_id=context.request_id,
                provider_kind=integration.provider_kind,
                provider_name=integration.provider_name,
            )
            self.session.flush()
            return JobExecutionResult(job_id=job.id, status=job.status, lease_acquired=True, message=str(exc))
        finally:
            lease_service.release(account_id=job.account_id, lease_key=lease_key, owner=owner)

    def _run_provider(self, integration: Integration, job: SyncJob) -> dict[str, object]:
        settings = integration.settings_json or {}
        if settings.get("runtime_stub_mode") == "success":
            return {
                "mode": "stub",
                "provider_kind": integration.provider_kind,
                "provider_name": integration.provider_name,
                "job_type": job.job_type,
                "scope": job.scope_json,
                "cursor": {"job_id": job.id, "integration_id": integration.id},
            }

        credentials = self._load_credentials(integration)
        provider_input = {**credentials, "_settings": settings, "_job_scope": job.scope_json}
        adapter = self._registry.get(integration.provider_kind, integration.provider_name)
        if adapter is None:
            raise PlatformCoreError(
                f"Provider adapter is not registered for {integration.provider_kind}:{integration.provider_name}."
            )
        if integration.provider_kind == "banking":
            bank_service = BankSyncService(self.session)
            account_records = adapter.fetch_accounts(provider_input)  # type: ignore[arg-type]
            account_stats, bank_accounts = bank_service.sync_accounts(integration, account_records)
            balance_records, balance_cursor = adapter.fetch_balances(provider_input)  # type: ignore[arg-type]
            balance_stats = bank_service.sync_balances(integration, bank_accounts, balance_records)
            transaction_records, transaction_cursor = adapter.fetch_transactions(provider_input)  # type: ignore[arg-type]
            transaction_stats = bank_service.sync_transactions(integration, bank_accounts, transaction_records)
            return {
                "mode": "provider",
                "provider_name": integration.provider_name,
                "stats": {
                    "bank_accounts": account_stats.as_dict(),
                    "balances": balance_stats.as_dict(),
                    "transactions": transaction_stats.as_dict(),
                },
                "cursor": {
                    "balances": balance_cursor.value,
                    "transactions": transaction_cursor.value,
                },
            }
        if integration.provider_kind == "ads":
            ads_service = AdsSyncService(self.session)
            date_from, date_to = self._resolve_ads_window(job.scope_json, settings, job.cursor_json)

            campaign_stats = self._ads_section_stats(job, "campaigns")
            campaign_cursor_state = self._ads_section_state(job, "campaigns")
            if not self._ads_section_completed(job, "campaigns"):
                self._mark_ads_section_running(job, "campaigns", date_from=date_from, date_to=date_to)
                campaign_records, campaign_cursor = adapter.fetch_campaigns(  # type: ignore[arg-type]
                    provider_input,
                    cursor=self._cursor_for_section(job, "campaigns"),
                )
                campaign_sync_stats = ads_service.sync_campaigns(integration, campaign_records)
                campaign_stats = campaign_sync_stats.as_dict()
                campaign_cursor_state = self._checkpoint_ads_section(
                    job,
                    "campaigns",
                    cursor=campaign_cursor,
                    stats=campaign_stats,
                    date_from=date_from,
                    date_to=date_to,
                )

            metric_stats = self._ads_section_stats(job, "ad_metrics")
            metric_cursor_state = self._ads_section_state(job, "ad_metrics")
            if not self._ads_section_completed(job, "ad_metrics"):
                self._mark_ads_section_running(job, "ad_metrics", date_from=date_from, date_to=date_to)
                metric_records, metric_cursor = adapter.fetch_ad_metrics(  # type: ignore[arg-type]
                    provider_input,
                    date_from=date_from,
                    date_to=date_to,
                    cursor=self._cursor_for_section(job, "ad_metrics"),
                )
                metric_sync_stats = ads_service.sync_ad_metrics(integration, metric_records)
                metric_stats = metric_sync_stats.as_dict()
                metric_cursor_state = self._checkpoint_ads_section(
                    job,
                    "ad_metrics",
                    cursor=metric_cursor,
                    stats=metric_stats,
                    date_from=date_from,
                    date_to=date_to,
                )

            lead_cursor_state = self._ads_section_state(job, "leads")
            lead_stats = self._ads_section_stats(job, "leads")
            customer_stats = self._ads_nested_stats(job, "leads", "customers")
            lead_event_stats = self._ads_nested_stats(job, "leads", "lead_events")
            source_feed_state = self._ads_nested_state(job, "leads", "source_feed")
            if not self._ads_section_completed(job, "leads"):
                self._mark_ads_section_running(job, "leads", date_from=date_from, date_to=date_to)
                lead_records, lead_cursor = adapter.fetch_leads(  # type: ignore[arg-type]
                    provider_input,
                    date_from=date_from,
                    date_to=date_to,
                    cursor=self._cursor_for_section(job, "leads"),
                )
                lead_source_feed: dict[str, dict[str, object]] = {}
                fetch_lead_source_feed = getattr(adapter, "fetch_lead_source_feed", None)
                if callable(fetch_lead_source_feed):
                    lead_source_feed, source_feed_cursor = fetch_lead_source_feed(
                        provider_input,
                        date_from=date_from,
                        date_to=date_to,
                        cursor=self._cursor_for_nested_ads_state(job, "leads", "source_feed", "lead_source_feed"),
                    )
                    source_feed_state = self._build_ads_cursor_state(
                        source_feed_cursor,
                        date_from=date_from,
                        date_to=date_to,
                        extra={"linked_leads": len(lead_source_feed)},
                    )
                lead_records = self._enrich_ads_leads(  # type: ignore[arg-type]
                    adapter,
                    provider_input,
                    lead_records,
                    source_feed=lead_source_feed,
                )
                customer_sync_stats, lead_sync_stats, lead_event_sync_stats = ads_service.sync_leads(integration, lead_records)
                customer_stats = customer_sync_stats.as_dict()
                lead_stats = lead_sync_stats.as_dict()
                lead_event_stats = lead_event_sync_stats.as_dict()
                extra: dict[str, object] = {
                    "customers": customer_stats,
                    "lead_events": lead_event_stats,
                }
                if source_feed_state:
                    extra["source_feed"] = source_feed_state
                lead_cursor_state = self._checkpoint_ads_section(
                    job,
                    "leads",
                    cursor=lead_cursor,
                    stats=lead_stats,
                    date_from=date_from,
                    date_to=date_to,
                    extra=extra,
                )
            return {
                "mode": "provider",
                "provider_name": integration.provider_name,
                "stats": {
                    "campaigns": campaign_stats,
                    "ad_metrics": metric_stats,
                    "lead_source_feed": self._ads_source_feed_stats(source_feed_state),
                    "customers": customer_stats,
                    "leads": lead_stats,
                    "lead_events": lead_event_stats,
                },
                "cursor": {
                    "window": self._ads_window_payload(date_from, date_to),
                    "campaigns": campaign_cursor_state,
                    "ad_metrics": metric_cursor_state,
                    "leads": lead_cursor_state,
                },
                "window": {"date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
            }
        if integration.provider_kind == "erp":
            erp_service = ERPSyncService(self.session)
            product_records, product_cursor = adapter.fetch_products(provider_input)  # type: ignore[arg-type]
            product_stats = erp_service.sync_products(integration, product_records)
            stock_records, stock_cursor = adapter.fetch_stock(provider_input)  # type: ignore[arg-type]
            stock_stats = erp_service.sync_stock(integration, stock_records)
            movement_records, movement_cursor = adapter.fetch_movements(provider_input)  # type: ignore[arg-type]
            movement_stats = erp_service.sync_movements(integration, movement_records)
            purchase_records, purchase_cursor = adapter.fetch_purchases(provider_input)  # type: ignore[arg-type]
            purchase_stats = erp_service.sync_purchases(integration, purchase_records)
            return {
                "mode": "provider",
                "provider_name": integration.provider_name,
                "stats": {
                    "products": product_stats.as_dict(),
                    "stock": stock_stats.as_dict(),
                    "movements": movement_stats.as_dict(),
                    "purchases": purchase_stats.as_dict(),
                },
                "cursor": {
                    "products": product_cursor.value,
                    "stock": stock_cursor.value,
                    "movements": movement_cursor.value,
                    "purchases": purchase_cursor.value,
                },
            }
        if integration.provider_kind == "messaging":
            raise NotImplementedError("Runtime provider execution for messaging is not implemented yet.")
        if integration.provider_kind == "spreadsheet":
            raise NotImplementedError("Runtime provider execution for spreadsheet is not implemented yet.")
        raise PlatformCoreError(f"Unsupported provider kind: {integration.provider_kind}")

    def _cursor_for_section(self, job: SyncJob, section: str) -> SyncCursor | None:
        section_cursor = self._ads_section_state(job, section)
        if section_cursor.get("status") == "completed":
            return None
        next_cursor = self._string_or_none(section_cursor.get("next_cursor"))
        if next_cursor is None:
            return None
        section_key = self._ads_section_cursor_key(section)
        if section_key is None:
            return None
        return SyncCursor(value={section_key: next_cursor})
        
    def _ads_section_cursor_key(self, section: str) -> str | None:
        return {
            "campaigns": "campaigns",
            "ad_metrics": "metrics",
            "leads": "leads",
        }.get(section)

    def _ads_section_state(self, job: SyncJob, section: str) -> dict[str, object]:
        section_cursor = (job.cursor_json or {}).get(section)
        return dict(section_cursor) if isinstance(section_cursor, dict) else {}

    def _ads_section_completed(self, job: SyncJob, section: str) -> bool:
        return self._ads_section_state(job, section).get("status") == "completed"

    def _ads_section_stats(self, job: SyncJob, section: str) -> dict[str, int]:
        section_state = self._ads_section_state(job, section)
        stats = section_state.get("stats")
        if isinstance(stats, dict):
            return {
                "created": int(stats.get("created", 0)),
                "updated": int(stats.get("updated", 0)),
                "skipped": int(stats.get("skipped", 0)),
            }
        return {"created": 0, "updated": 0, "skipped": 0}

    def _ads_nested_stats(self, job: SyncJob, section: str, key: str) -> dict[str, int]:
        section_state = self._ads_section_state(job, section)
        nested = section_state.get(key)
        if isinstance(nested, dict):
            return {
                "created": int(nested.get("created", 0)),
                "updated": int(nested.get("updated", 0)),
                "skipped": int(nested.get("skipped", 0)),
            }
        return {"created": 0, "updated": 0, "skipped": 0}

    def _ads_nested_state(self, job: SyncJob, section: str, key: str) -> dict[str, object]:
        section_state = self._ads_section_state(job, section)
        nested = section_state.get(key)
        return dict(nested) if isinstance(nested, dict) else {}

    def _ads_source_feed_stats(self, state: dict[str, object]) -> dict[str, int | bool | None]:
        return {
            "record_count": int(state.get("record_count", 0)),
            "linked_leads": int(state.get("linked_leads", 0)),
            "exhausted": bool(state.get("exhausted", False)) if state else None,
        }

    def _ads_window_payload(self, date_from: date, date_to: date) -> dict[str, str]:
        return {"date_from": date_from.isoformat(), "date_to": date_to.isoformat()}

    def _build_ads_cursor_state(
        self,
        cursor: SyncCursor,
        *,
        date_from: date,
        date_to: date,
        status: str = "completed",
        extra: dict[str, object] | None = None,
    ) -> dict[str, object]:
        state = {
            "status": status,
            "next_cursor": self._string_or_none(cursor.value.get("next_cursor")),
            "record_count": int(cursor.value.get("record_count", 0)),
            "exhausted": bool(cursor.value.get("exhausted", False)),
            "window": self._ads_window_payload(date_from, date_to),
            "checkpoint_at": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            state.update(extra)
        return state

    def _mark_ads_section_running(self, job: SyncJob, section: str, *, date_from: date, date_to: date) -> None:
        cursor_json = dict(job.cursor_json or {})
        current_state = self._ads_section_state(job, section)
        current_state["status"] = "running"
        current_state["window"] = self._ads_window_payload(date_from, date_to)
        current_state["checkpoint_at"] = datetime.now(timezone.utc).isoformat()
        cursor_json["window"] = self._ads_window_payload(date_from, date_to)
        cursor_json[section] = current_state
        job.cursor_json = cursor_json
        self.session.flush()

    def _checkpoint_ads_section(
        self,
        job: SyncJob,
        section: str,
        *,
        cursor: SyncCursor,
        stats: dict[str, int],
        date_from: date,
        date_to: date,
        extra: dict[str, object] | None = None,
    ) -> dict[str, object]:
        cursor_json = dict(job.cursor_json or {})
        state = self._build_ads_cursor_state(cursor, date_from=date_from, date_to=date_to, extra={"stats": stats})
        if extra:
            state.update(extra)
        cursor_json["window"] = self._ads_window_payload(date_from, date_to)
        cursor_json[section] = state
        job.cursor_json = cursor_json
        self.session.flush()
        return state

    def _enrich_ads_leads(
        self,
        adapter: Any,
        provider_input: dict[str, object],
        lead_records: list[AdsLeadRecord],
        *,
        source_feed: dict[str, dict[str, object]] | None = None,
    ) -> list[AdsLeadRecord]:
        enriched: list[AdsLeadRecord] = []
        for record in lead_records:
            source_info = dict((source_feed or {}).get(record.external_id, {}))
            per_lead_info = adapter.fetch_lead_source_info(provider_input, record.external_id)
            if per_lead_info:
                source_info.update(per_lead_info)
            if not source_info:
                enriched.append(record)
                continue
            contact = source_info.get("contact") if isinstance(source_info.get("contact"), dict) else {}
            metadata = dict(record.metadata)
            metadata["source_info"] = source_info
            enriched.append(
                replace(
                    record,
                    status=self._string_or_default(source_info.get("status"), record.status),
                    pipeline_stage=self._string_or_default(
                        source_info.get("pipeline_stage") or source_info.get("stage"),
                        record.pipeline_stage,
                    ),
                    contact_name=self._string_or_none(source_info.get("contact_name") or contact.get("name")) or record.contact_name,
                    phone=self._string_or_none(source_info.get("phone") or contact.get("phone")) or record.phone,
                    email=self._string_or_none(source_info.get("email") or contact.get("email")) or record.email,
                    campaign_external_id=self._string_or_none(
                        source_info.get("campaign_external_id") or source_info.get("campaign_id")
                    )
                    or record.campaign_external_id,
                    customer_external_id=self._string_or_none(
                        source_info.get("customer_external_id")
                        or source_info.get("contact_id")
                        or source_info.get("customer_id")
                    )
                    or record.customer_external_id,
                    first_response_due_at=self._datetime_or_default(
                        source_info.get("first_response_due_at") or source_info.get("response_due_at"),
                        record.first_response_due_at,
                    ),
                    first_responded_at=self._datetime_or_default(
                        source_info.get("first_responded_at")
                        or source_info.get("first_response_at")
                        or source_info.get("responded_at"),
                        record.first_responded_at,
                    ),
                    lost_reason=self._string_or_none(source_info.get("lost_reason")) or record.lost_reason,
                    metadata=metadata,
                )
            )
        return enriched

    def _cursor_for_nested_ads_state(
        self,
        job: SyncJob,
        section: str,
        nested_key: str,
        cursor_key: str,
    ) -> SyncCursor | None:
        nested_state = self._ads_nested_state(job, section, nested_key)
        next_cursor = self._string_or_none(nested_state.get("next_cursor"))
        if next_cursor is None:
            return None
        return SyncCursor(value={cursor_key: next_cursor})

    def _string_or_none(self, value: object | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    def _string_or_default(self, value: object | None, default: str) -> str:
        return self._string_or_none(value) or default

    def _datetime_or_default(self, value: object | None, default: datetime | None) -> datetime | None:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        parsed = self._string_or_none(value)
        if parsed is None:
            return default
        normalized = parsed.replace("Z", "+00:00")
        result = datetime.fromisoformat(normalized)
        if result.tzinfo is None:
            return result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)

    def _load_credentials(self, integration: Integration) -> dict[str, object]:
        credential = self.session.execute(
            select(IntegrationCredential).where(
                IntegrationCredential.account_id == integration.account_id,
                IntegrationCredential.integration_id == integration.id,
                IntegrationCredential.status == "active",
            ).order_by(IntegrationCredential.version.desc())
        ).scalars().first()
        if credential is None:
            return {}
        return self._crypto.decrypt_mapping(credential.secret_ciphertext)

    def _get_integration(self, account_id: int, integration_id: int) -> Integration:
        integration = self.session.execute(
            select(Integration).where(Integration.account_id == account_id, Integration.id == integration_id)
        ).scalar_one_or_none()
        if integration is None:
            raise TenantContextError("Integration not found in selected account.")
        return integration

    def _resolve_ads_window(
        self,
        scope_json: dict[str, object] | None,
        settings_json: dict[str, object] | None,
        cursor_json: dict[str, object] | None = None,
    ) -> tuple[date, date]:
        scope = scope_json or {}
        settings = settings_json or {}
        cursor = cursor_json or {}
        window = cursor.get("window") if isinstance(cursor.get("window"), dict) else {}
        today = datetime.now(timezone.utc).date()
        date_from_raw = scope.get("date_from") or window.get("date_from") or settings.get("default_date_from")
        date_to_raw = scope.get("date_to") or window.get("date_to") or settings.get("default_date_to")
        if isinstance(date_from_raw, str):
            date_from = date.fromisoformat(date_from_raw)
        else:
            lookback_days = int(scope.get("lookback_days") or settings.get("lookback_days") or 1)
            date_from = today - timedelta(days=max(0, lookback_days - 1))
        if isinstance(date_to_raw, str):
            date_to = date.fromisoformat(date_to_raw)
        else:
            date_to = today
        if date_to < date_from:
            raise PlatformCoreError("Ads sync window is invalid: date_to is earlier than date_from.")
        return date_from, date_to

    def _log(
        self,
        account_id: int,
        integration_id: int,
        sync_job_id: int | None,
        *,
        level: str,
        event_type: str,
        status: str,
        message: str,
        payload_json: dict[str, object],
        request_id: str | None,
        provider_kind: str,
        provider_name: str,
    ) -> None:
        self.session.add(
            IntegrationLog(
                account_id=account_id,
                integration_id=integration_id,
                sync_job_id=sync_job_id,
                level=level,
                event_type=event_type,
                status=status,
                provider_kind=provider_kind,
                provider_name=provider_name,
                request_id=request_id,
                message=message,
                payload_json=payload_json,
            )
        )
        self.session.flush()


class SchedulerService:
    def __init__(self, session: Session, *, worker_id: str, lease_ttl_seconds: int) -> None:
        self.session = session
        self.worker_id = worker_id
        self.lease_ttl_seconds = lease_ttl_seconds
        self._lease_service = RuntimeLeaseService(session)
        self._audit = AuditLogService(session)

    def run_once(self, *, run_rules: bool = True, run_sync_jobs: bool = True) -> dict[str, object]:
        rule_runs = self._run_rules_once() if run_rules else 0
        sync_runs = self._run_sync_jobs_once() if run_sync_jobs else 0
        self.session.flush()
        return {"rule_accounts_processed": rule_runs, "sync_jobs_processed": sync_runs}

    def _run_rules_once(self) -> int:
        processed = 0
        account_ids = self.session.execute(select(Account.id).where(Account.status == "active").order_by(Account.id.asc())).scalars().all()
        for account_id in account_ids:
            lease_key = f"rules:{account_id}"
            acquired = self._lease_service.acquire(
                account_id=account_id,
                lease_key=lease_key,
                owner=self.worker_id,
                ttl_seconds=self.lease_ttl_seconds,
                metadata={"scheduler": "rules"},
            )
            if not acquired:
                continue
            try:
                context = TenantContext(account_id=account_id, actor_user_id=None, source="scheduler", is_system=True)
                results = RuleEngineService(self.session).evaluate_account(context)
                self._audit.log(
                    context,
                    "scheduler.rules.run",
                    "account",
                    str(account_id),
                    details={"result_count": len(results)},
                )
                processed += 1
            finally:
                self._lease_service.release(account_id=account_id, lease_key=lease_key, owner=self.worker_id)
        return processed

    def _run_sync_jobs_once(self) -> int:
        runtime = RuntimeIntegrationService(self.session)
        jobs = runtime.claim_due_jobs(owner=self.worker_id, ttl_seconds=self.lease_ttl_seconds)
        processed = 0
        for job in jobs:
            runtime.execute_job(job.id, owner=self.worker_id, ttl_seconds=self.lease_ttl_seconds)
            processed += 1
        return processed


class AdminQueryService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_audit_logs(self, account_id: int, limit: int = 100) -> list[AuditLog]:
        return self.session.execute(
            select(AuditLog).where(AuditLog.account_id == account_id).order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).limit(limit)
        ).scalars().all()

    def recent_failed_sync_jobs(self, account_id: int, limit: int = 20) -> list[SyncJob]:
        return self.session.execute(
            select(SyncJob)
            .where(
                SyncJob.account_id == account_id,
                SyncJob.status.in_(("retry", "failed")),
            )
            .order_by(SyncJob.finished_at.desc(), SyncJob.id.desc())
            .limit(limit)
        ).scalars().all()

    def recent_failed_rule_runs(self, account_id: int, limit: int = 20) -> list[RuleExecution]:
        return self.session.execute(
            select(RuleExecution)
            .where(
                RuleExecution.account_id == account_id,
                (RuleExecution.error_message.is_not(None)) | (RuleExecution.status.in_(("failed", "error"))),
            )
            .order_by(RuleExecution.updated_at.desc(), RuleExecution.id.desc())
            .limit(limit)
        ).scalars().all()

    def overdue_tasks(self, account_id: int, *, now: datetime | None = None, limit: int = 50) -> list[Task]:
        effective_now = now or datetime.now(timezone.utc)
        return self.session.execute(
            select(Task)
            .where(
                Task.account_id == account_id,
                Task.status.notin_(["done", "completed", "cancelled"]),
                Task.completed_at.is_(None),
                Task.due_at.is_not(None),
                Task.due_at < effective_now,
            )
            .order_by(Task.due_at.asc(), Task.id.desc())
            .limit(limit)
        ).scalars().all()

    def active_critical_alerts(
        self,
        account_id: int,
        *,
        codes: set[str] | None = None,
        limit: int = 50,
    ) -> list[Alert]:
        critical_codes = codes or {
            "bank.balance_below_safe_threshold",
            "inventory.stock_below_threshold",
            "lead.no_first_response",
            "marketing.cpl_above_threshold",
            "leads.lost_above_threshold",
            "task.overdue_escalation",
        }
        return self.session.execute(
            select(Alert)
            .where(
                Alert.account_id == account_id,
                Alert.status == "open",
                Alert.code.in_(sorted(critical_codes)),
            )
            .order_by(Alert.last_detected_at.desc(), Alert.id.desc())
            .limit(limit)
        ).scalars().all()

    def integration_sync_status(self, account_id: int) -> list[dict[str, object]]:
        integrations = self.session.execute(
            select(Integration).where(Integration.account_id == account_id).order_by(Integration.id.asc())
        ).scalars().all()
        rows: list[dict[str, object]] = []
        for integration in integrations:
            latest_success = self.session.execute(
                select(SyncJob)
                .where(
                    SyncJob.account_id == account_id,
                    SyncJob.integration_id == integration.id,
                    SyncJob.status == "completed",
                )
                .order_by(SyncJob.finished_at.desc(), SyncJob.id.desc())
                .limit(1)
            ).scalars().first()
            latest_failure = self.session.execute(
                select(SyncJob)
                .where(
                    SyncJob.account_id == account_id,
                    SyncJob.integration_id == integration.id,
                    SyncJob.status.in_(("retry", "failed")),
                )
                .order_by(SyncJob.finished_at.desc(), SyncJob.id.desc())
                .limit(1)
            ).scalars().first()
            rows.append(
                {
                    "integration": integration,
                    "latest_success": latest_success,
                    "latest_failure": latest_failure,
                }
            )
        return rows

    def ops_summary(self, account_id: int, *, now: datetime | None = None) -> dict[str, object]:
        effective_now = now or datetime.now(timezone.utc)
        return {
            "generated_at": effective_now.astimezone(timezone.utc).isoformat(),
            "recent_failed_sync_jobs": self.recent_failed_sync_jobs(account_id, limit=10),
            "recent_failed_rule_runs": self.recent_failed_rule_runs(account_id, limit=10),
            "overdue_tasks": self.overdue_tasks(account_id, now=effective_now, limit=20),
            "active_critical_alerts": self.active_critical_alerts(account_id, limit=20),
            "integration_sync_status": self.integration_sync_status(account_id),
        }
