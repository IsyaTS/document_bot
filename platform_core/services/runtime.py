from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone, timedelta
import logging
from time import perf_counter
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
from platform_core.telegram_accounts import TelegramClientUnavailableError, TelegramQrLoginError, describe_session_sync
from platform_core.services.audit import AuditLogService
from platform_core.services.authz import AuthorizationService
from platform_core.services.automation import RuleEngineService
from platform_core.services.credentials import CredentialCrypto
from platform_core.services.provider_sync import AdsSyncService, BankSyncService, ERPSyncService
from platform_core.settings import load_platform_settings
from platform_core.tenancy import TenantContext, require_account_id


logger = logging.getLogger(__name__)


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
    membership: AccountUser
    role_code: str
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
        role_code = membership.role.code if membership.role is not None else ""
        return ResolvedRuntimeContext(
            context=context,
            account=account,
            actor_user=actor,
            membership=membership,
            role_code=role_code,
            permissions=permissions,
        )


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

    def supports_sync(self, provider_kind: str, provider_name: str) -> bool:
        del provider_name
        return provider_kind in {"banking", "ads", "erp"}

    def create_integration(
        self,
        context: TenantContext,
        *,
        provider_kind: str,
        provider_name: str,
        display_name: str,
        external_ref: str | None = None,
        status: str = "active",
        connection_mode: str = "polling",
        sync_mode: str = "manual",
        settings_json: dict[str, object] | None = None,
    ) -> Integration:
        require_account_id(context)
        adapter = self._registry.get(provider_kind, provider_name)
        if adapter is None:
            raise PlatformCoreError(f"Unsupported provider: {provider_kind}:{provider_name}.")
        integration = Integration(
            account_id=context.account_id,
            provider_kind=provider_kind,
            provider_name=provider_name,
            external_ref=external_ref.strip() if isinstance(external_ref, str) and external_ref.strip() else None,
            display_name=display_name.strip(),
            status=status,
            connection_mode=connection_mode,
            sync_mode=sync_mode,
            settings_json=settings_json or {},
        )
        self.session.add(integration)
        self.session.flush()
        self._audit.log(
            context,
            "runtime.integrations.create",
            "integration",
            str(integration.id),
            details={"provider_kind": provider_kind, "provider_name": provider_name, "display_name": display_name},
        )
        return integration

    def update_integration(
        self,
        context: TenantContext,
        *,
        integration_id: int,
        display_name: str | None = None,
        external_ref: str | None = None,
        status: str | None = None,
        connection_mode: str | None = None,
        sync_mode: str | None = None,
        settings_json: dict[str, object] | None = None,
    ) -> Integration:
        integration = self._get_integration(context.account_id, integration_id)
        if display_name is not None:
            integration.display_name = display_name.strip()
        if external_ref is not None:
            integration.external_ref = external_ref.strip() or None
        if status is not None:
            integration.status = status
        if connection_mode is not None:
            integration.connection_mode = connection_mode
        if sync_mode is not None:
            integration.sync_mode = sync_mode
        if settings_json is not None:
            integration.settings_json = settings_json
        self.session.flush()
        self._audit.log(
            context,
            "runtime.integrations.update",
            "integration",
            str(integration.id),
            details={"status": integration.status, "sync_mode": integration.sync_mode},
        )
        return integration

    def set_integration_status(
        self,
        context: TenantContext,
        *,
        integration_id: int,
        status: str,
    ) -> Integration:
        if status not in {"active", "disabled", "archived"}:
            raise PlatformCoreError(f"Unsupported integration status: {status}.")
        return self.update_integration(context, integration_id=integration_id, status=status)

    def save_credentials(
        self,
        context: TenantContext,
        *,
        integration_id: int,
        secret_payload: dict[str, object],
        credential_type: str = "primary",
        replace_mode: str = "merge",
    ) -> IntegrationCredential:
        integration = self._get_integration(context.account_id, integration_id)
        existing = self._active_credential(integration.id)
        if replace_mode not in {"merge", "replace"}:
            raise PlatformCoreError("replace_mode must be either 'merge' or 'replace'.")
        current_payload = self._crypto.decrypt_mapping(existing.secret_ciphertext) if existing is not None else {}
        merged = (
            self._merge_credentials(current_payload, secret_payload)
            if replace_mode == "merge"
            else self._merge_credentials({}, secret_payload)
        )
        if not merged:
            raise PlatformCoreError("Credential payload is empty after merge.")
        ciphertext, fingerprint = self._crypto.encrypt_mapping(merged)
        if existing is not None:
            existing.status = "rotated"
        next_version = (existing.version + 1) if existing is not None else 1
        credential = IntegrationCredential(
            account_id=context.account_id,
            integration_id=integration.id,
            credential_type=credential_type,
            status="active",
            version=next_version,
            secret_ciphertext=ciphertext,
            secret_fingerprint=fingerprint,
            metadata_json={"masked_keys": sorted(merged.keys())},
            last_rotated_at=datetime.now(timezone.utc),
        )
        self.session.add(credential)
        self.session.flush()
        self._audit.log(
            context,
            "runtime.integrations.credentials.save",
            "integration",
            str(integration.id),
            details={"credential_type": credential_type, "version": credential.version, "replace_mode": replace_mode},
        )
        return credential

    def clear_credentials(
        self,
        context: TenantContext,
        *,
        integration_id: int,
        credential_type: str = "primary",
    ) -> None:
        integration = self._get_integration(context.account_id, integration_id)
        active = self._active_credential(integration.id)
        if active is None:
            return
        active.status = "cleared"
        self.session.flush()
        self._audit.log(
            context,
            "runtime.integrations.credentials.clear",
            "integration",
            str(integration.id),
            details={"credential_type": credential_type, "cleared_version": active.version},
        )

    def test_connection(
        self,
        context: TenantContext,
        *,
        integration_id: int,
        override_payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        integration = self._get_integration(context.account_id, integration_id)
        credentials = self._load_credentials(integration)
        if override_payload:
            credentials = self._merge_credentials(credentials, override_payload)
        if not credentials:
            raise PlatformCoreError("Credentials are not configured for this integration.")
        adapter = self._registry.get(integration.provider_kind, integration.provider_name)
        if adapter is None:
            raise PlatformCoreError(
                f"Provider adapter is not registered for {integration.provider_kind}:{integration.provider_name}."
            )
        try:
            if integration.provider_kind == "banking":
                if hasattr(adapter, "connect_account"):
                    result = adapter.connect_account(credentials)  # type: ignore[attr-defined]
                    return {"connected": True, "provider": integration.provider_name, "details": result}
                accounts = adapter.fetch_accounts(credentials)  # type: ignore[attr-defined]
                return {"connected": True, "provider": integration.provider_name, "details": {"accounts_found": len(accounts)}}
            if integration.provider_kind == "ads":
                if hasattr(adapter, "connect_account"):
                    result = adapter.connect_account(credentials)  # type: ignore[attr-defined]
                    return {"connected": True, "provider": integration.provider_name, "details": result}
                campaigns, _ = adapter.fetch_campaigns(credentials)  # type: ignore[attr-defined]
                return {
                    "connected": True,
                    "provider": integration.provider_name,
                    "details": {"campaigns_sampled": len(campaigns)},
                }
            if integration.provider_kind == "erp":
                if hasattr(adapter, "connect_account"):
                    result = adapter.connect_account(credentials)  # type: ignore[attr-defined]
                    return {"connected": True, "provider": integration.provider_name, "details": result}
                products, _ = adapter.fetch_products(credentials)  # type: ignore[attr-defined]
                return {
                    "connected": True,
                    "provider": integration.provider_name,
                    "details": {"products_sampled": len(products)},
                }
            if integration.provider_kind == "messaging":
                if integration.provider_name == "telegram":
                    settings = load_platform_settings()
                    session_string = str(credentials.get("session_string") or "").strip()
                    if not settings.telegram_api_id or not settings.telegram_api_hash:
                        raise ValueError("PLATFORM_TELEGRAM_API_ID and PLATFORM_TELEGRAM_API_HASH are required.")
                    if not session_string:
                        raise ValueError("Telegram account is not connected yet. Scan the QR code first.")
                    identity = describe_session_sync(
                        api_id=settings.telegram_api_id,
                        api_hash=settings.telegram_api_hash,
                        session_string=session_string,
                    )
                    return {
                        "connected": True,
                        "provider": integration.provider_name,
                        "details": {
                            "mode": "user_session",
                            "send_path": "saved_messages",
                            "telegram_user_id": identity.user_id,
                            "telegram_username": identity.username,
                            "telegram_display_name": identity.display_name,
                            "telegram_phone": identity.phone,
                        },
                    }
                if integration.provider_name == "whatsapp":
                    api_token = str(credentials.get("api_token") or "").strip()
                    phone_number_id = str(credentials.get("phone_number_id") or "").strip()
                    if not api_token or not phone_number_id:
                        raise ValueError("WhatsApp credentials require api_token and phone_number_id.")
                    return {
                        "connected": True,
                        "provider": integration.provider_name,
                        "details": {"mode": "webhook_only", "send_path": "graph_api"},
                    }
                return {
                    "connected": True,
                    "provider": integration.provider_name,
                    "details": {"mode": "webhook_only"},
                }
            if integration.provider_kind == "spreadsheet":
                raise NotImplementedError("Spreadsheet connection test is not implemented yet.")
        except Exception as exc:
            return {
                "connected": False,
                "provider": integration.provider_name,
                "error_code": exc.__class__.__name__,
                "message": str(exc),
            }
        return {"connected": True, "provider": integration.provider_name, "details": {"mode": "noop"}}

    def integration_setup_payload(self, context: TenantContext, *, integration_id: int) -> dict[str, object]:
        integration = self._get_integration(context.account_id, integration_id)
        credential = self._active_credential(integration.id)
        masked_credentials = self._masked_credential_summary(credential)
        latest_jobs = self.session.execute(
            select(SyncJob)
            .where(SyncJob.account_id == context.account_id, SyncJob.integration_id == integration.id)
            .order_by(SyncJob.id.desc())
            .limit(10)
        ).scalars().all()
        latest_logs = self.session.execute(
            select(IntegrationLog)
            .where(IntegrationLog.account_id == context.account_id, IntegrationLog.integration_id == integration.id)
            .order_by(IntegrationLog.id.desc())
            .limit(10)
        ).scalars().all()
        return {
            "integration": integration,
            "masked_credentials": masked_credentials,
            "credential_version": credential.version if credential is not None else None,
            "credential_last_rotated_at": credential.last_rotated_at if credential is not None else None,
            "latest_jobs": latest_jobs,
            "latest_logs": latest_logs,
        }

    def credentials_payload(self, context: TenantContext, *, integration_id: int) -> dict[str, object]:
        integration = self._get_integration(context.account_id, integration_id)
        return self._load_credentials(integration)

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
        if not self.supports_sync(integration.provider_kind, integration.provider_name):
            if integration.provider_kind == "messaging":
                raise PlatformCoreError("Messaging integrations are webhook-driven and do not support manual sync jobs.")
            raise PlatformCoreError(
                f"Manual sync jobs are not supported for provider {integration.provider_kind}:{integration.provider_name}."
            )
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
        if integration.provider_kind == "erp" and job.job_type == "full_sync":
            previous_cursors = self.session.execute(
                select(SyncJob.cursor_json)
                .where(
                    SyncJob.account_id == context.account_id,
                    SyncJob.integration_id == integration.id,
                    SyncJob.cursor_json.is_not(None),
                )
                .order_by(SyncJob.id.desc())
                .limit(10)
            ).scalars().all()
            for previous_cursor in previous_cursors:
                if isinstance(previous_cursor, dict) and previous_cursor:
                    job.cursor_json = previous_cursor
                    break
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
            select(SyncJob)
            .join(Integration, Integration.id == SyncJob.integration_id)
            .where(
                SyncJob.status.in_(("pending", "retry")),
                SyncJob.scheduled_at <= now,
                SyncJob.attempts_count < SyncJob.max_attempts,
                Integration.status == "active",
            )
            .order_by(SyncJob.scheduled_at.asc(), SyncJob.id.asc())
            .limit(limit)
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
        lease_service = RuntimeLeaseService(self.session)
        job_account_id: int | None = None
        integration_id: int | None = None
        lease_key: str | None = None
        acquired = False
        attempt_after_start = 0
        try:
            job = self.session.execute(select(SyncJob).where(SyncJob.id == job_id)).scalar_one_or_none()
            if job is None:
                logger.error(
                    "runtime_sync_job_result tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s error_code=%s error_message=%s",
                    "unknown",
                    "unknown",
                    "unknown",
                    job_id,
                    "load_job",
                    "failed",
                    "SyncJobNotFound",
                    "Sync job row was not found.",
                )
                return JobExecutionResult(
                    job_id=job_id,
                    status="failed",
                    lease_acquired=False,
                    message="Sync job row was not found.",
                )

            job_account_id = job.account_id
            integration_id = job.integration_id
            attempt_after_start = job.attempts_count
            lease_key = f"sync_job:{job.id}"
            acquired = lease_service.acquire(
                account_id=job_account_id,
                lease_key=lease_key,
                owner=owner,
                ttl_seconds=ttl_seconds,
                metadata={"job_id": job.id},
            )
            if not acquired:
                logger.info(
                    "runtime_sync_job_result tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s error_code=%s error_message=%s",
                    job.account_id,
                    job.integration_id,
                    f"{job.provider_kind}:{job.provider_name}",
                    job.id,
                    "acquire_lease",
                    "skipped",
                    "",
                    "Lease not acquired.",
                )
                return JobExecutionResult(job_id=job.id, status=job.status, lease_acquired=False, message="Lease not acquired.")

            integration = self.session.execute(select(Integration).where(Integration.id == job.integration_id)).scalar_one_or_none()
            context = TenantContext(account_id=job.account_id, actor_user_id=None, source="worker", is_system=True)
            if integration is None:
                job.status = "failed"
                job.finished_at = datetime.now(timezone.utc)
                job.locked_by = owner
                job.error_code = "IntegrationNotFound"
                job.error_message = "Integration row was not found for sync job."
                logger.error(
                    "runtime_sync_job_result tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s error_code=%s error_message=%s",
                    job.account_id,
                    job.integration_id,
                    f"{job.provider_kind}:{job.provider_name}",
                    job.id,
                    "load_integration",
                    "failed",
                    job.error_code,
                    job.error_message,
                )
                self.session.flush()
                return JobExecutionResult(
                    job_id=job.id,
                    status="failed",
                    lease_acquired=True,
                    message=job.error_message,
                )

            logger.info(
                "runtime_sync_job_result tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s error_code=%s error_message=%s",
                context.account_id,
                integration.id,
                f"{integration.provider_kind}:{integration.provider_name}",
                job.id,
                "load_integration",
                "ok",
                "",
                "",
            )
            if job.status not in {"running", "pending", "retry"}:
                return JobExecutionResult(job_id=job.id, status=job.status, lease_acquired=True, message="Job already terminal.")

            previous_status = job.status
            job.status = "running"
            job.locked_by = owner
            job.started_at = job.started_at or datetime.now(timezone.utc)
            if job.attempts_count <= 0 or previous_status in {"pending", "retry"}:
                job.attempts_count += 1
            attempt_after_start = job.attempts_count
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
            logger.info(
                "runtime_sync_job_result tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s error_code=%s error_message=%s",
                context.account_id,
                integration.id,
                f"{integration.provider_kind}:{integration.provider_name}",
                job.id,
                "provider_sync",
                "running",
                "",
                "",
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
            logger.info(
                "runtime_sync_job_result tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s error_code=%s error_message=%s",
                context.account_id,
                integration.id,
                f"{integration.provider_kind}:{integration.provider_name}",
                job.id,
                "provider_sync",
                "completed",
                "",
                "",
            )
            self.session.flush()
            return JobExecutionResult(job_id=job.id, status="completed", lease_acquired=True, message="Job completed.")
        except Exception as exc:
            self.session.rollback()
            job = self.session.execute(select(SyncJob).where(SyncJob.id == job_id)).scalar_one_or_none()
            integration = (
                self.session.execute(select(Integration).where(Integration.id == integration_id)).scalar_one_or_none()
                if integration_id is not None
                else None
            )
            if job is None:
                logger.error(
                    "runtime_sync_job_result tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s error_code=%s error_message=%s",
                    job_account_id if job_account_id is not None else "unknown",
                    integration_id if integration_id is not None else "unknown",
                    (
                        f"{integration.provider_kind}:{integration.provider_name}"
                        if integration is not None
                        else "unknown"
                    ),
                    job_id,
                    "exception_recovery",
                    "failed",
                    exc.__class__.__name__,
                    str(exc),
                )
                return JobExecutionResult(job_id=job_id, status="failed", lease_acquired=acquired, message=str(exc))
            job.attempts_count = max(job.attempts_count, attempt_after_start)
            job.status = "retry" if job.attempts_count < job.max_attempts else "failed"
            job.finished_at = datetime.now(timezone.utc)
            job.locked_by = owner
            if job.status == "retry":
                job.scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=min(300, 15 * max(1, job.attempts_count)))
            job.error_code = exc.__class__.__name__
            job.error_message = str(exc)
            provider_value = (
                f"{integration.provider_kind}:{integration.provider_name}"
                if integration is not None
                else f"{job.provider_kind}:{job.provider_name}"
            )
            tenant_value = job.account_id
            if integration is not None:
                failure_context = TenantContext(account_id=job.account_id, actor_user_id=None, source="worker", is_system=True)
                self._log(
                    failure_context.account_id,
                    integration.id,
                    job.id,
                    level="error",
                    event_type="sync.failed",
                    status=job.status,
                    message=f"Sync job {job.id} failed: {exc}",
                    payload_json={"error": str(exc), "job_type": job.job_type},
                    request_id=failure_context.request_id,
                    provider_kind=integration.provider_kind,
                    provider_name=integration.provider_name,
                )
            logger.error(
                "runtime_sync_job_result tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s error_code=%s error_message=%s",
                tenant_value,
                job.integration_id,
                provider_value,
                job.id,
                "provider_sync",
                job.status,
                job.error_code,
                job.error_message,
            )
            self.session.flush()
            return JobExecutionResult(job_id=job.id, status=job.status, lease_acquired=True, message=str(exc))
        finally:
            if acquired and job_account_id is not None and lease_key is not None:
                lease_service.release(account_id=job_account_id, lease_key=lease_key, owner=owner)

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
            provider_value = f"{integration.provider_kind}:{integration.provider_name}"
            movement_cursor_state = (job.cursor_json or {}).get("movements")
            movement_cursor = SyncCursor(value=dict(movement_cursor_state)) if isinstance(movement_cursor_state, dict) else None

            def _erp_stage_log(
                stage: str,
                *,
                result: str,
                duration_ms: int | None = None,
                records_count: int | None = None,
                error_code: str | None = None,
                error_message: str | None = None,
            ) -> None:
                logger.info(
                    "runtime_erp_sync_stage tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s duration_ms=%s records_count=%s error_code=%s error_message=%s",
                    integration.account_id,
                    integration.id,
                    provider_value,
                    job.id,
                    stage,
                    result,
                    duration_ms if duration_ms is not None else "",
                    records_count if records_count is not None else "",
                    error_code or "",
                    error_message or "",
                )

            def _run_erp_stage(stage_name: str, operation):
                started = perf_counter()
                _erp_stage_log(stage_name, result="start")
                try:
                    result = operation()
                except Exception as exc:
                    _erp_stage_log(
                        stage_name,
                        result="failed",
                        duration_ms=int((perf_counter() - started) * 1000),
                        error_code=exc.__class__.__name__,
                        error_message=str(exc),
                    )
                    raise
                records_count = None
                if isinstance(result, tuple) and result:
                    first = result[0]
                    if isinstance(first, list):
                        records_count = len(first)
                elif hasattr(result, "as_dict"):
                    stats_payload = result.as_dict()
                    records_count = int(stats_payload.get("created", 0)) + int(stats_payload.get("updated", 0)) + int(stats_payload.get("skipped", 0))
                _erp_stage_log(
                    stage_name,
                    result="done",
                    duration_ms=int((perf_counter() - started) * 1000),
                    records_count=records_count,
                )
                return result

            product_records, product_cursor = _run_erp_stage(
                "fetch_products",
                lambda: adapter.fetch_products(provider_input),  # type: ignore[arg-type]
            )
            product_stats = _run_erp_stage(
                "sync_products",
                lambda: erp_service.sync_products(integration, product_records),
            )
            stock_records, stock_cursor = _run_erp_stage(
                "fetch_stock",
                lambda: adapter.fetch_stock(provider_input),  # type: ignore[arg-type]
            )
            stock_stats = _run_erp_stage(
                "sync_stock",
                lambda: erp_service.sync_stock(integration, stock_records),
            )
            if movement_cursor is not None:
                logger.info(
                    "runtime_erp_movement_cursor tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s cursor=%s",
                    integration.account_id,
                    integration.id,
                    provider_value,
                    job.id,
                    "fetch_movements",
                    "start",
                    movement_cursor.value,
                )
            movement_records, movement_cursor = _run_erp_stage(
                "fetch_movements",
                lambda: adapter.fetch_movements(provider_input, cursor=movement_cursor),  # type: ignore[arg-type]
            )
            movement_stats = _run_erp_stage(
                "sync_movements",
                lambda: erp_service.sync_movements(integration, movement_records),
            )
            cursor_json = dict(job.cursor_json or {})
            cursor_json["movements"] = movement_cursor.value
            job.cursor_json = cursor_json
            logger.info(
                "runtime_erp_movement_cursor tenant_id=%s integration_id=%s provider=%s sync_job_id=%s stage=%s result=%s fetched_count=%s next_cursor=%s completed_window=%s",
                integration.account_id,
                integration.id,
                provider_value,
                job.id,
                "sync_movements",
                "done",
                movement_cursor.value.get("demand_documents_fetched", ""),
                movement_cursor.value.get("next_cursor", ""),
                movement_cursor.value.get("completed_window", ""),
            )
            self.session.flush()
            purchase_records, purchase_cursor = _run_erp_stage(
                "fetch_purchases",
                lambda: adapter.fetch_purchases(provider_input),  # type: ignore[arg-type]
            )
            purchase_stats = _run_erp_stage(
                "sync_purchases",
                lambda: erp_service.sync_purchases(integration, purchase_records),
            )
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
        credential = self._active_credential(integration.id, integration.account_id)
        if credential is None:
            return {}
        return self._crypto.decrypt_mapping(credential.secret_ciphertext)

    def _active_credential(self, integration_id: int, account_id: int | None = None) -> IntegrationCredential | None:
        filters = [IntegrationCredential.integration_id == integration_id, IntegrationCredential.status == "active"]
        if account_id is not None:
            filters.append(IntegrationCredential.account_id == account_id)
        return self.session.execute(
            select(IntegrationCredential)
            .where(*filters)
            .order_by(IntegrationCredential.version.desc())
        ).scalars().first()

    def _merge_credentials(self, existing: dict[str, object], updates: dict[str, object]) -> dict[str, object]:
        merged = dict(existing)
        for key, value in updates.items():
            if value is None:
                continue
            if isinstance(value, str):
                normalized = value.strip()
                if normalized == "":
                    continue
                merged[key] = normalized
                continue
            merged[key] = value
        return merged

    def _masked_credential_summary(self, credential: IntegrationCredential | None) -> dict[str, str]:
        if credential is None:
            return {}
        payload = self._crypto.decrypt_mapping(credential.secret_ciphertext)
        summary: dict[str, str] = {}
        for key, value in payload.items():
            if value is None:
                continue
            if isinstance(value, dict):
                summary[key] = "configured"
                continue
            text = str(value)
            if key.endswith("token") or "password" in key or "secret" in key or "session" in key or key.endswith("_string"):
                suffix = text[-4:] if len(text) >= 4 else "****"
                summary[key] = f"••••{suffix}"
            elif key == "fixture_payload":
                summary[key] = "configured"
            else:
                summary[key] = text
        return summary

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
    def __init__(self, session: Session, *, worker_id: str, lease_ttl_seconds: int, sync_auto_interval_seconds: int = 600) -> None:
        self.session = session
        self.worker_id = worker_id
        self.lease_ttl_seconds = lease_ttl_seconds
        self.sync_auto_interval_seconds = max(60, int(sync_auto_interval_seconds))
        self._lease_service = RuntimeLeaseService(session)
        self._audit = AuditLogService(session)

    def run_once(self, *, run_rules: bool = True, run_sync_jobs: bool = True) -> dict[str, object]:
        rule_runs = self._run_rules_once() if run_rules else 0
        enqueued_sync_jobs = self._enqueue_periodic_sync_jobs_once() if run_sync_jobs else 0
        sync_runs = self._run_sync_jobs_once() if run_sync_jobs else 0
        self.session.flush()
        return {
            "rule_accounts_processed": rule_runs,
            "sync_jobs_enqueued": enqueued_sync_jobs,
            "sync_jobs_processed": sync_runs,
        }

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

    def _enqueue_periodic_sync_jobs_once(self) -> int:
        runtime = RuntimeIntegrationService(self.session)
        now = datetime.now(timezone.utc)
        integrations = self.session.execute(
            select(Integration).where(
                Integration.status == "active",
            ).order_by(Integration.account_id.asc(), Integration.id.asc())
        ).scalars().all()
        enqueued = 0
        for integration in integrations:
            if not runtime.supports_sync(integration.provider_kind, integration.provider_name):
                continue
            if not self._should_enqueue_periodic_sync(runtime, integration, now):
                continue
            bucket = int(now.timestamp()) // self.sync_auto_interval_seconds
            context = TenantContext(account_id=integration.account_id, actor_user_id=None, source="scheduler", is_system=True)
            runtime.enqueue_sync_job(
                context,
                integration_id=integration.id,
                job_type="full_sync",
                trigger_mode="scheduled",
                idempotency_key=f"scheduled-sync:{integration.id}:{bucket}",
                scope_json={"source": "scheduler", "interval_seconds": self.sync_auto_interval_seconds},
                scheduled_at=now,
            )
            enqueued += 1
        return enqueued

    def _should_enqueue_periodic_sync(
        self,
        runtime: RuntimeIntegrationService,
        integration: Integration,
        now: datetime,
    ) -> bool:
        del runtime
        pending_job = self.session.execute(
            select(SyncJob).where(
                SyncJob.account_id == integration.account_id,
                SyncJob.integration_id == integration.id,
                SyncJob.status.in_(("pending", "running", "retry")),
            )
        ).scalars().first()
        if pending_job is not None:
            return False
        latest_job = self.session.execute(
            select(SyncJob)
            .where(
                SyncJob.account_id == integration.account_id,
                SyncJob.integration_id == integration.id,
            )
            .order_by(SyncJob.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        last_activity = integration.last_sync_at
        if latest_job is not None:
            latest_job_at = latest_job.finished_at or latest_job.started_at or latest_job.scheduled_at
            if latest_job_at is not None and (last_activity is None or latest_job_at > last_activity):
                last_activity = latest_job_at
        if last_activity is None:
            return True
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=timezone.utc)
        else:
            last_activity = last_activity.astimezone(timezone.utc)
        return (now - last_activity) >= timedelta(seconds=self.sync_auto_interval_seconds)


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
