from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from platform_core.models import AuditLog
from platform_core.tenancy import TenantContext, require_account_id


class AuditLogService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def log(
        self,
        context: TenantContext,
        action: str,
        entity_type: str,
        entity_id: str,
        *,
        status: str = "success",
        details: dict[str, object] | None = None,
    ) -> AuditLog:
        audit_log = AuditLog(
            account_id=require_account_id(context),
            actor_user_id=context.actor_user_id,
            source=context.source,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            status=status,
            request_id=context.request_id,
            details_json=details or {},
            created_at=datetime.now(timezone.utc),
        )
        self.session.add(audit_log)
        self.session.flush()
        return audit_log
