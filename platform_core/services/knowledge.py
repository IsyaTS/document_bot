from __future__ import annotations

from sqlalchemy import Text, cast, func, or_, select
from sqlalchemy.orm import Session

from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.models import KnowledgeItem
from platform_core.tenancy import TenantContext, require_account_id


class KnowledgeService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_items(
        self,
        context: TenantContext,
        *,
        q: str | None = None,
        item_type: str | None = None,
        status: str | None = None,
        customer_id: int | None = None,
        deal_id: int | None = None,
        limit: int = 200,
    ) -> list[KnowledgeItem]:
        account_id = require_account_id(context)
        query = select(KnowledgeItem).where(KnowledgeItem.account_id == account_id)
        if item_type:
            query = query.where(KnowledgeItem.item_type == item_type)
        if status:
            query = query.where(KnowledgeItem.status == status)
        if customer_id is not None:
            query = query.where(KnowledgeItem.customer_id == customer_id)
        if deal_id is not None:
            query = query.where(KnowledgeItem.deal_id == deal_id)
        if q:
            pattern = f"%{q.strip()}%"
            query = query.where(
                or_(
                    KnowledgeItem.title.ilike(pattern),
                    KnowledgeItem.summary.ilike(pattern),
                    KnowledgeItem.body_text.ilike(pattern),
                    KnowledgeItem.file_name.ilike(pattern),
                    cast(KnowledgeItem.tags_json, Text).ilike(pattern),
                )
            )
        return self.session.execute(
            query.order_by(KnowledgeItem.created_at.desc(), KnowledgeItem.id.desc()).limit(max(1, min(limit, 500)))
        ).scalars().all()

    def get_item(self, context: TenantContext, item_id: int) -> KnowledgeItem:
        account_id = require_account_id(context)
        item = self.session.execute(
            select(KnowledgeItem).where(KnowledgeItem.account_id == account_id, KnowledgeItem.id == item_id)
        ).scalar_one_or_none()
        if item is None:
            raise TenantContextError("Knowledge item not found in selected account.")
        return item

    def create_item(
        self,
        context: TenantContext,
        *,
        title: str,
        summary: str | None = None,
        body_text: str | None = None,
        item_type: str = "note",
        source_kind: str = "manual",
        status: str = "active",
        visibility: str = "internal",
        customer_id: int | None = None,
        deal_id: int | None = None,
        document_id: int | None = None,
        file_name: str | None = None,
        file_path: str | None = None,
        mime_type: str | None = None,
        content_size_bytes: int | None = None,
        content_sha256: str | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> KnowledgeItem:
        account_id = require_account_id(context)
        normalized_title = title.strip()
        if not normalized_title:
            raise PlatformCoreError("Knowledge item title is required.")
        item = KnowledgeItem(
            account_id=account_id,
            created_by_user_id=context.actor_user_id,
            customer_id=customer_id,
            deal_id=deal_id,
            document_id=document_id,
            item_type=item_type,
            source_kind=source_kind,
            title=normalized_title,
            summary=(summary or "").strip() or None,
            body_text=(body_text or "").strip() or None,
            status=status,
            visibility=visibility,
            file_name=(file_name or "").strip() or None,
            file_path=(file_path or "").strip() or None,
            mime_type=(mime_type or "").strip() or None,
            content_size_bytes=content_size_bytes,
            content_sha256=(content_sha256 or "").strip() or None,
            tags_json=self._normalize_tags(tags),
            metadata_json=metadata or {},
        )
        self.session.add(item)
        self.session.flush()
        return item

    def update_status(self, context: TenantContext, item_id: int, *, status: str) -> KnowledgeItem:
        if status not in {"active", "archived"}:
            raise PlatformCoreError("Unsupported knowledge item status.")
        item = self.get_item(context, item_id)
        item.status = status
        self.session.flush()
        return item

    def count_active_items(self, account_id: int) -> int:
        return int(
            self.session.execute(
                select(func.count(KnowledgeItem.id)).where(
                    KnowledgeItem.account_id == account_id,
                    KnowledgeItem.status == "active",
                )
            ).scalar_one()
            or 0
        )

    def _normalize_tags(self, tags: list[str] | None) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in tags or []:
            value = raw.strip()
            if not value:
                continue
            lowered = value.lower()
            if lowered in seen:
                continue
            normalized.append(value)
            seen.add(lowered)
        return normalized
