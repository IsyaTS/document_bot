from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Index, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from platform_core.db import Base
from platform_core.models.base import AccountScopedMixin, TimestampMixin


class RuntimeLease(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "runtime_leases"
    __table_args__ = (
        UniqueConstraint("account_id", "lease_key"),
        Index("ix_runtime_leases_account_expires_at", "account_id", "expires_at"),
        Index("ix_runtime_leases_account_owner", "account_id", "owner"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lease_key: Mapped[str] = mapped_column(String(191), nullable=False)
    owner: Mapped[str] = mapped_column(String(128), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
