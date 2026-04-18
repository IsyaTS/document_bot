from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Index, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from platform_core.db import Base
from platform_core.models.base import AccountScopedMixin, TimestampMixin


class Integration(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "integrations"
    __table_args__ = (
        UniqueConstraint("account_id", "provider_kind", "provider_name", "external_ref"),
        Index("ix_integrations_account_status", "account_id", "status"),
        Index("ix_integrations_account_provider_kind_status", "account_id", "provider_kind", "status"),
        Index("ix_integrations_account_provider_name", "account_id", "provider_name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    provider_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_name: Mapped[str] = mapped_column(String(64), nullable=False)
    external_ref: Mapped[str | None] = mapped_column(String(128), nullable=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    connection_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="polling")
    sync_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    webhook_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_webhook_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    settings_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class IntegrationCredential(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "integration_credentials"
    __table_args__ = (
        UniqueConstraint("account_id", "integration_id", "credential_type", "version"),
        Index("ix_integration_credentials_account_integration_status", "account_id", "integration_id", "status"),
        Index("ix_integration_credentials_account_type_status", "account_id", "credential_type", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    integration_id: Mapped[int] = mapped_column(ForeignKey("integrations.id", ondelete="CASCADE"), nullable=False)
    credential_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    version: Mapped[int] = mapped_column(nullable=False, default=1)
    secret_ciphertext: Mapped[str] = mapped_column(Text, nullable=False)
    secret_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    last_rotated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ProviderToken(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "provider_tokens"
    __table_args__ = (
        UniqueConstraint("account_id", "integration_id", "token_type"),
        Index("ix_provider_tokens_account_integration_status", "account_id", "integration_id", "status"),
        Index("ix_provider_tokens_account_expires_at", "account_id", "expires_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    integration_id: Mapped[int] = mapped_column(ForeignKey("integrations.id", ondelete="CASCADE"), nullable=False)
    token_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    access_token_ciphertext: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_token_ciphertext: Mapped[str | None] = mapped_column(Text, nullable=True)
    scopes_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SyncJob(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "sync_jobs"
    __table_args__ = (
        UniqueConstraint("account_id", "idempotency_key"),
        Index("ix_sync_jobs_account_status_scheduled_at", "account_id", "status", "scheduled_at"),
        Index("ix_sync_jobs_account_integration_status", "account_id", "integration_id", "status"),
        Index("ix_sync_jobs_account_provider_kind_job_type", "account_id", "provider_kind", "job_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    integration_id: Mapped[int] = mapped_column(ForeignKey("integrations.id", ondelete="CASCADE"), nullable=False)
    provider_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_name: Mapped[str] = mapped_column(String(64), nullable=False)
    job_type: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    attempts_count: Mapped[int] = mapped_column(nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(nullable=False, default=5)
    locked_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    scope_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    cursor_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class IntegrationLog(Base, AccountScopedMixin):
    __tablename__ = "integration_logs"
    __table_args__ = (
        Index("ix_integration_logs_account_integration_created_at", "account_id", "integration_id", "created_at"),
        Index("ix_integration_logs_account_sync_job_created_at", "account_id", "sync_job_id", "created_at"),
        Index("ix_integration_logs_account_level_created_at", "account_id", "level", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    integration_id: Mapped[int] = mapped_column(ForeignKey("integrations.id", ondelete="CASCADE"), nullable=False)
    sync_job_id: Mapped[int | None] = mapped_column(ForeignKey("sync_jobs.id", ondelete="SET NULL"), nullable=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="ok")
    provider_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_name: Mapped[str] = mapped_column(String(64), nullable=False)
    request_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class IntegrationEntityMapping(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "integration_entity_mappings"
    __table_args__ = (
        UniqueConstraint("account_id", "integration_id", "provider_entity_type", "external_id"),
        Index(
            "ix_integration_entity_mappings_account_integration_entity",
            "account_id",
            "integration_id",
            "provider_entity_type",
        ),
        Index(
            "ix_integration_entity_mappings_account_canonical_entity",
            "account_id",
            "canonical_entity_type",
            "canonical_entity_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    integration_id: Mapped[int] = mapped_column(ForeignKey("integrations.id", ondelete="CASCADE"), nullable=False)
    provider_entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str] = mapped_column(String(191), nullable=False)
    canonical_entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_entity_id: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
