from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Index, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from platform_core.db import Base
from platform_core.models.base import AccountScopedMixin, TimestampMixin


class Rule(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "rules"
    __table_args__ = (
        UniqueConstraint("account_id", "code"),
        Index("ix_rules_account_status", "account_id", "status"),
        Index("ix_rules_account_rule_type_status", "account_id", "rule_type", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    active_version_number: Mapped[int] = mapped_column(nullable=False, default=1)


class RuleVersion(Base):
    __tablename__ = "rule_versions"
    __table_args__ = (
        UniqueConstraint("rule_id", "version_number"),
        Index("ix_rule_versions_rule_status", "rule_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("rules.id", ondelete="CASCADE"), nullable=False)
    version_number: Mapped[int] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    config_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ThresholdConfig(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "threshold_configs"
    __table_args__ = (
        UniqueConstraint("account_id", "rule_id", "threshold_key"),
        Index("ix_threshold_configs_account_rule_status", "account_id", "rule_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("rules.id", ondelete="CASCADE"), nullable=False)
    threshold_key: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    value_numeric: Mapped[str | None] = mapped_column(String(64), nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class RuleExecution(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "rule_executions"
    __table_args__ = (
        UniqueConstraint("account_id", "execution_key"),
        Index("ix_rule_executions_account_rule_status", "account_id", "rule_id", "status"),
        Index("ix_rule_executions_account_entity_status", "account_id", "evaluated_entity_type", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("rules.id", ondelete="CASCADE"), nullable=False)
    rule_version_id: Mapped[int] = mapped_column(ForeignKey("rule_versions.id", ondelete="CASCADE"), nullable=False)
    execution_key: Mapped[str] = mapped_column(String(191), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="triggered")
    evaluated_entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    evaluated_entity_id: Mapped[str] = mapped_column(String(64), nullable=False)
    window_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    run_count: Mapped[int] = mapped_column(nullable=False, default=1)
    last_evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    first_triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    alert_id: Mapped[int | None] = mapped_column(ForeignKey("alerts.id", ondelete="SET NULL"), nullable=True)
    task_id: Mapped[int | None] = mapped_column(ForeignKey("tasks.id", ondelete="SET NULL"), nullable=True)
    recommendation_id: Mapped[int | None] = mapped_column(ForeignKey("recommendations.id", ondelete="SET NULL"), nullable=True)
    details_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
