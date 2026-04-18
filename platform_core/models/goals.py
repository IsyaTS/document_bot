from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import JSON, Boolean, Date, ForeignKey, Index, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from platform_core.db import Base
from platform_core.models.base import AccountScopedMixin, TimestampMixin


class Goal(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "goals"
    __table_args__ = (
        Index("ix_goals_account_status_period", "account_id", "status", "period_kind"),
        Index("ix_goals_account_owner_status", "account_id", "owner_user_id", "status"),
        Index("ix_goals_account_start_end", "account_id", "period_start", "period_end"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    owner_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    period_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    settings_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class GoalTarget(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "goal_targets"
    __table_args__ = (
        UniqueConstraint("account_id", "goal_id", "metric_code"),
        Index("ix_goal_targets_account_goal", "account_id", "goal_id"),
        Index("ix_goal_targets_account_metric", "account_id", "metric_code"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    goal_id: Mapped[int] = mapped_column(ForeignKey("goals.id", ondelete="CASCADE"), nullable=False)
    metric_code: Mapped[str] = mapped_column(String(64), nullable=False)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)
    target_value: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    settings_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
