from __future__ import annotations

from sqlalchemy import JSON, Boolean, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from platform_core.db import Base
from platform_core.models.base import AccountScopedMixin, TimestampMixin


class DashboardConfig(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "dashboard_configs"
    __table_args__ = (
        UniqueConstraint("account_id", "code"),
        Index("ix_dashboard_configs_account_status", "account_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    settings_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class DashboardWidgetConfig(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "dashboard_widget_configs"
    __table_args__ = (
        UniqueConstraint("account_id", "dashboard_config_id", "widget_key"),
        Index("ix_dashboard_widget_configs_account_dashboard_position", "account_id", "dashboard_config_id", "position"),
        Index("ix_dashboard_widget_configs_account_enabled", "account_id", "is_enabled"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    dashboard_config_id: Mapped[int] = mapped_column(
        ForeignKey("dashboard_configs.id", ondelete="CASCADE"),
        nullable=False,
    )
    widget_key: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    position: Mapped[int] = mapped_column(nullable=False, default=0)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    settings_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
