"""dashboard foundation

Revision ID: 91d1f3a2a001
Revises: 2a718c2f124b
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "91d1f3a2a001"
down_revision = "2a718c2f124b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dashboard_configs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("settings_json", sa.JSON(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dashboard_configs")),
        sa.UniqueConstraint("account_id", "code", name=op.f("uq_dashboard_configs_account_id")),
    )
    op.create_index(op.f("ix_dashboard_configs_account_id"), "dashboard_configs", ["account_id"], unique=False)
    op.create_index(
        "ix_dashboard_configs_account_status",
        "dashboard_configs",
        ["account_id", "status"],
        unique=False,
    )

    op.create_table(
        "dashboard_widget_configs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dashboard_config_id", sa.Integer(), nullable=False),
        sa.Column("widget_key", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False),
        sa.Column("settings_json", sa.JSON(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dashboard_config_id"], ["dashboard_configs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dashboard_widget_configs")),
        sa.UniqueConstraint(
            "account_id",
            "dashboard_config_id",
            "widget_key",
            name=op.f("uq_dashboard_widget_configs_account_id"),
        ),
    )
    op.create_index(op.f("ix_dashboard_widget_configs_account_id"), "dashboard_widget_configs", ["account_id"], unique=False)
    op.create_index(
        "ix_dashboard_widget_configs_account_dashboard_position",
        "dashboard_widget_configs",
        ["account_id", "dashboard_config_id", "position"],
        unique=False,
    )
    op.create_index(
        "ix_dashboard_widget_configs_account_enabled",
        "dashboard_widget_configs",
        ["account_id", "is_enabled"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_dashboard_widget_configs_account_enabled", table_name="dashboard_widget_configs")
    op.drop_index("ix_dashboard_widget_configs_account_dashboard_position", table_name="dashboard_widget_configs")
    op.drop_index(op.f("ix_dashboard_widget_configs_account_id"), table_name="dashboard_widget_configs")
    op.drop_table("dashboard_widget_configs")
    op.drop_index("ix_dashboard_configs_account_status", table_name="dashboard_configs")
    op.drop_index(op.f("ix_dashboard_configs_account_id"), table_name="dashboard_configs")
    op.drop_table("dashboard_configs")
