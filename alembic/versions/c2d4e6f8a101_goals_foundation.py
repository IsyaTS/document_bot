"""goals foundation

Revision ID: c2d4e6f8a101
Revises: b5f6a7c8d901
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "c2d4e6f8a101"
down_revision = "b5f6a7c8d901"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "goals",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("owner_user_id", sa.Integer(), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("period_kind", sa.String(length=16), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("is_primary", sa.Boolean(), nullable=False),
        sa.Column("settings_json", sa.JSON(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_goals")),
    )
    op.create_index(op.f("ix_goals_account_id"), "goals", ["account_id"], unique=False)
    op.create_index("ix_goals_account_status_period", "goals", ["account_id", "status", "period_kind"], unique=False)
    op.create_index("ix_goals_account_owner_status", "goals", ["account_id", "owner_user_id", "status"], unique=False)
    op.create_index("ix_goals_account_start_end", "goals", ["account_id", "period_start", "period_end"], unique=False)

    op.create_table(
        "goal_targets",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("goal_id", sa.Integer(), nullable=False),
        sa.Column("metric_code", sa.String(length=64), nullable=False),
        sa.Column("direction", sa.String(length=16), nullable=False),
        sa.Column("target_value", sa.Numeric(18, 2), nullable=False),
        sa.Column("settings_json", sa.JSON(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["goal_id"], ["goals.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_goal_targets")),
        sa.UniqueConstraint("account_id", "goal_id", "metric_code", name=op.f("uq_goal_targets_account_id")),
    )
    op.create_index(op.f("ix_goal_targets_account_id"), "goal_targets", ["account_id"], unique=False)
    op.create_index("ix_goal_targets_account_goal", "goal_targets", ["account_id", "goal_id"], unique=False)
    op.create_index("ix_goal_targets_account_metric", "goal_targets", ["account_id", "metric_code"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_goal_targets_account_metric", table_name="goal_targets")
    op.drop_index("ix_goal_targets_account_goal", table_name="goal_targets")
    op.drop_index(op.f("ix_goal_targets_account_id"), table_name="goal_targets")
    op.drop_table("goal_targets")

    op.drop_index("ix_goals_account_start_end", table_name="goals")
    op.drop_index("ix_goals_account_owner_status", table_name="goals")
    op.drop_index("ix_goals_account_status_period", table_name="goals")
    op.drop_index(op.f("ix_goals_account_id"), table_name="goals")
    op.drop_table("goals")
