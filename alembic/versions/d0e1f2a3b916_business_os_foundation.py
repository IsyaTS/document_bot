"""business os foundation

Revision ID: d0e1f2a3b916
Revises: c9d0e1f2a815
Create Date: 2026-04-19 11:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "d0e1f2a3b916"
down_revision = "c9d0e1f2a815"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notification_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("body_text", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_notification_events_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], name=op.f("fk_notification_events_created_by_user_id_users"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notification_events")),
    )
    op.create_index("ix_notification_events_account_channel_created_at", "notification_events", ["account_id", "channel", "created_at"], unique=False)
    op.create_index("ix_notification_events_account_status_created_at", "notification_events", ["account_id", "status", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_notification_events_account_status_created_at", table_name="notification_events")
    op.drop_index("ix_notification_events_account_channel_created_at", table_name="notification_events")
    op.drop_table("notification_events")
