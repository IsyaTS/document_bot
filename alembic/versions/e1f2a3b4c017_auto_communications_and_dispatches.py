"""auto communications batches and notification dispatches

Revision ID: e1f2a3b4c017
Revises: d0e1f2a3b916
Create Date: 2026-04-19 13:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "e1f2a3b4c017"
down_revision = "d0e1f2a3b916"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "communication_import_batches",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("batch_ref", sa.String(length=128), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("imported_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("critical_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_communication_import_batches_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], name=op.f("fk_communication_import_batches_created_by_user_id_users"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_communication_import_batches")),
    )
    op.create_index(
        "ix_communication_import_batches_account_status_created_at",
        "communication_import_batches",
        ["account_id", "status", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_communication_import_batches_account_source_created_at",
        "communication_import_batches",
        ["account_id", "source_kind", "created_at"],
        unique=False,
    )

    op.create_table(
        "notification_dispatches",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("notification_event_id", sa.Integer(), nullable=False),
        sa.Column("dispatched_by_user_id", sa.Integer(), nullable=True),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("target_ref", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivery_path", sa.String(length=512), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_notification_dispatches_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dispatched_by_user_id"], ["users.id"], name=op.f("fk_notification_dispatches_dispatched_by_user_id_users"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["notification_event_id"], ["notification_events.id"], name=op.f("fk_notification_dispatches_notification_event_id_notification_events"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notification_dispatches")),
    )
    op.create_index(
        "ix_notification_dispatches_account_channel_created_at",
        "notification_dispatches",
        ["account_id", "channel", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_notification_dispatches_account_status_created_at",
        "notification_dispatches",
        ["account_id", "status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_notification_dispatches_account_status_created_at", table_name="notification_dispatches")
    op.drop_index("ix_notification_dispatches_account_channel_created_at", table_name="notification_dispatches")
    op.drop_table("notification_dispatches")
    op.drop_index("ix_communication_import_batches_account_source_created_at", table_name="communication_import_batches")
    op.drop_index("ix_communication_import_batches_account_status_created_at", table_name="communication_import_batches")
    op.drop_table("communication_import_batches")
