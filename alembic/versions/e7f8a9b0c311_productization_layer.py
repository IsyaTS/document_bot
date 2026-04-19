"""productization layer

Revision ID: e7f8a9b0c311
Revises: d8e9f0a1b202
Create Date: 2026-04-19 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "e7f8a9b0c311"
down_revision = "d8e9f0a1b202"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("accounts") as batch_op:
        batch_op.add_column(sa.Column("plan_type", sa.String(length=32), nullable=False, server_default="internal"))
        batch_op.add_column(sa.Column("settings_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")))
        batch_op.add_column(sa.Column("feature_flags_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")))
        batch_op.add_column(sa.Column("soft_limits_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")))
        batch_op.alter_column("plan_type", server_default=None)
        batch_op.alter_column("settings_json", server_default=None)
        batch_op.alter_column("feature_flags_json", server_default=None)
        batch_op.alter_column("soft_limits_json", server_default=None)


def downgrade() -> None:
    with op.batch_alter_table("accounts") as batch_op:
        batch_op.drop_column("soft_limits_json")
        batch_op.drop_column("feature_flags_json")
        batch_op.drop_column("settings_json")
        batch_op.drop_column("plan_type")
