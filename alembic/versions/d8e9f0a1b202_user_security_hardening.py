"""user security hardening

Revision ID: d8e9f0a1b202
Revises: c2d4e6f8a101
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "d8e9f0a1b202"
down_revision = "c2d4e6f8a101"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("password_hash", sa.String(length=512), nullable=True))
    op.add_column("users", sa.Column("password_set_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("users", sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("users", sa.Column("failed_login_attempts", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("users", sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True))
    op.add_column("users", sa.Column("invite_token_hash", sa.String(length=128), nullable=True))
    op.add_column("users", sa.Column("invite_sent_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("users", sa.Column("invite_accepted_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("users", sa.Column("reset_token_hash", sa.String(length=128), nullable=True))
    op.add_column("users", sa.Column("reset_requested_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("users", sa.Column("auth_version", sa.Integer(), nullable=False, server_default="1"))

    with op.batch_alter_table("users") as batch_op:
        batch_op.alter_column("failed_login_attempts", server_default=None)
        batch_op.alter_column("auth_version", server_default=None)


def downgrade() -> None:
    op.drop_column("users", "auth_version")
    op.drop_column("users", "reset_requested_at")
    op.drop_column("users", "reset_token_hash")
    op.drop_column("users", "invite_accepted_at")
    op.drop_column("users", "invite_sent_at")
    op.drop_column("users", "invite_token_hash")
    op.drop_column("users", "locked_until")
    op.drop_column("users", "failed_login_attempts")
    op.drop_column("users", "last_login_at")
    op.drop_column("users", "password_set_at")
    op.drop_column("users", "password_hash")
