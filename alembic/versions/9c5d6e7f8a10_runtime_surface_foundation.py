"""runtime surface foundation

Revision ID: 9c5d6e7f8a10
Revises: 91d1f3a2a001
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "9c5d6e7f8a10"
down_revision = "91d1f3a2a001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "runtime_leases",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("lease_key", sa.String(length=191), nullable=False),
        sa.Column("owner", sa.String(length=128), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runtime_leases")),
        sa.UniqueConstraint("account_id", "lease_key", name=op.f("uq_runtime_leases_account_id")),
    )
    op.create_index(op.f("ix_runtime_leases_account_id"), "runtime_leases", ["account_id"], unique=False)
    op.create_index("ix_runtime_leases_account_expires_at", "runtime_leases", ["account_id", "expires_at"], unique=False)
    op.create_index("ix_runtime_leases_account_owner", "runtime_leases", ["account_id", "owner"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_runtime_leases_account_owner", table_name="runtime_leases")
    op.drop_index("ix_runtime_leases_account_expires_at", table_name="runtime_leases")
    op.drop_index(op.f("ix_runtime_leases_account_id"), table_name="runtime_leases")
    op.drop_table("runtime_leases")
