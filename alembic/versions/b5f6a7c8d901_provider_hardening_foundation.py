"""provider hardening foundation

Revision ID: b5f6a7c8d901
Revises: 9c5d6e7f8a10
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "b5f6a7c8d901"
down_revision = "9c5d6e7f8a10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "integration_entity_mappings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("integration_id", sa.Integer(), nullable=False),
        sa.Column("provider_entity_type", sa.String(length=64), nullable=False),
        sa.Column("external_id", sa.String(length=191), nullable=False),
        sa.Column("canonical_entity_type", sa.String(length=64), nullable=False),
        sa.Column("canonical_entity_id", sa.String(length=64), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["integration_id"], ["integrations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_integration_entity_mappings")),
        sa.UniqueConstraint(
            "account_id",
            "integration_id",
            "provider_entity_type",
            "external_id",
            name=op.f("uq_integration_entity_mappings_account_id"),
        ),
    )
    op.create_index(
        op.f("ix_integration_entity_mappings_account_id"),
        "integration_entity_mappings",
        ["account_id"],
        unique=False,
    )
    op.create_index(
        "ix_integration_entity_mappings_account_integration_entity",
        "integration_entity_mappings",
        ["account_id", "integration_id", "provider_entity_type"],
        unique=False,
    )
    op.create_index(
        "ix_integration_entity_mappings_account_canonical_entity",
        "integration_entity_mappings",
        ["account_id", "canonical_entity_type", "canonical_entity_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_integration_entity_mappings_account_canonical_entity", table_name="integration_entity_mappings")
    op.drop_index("ix_integration_entity_mappings_account_integration_entity", table_name="integration_entity_mappings")
    op.drop_index(op.f("ix_integration_entity_mappings_account_id"), table_name="integration_entity_mappings")
    op.drop_table("integration_entity_mappings")
