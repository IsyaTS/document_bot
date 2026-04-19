"""billing settlements foundation

Revision ID: f3a4b5c6d219
Revises: f2a3b4c5d118
Create Date: 2026-04-19 11:50:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "f3a4b5c6d219"
down_revision = "f2a3b4c5d118"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "document_settlements",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("document_id", sa.Integer(), nullable=False),
        sa.Column("recorded_by_user_id", sa.Integer(), nullable=True),
        sa.Column("settlement_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("settlement_date", sa.Date(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(length=8), nullable=False),
        sa.Column("reference", sa.String(length=128), nullable=True),
        sa.Column("notes_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["document_id"], ["documents.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["recorded_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_document_settlements")),
    )
    op.create_index(
        "ix_document_settlements_account_document_settlement_date",
        "document_settlements",
        ["account_id", "document_id", "settlement_date"],
        unique=False,
    )
    op.create_index(
        "ix_document_settlements_account_type_settlement_date",
        "document_settlements",
        ["account_id", "settlement_type", "settlement_date"],
        unique=False,
    )
    op.create_index(
        "ix_document_settlements_account_status_created_at",
        "document_settlements",
        ["account_id", "status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_document_settlements_account_status_created_at", table_name="document_settlements")
    op.drop_index("ix_document_settlements_account_type_settlement_date", table_name="document_settlements")
    op.drop_index("ix_document_settlements_account_document_settlement_date", table_name="document_settlements")
    op.drop_table("document_settlements")
