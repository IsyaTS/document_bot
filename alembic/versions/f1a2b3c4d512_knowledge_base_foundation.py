"""knowledge base foundation

Revision ID: f1a2b3c4d512
Revises: e7f8a9b0c311
Create Date: 2026-04-19 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "f1a2b3c4d512"
down_revision = "e7f8a9b0c311"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "knowledge_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("customer_id", sa.Integer(), nullable=True),
        sa.Column("deal_id", sa.Integer(), nullable=True),
        sa.Column("document_id", sa.Integer(), nullable=True),
        sa.Column("item_type", sa.String(length=32), nullable=False),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("body_text", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("visibility", sa.String(length=32), nullable=False),
        sa.Column("file_name", sa.String(length=255), nullable=True),
        sa.Column("file_path", sa.String(length=512), nullable=True),
        sa.Column("mime_type", sa.String(length=128), nullable=True),
        sa.Column("content_size_bytes", sa.Integer(), nullable=True),
        sa.Column("content_sha256", sa.String(length=64), nullable=True),
        sa.Column("tags_json", sa.JSON(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_knowledge_items_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], name=op.f("fk_knowledge_items_created_by_user_id_users"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["customer_id"], ["customers.id"], name=op.f("fk_knowledge_items_customer_id_customers"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["deal_id"], ["deals.id"], name=op.f("fk_knowledge_items_deal_id_deals"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["document_id"], ["documents.id"], name=op.f("fk_knowledge_items_document_id_documents"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_knowledge_items")),
    )
    op.create_index("ix_knowledge_items_account_status_created_at", "knowledge_items", ["account_id", "status", "created_at"], unique=False)
    op.create_index("ix_knowledge_items_account_type_created_at", "knowledge_items", ["account_id", "item_type", "created_at"], unique=False)
    op.create_index("ix_knowledge_items_account_customer_created_at", "knowledge_items", ["account_id", "customer_id", "created_at"], unique=False)
    op.create_index("ix_knowledge_items_account_deal_created_at", "knowledge_items", ["account_id", "deal_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_knowledge_items_account_deal_created_at", table_name="knowledge_items")
    op.drop_index("ix_knowledge_items_account_customer_created_at", table_name="knowledge_items")
    op.drop_index("ix_knowledge_items_account_type_created_at", table_name="knowledge_items")
    op.drop_index("ix_knowledge_items_account_status_created_at", table_name="knowledge_items")
    op.drop_table("knowledge_items")
