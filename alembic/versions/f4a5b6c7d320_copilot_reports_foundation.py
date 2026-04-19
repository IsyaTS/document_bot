"""copilot reports foundation

Revision ID: f4a5b6c7d320
Revises: f3a4b5c6d219
Create Date: 2026-04-19 16:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "f4a5b6c7d320"
down_revision = "f3a4b5c6d219"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "copilot_reports",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("generation_mode", sa.String(length=32), nullable=False),
        sa.Column("model_name", sa.String(length=128), nullable=True),
        sa.Column("provider_response_id", sa.String(length=128), nullable=True),
        sa.Column("question_text", sa.Text(), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("summary_text", sa.Text(), nullable=True),
        sa.Column("markdown_text", sa.Text(), nullable=True),
        sa.Column("obsidian_note_path", sa.String(length=512), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_copilot_reports")),
    )
    op.create_index(
        "ix_copilot_reports_account_status_created_at",
        "copilot_reports",
        ["account_id", "status", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_copilot_reports_account_scope_created_at",
        "copilot_reports",
        ["account_id", "scope", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_copilot_reports_account_scope_created_at", table_name="copilot_reports")
    op.drop_index("ix_copilot_reports_account_status_created_at", table_name="copilot_reports")
    op.drop_table("copilot_reports")
