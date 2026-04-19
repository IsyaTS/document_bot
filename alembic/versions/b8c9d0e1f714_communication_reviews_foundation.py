"""communication reviews foundation

Revision ID: b8c9d0e1f714
Revises: a7b8c9d0e613
Create Date: 2026-04-19 10:25:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "b8c9d0e1f714"
down_revision = "a7b8c9d0e613"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "communication_reviews",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("customer_id", sa.Integer(), nullable=True),
        sa.Column("lead_id", sa.Integer(), nullable=True),
        sa.Column("employee_id", sa.Integer(), nullable=True),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("direction", sa.String(length=16), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("transcript_text", sa.Text(), nullable=False),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("quality_status", sa.String(length=32), nullable=False),
        sa.Column("sentiment", sa.String(length=16), nullable=False),
        sa.Column("response_delay_minutes", sa.Integer(), nullable=True),
        sa.Column("next_step_present", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("follow_up_status", sa.String(length=32), nullable=False),
        sa.Column("summary_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_communication_reviews_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], name=op.f("fk_communication_reviews_created_by_user_id_users"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["customer_id"], ["customers.id"], name=op.f("fk_communication_reviews_customer_id_customers"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"], name=op.f("fk_communication_reviews_lead_id_leads"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["employee_id"], ["employees.id"], name=op.f("fk_communication_reviews_employee_id_employees"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_communication_reviews")),
    )
    op.create_index("ix_communication_reviews_account_status_created_at", "communication_reviews", ["account_id", "quality_status", "created_at"], unique=False)
    op.create_index("ix_communication_reviews_account_customer_created_at", "communication_reviews", ["account_id", "customer_id", "created_at"], unique=False)
    op.create_index("ix_communication_reviews_account_lead_created_at", "communication_reviews", ["account_id", "lead_id", "created_at"], unique=False)
    op.create_index("ix_communication_reviews_account_employee_created_at", "communication_reviews", ["account_id", "employee_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_communication_reviews_account_employee_created_at", table_name="communication_reviews")
    op.drop_index("ix_communication_reviews_account_lead_created_at", table_name="communication_reviews")
    op.drop_index("ix_communication_reviews_account_customer_created_at", table_name="communication_reviews")
    op.drop_index("ix_communication_reviews_account_status_created_at", table_name="communication_reviews")
    op.drop_table("communication_reviews")
