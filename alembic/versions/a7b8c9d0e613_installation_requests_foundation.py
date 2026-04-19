"""installation requests foundation

Revision ID: a7b8c9d0e613
Revises: f1a2b3c4d512
Create Date: 2026-04-19 00:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "a7b8c9d0e613"
down_revision = "f1a2b3c4d512"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "installation_requests",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("customer_id", sa.Integer(), nullable=True),
        sa.Column("deal_id", sa.Integer(), nullable=True),
        sa.Column("assigned_employee_id", sa.Integer(), nullable=True),
        sa.Column("request_number", sa.String(length=64), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("address", sa.String(length=255), nullable=True),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_installation_requests_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["customer_id"], ["customers.id"], name=op.f("fk_installation_requests_customer_id_customers"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["deal_id"], ["deals.id"], name=op.f("fk_installation_requests_deal_id_deals"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["assigned_employee_id"], ["employees.id"], name=op.f("fk_installation_requests_assigned_employee_id_employees"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_installation_requests")),
    )
    op.create_index("ix_installation_requests_account_status_scheduled_for", "installation_requests", ["account_id", "status", "scheduled_for"], unique=False)
    op.create_index("ix_installation_requests_account_customer_created_at", "installation_requests", ["account_id", "customer_id", "created_at"], unique=False)
    op.create_index("ix_installation_requests_account_deal_created_at", "installation_requests", ["account_id", "deal_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_installation_requests_account_deal_created_at", table_name="installation_requests")
    op.drop_index("ix_installation_requests_account_customer_created_at", table_name="installation_requests")
    op.drop_index("ix_installation_requests_account_status_scheduled_for", table_name="installation_requests")
    op.drop_table("installation_requests")
