"""execution discipline and payroll payments

Revision ID: f2a3b4c5d118
Revises: e1f2a3b4c017
Create Date: 2026-04-19 13:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "f2a3b4c5d118"
down_revision = "e1f2a3b4c017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payroll_payments",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("payroll_entry_id", sa.Integer(), nullable=False),
        sa.Column("recorded_by_user_id", sa.Integer(), nullable=True),
        sa.Column("payment_date", sa.Date(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False, server_default="0"),
        sa.Column("payment_ref", sa.String(length=128), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_payroll_payments_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["payroll_entry_id"], ["payroll_entries.id"], name=op.f("fk_payroll_payments_payroll_entry_id_payroll_entries"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["recorded_by_user_id"], ["users.id"], name=op.f("fk_payroll_payments_recorded_by_user_id_users"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payroll_payments")),
    )
    op.create_index("ix_payroll_payments_account_entry_payment_date", "payroll_payments", ["account_id", "payroll_entry_id", "payment_date"], unique=False)
    op.create_index("ix_payroll_payments_account_status_payment_date", "payroll_payments", ["account_id", "status", "payment_date"], unique=False)

    op.create_table(
        "task_checkins",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("employee_id", sa.Integer(), nullable=True),
        sa.Column("checkin_type", sa.String(length=32), nullable=False),
        sa.Column("note_text", sa.Text(), nullable=True),
        sa.Column("status_after", sa.String(length=32), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_task_checkins_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["actor_user_id"], ["users.id"], name=op.f("fk_task_checkins_actor_user_id_users"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["employee_id"], ["employees.id"], name=op.f("fk_task_checkins_employee_id_employees"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], name=op.f("fk_task_checkins_task_id_tasks"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_task_checkins")),
    )
    op.create_index("ix_task_checkins_account_task_created_at", "task_checkins", ["account_id", "task_id", "created_at"], unique=False)
    op.create_index("ix_task_checkins_account_employee_created_at", "task_checkins", ["account_id", "employee_id", "created_at"], unique=False)
    op.create_index("ix_task_checkins_account_type_created_at", "task_checkins", ["account_id", "checkin_type", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_task_checkins_account_type_created_at", table_name="task_checkins")
    op.drop_index("ix_task_checkins_account_employee_created_at", table_name="task_checkins")
    op.drop_index("ix_task_checkins_account_task_created_at", table_name="task_checkins")
    op.drop_table("task_checkins")
    op.drop_index("ix_payroll_payments_account_status_payment_date", table_name="payroll_payments")
    op.drop_index("ix_payroll_payments_account_entry_payment_date", table_name="payroll_payments")
    op.drop_table("payroll_payments")
