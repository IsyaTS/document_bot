"""payroll and employee kpi foundation

Revision ID: c9d0e1f2a815
Revises: b8c9d0e1f714
Create Date: 2026-04-19 10:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "c9d0e1f2a815"
down_revision = "b8c9d0e1f714"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("employees", sa.Column("base_salary", sa.Numeric(18, 2), nullable=False, server_default="0"))
    op.add_column("employees", sa.Column("commission_rate_pct", sa.Numeric(8, 2), nullable=False, server_default="0"))
    op.add_column("employees", sa.Column("kpi_bonus_amount", sa.Numeric(18, 2), nullable=False, server_default="0"))
    op.add_column("employees", sa.Column("penalty_per_overdue_task", sa.Numeric(18, 2), nullable=False, server_default="0"))
    op.add_column("employees", sa.Column("penalty_per_quality_breach", sa.Numeric(18, 2), nullable=False, server_default="0"))

    op.create_table(
        "payroll_periods",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("approved_by_user_id", sa.Integer(), nullable=True),
        sa.Column("period_kind", sa.String(length=16), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_payroll_periods_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["approved_by_user_id"], ["users.id"], name=op.f("fk_payroll_periods_approved_by_user_id_users"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payroll_periods")),
        sa.UniqueConstraint("account_id", "period_kind", "period_start", "period_end", name=op.f("uq_payroll_periods_account_id")),
    )
    op.create_index("ix_payroll_periods_account_status_period_start", "payroll_periods", ["account_id", "status", "period_start"], unique=False)

    op.create_table(
        "employee_kpis",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("employee_id", sa.Integer(), nullable=False),
        sa.Column("metric_code", sa.String(length=64), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("target_value", sa.Numeric(18, 2), nullable=True),
        sa.Column("actual_value", sa.Numeric(18, 2), nullable=False),
        sa.Column("score_pct", sa.Numeric(8, 2), nullable=True),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_employee_kpis_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["employee_id"], ["employees.id"], name=op.f("fk_employee_kpis_employee_id_employees"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_employee_kpis")),
        sa.UniqueConstraint("account_id", "employee_id", "period_start", "period_end", "metric_code", name=op.f("uq_employee_kpis_account_id")),
    )
    op.create_index("ix_employee_kpis_account_employee_period", "employee_kpis", ["account_id", "employee_id", "period_start", "period_end"], unique=False)

    op.create_table(
        "payroll_entries",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("payroll_period_id", sa.Integer(), nullable=False),
        sa.Column("employee_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("base_salary_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("commission_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("bonus_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("penalty_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("gross_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("net_amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("summary_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], name=op.f("fk_payroll_entries_account_id_accounts"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["employee_id"], ["employees.id"], name=op.f("fk_payroll_entries_employee_id_employees"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["payroll_period_id"], ["payroll_periods.id"], name=op.f("fk_payroll_entries_payroll_period_id_payroll_periods"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payroll_entries")),
        sa.UniqueConstraint("account_id", "payroll_period_id", "employee_id", name=op.f("uq_payroll_entries_account_id")),
    )
    op.create_index("ix_payroll_entries_account_period_status", "payroll_entries", ["account_id", "payroll_period_id", "status"], unique=False)
    op.create_index("ix_payroll_entries_account_employee_period", "payroll_entries", ["account_id", "employee_id", "payroll_period_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_payroll_entries_account_employee_period", table_name="payroll_entries")
    op.drop_index("ix_payroll_entries_account_period_status", table_name="payroll_entries")
    op.drop_table("payroll_entries")
    op.drop_index("ix_employee_kpis_account_employee_period", table_name="employee_kpis")
    op.drop_table("employee_kpis")
    op.drop_index("ix_payroll_periods_account_status_period_start", table_name="payroll_periods")
    op.drop_table("payroll_periods")
    op.drop_column("employees", "penalty_per_quality_breach")
    op.drop_column("employees", "penalty_per_overdue_task")
    op.drop_column("employees", "kpi_bonus_amount")
    op.drop_column("employees", "commission_rate_pct")
    op.drop_column("employees", "base_salary")
