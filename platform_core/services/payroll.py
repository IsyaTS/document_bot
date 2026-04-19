from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.models import CommunicationReview, Employee, EmployeeKPI, PayrollEntry, PayrollPeriod, Task
from platform_core.tenancy import TenantContext, require_account_id


PAYROLL_METRIC_DEFINITIONS: list[dict[str, str]] = [
    {"code": "revenue_generated", "label": "Revenue generated"},
    {"code": "completed_tasks", "label": "Completed tasks"},
    {"code": "overdue_tasks", "label": "Overdue tasks"},
    {"code": "quality_breaches", "label": "Communication quality breaches"},
]


@dataclass(frozen=True)
class PayrollComputation:
    employee: Employee
    base_salary_amount: Decimal
    commission_amount: Decimal
    bonus_amount: Decimal
    penalty_amount: Decimal
    gross_amount: Decimal
    net_amount: Decimal
    summary_json: dict[str, object]


class PayrollService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_periods(self, context: TenantContext) -> list[PayrollPeriod]:
        account_id = require_account_id(context)
        return self.session.execute(
            select(PayrollPeriod)
            .where(PayrollPeriod.account_id == account_id)
            .order_by(PayrollPeriod.period_start.desc(), PayrollPeriod.id.desc())
        ).scalars().all()

    def list_entries(self, context: TenantContext, payroll_period_id: int | None = None) -> list[PayrollEntry]:
        account_id = require_account_id(context)
        stmt = select(PayrollEntry).where(PayrollEntry.account_id == account_id)
        if payroll_period_id is not None:
            stmt = stmt.where(PayrollEntry.payroll_period_id == payroll_period_id)
        return self.session.execute(stmt.order_by(PayrollEntry.id.desc())).scalars().all()

    def list_kpis(self, context: TenantContext, *, period_start: date | None = None, period_end: date | None = None) -> list[EmployeeKPI]:
        account_id = require_account_id(context)
        stmt = select(EmployeeKPI).where(EmployeeKPI.account_id == account_id)
        if period_start is not None and period_end is not None:
            stmt = stmt.where(EmployeeKPI.period_start == period_start, EmployeeKPI.period_end == period_end)
        return self.session.execute(stmt.order_by(EmployeeKPI.employee_id.asc(), EmployeeKPI.metric_code.asc())).scalars().all()

    def update_employee_compensation(
        self,
        context: TenantContext,
        *,
        employee_id: int,
        base_salary: Decimal,
        commission_rate_pct: Decimal,
        kpi_bonus_amount: Decimal,
        penalty_per_overdue_task: Decimal,
        penalty_per_quality_breach: Decimal,
    ) -> Employee:
        employee = self._employee(context, employee_id)
        for value in [base_salary, commission_rate_pct, kpi_bonus_amount, penalty_per_overdue_task, penalty_per_quality_breach]:
            if value < 0:
                raise PlatformCoreError("Compensation values cannot be negative.")
        employee.base_salary = base_salary
        employee.commission_rate_pct = commission_rate_pct
        employee.kpi_bonus_amount = kpi_bonus_amount
        employee.penalty_per_overdue_task = penalty_per_overdue_task
        employee.penalty_per_quality_breach = penalty_per_quality_breach
        self.session.flush()
        return employee

    def create_period(
        self,
        context: TenantContext,
        *,
        period_kind: str,
        period_start: date,
        period_end: date,
        notes: str | None,
    ) -> PayrollPeriod:
        account_id = require_account_id(context)
        if period_end < period_start:
            raise PlatformCoreError("Payroll period end cannot be before start.")
        period = PayrollPeriod(
            account_id=account_id,
            period_kind=period_kind,
            period_start=period_start,
            period_end=period_end,
            status="draft",
            notes_json={"notes": notes or ""},
        )
        self.session.add(period)
        self.session.flush()
        return period

    def upsert_kpi(
        self,
        context: TenantContext,
        *,
        employee_id: int,
        metric_code: str,
        period_start: date,
        period_end: date,
        actual_value: Decimal,
        target_value: Decimal | None,
        source_kind: str,
        payload_json: dict[str, object] | None = None,
    ) -> EmployeeKPI:
        account_id = require_account_id(context)
        self._employee(context, employee_id)
        metric_code = metric_code.strip()
        if not metric_code:
            raise PlatformCoreError("KPI metric code is required.")
        existing = self.session.execute(
            select(EmployeeKPI).where(
                EmployeeKPI.account_id == account_id,
                EmployeeKPI.employee_id == employee_id,
                EmployeeKPI.metric_code == metric_code,
                EmployeeKPI.period_start == period_start,
                EmployeeKPI.period_end == period_end,
            )
        ).scalar_one_or_none()
        score_pct = None
        if target_value is not None and target_value > 0:
            score_pct = (actual_value / target_value) * Decimal("100")
        if existing is None:
            existing = EmployeeKPI(
                account_id=account_id,
                employee_id=employee_id,
                metric_code=metric_code,
                period_start=period_start,
                period_end=period_end,
                source_kind=source_kind,
            )
            self.session.add(existing)
        existing.actual_value = actual_value
        existing.target_value = target_value
        existing.score_pct = score_pct
        existing.source_kind = source_kind
        existing.payload_json = payload_json or {}
        self.session.flush()
        return existing

    def ensure_derived_kpis(self, context: TenantContext, payroll_period: PayrollPeriod) -> list[EmployeeKPI]:
        account_id = require_account_id(context)
        employees = self.session.execute(
            select(Employee).where(Employee.account_id == account_id, Employee.status == "active").order_by(Employee.id.asc())
        ).scalars().all()
        period_start_dt = datetime.combine(payroll_period.period_start, time.min, tzinfo=timezone.utc)
        period_end_dt = datetime.combine(payroll_period.period_end, time.max, tzinfo=timezone.utc)
        rows: list[EmployeeKPI] = []
        for employee in employees:
            completed_tasks = self.session.execute(
                select(Task).where(
                    Task.account_id == account_id,
                    Task.assignee_employee_id == employee.id,
                    Task.status == "done",
                    Task.completed_at.is_not(None),
                    Task.completed_at >= period_start_dt,
                    Task.completed_at <= period_end_dt,
                )
            ).scalars().all()
            overdue_tasks = self.session.execute(
                select(Task).where(
                    Task.account_id == account_id,
                    Task.assignee_employee_id == employee.id,
                    Task.status == "open",
                    Task.due_at.is_not(None),
                    Task.due_at <= period_end_dt,
                )
            ).scalars().all()
            quality_breaches = self.session.execute(
                select(CommunicationReview).where(
                    CommunicationReview.account_id == account_id,
                    CommunicationReview.employee_id == employee.id,
                    CommunicationReview.created_at >= period_start_dt,
                    CommunicationReview.created_at <= period_end_dt,
                    CommunicationReview.quality_status.in_(["warning", "critical"]),
                )
            ).scalars().all()
            rows.append(
                self.upsert_kpi(
                    context,
                    employee_id=employee.id,
                    metric_code="completed_tasks",
                    period_start=payroll_period.period_start,
                    period_end=payroll_period.period_end,
                    actual_value=Decimal(len(completed_tasks)),
                    target_value=None,
                    source_kind="derived",
                )
            )
            rows.append(
                self.upsert_kpi(
                    context,
                    employee_id=employee.id,
                    metric_code="overdue_tasks",
                    period_start=payroll_period.period_start,
                    period_end=payroll_period.period_end,
                    actual_value=Decimal(len(overdue_tasks)),
                    target_value=None,
                    source_kind="derived",
                )
            )
            rows.append(
                self.upsert_kpi(
                    context,
                    employee_id=employee.id,
                    metric_code="quality_breaches",
                    period_start=payroll_period.period_start,
                    period_end=payroll_period.period_end,
                    actual_value=Decimal(len(quality_breaches)),
                    target_value=None,
                    source_kind="derived",
                )
            )
        return rows

    def compute_period(self, context: TenantContext, payroll_period_id: int) -> list[PayrollEntry]:
        payroll_period = self._period(context, payroll_period_id)
        if payroll_period.status == "paid":
            raise PlatformCoreError("Paid payroll period cannot be recomputed.")
        self.ensure_derived_kpis(context, payroll_period)
        account_id = require_account_id(context)
        employees = self.session.execute(
            select(Employee).where(Employee.account_id == account_id, Employee.status == "active").order_by(Employee.id.asc())
        ).scalars().all()
        entries: list[PayrollEntry] = []
        for employee in employees:
            computation = self._compute_employee_payroll(context, employee, payroll_period)
            entry = self.session.execute(
                select(PayrollEntry).where(
                    PayrollEntry.account_id == account_id,
                    PayrollEntry.payroll_period_id == payroll_period.id,
                    PayrollEntry.employee_id == employee.id,
                )
            ).scalar_one_or_none()
            if entry is None:
                entry = PayrollEntry(
                    account_id=account_id,
                    payroll_period_id=payroll_period.id,
                    employee_id=employee.id,
                )
                self.session.add(entry)
            entry.status = "draft"
            entry.base_salary_amount = computation.base_salary_amount
            entry.commission_amount = computation.commission_amount
            entry.bonus_amount = computation.bonus_amount
            entry.penalty_amount = computation.penalty_amount
            entry.gross_amount = computation.gross_amount
            entry.net_amount = computation.net_amount
            entry.summary_json = computation.summary_json
            entries.append(entry)
        payroll_period.status = "draft"
        self.session.flush()
        return entries

    def set_period_status(
        self,
        context: TenantContext,
        *,
        payroll_period_id: int,
        status_code: str,
        approved_by_user_id: int | None,
    ) -> PayrollPeriod:
        payroll_period = self._period(context, payroll_period_id)
        if status_code not in {"draft", "approved", "paid"}:
            raise PlatformCoreError("Unsupported payroll period status.")
        payroll_period.status = status_code
        if status_code == "approved":
            payroll_period.approved_by_user_id = approved_by_user_id
        if status_code == "paid":
            payroll_period.approved_by_user_id = approved_by_user_id
            payroll_period.paid_at = datetime.now(timezone.utc)
            for entry in self.list_entries(context, payroll_period_id=payroll_period.id):
                entry.status = "paid"
        elif status_code == "approved":
            for entry in self.list_entries(context, payroll_period_id=payroll_period.id):
                entry.status = "approved"
        else:
            payroll_period.paid_at = None
            for entry in self.list_entries(context, payroll_period_id=payroll_period.id):
                entry.status = "draft"
        self.session.flush()
        return payroll_period

    def _compute_employee_payroll(self, context: TenantContext, employee: Employee, payroll_period: PayrollPeriod) -> PayrollComputation:
        kpis = {
            item.metric_code: item
            for item in self.list_kpis(
                context,
                period_start=payroll_period.period_start,
                period_end=payroll_period.period_end,
            )
            if item.employee_id == employee.id
        }
        revenue_generated = Decimal(str(kpis.get("revenue_generated").actual_value if kpis.get("revenue_generated") is not None else "0"))
        overdue_tasks = Decimal(str(kpis.get("overdue_tasks").actual_value if kpis.get("overdue_tasks") is not None else "0"))
        quality_breaches = Decimal(str(kpis.get("quality_breaches").actual_value if kpis.get("quality_breaches") is not None else "0"))
        score_values = [
            Decimal(str(item.score_pct))
            for item in kpis.values()
            if item.score_pct is not None and item.metric_code != "revenue_generated"
        ]
        average_score = sum(score_values, Decimal("0")) / Decimal(len(score_values)) if score_values else None
        base_salary_amount = Decimal(employee.base_salary)
        commission_amount = (revenue_generated * Decimal(employee.commission_rate_pct) / Decimal("100")).quantize(Decimal("0.01"))
        if average_score is None:
            bonus_amount = Decimal("0.00")
        else:
            ratio = min(Decimal("1.50"), max(Decimal("0.00"), average_score / Decimal("100")))
            bonus_amount = (Decimal(employee.kpi_bonus_amount) * ratio).quantize(Decimal("0.01"))
        penalty_amount = (
            overdue_tasks * Decimal(employee.penalty_per_overdue_task)
            + quality_breaches * Decimal(employee.penalty_per_quality_breach)
        ).quantize(Decimal("0.01"))
        gross_amount = (base_salary_amount + commission_amount + bonus_amount).quantize(Decimal("0.01"))
        net_amount = max(Decimal("0.00"), gross_amount - penalty_amount).quantize(Decimal("0.01"))
        return PayrollComputation(
            employee=employee,
            base_salary_amount=base_salary_amount,
            commission_amount=commission_amount,
            bonus_amount=bonus_amount,
            penalty_amount=penalty_amount,
            gross_amount=gross_amount,
            net_amount=net_amount,
            summary_json={
                "employee_name": employee.full_name,
                "period_start": payroll_period.period_start.isoformat(),
                "period_end": payroll_period.period_end.isoformat(),
                "revenue_generated": str(revenue_generated),
                "overdue_tasks": str(overdue_tasks),
                "quality_breaches": str(quality_breaches),
                "average_score_pct": str(average_score.quantize(Decimal("0.01"))) if average_score is not None else None,
            },
        )

    def _employee(self, context: TenantContext, employee_id: int) -> Employee:
        account_id = require_account_id(context)
        employee = self.session.execute(
            select(Employee).where(Employee.account_id == account_id, Employee.id == employee_id)
        ).scalar_one_or_none()
        if employee is None:
            raise TenantContextError("Employee not found in selected account.")
        return employee

    def _period(self, context: TenantContext, payroll_period_id: int) -> PayrollPeriod:
        account_id = require_account_id(context)
        payroll_period = self.session.execute(
            select(PayrollPeriod).where(PayrollPeriod.account_id == account_id, PayrollPeriod.id == payroll_period_id)
        ).scalar_one_or_none()
        if payroll_period is None:
            raise TenantContextError("Payroll period not found in selected account.")
        return payroll_period
