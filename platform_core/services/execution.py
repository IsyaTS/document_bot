from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.models import Employee, PayrollEntry, PayrollPayment, Task, TaskCheckin, TaskEvent
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class EmployeeExecutionSummary:
    employee: Employee
    recent_checkins: list[TaskCheckin]
    blocker_count: int
    resolution_count: int


class ExecutionService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_task_checkins(
        self,
        context: TenantContext,
        *,
        task_id: int | None = None,
        employee_id: int | None = None,
    ) -> list[TaskCheckin]:
        account_id = require_account_id(context)
        stmt = select(TaskCheckin).where(TaskCheckin.account_id == account_id)
        if task_id is not None:
            stmt = stmt.where(TaskCheckin.task_id == task_id)
        if employee_id is not None:
            stmt = stmt.where(TaskCheckin.employee_id == employee_id)
        return self.session.execute(stmt.order_by(TaskCheckin.created_at.desc(), TaskCheckin.id.desc())).scalars().all()

    def create_task_checkin(
        self,
        context: TenantContext,
        *,
        task_id: int,
        actor_user_id: int | None,
        employee_id: int | None,
        checkin_type: str,
        note_text: str | None,
        status_after: str | None = None,
    ) -> tuple[Task, TaskCheckin]:
        account_id = require_account_id(context)
        task = self._task(account_id, task_id)
        if employee_id is not None:
            self._employee(account_id, employee_id)
        normalized_type = (checkin_type or "progress").strip() or "progress"
        if normalized_type not in {"progress", "blocker", "resolution", "review"}:
            raise PlatformCoreError("Unsupported task check-in type.")
        normalized_status = (status_after or "").strip() or None
        if normalized_status not in {None, "open", "done"}:
            raise PlatformCoreError("Unsupported task status transition.")
        note = (note_text or "").strip() or None
        if normalized_type in {"blocker", "resolution"} and not note:
            raise PlatformCoreError("A note is required for blocker or resolution check-ins.")
        checkin = TaskCheckin(
            account_id=account_id,
            task_id=task.id,
            actor_user_id=actor_user_id,
            employee_id=employee_id or task.assignee_employee_id,
            checkin_type=normalized_type,
            note_text=note,
            status_after=normalized_status,
            payload_json={"source": "execution-discipline"},
        )
        self.session.add(checkin)
        if normalized_status is not None:
            task.status = normalized_status
            task.completed_at = datetime.now(timezone.utc) if normalized_status == "done" else None
        self.session.add(
            TaskEvent(
                account_id=account_id,
                task_id=task.id,
                actor_user_id=actor_user_id,
                event_type=f"task.checkin.{normalized_type}",
                event_at=datetime.now(timezone.utc),
                payload_json={"checkin_type": normalized_type, "status_after": normalized_status, "note": note},
            )
        )
        self.session.flush()
        return task, checkin

    def employee_execution_summary(self, context: TenantContext, employee_id: int) -> EmployeeExecutionSummary:
        employee = self._employee(require_account_id(context), employee_id)
        checkins = self.list_task_checkins(context, employee_id=employee_id)[:12]
        blocker_count = sum(1 for item in checkins if item.checkin_type == "blocker")
        resolution_count = sum(1 for item in checkins if item.checkin_type == "resolution")
        return EmployeeExecutionSummary(
            employee=employee,
            recent_checkins=checkins,
            blocker_count=blocker_count,
            resolution_count=resolution_count,
        )

    def list_payroll_payments(self, context: TenantContext, *, payroll_entry_id: int | None = None) -> list[PayrollPayment]:
        account_id = require_account_id(context)
        stmt = select(PayrollPayment).where(PayrollPayment.account_id == account_id)
        if payroll_entry_id is not None:
            stmt = stmt.where(PayrollPayment.payroll_entry_id == payroll_entry_id)
        return self.session.execute(stmt.order_by(PayrollPayment.payment_date.desc(), PayrollPayment.id.desc())).scalars().all()

    def record_payroll_payment(
        self,
        context: TenantContext,
        *,
        payroll_entry_id: int,
        recorded_by_user_id: int | None,
        payment_date_value: date,
        amount: Decimal,
        payment_ref: str | None,
        status_code: str = "recorded",
    ) -> tuple[PayrollEntry, PayrollPayment]:
        account_id = require_account_id(context)
        entry = self._payroll_entry(account_id, payroll_entry_id)
        if amount <= 0:
            raise PlatformCoreError("Payroll payment amount must be positive.")
        if status_code not in {"recorded", "confirmed"}:
            raise PlatformCoreError("Unsupported payroll payment status.")
        payment = PayrollPayment(
            account_id=account_id,
            payroll_entry_id=entry.id,
            recorded_by_user_id=recorded_by_user_id,
            payment_date=payment_date_value,
            amount=amount,
            payment_ref=(payment_ref or "").strip() or None,
            status=status_code,
            payload_json={"net_amount": str(entry.net_amount)},
        )
        self.session.add(payment)
        paid_total = sum(
            Decimal(item.amount)
            for item in self.list_payroll_payments(context, payroll_entry_id=entry.id)
        ) + amount
        if paid_total >= Decimal(entry.net_amount):
            entry.status = "paid"
        self.session.flush()
        return entry, payment

    def _task(self, account_id: int, task_id: int) -> Task:
        task = self.session.execute(
            select(Task).where(Task.account_id == account_id, Task.id == task_id)
        ).scalar_one_or_none()
        if task is None:
            raise TenantContextError("Task not found in selected account.")
        return task

    def _employee(self, account_id: int, employee_id: int) -> Employee:
        employee = self.session.execute(
            select(Employee).where(Employee.account_id == account_id, Employee.id == employee_id)
        ).scalar_one_or_none()
        if employee is None:
            raise TenantContextError("Employee not found in selected account.")
        return employee

    def _payroll_entry(self, account_id: int, payroll_entry_id: int) -> PayrollEntry:
        entry = self.session.execute(
            select(PayrollEntry).where(PayrollEntry.account_id == account_id, PayrollEntry.id == payroll_entry_id)
        ).scalar_one_or_none()
        if entry is None:
            raise TenantContextError("Payroll entry not found in selected account.")
        return entry
