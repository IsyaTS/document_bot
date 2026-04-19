from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.models import Alert, Employee, Task, User
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class EmployeeSnapshot:
    employee: Employee
    open_tasks: int
    overdue_tasks: int
    completed_7d: int
    completed_30d: int
    avg_completion_hours_30d: float | None
    open_alerts: int
    status: str


class PeopleService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_employees(self, context: TenantContext, *, status: str | None = None) -> list[Employee]:
        account_id = require_account_id(context)
        query = select(Employee).where(Employee.account_id == account_id)
        if status:
            query = query.where(Employee.status == status)
        return self.session.execute(query.order_by(Employee.full_name.asc(), Employee.id.asc())).scalars().all()

    def get_employee(self, context: TenantContext, employee_id: int) -> Employee:
        account_id = require_account_id(context)
        employee = self.session.execute(
            select(Employee).where(Employee.account_id == account_id, Employee.id == employee_id)
        ).scalar_one_or_none()
        if employee is None:
            raise TenantContextError("Employee not found in selected account.")
        return employee

    def upsert_employee(
        self,
        context: TenantContext,
        *,
        employee_id: int | None = None,
        user_id: int | None = None,
        employee_code: str | None = None,
        full_name: str,
        role_title: str | None = None,
        department: str | None = None,
        email: str | None = None,
        phone: str | None = None,
        status: str = "active",
    ) -> Employee:
        account_id = require_account_id(context)
        normalized_name = full_name.strip()
        if not normalized_name:
            raise PlatformCoreError("Employee full name is required.")
        if status not in {"active", "disabled"}:
            raise PlatformCoreError("Unsupported employee status.")
        if user_id is not None:
            user = self.session.get(User, user_id)
            if user is None:
                raise PlatformCoreError("Linked user not found.")
        if employee_id is not None:
            employee = self.get_employee(context, employee_id)
        else:
            employee = Employee(account_id=account_id)
            self.session.add(employee)
        employee.user_id = user_id
        employee.employee_code = employee_code.strip() if employee_code else None
        employee.full_name = normalized_name
        employee.role_title = role_title.strip() if role_title else None
        employee.department = department.strip() if department else None
        employee.email = email.strip().lower() if email else None
        employee.phone = phone.strip() if phone else None
        employee.status = status
        self.session.flush()
        return employee

    def employee_snapshots(self, context: TenantContext) -> list[EmployeeSnapshot]:
        account_id = require_account_id(context)
        employees = self.list_employees(context)
        tasks = self.session.execute(select(Task).where(Task.account_id == account_id)).scalars().all()
        alerts = self.session.execute(
            select(Alert).where(Alert.account_id == account_id, Alert.status == "open")
        ).scalars().all()
        now = datetime.now(timezone.utc)
        completed_7d_start = now - timedelta(days=7)
        completed_30d_start = now - timedelta(days=30)
        snapshots: list[EmployeeSnapshot] = []
        for employee in employees:
            employee_tasks = [
                item
                for item in tasks
                if item.assignee_employee_id == employee.id or (employee.user_id is not None and item.assignee_user_id == employee.user_id)
            ]
            open_tasks = [item for item in employee_tasks if item.status == "open"]
            overdue_tasks = [
                item for item in open_tasks if item.due_at is not None and self._dt(item.due_at) <= now
            ]
            completed_tasks = [item for item in employee_tasks if item.completed_at is not None]
            completed_7d = [item for item in completed_tasks if self._dt(item.completed_at) >= completed_7d_start]
            completed_30d = [item for item in completed_tasks if self._dt(item.completed_at) >= completed_30d_start]
            completion_durations = []
            for item in completed_30d:
                created_at = self._dt(item.created_at)
                completed_at = self._dt(item.completed_at)
                if completed_at >= created_at:
                    completion_durations.append((completed_at - created_at).total_seconds() / 3600)
            open_alerts = [
                item
                for item in alerts
                if employee.user_id is not None and item.assigned_user_id == employee.user_id
            ]
            if employee.status != "active":
                status_code = "disabled"
            elif overdue_tasks or len(open_alerts) >= 2:
                status_code = "critical"
            elif len(open_tasks) >= 4 or open_alerts:
                status_code = "warning"
            else:
                status_code = "healthy"
            snapshots.append(
                EmployeeSnapshot(
                    employee=employee,
                    open_tasks=len(open_tasks),
                    overdue_tasks=len(overdue_tasks),
                    completed_7d=len(completed_7d),
                    completed_30d=len(completed_30d),
                    avg_completion_hours_30d=(sum(completion_durations) / len(completion_durations)) if completion_durations else None,
                    open_alerts=len(open_alerts),
                    status=status_code,
                )
            )
        snapshots.sort(key=lambda item: (-self._status_weight(item.status), item.employee.full_name.lower(), item.employee.id))
        return snapshots

    def count_active_employees(self, account_id: int) -> int:
        return len(
            self.session.execute(
                select(Employee.id).where(Employee.account_id == account_id, Employee.status == "active")
            ).all()
        )

    def _dt(self, value: datetime | None) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _status_weight(self, status_code: str) -> int:
        return {"critical": 3, "warning": 2, "healthy": 1, "disabled": 0}.get(status_code, 0)
