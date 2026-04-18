from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.models import (
    Account,
    AccountUser,
    AdMetric,
    Alert,
    BalanceSnapshot,
    BankAccount,
    DailyKPI,
    Deal,
    Expense,
    Goal,
    GoalTarget,
    Lead,
    Product,
    Rule,
    RuleExecution,
    StockItem,
    Task,
    User,
)
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class GoalMetricDefinition:
    code: str
    label: str
    direction: str
    unit: str


GOAL_METRIC_DEFINITIONS: dict[str, GoalMetricDefinition] = {
    "revenue": GoalMetricDefinition(code="revenue", label="Revenue", direction="min", unit="currency"),
    "net_profit": GoalMetricDefinition(code="net_profit", label="Net Profit", direction="min", unit="currency"),
    "incoming_leads": GoalMetricDefinition(code="incoming_leads", label="Incoming Leads", direction="min", unit="count"),
    "lost_leads": GoalMetricDefinition(code="lost_leads", label="Lost Leads", direction="max", unit="count"),
    "cpl": GoalMetricDefinition(code="cpl", label="CPL", direction="max", unit="currency"),
    "first_response_breaches": GoalMetricDefinition(
        code="first_response_breaches",
        label="First Response Breaches",
        direction="max",
        unit="count",
    ),
    "low_stock_items": GoalMetricDefinition(code="low_stock_items", label="Low Stock Items", direction="max", unit="count"),
    "available_cash": GoalMetricDefinition(code="available_cash", label="Available Cash", direction="min", unit="currency"),
}


class GoalService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_goal_metric_definitions(self) -> list[GoalMetricDefinition]:
        return list(GOAL_METRIC_DEFINITIONS.values())

    def list_goals(self, context: TenantContext, *, status: str | None = None) -> list[Goal]:
        account_id = require_account_id(context)
        query = select(Goal).where(Goal.account_id == account_id)
        if status:
            query = query.where(Goal.status == status)
        return self.session.execute(
            query.order_by(Goal.period_start.desc(), Goal.id.desc())
        ).scalars().all()

    def list_current_goals(self, context: TenantContext, *, on_date: date | None = None) -> list[Goal]:
        account_id = require_account_id(context)
        today = on_date or datetime.now(timezone.utc).date()
        return self.session.execute(
            select(Goal)
            .where(
                Goal.account_id == account_id,
                Goal.status == "active",
                Goal.period_start <= today,
                Goal.period_end >= today,
            )
            .order_by(Goal.is_primary.desc(), Goal.period_start.asc(), Goal.id.asc())
        ).scalars().all()

    def get_goal(self, context: TenantContext, goal_id: int) -> Goal:
        account_id = require_account_id(context)
        goal = self.session.execute(
            select(Goal).where(Goal.account_id == account_id, Goal.id == goal_id)
        ).scalar_one_or_none()
        if goal is None:
            raise TenantContextError("Goal not found in selected account.")
        return goal

    def create_goal(
        self,
        context: TenantContext,
        *,
        title: str,
        description: str | None,
        period_kind: str,
        period_start: date,
        period_end: date,
        owner_user_id: int | None,
        is_primary: bool,
        status: str,
        targets: list[dict[str, object]],
    ) -> Goal:
        account_id = require_account_id(context)
        self._validate_period(period_kind, period_start, period_end)
        owner = self._resolve_owner(account_id, owner_user_id)
        goal = Goal(
            account_id=account_id,
            owner_user_id=owner.id if owner is not None else None,
            title=title.strip(),
            description=(description or "").strip() or None,
            period_kind=period_kind,
            period_start=period_start,
            period_end=period_end,
            is_primary=is_primary,
            status=status,
            settings_json={},
        )
        self.session.add(goal)
        self.session.flush()
        self._replace_targets(goal, targets)
        self.session.flush()
        return goal

    def update_goal(
        self,
        context: TenantContext,
        goal_id: int,
        *,
        title: str | None = None,
        description: str | None = None,
        period_kind: str | None = None,
        period_start: date | None = None,
        period_end: date | None = None,
        owner_user_id: int | None = None,
        is_primary: bool | None = None,
        status: str | None = None,
        targets: list[dict[str, object]] | None = None,
    ) -> Goal:
        goal = self.get_goal(context, goal_id)
        next_period_kind = period_kind or goal.period_kind
        next_period_start = period_start or goal.period_start
        next_period_end = period_end or goal.period_end
        self._validate_period(next_period_kind, next_period_start, next_period_end)

        if title is not None:
            goal.title = title.strip()
        if description is not None:
            goal.description = description.strip() or None
        if period_kind is not None:
            goal.period_kind = period_kind
        if period_start is not None:
            goal.period_start = period_start
        if period_end is not None:
            goal.period_end = period_end
        if owner_user_id is not None:
            owner = self._resolve_owner(goal.account_id, owner_user_id)
            goal.owner_user_id = owner.id if owner is not None else None
        if is_primary is not None:
            goal.is_primary = is_primary
        if status is not None:
            goal.status = status
        if targets is not None:
            self._replace_targets(goal, targets)
        self.session.flush()
        return goal

    def get_goal_metrics(self, context: TenantContext, goal_id: int) -> dict[str, object]:
        goal = self.get_goal(context, goal_id)
        targets = self._load_targets(goal.id)
        metric_rows = [self._build_metric_row(goal, target) for target in targets]
        return {
            "goal": goal,
            "targets": targets,
            "metrics": metric_rows,
            "summary": self._goal_summary(metric_rows),
        }

    def get_dashboard_goal_snapshot(self, context: TenantContext) -> list[dict[str, object]]:
        payloads: list[dict[str, object]] = []
        for goal in self.list_current_goals(context):
            snapshot = self.get_goal_metrics(context, goal.id)
            payloads.append(
                {
                    "goal": snapshot["goal"],
                    "metrics": snapshot["metrics"],
                    "summary": snapshot["summary"],
                }
            )
        return payloads

    def _load_targets(self, goal_id: int) -> list[GoalTarget]:
        goal = self.session.get(Goal, goal_id)
        if goal is None:
            return []
        return self.session.execute(
            select(GoalTarget)
            .where(GoalTarget.account_id == goal.account_id, GoalTarget.goal_id == goal_id)
            .order_by(GoalTarget.metric_code.asc(), GoalTarget.id.asc())
        ).scalars().all()

    def _replace_targets(self, goal: Goal, targets: list[dict[str, object]]) -> None:
        normalized = self._normalize_targets(targets)
        existing = self._load_targets(goal.id)
        for row in existing:
            self.session.delete(row)
        self.session.flush()
        for item in normalized:
            definition = GOAL_METRIC_DEFINITIONS[item["metric_code"]]
            self.session.add(
                GoalTarget(
                    account_id=goal.account_id,
                    goal_id=goal.id,
                    metric_code=item["metric_code"],
                    direction=str(item.get("direction") or definition.direction),
                    target_value=item["target_value"],
                    settings_json={},
                )
            )

    def _normalize_targets(self, targets: list[dict[str, object]]) -> list[dict[str, object]]:
        if not targets:
            raise PlatformCoreError("Goal must include at least one metric target.")
        seen: set[str] = set()
        normalized: list[dict[str, object]] = []
        for item in targets:
            metric_code = str(item.get("metric_code") or "").strip()
            if metric_code not in GOAL_METRIC_DEFINITIONS:
                raise PlatformCoreError(f"Unsupported goal metric: {metric_code or '<empty>'}.")
            if metric_code in seen:
                raise PlatformCoreError(f"Duplicate goal metric: {metric_code}.")
            raw_target = item.get("target_value")
            if raw_target in (None, ""):
                continue
            target_value = Decimal(str(raw_target))
            normalized.append(
                {
                    "metric_code": metric_code,
                    "target_value": target_value,
                    "direction": str(item.get("direction") or GOAL_METRIC_DEFINITIONS[metric_code].direction),
                }
            )
            seen.add(metric_code)
        if not normalized:
            raise PlatformCoreError("Goal must include at least one non-empty metric target.")
        return normalized

    def _resolve_owner(self, account_id: int, owner_user_id: int | None) -> User | None:
        if owner_user_id is None:
            return None
        owner = self.session.execute(
            select(User).where(User.id == owner_user_id)
        ).scalar_one_or_none()
        if owner is None:
            raise PlatformCoreError("Goal owner user could not be resolved.")
        membership = self.session.execute(
            select(AccountUser).where(
                AccountUser.account_id == account_id,
                AccountUser.user_id == owner.id,
                AccountUser.status == "active",
            )
        ).scalar_one_or_none()
        if membership is None:
            raise PlatformCoreError("Goal owner user is not an active member of the selected account.")
        return owner

    def _goal_summary(self, metrics: list[dict[str, object]]) -> dict[str, object]:
        if not metrics:
            return {"status": "warning", "critical_count": 0, "warning_count": 0, "on_track_count": 0}
        critical_count = sum(1 for metric in metrics if metric["status"] == "critical")
        warning_count = sum(1 for metric in metrics if metric["status"] == "warning")
        on_track_count = sum(1 for metric in metrics if metric["status"] == "on_track")
        if critical_count:
            status = "critical"
        elif warning_count:
            status = "warning"
        else:
            status = "on_track"
        return {
            "status": status,
            "critical_count": critical_count,
            "warning_count": warning_count,
            "on_track_count": on_track_count,
        }

    def _build_metric_row(self, goal: Goal, target: GoalTarget) -> dict[str, object]:
        definition = GOAL_METRIC_DEFINITIONS[target.metric_code]
        actual = self._actual_metric_value(goal.account_id, target.metric_code, goal.period_start, goal.period_end)
        target_value = self._decimal(target.target_value)
        delta = actual - target_value
        status = self._metric_status(actual=actual, target_value=target_value, direction=target.direction)
        return {
            "metric_code": target.metric_code,
            "label": definition.label,
            "unit": definition.unit,
            "direction": target.direction,
            "target": self._num(target_value),
            "actual": self._num(actual),
            "delta": self._num(delta),
            "status": status,
        }

    def _metric_status(self, *, actual: Decimal, target_value: Decimal, direction: str) -> str:
        if direction == "max":
            if actual <= target_value:
                return "on_track"
            if target_value <= Decimal("0"):
                return "critical"
            if actual <= target_value * Decimal("1.15"):
                return "warning"
            return "critical"
        if actual >= target_value:
            return "on_track"
        if target_value <= Decimal("0"):
            return "warning" if actual == Decimal("0") else "critical"
        if actual >= target_value * Decimal("0.85"):
            return "warning"
        return "critical"

    def _actual_metric_value(self, account_id: int, metric_code: str, period_start: date, period_end: date) -> Decimal:
        account = self.session.execute(select(Account).where(Account.id == account_id)).scalar_one()
        period_start_at, period_end_at = self._period_bounds(account, period_start, period_end)

        if metric_code == "revenue":
            return self._metric_revenue(account_id, period_start, period_end, period_start_at, period_end_at)
        if metric_code == "net_profit":
            return self._metric_net_profit(account_id, period_start, period_end, period_start_at, period_end_at)
        if metric_code == "incoming_leads":
            return Decimal(
                self.session.execute(
                    select(func.count())
                    .select_from(Lead)
                    .where(Lead.account_id == account_id, Lead.created_at >= period_start_at, Lead.created_at <= period_end_at)
                ).scalar_one()
            )
        if metric_code == "lost_leads":
            return Decimal(
                self.session.execute(
                    select(func.count())
                    .select_from(Lead)
                    .where(
                        Lead.account_id == account_id,
                        Lead.status == "lost",
                        Lead.updated_at >= period_start_at,
                        Lead.updated_at <= period_end_at,
                    )
                ).scalar_one()
            )
        if metric_code == "cpl":
            spend = self._decimal(
                self.session.execute(
                    select(func.coalesce(func.sum(AdMetric.spend), 0))
                    .where(
                        AdMetric.account_id == account_id,
                        AdMetric.metric_date >= period_start,
                        AdMetric.metric_date <= period_end,
                    )
                ).scalar_one()
            )
            leads_count = int(
                self.session.execute(
                    select(func.coalesce(func.sum(AdMetric.leads_count), 0))
                    .where(
                        AdMetric.account_id == account_id,
                        AdMetric.metric_date >= period_start,
                        AdMetric.metric_date <= period_end,
                    )
                ).scalar_one()
            )
            if leads_count > 0:
                return spend / Decimal(leads_count)
            return spend
        if metric_code == "first_response_breaches":
            return Decimal(
                self.session.execute(
                    select(func.count())
                    .select_from(RuleExecution)
                    .join(Rule, Rule.id == RuleExecution.rule_id)
                    .where(
                        RuleExecution.account_id == account_id,
                        Rule.code == "lead.no_first_response",
                        RuleExecution.last_triggered_at >= period_start_at,
                        RuleExecution.last_triggered_at <= period_end_at,
                    )
                ).scalar_one()
            )
        if metric_code == "low_stock_items":
            return Decimal(self._metric_low_stock_items(account_id))
        if metric_code == "available_cash":
            return self._metric_available_cash(account_id, period_end_at)
        raise PlatformCoreError(f"Unsupported goal metric: {metric_code}.")

    def _metric_revenue(
        self,
        account_id: int,
        period_start: date,
        period_end: date,
        period_start_at: datetime,
        period_end_at: datetime,
    ) -> Decimal:
        kpi_value = self._sum_daily_kpi(account_id, period_start, period_end, "revenue")
        if kpi_value != Decimal("0"):
            return kpi_value
        return self._decimal(
            self.session.execute(
                select(func.coalesce(func.sum(Deal.amount_total), 0))
                .where(
                    Deal.account_id == account_id,
                    Deal.status.in_(("won", "closed")),
                    func.coalesce(Deal.closed_at, Deal.created_at) >= period_start_at,
                    func.coalesce(Deal.closed_at, Deal.created_at) <= period_end_at,
                )
            ).scalar_one()
        )

    def _metric_net_profit(
        self,
        account_id: int,
        period_start: date,
        period_end: date,
        period_start_at: datetime,
        period_end_at: datetime,
    ) -> Decimal:
        kpi_value = self._sum_daily_kpi(account_id, period_start, period_end, "net_profit")
        if kpi_value != Decimal("0"):
            return kpi_value
        gross_profit = self._decimal(
            self.session.execute(
                select(func.coalesce(func.sum(Deal.gross_profit), 0))
                .where(
                    Deal.account_id == account_id,
                    Deal.status.in_(("won", "closed")),
                    func.coalesce(Deal.closed_at, Deal.created_at) >= period_start_at,
                    func.coalesce(Deal.closed_at, Deal.created_at) <= period_end_at,
                )
            ).scalar_one()
        )
        expenses = self._decimal(
            self.session.execute(
                select(func.coalesce(func.sum(Expense.amount), 0))
                .where(
                    Expense.account_id == account_id,
                    Expense.expense_date >= period_start,
                    Expense.expense_date <= period_end,
                )
            ).scalar_one()
        )
        return gross_profit - expenses

    def _metric_low_stock_items(self, account_id: int) -> int:
        rows = self.session.execute(
            select(
                StockItem.quantity_on_hand,
                StockItem.quantity_reserved,
                StockItem.min_quantity,
                Product.min_stock_level,
            )
            .join(Product, Product.id == StockItem.product_id)
            .where(StockItem.account_id == account_id, Product.account_id == account_id)
        ).all()
        low_stock = 0
        for quantity_on_hand, quantity_reserved, min_quantity, min_stock_level in rows:
            available = self._decimal(quantity_on_hand) - self._decimal(quantity_reserved)
            threshold = self._decimal(min_quantity)
            if threshold <= Decimal("0"):
                threshold = self._decimal(min_stock_level)
            if available < threshold:
                low_stock += 1
        return low_stock

    def _metric_available_cash(self, account_id: int, period_end_at: datetime) -> Decimal:
        bank_accounts = self.session.execute(
            select(BankAccount.id).where(BankAccount.account_id == account_id, BankAccount.status == "active")
        ).scalars().all()
        if not bank_accounts:
            return Decimal("0")
        rows = self.session.execute(
            select(BalanceSnapshot)
            .where(
                BalanceSnapshot.account_id == account_id,
                BalanceSnapshot.bank_account_id.in_(bank_accounts),
                BalanceSnapshot.snapshot_at <= period_end_at,
            )
            .order_by(BalanceSnapshot.bank_account_id.asc(), BalanceSnapshot.snapshot_at.desc())
        ).scalars().all()
        latest_by_account: dict[int, BalanceSnapshot] = {}
        for row in rows:
            latest_by_account.setdefault(row.bank_account_id, row)
        total = Decimal("0")
        for snapshot in latest_by_account.values():
            available = self._decimal(snapshot.available_balance)
            if available == Decimal("0"):
                available = self._decimal(snapshot.balance)
            total += available
        return total

    def _sum_daily_kpi(self, account_id: int, period_start: date, period_end: date, metric_code: str) -> Decimal:
        return self._decimal(
            self.session.execute(
                select(func.coalesce(func.sum(DailyKPI.value_numeric), 0))
                .where(
                    DailyKPI.account_id == account_id,
                    DailyKPI.kpi_date >= period_start,
                    DailyKPI.kpi_date <= period_end,
                    DailyKPI.metric_code == metric_code,
                )
            ).scalar_one()
        )

    def _validate_period(self, period_kind: str, period_start: date, period_end: date) -> None:
        if period_kind not in {"day", "week", "month"}:
            raise PlatformCoreError("Goal period_kind must be one of: day, week, month.")
        if period_end < period_start:
            raise PlatformCoreError("Goal period_end cannot be earlier than period_start.")

    def _period_bounds(self, account: Account, period_start: date, period_end: date) -> tuple[datetime, datetime]:
        try:
            zone = ZoneInfo(account.default_timezone)
        except Exception:
            zone = timezone.utc
        start_at = datetime.combine(period_start, time.min, tzinfo=zone).astimezone(timezone.utc)
        end_at = datetime.combine(period_end, time.max, tzinfo=zone).astimezone(timezone.utc)
        return start_at, end_at

    def _decimal(self, value: object | None) -> Decimal:
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def _num(self, value: Decimal) -> float:
        return float(value.quantize(Decimal("0.01")))
