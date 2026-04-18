from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from platform_core.dashboard_defaults import DEFAULT_DASHBOARD
from platform_core.models import (
    Account,
    AdMetric,
    Alert,
    BalanceSnapshot,
    BankAccount,
    BankTransaction,
    Campaign,
    DailyKPI,
    DashboardConfig,
    DashboardWidgetConfig,
    Deal,
    Expense,
    Lead,
    Product,
    Recommendation,
    Rule,
    StockItem,
    Task,
    ThresholdConfig,
)
from platform_core.services.authz import AuthorizationService
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class DashboardPeriod:
    code: str
    label: str
    start_at: datetime
    end_at: datetime
    start_date: date
    end_date: date
    timezone_name: str


class DashboardCatalogService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def seed_default_dashboard(self, context: TenantContext) -> DashboardConfig:
        account_id = require_account_id(context)
        dashboard = self.session.execute(
            select(DashboardConfig).where(
                DashboardConfig.account_id == account_id,
                DashboardConfig.code == DEFAULT_DASHBOARD["code"],
            )
        ).scalar_one_or_none()
        if dashboard is None:
            dashboard = DashboardConfig(
                account_id=account_id,
                code=DEFAULT_DASHBOARD["code"],
                name=DEFAULT_DASHBOARD["name"],
                status="active",
                settings_json=DEFAULT_DASHBOARD["settings"],
            )
            self.session.add(dashboard)
            try:
                self.session.flush()
            except IntegrityError:
                self.session.rollback()
                dashboard = self.session.execute(
                    select(DashboardConfig).where(
                        DashboardConfig.account_id == account_id,
                        DashboardConfig.code == DEFAULT_DASHBOARD["code"],
                    )
                ).scalar_one()

        for widget in DEFAULT_DASHBOARD["widgets"]:
            existing = self.session.execute(
                select(DashboardWidgetConfig).where(
                    DashboardWidgetConfig.account_id == account_id,
                    DashboardWidgetConfig.dashboard_config_id == dashboard.id,
                    DashboardWidgetConfig.widget_key == widget["key"],
                )
            ).scalar_one_or_none()
            if existing is None:
                self.session.add(
                    DashboardWidgetConfig(
                        account_id=account_id,
                        dashboard_config_id=dashboard.id,
                        widget_key=widget["key"],
                        title=widget["title"],
                        position=widget["position"],
                        is_enabled=True,
                        settings_json={},
                    )
                )
        self.session.flush()
        return dashboard


class ExecutiveDashboardService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._authz = AuthorizationService(session)

    def get_dashboard(self, context: TenantContext, period_code: str = "today") -> dict[str, object]:
        account_id = require_account_id(context)
        self._authz.require(context, "dashboard.read")

        dashboard = self.session.execute(
            select(DashboardConfig).where(
                DashboardConfig.account_id == account_id,
                DashboardConfig.code == DEFAULT_DASHBOARD["code"],
                DashboardConfig.status == "active",
            )
        ).scalar_one_or_none()
        if dashboard is None:
            dashboard = DashboardCatalogService(self.session).seed_default_dashboard(context)

        period = self._resolve_period(account_id, period_code)
        widget_configs = self.session.execute(
            select(DashboardWidgetConfig)
            .where(
                DashboardWidgetConfig.account_id == account_id,
                DashboardWidgetConfig.dashboard_config_id == dashboard.id,
                DashboardWidgetConfig.is_enabled.is_(True),
            )
            .order_by(DashboardWidgetConfig.position.asc(), DashboardWidgetConfig.id.asc())
        ).scalars().all()

        problems = self._top_problems(account_id, period, limit=5)
        money = self._money_widget(account_id, period)
        financial_result = self._financial_result_widget(account_id, period)
        leads_sales = self._leads_sales_widget(account_id, period)
        advertising = self._advertising_widget(account_id, period)
        stock = self._stock_widget(account_id, period)
        management = self._management_widget(account_id, period, problems)
        owner_panel = self._owner_panel_widget(account_id, period, money, financial_result, leads_sales, management, problems)

        widget_payloads = {
            "money": money,
            "financial_result": financial_result,
            "leads_sales": leads_sales,
            "advertising": advertising,
            "stock": stock,
            "management": management,
            "owner_panel": owner_panel,
        }
        widgets: list[dict[str, object]] = []
        for widget in widget_configs:
            widgets.append(
                {
                    "widget_key": widget.widget_key,
                    "title": widget.title,
                    "position": widget.position,
                    "payload": widget_payloads[widget.widget_key],
                }
            )

        return {
            "dashboard_code": dashboard.code,
            "dashboard_name": dashboard.name,
            "account_id": account_id,
            "period": {
                "code": period.code,
                "label": period.label,
                "start_at": period.start_at.isoformat(),
                "end_at": period.end_at.isoformat(),
                "timezone": period.timezone_name,
            },
            "widgets": widgets,
        }

    def _resolve_period(self, account_id: int, period_code: str) -> DashboardPeriod:
        account = self.session.execute(select(Account).where(Account.id == account_id)).scalar_one()
        try:
            zone = ZoneInfo(account.default_timezone)
        except Exception:
            zone = timezone.utc
        now = datetime.now(zone)
        current_date = now.date()

        if period_code == "yesterday":
            day = current_date - timedelta(days=1)
            start = datetime.combine(day, time.min, tzinfo=zone)
            end = datetime.combine(day, time.max, tzinfo=zone)
            label = "Yesterday"
        elif period_code == "week":
            start_date = current_date - timedelta(days=current_date.weekday())
            start = datetime.combine(start_date, time.min, tzinfo=zone)
            end = now
            label = "Week"
        elif period_code == "month":
            start_date = current_date.replace(day=1)
            start = datetime.combine(start_date, time.min, tzinfo=zone)
            end = now
            label = "Month"
        else:
            start = datetime.combine(current_date, time.min, tzinfo=zone)
            end = now
            label = "Today"
            period_code = "today"

        return DashboardPeriod(
            code=period_code,
            label=label,
            start_at=start.astimezone(timezone.utc),
            end_at=end.astimezone(timezone.utc),
            start_date=start.date(),
            end_date=end.date(),
            timezone_name=str(zone),
        )

    def _money_widget(self, account_id: int, period: DashboardPeriod) -> dict[str, object]:
        bank_accounts = self.session.execute(
            select(BankAccount).where(BankAccount.account_id == account_id, BankAccount.status == "active")
        ).scalars().all()
        snapshots = self.session.execute(
            select(BalanceSnapshot)
            .where(BalanceSnapshot.account_id == account_id, BalanceSnapshot.snapshot_at <= period.end_at)
            .order_by(BalanceSnapshot.bank_account_id.asc(), BalanceSnapshot.snapshot_at.desc())
        ).scalars().all()
        latest_by_account: dict[int, BalanceSnapshot] = {}
        for snapshot in snapshots:
            latest_by_account.setdefault(snapshot.bank_account_id, snapshot)

        transactions = self.session.execute(
            select(BankTransaction).where(
                BankTransaction.account_id == account_id,
                BankTransaction.posted_at >= period.start_at,
                BankTransaction.posted_at <= period.end_at,
            )
        ).scalars().all()
        expenses = self.session.execute(
            select(Expense).where(
                Expense.account_id == account_id,
                Expense.expense_date >= period.start_date,
                Expense.expense_date <= period.end_date,
            )
        ).scalars().all()

        safe_threshold = self._threshold_value(account_id, "bank.balance_below_safe_threshold", "safe_balance", Decimal("100000"))
        total_balance = Decimal("0")
        total_available = Decimal("0")
        risky_gap = Decimal("0")
        balances = []
        for bank_account in bank_accounts:
            latest = latest_by_account.get(bank_account.id)
            balance = self._decimal(latest.balance if latest is not None else None)
            available = self._decimal(latest.available_balance if latest is not None else None, fallback=balance)
            total_balance += balance
            total_available += available
            risky_gap += max(Decimal("0"), safe_threshold - available)
            balances.append(
                {
                    "bank_account_id": bank_account.id,
                    "name": bank_account.name,
                    "provider": bank_account.provider,
                    "currency": bank_account.currency,
                    "balance": self._num(balance),
                    "available_balance": self._num(available),
                    "snapshot_at": self._iso(latest.snapshot_at if latest is not None else None),
                }
            )

        inflow = sum((self._decimal(item.amount) for item in transactions if item.direction == "inflow"), Decimal("0"))
        outflow = sum((self._decimal(item.amount) for item in transactions if item.direction == "outflow"), Decimal("0"))
        expense_total = sum((self._decimal(item.amount) for item in expenses), Decimal("0"))
        balances.sort(key=lambda row: row["available_balance"], reverse=True)

        return {
            "summary": {
                "total_balance": self._num(total_balance),
                "total_available_balance": self._num(total_available),
                "inflow": self._num(inflow),
                "outflow": self._num(outflow),
                "expenses": self._num(expense_total),
                "free_cash": self._num(total_available),
                "risky_cash_gap": self._num(risky_gap),
                "safe_balance_threshold": self._num(safe_threshold),
            },
            "balances": balances[:5],
        }

    def _financial_result_widget(self, account_id: int, period: DashboardPeriod) -> dict[str, object]:
        deals = self.session.execute(
            select(Deal).where(
                Deal.account_id == account_id,
                Deal.status.in_(("won", "closed")),
            )
        ).scalars().all()
        period_deals = [
            deal
            for deal in deals
            if self._in_period(self._dt(deal.closed_at) or self._dt(deal.created_at), period)
        ]
        expenses = self.session.execute(
            select(Expense).where(
                Expense.account_id == account_id,
                Expense.expense_date >= period.start_date,
                Expense.expense_date <= period.end_date,
            )
        ).scalars().all()
        kpis = self.session.execute(
            select(DailyKPI).where(
                DailyKPI.account_id == account_id,
                DailyKPI.kpi_date >= period.start_date,
                DailyKPI.kpi_date <= period.end_date,
            )
        ).scalars().all()

        revenue = sum((self._decimal(deal.amount_total) for deal in period_deals), Decimal("0"))
        gross_profit = sum((self._decimal(deal.gross_profit) for deal in period_deals), Decimal("0"))
        expense_total = sum((self._decimal(item.amount) for item in expenses), Decimal("0"))
        net_profit_fact = self._sum_kpis(kpis, "net_profit")
        net_profit = net_profit_fact if net_profit_fact != Decimal("0") else gross_profit - expense_total

        kpi_summary = []
        for metric_code in ("revenue", "gross_profit", "net_profit"):
            fact = self._sum_kpis(kpis, metric_code)
            if fact == Decimal("0"):
                if metric_code == "revenue":
                    fact = revenue
                elif metric_code == "gross_profit":
                    fact = gross_profit
                elif metric_code == "net_profit":
                    fact = net_profit
            plan = self._sum_kpi_plans(kpis, metric_code)
            delta = fact - plan if plan is not None else None
            kpi_summary.append(
                {
                    "metric_code": metric_code,
                    "fact": self._num(fact),
                    "plan": self._num(plan) if plan is not None else None,
                    "delta": self._num(delta) if delta is not None else None,
                }
            )

        return {
            "summary": {
                "revenue": self._num(revenue),
                "gross_profit": self._num(gross_profit),
                "net_profit": self._num(net_profit),
                "daily_kpi_summary": kpi_summary,
            }
        }

    def _leads_sales_widget(self, account_id: int, period: DashboardPeriod) -> dict[str, object]:
        leads = self.session.execute(select(Lead).where(Lead.account_id == account_id)).scalars().all()
        deals = self.session.execute(select(Deal).where(Deal.account_id == account_id)).scalars().all()
        alerts = self.session.execute(
            select(Alert).where(
                Alert.account_id == account_id,
                Alert.status == "open",
                Alert.code == "lead.no_first_response",
            )
        ).scalars().all()

        incoming_leads = [lead for lead in leads if self._in_period(self._dt(lead.created_at), period)]
        lost_leads = [lead for lead in leads if lead.status == "lost" and self._in_period(self._dt(lead.updated_at), period)]
        won_deals = [
            deal for deal in deals
            if deal.status in {"won", "closed"} and self._in_period(self._dt(deal.closed_at) or self._dt(deal.created_at), period)
        ]
        open_deals = [deal for deal in deals if deal.status == "open"]

        status_breakdown: dict[str, int] = {}
        for lead in incoming_leads:
            status_breakdown[lead.status] = status_breakdown.get(lead.status, 0) + 1

        incoming_count = len(incoming_leads)
        won_count = len(won_deals)
        lost_count = len(lost_leads)

        return {
            "summary": {
                "incoming_leads": incoming_count,
                "lost_leads": lost_count,
                "first_response_sla_breaches": len(alerts),
                "open_deals": len(open_deals),
                "won_deals": won_count,
                "lead_to_deal_conversion_pct": self._pct(won_count, incoming_count),
                "lost_lead_rate_pct": self._pct(lost_count, incoming_count),
            },
            "status_breakdown": [
                {"status": status, "count": count}
                for status, count in sorted(status_breakdown.items(), key=lambda item: (-item[1], item[0]))
            ],
        }

    def _advertising_widget(self, account_id: int, period: DashboardPeriod) -> dict[str, object]:
        metrics = self.session.execute(
            select(AdMetric).where(
                AdMetric.account_id == account_id,
                AdMetric.metric_date >= period.start_date,
                AdMetric.metric_date <= period.end_date,
            )
        ).scalars().all()
        campaigns = {
            campaign.id: campaign
            for campaign in self.session.execute(select(Campaign).where(Campaign.account_id == account_id)).scalars().all()
        }
        deals = self.session.execute(
            select(Deal).where(
                Deal.account_id == account_id,
                Deal.status.in_(("won", "closed")),
            )
        ).scalars().all()
        acquired_customers = len(
            [
                deal for deal in deals
                if self._in_period(self._dt(deal.closed_at) or self._dt(deal.created_at), period)
            ]
        )
        max_cpl = self._threshold_value(account_id, "marketing.cpl_above_threshold", "max_cpl", Decimal("1200"))
        spend = sum((self._decimal(metric.spend) for metric in metrics), Decimal("0"))
        leads_count = sum(metric.leads_count for metric in metrics)
        conversions_count = sum(metric.conversions_count for metric in metrics)
        cpl = spend / Decimal(leads_count) if leads_count > 0 else None

        deviations = []
        for metric in metrics:
            metric_spend = self._decimal(metric.spend)
            metric_cpl = metric_spend / Decimal(metric.leads_count) if metric.leads_count > 0 else None
            if (metric_cpl is not None and metric_cpl > max_cpl) or (metric.leads_count == 0 and metric_spend > Decimal("0")):
                campaign = campaigns.get(metric.campaign_id)
                deviations.append(
                    {
                        "campaign_id": metric.campaign_id,
                        "campaign_name": campaign.name if campaign else f"Campaign {metric.campaign_id}",
                        "metric_date": metric.metric_date.isoformat(),
                        "spend": self._num(metric_spend),
                        "leads_count": metric.leads_count,
                        "cpl": self._num(metric_cpl) if metric_cpl is not None else None,
                    }
                )
        deviations.sort(key=lambda item: (item["cpl"] or 0, item["spend"]), reverse=True)

        return {
            "summary": {
                "spend": self._num(spend),
                "leads_count": leads_count,
                "cpl": self._num(cpl) if cpl is not None else None,
                "conversions_count": conversions_count,
                "cac_ready": {
                    "spend": self._num(spend),
                    "acquired_customers_count": acquired_customers,
                    "cac": self._num(spend / Decimal(acquired_customers)) if acquired_customers > 0 else None,
                    "attribution_status": "partial",
                },
            },
            "campaign_deviations": deviations[:5],
        }

    def _stock_widget(self, account_id: int, period: DashboardPeriod) -> dict[str, object]:
        del period
        products = {
            product.id: product
            for product in self.session.execute(select(Product).where(Product.account_id == account_id)).scalars().all()
        }
        stock_items = self.session.execute(select(StockItem).where(StockItem.account_id == account_id)).scalars().all()
        low_stock = []
        total_on_hand = Decimal("0")
        total_reserved = Decimal("0")
        for item in stock_items:
            on_hand = self._decimal(item.quantity_on_hand)
            reserved = self._decimal(item.quantity_reserved)
            available = on_hand - reserved
            threshold = self._decimal(item.min_quantity)
            if threshold <= Decimal("0"):
                threshold = self._decimal(products.get(item.product_id).min_stock_level if products.get(item.product_id) else None)
            total_on_hand += on_hand
            total_reserved += reserved
            if available < threshold:
                product = products.get(item.product_id)
                deficit = threshold - available
                low_stock.append(
                    {
                        "stock_item_id": item.id,
                        "product_id": item.product_id,
                        "product_name": product.name if product else f"Product {item.product_id}",
                        "available": self._num(available),
                        "on_hand": self._num(on_hand),
                        "reserved": self._num(reserved),
                        "threshold": self._num(threshold),
                        "deficit": self._num(deficit),
                    }
                )
        low_stock.sort(key=lambda row: row["deficit"], reverse=True)
        return {
            "summary": {
                "low_stock_items": len(low_stock),
                "total_on_hand": self._num(total_on_hand),
                "total_reserved": self._num(total_reserved),
            },
            "problem_positions": low_stock[:5],
        }

    def _management_widget(self, account_id: int, period: DashboardPeriod, problems: list[dict[str, object]]) -> dict[str, object]:
        tasks = self.session.execute(
            select(Task).where(Task.account_id == account_id, Task.status == "open")
        ).scalars().all()
        alerts = self.session.execute(
            select(Alert).where(Alert.account_id == account_id, Alert.status == "open")
        ).scalars().all()
        recommendations = self.session.execute(
            select(Recommendation).where(Recommendation.account_id == account_id, Recommendation.status == "open")
        ).scalars().all()
        overdue_tasks = [task for task in tasks if task.due_at is not None and self._dt(task.due_at) < period.end_at]

        return {
            "summary": {
                "open_tasks": len(tasks),
                "overdue_tasks": len(overdue_tasks),
                "active_alerts": len(alerts),
                "open_recommendations": len(recommendations),
            },
            "top_problems_today": problems[:5],
        }

    def _owner_panel_widget(
        self,
        account_id: int,
        period: DashboardPeriod,
        money: dict[str, object],
        financial_result: dict[str, object],
        leads_sales: dict[str, object],
        management: dict[str, object],
        problems: list[dict[str, object]],
    ) -> dict[str, object]:
        del period
        key_numbers = [
            {"metric_code": "available_cash", "value": money["summary"]["total_available_balance"]},
            {"metric_code": "revenue", "value": financial_result["summary"]["revenue"]},
            {"metric_code": "net_profit", "value": financial_result["summary"]["net_profit"]},
            {"metric_code": "incoming_leads", "value": leads_sales["summary"]["incoming_leads"]},
            {"metric_code": "active_alerts", "value": management["summary"]["active_alerts"]},
        ]
        actions = self._top_actions(account_id=account_id, limit=5)
        return {
            "top_numbers": key_numbers[:5],
            "top_problems": problems[:5],
            "attention_zones": actions[:5],
        }

    def _top_actions(self, account_id: int, limit: int) -> list[dict[str, object]]:
        priority_weights = {"critical": 100, "high": 80, "normal": 50, "low": 20}
        tasks = self.session.execute(
            select(Task)
            .where(Task.account_id == account_id, Task.status == "open")
        ).scalars().all()
        tasks.sort(
            key=lambda task: (
                -priority_weights.get(task.priority, 0),
                0 if task.due_at is not None and self._dt(task.due_at) < datetime.now(timezone.utc) else 1,
                self._dt(task.due_at).timestamp() if task.due_at is not None else float("inf"),
            ),
        )
        recommendations = self.session.execute(
            select(Recommendation)
            .where(Recommendation.account_id == account_id, Recommendation.status == "open")
            .order_by(Recommendation.created_at.asc())
        ).scalars().all()

        seen: set[str] = set()
        actions: list[dict[str, object]] = []
        for task in tasks:
            key = task.dedupe_key or f"task:{task.id}"
            if key in seen:
                continue
            seen.add(key)
            actions.append(
                {
                    "action_type": "task",
                    "task_id": task.id,
                    "title": task.title,
                    "priority": task.priority,
                    "due_at": self._iso(task.due_at),
                    "related_entity_type": task.related_entity_type,
                    "related_entity_id": task.related_entity_id,
                }
            )
            if len(actions) >= limit:
                return actions

        for recommendation in recommendations:
            key = recommendation.dedupe_key or f"recommendation:{recommendation.id}"
            if key in seen:
                continue
            seen.add(key)
            actions.append(
                {
                    "action_type": "recommendation",
                    "recommendation_id": recommendation.id,
                    "title": recommendation.title,
                    "related_entity_type": recommendation.related_entity_type,
                    "related_entity_id": recommendation.related_entity_id,
                }
            )
            if len(actions) >= limit:
                return actions
        return actions

    def _top_problems(self, account_id: int, period: DashboardPeriod, limit: int) -> list[dict[str, object]]:
        alerts = self.session.execute(
            select(Alert).where(Alert.account_id == account_id, Alert.status == "open")
        ).scalars().all()
        tasks = self.session.execute(
            select(Task).where(Task.account_id == account_id, Task.status == "open")
        ).scalars().all()
        recommendations = self.session.execute(
            select(Recommendation).where(Recommendation.account_id == account_id, Recommendation.status == "open")
        ).scalars().all()

        tasks_by_key = {task.dedupe_key: task for task in tasks if task.dedupe_key}
        recommendations_by_key = {
            recommendation.dedupe_key: recommendation for recommendation in recommendations if recommendation.dedupe_key
        }
        severity_weights = {"critical": 100, "warning": 60, "info": 30}
        code_weights = {
            "bank.balance_below_safe_threshold": 25,
            "inventory.stock_below_threshold": 15,
            "marketing.cpl_above_threshold": 15,
            "lead.no_first_response": 10,
            "leads.lost_above_threshold": 10,
            "task.overdue_escalation": 10,
        }
        problems = []
        for alert in alerts:
            task = tasks_by_key.get(alert.dedupe_key)
            recommendation = recommendations_by_key.get(alert.dedupe_key)
            score = severity_weights.get(alert.severity, 20) + code_weights.get(alert.code, 0)
            if self._dt(alert.last_detected_at) >= period.start_at:
                score += 10
            if task is not None and task.due_at is not None and self._dt(task.due_at) < period.end_at:
                score += 20
            if recommendation is not None:
                score += 5
            problems.append(
                {
                    "alert_id": alert.id,
                    "code": alert.code,
                    "severity": alert.severity,
                    "title": alert.title,
                    "status": alert.status,
                    "score": score,
                    "last_detected_at": self._iso(alert.last_detected_at),
                    "task_id": task.id if task is not None else None,
                    "recommendation_id": recommendation.id if recommendation is not None else None,
                    "related_entity_type": alert.related_entity_type,
                    "related_entity_id": alert.related_entity_id,
                }
            )
        problems.sort(key=lambda item: (item["score"], item["last_detected_at"] or ""), reverse=True)
        return problems[:limit]

    def _threshold_value(self, account_id: int, rule_code: str, threshold_key: str, fallback: Decimal) -> Decimal:
        value = self.session.execute(
            select(ThresholdConfig.value_numeric)
            .join(Rule, Rule.id == ThresholdConfig.rule_id)
            .where(
                ThresholdConfig.account_id == account_id,
                Rule.code == rule_code,
                ThresholdConfig.threshold_key == threshold_key,
                ThresholdConfig.status == "active",
            )
            .limit(1)
        ).scalar_one_or_none()
        return Decimal(str(value)) if value is not None else fallback

    def _sum_kpis(self, rows: list[DailyKPI], metric_code: str) -> Decimal:
        total = Decimal("0")
        for row in rows:
            if row.metric_code == metric_code:
                total += self._decimal(row.value_numeric)
        return total

    def _sum_kpi_plans(self, rows: list[DailyKPI], metric_code: str) -> Decimal | None:
        total: Decimal | None = None
        for row in rows:
            if row.metric_code != metric_code:
                continue
            payload = row.payload_json or {}
            plan_value = payload.get("plan_value")
            if plan_value is None:
                continue
            total = (total or Decimal("0")) + Decimal(str(plan_value))
        return total

    def _in_period(self, value: datetime | None, period: DashboardPeriod) -> bool:
        if value is None:
            return False
        normalized = self._dt(value)
        return period.start_at <= normalized <= period.end_at

    def _dt(self, value: datetime | None) -> datetime:
        if value is None:
            return datetime.min.replace(tzinfo=timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _decimal(self, value: object | None, fallback: Decimal = Decimal("0")) -> Decimal:
        if value is None:
            return fallback
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def _num(self, value: Decimal | None) -> float | None:
        if value is None:
            return None
        return float(value.quantize(Decimal("0.01")))

    def _pct(self, numerator: int, denominator: int) -> float | None:
        if denominator <= 0:
            return None
        value = (Decimal(numerator) / Decimal(denominator)) * Decimal("100")
        return float(value.quantize(Decimal("0.01")))

    def _iso(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return self._dt(value).isoformat()
