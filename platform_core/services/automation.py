from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from platform_core.automation_defaults import DEFAULT_RULES
from platform_core.models import (
    AccountUser,
    AdMetric,
    Alert,
    BalanceSnapshot,
    BankAccount,
    Campaign,
    Lead,
    Product,
    Recommendation,
    Role,
    Rule,
    RuleExecution,
    RuleVersion,
    StockItem,
    Task,
    TaskEvent,
    ThresholdConfig,
)
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class RuleRunResult:
    rule_code: str
    execution_key: str
    alert_id: int | None
    task_id: int | None
    recommendation_id: int | None
    status: str


class RuleCatalogService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def seed_default_rules(self, context: TenantContext) -> list[Rule]:
        account_id = require_account_id(context)
        seeded: list[Rule] = []
        for definition in DEFAULT_RULES:
            rule = self.session.execute(
                select(Rule).where(Rule.account_id == account_id, Rule.code == definition["code"])
            ).scalar_one_or_none()
            if rule is None:
                rule = Rule(
                    account_id=account_id,
                    code=definition["code"],
                    name=definition["name"],
                    rule_type=definition["rule_type"],
                    status="active",
                    description=definition["description"],
                    active_version_number=1,
                )
                self.session.add(rule)
                self.session.flush()

            version = self.session.execute(
                select(RuleVersion).where(RuleVersion.rule_id == rule.id, RuleVersion.version_number == rule.active_version_number)
            ).scalar_one_or_none()
            if version is None:
                version = RuleVersion(
                    rule_id=rule.id,
                    version_number=rule.active_version_number,
                    status="active",
                    created_by_user_id=context.actor_user_id,
                    config_json=definition["config"],
                )
                self.session.add(version)
                self.session.flush()

            for key, value in definition["thresholds"].items():
                threshold = self.session.execute(
                    select(ThresholdConfig).where(
                        ThresholdConfig.account_id == account_id,
                        ThresholdConfig.rule_id == rule.id,
                        ThresholdConfig.threshold_key == key,
                    )
                ).scalar_one_or_none()
                if threshold is None:
                    threshold = ThresholdConfig(
                        account_id=account_id,
                        rule_id=rule.id,
                        threshold_key=key,
                        status="active",
                        value_numeric=value,
                    )
                    self.session.add(threshold)
            seeded.append(rule)
        self.session.flush()
        return seeded


class RuleOutputService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def _resolve_assignee_user_id(self, account_id: int, preferred_role_code: str | None) -> int | None:
        if preferred_role_code:
            row = self.session.execute(
                select(AccountUser.user_id)
                .join(Role, Role.id == AccountUser.role_id)
                .where(
                    AccountUser.account_id == account_id,
                    AccountUser.status == "active",
                    Role.code == preferred_role_code,
                )
                .limit(1)
            ).first()
            if row:
                return int(row[0])
        row = self.session.execute(
            select(AccountUser.user_id).where(AccountUser.account_id == account_id, AccountUser.status == "active").limit(1)
        ).first()
        return int(row[0]) if row else None

    def ensure_alert(
        self,
        *,
        account_id: int,
        rule_id: int,
        dedupe_key: str,
        code: str,
        title: str,
        description: str,
        severity: str,
        related_entity_type: str,
        related_entity_id: str,
        assigned_user_id: int | None,
        now: datetime,
    ) -> Alert:
        alert = self.session.execute(
            select(Alert).where(Alert.account_id == account_id, Alert.dedupe_key == dedupe_key)
        ).scalar_one_or_none()
        if alert is None:
            alert = Alert(
                account_id=account_id,
                assigned_user_id=assigned_user_id,
                source_rule_id=rule_id,
                dedupe_key=dedupe_key,
                code=code,
                severity=severity,
                status="open",
                title=title,
                description=description,
                related_entity_type=related_entity_type,
                related_entity_id=related_entity_id,
                first_detected_at=now,
                last_detected_at=now,
            )
            self.session.add(alert)
            self.session.flush()
            return alert
        alert.assigned_user_id = assigned_user_id
        alert.source_rule_id = rule_id
        alert.status = "open"
        alert.title = title
        alert.description = description
        alert.severity = severity
        alert.related_entity_type = related_entity_type
        alert.related_entity_id = related_entity_id
        if alert.first_detected_at is None:
            alert.first_detected_at = now
        alert.last_detected_at = now
        self.session.flush()
        return alert

    def ensure_task(
        self,
        *,
        account_id: int,
        rule_id: int,
        dedupe_key: str,
        title: str,
        description: str,
        source: str,
        priority: str,
        due_at: datetime | None,
        related_entity_type: str,
        related_entity_id: str,
        assignee_role_code: str | None,
        created_by_user_id: int | None,
        now: datetime,
    ) -> Task:
        task = self.session.execute(
            select(Task).where(Task.account_id == account_id, Task.dedupe_key == dedupe_key)
        ).scalar_one_or_none()
        assignee_user_id = self._resolve_assignee_user_id(account_id, assignee_role_code)
        if task is None:
            task = Task(
                account_id=account_id,
                assignee_user_id=assignee_user_id,
                created_by_user_id=created_by_user_id,
                source_rule_id=rule_id,
                dedupe_key=dedupe_key,
                escalation_level=0,
                source=source,
                title=title,
                description=description,
                status="open",
                priority=priority,
                due_at=due_at,
                related_entity_type=related_entity_type,
                related_entity_id=related_entity_id,
            )
            self.session.add(task)
            self.session.flush()
            self.session.add(
                TaskEvent(
                    account_id=account_id,
                    task_id=task.id,
                    actor_user_id=created_by_user_id,
                    event_type="task.created_by_rule",
                    event_at=now,
                    payload_json={"rule_id": rule_id, "dedupe_key": dedupe_key},
                )
            )
            self.session.flush()
            return task
        task.assignee_user_id = assignee_user_id
        task.source_rule_id = rule_id
        task.status = "open"
        task.title = title
        task.description = description
        task.priority = priority
        task.due_at = due_at
        task.related_entity_type = related_entity_type
        task.related_entity_id = related_entity_id
        self.session.flush()
        return task

    def ensure_recommendation(
        self,
        *,
        account_id: int,
        rule_id: int,
        alert_id: int | None,
        dedupe_key: str,
        code: str,
        title: str,
        description: str,
        related_entity_type: str,
        related_entity_id: str,
    ) -> Recommendation:
        recommendation = self.session.execute(
            select(Recommendation).where(Recommendation.account_id == account_id, Recommendation.dedupe_key == dedupe_key)
        ).scalar_one_or_none()
        if recommendation is None:
            recommendation = Recommendation(
                account_id=account_id,
                alert_id=alert_id,
                source_rule_id=rule_id,
                dedupe_key=dedupe_key,
                status="open",
                code=code,
                title=title,
                description=description,
                related_entity_type=related_entity_type,
                related_entity_id=related_entity_id,
            )
            self.session.add(recommendation)
            self.session.flush()
            return recommendation
        recommendation.alert_id = alert_id
        recommendation.source_rule_id = rule_id
        recommendation.status = "open"
        recommendation.title = title
        recommendation.description = description
        recommendation.related_entity_type = related_entity_type
        recommendation.related_entity_id = related_entity_id
        self.session.flush()
        return recommendation

    def escalate_task(self, *, account_id: int, task: Task, rule_id: int, now: datetime, actor_user_id: int | None) -> Task:
        task.source_rule_id = rule_id
        task.escalation_level += 1
        task.escalated_at = now
        task.priority = "critical"
        self.session.flush()
        self.session.add(
            TaskEvent(
                account_id=account_id,
                task_id=task.id,
                actor_user_id=actor_user_id,
                event_type="task.escalated_by_rule",
                event_at=now,
                payload_json={"rule_id": rule_id, "escalation_level": task.escalation_level},
            )
        )
        self.session.flush()
        return task


class RuleEngineService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.output_service = RuleOutputService(session)

    def evaluate_account(
        self,
        context: TenantContext,
        now: datetime | None = None,
        rule_codes: set[str] | None = None,
    ) -> list[RuleRunResult]:
        account_id = require_account_id(context)
        now = now or datetime.now(timezone.utc)
        query = select(Rule).where(Rule.account_id == account_id, Rule.status == "active")
        if rule_codes:
            query = query.where(Rule.code.in_(sorted(rule_codes)))
        rules = self.session.execute(query.order_by(Rule.id)).scalars().all()
        results: list[RuleRunResult] = []
        for rule in rules:
            version = self.session.execute(
                select(RuleVersion).where(RuleVersion.rule_id == rule.id, RuleVersion.version_number == rule.active_version_number)
            ).scalar_one()
            thresholds = {
                row.threshold_key: row.value_numeric or row.value_text or ""
                for row in self.session.execute(
                    select(ThresholdConfig).where(
                        ThresholdConfig.account_id == account_id,
                        ThresholdConfig.rule_id == rule.id,
                        ThresholdConfig.status == "active",
                    )
                ).scalars()
            }
            if rule.code == "lead.no_first_response":
                results.extend(self._run_lead_no_first_response(context, rule, version, thresholds, now))
            elif rule.code == "marketing.cpl_above_threshold":
                results.extend(self._run_cpl_above_threshold(context, rule, version, thresholds, now))
            elif rule.code == "inventory.stock_below_threshold":
                results.extend(self._run_stock_below_threshold(context, rule, version, thresholds, now))
            elif rule.code == "task.overdue_escalation":
                results.extend(self._run_overdue_task_escalation(context, rule, version, thresholds, now))
            elif rule.code == "bank.balance_below_safe_threshold":
                results.extend(self._run_bank_balance_below_threshold(context, rule, version, thresholds, now))
            elif rule.code == "leads.lost_above_threshold":
                results.extend(self._run_lost_leads_above_threshold(context, rule, version, thresholds, now))
        self.session.flush()
        return results

    def _get_or_create_execution(
        self,
        *,
        account_id: int,
        rule: Rule,
        version: RuleVersion,
        execution_key: str,
        evaluated_entity_type: str,
        evaluated_entity_id: str,
        window_key: str | None,
        now: datetime,
        details: dict[str, object],
    ) -> RuleExecution:
        execution = self.session.execute(
            select(RuleExecution).where(RuleExecution.account_id == account_id, RuleExecution.execution_key == execution_key)
        ).scalar_one_or_none()
        if execution is None:
            execution = RuleExecution(
                account_id=account_id,
                rule_id=rule.id,
                rule_version_id=version.id,
                execution_key=execution_key,
                status="triggered",
                evaluated_entity_type=evaluated_entity_type,
                evaluated_entity_id=evaluated_entity_id,
                window_key=window_key,
                run_count=1,
                last_evaluated_at=now,
                first_triggered_at=now,
                last_triggered_at=now,
                details_json=details,
            )
            self.session.add(execution)
            self.session.flush()
            return execution
        execution.run_count += 1
        execution.status = "triggered"
        execution.last_evaluated_at = now
        execution.last_triggered_at = now
        execution.details_json = details
        self.session.flush()
        return execution

    def _numeric_threshold(self, thresholds: dict[str, str], key: str, default: str = "0") -> Decimal:
        return Decimal(thresholds.get(key) or default)

    def _dt(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _assignee_role_code(self, version: RuleVersion) -> str | None:
        value = version.config_json.get("assignee_role_code")
        return str(value) if value else None

    def _severity(self, version: RuleVersion, default: str = "warning") -> str:
        value = version.config_json.get("severity")
        return str(value) if value else default

    def _run_lead_no_first_response(
        self, context: TenantContext, rule: Rule, version: RuleVersion, thresholds: dict[str, str], now: datetime
    ) -> list[RuleRunResult]:
        account_id = require_account_id(context)
        threshold_minutes = int(self._numeric_threshold(thresholds, "first_response_minutes", "30"))
        rows = self.session.execute(
            select(Lead).where(
                Lead.account_id == account_id,
                Lead.status.notin_(["lost", "won", "closed"]),
                Lead.first_responded_at.is_(None),
            )
        ).scalars()
        results: list[RuleRunResult] = []
        for lead in rows:
            due_at = self._dt(lead.first_response_due_at) or (self._dt(lead.created_at) + timedelta(minutes=threshold_minutes))
            if due_at > now:
                continue
            execution_key = f"{rule.code}:lead:{lead.id}"
            execution = self._get_or_create_execution(
                account_id=account_id,
                rule=rule,
                version=version,
                execution_key=execution_key,
                evaluated_entity_type="lead",
                evaluated_entity_id=str(lead.id),
                window_key=None,
                now=now,
                details={"threshold_minutes": threshold_minutes, "due_at": due_at.isoformat()},
            )
            alert = self.output_service.ensure_alert(
                account_id=account_id,
                rule_id=rule.id,
                dedupe_key=execution_key,
                code=rule.code,
                title="Lead waiting for first response",
                description=f"Lead #{lead.id} has not received the first response within {threshold_minutes} minutes.",
                severity=self._severity(version),
                related_entity_type="lead",
                related_entity_id=str(lead.id),
                assigned_user_id=self.output_service._resolve_assignee_user_id(account_id, self._assignee_role_code(version)),
                now=now,
            )
            task = self.output_service.ensure_task(
                account_id=account_id,
                rule_id=rule.id,
                dedupe_key=execution_key,
                title=f"Respond to lead #{lead.id}",
                description="Provide the first response and update the lead status.",
                source="rule_engine",
                priority="high",
                due_at=now,
                related_entity_type="lead",
                related_entity_id=str(lead.id),
                assignee_role_code=self._assignee_role_code(version),
                created_by_user_id=context.actor_user_id,
                now=now,
            )
            execution.alert_id = alert.id
            execution.task_id = task.id
            results.append(RuleRunResult(rule.code, execution_key, alert.id, task.id, None, execution.status))
        return results

    def _run_cpl_above_threshold(
        self, context: TenantContext, rule: Rule, version: RuleVersion, thresholds: dict[str, str], now: datetime
    ) -> list[RuleRunResult]:
        account_id = require_account_id(context)
        max_cpl = self._numeric_threshold(thresholds, "max_cpl", "1200")
        rows = self.session.execute(
            select(AdMetric, Campaign).join(Campaign, Campaign.id == AdMetric.campaign_id).where(AdMetric.account_id == account_id)
        ).all()
        results: list[RuleRunResult] = []
        for metric, campaign in rows:
            if metric.leads_count <= 0:
                continue
            cpl = Decimal(metric.spend) / Decimal(metric.leads_count)
            if cpl <= max_cpl:
                continue
            execution_key = f"{rule.code}:campaign:{campaign.id}:date:{metric.metric_date.isoformat()}"
            execution = self._get_or_create_execution(
                account_id=account_id,
                rule=rule,
                version=version,
                execution_key=execution_key,
                evaluated_entity_type="campaign",
                evaluated_entity_id=str(campaign.id),
                window_key=metric.metric_date.isoformat(),
                now=now,
                details={"cpl": str(cpl), "max_cpl": str(max_cpl)},
            )
            alert = self.output_service.ensure_alert(
                account_id=account_id,
                rule_id=rule.id,
                dedupe_key=execution_key,
                code=rule.code,
                title="CPL above threshold",
                description=f"Campaign {campaign.name} has CPL {cpl:.2f}, above threshold {max_cpl:.2f}.",
                severity=self._severity(version),
                related_entity_type="campaign",
                related_entity_id=str(campaign.id),
                assigned_user_id=self.output_service._resolve_assignee_user_id(account_id, self._assignee_role_code(version)),
                now=now,
            )
            task = self.output_service.ensure_task(
                account_id=account_id,
                rule_id=rule.id,
                dedupe_key=execution_key,
                title=f"Review CPL for {campaign.name}",
                description="Check spend, targeting and creative efficiency.",
                source="rule_engine",
                priority="high",
                due_at=now + timedelta(hours=4),
                related_entity_type="campaign",
                related_entity_id=str(campaign.id),
                assignee_role_code=self._assignee_role_code(version),
                created_by_user_id=context.actor_user_id,
                now=now,
            )
            recommendation = self.output_service.ensure_recommendation(
                account_id=account_id,
                rule_id=rule.id,
                alert_id=alert.id,
                dedupe_key=execution_key,
                code=rule.code,
                title="Reduce CPL",
                description="Pause weak segments or adjust bids before spend compounds further.",
                related_entity_type="campaign",
                related_entity_id=str(campaign.id),
            )
            execution.alert_id = alert.id
            execution.task_id = task.id
            execution.recommendation_id = recommendation.id
            results.append(RuleRunResult(rule.code, execution_key, alert.id, task.id, recommendation.id, execution.status))
        return results

    def _run_stock_below_threshold(
        self, context: TenantContext, rule: Rule, version: RuleVersion, thresholds: dict[str, str], now: datetime
    ) -> list[RuleRunResult]:
        account_id = require_account_id(context)
        rows = self.session.execute(
            select(StockItem, Product).join(Product, Product.id == StockItem.product_id).where(StockItem.account_id == account_id)
        ).all()
        results: list[RuleRunResult] = []
        for stock_item, product in rows:
            min_threshold = Decimal(stock_item.min_quantity or 0)
            if min_threshold <= 0:
                min_threshold = Decimal(product.min_stock_level or 0)
            available = Decimal(stock_item.quantity_on_hand) - Decimal(stock_item.quantity_reserved)
            if available >= min_threshold:
                continue
            execution_key = f"{rule.code}:stock_item:{stock_item.id}"
            execution = self._get_or_create_execution(
                account_id=account_id,
                rule=rule,
                version=version,
                execution_key=execution_key,
                evaluated_entity_type="stock_item",
                evaluated_entity_id=str(stock_item.id),
                window_key=None,
                now=now,
                details={"available": str(available), "min_threshold": str(min_threshold)},
            )
            alert = self.output_service.ensure_alert(
                account_id=account_id,
                rule_id=rule.id,
                dedupe_key=execution_key,
                code=rule.code,
                title="Stock below threshold",
                description=f"{product.name} is below threshold: available {available}, threshold {min_threshold}.",
                severity=self._severity(version, "critical"),
                related_entity_type="stock_item",
                related_entity_id=str(stock_item.id),
                assigned_user_id=self.output_service._resolve_assignee_user_id(account_id, self._assignee_role_code(version)),
                now=now,
            )
            task = self.output_service.ensure_task(
                account_id=account_id,
                rule_id=rule.id,
                dedupe_key=execution_key,
                title=f"Replenish {product.name}",
                description="Create or update procurement action for low stock item.",
                source="rule_engine",
                priority="high",
                due_at=now + timedelta(days=1),
                related_entity_type="stock_item",
                related_entity_id=str(stock_item.id),
                assignee_role_code=self._assignee_role_code(version),
                created_by_user_id=context.actor_user_id,
                now=now,
            )
            recommendation = self.output_service.ensure_recommendation(
                account_id=account_id,
                rule_id=rule.id,
                alert_id=alert.id,
                dedupe_key=execution_key,
                code=rule.code,
                title="Reorder inventory",
                description="Purchase additional stock before availability drops further.",
                related_entity_type="stock_item",
                related_entity_id=str(stock_item.id),
            )
            execution.alert_id = alert.id
            execution.task_id = task.id
            execution.recommendation_id = recommendation.id
            results.append(RuleRunResult(rule.code, execution_key, alert.id, task.id, recommendation.id, execution.status))
        return results

    def _run_overdue_task_escalation(
        self, context: TenantContext, rule: Rule, version: RuleVersion, thresholds: dict[str, str], now: datetime
    ) -> list[RuleRunResult]:
        account_id = require_account_id(context)
        grace_minutes = int(self._numeric_threshold(thresholds, "grace_minutes", "0"))
        overdue_before = now - timedelta(minutes=grace_minutes)
        rows = self.session.execute(
            select(Task).where(
                Task.account_id == account_id,
                Task.status.notin_(["done", "completed", "cancelled"]),
                Task.completed_at.is_(None),
                Task.due_at.is_not(None),
                Task.due_at < overdue_before,
            )
        ).scalars()
        results: list[RuleRunResult] = []
        for task in rows:
            due_at = self._dt(task.due_at)
            if due_at is None or due_at >= overdue_before:
                continue
            execution_key = f"{rule.code}:task:{task.id}"
            execution = self._get_or_create_execution(
                account_id=account_id,
                rule=rule,
                version=version,
                execution_key=execution_key,
                evaluated_entity_type="task",
                evaluated_entity_id=str(task.id),
                window_key=None,
                now=now,
                details={"due_at": due_at.isoformat()},
            )
            escalated_task = self.output_service.escalate_task(
                account_id=account_id,
                task=task,
                rule_id=rule.id,
                now=now,
                actor_user_id=context.actor_user_id,
            )
            alert = self.output_service.ensure_alert(
                account_id=account_id,
                rule_id=rule.id,
                dedupe_key=execution_key,
                code=rule.code,
                title="Task overdue",
                description=f"Task #{task.id} is overdue and has been escalated.",
                severity=self._severity(version),
                related_entity_type="task",
                related_entity_id=str(task.id),
                assigned_user_id=task.assignee_user_id,
                now=now,
            )
            execution.alert_id = alert.id
            execution.task_id = escalated_task.id
            results.append(RuleRunResult(rule.code, execution_key, alert.id, escalated_task.id, None, execution.status))
        return results

    def _run_bank_balance_below_threshold(
        self, context: TenantContext, rule: Rule, version: RuleVersion, thresholds: dict[str, str], now: datetime
    ) -> list[RuleRunResult]:
        account_id = require_account_id(context)
        safe_balance = self._numeric_threshold(thresholds, "safe_balance", "100000")
        subquery = (
            select(
                BalanceSnapshot.bank_account_id,
                func.max(BalanceSnapshot.snapshot_at).label("latest_snapshot_at"),
            )
            .where(BalanceSnapshot.account_id == account_id)
            .group_by(BalanceSnapshot.bank_account_id)
            .subquery()
        )
        rows = self.session.execute(
            select(BalanceSnapshot, BankAccount)
            .join(BankAccount, BankAccount.id == BalanceSnapshot.bank_account_id)
            .join(
                subquery,
                and_(
                    subquery.c.bank_account_id == BalanceSnapshot.bank_account_id,
                    subquery.c.latest_snapshot_at == BalanceSnapshot.snapshot_at,
                ),
            )
            .where(BalanceSnapshot.account_id == account_id)
        ).all()
        results: list[RuleRunResult] = []
        for snapshot, bank_account in rows:
            current_balance = Decimal(snapshot.available_balance if snapshot.available_balance is not None else snapshot.balance)
            if current_balance >= safe_balance:
                continue
            execution_key = f"{rule.code}:bank_account:{bank_account.id}"
            execution = self._get_or_create_execution(
                account_id=account_id,
                rule=rule,
                version=version,
                execution_key=execution_key,
                evaluated_entity_type="bank_account",
                evaluated_entity_id=str(bank_account.id),
                window_key=None,
                now=now,
                details={"balance": str(current_balance), "safe_balance": str(safe_balance)},
            )
            alert = self.output_service.ensure_alert(
                account_id=account_id,
                rule_id=rule.id,
                dedupe_key=execution_key,
                code=rule.code,
                title="Bank balance below threshold",
                description=f"{bank_account.name} balance {current_balance} is below safe threshold {safe_balance}.",
                severity=self._severity(version, "critical"),
                related_entity_type="bank_account",
                related_entity_id=str(bank_account.id),
                assigned_user_id=self.output_service._resolve_assignee_user_id(account_id, self._assignee_role_code(version)),
                now=now,
            )
            execution.alert_id = alert.id
            results.append(RuleRunResult(rule.code, execution_key, alert.id, None, None, execution.status))
        return results

    def _run_lost_leads_above_threshold(
        self, context: TenantContext, rule: Rule, version: RuleVersion, thresholds: dict[str, str], now: datetime
    ) -> list[RuleRunResult]:
        account_id = require_account_id(context)
        max_daily_lost = int(self._numeric_threshold(thresholds, "max_daily_lost_leads", "5"))
        window_date = now.date()
        lost_count = self.session.execute(
            select(func.count(Lead.id)).where(
                Lead.account_id == account_id,
                Lead.status == "lost",
                func.date(Lead.updated_at) == window_date,
            )
        ).scalar_one()
        results: list[RuleRunResult] = []
        if int(lost_count) <= max_daily_lost:
            return results
        execution_key = f"{rule.code}:date:{window_date.isoformat()}"
        execution = self._get_or_create_execution(
            account_id=account_id,
            rule=rule,
            version=version,
            execution_key=execution_key,
            evaluated_entity_type="lead_portfolio",
            evaluated_entity_id=window_date.isoformat(),
            window_key=window_date.isoformat(),
            now=now,
            details={"lost_count": int(lost_count), "max_daily_lost_leads": max_daily_lost},
        )
        alert = self.output_service.ensure_alert(
            account_id=account_id,
            rule_id=rule.id,
            dedupe_key=execution_key,
            code=rule.code,
            title="Lost leads above threshold",
            description=f"Lost leads today: {int(lost_count)}, threshold: {max_daily_lost}.",
            severity=self._severity(version),
            related_entity_type="lead_portfolio",
            related_entity_id=window_date.isoformat(),
            assigned_user_id=self.output_service._resolve_assignee_user_id(account_id, self._assignee_role_code(version)),
            now=now,
        )
        execution.alert_id = alert.id
        results.append(RuleRunResult(rule.code, execution_key, alert.id, None, None, execution.status))
        return results
