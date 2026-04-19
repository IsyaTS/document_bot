from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import JSON, Date, DateTime, ForeignKey, Index, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from platform_core.db import Base
from platform_core.models.base import AccountScopedMixin, TimestampMixin


class Customer(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "customers"
    __table_args__ = (
        UniqueConstraint("account_id", "external_id"),
        Index("ix_customers_account_name", "account_id", "name"),
        Index("ix_customers_account_status", "account_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    customer_type: Mapped[str] = mapped_column(String(32), nullable=False, default="business")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    inn: Mapped[str | None] = mapped_column(String(32), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class Lead(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "leads"
    __table_args__ = (
        UniqueConstraint("account_id", "source", "external_id"),
        Index("ix_leads_account_status_created_at", "account_id", "status", "created_at"),
        Index("ix_leads_account_customer_created_at", "account_id", "customer_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="manual")
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    contact_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="new")
    pipeline_stage: Mapped[str] = mapped_column(String(64), nullable=False, default="new")
    first_response_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_responded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lost_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class LeadEvent(Base, AccountScopedMixin):
    __tablename__ = "lead_events"
    __table_args__ = (
        Index("ix_lead_events_account_lead_event_at", "account_id", "lead_id", "event_at"),
        Index("ix_lead_events_account_type_event_at", "account_id", "event_type", "event_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id", ondelete="CASCADE"), nullable=False)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    payload_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class Deal(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "deals"
    __table_args__ = (
        UniqueConstraint("account_id", "deal_number"),
        Index("ix_deals_account_status_created_at", "account_id", "status", "created_at"),
        Index("ix_deals_account_customer_id", "account_id", "customer_id"),
        Index("ix_deals_account_lead_id", "account_id", "lead_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"), nullable=True)
    lead_id: Mapped[int | None] = mapped_column(ForeignKey("leads.id", ondelete="SET NULL"), nullable=True)
    owner_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    deal_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    stage: Mapped[str] = mapped_column(String(64), nullable=False, default="new")
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="RUB")
    amount_total: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    cost_total: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    gross_profit: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ProductCategory(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "product_categories"
    __table_args__ = (
        UniqueConstraint("account_id", "code"),
        Index("ix_product_categories_account_parent_id", "account_id", "parent_id"),
        Index("ix_product_categories_account_name", "account_id", "name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey("product_categories.id", ondelete="SET NULL"), nullable=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")


class Product(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("account_id", "sku"),
        Index("ix_products_account_category_status", "account_id", "category_id", "status"),
        Index("ix_products_account_name", "account_id", "name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    category_id: Mapped[int | None] = mapped_column(ForeignKey("product_categories.id", ondelete="SET NULL"), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(64), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    product_type: Mapped[str] = mapped_column(String(32), nullable=False, default="stock")
    unit: Mapped[str] = mapped_column(String(32), nullable=False, default="pcs")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    list_price: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    cost_price: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    min_stock_level: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    attributes_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class Warehouse(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "warehouses"
    __table_args__ = (
        UniqueConstraint("account_id", "code"),
        Index("ix_warehouses_account_status", "account_id", "status"),
        Index("ix_warehouses_account_name", "account_id", "name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    location: Mapped[str | None] = mapped_column(String(255), nullable=True)


class BankAccount(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "bank_accounts"
    __table_args__ = (
        UniqueConstraint("account_id", "provider", "external_id"),
        Index("ix_bank_accounts_account_provider_status", "account_id", "provider", "status"),
        Index("ix_bank_accounts_account_status", "account_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    account_mask: Mapped[str | None] = mapped_column(String(64), nullable=True)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="RUB")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Purchase(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "purchases"
    __table_args__ = (
        UniqueConstraint("account_id", "purchase_number"),
        Index("ix_purchases_account_status_ordered_at", "account_id", "status", "ordered_at"),
        Index("ix_purchases_account_supplier_id", "account_id", "supplier_customer_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    supplier_customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"), nullable=True)
    warehouse_id: Mapped[int | None] = mapped_column(ForeignKey("warehouses.id", ondelete="SET NULL"), nullable=True)
    purchase_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="draft")
    ordered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="RUB")
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    notes_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class Expense(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "expenses"
    __table_args__ = (
        Index("ix_expenses_account_expense_date", "account_id", "expense_date"),
        Index("ix_expenses_account_category_expense_date", "account_id", "category", "expense_date"),
        Index("ix_expenses_account_bank_account_id", "account_id", "bank_account_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    supplier_customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"), nullable=True)
    bank_account_id: Mapped[int | None] = mapped_column(ForeignKey("bank_accounts.id", ondelete="SET NULL"), nullable=True)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="posted")
    expense_date: Mapped[date] = mapped_column(Date, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="RUB")
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    reference_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reference_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


class Campaign(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "campaigns"
    __table_args__ = (
        UniqueConstraint("account_id", "source", "external_id"),
        Index("ix_campaigns_account_status", "account_id", "status"),
        Index("ix_campaigns_account_source_status", "account_id", "source", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    started_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    ended_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    budget_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="RUB")


class AdMetric(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "ad_metrics"
    __table_args__ = (
        UniqueConstraint("account_id", "campaign_id", "metric_date"),
        Index("ix_ad_metrics_account_metric_date", "account_id", "metric_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False)
    metric_date: Mapped[date] = mapped_column(Date, nullable=False)
    impressions: Mapped[int] = mapped_column(nullable=False, default=0)
    clicks: Mapped[int] = mapped_column(nullable=False, default=0)
    spend: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    leads_count: Mapped[int] = mapped_column(nullable=False, default=0)
    conversions_count: Mapped[int] = mapped_column(nullable=False, default=0)


class Employee(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "employees"
    __table_args__ = (
        UniqueConstraint("account_id", "employee_code"),
        Index("ix_employees_account_status", "account_id", "status"),
        Index("ix_employees_account_user_id", "account_id", "user_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    employee_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    role_title: Mapped[str | None] = mapped_column(String(128), nullable=True)
    department: Mapped[str | None] = mapped_column(String(128), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    hired_at: Mapped[date | None] = mapped_column(Date, nullable=True)


class Task(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "tasks"
    __table_args__ = (
        UniqueConstraint("account_id", "dedupe_key"),
        Index("ix_tasks_account_status_due_at", "account_id", "status", "due_at"),
        Index("ix_tasks_account_assignee_user_status", "account_id", "assignee_user_id", "status"),
        Index("ix_tasks_account_assignee_employee_status", "account_id", "assignee_employee_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    assignee_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    assignee_employee_id: Mapped[int | None] = mapped_column(ForeignKey("employees.id", ondelete="SET NULL"), nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    source_rule_id: Mapped[int | None] = mapped_column(ForeignKey("rules.id", ondelete="SET NULL"), nullable=True)
    dedupe_key: Mapped[str | None] = mapped_column(String(191), nullable=True)
    escalation_level: Mapped[int] = mapped_column(nullable=False, default=0)
    escalated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="manual")
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    priority: Mapped[str] = mapped_column(String(32), nullable=False, default="normal")
    due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    related_entity_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    related_entity_id: Mapped[str | None] = mapped_column(String(64), nullable=True)


class TaskEvent(Base, AccountScopedMixin):
    __tablename__ = "task_events"
    __table_args__ = (
        Index("ix_task_events_account_task_event_at", "account_id", "task_id", "event_at"),
        Index("ix_task_events_account_type_event_at", "account_id", "event_type", "event_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[int] = mapped_column(ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    payload_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class Alert(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "alerts"
    __table_args__ = (
        UniqueConstraint("account_id", "dedupe_key"),
        Index("ix_alerts_account_status_severity_last_detected", "account_id", "status", "severity", "last_detected_at"),
        Index("ix_alerts_account_code_status", "account_id", "code", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    assigned_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    source_rule_id: Mapped[int | None] = mapped_column(ForeignKey("rules.id", ondelete="SET NULL"), nullable=True)
    dedupe_key: Mapped[str | None] = mapped_column(String(191), nullable=True)
    code: Mapped[str] = mapped_column(String(128), nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False, default="warning")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    related_entity_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    related_entity_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Recommendation(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "recommendations"
    __table_args__ = (
        UniqueConstraint("account_id", "dedupe_key"),
        Index("ix_recommendations_account_status_created_at", "account_id", "status", "created_at"),
        Index("ix_recommendations_account_alert_id", "account_id", "alert_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    alert_id: Mapped[int | None] = mapped_column(ForeignKey("alerts.id", ondelete="SET NULL"), nullable=True)
    source_rule_id: Mapped[int | None] = mapped_column(ForeignKey("rules.id", ondelete="SET NULL"), nullable=True)
    dedupe_key: Mapped[str | None] = mapped_column(String(191), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    code: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    related_entity_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    related_entity_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dismissed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class StockItem(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "stock_items"
    __table_args__ = (
        UniqueConstraint("account_id", "warehouse_id", "product_id"),
        Index("ix_stock_items_account_product_id", "account_id", "product_id"),
        Index("ix_stock_items_account_warehouse_id", "account_id", "warehouse_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id", ondelete="CASCADE"), nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    quantity_on_hand: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    quantity_reserved: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    min_quantity: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    reorder_quantity: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    last_movement_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class StockMovement(Base, AccountScopedMixin):
    __tablename__ = "stock_movements"
    __table_args__ = (
        Index("ix_stock_movements_account_product_occurred_at", "account_id", "product_id", "occurred_at"),
        Index("ix_stock_movements_account_warehouse_occurred_at", "account_id", "warehouse_id", "occurred_at"),
        Index("ix_stock_movements_account_purchase_id", "account_id", "purchase_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id", ondelete="CASCADE"), nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    purchase_id: Mapped[int | None] = mapped_column(ForeignKey("purchases.id", ondelete="SET NULL"), nullable=True)
    movement_type: Mapped[str] = mapped_column(String(32), nullable=False)
    reference_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reference_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    quantity_delta: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    unit_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    notes_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class BalanceSnapshot(Base, AccountScopedMixin):
    __tablename__ = "balance_snapshots"
    __table_args__ = (
        UniqueConstraint("account_id", "bank_account_id", "snapshot_at"),
        Index("ix_balance_snapshots_account_bank_snapshot_at", "account_id", "bank_account_id", "snapshot_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    bank_account_id: Mapped[int] = mapped_column(ForeignKey("bank_accounts.id", ondelete="CASCADE"), nullable=False)
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    balance: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    available_balance: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class BankTransaction(Base, AccountScopedMixin):
    __tablename__ = "bank_transactions"
    __table_args__ = (
        UniqueConstraint("account_id", "bank_account_id", "provider_transaction_id"),
        Index("ix_bank_transactions_account_bank_posted_at", "account_id", "bank_account_id", "posted_at"),
        Index("ix_bank_transactions_account_posted_at", "account_id", "posted_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    bank_account_id: Mapped[int] = mapped_column(ForeignKey("bank_accounts.id", ondelete="CASCADE"), nullable=False)
    provider_transaction_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="RUB")
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    counterparty_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    balance_after: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    payload_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class DailyKPI(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "daily_kpis"
    __table_args__ = (
        UniqueConstraint("account_id", "kpi_date", "metric_code"),
        Index("ix_daily_kpis_account_kpi_date", "account_id", "kpi_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    kpi_date: Mapped[date] = mapped_column(Date, nullable=False)
    metric_code: Mapped[str] = mapped_column(String(128), nullable=False)
    value_numeric: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    payload_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class Document(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "documents"
    __table_args__ = (
        UniqueConstraint("account_id", "document_type", "document_number"),
        Index("ix_documents_account_issued_at", "account_id", "issued_at"),
        Index("ix_documents_account_customer_issued_at", "account_id", "customer_id", "issued_at"),
        Index("ix_documents_account_deal_id", "account_id", "deal_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"), nullable=True)
    deal_id: Mapped[int | None] = mapped_column(ForeignKey("deals.id", ondelete="SET NULL"), nullable=True)
    document_type: Mapped[str] = mapped_column(String(64), nullable=False)
    document_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="draft")
    file_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    storage_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="local")
    issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0.00"))
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="RUB")
    snapshot_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class InstallationRequest(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "installation_requests"
    __table_args__ = (
        Index("ix_installation_requests_account_status_scheduled_for", "account_id", "status", "scheduled_for"),
        Index("ix_installation_requests_account_customer_created_at", "account_id", "customer_id", "created_at"),
        Index("ix_installation_requests_account_deal_created_at", "account_id", "deal_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"), nullable=True)
    deal_id: Mapped[int | None] = mapped_column(ForeignKey("deals.id", ondelete="SET NULL"), nullable=True)
    assigned_employee_id: Mapped[int | None] = mapped_column(ForeignKey("employees.id", ondelete="SET NULL"), nullable=True)
    request_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    address: Mapped[str | None] = mapped_column(String(255), nullable=True)
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class KnowledgeItem(Base, AccountScopedMixin, TimestampMixin):
    __tablename__ = "knowledge_items"
    __table_args__ = (
        Index("ix_knowledge_items_account_status_created_at", "account_id", "status", "created_at"),
        Index("ix_knowledge_items_account_type_created_at", "account_id", "item_type", "created_at"),
        Index("ix_knowledge_items_account_customer_created_at", "account_id", "customer_id", "created_at"),
        Index("ix_knowledge_items_account_deal_created_at", "account_id", "deal_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"), nullable=True)
    deal_id: Mapped[int | None] = mapped_column(ForeignKey("deals.id", ondelete="SET NULL"), nullable=True)
    document_id: Mapped[int | None] = mapped_column(ForeignKey("documents.id", ondelete="SET NULL"), nullable=True)
    item_type: Mapped[str] = mapped_column(String(32), nullable=False, default="note")
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    visibility: Mapped[str] = mapped_column(String(32), nullable=False, default="internal")
    file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    content_size_bytes: Mapped[int | None] = mapped_column(nullable=True)
    content_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tags_json: Mapped[list[object]] = mapped_column(JSON, nullable=False, default=list)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
