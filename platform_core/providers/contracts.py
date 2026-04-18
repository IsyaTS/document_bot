from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal


@dataclass(frozen=True)
class SyncCursor:
    value: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class BankAccountRecord:
    external_id: str
    name: str
    currency: str
    account_mask: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class BankBalanceRecord:
    external_account_id: str
    snapshot_at: datetime
    balance: Decimal
    available_balance: Decimal | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class BankTransactionRecord:
    external_account_id: str
    provider_transaction_id: str
    direction: str
    posted_at: datetime
    amount: Decimal
    currency: str
    description: str | None = None
    counterparty_name: str | None = None
    balance_after: Decimal | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AdsCampaignRecord:
    external_id: str
    source: str
    name: str
    status: str
    started_at: date | None = None
    ended_at: date | None = None
    budget_amount: Decimal | None = None
    currency: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AdsMetricsRecord:
    campaign_external_id: str
    metric_date: date
    impressions: int
    clicks: int
    spend: Decimal
    leads_count: int = 0
    conversions_count: int = 0
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AdsLeadRecord:
    external_id: str
    title: str
    created_at: datetime
    source: str = "avito"
    status: str = "new"
    pipeline_stage: str = "new"
    contact_name: str | None = None
    phone: str | None = None
    email: str | None = None
    campaign_external_id: str | None = None
    customer_external_id: str | None = None
    first_response_due_at: datetime | None = None
    first_responded_at: datetime | None = None
    lost_reason: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ERPProductRecord:
    external_id: str
    sku: str | None
    name: str
    unit: str
    status: str
    list_price: Decimal | None = None
    cost_price: Decimal | None = None
    category_code: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ERPStockRecord:
    external_product_id: str
    external_warehouse_id: str
    quantity_on_hand: Decimal
    quantity_reserved: Decimal = Decimal("0")
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ERPStockMovementRecord:
    external_product_id: str
    external_warehouse_id: str
    movement_type: str
    quantity_delta: Decimal
    occurred_at: datetime
    unit_cost: Decimal | None = None
    external_reference_id: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ERPPurchaseRecord:
    external_id: str
    purchase_number: str | None
    status: str
    total_amount: Decimal
    currency: str
    ordered_at: datetime | None = None
    received_at: datetime | None = None
    supplier_external_id: str | None = None
    warehouse_external_id: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class MessageSendRequest:
    channel: str
    recipient_external_id: str
    body: str
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class MessageSendResult:
    provider_message_id: str
    sent_at: datetime
    status: str
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class InboundMessageRecord:
    external_message_id: str
    conversation_external_id: str
    sender_external_id: str
    received_at: datetime
    body: str
    metadata: dict[str, object] = field(default_factory=dict)


class ProviderAdapter(ABC):
    provider_kind: str
    provider_name: str

    def descriptor(self) -> dict[str, str]:
        return {"provider_kind": self.provider_kind, "provider_name": self.provider_name}


class BankProvider(ProviderAdapter):
    provider_kind = "banking"

    @abstractmethod
    def connect_account(self, credentials: dict[str, object]) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def fetch_accounts(self, credentials: dict[str, object]) -> list[BankAccountRecord]:
        raise NotImplementedError

    @abstractmethod
    def fetch_balances(self, credentials: dict[str, object], cursor: SyncCursor | None = None) -> tuple[list[BankBalanceRecord], SyncCursor]:
        raise NotImplementedError

    @abstractmethod
    def fetch_transactions(self, credentials: dict[str, object], cursor: SyncCursor | None = None) -> tuple[list[BankTransactionRecord], SyncCursor]:
        raise NotImplementedError

    @abstractmethod
    def handle_webhook(self, headers: dict[str, str], body: bytes) -> dict[str, object]:
        raise NotImplementedError


class AdsProvider(ProviderAdapter):
    provider_kind = "ads"

    @abstractmethod
    def fetch_campaigns(self, credentials: dict[str, object], cursor: SyncCursor | None = None) -> tuple[list[AdsCampaignRecord], SyncCursor]:
        raise NotImplementedError

    @abstractmethod
    def fetch_ad_metrics(
        self,
        credentials: dict[str, object],
        *,
        date_from: date,
        date_to: date,
        cursor: SyncCursor | None = None,
    ) -> tuple[list[AdsMetricsRecord], SyncCursor]:
        raise NotImplementedError

    @abstractmethod
    def fetch_lead_source_info(self, credentials: dict[str, object], lead_external_id: str) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def fetch_leads(
        self,
        credentials: dict[str, object],
        *,
        date_from: date,
        date_to: date,
        cursor: SyncCursor | None = None,
    ) -> tuple[list[AdsLeadRecord], SyncCursor]:
        raise NotImplementedError


class ERPProvider(ProviderAdapter):
    provider_kind = "erp"

    @abstractmethod
    def fetch_products(self, credentials: dict[str, object], cursor: SyncCursor | None = None) -> tuple[list[ERPProductRecord], SyncCursor]:
        raise NotImplementedError

    @abstractmethod
    def fetch_stock(self, credentials: dict[str, object], cursor: SyncCursor | None = None) -> tuple[list[ERPStockRecord], SyncCursor]:
        raise NotImplementedError

    @abstractmethod
    def fetch_movements(self, credentials: dict[str, object], cursor: SyncCursor | None = None) -> tuple[list[ERPStockMovementRecord], SyncCursor]:
        raise NotImplementedError

    @abstractmethod
    def fetch_purchases(self, credentials: dict[str, object], cursor: SyncCursor | None = None) -> tuple[list[ERPPurchaseRecord], SyncCursor]:
        raise NotImplementedError


class MessagingProvider(ProviderAdapter):
    provider_kind = "messaging"

    @abstractmethod
    def send(self, credentials: dict[str, object], request: MessageSendRequest) -> MessageSendResult:
        raise NotImplementedError

    @abstractmethod
    def receive_webhook(self, headers: dict[str, str], body: bytes) -> list[InboundMessageRecord]:
        raise NotImplementedError

    @abstractmethod
    def fetch_conversation_metrics(
        self,
        credentials: dict[str, object],
        *,
        date_from: date,
        date_to: date,
    ) -> dict[str, object]:
        raise NotImplementedError


class SpreadsheetProvider(ProviderAdapter):
    provider_kind = "spreadsheet"

    @abstractmethod
    def fetch_rows(
        self,
        credentials: dict[str, object],
        *,
        sheet_ref: str,
        cursor: SyncCursor | None = None,
    ) -> tuple[list[dict[str, object]], SyncCursor]:
        raise NotImplementedError


class ProviderRegistry:
    def __init__(self, providers: list[ProviderAdapter] | None = None) -> None:
        self._providers: dict[tuple[str, str], ProviderAdapter] = {}
        for provider in providers or []:
            self.register(provider)

    def register(self, provider: ProviderAdapter) -> None:
        self._providers[(provider.provider_kind, provider.provider_name)] = provider

    def get(self, provider_kind: str, provider_name: str) -> ProviderAdapter | None:
        return self._providers.get((provider_kind, provider_name))

    def descriptors(self) -> list[dict[str, str]]:
        return [provider.descriptor() for provider in self._providers.values()]
