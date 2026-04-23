from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import requests

from platform_core.providers.contracts import (
    AdsCampaignRecord,
    AdsLeadRecord,
    AdsMetricsRecord,
    AdsProvider,
    BankAccountRecord,
    BankBalanceRecord,
    BankProvider,
    BankTransactionRecord,
    InboundMessageRecord,
    ERPProductRecord,
    ERPPurchaseRecord,
    ERPProvider,
    ERPStockMovementRecord,
    ERPStockRecord,
    MessagingProvider,
    MessageSendResult,
    SpreadsheetProvider,
    SyncCursor,
)
from platform_core.providers.avito_client import AvitoAPIClient
from platform_core.providers.moysklad_client import MoySkladAPIClient
from platform_core.settings import load_platform_settings
from platform_core.telegram_accounts import send_saved_message_sync

logger = logging.getLogger(__name__)


def _dt(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _date(value: str | None) -> date | None:
    return _dt(value).date() if value else None


def _decimal(value: object | None, scale: str = "1") -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)) / Decimal(scale)


def _load_fixture(credentials: dict[str, object], key: str) -> list[dict[str, Any]]:
    fixture_payload = credentials.get("fixture_payload") or {}
    if isinstance(fixture_payload, str):
        fixture_payload = json.loads(fixture_payload)
    if isinstance(fixture_payload, dict):
        rows = fixture_payload.get(key, [])
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, dict)]
    return []


def _deep_value(payload: dict[str, Any], key_path: str) -> Any:
    current: Any = payload
    for part in key_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


class GenericBankProviderAdapter(BankProvider):
    provider_name = "generic_bank"

    def connect_account(self, credentials: dict[str, object]) -> dict[str, object]:
        return {
            "provider_name": self.provider_name,
            "mode": "fixture" if credentials.get("fixture_payload") else "manual",
            "connected": True,
        }

    def fetch_accounts(self, credentials: dict[str, object]) -> list[BankAccountRecord]:
        rows = _load_fixture(credentials, "accounts")
        return [
            BankAccountRecord(
                external_id=str(row["external_id"]),
                name=str(row["name"]),
                currency=str(row.get("currency", "RUB")),
                account_mask=str(row["account_mask"]) if row.get("account_mask") else None,
                metadata={k: v for k, v in row.items() if k not in {"external_id", "name", "currency", "account_mask"}},
            )
            for row in rows
        ]

    def fetch_balances(
        self,
        credentials: dict[str, object],
        cursor: SyncCursor | None = None,
    ) -> tuple[list[BankBalanceRecord], SyncCursor]:
        del cursor
        rows = _load_fixture(credentials, "balances")
        records = [
            BankBalanceRecord(
                external_account_id=str(row["external_account_id"]),
                snapshot_at=_dt(str(row["snapshot_at"])) or datetime.now(timezone.utc),
                balance=_decimal(row.get("balance")) or Decimal("0"),
                available_balance=_decimal(row.get("available_balance")),
                metadata={k: v for k, v in row.items() if k not in {"external_account_id", "snapshot_at", "balance", "available_balance"}},
            )
            for row in rows
        ]
        return records, SyncCursor(value={"balances": len(records)})

    def fetch_transactions(
        self,
        credentials: dict[str, object],
        cursor: SyncCursor | None = None,
    ) -> tuple[list[BankTransactionRecord], SyncCursor]:
        del cursor
        rows = _load_fixture(credentials, "transactions")
        records = [
            BankTransactionRecord(
                external_account_id=str(row["external_account_id"]),
                provider_transaction_id=str(row["provider_transaction_id"]),
                direction=str(row["direction"]),
                posted_at=_dt(str(row["posted_at"])) or datetime.now(timezone.utc),
                amount=_decimal(row.get("amount")) or Decimal("0"),
                currency=str(row.get("currency", "RUB")),
                description=str(row["description"]) if row.get("description") else None,
                counterparty_name=str(row["counterparty_name"]) if row.get("counterparty_name") else None,
                balance_after=_decimal(row.get("balance_after")),
                metadata={k: v for k, v in row.items() if k not in {"external_account_id", "provider_transaction_id", "direction", "posted_at", "amount", "currency", "description", "counterparty_name", "balance_after"}},
            )
            for row in rows
        ]
        return records, SyncCursor(value={"transactions": len(records)})

    def handle_webhook(self, headers: dict[str, str], body: bytes) -> dict[str, object]:
        payload = json.loads(body.decode("utf-8")) if body else {}
        return {"headers": headers, "payload": payload}


class AvitoAdsProviderAdapter(AdsProvider):
    provider_name = "avito"
    campaigns_path_template = "/messaging/v1/accounts/{account_ref}/campaigns"
    metrics_path_template = "/messaging/v1/accounts/{account_ref}/campaigns/stats"
    leads_path_template = "/messaging/v1/accounts/{account_ref}/leads"

    # Concrete live credential schema:
    # access_token, account_external_id,
    # optional: base_url, timeout_seconds, max_retries, backoff_seconds,
    # campaigns_params, metrics_params, leads_params.
    # Fixture mode additionally supports: fixture_payload, lead_sources.

    def connect_account(self, credentials: dict[str, object]) -> dict[str, object]:
        if credentials.get("fixture_payload"):
            return {
                "provider_name": self.provider_name,
                "mode": "fixture",
                "connected": True,
                "account_ref": self._account_ref(credentials) if credentials.get("account_external_id") else None,
            }
        account_ref = self._account_ref(credentials)
        profile: dict[str, Any] = {}
        try:
            profile = self._client(credentials).fetch_json("/core/v1/accounts/self")
        except requests.RequestException:
            profile = {}
        return {
            "provider_name": self.provider_name,
            "mode": "live",
            "connected": True,
            "account_ref": account_ref,
            "profile_name": self._string_field(profile, "name"),
            "profile_url": self._string_field(profile, "profile_url"),
        }

    def _cursor_token(self, cursor: SyncCursor | None, key: str) -> str | None:
        if cursor is None:
            return None
        value = cursor.value.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            nested = value.get("next_cursor")
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
        return None

    def _cursor_payload(self, next_cursor: str | None, record_count: int) -> SyncCursor:
        return SyncCursor(
            value={
                "next_cursor": next_cursor,
                "record_count": record_count,
                "exhausted": next_cursor is None,
            }
        )

    def _field(self, payload: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            value = _deep_value(payload, key)
            if value is not None and value != "":
                return value
        return None

    def _string_field(self, payload: dict[str, Any], *keys: str) -> str | None:
        value = self._field(payload, *keys)
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    def _int_field(self, payload: dict[str, Any], *keys: str) -> int:
        value = self._field(payload, *keys)
        if value is None or value == "":
            return 0
        return int(value)

    def _decimal_field(self, payload: dict[str, Any], *keys: str) -> Decimal | None:
        value = self._field(payload, *keys)
        return _decimal(value) if value is not None and value != "" else None

    def _datetime_field(self, payload: dict[str, Any], *keys: str) -> datetime | None:
        value = self._field(payload, *keys)
        return _dt(str(value)) if value is not None and value != "" else None

    def _date_field(self, payload: dict[str, Any], *keys: str) -> date | None:
        value = self._field(payload, *keys)
        return _date(str(value)) if value is not None and value != "" else None

    def _metadata(self, payload: dict[str, Any], excluded: set[str]) -> dict[str, object]:
        return {key: value for key, value in payload.items() if key not in excluded}

    def fetch_campaigns(self, credentials: dict[str, object], cursor=None):
        rows = _load_fixture(credentials, "campaigns")
        next_cursor = None
        if not rows:
            client = self._client(credentials)
            account_ref = self._account_ref(credentials)
            try:
                rows, next_cursor = client.fetch_paginated(
                    self.campaigns_path_template.format(account_ref=account_ref),
                    params=self._query_params(credentials, "campaigns"),
                    items_key="campaigns",
                    cursor=self._cursor_token(cursor, "campaigns"),
                    cursor_param="cursor",
                )
            except requests.HTTPError as exc:
                if self._is_nonfatal_avito_section_error(exc):
                    rows, next_cursor = [], None
                else:
                    raise
        records = [
            AdsCampaignRecord(
                external_id=str(self._field(row, "external_id", "campaign_id", "campaignId", "id", "itemId")),
                source=self._string_field(row, "source", "channel") or "avito",
                name=str(
                    self._field(
                        row,
                        "name",
                        "title",
                        "campaign_name",
                        "ad_name",
                        "item_title",
                        "id",
                    )
                ),
                status=self._string_field(row, "status", "state", "campaign_status", "ad_status") or "active",
                started_at=self._date_field(row, "started_at", "start_date", "startDate", "created_at", "createdAt"),
                ended_at=self._date_field(row, "ended_at", "end_date", "endDate", "finished_at"),
                budget_amount=self._decimal_field(row, "budget_amount", "budget", "budget.limit", "daily_budget"),
                currency=self._string_field(row, "currency", "budget.currency", "price.currency"),
                metadata=self._metadata(
                    row,
                    {
                        "external_id",
                        "campaign_id",
                        "campaignId",
                        "id",
                        "itemId",
                        "source",
                        "channel",
                        "name",
                        "title",
                        "campaign_name",
                        "ad_name",
                        "item_title",
                        "status",
                        "state",
                        "campaign_status",
                        "ad_status",
                        "started_at",
                        "start_date",
                        "startDate",
                        "created_at",
                        "createdAt",
                        "ended_at",
                        "end_date",
                        "endDate",
                        "finished_at",
                        "budget_amount",
                        "budget",
                        "daily_budget",
                        "currency",
                        "price",
                    },
                ),
            )
            for row in rows
        ]
        return records, self._cursor_payload(next_cursor, len(records))

    def fetch_ad_metrics(self, credentials: dict[str, object], *, date_from, date_to, cursor=None):
        rows = _load_fixture(credentials, "ad_metrics")
        next_cursor = None
        if not rows:
            client = self._client(credentials)
            account_ref = self._account_ref(credentials)
            try:
                rows, next_cursor = client.fetch_paginated(
                    self.metrics_path_template.format(account_ref=account_ref),
                    params={
                        **self._query_params(credentials, "metrics"),
                        "dateFrom": date_from.isoformat(),
                        "dateTo": date_to.isoformat(),
                    },
                    items_key="metrics",
                    cursor=self._cursor_token(cursor, "metrics"),
                    cursor_param="cursor",
                )
            except requests.HTTPError as exc:
                if self._is_nonfatal_avito_section_error(exc):
                    rows, next_cursor = [], None
                else:
                    raise
        records = []
        for row in rows:
            metric_date = self._date_field(row, "metric_date", "date", "stats_date", "day")
            if metric_date is None or metric_date < date_from or metric_date > date_to:
                continue
            records.append(
                AdsMetricsRecord(
                    campaign_external_id=str(
                        self._field(row, "campaign_external_id", "campaign_id", "campaignId", "id", "itemId")
                    ),
                    metric_date=metric_date,
                    impressions=self._int_field(row, "impressions", "views"),
                    clicks=self._int_field(row, "clicks", "contacts"),
                    spend=self._decimal_field(row, "spend", "spent", "cost") or Decimal("0"),
                    leads_count=self._int_field(row, "leads_count", "contacts", "leads", "uniq_contacts"),
                    conversions_count=self._int_field(row, "conversions_count", "conversions", "orders"),
                    metadata=self._metadata(
                        row,
                        {
                            "campaign_external_id",
                            "campaign_id",
                            "campaignId",
                            "id",
                            "itemId",
                            "metric_date",
                            "date",
                            "stats_date",
                            "day",
                            "impressions",
                            "views",
                            "clicks",
                            "contacts",
                            "spend",
                            "spent",
                            "cost",
                            "leads_count",
                            "leads",
                            "uniq_contacts",
                            "conversions_count",
                            "conversions",
                            "orders",
                        },
                    ),
                )
            )
        return records, self._cursor_payload(next_cursor, len(records))

    def fetch_lead_source_info(self, credentials: dict[str, object], lead_external_id: str) -> dict[str, object]:
        info_map = credentials.get("lead_sources", {})
        if isinstance(info_map, dict):
            return dict(info_map.get(lead_external_id, {}))
        return {}

    def fetch_lead_source_feed(
        self,
        credentials: dict[str, object],
        *,
        date_from,
        date_to,
        cursor=None,
    ) -> tuple[dict[str, dict[str, object]], SyncCursor]:
        rows = _load_fixture(credentials, "lead_source_feed")
        if not rows:
            rows = self._source_feed_rows(credentials)
        next_cursor = None
        path = self._string_field(credentials, "lead_source_feed_path", "conversation_feed_path")
        if not rows and path:
            client = self._client(credentials)
            account_ref = self._account_ref(credentials)
            try:
                rows, next_cursor = client.fetch_paginated(
                    path.format(account_ref=account_ref),
                    params={
                        **self._query_params(credentials, "lead_source_feed"),
                        "dateFrom": date_from.isoformat(),
                        "dateTo": date_to.isoformat(),
                    },
                    items_key=str(credentials.get("lead_source_feed_items_key") or "items"),
                    cursor=self._cursor_token(cursor, "lead_source_feed"),
                    cursor_param=str(credentials.get("lead_source_feed_cursor_param") or "cursor"),
                )
            except requests.HTTPError as exc:
                if self._is_nonfatal_avito_section_error(exc):
                    rows, next_cursor = [], None
                else:
                    raise
        feed: dict[str, dict[str, object]] = {}
        for row in rows:
            normalized = self._normalize_source_feed_row(row)
            lead_external_id = self._string_field(normalized, "lead_external_id")
            if lead_external_id:
                feed[lead_external_id] = normalized
        return feed, self._cursor_payload(next_cursor, len(feed))

    def fetch_leads(self, credentials: dict[str, object], *, date_from, date_to, cursor=None):
        rows = _load_fixture(credentials, "leads")
        next_cursor = None
        if not rows:
            client = self._client(credentials)
            account_ref = self._account_ref(credentials)
            try:
                rows, next_cursor = client.fetch_paginated(
                    self.leads_path_template.format(account_ref=account_ref),
                    params={
                        **self._query_params(credentials, "leads"),
                        "dateFrom": date_from.isoformat(),
                        "dateTo": date_to.isoformat(),
                    },
                    items_key="leads",
                    cursor=self._cursor_token(cursor, "leads"),
                    cursor_param="cursor",
                )
            except requests.HTTPError as exc:
                if self._is_nonfatal_avito_section_error(exc):
                    rows, next_cursor = [], None
                else:
                    raise
        records: list[AdsLeadRecord] = []
        for row in rows:
            created_at = self._datetime_field(
                row,
                "created_at",
                "createdAt",
                "published_at",
                "created",
                "conversation.created_at",
            ) or datetime.now(timezone.utc)
            created_date = created_at.date()
            if created_date < date_from or created_date > date_to:
                continue
            external_id = str(self._field(row, "external_id", "lead_id", "leadId", "id"))
            campaign_external_id = self._string_field(
                row,
                "campaign_external_id",
                "campaign_id",
                "campaignId",
                "item_id",
                "itemId",
                "ad_id",
            )
            customer_external_id = self._string_field(
                row,
                "customer_external_id",
                "contact_id",
                "customer_id",
                "customer.id",
                "contact.id",
                "user_id",
            )
            contact = row.get("contact") if isinstance(row.get("contact"), dict) else {}
            customer = row.get("customer") if isinstance(row.get("customer"), dict) else {}
            records.append(
                AdsLeadRecord(
                    external_id=external_id,
                    title=str(
                        self._field(
                            row,
                            "title",
                            "ad_title",
                            "subject",
                            "item_title",
                            "campaign_name",
                        )
                        or f"Avito lead {external_id}"
                    ),
                    created_at=created_at,
                    source=self._string_field(row, "source", "channel") or "avito",
                    status=self._string_field(row, "status", "lead_status", "source_status", "state") or "new",
                    pipeline_stage=self._string_field(row, "pipeline_stage", "stage", "status") or "new",
                    contact_name=self._string_field(
                        row,
                        "contact_name",
                        "contact.name",
                        "customer.name",
                    )
                    or (str(contact.get("name")) if contact.get("name") else None)
                    or (str(customer.get("name")) if customer.get("name") else None),
                    phone=self._string_field(row, "phone", "contact.phone", "customer.phone")
                    or (str(contact.get("phone")) if contact.get("phone") else None)
                    or (str(customer.get("phone")) if customer.get("phone") else None),
                    email=self._string_field(row, "email", "contact.email", "customer.email")
                    or (str(contact.get("email")) if contact.get("email") else None)
                    or (str(customer.get("email")) if customer.get("email") else None),
                    campaign_external_id=campaign_external_id,
                    customer_external_id=customer_external_id,
                    first_response_due_at=self._datetime_field(
                        row,
                        "first_response_due_at",
                        "response_due_at",
                        "sla.first_response_due_at",
                    ),
                    first_responded_at=self._datetime_field(
                        row,
                        "first_responded_at",
                        "first_response_at",
                        "responded_at",
                        "response.first_at",
                    ),
                    lost_reason=self._string_field(row, "lost_reason", "close_reason", "decline_reason"),
                    metadata=self._metadata(
                        row,
                        {
                            "external_id",
                            "lead_id",
                            "leadId",
                            "id",
                            "title",
                            "ad_title",
                            "subject",
                            "item_title",
                            "campaign_name",
                            "created_at",
                            "createdAt",
                            "published_at",
                            "created",
                            "source",
                            "channel",
                            "status",
                            "lead_status",
                            "source_status",
                            "state",
                            "pipeline_stage",
                            "stage",
                            "contact_name",
                            "phone",
                            "email",
                            "campaign_external_id",
                            "campaign_id",
                            "campaignId",
                            "item_id",
                            "itemId",
                            "ad_id",
                            "customer_external_id",
                            "contact_id",
                            "customer_id",
                            "user_id",
                            "first_response_due_at",
                            "response_due_at",
                            "sla",
                            "first_responded_at",
                            "first_response_at",
                            "responded_at",
                            "response",
                            "lost_reason",
                            "close_reason",
                            "decline_reason",
                            "contact",
                            "customer",
                            "conversation",
                        },
                    ),
                )
            )
        return records, self._cursor_payload(next_cursor, len(records))

    def _client(self, credentials: dict[str, object]) -> AvitoAPIClient:
        token = credentials.get("access_token") or credentials.get("bearer_token")
        if not token:
            raise ValueError("Avito credentials require access_token for live API mode.")
        return AvitoAPIClient(
            access_token=str(token),
            base_url=str(credentials.get("base_url") or "https://api.avito.ru"),
            timeout_seconds=int(credentials.get("timeout_seconds") or 30),
            max_retries=int(credentials.get("max_retries") or 3),
            backoff_seconds=float(credentials.get("backoff_seconds") or 1.0),
        )

    def _account_ref(self, credentials: dict[str, object]) -> str:
        account_ref = credentials.get("account_external_id") or credentials.get("account_id") or credentials.get("user_id")
        if not account_ref and not credentials.get("fixture_payload"):
            account_ref = self._resolve_account_ref_from_api(credentials)
            if account_ref:
                credentials["account_external_id"] = str(account_ref)
        if not account_ref:
            raise ValueError("Avito credentials require account_external_id for live API mode.")
        return str(account_ref)

    def _resolve_account_ref_from_api(self, credentials: dict[str, object]) -> str | None:
        client = self._client(credentials)
        candidate_paths = (
            "/core/v1/accounts/self",
            "/core/v1/accounts",
            "/messenger/v1/accounts",
        )
        for path in candidate_paths:
            try:
                payload = client.fetch_json(path)
            except requests.RequestException:
                continue
            resolved = self._extract_account_ref(payload)
            if resolved:
                return resolved
        return None

    def _extract_account_ref(self, payload: dict[str, Any]) -> str | None:
        if not isinstance(payload, dict):
            return None
        direct = self._string_field(payload, "id", "user_id", "account_id", "accountId", "userId")
        if direct:
            return direct
        for list_key in ("accounts", "result.accounts", "data.accounts", "items", "result.items"):
            rows = self._field(payload, list_key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                value = self._string_field(row, "id", "user_id", "account_id", "accountId", "userId")
                if value:
                    return value
        return None

    def _query_params(self, credentials: dict[str, object], section: str) -> dict[str, object]:
        params = credentials.get(f"{section}_params") or {}
        return dict(params) if isinstance(params, dict) else {}

    def _is_nonfatal_avito_section_error(self, exc: requests.HTTPError) -> bool:
        status_code = exc.response.status_code if exc.response is not None else None
        return status_code in {403, 404}

    def _source_feed_rows(self, credentials: dict[str, object]) -> list[dict[str, Any]]:
        raw = credentials.get("lead_sources")
        if isinstance(raw, dict):
            rows: list[dict[str, Any]] = []
            for lead_external_id, payload in raw.items():
                if not isinstance(payload, dict):
                    continue
                rows.append({"lead_external_id": lead_external_id, **payload})
            return rows
        if isinstance(raw, list):
            return [dict(item) for item in raw if isinstance(item, dict)]
        return []

    def _normalize_source_feed_row(self, row: dict[str, Any]) -> dict[str, object]:
        contact = row.get("contact") if isinstance(row.get("contact"), dict) else {}
        customer = row.get("customer") if isinstance(row.get("customer"), dict) else {}
        conversation = row.get("conversation") if isinstance(row.get("conversation"), dict) else {}
        normalized: dict[str, object] = {
            "lead_external_id": self._string_field(row, "lead_external_id", "lead_id", "leadId", "external_id", "id"),
            "source": self._string_field(row, "source", "source_name", "channel", "origin") or "avito",
            "source_status": self._string_field(row, "source_status", "conversation_status", "status"),
            "status": self._string_field(row, "status", "lead_status", "conversation.status"),
            "pipeline_stage": self._string_field(row, "pipeline_stage", "stage", "stage_code"),
            "campaign_external_id": self._string_field(
                row,
                "campaign_external_id",
                "campaign_id",
                "campaignId",
                "item_id",
                "itemId",
                "ad_id",
            ),
            "customer_external_id": self._string_field(
                row,
                "customer_external_id",
                "contact_id",
                "customer_id",
                "customer.id",
                "contact.id",
                "user_id",
            ),
            "contact_name": self._string_field(row, "contact_name", "contact.name", "customer.name")
            or (str(contact.get("name")) if contact.get("name") else None)
            or (str(customer.get("name")) if customer.get("name") else None),
            "phone": self._string_field(row, "phone", "contact.phone", "customer.phone")
            or (str(contact.get("phone")) if contact.get("phone") else None)
            or (str(customer.get("phone")) if customer.get("phone") else None),
            "email": self._string_field(row, "email", "contact.email", "customer.email")
            or (str(contact.get("email")) if contact.get("email") else None)
            or (str(customer.get("email")) if customer.get("email") else None),
            "first_response_due_at": self._datetime_field(
                row,
                "first_response_due_at",
                "response_due_at",
                "sla.first_response_due_at",
            ),
            "first_responded_at": self._datetime_field(
                row,
                "first_responded_at",
                "first_response_at",
                "responded_at",
                "response.first_at",
            ),
            "conversation_external_id": self._string_field(
                row,
                "conversation_external_id",
                "conversation_id",
                "chat_id",
                "dialog_id",
                "conversation.id",
            )
            or (str(conversation.get("id")) if conversation.get("id") else None),
            "conversation_created_at": self._datetime_field(
                row,
                "conversation_created_at",
                "conversation_started_at",
                "first_message_at",
                "conversation.created_at",
            ),
            "last_message_at": self._datetime_field(
                row,
                "last_message_at",
                "updated_at",
                "last_activity_at",
                "conversation.last_message_at",
                "last_incoming_message_at",
            ),
            "closed_at": self._datetime_field(row, "closed_at", "closedAt", "lost_at", "conversation.closed_at"),
            "lost_reason": self._string_field(row, "lost_reason", "close_reason", "decline_reason"),
        }
        for key in ("source", "source_status", "status", "pipeline_stage"):
            if normalized.get(key) is None:
                normalized.pop(key, None)
        normalized["metadata"] = self._metadata(
            row,
            {
                "lead_external_id",
                "lead_id",
                "leadId",
                "external_id",
                "id",
                "source",
                "source_name",
                "channel",
                "origin",
                "source_status",
                "conversation_status",
                "status",
                "lead_status",
                "pipeline_stage",
                "stage",
                "stage_code",
                "campaign_external_id",
                "campaign_id",
                "campaignId",
                "item_id",
                "itemId",
                "ad_id",
                "customer_external_id",
                "contact_id",
                "customer_id",
                "user_id",
                "contact_name",
                "phone",
                "email",
                "first_response_due_at",
                "response_due_at",
                "sla",
                "first_responded_at",
                "first_response_at",
                "responded_at",
                "response",
                "conversation_external_id",
                "conversation_id",
                "chat_id",
                "dialog_id",
                "conversation_created_at",
                "conversation_started_at",
                "first_message_at",
                "last_message_at",
                "updated_at",
                "last_activity_at",
                "last_incoming_message_at",
                "closed_at",
                "closedAt",
                "lost_at",
                "lost_reason",
                "close_reason",
                "decline_reason",
                "contact",
                "customer",
                "conversation",
            },
        )
        return {key: value for key, value in normalized.items() if value is not None}


class MoySkladERPProviderAdapter(ERPProvider):
    provider_name = "moysklad"

    def _cursor_int(self, cursor: SyncCursor | None, key: str, default: int) -> int:
        if cursor is None or not isinstance(cursor.value, dict):
            return default
        value = cursor.value.get(key)
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return default

    def _job_scope_int(self, credentials: dict[str, object], key: str, default: int) -> int:
        scope = credentials.get("_job_scope") or {}
        if isinstance(scope, dict):
            value = scope.get(key)
            try:
                return max(1, int(value))
            except (TypeError, ValueError):
                pass
        value = credentials.get(key)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return default

    def _row_ref(self, row: dict[str, Any]) -> str:
        return str(
            row.get("id")
            or row.get("name")
            or row.get("article")
            or row.get("external_id")
            or "unknown"
        ).strip()

    def _log_row_skip(self, *, entity_path: str, stage: str, row: dict[str, Any], exc: Exception) -> None:
        logger.warning(
            "moysklad_row_skipped entity_path=%s stage=%s row_ref=%s error=%s",
            entity_path,
            stage,
            self._row_ref(row),
            exc,
        )

    def _entity_id(self, value, *fallbacks) -> str | None:
        candidates = (value,) + fallbacks
        for candidate in candidates:
            if isinstance(candidate, dict):
                if candidate.get("id"):
                    return str(candidate["id"])
                href = str(candidate.get("href") or "").strip()
                if href:
                    normalized_href = href.split("?", 1)[0].rstrip("/")
                    entity_id = normalized_href.split("/")[-1]
                    if entity_id:
                        return entity_id
                meta = candidate.get("meta") or {}
                meta_href = str(meta.get("href") or "").strip()
                if meta_href:
                    normalized_href = meta_href.split("?", 1)[0].rstrip("/")
                    entity_id = normalized_href.split("/")[-1]
                    if entity_id:
                        return entity_id
            elif candidate not in (None, ""):
                text = str(candidate).strip()
                if not text:
                    continue
                if "/" in text or "?" in text:
                    normalized_href = text.split("?", 1)[0].rstrip("/")
                    entity_id = normalized_href.split("/")[-1]
                    if entity_id:
                        return entity_id
                return text
        return None

    def connect_account(self, credentials: dict[str, object]) -> dict[str, object]:
        if credentials.get("fixture_payload"):
            return {
                "provider_name": self.provider_name,
                "mode": "fixture",
                "connected": True,
            }
        products, _ = self.fetch_products(credentials)
        return {
            "provider_name": self.provider_name,
            "mode": "live",
            "connected": True,
            "products_sampled": len(products),
        }

    def fetch_products(self, credentials: dict[str, object], cursor=None):
        del cursor
        rows = _load_fixture(credentials, "products")
        if not rows:
            rows = self._client(credentials).fetch_all_rows("entity/assortment", params={"limit": 100})
        records: list[ERPProductRecord] = []
        skipped_rows = 0
        for row in rows:
            try:
                if row.get("archived") is True:
                    status = "archived"
                else:
                    status = "active"
                sale_prices = row.get("salePrices") or []
                sale_price = None
                if isinstance(sale_prices, list) and sale_prices:
                    sale_price = _decimal(sale_prices[0].get("value"), "100")
                buy_price = _decimal((row.get("buyPrice") or {}).get("value"), "100")
                category = row.get("productFolder") or {}
                records.append(
                    ERPProductRecord(
                        external_id=str(row["id"]),
                        sku=str(row["article"]) if row.get("article") else None,
                        name=str(row["name"]),
                        unit=str((row.get("uom") or {}).get("name", "pcs")),
                        status=status,
                        list_price=sale_price,
                        cost_price=buy_price,
                        category_code=str(category.get("id")) if category.get("id") else None,
                        metadata={
                            "category_name": category.get("name"),
                            "path_name": row.get("pathName"),
                            "raw_type": (row.get("meta") or {}).get("type"),
                        },
                    )
                )
            except Exception as exc:
                skipped_rows += 1
                self._log_row_skip(entity_path="entity/assortment", stage="fetch_products", row=row, exc=exc)
        return records, SyncCursor(value={"products": len(records), "skipped_rows": skipped_rows})

    def fetch_stock(self, credentials: dict[str, object], cursor=None):
        del cursor
        rows = _load_fixture(credentials, "stock")
        if not rows:
            rows = self._client(credentials).fetch_all_rows("report/stock/bystore", params={"limit": 100})
        records: list[ERPStockRecord] = []
        skipped_rows = 0
        for row in rows:
            try:
                stock_by_store = row.get("stockByStore") or row.get("stock_by_store") or []
                product_id = self._entity_id(
                    row.get("external_product_id"),
                    row.get("assortment"),
                    row.get("product"),
                    row.get("meta"),
                    row.get("id"),
                )
                product_name = row.get("product_name") or (row.get("assortment") or {}).get("name") or row.get("name")
                if isinstance(stock_by_store, list) and stock_by_store:
                    for item in stock_by_store:
                        warehouse_id = self._entity_id(
                            item.get("external_warehouse_id"),
                            item.get("store"),
                            item.get("warehouse"),
                            item.get("meta"),
                            item.get("storeId"),
                        )
                        if not product_id or not warehouse_id:
                            continue
                        records.append(
                            ERPStockRecord(
                                external_product_id=product_id,
                                external_warehouse_id=warehouse_id,
                                quantity_on_hand=_decimal(item.get("quantity_on_hand", item.get("stock"))) or Decimal("0"),
                                quantity_reserved=_decimal(item.get("quantity_reserved", item.get("reserve"))) or Decimal("0"),
                                metadata={
                                    "warehouse_name": item.get("warehouse_name")
                                    or item.get("name")
                                    or (item.get("store") or {}).get("name")
                                    or (item.get("warehouse") or {}).get("name"),
                                    "product_name": product_name,
                                },
                            )
                        )
                    continue
                warehouse_id = self._entity_id(
                    row.get("external_warehouse_id"),
                    row.get("store"),
                    row.get("warehouse"),
                    row.get("storeId"),
                )
                if not product_id or not warehouse_id:
                    continue
                records.append(
                    ERPStockRecord(
                        external_product_id=product_id,
                        external_warehouse_id=warehouse_id,
                        quantity_on_hand=_decimal(row.get("quantity_on_hand", row.get("stock"))) or Decimal("0"),
                        quantity_reserved=_decimal(row.get("quantity_reserved", row.get("reserve"))) or Decimal("0"),
                        metadata={
                            "warehouse_name": row.get("warehouse_name")
                            or row.get("name")
                            or (row.get("store") or {}).get("name")
                            or (row.get("warehouse") or {}).get("name"),
                            "product_name": product_name,
                        },
                    )
                )
            except Exception as exc:
                skipped_rows += 1
                self._log_row_skip(entity_path="report/stock/bystore", stage="fetch_stock", row=row, exc=exc)
        return records, SyncCursor(value={"stock": len(records), "skipped_rows": skipped_rows})

    def fetch_movements(self, credentials: dict[str, object], cursor=None):
        records: list[ERPStockMovementRecord] = []
        transfer_rows = _load_fixture(credentials, "movements")
        demand_rows = _load_fixture(credentials, "demands")
        demand_offset = self._cursor_int(cursor, "demand_offset", 0)
        demand_limit = self._job_scope_int(credentials, "movements_demand_page_size", 100)
        demand_max_pages = self._job_scope_int(credentials, "movements_demand_max_pages", 5)
        demand_completed = False
        demand_pages_fetched = 0
        next_demand_offset = demand_offset
        if not transfer_rows and not demand_rows:
            client = self._client(credentials)
            transfer_rows = client.fetch_all_rows("entity/move", params={"limit": 100, "expand": "positions,targetStore,sourceStore"})
            logger.info(
                "moysklad_fetch_movements_window provider=%s stage=%s cursor_offset=%s page_size=%s max_pages=%s result=%s",
                self.provider_name,
                "fetch_movements.demand",
                demand_offset,
                demand_limit,
                demand_max_pages,
                "start",
            )
            for page_index in range(demand_max_pages):
                current_offset = demand_offset + (page_index * demand_limit)
                rows = client.fetch_rows(
                    "entity/demand",
                    params={
                        "limit": demand_limit,
                        "offset": current_offset,
                        "expand": "positions,store,agent",
                    },
                )
                demand_rows.extend(rows)
                demand_pages_fetched += 1
                next_demand_offset = current_offset + len(rows)
                if not rows or len(rows) < demand_limit:
                    demand_completed = True
                    break
            logger.info(
                "moysklad_fetch_movements_window provider=%s stage=%s cursor_offset=%s fetched_count=%s pages_fetched=%s next_cursor=%s completed_window=%s result=%s",
                self.provider_name,
                "fetch_movements.demand",
                demand_offset,
                len(demand_rows),
                demand_pages_fetched,
                next_demand_offset,
                demand_completed,
                "done",
            )
        elif demand_rows:
            next_demand_offset = len(demand_rows)
            demand_completed = True
        skipped_rows = 0
        for row in transfer_rows:
            try:
                positions = ((row.get("positions") or {}).get("rows") or []) if isinstance(row.get("positions"), dict) else []
                warehouse_id = self._entity_id(row.get("targetStore"), row.get("sourceStore"))
                warehouse_name = ((row.get("targetStore") or {}).get("name") if isinstance(row.get("targetStore"), dict) else None) or ((row.get("sourceStore") or {}).get("name") if isinstance(row.get("sourceStore"), dict) else None)
                occurred_at = _dt(row.get("moment")) or datetime.now(timezone.utc)
                move_ref = str(row.get("id") or row.get("name") or "").strip()
                if not positions:
                    continue
                for position in positions:
                    product_id = self._entity_id(position.get("external_product_id"), position.get("assortment"))
                    if not product_id or not warehouse_id:
                        continue
                    quantity = _decimal(position.get("quantity")) or Decimal("0")
                    if quantity == 0:
                        continue
                    line_ref = str(position.get("id") or product_id).strip()
                    records.append(
                        ERPStockMovementRecord(
                            external_product_id=product_id,
                            external_warehouse_id=warehouse_id,
                            movement_type="transfer",
                            quantity_delta=quantity,
                            occurred_at=occurred_at,
                            unit_cost=_decimal(position.get("price"), "100") or _decimal(row.get("sum"), "100"),
                            external_reference_id=f"move:{move_ref}:{line_ref}" if move_ref else None,
                            metadata={
                                "warehouse_name": warehouse_name,
                                "source_warehouse_name": (row.get("sourceStore") or {}).get("name") if isinstance(row.get("sourceStore"), dict) else None,
                                "target_warehouse_name": (row.get("targetStore") or {}).get("name") if isinstance(row.get("targetStore"), dict) else None,
                                "move_name": row.get("name"),
                            },
                        )
                    )
            except Exception as exc:
                skipped_rows += 1
                self._log_row_skip(entity_path="entity/move", stage="fetch_movements.transfer", row=row, exc=exc)
        for row in demand_rows:
            try:
                positions = ((row.get("positions") or {}).get("rows") or []) if isinstance(row.get("positions"), dict) else []
                warehouse_id = self._entity_id(row.get("external_warehouse_id"), row.get("store"))
                warehouse_name = row.get("warehouse_name") or (row.get("store") or {}).get("name")
                occurred_at = _dt(row.get("moment")) or datetime.now(timezone.utc)
                demand_ref = str(row.get("id") or row.get("name") or row.get("demand_number") or "").strip()
                if not positions:
                    continue
                for position in positions:
                    product_id = self._entity_id(position.get("external_product_id"), position.get("assortment"))
                    if not product_id or not warehouse_id:
                        continue
                    quantity = _decimal(position.get("quantity")) or Decimal("0")
                    if quantity == 0:
                        continue
                    unit_price = _decimal(position.get("price"), "100")
                    total_amount = _decimal(position.get("sum"), "100")
                    line_ref = str(position.get("id") or product_id).strip()
                    records.append(
                        ERPStockMovementRecord(
                            external_product_id=product_id,
                            external_warehouse_id=warehouse_id,
                            movement_type="shipment_dispatch",
                            quantity_delta=-quantity,
                            occurred_at=occurred_at,
                            unit_cost=_decimal(position.get("cost"), "100") or _decimal(position.get("buyPrice"), "100"),
                            external_reference_id=f"demand:{demand_ref}:{line_ref}" if demand_ref else None,
                            metadata={
                                "warehouse_name": warehouse_name,
                                "customer_external_id": self._entity_id(row.get("customer_external_id"), row.get("agent")),
                                "customer_name": row.get("customer_name") or (row.get("agent") or {}).get("name"),
                                "unit_price": str(unit_price) if unit_price is not None else None,
                                "total_amount": str(total_amount) if total_amount is not None else None,
                                "document_number": row.get("demand_number") or row.get("name"),
                                "demand_name": row.get("name"),
                                "source_document_type": "demand",
                            },
                        )
                    )
            except Exception as exc:
                skipped_rows += 1
                self._log_row_skip(entity_path="entity/demand", stage="fetch_movements.demand", row=row, exc=exc)
        return records, SyncCursor(
            value={
                "movements": len(records),
                "skipped_rows": skipped_rows,
                "demand_offset": next_demand_offset,
                "demand_page_size": demand_limit,
                "demand_pages_fetched": demand_pages_fetched,
                "demand_documents_fetched": len(demand_rows),
                "demand_completed": demand_completed,
                "completed_window": demand_completed,
                "next_cursor": {"demand_offset": next_demand_offset},
            }
        )

    def fetch_purchases(self, credentials: dict[str, object], cursor=None):
        del cursor
        rows = _load_fixture(credentials, "purchases")
        if not rows:
            rows = self._client(credentials).fetch_all_rows("entity/supply", params={"limit": 100, "expand": "agent,store"})
        records: list[ERPPurchaseRecord] = []
        skipped_rows = 0
        for row in rows:
            try:
                records.append(
                    ERPPurchaseRecord(
                        external_id=str(row["external_id"] if "external_id" in row else row["id"]),
                        purchase_number=str(row.get("purchase_number") or row.get("name")) if row.get("purchase_number") or row.get("name") else None,
                        status=str(row.get("status", "received" if row.get("applicable", True) else "draft")),
                        total_amount=_decimal(row.get("total_amount", row.get("sum")), "100") or Decimal("0"),
                        currency=str(row.get("currency", "RUB")),
                        ordered_at=_dt(row.get("ordered_at") or row.get("moment")),
                        received_at=_dt(row.get("received_at") or row.get("moment")),
                        supplier_external_id=self._entity_id(row.get("supplier_external_id"), row.get("agent")),
                        warehouse_external_id=self._entity_id(row.get("warehouse_external_id"), row.get("store")),
                        metadata={
                            "supplier_name": row.get("supplier_name") or (row.get("agent") or {}).get("name"),
                            "warehouse_name": row.get("warehouse_name") or (row.get("store") or {}).get("name"),
                        },
                    )
                )
            except Exception as exc:
                skipped_rows += 1
                self._log_row_skip(entity_path="entity/supply", stage="fetch_purchases", row=row, exc=exc)
        return records, SyncCursor(value={"purchases": len(records), "skipped_rows": skipped_rows})

    def _client(self, credentials: dict[str, object]) -> MoySkladAPIClient:
        login = str(credentials.get("login") or "")
        password = str(credentials.get("password") or "")
        if not login or not password:
            raise ValueError("MoySklad credentials require login and password for live API mode.")
        return MoySkladAPIClient(
            login=login,
            password=password,
            base_url=str(credentials.get("base_url") or "https://api.moysklad.ru/api/remap/1.2"),
            timeout_seconds=int(credentials.get("timeout_seconds", 30)),
        )


class TelegramMessagingProviderAdapter(MessagingProvider):
    provider_name = "telegram"

    def send(self, credentials: dict[str, object], request):
        settings = load_platform_settings()
        if not settings.telegram_api_id or not settings.telegram_api_hash:
            raise ValueError("PLATFORM_TELEGRAM_API_ID and PLATFORM_TELEGRAM_API_HASH are required.")
        session_string = str(credentials.get("session_string") or "").strip()
        if not session_string:
            raise ValueError("Telegram credentials require session_string.")
        result = send_saved_message_sync(
            api_id=settings.telegram_api_id,
            api_hash=settings.telegram_api_hash,
            session_string=session_string,
            text=request.body,
            peer=str(request.recipient_external_id or "me").strip() or "me",
        )
        return MessageSendResult(
            provider_message_id=str(result.get("message_id") or "telegram-message"),
            sent_at=result.get("sent_at") if isinstance(result.get("sent_at"), datetime) else datetime.now(timezone.utc),
            status="sent",
            metadata={"telegram_response": result},
        )

    def receive_webhook(self, headers: dict[str, str], body: bytes):
        del headers
        payload = json.loads(body.decode("utf-8"))
        items = []
        for field_name in ("message", "edited_message", "channel_post", "edited_channel_post"):
            message = payload.get(field_name)
            if not isinstance(message, dict):
                continue
            message_id = message.get("message_id")
            chat = message.get("chat") or {}
            sender = message.get("from") or chat
            body_text = str(message.get("text") or message.get("caption") or "").strip()
            if not body_text:
                continue
            received_at = datetime.fromtimestamp(int(message.get("date") or datetime.now(timezone.utc).timestamp()), tz=timezone.utc)
            items.append(
                InboundMessageRecord(
                    external_message_id=str(message_id or payload.get("update_id") or "telegram"),
                    conversation_external_id=str(chat.get("id") or sender.get("id") or "telegram-chat"),
                    sender_external_id=str(sender.get("id") or chat.get("id") or "telegram-user"),
                    received_at=received_at,
                    body=body_text,
                    metadata={
                        "channel": "message",
                        "direction": "inbound",
                        "provider_name": "telegram",
                        "title": f"Telegram message from {sender.get('username') or sender.get('first_name') or sender.get('id') or 'user'}",
                        "chat_type": chat.get("type"),
                        "sender_username": sender.get("username"),
                    },
                )
            )
        return items

    def fetch_conversation_metrics(self, credentials: dict[str, object], *, date_from, date_to):
        del credentials, date_from, date_to
        return {"provider_name": "telegram", "status": "webhook_only"}


class WhatsAppMessagingProviderAdapter(MessagingProvider):
    provider_name = "whatsapp"

    def send(self, credentials: dict[str, object], request):
        api_token = str(credentials.get("api_token") or "")
        phone_number_id = str(credentials.get("phone_number_id") or "")
        if not api_token or not phone_number_id:
            raise ValueError("WhatsApp credentials require api_token and phone_number_id.")
        response = requests.post(
            f"https://graph.facebook.com/v21.0/{phone_number_id}/messages",
            headers={"Authorization": f"Bearer {api_token}"},
            json={
                "messaging_product": "whatsapp",
                "to": request.recipient_external_id,
                "type": "text",
                "text": {"body": request.body},
            },
            timeout=int(credentials.get("timeout_seconds", 15)),
        )
        response.raise_for_status()
        payload = response.json()
        message_ids = payload.get("messages") or []
        provider_message_id = str((message_ids[0] or {}).get("id") if message_ids else "whatsapp-message")
        return MessageSendResult(
            provider_message_id=provider_message_id,
            sent_at=datetime.now(timezone.utc),
            status="sent",
            metadata={"whatsapp_response": payload},
        )

    def receive_webhook(self, headers: dict[str, str], body: bytes):
        del headers
        payload = json.loads(body.decode("utf-8"))
        records: list[InboundMessageRecord] = []
        for entry in payload.get("entry") or []:
            for change in (entry or {}).get("changes") or []:
                value = (change or {}).get("value") or {}
                contacts = value.get("contacts") or []
                contact_name = ((contacts[0] or {}).get("profile") or {}).get("name") if contacts else None
                for message in value.get("messages") or []:
                    message_type = message.get("type")
                    if message_type == "text":
                        body_text = str(((message.get("text") or {}).get("body") or "")).strip()
                    elif message_type == "button":
                        body_text = str(((message.get("button") or {}).get("text") or "")).strip()
                    else:
                        body_text = ""
                    if not body_text:
                        continue
                    ts = int(message.get("timestamp") or datetime.now(timezone.utc).timestamp())
                    records.append(
                        InboundMessageRecord(
                            external_message_id=str(message.get("id") or f"wa-{ts}"),
                            conversation_external_id=str(value.get("metadata", {}).get("display_phone_number") or message.get("from") or "whatsapp-chat"),
                            sender_external_id=str(message.get("from") or "whatsapp-user"),
                            received_at=datetime.fromtimestamp(ts, tz=timezone.utc),
                            body=body_text,
                            metadata={
                                "channel": "message",
                                "direction": "inbound",
                                "provider_name": "whatsapp",
                                "title": f"WhatsApp message from {contact_name or message.get('from') or 'user'}",
                                "contact_name": contact_name,
                                "message_type": message_type,
                            },
                        )
                    )
        return records

    def fetch_conversation_metrics(self, credentials: dict[str, object], *, date_from, date_to):
        del credentials, date_from, date_to
        return {"provider_name": "whatsapp", "status": "webhook_only"}


class GoogleSheetsSpreadsheetProviderAdapter(SpreadsheetProvider):
    provider_name = "google_sheets"

    def fetch_rows(self, credentials: dict[str, object], *, sheet_ref: str, cursor=None):
        del credentials, sheet_ref, cursor
        raise NotImplementedError("GoogleSheetsSpreadsheetProviderAdapter is not implemented in this stage.")
