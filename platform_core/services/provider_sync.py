from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.models import (
    AdMetric,
    BankAccount,
    BankTransaction,
    BalanceSnapshot,
    Campaign,
    Customer,
    Integration,
    IntegrationEntityMapping,
    Lead,
    LeadEvent,
    Product,
    ProductCategory,
    Purchase,
    StockItem,
    StockMovement,
    Warehouse,
)
from platform_core.providers.contracts import (
    AdsCampaignRecord,
    AdsLeadRecord,
    AdsMetricsRecord,
    BankAccountRecord,
    BankBalanceRecord,
    BankTransactionRecord,
    ERPProductRecord,
    ERPPurchaseRecord,
    ERPStockMovementRecord,
    ERPStockRecord,
)


@dataclass(frozen=True)
class SyncStats:
    created: int = 0
    updated: int = 0
    skipped: int = 0

    def as_dict(self) -> dict[str, int]:
        return {"created": self.created, "updated": self.updated, "skipped": self.skipped}


class IntegrationMappingService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def resolve(
        self,
        *,
        account_id: int,
        integration_id: int,
        provider_entity_type: str,
        external_id: str,
    ) -> IntegrationEntityMapping | None:
        return self.session.execute(
            select(IntegrationEntityMapping).where(
                IntegrationEntityMapping.account_id == account_id,
                IntegrationEntityMapping.integration_id == integration_id,
                IntegrationEntityMapping.provider_entity_type == provider_entity_type,
                IntegrationEntityMapping.external_id == external_id,
            )
        ).scalar_one_or_none()

    def upsert(
        self,
        *,
        account_id: int,
        integration_id: int,
        provider_entity_type: str,
        external_id: str,
        canonical_entity_type: str,
        canonical_entity_id: int | str,
        metadata: dict[str, object] | None = None,
    ) -> IntegrationEntityMapping:
        mapping = self.resolve(
            account_id=account_id,
            integration_id=integration_id,
            provider_entity_type=provider_entity_type,
            external_id=external_id,
        )
        now = datetime.now(timezone.utc)
        canonical_id = str(canonical_entity_id)
        if mapping is None:
            mapping = IntegrationEntityMapping(
                account_id=account_id,
                integration_id=integration_id,
                provider_entity_type=provider_entity_type,
                external_id=external_id,
                canonical_entity_type=canonical_entity_type,
                canonical_entity_id=canonical_id,
                metadata_json=metadata or {},
                last_seen_at=now,
            )
            self.session.add(mapping)
            self.session.flush()
            return mapping
        mapping.canonical_entity_type = canonical_entity_type
        mapping.canonical_entity_id = canonical_id
        mapping.metadata_json = metadata or mapping.metadata_json
        mapping.last_seen_at = now
        self.session.flush()
        return mapping


class BankSyncService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def sync_accounts(self, integration: Integration, records: list[BankAccountRecord]) -> tuple[SyncStats, dict[str, BankAccount]]:
        created = updated = skipped = 0
        by_external_id: dict[str, BankAccount] = {}
        for record in records:
            existing = self.session.execute(
                select(BankAccount).where(
                    BankAccount.account_id == integration.account_id,
                    BankAccount.provider == integration.provider_name,
                    BankAccount.external_id == record.external_id,
                )
            ).scalar_one_or_none()
            if existing is None:
                existing = BankAccount(
                    account_id=integration.account_id,
                    provider=integration.provider_name,
                    external_id=record.external_id,
                    name=record.name,
                    account_mask=record.account_mask,
                    currency=record.currency,
                    status="active",
                )
                self.session.add(existing)
                self.session.flush()
                created += 1
            else:
                changed = False
                if existing.name != record.name:
                    existing.name = record.name
                    changed = True
                if existing.account_mask != record.account_mask:
                    existing.account_mask = record.account_mask
                    changed = True
                if existing.currency != record.currency:
                    existing.currency = record.currency
                    changed = True
                if existing.status != "active":
                    existing.status = "active"
                    changed = True
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
            by_external_id[record.external_id] = existing
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped), by_external_id

    def sync_balances(
        self,
        integration: Integration,
        bank_accounts: dict[str, BankAccount],
        records: list[BankBalanceRecord],
    ) -> SyncStats:
        created = updated = skipped = 0
        for record in records:
            bank_account = bank_accounts.get(record.external_account_id)
            if bank_account is None:
                skipped += 1
                continue
            snapshot = self.session.execute(
                select(BalanceSnapshot).where(
                    BalanceSnapshot.account_id == integration.account_id,
                    BalanceSnapshot.bank_account_id == bank_account.id,
                    BalanceSnapshot.snapshot_at == record.snapshot_at,
                )
            ).scalar_one_or_none()
            if snapshot is None:
                self.session.add(
                    BalanceSnapshot(
                        account_id=integration.account_id,
                        bank_account_id=bank_account.id,
                        snapshot_at=record.snapshot_at,
                        balance=record.balance,
                        available_balance=record.available_balance,
                    )
                )
                created += 1
            else:
                changed = False
                if snapshot.balance != record.balance:
                    snapshot.balance = record.balance
                    changed = True
                if snapshot.available_balance != record.available_balance:
                    snapshot.available_balance = record.available_balance
                    changed = True
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped)

    def sync_transactions(
        self,
        integration: Integration,
        bank_accounts: dict[str, BankAccount],
        records: list[BankTransactionRecord],
    ) -> SyncStats:
        created = updated = skipped = 0
        for record in records:
            bank_account = bank_accounts.get(record.external_account_id)
            if bank_account is None:
                skipped += 1
                continue
            transaction = self.session.execute(
                select(BankTransaction).where(
                    BankTransaction.account_id == integration.account_id,
                    BankTransaction.bank_account_id == bank_account.id,
                    BankTransaction.provider_transaction_id == record.provider_transaction_id,
                )
            ).scalar_one_or_none()
            if transaction is None:
                transaction = BankTransaction(
                    account_id=integration.account_id,
                    bank_account_id=bank_account.id,
                    provider_transaction_id=record.provider_transaction_id,
                    direction=record.direction,
                    posted_at=record.posted_at,
                    amount=record.amount,
                    currency=record.currency,
                    description=record.description,
                    counterparty_name=record.counterparty_name,
                    balance_after=record.balance_after,
                    payload_json=record.metadata,
                )
                self.session.add(transaction)
                created += 1
            else:
                changed = False
                for attr, value in {
                    "direction": record.direction,
                    "posted_at": record.posted_at,
                    "amount": record.amount,
                    "currency": record.currency,
                    "description": record.description,
                    "counterparty_name": record.counterparty_name,
                    "balance_after": record.balance_after,
                    "payload_json": record.metadata,
                }.items():
                    if getattr(transaction, attr) != value:
                        setattr(transaction, attr, value)
                        changed = True
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped)


class AdsSyncService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.mapping_service = IntegrationMappingService(session)

    def sync_campaigns(self, integration: Integration, records: list[AdsCampaignRecord]) -> SyncStats:
        created = updated = skipped = 0
        for record in records:
            mapping = self.mapping_service.resolve(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="campaign",
                external_id=record.external_id,
            )
            campaign = self.session.get(Campaign, int(mapping.canonical_entity_id)) if mapping is not None else None
            if campaign is None:
                campaign = self.session.execute(
                    select(Campaign).where(
                        Campaign.account_id == integration.account_id,
                        Campaign.source == record.source,
                        Campaign.external_id == record.external_id,
                    )
                ).scalar_one_or_none()
            if campaign is None:
                campaign = Campaign(
                    account_id=integration.account_id,
                    source=record.source,
                    external_id=record.external_id,
                    name=record.name,
                    status=record.status,
                    started_at=record.started_at,
                    ended_at=record.ended_at,
                    budget_amount=record.budget_amount or Decimal("0"),
                    currency=record.currency or "RUB",
                )
                self.session.add(campaign)
                self.session.flush()
                created += 1
            else:
                changed = False
                for attr, value in {
                    "source": record.source,
                    "external_id": record.external_id,
                    "name": record.name,
                    "status": record.status,
                    "started_at": record.started_at,
                    "ended_at": record.ended_at,
                    "budget_amount": record.budget_amount or Decimal("0"),
                    "currency": record.currency or "RUB",
                }.items():
                    if getattr(campaign, attr) != value:
                        setattr(campaign, attr, value)
                        changed = True
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
            self.mapping_service.upsert(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="campaign",
                external_id=record.external_id,
                canonical_entity_type="campaign",
                canonical_entity_id=campaign.id,
                metadata={
                    "name": record.name,
                    "status": record.status,
                    "source": record.source,
                    "currency": record.currency,
                },
            )
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped)

    def sync_ad_metrics(self, integration: Integration, records: list[AdsMetricsRecord]) -> SyncStats:
        created = updated = skipped = 0
        for record in records:
            campaign = self._resolve_campaign(integration, record.campaign_external_id)
            if campaign is None:
                skipped += 1
                continue
            metric = self.session.execute(
                select(AdMetric).where(
                    AdMetric.account_id == integration.account_id,
                    AdMetric.campaign_id == campaign.id,
                    AdMetric.metric_date == record.metric_date,
                )
            ).scalar_one_or_none()
            if metric is None:
                metric = AdMetric(
                    account_id=integration.account_id,
                    campaign_id=campaign.id,
                    metric_date=record.metric_date,
                    impressions=record.impressions,
                    clicks=record.clicks,
                    spend=record.spend,
                    leads_count=record.leads_count,
                    conversions_count=record.conversions_count,
                )
                self.session.add(metric)
                created += 1
            else:
                changed = False
                for attr, value in {
                    "impressions": record.impressions,
                    "clicks": record.clicks,
                    "spend": record.spend,
                    "leads_count": record.leads_count,
                    "conversions_count": record.conversions_count,
                }.items():
                    if getattr(metric, attr) != value:
                        setattr(metric, attr, value)
                        changed = True
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
            self.mapping_service.upsert(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="ad_metric",
                external_id=f"{record.campaign_external_id}:{record.metric_date.isoformat()}",
                canonical_entity_type="ad_metric",
                canonical_entity_id=metric.id,
                metadata={
                    "campaign_external_id": record.campaign_external_id,
                    "metric_date": record.metric_date.isoformat(),
                    "leads_count": record.leads_count,
                    "spend": str(record.spend),
                },
            )
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped)

    def sync_leads(self, integration: Integration, records: list[AdsLeadRecord]) -> tuple[SyncStats, SyncStats, SyncStats]:
        customer_created = customer_updated = customer_skipped = 0
        lead_created = lead_updated = lead_skipped = 0
        event_created = event_updated = event_skipped = 0

        for record in records:
            customer, customer_status = self._resolve_customer_for_lead(integration, record)
            if customer_status == "created":
                customer_created += 1
            elif customer_status == "updated":
                customer_updated += 1
            else:
                customer_skipped += 1

            mapping = self.mapping_service.resolve(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="lead",
                external_id=record.external_id,
            )
            lead = self.session.get(Lead, int(mapping.canonical_entity_id)) if mapping is not None else None
            previous_status = lead.status if lead is not None else None
            if lead is None:
                lead = self.session.execute(
                    select(Lead).where(
                        Lead.account_id == integration.account_id,
                        Lead.source == record.source,
                        Lead.external_id == record.external_id,
                    )
                ).scalar_one_or_none()
            if lead is None:
                lead = self.session.execute(
                    select(Lead).where(
                        Lead.account_id == integration.account_id,
                        Lead.external_id == record.external_id,
                    )
                ).scalars().first()
            if lead is None:
                lead = Lead(
                    account_id=integration.account_id,
                    customer_id=customer.id if customer else None,
                    source=record.source,
                    external_id=record.external_id,
                    title=record.title,
                    contact_name=record.contact_name,
                    phone=record.phone,
                    email=record.email,
                    status=record.status,
                    pipeline_stage=record.pipeline_stage,
                    first_response_due_at=record.first_response_due_at,
                    first_responded_at=record.first_responded_at,
                    lost_reason=record.lost_reason,
                )
                self.session.add(lead)
                self.session.flush()
                lead_created += 1
            else:
                changed = False
                for attr, value in {
                    "customer_id": customer.id if customer else None,
                    "title": record.title,
                    "contact_name": record.contact_name,
                    "phone": record.phone,
                    "email": record.email,
                    "status": record.status,
                    "pipeline_stage": record.pipeline_stage,
                    "first_response_due_at": record.first_response_due_at,
                    "first_responded_at": record.first_responded_at,
                    "lost_reason": record.lost_reason,
                }.items():
                    if getattr(lead, attr) != value:
                        setattr(lead, attr, value)
                        changed = True
                lead_updated += 1 if changed else 0
                lead_skipped += 0 if changed else 1
                self.session.flush()
            self.mapping_service.upsert(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="lead",
                external_id=record.external_id,
                canonical_entity_type="lead",
                canonical_entity_id=lead.id,
                metadata={
                    "campaign_external_id": record.campaign_external_id,
                    "customer_external_id": record.customer_external_id,
                    "source": record.source,
                    "status": record.status,
                    "pipeline_stage": record.pipeline_stage,
                    "source_status": self._lead_source_value(record, "source_status"),
                    "conversation_external_id": self._lead_source_value(record, "conversation_external_id"),
                    "last_message_at": self._lead_source_value(record, "last_message_at"),
                },
            )

            created_event = self._ensure_lead_event(
                integration=integration,
                lead=lead,
                external_id=f"lead_ingested:{record.external_id}",
                event_type="lead.ingested_from_provider",
                event_at=record.created_at,
                payload_json={
                    "provider_name": integration.provider_name,
                    "source": record.source,
                    "status": record.status,
                    "pipeline_stage": record.pipeline_stage,
                    "campaign_external_id": record.campaign_external_id,
                    "customer_external_id": record.customer_external_id,
                    "first_response_due_at": self._isoformat(record.first_response_due_at),
                    "first_responded_at": self._isoformat(record.first_responded_at),
                    "lost_reason": record.lost_reason,
                    **self._lead_source_event_payload(record),
                    **record.metadata,
                },
            )
            if created_event == "created":
                event_created += 1
            elif created_event == "updated":
                event_updated += 1
            else:
                event_skipped += 1

            if previous_status and previous_status != record.status:
                status_event = self._ensure_lead_event(
                    integration=integration,
                    lead=lead,
                    external_id=f"lead_status:{record.external_id}:{record.status}",
                    event_type="lead.status_synced_from_provider",
                    event_at=datetime.now(timezone.utc),
                    payload_json={"previous_status": previous_status, "current_status": record.status},
                )
                if status_event == "created":
                    event_created += 1
                elif status_event == "updated":
                    event_updated += 1
                else:
                    event_skipped += 1

            conversation_event = self._ensure_conversation_event(integration=integration, lead=lead, record=record)
            if conversation_event == "created":
                event_created += 1
            elif conversation_event == "updated":
                event_updated += 1
            elif conversation_event == "skipped":
                event_skipped += 1

            response_event = self._ensure_first_response_event(integration=integration, lead=lead, record=record)
            if response_event == "created":
                event_created += 1
            elif response_event == "updated":
                event_updated += 1
            elif response_event == "skipped":
                event_skipped += 1

            terminal_event = self._ensure_terminal_status_event(integration=integration, lead=lead, record=record)
            if terminal_event == "created":
                event_created += 1
            elif terminal_event == "updated":
                event_updated += 1
            elif terminal_event == "skipped":
                event_skipped += 1

        self.session.flush()
        return (
            SyncStats(created=customer_created, updated=customer_updated, skipped=customer_skipped),
            SyncStats(created=lead_created, updated=lead_updated, skipped=lead_skipped),
            SyncStats(created=event_created, updated=event_updated, skipped=event_skipped),
        )

    def _resolve_campaign(self, integration: Integration, external_campaign_id: str) -> Campaign | None:
        mapping = self.mapping_service.resolve(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="campaign",
            external_id=external_campaign_id,
        )
        if mapping is not None:
            campaign = self.session.get(Campaign, int(mapping.canonical_entity_id))
            if campaign is not None:
                return campaign
        return self.session.execute(
            select(Campaign).where(
                Campaign.account_id == integration.account_id,
                Campaign.source == integration.provider_name,
                Campaign.external_id == external_campaign_id,
            )
        ).scalar_one_or_none()

    def _resolve_customer_for_lead(self, integration: Integration, record: AdsLeadRecord) -> tuple[Customer | None, str]:
        external_customer_id = (
            record.customer_external_id
            or record.phone
            or record.email
            or f"lead:{record.external_id}:customer"
        )
        notes_payload = {
            "source": record.source,
            "campaign_external_id": record.campaign_external_id,
            "customer_external_id": record.customer_external_id,
            "source_status": self._lead_source_value(record, "source_status"),
            "conversation_external_id": self._lead_source_value(record, "conversation_external_id"),
            "conversation_created_at": self._lead_source_value(record, "conversation_created_at"),
            "last_message_at": self._lead_source_value(record, "last_message_at"),
            **record.metadata,
        }
        mapping = self.mapping_service.resolve(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="customer",
            external_id=external_customer_id,
        )
        customer = self.session.get(Customer, int(mapping.canonical_entity_id)) if mapping is not None else None
        if customer is None:
            customer = self.session.execute(
                select(Customer).where(
                    Customer.account_id == integration.account_id,
                    Customer.external_id == external_customer_id,
                )
            ).scalar_one_or_none()
        customer_name = record.contact_name or record.title
        if customer is None:
            customer = Customer(
                account_id=integration.account_id,
                external_id=external_customer_id,
                name=customer_name,
                customer_type="individual",
                status="active",
                phone=record.phone,
                email=record.email,
                notes_json=notes_payload,
            )
            self.session.add(customer)
            self.session.flush()
            status = "created"
        else:
            changed = False
            for attr, value in {
                "name": customer_name,
                "phone": record.phone,
                "email": record.email,
                "status": "active",
            }.items():
                if value and getattr(customer, attr) != value:
                    setattr(customer, attr, value)
                    changed = True
            if customer.notes_json != notes_payload:
                customer.notes_json = notes_payload
                changed = True
            self.session.flush()
            status = "updated" if changed else "skipped"
        self.mapping_service.upsert(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="customer",
            external_id=external_customer_id,
            canonical_entity_type="customer",
            canonical_entity_id=customer.id,
            metadata={
                "name": customer.name,
                "source": record.source,
                "phone": record.phone,
                "email": record.email,
                "customer_external_id": record.customer_external_id,
                "source_status": self._lead_source_value(record, "source_status"),
                "conversation_external_id": self._lead_source_value(record, "conversation_external_id"),
            },
        )
        return customer, status

    def _ensure_lead_event(
        self,
        *,
        integration: Integration,
        lead: Lead,
        external_id: str,
        event_type: str,
        event_at: datetime,
        payload_json: dict[str, object],
    ) -> str:
        mapping = self.mapping_service.resolve(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="lead_event",
            external_id=external_id,
        )
        event = self.session.get(LeadEvent, int(mapping.canonical_entity_id)) if mapping is not None else None
        if event is None:
            event = self.session.execute(
                select(LeadEvent).where(
                    LeadEvent.account_id == integration.account_id,
                    LeadEvent.lead_id == lead.id,
                    LeadEvent.event_type == event_type,
                    LeadEvent.event_at == event_at,
                )
            ).scalars().first()
        if event is None:
            event = LeadEvent(
                account_id=integration.account_id,
                lead_id=lead.id,
                actor_user_id=None,
                event_type=event_type,
                event_at=event_at,
                payload_json=payload_json,
            )
            self.session.add(event)
            self.session.flush()
            status = "created"
        else:
            changed = False
            if event.event_type != event_type:
                event.event_type = event_type
                changed = True
            if event.event_at != event_at:
                event.event_at = event_at
                changed = True
            if event.payload_json != payload_json:
                event.payload_json = payload_json
                changed = True
            self.session.flush()
            status = "updated" if changed else "skipped"
        self.mapping_service.upsert(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="lead_event",
            external_id=external_id,
            canonical_entity_type="lead_event",
            canonical_entity_id=event.id,
            metadata={
                "event_type": event_type,
                "lead_id": lead.id,
                "provider_name": integration.provider_name,
            },
        )
        return status

    def _ensure_conversation_event(self, *, integration: Integration, lead: Lead, record: AdsLeadRecord) -> str | None:
        source_info = self._lead_source_info(record)
        conversation_id = self._lead_source_value(record, "conversation_external_id")
        conversation_created_at = self._lead_source_dt(record, "conversation_created_at")
        last_message_at = self._lead_source_dt(record, "last_message_at")
        source_status = self._lead_source_value(record, "source_status")
        if not any((conversation_id, conversation_created_at, last_message_at, source_status)):
            return None
        event_at = last_message_at or conversation_created_at or record.created_at
        identity = conversation_id or self._isoformat(last_message_at) or self._isoformat(conversation_created_at) or "state"
        return self._ensure_lead_event(
            integration=integration,
            lead=lead,
            external_id=f"lead_conversation:{record.external_id}:{identity}",
            event_type="lead.conversation_synced_from_provider",
            event_at=event_at,
            payload_json={
                "provider_name": integration.provider_name,
                "source": record.source,
                "source_status": source_status,
                "conversation_external_id": conversation_id,
                "conversation_created_at": self._isoformat(conversation_created_at),
                "last_message_at": self._isoformat(last_message_at),
                "source_info": source_info,
            },
        )

    def _ensure_first_response_event(self, *, integration: Integration, lead: Lead, record: AdsLeadRecord) -> str | None:
        if record.first_responded_at is None:
            return None
        return self._ensure_lead_event(
            integration=integration,
            lead=lead,
            external_id=f"lead_first_response:{record.external_id}:{record.first_responded_at.isoformat()}",
            event_type="lead.first_response_synced_from_provider",
            event_at=record.first_responded_at,
            payload_json={
                "provider_name": integration.provider_name,
                "first_response_due_at": self._isoformat(record.first_response_due_at),
                "first_responded_at": self._isoformat(record.first_responded_at),
                "response_latency_seconds": self._response_latency_seconds(record),
            },
        )

    def _ensure_terminal_status_event(self, *, integration: Integration, lead: Lead, record: AdsLeadRecord) -> str | None:
        source_status = (self._lead_source_value(record, "source_status") or "").lower()
        terminal_status = record.status.lower()
        if terminal_status != "lost" and source_status not in {"lost", "closed", "archived"}:
            return None
        event_at = self._lead_source_dt(record, "closed_at") or self._lead_source_dt(record, "last_message_at") or datetime.now(timezone.utc)
        event_type = "lead.lost_synced_from_provider" if terminal_status == "lost" or record.lost_reason else "lead.closed_synced_from_provider"
        terminal_key = record.lost_reason or source_status or terminal_status
        return self._ensure_lead_event(
            integration=integration,
            lead=lead,
            external_id=f"lead_terminal:{record.external_id}:{terminal_key}",
            event_type=event_type,
            event_at=event_at,
            payload_json={
                "provider_name": integration.provider_name,
                "status": record.status,
                "source_status": self._lead_source_value(record, "source_status"),
                "pipeline_stage": record.pipeline_stage,
                "lost_reason": record.lost_reason,
                "closed_at": self._isoformat(self._lead_source_dt(record, "closed_at")),
                "last_message_at": self._isoformat(self._lead_source_dt(record, "last_message_at")),
            },
        )

    def _lead_source_info(self, record: AdsLeadRecord) -> dict[str, object]:
        source_info = record.metadata.get("source_info")
        return dict(source_info) if isinstance(source_info, dict) else {}

    def _lead_source_value(self, record: AdsLeadRecord, key: str) -> str | None:
        value = self._lead_source_info(record).get(key)
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    def _lead_source_dt(self, record: AdsLeadRecord, key: str) -> datetime | None:
        value = self._lead_source_info(record).get(key)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if not value:
            return None
        normalized = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _isoformat(self, value: datetime | None) -> str | None:
        return value.astimezone(timezone.utc).isoformat() if value is not None else None

    def _response_latency_seconds(self, record: AdsLeadRecord) -> int | None:
        if record.first_response_due_at is None or record.first_responded_at is None:
            return None
        delta = record.first_responded_at - record.first_response_due_at
        return int(delta.total_seconds())

    def _lead_source_event_payload(self, record: AdsLeadRecord) -> dict[str, object]:
        source_info = self._lead_source_info(record)
        if not source_info:
            return {}
        return {
            "source_status": source_info.get("source_status"),
            "conversation_external_id": source_info.get("conversation_external_id"),
            "conversation_created_at": self._isoformat(self._lead_source_dt(record, "conversation_created_at")),
            "last_message_at": self._isoformat(self._lead_source_dt(record, "last_message_at")),
            "closed_at": self._isoformat(self._lead_source_dt(record, "closed_at")),
        }


class ERPSyncService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.mapping_service = IntegrationMappingService(session)

    def sync_products(self, integration: Integration, records: list[ERPProductRecord]) -> SyncStats:
        created = updated = skipped = 0
        for record in records:
            category_id = self._resolve_category(integration, record)
            mapping = self.mapping_service.resolve(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="product",
                external_id=record.external_id,
            )
            product = None
            if mapping is not None:
                product = self.session.get(Product, int(mapping.canonical_entity_id))
            if product is None and record.sku:
                product = self.session.execute(
                    select(Product).where(Product.account_id == integration.account_id, Product.sku == record.sku)
                ).scalar_one_or_none()
            if product is None:
                product = Product(
                    account_id=integration.account_id,
                    category_id=category_id,
                    sku=record.sku,
                    name=record.name,
                    unit=record.unit,
                    status=record.status,
                    list_price=record.list_price or Decimal("0"),
                    cost_price=record.cost_price or Decimal("0"),
                    attributes_json=record.metadata,
                )
                self.session.add(product)
                self.session.flush()
                created += 1
            else:
                changed = False
                for attr, value in {
                    "category_id": category_id,
                    "sku": record.sku,
                    "name": record.name,
                    "unit": record.unit,
                    "status": record.status,
                    "list_price": record.list_price or Decimal("0"),
                    "cost_price": record.cost_price or Decimal("0"),
                    "attributes_json": record.metadata,
                }.items():
                    if getattr(product, attr) != value:
                        setattr(product, attr, value)
                        changed = True
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
            self.mapping_service.upsert(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="product",
                external_id=record.external_id,
                canonical_entity_type="product",
                canonical_entity_id=product.id,
                metadata={"sku": record.sku, "name": record.name},
            )
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped)

    def sync_stock(self, integration: Integration, records: list[ERPStockRecord]) -> SyncStats:
        created = updated = skipped = 0
        for record in records:
            product = self._resolve_entity(integration, "product", record.external_product_id, Product)
            warehouse = self._resolve_warehouse(integration, record.external_warehouse_id, record.metadata)
            if product is None or warehouse is None:
                skipped += 1
                continue
            stock_item = self.session.execute(
                select(StockItem).where(
                    StockItem.account_id == integration.account_id,
                    StockItem.product_id == product.id,
                    StockItem.warehouse_id == warehouse.id,
                )
            ).scalar_one_or_none()
            if stock_item is None:
                stock_item = StockItem(
                    account_id=integration.account_id,
                    warehouse_id=warehouse.id,
                    product_id=product.id,
                    quantity_on_hand=record.quantity_on_hand,
                    quantity_reserved=record.quantity_reserved,
                    last_movement_at=datetime.now(timezone.utc),
                )
                self.session.add(stock_item)
                created += 1
            else:
                changed = False
                if stock_item.quantity_on_hand != record.quantity_on_hand:
                    stock_item.quantity_on_hand = record.quantity_on_hand
                    changed = True
                if stock_item.quantity_reserved != record.quantity_reserved:
                    stock_item.quantity_reserved = record.quantity_reserved
                    changed = True
                stock_item.last_movement_at = datetime.now(timezone.utc)
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped)

    def sync_purchases(self, integration: Integration, records: list[ERPPurchaseRecord]) -> SyncStats:
        created = updated = skipped = 0
        for record in records:
            warehouse = (
                self._resolve_warehouse(integration, record.warehouse_external_id, {"warehouse_name": record.metadata.get("warehouse_name")})
                if record.warehouse_external_id
                else None
            )
            supplier = (
                self._resolve_customer(integration, record.supplier_external_id, str(record.metadata.get("supplier_name") or "Supplier"))
                if record.supplier_external_id
                else None
            )
            mapping = self.mapping_service.resolve(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="purchase",
                external_id=record.external_id,
            )
            purchase = self.session.get(Purchase, int(mapping.canonical_entity_id)) if mapping is not None else None
            if purchase is None and record.purchase_number:
                purchase = self.session.execute(
                    select(Purchase).where(
                        Purchase.account_id == integration.account_id,
                        Purchase.purchase_number == record.purchase_number,
                    )
                ).scalar_one_or_none()
            if purchase is None:
                purchase = Purchase(
                    account_id=integration.account_id,
                    supplier_customer_id=supplier.id if supplier else None,
                    warehouse_id=warehouse.id if warehouse else None,
                    purchase_number=record.purchase_number,
                    status=record.status,
                    ordered_at=record.ordered_at,
                    received_at=record.received_at,
                    currency=record.currency,
                    total_amount=record.total_amount,
                    notes_json=record.metadata,
                )
                self.session.add(purchase)
                self.session.flush()
                created += 1
            else:
                changed = False
                for attr, value in {
                    "supplier_customer_id": supplier.id if supplier else None,
                    "warehouse_id": warehouse.id if warehouse else None,
                    "purchase_number": record.purchase_number,
                    "status": record.status,
                    "ordered_at": record.ordered_at,
                    "received_at": record.received_at,
                    "currency": record.currency,
                    "total_amount": record.total_amount,
                    "notes_json": record.metadata,
                }.items():
                    if getattr(purchase, attr) != value:
                        setattr(purchase, attr, value)
                        changed = True
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
            self.mapping_service.upsert(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="purchase",
                external_id=record.external_id,
                canonical_entity_type="purchase",
                canonical_entity_id=purchase.id,
                metadata={"purchase_number": record.purchase_number},
            )
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped)

    def sync_movements(self, integration: Integration, records: list[ERPStockMovementRecord]) -> SyncStats:
        created = updated = skipped = 0
        for record in records:
            product = self._resolve_entity(integration, "product", record.external_product_id, Product)
            warehouse = self._resolve_warehouse(integration, record.external_warehouse_id, record.metadata)
            if product is None or warehouse is None:
                skipped += 1
                continue
            external_ref = record.external_reference_id or self._synthetic_reference(record)
            mapping = self.mapping_service.resolve(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="stock_movement",
                external_id=external_ref,
            )
            movement = self.session.get(StockMovement, int(mapping.canonical_entity_id)) if mapping is not None else None
            if movement is None:
                movement = StockMovement(
                    account_id=integration.account_id,
                    warehouse_id=warehouse.id,
                    product_id=product.id,
                    movement_type=record.movement_type,
                    reference_type="provider_sync",
                    reference_id=external_ref,
                    quantity_delta=record.quantity_delta,
                    unit_cost=record.unit_cost or Decimal("0"),
                    occurred_at=record.occurred_at,
                    notes_json=record.metadata,
                )
                self.session.add(movement)
                self.session.flush()
                created += 1
            else:
                changed = False
                for attr, value in {
                    "movement_type": record.movement_type,
                    "quantity_delta": record.quantity_delta,
                    "unit_cost": record.unit_cost or Decimal("0"),
                    "occurred_at": record.occurred_at,
                    "notes_json": record.metadata,
                }.items():
                    if getattr(movement, attr) != value:
                        setattr(movement, attr, value)
                        changed = True
                updated += 1 if changed else 0
                skipped += 0 if changed else 1
            self.mapping_service.upsert(
                account_id=integration.account_id,
                integration_id=integration.id,
                provider_entity_type="stock_movement",
                external_id=external_ref,
                canonical_entity_type="stock_movement",
                canonical_entity_id=movement.id,
                metadata={"movement_type": record.movement_type},
            )
        self.session.flush()
        return SyncStats(created=created, updated=updated, skipped=skipped)

    def _resolve_category(self, integration: Integration, record: ERPProductRecord) -> int | None:
        if not record.category_code:
            return None
        mapping = self.mapping_service.resolve(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="product_category",
            external_id=record.category_code,
        )
        category = self.session.get(ProductCategory, int(mapping.canonical_entity_id)) if mapping is not None else None
        if category is None:
            code = self._slug(record.category_code)
            category = self.session.execute(
                select(ProductCategory).where(
                    ProductCategory.account_id == integration.account_id,
                    ProductCategory.code == code,
                )
            ).scalar_one_or_none()
        if category is None:
            category = ProductCategory(
                account_id=integration.account_id,
                code=self._slug(record.category_code),
                name=str(record.metadata.get("category_name") or record.category_code),
                status="active",
            )
            self.session.add(category)
            self.session.flush()
        self.mapping_service.upsert(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="product_category",
            external_id=record.category_code,
            canonical_entity_type="product_category",
            canonical_entity_id=category.id,
            metadata={"name": category.name},
        )
        return category.id

    def _resolve_warehouse(
        self,
        integration: Integration,
        external_warehouse_id: str | None,
        metadata: dict[str, object] | None,
    ) -> Warehouse | None:
        if not external_warehouse_id:
            return None
        mapping = self.mapping_service.resolve(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="warehouse",
            external_id=external_warehouse_id,
        )
        warehouse = self.session.get(Warehouse, int(mapping.canonical_entity_id)) if mapping is not None else None
        if warehouse is None:
            code = self._slug(external_warehouse_id)
            warehouse = self.session.execute(
                select(Warehouse).where(Warehouse.account_id == integration.account_id, Warehouse.code == code)
            ).scalar_one_or_none()
        if warehouse is None:
            warehouse = Warehouse(
                account_id=integration.account_id,
                code=self._slug(external_warehouse_id),
                name=str((metadata or {}).get("warehouse_name") or f"Warehouse {external_warehouse_id[:8]}"),
                status="active",
            )
            self.session.add(warehouse)
            self.session.flush()
        self.mapping_service.upsert(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="warehouse",
            external_id=external_warehouse_id,
            canonical_entity_type="warehouse",
            canonical_entity_id=warehouse.id,
            metadata={"name": warehouse.name},
        )
        return warehouse

    def _resolve_customer(self, integration: Integration, external_customer_id: str, customer_name: str) -> Customer:
        mapping = self.mapping_service.resolve(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="customer",
            external_id=external_customer_id,
        )
        customer = self.session.get(Customer, int(mapping.canonical_entity_id)) if mapping is not None else None
        if customer is None:
            customer = Customer(account_id=integration.account_id, name=customer_name)
            self.session.add(customer)
            self.session.flush()
        elif customer.name != customer_name and customer_name:
            customer.name = customer_name
            self.session.flush()
        self.mapping_service.upsert(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type="customer",
            external_id=external_customer_id,
            canonical_entity_type="customer",
            canonical_entity_id=customer.id,
            metadata={"name": customer_name},
        )
        return customer

    def _resolve_entity(self, integration: Integration, provider_entity_type: str, external_id: str, model):
        mapping = self.mapping_service.resolve(
            account_id=integration.account_id,
            integration_id=integration.id,
            provider_entity_type=provider_entity_type,
            external_id=external_id,
        )
        if mapping is None:
            return None
        return self.session.get(model, int(mapping.canonical_entity_id))

    def _synthetic_reference(self, record: ERPStockMovementRecord) -> str:
        return f"{record.external_product_id}:{record.external_warehouse_id}:{record.movement_type}:{record.occurred_at.isoformat()}:{record.quantity_delta}"

    def _slug(self, value: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
        return (cleaned or "item")[:64]
