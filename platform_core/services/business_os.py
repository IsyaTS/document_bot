from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from email.message import EmailMessage
from pathlib import Path
import re
import smtplib

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.models import (
    Alert,
    CommunicationReview,
    Customer,
    Document,
    InstallationRequest,
    NotificationEvent,
    NotificationDispatch,
    Product,
    Purchase,
    StockItem,
    StockMovement,
    Task,
    Warehouse,
)
from platform_core.runtime_obsidian import export_notification_dispatch_note
from platform_core.providers.contracts import MessageSendRequest
from platform_core.settings import load_platform_settings
from platform_core.services.runtime import (
    AdminQueryService,
    ResolvedRuntimeContext,
    RuntimeAutomationService,
    RuntimeIntegrationService,
    build_provider_registry,
)


@dataclass(frozen=True)
class InventoryInsight:
    stock_item: StockItem
    product: Product
    warehouse: Warehouse
    days_since_movement: int | None
    reorder_needed: bool
    stagnant: bool


class BusinessOSService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_customers(self, runtime: ResolvedRuntimeContext) -> list[Customer]:
        return self.session.execute(
            select(Customer).where(Customer.account_id == runtime.account.id).order_by(Customer.updated_at.desc(), Customer.id.desc())
        ).scalars().all()

    def customer_snapshot(self, runtime: ResolvedRuntimeContext, customer_id: int) -> dict[str, object]:
        customer = self.session.execute(
            select(Customer).where(Customer.account_id == runtime.account.id, Customer.id == customer_id)
        ).scalar_one()
        documents = self.session.execute(
            select(Document).where(Document.account_id == runtime.account.id, Document.customer_id == customer.id).order_by(Document.created_at.desc())
        ).scalars().all()
        purchases = self.session.execute(
            select(Purchase).where(Purchase.account_id == runtime.account.id, Purchase.supplier_customer_id == customer.id).order_by(Purchase.created_at.desc())
        ).scalars().all()
        reviews = self.session.execute(
            select(CommunicationReview).where(CommunicationReview.account_id == runtime.account.id, CommunicationReview.customer_id == customer.id).order_by(CommunicationReview.created_at.desc())
        ).scalars().all()
        tasks = self.session.execute(
            select(Task).where(
                Task.account_id == runtime.account.id,
                Task.related_entity_type.in_(["customer", "communication_review"]),
            ).order_by(Task.created_at.desc())
        ).scalars().all()
        alerts = self.session.execute(
            select(Alert).where(Alert.account_id == runtime.account.id).order_by(Alert.last_detected_at.desc())
        ).scalars().all()
        return {
            "customer": customer,
            "documents": documents,
            "purchases": purchases,
            "reviews": reviews,
            "tasks": tasks[:12],
            "alerts": alerts[:12],
        }

    def inventory_insights(self, runtime: ResolvedRuntimeContext, *, stagnant_days: int = 30) -> list[InventoryInsight]:
        items = self.session.execute(
            select(StockItem).where(StockItem.account_id == runtime.account.id).order_by(StockItem.id.asc())
        ).scalars().all()
        if not items:
            return []
        product_map = {
            item.id: item
            for item in self.session.execute(
                select(Product).where(Product.account_id == runtime.account.id, Product.id.in_({item.product_id for item in items}))
            ).scalars().all()
        }
        warehouse_map = {
            item.id: item
            for item in self.session.execute(
                select(Warehouse).where(Warehouse.account_id == runtime.account.id, Warehouse.id.in_({item.warehouse_id for item in items}))
            ).scalars().all()
        }
        now = datetime.now(timezone.utc)
        rows: list[InventoryInsight] = []
        for item in items:
            product = product_map[item.product_id]
            warehouse = warehouse_map[item.warehouse_id]
            if item.last_movement_at is None:
                days_since = None
            else:
                movement_at = item.last_movement_at
                if movement_at.tzinfo is None:
                    movement_at = movement_at.replace(tzinfo=timezone.utc)
                days_since = max(0, (now - movement_at).days)
            min_level = max(Decimal(item.min_quantity), Decimal(product.min_stock_level))
            reorder_needed = Decimal(item.quantity_on_hand) <= min_level
            stagnant = Decimal(item.quantity_on_hand) > 0 and (days_since is None or days_since >= stagnant_days)
            rows.append(
                InventoryInsight(
                    stock_item=item,
                    product=product,
                    warehouse=warehouse,
                    days_since_movement=days_since,
                    reorder_needed=reorder_needed,
                    stagnant=stagnant,
                )
            )
        rows.sort(key=lambda item: (not item.reorder_needed, not item.stagnant, item.product.name.lower()))
        return rows

    def update_purchase_status(self, runtime: ResolvedRuntimeContext, purchase_id: int, status_code: str) -> Purchase:
        purchase = self.session.execute(
            select(Purchase).where(Purchase.account_id == runtime.account.id, Purchase.id == purchase_id)
        ).scalar_one()
        if status_code not in {"draft", "requested", "approved", "ordered", "received", "cancelled"}:
            raise ValueError("Unsupported purchase status.")
        purchase.status = status_code
        self.session.flush()
        return purchase

    def update_installation_status(self, runtime: ResolvedRuntimeContext, installation_id: int, status_code: str) -> InstallationRequest:
        request_item = self.session.execute(
            select(InstallationRequest).where(InstallationRequest.account_id == runtime.account.id, InstallationRequest.id == installation_id)
        ).scalar_one()
        if status_code not in {"open", "scheduled", "en_route", "on_site", "done", "cancelled"}:
            raise ValueError("Unsupported installation status.")
        request_item.status = status_code
        self.session.flush()
        return request_item

    def update_document_status(self, runtime: ResolvedRuntimeContext, document_id: int, status_code: str) -> Document:
        document = self.session.execute(
            select(Document).where(Document.account_id == runtime.account.id, Document.id == document_id)
        ).scalar_one()
        if status_code not in {"draft", "issued", "sent", "accepted", "paid", "archived"}:
            raise ValueError("Unsupported document status.")
        document.status = status_code
        self.session.flush()
        return document

    def render_document_preview(self, runtime: ResolvedRuntimeContext, document_id: int) -> str:
        document = self.session.execute(
            select(Document).where(Document.account_id == runtime.account.id, Document.id == document_id)
        ).scalar_one()
        customer = None
        if document.customer_id is not None:
            customer = self.session.execute(
                select(Customer).where(Customer.account_id == runtime.account.id, Customer.id == document.customer_id)
            ).scalar_one_or_none()
        summary = str((document.snapshot_json or {}).get("summary") or "").strip()
        header = {
            "invoice": "INVOICE",
            "claim": "CLAIM",
            "purchase_order": "PURCHASE ORDER",
        }.get(document.document_type, document.document_type.upper())
        lines = [
            header,
            f"Account: {runtime.account.name}",
            f"Document: {document.document_number or document.id}",
            f"Customer: {customer.name if customer is not None else 'not linked'}",
            f"Status: {document.status}",
            f"Amount: {document.total_amount} {document.currency}",
            "",
            summary or "No summary provided.",
        ]
        if document.document_type == "claim":
            lines.extend([
                "",
                "Requested action:",
                "- review the complaint",
                "- confirm the owner and response date",
                "- close the loop with the customer",
            ])
        elif document.document_type == "invoice":
            lines.extend([
                "",
                "Payment instructions:",
                "- confirm goods or service scope",
                "- send invoice to customer",
                "- track payment status until paid",
            ])
        elif document.document_type == "purchase_order":
            lines.extend([
                "",
                "Procurement instructions:",
                "- confirm supplier and stock line",
                "- move request to approved / ordered",
                "- receive stock and close the order",
            ])
        return "\n".join(lines)

    def list_notifications(self, runtime: ResolvedRuntimeContext) -> list[NotificationEvent]:
        return self.session.execute(
            select(NotificationEvent).where(NotificationEvent.account_id == runtime.account.id).order_by(NotificationEvent.created_at.desc())
        ).scalars().all()

    def list_dispatches(self, runtime: ResolvedRuntimeContext) -> list[NotificationDispatch]:
        return self.session.execute(
            select(NotificationDispatch)
            .where(NotificationDispatch.account_id == runtime.account.id)
            .order_by(NotificationDispatch.created_at.desc(), NotificationDispatch.id.desc())
        ).scalars().all()

    def generate_notification(self, runtime: ResolvedRuntimeContext, *, channel: str, event_type: str, title: str, body_text: str, created_by_user_id: int | None) -> NotificationEvent:
        event = NotificationEvent(
            account_id=runtime.account.id,
            created_by_user_id=created_by_user_id,
            channel=channel,
            event_type=event_type,
            title=title,
            body_text=body_text,
            status="generated",
            payload_json={"channel": channel, "event_type": event_type},
        )
        self.session.add(event)
        self.session.flush()
        return event

    def generate_default_digests(self, runtime: ResolvedRuntimeContext, *, created_by_user_id: int | None) -> list[NotificationEvent]:
        automation = RuntimeAutomationService(self.session)
        alerts = [item for item in automation.list_alerts(runtime.context) if item.status == "open" and item.severity == "critical"]
        tasks = [item for item in automation.list_tasks(runtime.context) if item.status == "open" and item.due_at is not None]
        ops = AdminQueryService(self.session).ops_summary(runtime.account.id)
        events = [
            self.generate_notification(
                runtime,
                channel="internal",
                event_type="daily_brief",
                title=f"{runtime.account.name}: daily brief",
                body_text=f"Critical alerts: {len(alerts)}. Overdue tasks: {len(tasks)}. Failed sync jobs: {len(ops['recent_failed_sync_jobs'])}.",
                created_by_user_id=created_by_user_id,
            ),
            self.generate_notification(
                runtime,
                channel="telegram",
                event_type="critical_alerts_digest",
                title=f"{runtime.account.name}: critical alerts digest",
                body_text="\n".join(f"- {item.title}" for item in alerts[:10]) or "No critical alerts.",
                created_by_user_id=created_by_user_id,
            ),
            self.generate_notification(
                runtime,
                channel="email",
                event_type="failed_sync_digest",
                title=f"{runtime.account.name}: failed sync digest",
                body_text="\n".join(f"- job #{item.id}: {item.error_message or item.provider_name}" for item in ops["recent_failed_sync_jobs"][:10]) or "No failed sync jobs.",
                created_by_user_id=created_by_user_id,
            ),
        ]
        return events

    def dispatch_notification(
        self,
        runtime: ResolvedRuntimeContext,
        *,
        notification_event_id: int,
        dispatched_by_user_id: int | None,
        channel: str | None = None,
        target_ref: str | None = None,
    ) -> NotificationDispatch:
        event = self.session.execute(
            select(NotificationEvent).where(
                NotificationEvent.account_id == runtime.account.id,
                NotificationEvent.id == notification_event_id,
            )
        ).scalar_one()
        dispatch_channel = (channel or event.channel or "internal").strip() or "internal"
        generated_at = datetime.now(timezone.utc)
        delivery_path = self._write_notification_artifact(
            runtime.account.slug,
            channel=dispatch_channel,
            event=event,
            generated_at=generated_at,
        )
        relative_path = delivery_path.relative_to(self._project_root())
        resolved_target = self._resolve_dispatch_target(runtime, dispatch_channel, target_ref)
        payload = {
            "event_type": event.event_type,
            "original_channel": event.channel,
            "target_ref": resolved_target,
        }
        dispatch_status = "delivered"
        try:
            payload.update(
                self._deliver_notification(
                    runtime,
                    event=event,
                    channel=dispatch_channel,
                    target_ref=resolved_target,
                    artifact_path=delivery_path,
                    generated_at=generated_at,
                )
            )
        except Exception as exc:
            dispatch_status = "failed"
            payload["error"] = str(exc)
        dispatch = NotificationDispatch(
            account_id=runtime.account.id,
            notification_event_id=event.id,
            dispatched_by_user_id=dispatched_by_user_id,
            channel=dispatch_channel,
            target_ref=resolved_target,
            status=dispatch_status,
            dispatched_at=generated_at,
            delivery_path=str(relative_path),
            payload_json=payload,
        )
        self.session.add(dispatch)
        event.status = "delivered" if dispatch_status == "delivered" else "failed"
        event.payload_json = {
            **(event.payload_json or {}),
            "last_dispatch_path": str(relative_path),
            "last_dispatch_channel": dispatch_channel,
            "last_dispatched_at": generated_at.isoformat(),
            "last_dispatch_target": resolved_target,
            "last_dispatch_status": dispatch_status,
            **({"last_dispatch_error": payload.get("error")} if payload.get("error") else {}),
        }
        self.session.flush()
        return dispatch

    def dispatch_default_digests(
        self,
        runtime: ResolvedRuntimeContext,
        *,
        created_by_user_id: int | None,
        channels: list[str] | None = None,
    ) -> list[NotificationDispatch]:
        events = self.generate_default_digests(runtime, created_by_user_id=created_by_user_id)
        normalized_channels = [item.strip() for item in (channels or []) if item and item.strip()]
        if not normalized_channels:
            normalized_channels = []
        dispatches: list[NotificationDispatch] = []
        for event in events:
            dispatch_channel = normalized_channels[0] if len(normalized_channels) == 1 else None
            dispatches.append(
                self.dispatch_notification(
                    runtime,
                    notification_event_id=event.id,
                    dispatched_by_user_id=created_by_user_id,
                    channel=dispatch_channel,
                )
            )
        return dispatches

    def advisor_items(self, runtime: ResolvedRuntimeContext) -> list[dict[str, object]]:
        automation = RuntimeAutomationService(self.session)
        alerts = [item for item in automation.list_alerts(runtime.context) if item.status == "open"]
        tasks = [item for item in automation.list_tasks(runtime.context) if item.status == "open"]
        inventory_rows = self.inventory_insights(runtime, stagnant_days=30)
        reviews = self.session.execute(
            select(CommunicationReview).where(CommunicationReview.account_id == runtime.account.id).order_by(CommunicationReview.created_at.desc())
        ).scalars().all()
        items: list[dict[str, object]] = []
        if any(item.severity == "critical" for item in alerts):
            items.append({
                "title": "Critical alerts need owner review",
                "reason": f"{sum(1 for item in alerts if item.severity == 'critical')} critical alerts are still open.",
                "action": f"/admin/{runtime.account.slug}/alerts-tasks?severity=critical",
            })
        at_risk_reviews = [item for item in reviews if item.quality_status == "critical"]
        if at_risk_reviews:
            items.append({
                "title": "Communication quality is hurting conversion",
                "reason": f"{len(at_risk_reviews)} critical transcript reviews need follow-up.",
                "action": f"/admin/{runtime.account.slug}/communications",
            })
        reorder_rows = [item for item in inventory_rows if item.reorder_needed]
        if reorder_rows:
            items.append({
                "title": "Reorder stock before service quality drops",
                "reason": f"{len(reorder_rows)} stock rows are at or below minimum level.",
                "action": f"/admin/{runtime.account.slug}/inventory",
            })
        stagnant_rows = [item for item in inventory_rows if item.stagnant]
        if stagnant_rows:
            items.append({
                "title": "Stagnant stock is locking cash",
                "reason": f"{len(stagnant_rows)} stock rows show weak movement.",
                "action": f"/admin/{runtime.account.slug}/inventory",
            })
        if not items:
            items.append({
                "title": "No urgent cross-domain action detected",
                "reason": "Current data does not show a major operational blocker.",
                "action": f"/admin/{runtime.account.slug}/dashboard",
            })
        return items

    def _write_notification_artifact(
        self,
        account_slug: str,
        *,
        channel: str,
        event: NotificationEvent,
        generated_at: datetime,
    ) -> Path:
        root = (self._project_root() / "data" / "runtime_notifications" / account_slug / channel).resolve()
        root.mkdir(parents=True, exist_ok=True)
        timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
        slug = re.sub(r"[^a-z0-9]+", "-", event.title.lower()).strip("-") or f"event-{event.id}"
        path = root / f"{timestamp}-{slug}.txt"
        lines = [
            f"title: {event.title}",
            f"channel: {channel}",
            f"event_type: {event.event_type}",
            f"generated_at: {generated_at.isoformat()}",
            "",
            event.body_text,
            "",
            "payload:",
            str(event.payload_json or {}),
        ]
        path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        latest_path = root / "latest.txt"
        latest_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        return path

    def _resolve_dispatch_target(
        self,
        runtime: ResolvedRuntimeContext,
        channel: str,
        target_ref: str | None,
    ) -> str | None:
        explicit = (target_ref or "").strip() or None
        if explicit is not None:
            return explicit
        settings = runtime.account.settings_json if isinstance(runtime.account.settings_json, dict) else {}
        if channel == "telegram":
            integration_ids = settings.get("notification_telegram_integration_ids") or []
            if isinstance(integration_ids, str):
                integration_ids = [item.strip() for item in integration_ids.split(",") if item.strip()]
            normalized_ids = [str(item).strip() for item in integration_ids if str(item).strip()]
            if normalized_ids:
                return ",".join(normalized_ids)
            service = RuntimeIntegrationService(self.session)
            auto_targets: list[str] = []
            for integration in service.list_integrations(runtime.context):
                if integration.provider_kind != "messaging" or integration.provider_name != "telegram":
                    continue
                if integration.status == "archived":
                    continue
                credentials = service._load_credentials(integration)
                if str(credentials.get("session_string") or "").strip():
                    auto_targets.append(str(integration.id))
            if auto_targets:
                return ",".join(auto_targets)
            chat_ids = settings.get("notification_telegram_chat_ids") or []
            if isinstance(chat_ids, str):
                chat_ids = [item.strip() for item in chat_ids.split(",") if item.strip()]
            return ",".join(str(item).strip() for item in chat_ids if str(item).strip()) or None
        if channel == "email":
            recipients = settings.get("notification_email_recipients") or []
            if isinstance(recipients, str):
                recipients = [item.strip() for item in recipients.split(",") if item.strip()]
            return ",".join(str(item).strip() for item in recipients if str(item).strip()) or None
        if channel == "webhook":
            webhook_url = str(settings.get("notification_webhook_url") or "").strip()
            return webhook_url or None
        return None

    def _deliver_notification(
        self,
        runtime: ResolvedRuntimeContext,
        *,
        event: NotificationEvent,
        channel: str,
        target_ref: str | None,
        artifact_path: Path,
        generated_at: datetime,
    ) -> dict[str, object]:
        if channel == "internal":
            self._export_dispatch_obsidian(
                runtime,
                event=event,
                channel=channel,
                generated_at=generated_at,
                target_ref=target_ref,
                artifact_path=artifact_path,
            )
            return {"mode": "artifact_only"}
        if channel == "webhook":
            return self._deliver_webhook(runtime, event, target_ref=target_ref, artifact_path=artifact_path, generated_at=generated_at)
        if channel == "telegram":
            return self._deliver_telegram(runtime, event, target_ref=target_ref, artifact_path=artifact_path, generated_at=generated_at)
        if channel == "email":
            return self._deliver_email(runtime, event, target_ref=target_ref, artifact_path=artifact_path, generated_at=generated_at)
        raise ValueError(f"Unsupported notification channel: {channel}")

    def _deliver_webhook(
        self,
        runtime: ResolvedRuntimeContext,
        event: NotificationEvent,
        *,
        target_ref: str | None,
        artifact_path: Path,
        generated_at: datetime,
    ) -> dict[str, object]:
        if not target_ref:
            raise ValueError("Webhook target is not configured.")
        response = requests.post(
            target_ref,
            json={
                "account_slug": runtime.account.slug,
                "account_name": runtime.account.name,
                "event_type": event.event_type,
                "channel": "webhook",
                "title": event.title,
                "body_text": event.body_text,
                "generated_at": generated_at.isoformat(),
                "artifact_path": str(artifact_path),
            },
            timeout=10,
        )
        response.raise_for_status()
        self._export_dispatch_obsidian(
            runtime,
            event=event,
            channel="webhook",
            generated_at=generated_at,
            target_ref=target_ref,
            artifact_path=artifact_path,
        )
        return {"mode": "webhook", "http_status": response.status_code}

    def _deliver_telegram(
        self,
        runtime: ResolvedRuntimeContext,
        event: NotificationEvent,
        *,
        target_ref: str | None,
        artifact_path: Path,
        generated_at: datetime,
    ) -> dict[str, object]:
        text = f"{event.title}\n\n{event.body_text}".strip()
        targets = [item.strip() for item in (target_ref or "").split(",") if item.strip()]
        integration_targets = [item for item in targets if item.isdigit()]
        chat_targets = [item for item in targets if not item.isdigit()]
        delivered = 0
        delivered_integrations: list[int] = []
        delivered_chat_ids: list[str] = []

        if integration_targets:
            service = RuntimeIntegrationService(self.session)
            registry = build_provider_registry()
            adapter = registry.get("messaging", "telegram")
            if adapter is None:
                raise ValueError("Telegram messaging provider is not registered.")
            for integration_id_raw in integration_targets:
                integration = service._get_integration(runtime.account.id, int(integration_id_raw))
                if integration.provider_kind != "messaging" or integration.provider_name != "telegram":
                    raise ValueError(f"Integration {integration.id} is not a Telegram messaging integration.")
                credentials = service._load_credentials(integration)
                adapter.send(
                    credentials,
                    MessageSendRequest(
                        channel="telegram",
                        recipient_external_id="me",
                        body=text,
                        metadata={"notification_event_id": event.id},
                    ),
                )
                delivered += 1
                delivered_integrations.append(integration.id)

        if chat_targets:
            settings = load_platform_settings()
            bot_token = settings.notification_telegram_bot_token
            if not bot_token:
                raise ValueError("PLATFORM_NOTIFICATION_TELEGRAM_BOT_TOKEN is not configured.")
            for chat_id in chat_targets:
                response = requests.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": chat_id, "text": text},
                    timeout=10,
                )
                response.raise_for_status()
                delivered += 1
                delivered_chat_ids.append(chat_id)

        if delivered <= 0:
            raise ValueError("Telegram delivery target is not configured.")
        self._export_dispatch_obsidian(
            runtime,
            event=event,
            channel="telegram",
            generated_at=generated_at,
            target_ref=target_ref,
            artifact_path=artifact_path,
        )
        return {
            "mode": "telegram",
            "delivered_count": delivered,
            "integration_ids": delivered_integrations,
            "chat_ids": delivered_chat_ids,
        }

    def _deliver_email(
        self,
        runtime: ResolvedRuntimeContext,
        event: NotificationEvent,
        *,
        target_ref: str | None,
        artifact_path: Path,
        generated_at: datetime,
    ) -> dict[str, object]:
        settings = load_platform_settings()
        if not settings.smtp_host or not settings.smtp_from_email:
            raise ValueError("SMTP settings are not configured.")
        recipients = [item.strip() for item in (target_ref or "").split(",") if item.strip()]
        if not recipients:
            raise ValueError("Email recipients are not configured.")
        message = EmailMessage()
        message["From"] = settings.smtp_from_email
        message["To"] = ", ".join(recipients)
        message["Subject"] = event.title
        message.set_content(event.body_text)
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=settings.smtp_timeout_seconds) as smtp:
            if settings.smtp_use_starttls:
                smtp.starttls()
            if settings.smtp_username:
                smtp.login(settings.smtp_username, settings.smtp_password or "")
            smtp.send_message(message)
        self._export_dispatch_obsidian(
            runtime,
            event=event,
            channel="email",
            generated_at=generated_at,
            target_ref=target_ref,
            artifact_path=artifact_path,
        )
        return {"mode": "email", "delivered_count": len(recipients)}

    def _export_dispatch_obsidian(
        self,
        runtime: ResolvedRuntimeContext,
        *,
        event: NotificationEvent,
        channel: str,
        generated_at: datetime,
        target_ref: str | None,
        artifact_path: Path,
    ) -> dict[str, str]:
        settings_payload = runtime.account.settings_json if isinstance(runtime.account.settings_json, dict) else {}
        if settings_payload.get("export_notifications_to_obsidian", True) is False:
            return {}
        markdown = "\n".join(
            [
                f"# {event.title}",
                "",
                f"- account: {runtime.account.name} ({runtime.account.slug})",
                f"- channel: {channel}",
                f"- target: {target_ref or 'default'}",
                f"- generated_at: {generated_at.isoformat()}",
                f"- artifact: {artifact_path}",
                "",
                event.body_text,
            ]
        ).strip() + "\n"
        return export_notification_dispatch_note(
            account_slug=runtime.account.slug,
            account_name=runtime.account.name,
            event_type=event.event_type,
            channel=channel,
            generated_at=generated_at.isoformat(),
            markdown_text=markdown,
        )

    def _project_root(self) -> Path:
        return Path(__file__).resolve().parent.parent.parent
