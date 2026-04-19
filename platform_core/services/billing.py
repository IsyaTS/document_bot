from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.models import Document, DocumentSettlement, PayrollEntry, PayrollPayment, PayrollPeriod
from platform_core.services.operations import OperationsService
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class PayrollRegisterSnapshot:
    payroll_period: PayrollPeriod
    entries: list[PayrollEntry]
    payments: list[PayrollPayment]
    total_net_amount: Decimal
    total_paid_amount: Decimal
    total_outstanding_amount: Decimal


class BillingService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_documents(self, context: TenantContext, *, document_types: list[str] | None = None) -> list[Document]:
        account_id = require_account_id(context)
        stmt = select(Document).where(Document.account_id == account_id).order_by(Document.issued_at.desc(), Document.id.desc())
        if document_types:
            stmt = stmt.where(Document.document_type.in_(document_types))
        return self.session.execute(stmt).scalars().all()

    def list_settlements(self, context: TenantContext, *, document_id: int | None = None) -> list[DocumentSettlement]:
        account_id = require_account_id(context)
        stmt = select(DocumentSettlement).where(DocumentSettlement.account_id == account_id).order_by(DocumentSettlement.settlement_date.desc(), DocumentSettlement.id.desc())
        if document_id is not None:
            stmt = stmt.where(DocumentSettlement.document_id == document_id)
        return self.session.execute(stmt).scalars().all()

    def create_billing_document(
        self,
        context: TenantContext,
        *,
        document_type: str,
        document_number: str | None,
        customer_id: int | None,
        total_amount: Decimal,
        issued_at: date | None,
        due_date: date | None,
        summary: str | None,
        template_code: str | None,
    ) -> Document:
        if document_type not in {"invoice", "claim"}:
            raise PlatformCoreError("Billing document type must be invoice or claim.")
        operations = OperationsService(self.session)
        document = operations.create_document(
            context,
            document_type=document_type,
            document_number=document_number,
            customer_id=customer_id,
            deal_id=None,
            status="issued" if document_type == "invoice" else "draft",
            issued_at=None if issued_at is None else date_to_datetime_utc(issued_at),
            total_amount=total_amount,
            summary=summary,
        )
        document.snapshot_json = {
            **(document.snapshot_json or {}),
            "due_date": due_date.isoformat() if due_date else None,
            "template_code": template_code or "",
        }
        self.session.flush()
        return document

    def record_settlement(
        self,
        context: TenantContext,
        *,
        document_id: int,
        recorded_by_user_id: int | None,
        settlement_type: str,
        settlement_date: date,
        amount: Decimal,
        reference: str | None,
        note: str | None,
        status: str = "recorded",
    ) -> tuple[Document, DocumentSettlement]:
        if settlement_type not in {"payment", "resolution", "writeoff"}:
            raise PlatformCoreError("Unsupported settlement type.")
        if status not in {"recorded", "confirmed", "cancelled"}:
            raise PlatformCoreError("Unsupported settlement status.")
        document = self._document(context, document_id)
        if amount < 0:
            raise PlatformCoreError("Settlement amount cannot be negative.")
        settlement = DocumentSettlement(
            account_id=document.account_id,
            document_id=document.id,
            recorded_by_user_id=recorded_by_user_id,
            settlement_type=settlement_type,
            status=status,
            settlement_date=settlement_date,
            amount=amount,
            currency=document.currency,
            reference=(reference or "").strip() or None,
            notes_json={"note": (note or "").strip() or None},
        )
        self.session.add(settlement)
        self.session.flush()

        confirmed_total = sum(
            Decimal(item.amount)
            for item in self.list_settlements(context, document_id=document.id)
            if item.status in {"recorded", "confirmed"}
        )
        snapshot = dict(document.snapshot_json or {})
        snapshot["settled_amount"] = str(confirmed_total)
        snapshot["last_settlement_date"] = settlement_date.isoformat()
        snapshot["last_settlement_type"] = settlement_type
        document.snapshot_json = snapshot
        if document.document_type == "invoice":
            document.status = "paid" if confirmed_total >= Decimal(document.total_amount) else "sent"
        elif document.document_type == "claim" and settlement_type in {"resolution", "writeoff"}:
            document.status = "accepted" if status in {"recorded", "confirmed"} else document.status
        self.session.flush()
        return document, settlement

    def payroll_register(self, context: TenantContext, *, payroll_period_id: int) -> PayrollRegisterSnapshot:
        account_id = require_account_id(context)
        period = self.session.execute(
            select(PayrollPeriod).where(PayrollPeriod.account_id == account_id, PayrollPeriod.id == payroll_period_id)
        ).scalar_one_or_none()
        if period is None:
            raise TenantContextError("Payroll period not found.")
        entries = self.session.execute(
            select(PayrollEntry).where(PayrollEntry.account_id == account_id, PayrollEntry.payroll_period_id == period.id).order_by(PayrollEntry.id.asc())
        ).scalars().all()
        entry_ids = [item.id for item in entries]
        payments = self.session.execute(
            select(PayrollPayment)
            .where(PayrollPayment.account_id == account_id, PayrollPayment.payroll_entry_id.in_(entry_ids if entry_ids else [-1]))
            .order_by(PayrollPayment.payment_date.asc(), PayrollPayment.id.asc())
        ).scalars().all()
        total_net = sum(Decimal(item.net_amount) for item in entries)
        total_paid = sum(Decimal(item.amount) for item in payments if item.status in {"recorded", "confirmed"})
        return PayrollRegisterSnapshot(
            payroll_period=period,
            entries=entries,
            payments=payments,
            total_net_amount=total_net,
            total_paid_amount=total_paid,
            total_outstanding_amount=total_net - total_paid,
        )

    def render_payroll_register(self, context: TenantContext, *, payroll_period_id: int, format_name: str) -> str:
        snapshot = self.payroll_register(context, payroll_period_id=payroll_period_id)
        payload = {
            "payroll_period_id": snapshot.payroll_period.id,
            "period_kind": snapshot.payroll_period.period_kind,
            "period_start": snapshot.payroll_period.period_start.isoformat(),
            "period_end": snapshot.payroll_period.period_end.isoformat(),
            "status": snapshot.payroll_period.status,
            "total_net_amount": str(snapshot.total_net_amount),
            "total_paid_amount": str(snapshot.total_paid_amount),
            "total_outstanding_amount": str(snapshot.total_outstanding_amount),
            "entries": [
                {
                    "entry_id": item.id,
                    "employee_id": item.employee_id,
                    "status": item.status,
                    "net_amount": str(item.net_amount),
                    "summary": item.summary_json or {},
                }
                for item in snapshot.entries
            ],
            "payments": [
                {
                    "payment_id": item.id,
                    "entry_id": item.payroll_entry_id,
                    "status": item.status,
                    "payment_date": item.payment_date.isoformat(),
                    "amount": str(item.amount),
                    "payment_ref": item.payment_ref,
                }
                for item in snapshot.payments
            ],
        }
        if format_name == "json":
            return json.dumps(payload, ensure_ascii=True, indent=2) + "\n"
        lines = [
            f"Payroll register #{snapshot.payroll_period.id}",
            f"Period: {snapshot.payroll_period.period_start.isoformat()} .. {snapshot.payroll_period.period_end.isoformat()}",
            f"Status: {snapshot.payroll_period.status}",
            f"Total net: {snapshot.total_net_amount}",
            f"Total paid: {snapshot.total_paid_amount}",
            f"Outstanding: {snapshot.total_outstanding_amount}",
            "",
            "Entries:",
        ]
        for item in snapshot.entries:
            lines.append(f"- entry #{item.id} employee #{item.employee_id}: {item.net_amount} ({item.status})")
        if snapshot.payments:
            lines.append("")
            lines.append("Payments:")
            for item in snapshot.payments:
                lines.append(f"- payment #{item.id} entry #{item.payroll_entry_id}: {item.amount} on {item.payment_date.isoformat()} ({item.status})")
        return "\n".join(lines).strip() + "\n"

    def _document(self, context: TenantContext, document_id: int) -> Document:
        account_id = require_account_id(context)
        document = self.session.execute(
            select(Document).where(Document.account_id == account_id, Document.id == document_id)
        ).scalar_one_or_none()
        if document is None:
            raise TenantContextError("Document not found in selected account.")
        return document


def date_to_datetime_utc(value: date):
    from datetime import datetime, timezone

    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
