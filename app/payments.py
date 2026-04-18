from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import qrcode

from app.company import Company
from app.parser import ParsedInput


def payment_qr_payload(company: Company, parsed: ParsedInput, number: str) -> str:
    data = company.data
    amount_kopecks = int((_items_total(parsed.items) * Decimal("100")).quantize(Decimal("1")))
    purpose = parsed.fields.get("payment_purpose") or f"Оплата счета N {number}"
    fields = {
        "Name": data["short_name"],
        "PersonalAcc": data["bank_account"],
        "BankName": data["bank_name"],
        "BIC": data["bik"],
        "CorrespAcc": data["corr_account"],
        "PayeeINN": data["inn"],
        "KPP": data.get("kpp", ""),
        "Sum": str(amount_kopecks),
        "Purpose": purpose,
    }
    return "ST00012|" + "|".join(f"{key}={quote(str(value))}" for key, value in fields.items() if value)


def generate_payment_qr_png(company: Company, parsed: ParsedInput, output_dir: Path, number: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"qr_invoice_{number}_{uuid4().hex[:8]}.png"
    image = qrcode.make(payment_qr_payload(company, parsed, number))
    image.save(path)
    return path


def _items_total(items: list[dict[str, str]]) -> Decimal:
    total = Decimal("0")
    for item in items:
        total += _decimal(item.get("total", "0"))
    return total


def _decimal(value: str) -> Decimal:
    try:
        clean = "".join(ch for ch in str(value).replace(",", ".") if ch.isdigit() or ch in ".-")
        return Decimal(clean or "0")
    except (InvalidOperation, ValueError):
        return Decimal("0")
