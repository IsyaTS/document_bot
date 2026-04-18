from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation


FIELD_ALIASES = {
    "клиент": "counterparty_name",
    "заказчик": "counterparty_name",
    "покупатель": "counterparty_name",
    "контрагент": "counterparty_name",
    "инн": "counterparty_inn",
    "кпп": "counterparty_kpp",
    "огрн": "counterparty_ogrn",
    "адрес": "counterparty_address",
    "телефон": "counterparty_phone",
    "email": "counterparty_email",
    "почта": "counterparty_email",
    "директор": "counterparty_manager",
    "представитель": "counterparty_manager",
    "сумма": "amount",
    "ндс": "vat",
    "скидка": "discount",
    "шаблон": "template",
    "предмет": "subject",
    "услуга": "subject",
    "услуги": "subject",
    "товар": "subject",
    "товары": "subject",
    "дверь": "door_item",
    "двери": "door_item",
    "модель": "model",
    "размер": "size",
    "размер проема": "opening_size",
    "проем": "opening_size",
    "толщина стены": "wall_depth",
    "цвет": "color",
    "открывание": "opening_side",
    "замерщик": "measurer",
    "адрес объекта": "object_address",
    "объект": "object_address",
    "гарантия": "warranty_period",
    "серийный номер": "serial_number",
    "груз": "subject",
    "маршрут": "route",
    "адрес погрузки": "loading_address",
    "адрес выгрузки": "unloading_address",
    "дата": "term",
    "срок": "term",
    "сроки": "term",
    "доставка": "delivery",
    "монтаж": "installation",
    "демонтаж": "dismantling",
    "подъем": "lifting",
    "доборы": "extras",
    "наличники": "trim",
    "фурнитура": "hardware",
    "оплата": "payment_terms",
    "назначение платежа": "payment_purpose",
    "основание": "basis",
    "номер": "number",
    "город": "city",
    "претензия": "claim_text",
    "позиция": "item",
    "позиции": "items",
}


@dataclass
class ParsedInput:
    fields: dict[str, str] = field(default_factory=dict)
    items: list[dict[str, str]] = field(default_factory=list)
    raw: str = ""


def parse_user_input(text: str) -> ParsedInput:
    result = ParsedInput(raw=text.strip())
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if ":" not in line:
            result.fields.setdefault("description", line)
            continue

        key, value = line.split(":", 1)
        normalized_key = _normalize_key(key)
        normalized_value = value.strip()
        target = FIELD_ALIASES.get(normalized_key, normalized_key)

        if target in {"item", "items", "door_item"}:
            result.items.extend(parse_items(normalized_value, default_name=key.strip()))
            continue
        if target in {"installation", "dismantling", "lifting", "extras", "trim", "hardware", "delivery"} and "|" in normalized_value:
            result.items.extend(parse_items(normalized_value, default_name=key.strip()))
            continue
        result.fields[target] = normalized_value

    if not result.items:
        subject = result.fields.get("subject") or result.fields.get("description") or "Услуги по заявке"
        amount = result.fields.get("amount") or "0"
        result.items = [{"name": subject, "qty": "1", "unit": "усл.", "price": amount, "total": amount}]

    apply_discount(result)
    return result


def parse_items(value: str, default_name: str | None = None) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    chunks = [chunk.strip() for chunk in re.split(r";|\n", value) if chunk.strip()]
    for chunk in chunks:
        parts = [part.strip() for part in chunk.split("|")]
        if len(parts) >= 4:
            name, qty, unit, price = parts[:4]
            total = _multiply(qty, price)
        elif len(parts) == 3:
            name, qty, price = parts
            unit = "шт."
            total = _multiply(qty, price)
        elif len(parts) == 2:
            name, price = parts
            qty = "1"
            unit = "усл."
            total = price
        else:
            name = parts[0] if default_name is None else f"{default_name}: {parts[0]}"
            qty = "1"
            unit = "усл."
            price = "0"
            total = "0"
        items.append({"name": name, "qty": qty, "unit": unit, "price": price, "total": total})
    return items


def _normalize_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _money_to_decimal(value: str) -> Decimal:
    clean = re.sub(r"[^\d,.-]", "", value).replace(",", ".")
    if not clean:
        return Decimal("0")
    try:
        return Decimal(clean)
    except InvalidOperation:
        return Decimal("0")


def _multiply(qty: str, price: str) -> str:
    total = _money_to_decimal(qty) * _money_to_decimal(price)
    return f"{total.quantize(Decimal('0.01'))}"


def apply_discount(result: ParsedInput) -> None:
    raw = result.fields.get("discount")
    result.items = [item for item in result.items if not item.get("name", "").startswith("Скидка")]
    if not raw:
        return

    clean = raw.strip().replace(",", ".")
    total = sum((_money_to_decimal(item.get("total", "0")) for item in result.items), Decimal("0"))
    if total <= 0:
        return

    if "%" in clean:
        percent = _money_to_decimal(clean)
        value = (total * percent / Decimal("100")).quantize(Decimal("0.01"))
        name = f"Скидка {percent}%"
    else:
        value = _money_to_decimal(clean).quantize(Decimal("0.01"))
        name = "Скидка"

    if value <= 0:
        return
    result.items.append({"name": name, "qty": "1", "unit": "усл.", "price": f"-{value}", "total": f"-{value}"})
