from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from app.config import BASE_DIR
from app.parser import ParsedInput


@dataclass(frozen=True)
class CatalogMatch:
    section: str
    name: str
    price: Decimal
    unit: str
    score: float
    note: str = ""


def load_door_catalog(path: Path | None = None) -> dict[str, Any]:
    source = path or BASE_DIR / "data" / "door_catalog.json"
    return json.loads(source.read_text(encoding="utf-8"))


def catalog_text() -> str:
    catalog = load_door_catalog()
    lines = ["Каталог для быстрых КП"]
    lines.append("")
    lines.append("Двери:")
    for item in catalog.get("doors", []):
        lines.append(f"- {item['name']}: {item['price']} руб./{item['unit']} ({item.get('note', '')})")
    lines.append("")
    lines.append("Услуги:")
    for item in catalog.get("services", []):
        lines.append(f"- {item['name']}: {item['price']} руб./{item['unit']}")
    lines.append("")
    lines.append("Шаблоны КП: " + ", ".join(catalog.get("templates", [])))
    lines.append("")
    lines.append("Можно искать неполно: стандарт, прем, монтаж, доставка.")
    return "\n".join(lines)


def update_catalog_price(name_query: str, price: int | float | Decimal, path: Path | None = None) -> str | None:
    source = path or BASE_DIR / "data" / "door_catalog.json"
    catalog = load_door_catalog(source)
    matches = search_catalog(name_query, limit=1, catalog=catalog)
    if not matches:
        return None

    match = matches[0]
    for section in ("doors", "services"):
        for item in catalog.get(section, []):
            if item["name"] == match.name:
                item["price"] = float(price) if isinstance(price, Decimal) else price
                source.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
                return item["name"]
    return None


def search_catalog(query: str, limit: int = 5, catalog: dict[str, Any] | None = None) -> list[CatalogMatch]:
    payload = catalog or load_door_catalog()
    normalized_query = _normalize(query)
    if not normalized_query:
        return []

    matches: list[CatalogMatch] = []
    for section in ("doors", "services"):
        for item in payload.get(section, []):
            normalized_name = _normalize(item["name"])
            score = _score(normalized_query, normalized_name)
            if score >= 0.34:
                matches.append(
                    CatalogMatch(
                        section=section,
                        name=item["name"],
                        price=_decimal(str(item.get("price", "0"))),
                        unit=item.get("unit", "шт."),
                        score=score,
                        note=item.get("note", ""),
                    )
                )
    matches.sort(key=lambda item: item.score, reverse=True)
    return matches[:limit]


def search_text(query: str) -> str:
    matches = search_catalog(query)
    if not matches:
        return "Ничего не нашел. Попробуйте короче: стандарт, премиум, монтаж, доставка."

    lines = ["Нашел в каталоге:"]
    for item in matches:
        lines.append(f"- {item.name}: {_format_money(item.price)} руб./{item.unit}")
    lines.append("")
    lines.append("Можно вставить в счет или КП так:")
    best = matches[0]
    lines.append(f"Позиции: {best.name} | 1 | {best.unit} | {_format_money(best.price)}")
    return "\n".join(lines)


def enrich_items_from_catalog(parsed: ParsedInput) -> None:
    for item in parsed.items:
        current_price = _decimal(item.get("price", "0"))
        maybe_qty = current_price > 0 and current_price <= 100 and item.get("qty", "1") == "1" and item.get("unit", "усл.") == "усл."
        if current_price > 0 and not maybe_qty:
            continue
        matches = search_catalog(item.get("name", ""), limit=1)
        if not matches:
            continue
        match = matches[0]
        qty = current_price if maybe_qty else _decimal(item.get("qty", "1"))
        total = (qty * match.price).quantize(Decimal("0.01"))
        item["name"] = match.name
        item["qty"] = _format_qty(qty)
        item["unit"] = match.unit
        item["price"] = _format_money(match.price)
        item["total"] = _format_money(total)


def _score(query: str, name: str) -> float:
    if query in name or name in query:
        return 1.0
    query_tokens = set(query.split())
    name_tokens = set(name.split())
    token_overlap = len(query_tokens & name_tokens) / max(len(query_tokens), 1)
    ratio = SequenceMatcher(None, query, name).ratio()
    prefix = _prefix_score(query, name_tokens)
    return max(ratio, token_overlap, prefix)


def _prefix_score(query: str, name_tokens: set[str]) -> float:
    hits = 0
    for token in name_tokens:
        if token.startswith(query) or query.startswith(token[: max(2, min(len(token), len(query)))]):
            hits += 1
    return hits / max(len(name_tokens), 1)


def _normalize(value: str) -> str:
    clean = re.sub(r"[^0-9a-zа-яё]+", " ", value.lower())
    return re.sub(r"\s+", " ", clean).strip()


def _decimal(value: str) -> Decimal:
    try:
        clean = "".join(ch for ch in str(value).replace(",", ".") if ch.isdigit() or ch in ".-")
        return Decimal(clean or "0")
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _format_money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def _format_qty(value: Decimal) -> str:
    if value == value.to_integral():
        return str(int(value))
    return str(value.normalize())
