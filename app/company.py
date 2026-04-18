from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import BASE_DIR


@dataclass(frozen=True)
class Company:
    key: str
    data: dict[str, Any]

    @property
    def label(self) -> str:
        return str(self.data["label"])

    @property
    def short_name(self) -> str:
        return str(self.data["short_name"])


def load_companies(path: Path | None = None) -> dict[str, Company]:
    source = path or BASE_DIR / "data" / "companies.json"
    payload = json.loads(source.read_text(encoding="utf-8"))
    return {key: Company(key=key, data=value) for key, value in payload.items()}


def requisites_text(company: Company) -> str:
    data = company.data
    lines = [
        data["full_name"],
        f"Адрес: {data['legal_address']}",
        f"ИНН: {data['inn']}",
        f"КПП: {data.get('kpp', '-')}",
        f"ОГРН/ОГРНИП: {data.get('ogrn', '-')}",
        f"Р/с: {data['bank_account']}",
        f"Банк: {data['bank_name']}",
        f"К/с: {data['corr_account']}",
        f"БИК: {data['bik']}",
        f"Телефон: {data['phone']}",
        f"Email: {data['email']}",
        f"{data['manager_title']}: {data['manager_name']}",
    ]
    return "\n".join(lines)
