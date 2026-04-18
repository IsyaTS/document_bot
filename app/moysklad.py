from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class MoySkladClient:
    login: str | None
    password: str | None

    @property
    def enabled(self) -> bool:
        return bool(self.login and self.password)

    def search_counterparty(self, query: str) -> list[dict[str, Any]]:
        if not self.enabled:
            return []

        response = requests.get(
            "https://api.moysklad.ru/api/remap/1.2/entity/counterparty",
            params={"search": query, "limit": 5},
            auth=(self.login, self.password),
            timeout=20,
            headers={"Accept-Encoding": "gzip", "User-Agent": "document-bot/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("rows", [])

    def search_assortment(self, query: str) -> list[dict[str, Any]]:
        if not self.enabled:
            return []

        response = requests.get(
            "https://api.moysklad.ru/api/remap/1.2/entity/assortment",
            params={"search": query, "limit": 5},
            auth=(self.login, self.password),
            timeout=20,
            headers={"Accept-Encoding": "gzip", "User-Agent": "document-bot/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("rows", [])
