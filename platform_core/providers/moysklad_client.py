from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


DEFAULT_BASE_URL = "https://api.moysklad.ru/api/remap/1.2"


@dataclass
class MoySkladAPIClient:
    login: str
    password: str
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: int = 30
    session: requests.Session | None = None

    def _session(self) -> requests.Session:
        return self.session or requests.Session()

    def fetch_rows(self, path: str, *, params: dict[str, object] | None = None) -> list[dict[str, Any]]:
        response = self._session().get(
            f"{self.base_url.rstrip('/')}/{path.lstrip('/')}",
            params=params or {},
            auth=(self.login, self.password),
            timeout=self.timeout_seconds,
            headers={"Accept-Encoding": "gzip", "User-Agent": "platform-runtime/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            rows = payload.get("rows")
            if isinstance(rows, list):
                return rows
        return []
