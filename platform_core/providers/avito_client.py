from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any

import requests


DEFAULT_BASE_URL = "https://api.avito.ru"


def _deep_get(payload: dict[str, Any], key_path: str | None) -> Any:
    if not key_path:
        return None
    current: Any = payload
    for part in key_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


@dataclass
class AvitoAPIClient:
    access_token: str
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: int = 30
    max_retries: int = 3
    backoff_seconds: float = 1.0
    session: requests.Session | None = None

    def _session(self) -> requests.Session:
        return self.session or requests.Session()

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "platform-runtime/1.0",
        }

    def fetch_paginated(
        self,
        path: str,
        *,
        params: dict[str, object] | None = None,
        items_key: str,
        cursor: str | None = None,
        cursor_param: str = "cursor",
        next_cursor_keys: tuple[str, ...] = ("next_cursor", "nextCursor", "pagination.next_cursor"),
        max_pages: int = 20,
    ) -> tuple[list[dict[str, Any]], str | None]:
        rows: list[dict[str, Any]] = []
        current_cursor = cursor
        last_cursor = cursor
        for _ in range(max_pages):
            page_params = dict(params or {})
            if current_cursor:
                page_params[cursor_param] = current_cursor
            payload = self._request_json(path, params=page_params)
            extracted = _deep_get(payload, items_key)
            if isinstance(extracted, list):
                rows.extend(dict(item) for item in extracted if isinstance(item, dict))

            next_cursor = None
            for key in next_cursor_keys:
                value = _deep_get(payload, key)
                if value:
                    next_cursor = str(value)
                    break
            if not next_cursor or next_cursor == current_cursor:
                last_cursor = current_cursor
                break
            last_cursor = next_cursor
            current_cursor = next_cursor
        return rows, last_cursor

    def fetch_json(self, path: str, *, params: dict[str, object] | None = None) -> dict[str, Any]:
        return self._request_json(path, params=params)

    def _request_json(self, path: str, *, params: dict[str, object] | None = None) -> dict[str, Any]:
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            response = None
            try:
                response = self._session().get(
                    url,
                    params=params or {},
                    timeout=self.timeout_seconds,
                    headers=self._headers(),
                )
                if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    time.sleep(self.backoff_seconds * attempt)
                    continue
                response.raise_for_status()
                payload = response.json()
                return payload if isinstance(payload, dict) else {}
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.backoff_seconds * attempt)
        return {}
