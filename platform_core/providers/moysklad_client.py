from __future__ import annotations

import base64
import logging
from time import perf_counter
from dataclasses import dataclass
from typing import Any

import requests


DEFAULT_BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
logger = logging.getLogger(__name__)


class MoySkladClientError(RuntimeError):
    def __init__(self, message: str, *, stage: str, entity_path: str, fields: list[str] | None = None) -> None:
        super().__init__(message)
        self.stage = stage
        self.entity_path = entity_path
        self.fields = fields or []


@dataclass
class MoySkladAPIClient:
    login: str
    password: str
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: int = 30
    session: requests.Session | None = None

    def _session(self) -> requests.Session:
        return self.session or requests.Session()

    def _headers(self) -> dict[str, str]:
        token = base64.b64encode(f"{self.login}:{self.password}".encode("utf-8")).decode("ascii")
        return {
            "Accept-Encoding": "gzip",
            "User-Agent": "platform-runtime/1.0",
            "Authorization": f"Basic {token}",
        }

    def fetch_rows(self, path: str, *, params: dict[str, object] | None = None) -> list[dict[str, Any]]:
        normalized_path = path.lstrip("/")
        url = f"{self.base_url.rstrip('/')}/{normalized_path}"
        request_started = perf_counter()
        try:
            response = self._session().get(
                url,
                params=params or {},
                timeout=self.timeout_seconds,
                headers=self._headers(),
            )
        except requests.Timeout as exc:
            duration_ms = int((perf_counter() - request_started) * 1000)
            logger.error(
                "moysklad_fetch_rows entity_path=%s stage=%s result=%s duration_ms=%s params=%s error_code=%s error_message=%s",
                normalized_path,
                "request_timeout",
                "failed",
                duration_ms,
                ",".join(f"{key}={value}" for key, value in sorted((params or {}).items())) or "none",
                exc.__class__.__name__,
                exc,
            )
            raise MoySkladClientError(
                "MoySklad request timed out.",
                stage="request_timeout",
                entity_path=normalized_path,
            ) from exc
        except UnicodeEncodeError as exc:
            non_ascii_fields = [
                field_name
                for field_name, value in (("login", self.login), ("password", self.password))
                if any(ord(ch) > 127 for ch in str(value))
            ]
            logger.warning(
                "moysklad_request_encoding_error stage=%s entity_path=%s fields=%s params=%s error=%s",
                "request",
                normalized_path,
                ",".join(non_ascii_fields) or "none",
                ",".join(sorted(str(key) for key in (params or {}).keys())) or "none",
                exc,
            )
            raise MoySkladClientError(
                "MoySklad auth encoding failed before request dispatch.",
                stage="request",
                entity_path=normalized_path,
                fields=non_ascii_fields,
            ) from exc
        duration_ms = int((perf_counter() - request_started) * 1000)
        logger.info(
            "moysklad_fetch_rows entity_path=%s stage=%s result=%s duration_ms=%s params=%s status_code=%s",
            normalized_path,
            "request",
            "ok",
            duration_ms,
            ",".join(f"{key}={value}" for key, value in sorted((params or {}).items())) or "none",
            response.status_code,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            rows = payload.get("rows")
            if isinstance(rows, list):
                return rows
        return []

    def fetch_all_rows(
        self,
        path: str,
        *,
        params: dict[str, object] | None = None,
        page_size: int = 100,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        base_params = dict(params or {})
        offset = int(base_params.pop("offset", 0) or 0)
        limit = int(base_params.pop("limit", page_size) or page_size)
        for page_index in range(max_pages):
            logger.info(
                "moysklad_fetch_all_rows entity_path=%s stage=%s result=%s page_index=%s offset=%s limit=%s total_fetched=%s",
                path.lstrip("/"),
                "page_request",
                "running",
                page_index,
                offset,
                limit,
                len(all_rows),
            )
            rows = self.fetch_rows(path, params={**base_params, "limit": limit, "offset": offset})
            logger.info(
                "moysklad_fetch_all_rows entity_path=%s stage=%s result=%s page_index=%s offset=%s limit=%s rows_count=%s total_fetched=%s",
                path.lstrip("/"),
                "page_response",
                "ok",
                page_index,
                offset,
                limit,
                len(rows),
                len(all_rows) + len(rows),
            )
            if not rows:
                logger.info(
                    "moysklad_fetch_all_rows entity_path=%s stage=%s result=%s stop_reason=%s total_fetched=%s",
                    path.lstrip("/"),
                    "pagination_stop",
                    "completed",
                    "empty_page",
                    len(all_rows),
                )
                break
            all_rows.extend(rows)
            if len(rows) < limit:
                logger.info(
                    "moysklad_fetch_all_rows entity_path=%s stage=%s result=%s stop_reason=%s total_fetched=%s",
                    path.lstrip("/"),
                    "pagination_stop",
                    "completed",
                    "short_page",
                    len(all_rows),
                )
                break
            offset += limit
        else:
            logger.warning(
                "moysklad_fetch_all_rows entity_path=%s stage=%s result=%s stop_reason=%s total_fetched=%s max_pages=%s",
                path.lstrip("/"),
                "pagination_stop",
                "capped",
                "max_pages_reached",
                len(all_rows),
                max_pages,
            )
        return all_rows
