from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from platform_core.settings import BASE_DIR


STATUS_DIR = BASE_DIR / "data" / "runtime_status"


def _status_path(name: str) -> Path:
    return STATUS_DIR / f"{name}.json"


def write_runtime_status(name: str, payload: dict[str, Any]) -> Path:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body.setdefault("written_at", datetime.now(timezone.utc).isoformat())
    target = _status_path(name)
    with NamedTemporaryFile("w", encoding="utf-8", dir=STATUS_DIR, delete=False) as handle:
        json.dump(body, handle, ensure_ascii=False, indent=2, default=str)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(target)
    return target


def read_runtime_status(name: str) -> dict[str, Any] | None:
    path = _status_path(name)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "status": "invalid",
            "error": "Malformed status payload.",
            "path": str(path),
            "written_at": datetime.now(timezone.utc).isoformat(),
        }
