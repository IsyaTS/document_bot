from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from platform_core.settings import BASE_DIR


DELIVERY_DIR = BASE_DIR / "data" / "runtime_delivery"


def _bundle_dir(scope: str) -> Path:
    safe_scope = str(scope or "general").strip().replace("/", "_")
    return DELIVERY_DIR / safe_scope


def write_delivery_bundle(
    *,
    scope: str,
    name: str,
    json_payload: dict[str, Any],
    markdown_text: str,
    text_text: str,
) -> dict[str, str]:
    target_dir = _bundle_dir(scope)
    target_dir.mkdir(parents=True, exist_ok=True)
    safe_name = str(name or "report").strip().replace("/", "_")
    json_path = target_dir / f"{safe_name}.json"
    md_path = target_dir / f"{safe_name}.md"
    txt_path = target_dir / f"{safe_name}.txt"
    json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    md_path.write_text(markdown_text, encoding="utf-8")
    txt_path.write_text(text_text, encoding="utf-8")
    return {
        "json_path": str(json_path),
        "markdown_path": str(md_path),
        "text_path": str(txt_path),
    }
