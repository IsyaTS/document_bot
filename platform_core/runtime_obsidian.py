from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from platform_core.settings import BASE_DIR, PlatformSettings, load_platform_settings


LOCAL_VAULT_DIR = BASE_DIR / "data" / "runtime_obsidian_vault"


def _safe_segment(value: str) -> str:
    text = str(value or "").strip().replace("\\", "-").replace("/", "-")
    return text or "untitled"


def _vault_root(settings: PlatformSettings) -> Path:
    return Path(settings.obsidian_vault_path).expanduser().resolve()


def _export_root(settings: PlatformSettings) -> Path:
    return _vault_root(settings) / _safe_segment(settings.obsidian_export_subdir)


def _frontmatter(title: str, metadata: dict[str, Any] | None = None) -> str:
    lines = ["---", f'title: "{title.replace(chr(34), chr(39))}"']
    for key, value in (metadata or {}).items():
        if value is None:
            continue
        if isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, (int, float)):
            rendered = str(value)
        else:
            rendered = f'"{str(value).replace(chr(34), chr(39))}"'
        lines.append(f"{_safe_segment(str(key)).replace('-', '_')}: {rendered}")
    lines.append("---")
    return "\n".join(lines)


def write_obsidian_note(
    *,
    folder_parts: list[str],
    note_name: str,
    title: str,
    markdown_text: str,
    metadata: dict[str, Any] | None = None,
    settings: PlatformSettings | None = None,
) -> dict[str, str]:
    resolved = settings or load_platform_settings()
    root = _export_root(resolved)
    target_dir = root
    for part in folder_parts:
        target_dir = target_dir / _safe_segment(part)
    target_dir.mkdir(parents=True, exist_ok=True)
    safe_note_name = _safe_segment(note_name)
    body = _frontmatter(title, metadata) + "\n\n" + markdown_text.lstrip()
    note_path = target_dir / f"{safe_note_name}.md"
    note_path.write_text(body, encoding="utf-8")
    latest_path = target_dir / "Latest.md"
    latest_path.write_text(body, encoding="utf-8")
    return {
        "vault_root": str(_vault_root(resolved)),
        "export_root": str(root),
        "note_path": str(note_path),
        "latest_path": str(latest_path),
    }


def export_account_delivery_note(
    *,
    account_slug: str,
    account_name: str,
    generated_at: str,
    markdown_text: str,
    settings: PlatformSettings | None = None,
) -> dict[str, str]:
    timestamp = generated_at or datetime.now(timezone.utc).isoformat()
    note_name = f"{timestamp[:10]} {account_slug} delivery"
    return write_obsidian_note(
        folder_parts=["Accounts", account_slug],
        note_name=note_name,
        title=f"{account_name} Delivery Brief",
        markdown_text=markdown_text,
        metadata={
            "account_slug": account_slug,
            "scope": "account_delivery",
            "generated_at": timestamp,
        },
        settings=settings,
    )


def export_portfolio_brief_note(
    *,
    generated_at: str,
    markdown_text: str,
    settings: PlatformSettings | None = None,
) -> dict[str, str]:
    timestamp = generated_at or datetime.now(timezone.utc).isoformat()
    note_name = f"{timestamp[:10]} portfolio brief"
    return write_obsidian_note(
        folder_parts=["Portfolio"],
        note_name=note_name,
        title="Portfolio Brief",
        markdown_text=markdown_text,
        metadata={
            "scope": "portfolio_brief",
            "generated_at": timestamp,
        },
        settings=settings,
    )


def export_notification_dispatch_note(
    *,
    account_slug: str,
    account_name: str,
    event_type: str,
    channel: str,
    generated_at: str,
    markdown_text: str,
    settings: PlatformSettings | None = None,
) -> dict[str, str]:
    timestamp = generated_at or datetime.now(timezone.utc).isoformat()
    note_name = f"{timestamp[:10]} {account_slug} {event_type} {channel}"
    return write_obsidian_note(
        folder_parts=["Accounts", account_slug, "Notifications"],
        note_name=note_name,
        title=f"{account_name} {event_type} dispatch",
        markdown_text=markdown_text,
        metadata={
            "account_slug": account_slug,
            "scope": "notification_dispatch",
            "event_type": event_type,
            "channel": channel,
            "generated_at": timestamp,
        },
        settings=settings,
    )


def export_copilot_report_note(
    *,
    account_slug: str,
    account_name: str,
    generated_at: str,
    title: str,
    markdown_text: str,
    generation_mode: str,
    model_name: str | None = None,
    settings: PlatformSettings | None = None,
) -> dict[str, str]:
    timestamp = generated_at or datetime.now(timezone.utc).isoformat()
    note_name = f"{timestamp[:10]} {account_slug} copilot"
    return write_obsidian_note(
        folder_parts=["Accounts", account_slug, "Copilot"],
        note_name=note_name,
        title=title or f"{account_name} Copilot Report",
        markdown_text=markdown_text,
        metadata={
            "account_slug": account_slug,
            "scope": "copilot_report",
            "generated_at": timestamp,
            "generation_mode": generation_mode,
            "model_name": model_name or "",
        },
        settings=settings,
    )
