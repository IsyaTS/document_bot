from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    openai_api_key: str | None
    openai_model: str
    moysklad_login: str | None
    moysklad_password: str | None
    bot_admin_ids: set[int]
    default_group_chat_id: int | None
    documents_dir: Path
    database_path: Path


def _read_admin_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    ids: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        value = part.strip()
        if value:
            ids.add(int(value))
    return ids


def _path_from_env(name: str, default: str) -> Path:
    raw = os.getenv(name, default)
    path = Path(raw)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path


def _read_optional_int(raw: str | None) -> int | None:
    if not raw:
        return None
    value = raw.strip()
    if not value:
        return None
    return int(value)


def load_settings() -> Settings:
    load_dotenv(BASE_DIR / ".env")

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Copy .env.example to .env and add a fresh token.")

    return Settings(
        telegram_bot_token=token,
        openai_api_key=os.getenv("OPENAI_API_KEY") or None,
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini",
        moysklad_login=os.getenv("MOYSKLAD_LOGIN") or None,
        moysklad_password=os.getenv("MOYSKLAD_PASSWORD") or None,
        bot_admin_ids=_read_admin_ids(os.getenv("BOT_ADMIN_IDS")),
        default_group_chat_id=_read_optional_int(os.getenv("DEFAULT_GROUP_CHAT_ID")),
        documents_dir=_path_from_env("DOCUMENTS_DIR", "generated"),
        database_path=_path_from_env("DATABASE_PATH", "data/bot.sqlite3"),
    )
