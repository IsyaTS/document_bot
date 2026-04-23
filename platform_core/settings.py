from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PlatformSettings:
    app_version: str
    environment: str
    database_url: str
    secret_key: str
    credentials_key: str | None
    default_timezone: str
    internal_api_token: str
    admin_access_code: str
    obsidian_vault_path: str
    obsidian_export_subdir: str
    api_host: str
    api_port: int
    worker_id: str
    worker_poll_interval_seconds: int
    runtime_lease_ttl_seconds: int
    notification_telegram_bot_token: str | None
    telegram_api_id: str | None
    telegram_api_hash: str | None
    avito_client_id: str | None
    avito_client_secret: str | None
    avito_oauth_scope: str | None
    openai_api_key: str | None
    openai_model: str
    openai_reasoning_effort: str
    smtp_host: str | None
    smtp_port: int
    smtp_username: str | None
    smtp_password: str | None
    smtp_from_email: str | None
    smtp_use_starttls: bool
    smtp_timeout_seconds: int


def _default_database_url() -> str:
    path = (BASE_DIR / "data" / "platform.sqlite3").resolve()
    return f"sqlite+pysqlite:///{path.as_posix()}"


def _default_app_version() -> str:
    try:
        value = subprocess.check_output(
            ["git", "-C", str(BASE_DIR), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if value:
            return f"0.1.0+{value}"
    except Exception:
        pass
    return "0.1.0"


def load_platform_settings() -> PlatformSettings:
    load_dotenv(BASE_DIR / ".env")

    environment = os.getenv("PLATFORM_ENV", "development").strip() or "development"
    secret_key = os.getenv("PLATFORM_SECRET_KEY", "local-dev-secret-key").strip() or "local-dev-secret-key"
    if environment == "production" and secret_key == "local-dev-secret-key":
        raise RuntimeError("PLATFORM_SECRET_KEY must be set explicitly in production.")

    return PlatformSettings(
        app_version=os.getenv("PLATFORM_APP_VERSION", _default_app_version()).strip() or _default_app_version(),
        environment=environment,
        database_url=os.getenv("PLATFORM_DATABASE_URL", _default_database_url()).strip() or _default_database_url(),
        secret_key=secret_key,
        credentials_key=os.getenv("PLATFORM_CREDENTIALS_KEY") or None,
        default_timezone=os.getenv("PLATFORM_DEFAULT_TIMEZONE", "Etc/UTC").strip() or "Etc/UTC",
        internal_api_token=os.getenv("PLATFORM_INTERNAL_API_TOKEN", secret_key).strip() or secret_key,
        admin_access_code=os.getenv("PLATFORM_ADMIN_ACCESS_CODE", secret_key).strip() or secret_key,
        obsidian_vault_path=os.getenv("PLATFORM_OBSIDIAN_VAULT_PATH", str((BASE_DIR / "data" / "runtime_obsidian_vault").resolve())).strip()
        or str((BASE_DIR / "data" / "runtime_obsidian_vault").resolve()),
        obsidian_export_subdir=os.getenv("PLATFORM_OBSIDIAN_EXPORT_SUBDIR", "Hermes Platform").strip() or "Hermes Platform",
        api_host=os.getenv("PLATFORM_API_HOST", "0.0.0.0").strip() or "0.0.0.0",
        api_port=max(1, int(os.getenv("PLATFORM_API_PORT", "8000"))),
        worker_id=os.getenv("PLATFORM_WORKER_ID", "worker-default").strip() or "worker-default",
        worker_poll_interval_seconds=max(1, int(os.getenv("PLATFORM_WORKER_POLL_INTERVAL_SECONDS", "30"))),
        runtime_lease_ttl_seconds=max(15, int(os.getenv("PLATFORM_RUNTIME_LEASE_TTL_SECONDS", "120"))),
        notification_telegram_bot_token=os.getenv("PLATFORM_NOTIFICATION_TELEGRAM_BOT_TOKEN") or None,
        telegram_api_id=os.getenv("PLATFORM_TELEGRAM_API_ID") or None,
        telegram_api_hash=os.getenv("PLATFORM_TELEGRAM_API_HASH") or None,
        avito_client_id=os.getenv("PLATFORM_AVITO_CLIENT_ID") or None,
        avito_client_secret=os.getenv("PLATFORM_AVITO_CLIENT_SECRET") or None,
        avito_oauth_scope=os.getenv("PLATFORM_AVITO_OAUTH_SCOPE") or None,
        openai_api_key=os.getenv("PLATFORM_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") or None,
        openai_model=(os.getenv("PLATFORM_OPENAI_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-5-mini").strip() or "gpt-5-mini",
        openai_reasoning_effort=(os.getenv("PLATFORM_OPENAI_REASONING_EFFORT", "medium").strip() or "medium"),
        smtp_host=os.getenv("PLATFORM_SMTP_HOST") or None,
        smtp_port=max(1, int(os.getenv("PLATFORM_SMTP_PORT", "587"))),
        smtp_username=os.getenv("PLATFORM_SMTP_USERNAME") or None,
        smtp_password=os.getenv("PLATFORM_SMTP_PASSWORD") or None,
        smtp_from_email=os.getenv("PLATFORM_SMTP_FROM_EMAIL") or None,
        smtp_use_starttls=(os.getenv("PLATFORM_SMTP_USE_STARTTLS", "true").strip().lower() not in {"0", "false", "no"}),
        smtp_timeout_seconds=max(1, int(os.getenv("PLATFORM_SMTP_TIMEOUT_SECONDS", "15"))),
    )
