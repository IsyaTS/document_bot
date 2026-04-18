from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PlatformSettings:
    environment: str
    database_url: str
    secret_key: str
    credentials_key: str | None
    default_timezone: str
    internal_api_token: str
    api_host: str
    api_port: int
    worker_id: str
    worker_poll_interval_seconds: int
    runtime_lease_ttl_seconds: int


def _default_database_url() -> str:
    path = (BASE_DIR / "data" / "platform.sqlite3").resolve()
    return f"sqlite+pysqlite:///{path.as_posix()}"


def load_platform_settings() -> PlatformSettings:
    load_dotenv(BASE_DIR / ".env")

    environment = os.getenv("PLATFORM_ENV", "development").strip() or "development"
    secret_key = os.getenv("PLATFORM_SECRET_KEY", "local-dev-secret-key").strip() or "local-dev-secret-key"
    if environment == "production" and secret_key == "local-dev-secret-key":
        raise RuntimeError("PLATFORM_SECRET_KEY must be set explicitly in production.")

    return PlatformSettings(
        environment=environment,
        database_url=os.getenv("PLATFORM_DATABASE_URL", _default_database_url()).strip() or _default_database_url(),
        secret_key=secret_key,
        credentials_key=os.getenv("PLATFORM_CREDENTIALS_KEY") or None,
        default_timezone=os.getenv("PLATFORM_DEFAULT_TIMEZONE", "Etc/UTC").strip() or "Etc/UTC",
        internal_api_token=os.getenv("PLATFORM_INTERNAL_API_TOKEN", secret_key).strip() or secret_key,
        api_host=os.getenv("PLATFORM_API_HOST", "0.0.0.0").strip() or "0.0.0.0",
        api_port=max(1, int(os.getenv("PLATFORM_API_PORT", "8000"))),
        worker_id=os.getenv("PLATFORM_WORKER_ID", "worker-default").strip() or "worker-default",
        worker_poll_interval_seconds=max(1, int(os.getenv("PLATFORM_WORKER_POLL_INTERVAL_SECONDS", "30"))),
        runtime_lease_ttl_seconds=max(15, int(os.getenv("PLATFORM_RUNTIME_LEASE_TTL_SECONDS", "120"))),
    )
