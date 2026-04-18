from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from sqlalchemy import create_engine, text

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from platform_core.settings import load_platform_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wait until the configured database becomes reachable.")
    parser.add_argument("--database-url", default=None, help="Override database URL.")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds.")
    parser.add_argument("--interval", type=float, default=2.0, help="Polling interval in seconds.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database_url = args.database_url or load_platform_settings().database_url
    deadline = time.time() + max(1, args.timeout)
    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    last_error: Exception | None = None

    while time.time() < deadline:
        engine = create_engine(database_url, future=True, pool_pre_ping=True, connect_args=connect_args)
        try:
            with engine.connect() as connection:
                connection.execute(text("select 1"))
            print("database-ready")
            return
        except Exception as exc:  # pragma: no cover - operational polling
            last_error = exc
            time.sleep(max(0.1, args.interval))
        finally:
            engine.dispose()

    raise SystemExit(f"Database did not become ready within {args.timeout}s: {last_error}")


if __name__ == "__main__":
    main()
