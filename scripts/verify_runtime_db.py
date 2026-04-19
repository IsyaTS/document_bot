from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from platform_core.settings import load_platform_settings
from platform_core.runtime_status import write_runtime_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify runtime DB connectivity and schema head.")
    parser.add_argument("--database-url", default=None, help="Override database URL.")
    parser.add_argument("--expected-head", default="e7f8a9b0c311", help="Expected alembic revision.")
    parser.add_argument("--account-slug", default="hermes", help="Account slug expected to exist.")
    parser.add_argument("--integration-external-ref", default="hermes-avito-main", help="Hermes integration ref expected to exist.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_platform_settings()
    database_url = args.database_url or settings.database_url
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, future=True, pool_pre_ping=True, connect_args=connect_args)
    try:
        with engine.connect() as connection:
            connection.execute(text("select 1"))
            revision = connection.execute(text("select version_num from alembic_version")).scalar_one()
            account_id = connection.execute(
                text("select id from accounts where slug = :slug"),
                {"slug": args.account_slug},
            ).scalar_one()
            integration_id = connection.execute(
                text("select id from integrations where account_id = :account_id and external_ref = :external_ref"),
                {"account_id": int(account_id), "external_ref": args.integration_external_ref},
            ).scalar_one()
            payload = {
                "status": "ok",
                "database_url": database_url,
                "revision": revision,
                "account_slug": args.account_slug,
                "account_id": int(account_id),
                "integration_external_ref": args.integration_external_ref,
                "integration_id": int(integration_id),
            }
            write_runtime_status("verify_status", payload)
            print(payload)
            if revision != args.expected_head:
                raise SystemExit(
                    f"Unexpected alembic revision: {revision}. Expected {args.expected_head}."
                )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
