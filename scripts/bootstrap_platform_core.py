from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from platform_core.db import create_session_factory
from platform_core.services.bootstrap import CoreBootstrapService
from platform_core.settings import load_platform_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap the platform core with the first account and admin user.")
    parser.add_argument("--account-slug", required=True, help="Unique account slug, for example: main")
    parser.add_argument("--account-name", required=True, help="Display account name")
    parser.add_argument("--admin-email", required=True, help="Initial admin email")
    parser.add_argument("--admin-full-name", required=True, help="Initial admin full name")
    parser.add_argument("--role", default="owner", help="Initial membership role code, default: owner")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_platform_settings()
    session_factory = create_session_factory(settings)

    with session_factory.begin() as session:
        service = CoreBootstrapService(session, default_timezone=settings.default_timezone)
        result = service.bootstrap_account(
            account_slug=args.account_slug,
            account_name=args.account_name,
            admin_email=args.admin_email,
            admin_full_name=args.admin_full_name,
            membership_role_code=args.role,
        )

    print(
        "Bootstrap completed: "
        f"account_id={result.account_id}, "
        f"user_id={result.user_id}, "
        f"membership_id={result.membership_id}, "
        f"account_created={result.account_created}, "
        f"user_created={result.user_created}, "
        f"membership_created={result.membership_created}"
    )


if __name__ == "__main__":
    main()
