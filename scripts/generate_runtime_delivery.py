from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate account or portfolio delivery snapshots through the internal runtime API.")
    parser.add_argument("--api-base", default=os.getenv("PLATFORM_RUNTIME_API_BASE", "http://127.0.0.1:18000"))
    parser.add_argument("--internal-token", default=os.getenv("PLATFORM_INTERNAL_API_TOKEN", "local-dev-secret-key"))
    parser.add_argument("--account-slug", help="Generate account delivery snapshot for this account slug.")
    parser.add_argument("--actor-email", required=True, help="Actor email used for account or portfolio visibility.")
    parser.add_argument("--portfolio", action="store_true", help="Generate portfolio brief snapshot.")
    parser.add_argument("--obsidian", action="store_true", help="Also export the generated markdown into the configured Obsidian vault.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.account_slug and not args.portfolio:
        raise SystemExit("Use --account-slug or --portfolio.")
    headers = {
        "X-Internal-API-Token": args.internal_token,
        "Content-Type": "application/json",
    }
    session = requests.Session()
    if args.account_slug:
        response = session.post(
            f"{args.api_base}/internal/reports/accounts/{args.account_slug}/delivery",
            headers=headers,
            json={"actor_email": args.actor_email, "export_obsidian": args.obsidian},
            timeout=60,
        )
    else:
        response = session.post(
            f"{args.api_base}/internal/reports/portfolio/brief",
            headers=headers,
            json={"actor_email": args.actor_email, "export_obsidian": args.obsidian},
            timeout=60,
        )
    if not response.ok:
        raise SystemExit(f"delivery generation failed: {response.status_code} {response.text}")
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
