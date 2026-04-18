from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from platform_core.settings import load_platform_settings
from platform_runtime.worker import run_worker_loop, run_worker_once


def main() -> None:
    settings = load_platform_settings()
    parser = argparse.ArgumentParser(description="Run platform runtime worker.")
    parser.add_argument("--loop", action="store_true", help="Run continuously instead of a single tick.")
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=settings.worker_poll_interval_seconds,
        help="Polling interval in seconds for --loop mode.",
    )
    args = parser.parse_args()

    if args.loop:
        run_worker_loop(poll_interval_seconds=args.poll_interval)
        return

    result = run_worker_once()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
