#!/usr/bin/env bash
set -euo pipefail

python scripts/wait_for_db.py --timeout 60
alembic upgrade head
exec python scripts/run_platform_runtime_worker.py --loop --poll-interval "${PLATFORM_WORKER_POLL_INTERVAL_SECONDS:-30}"
