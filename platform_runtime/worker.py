from __future__ import annotations

import time
from datetime import datetime, timezone

from platform_core.db import create_session_factory
from platform_core.runtime_status import write_runtime_status
from platform_core.services.runtime import SchedulerService
from platform_core.settings import load_platform_settings


def run_worker_once() -> dict[str, object]:
    settings = load_platform_settings()
    Session = create_session_factory(settings)
    started_at = datetime.now(timezone.utc)
    with Session() as session:
        try:
            result = SchedulerService(
                session,
                worker_id=settings.worker_id,
                lease_ttl_seconds=settings.runtime_lease_ttl_seconds,
            ).run_once()
            session.commit()
            write_runtime_status(
                "worker_status",
                {
                    "status": "ok",
                    "worker_id": settings.worker_id,
                    "started_at": started_at.isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "result": result,
                },
            )
            return result
        except Exception as exc:
            session.rollback()
            write_runtime_status(
                "worker_status",
                {
                    "status": "error",
                    "worker_id": settings.worker_id,
                    "started_at": started_at.isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "error": str(exc),
                },
            )
            raise


def run_worker_loop(*, poll_interval_seconds: int = 30) -> None:
    while True:
        run_worker_once()
        time.sleep(max(1, poll_interval_seconds))
