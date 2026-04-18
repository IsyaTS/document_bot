from __future__ import annotations

import time

from platform_core.db import create_session_factory
from platform_core.services.runtime import SchedulerService
from platform_core.settings import load_platform_settings


def run_worker_once() -> dict[str, object]:
    settings = load_platform_settings()
    Session = create_session_factory(settings)
    with Session() as session:
        result = SchedulerService(
            session,
            worker_id=settings.worker_id,
            lease_ttl_seconds=settings.runtime_lease_ttl_seconds,
        ).run_once()
        session.commit()
        return result


def run_worker_loop(*, poll_interval_seconds: int = 30) -> None:
    while True:
        run_worker_once()
        time.sleep(max(1, poll_interval_seconds))
