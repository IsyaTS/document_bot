from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlalchemy import select

from platform_core.db import create_session_factory
from platform_core.models import Account
from platform_core.services.runtime import AdminQueryService
from platform_core.settings import load_platform_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print Hermes operational visibility report.")
    parser.add_argument("--account-slug", default="hermes", help="Account slug to inspect.")
    return parser.parse_args()


def _serialize_dt(value) -> str | None:
    return value.isoformat() if value is not None else None


def main() -> None:
    args = parse_args()
    Session = create_session_factory(load_platform_settings())
    with Session() as session:
        account = session.execute(select(Account).where(Account.slug == args.account_slug)).scalar_one()
        payload = AdminQueryService(session).ops_summary(account.id)
        print(
            json.dumps(
                {
                    "account": {"id": account.id, "slug": account.slug, "name": account.name},
                    "generated_at": payload["generated_at"],
                    "recent_failed_sync_jobs": [
                        {
                            "id": job.id,
                            "integration_id": job.integration_id,
                            "provider_name": job.provider_name,
                            "status": job.status,
                            "scheduled_at": _serialize_dt(job.scheduled_at),
                            "finished_at": _serialize_dt(job.finished_at),
                            "error_code": job.error_code,
                            "error_message": job.error_message,
                        }
                        for job in payload["recent_failed_sync_jobs"]
                    ],
                    "recent_failed_rule_runs": [
                        {
                            "id": execution.id,
                            "execution_key": execution.execution_key,
                            "status": execution.status,
                            "error_message": execution.error_message,
                            "updated_at": _serialize_dt(execution.updated_at),
                        }
                        for execution in payload["recent_failed_rule_runs"]
                    ],
                    "overdue_tasks": [
                        {
                            "id": task.id,
                            "title": task.title,
                            "priority": task.priority,
                            "status": task.status,
                            "due_at": _serialize_dt(task.due_at),
                            "escalation_level": task.escalation_level,
                            "related_entity_type": task.related_entity_type,
                            "related_entity_id": task.related_entity_id,
                        }
                        for task in payload["overdue_tasks"]
                    ],
                    "active_critical_alerts": [
                        {
                            "id": alert.id,
                            "code": alert.code,
                            "severity": alert.severity,
                            "status": alert.status,
                            "title": alert.title,
                            "last_detected_at": _serialize_dt(alert.last_detected_at),
                            "related_entity_type": alert.related_entity_type,
                            "related_entity_id": alert.related_entity_id,
                        }
                        for alert in payload["active_critical_alerts"]
                    ],
                    "integration_sync_status": [
                        {
                            "integration_id": item["integration"].id,
                            "external_ref": item["integration"].external_ref,
                            "provider_name": item["integration"].provider_name,
                            "last_sync_at": _serialize_dt(item["integration"].last_sync_at),
                            "latest_success_job_id": item["latest_success"].id if item["latest_success"] is not None else None,
                            "latest_success_finished_at": _serialize_dt(item["latest_success"].finished_at) if item["latest_success"] is not None else None,
                            "latest_failure_job_id": item["latest_failure"].id if item["latest_failure"] is not None else None,
                            "latest_failure_status": item["latest_failure"].status if item["latest_failure"] is not None else None,
                        }
                        for item in payload["integration_sync_status"]
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
