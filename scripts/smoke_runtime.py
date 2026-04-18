from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select, text

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from platform_core.db import create_session_factory
from platform_core.models import (
    Account,
    AdMetric,
    Campaign,
    Customer,
    Integration,
    IntegrationEntityMapping,
    Lead,
    LeadEvent,
    User,
)
from platform_core.services.dashboard import ExecutiveDashboardService
from platform_core.services.runtime import RuntimeAutomationService, RuntimeIntegrationService
from platform_core.settings import load_platform_settings
from platform_core.tenancy import TenantContext


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run production-minded smoke checks against the runtime DB.")
    parser.add_argument("--account-slug", default="hermes", help="Account slug to validate.")
    parser.add_argument("--actor-email", default="owner@hermes.local", help="Hermes operator email.")
    parser.add_argument("--integration-id", type=int, default=None, help="Integration id to use for sync smoke.")
    parser.add_argument("--period", default="today", choices=("today", "yesterday", "week", "month"))
    return parser.parse_args()


def _count_entities(session, account_id: int) -> dict[str, int]:
    return {
        "campaigns": int(session.execute(select(func.count(Campaign.id)).where(Campaign.account_id == account_id)).scalar_one()),
        "ad_metrics": int(session.execute(select(func.count(AdMetric.id)).where(AdMetric.account_id == account_id)).scalar_one()),
        "customers": int(session.execute(select(func.count(Customer.id)).where(Customer.account_id == account_id)).scalar_one()),
        "leads": int(session.execute(select(func.count(Lead.id)).where(Lead.account_id == account_id)).scalar_one()),
        "lead_events": int(session.execute(select(func.count(LeadEvent.id)).where(LeadEvent.account_id == account_id)).scalar_one()),
        "mappings": int(
            session.execute(
                select(func.count(IntegrationEntityMapping.id)).where(IntegrationEntityMapping.account_id == account_id)
            ).scalar_one()
        ),
    }


def main() -> None:
    args = parse_args()
    settings = load_platform_settings()
    Session = create_session_factory(settings)
    now = datetime.now(timezone.utc)

    with Session() as session:
        session.execute(text("select 1"))
        account = session.execute(select(Account).where(Account.slug == args.account_slug)).scalar_one()
        actor = session.execute(select(User).where(User.email == args.actor_email)).scalar_one()
        integration = None
        if args.integration_id is not None:
            integration = session.get(Integration, args.integration_id)
        if integration is None:
            integration = session.execute(
                select(Integration)
                .where(Integration.account_id == account.id, Integration.status == "active")
                .order_by(Integration.id.asc())
            ).scalars().first()
        if integration is None:
            raise SystemExit(f"No active integration found for account {account.slug}.")

        context = TenantContext(account_id=account.id, actor_user_id=actor.id, source="smoke", is_system=True)

        dashboard = ExecutiveDashboardService(session).get_dashboard(context, period_code=args.period)
        widget_map = {item["widget_key"]: item["payload"] for item in dashboard["widgets"]}
        advertising_summary = widget_map["advertising"]["summary"]
        leads_sales_summary = widget_map["leads_sales"]["summary"]

        rule_results = RuntimeAutomationService(session).run_all_rules(context, now=now)
        rule_codes = sorted({item["rule_code"] for item in rule_results})

        before_counts = _count_entities(session, account.id)
        integration_service = RuntimeIntegrationService(session)
        idempotency_key = f"smoke:{account.slug}:{integration.id}:{now.strftime('%Y%m%dT%H%M%S')}"
        job, created = integration_service.enqueue_sync_job(
            context,
            integration_id=integration.id,
            idempotency_key=idempotency_key,
        )
        first_run = integration_service.execute_job(job.id, owner=f"smoke-{account.slug}", ttl_seconds=settings.runtime_lease_ttl_seconds)
        same_job, created_second = integration_service.enqueue_sync_job(
            context,
            integration_id=integration.id,
            idempotency_key=idempotency_key,
        )
        after_counts = _count_entities(session, account.id)
        session.commit()

    if first_run.status not in {"completed", "retry"}:
        raise SystemExit(f"Unexpected sync status: {first_run.status}")
    if same_job.id != job.id or created_second:
        raise SystemExit("Duplicate prevention failed: idempotency key did not reuse the same sync job.")

    print(
        json.dumps(
            {
                "database_url": settings.database_url,
                "account": {"id": account.id, "slug": account.slug, "actor_email": actor.email},
                "integration": {"id": integration.id, "provider_name": integration.provider_name, "external_ref": integration.external_ref},
                "dashboard": {
                    "advertising": advertising_summary,
                    "leads_sales": leads_sales_summary,
                },
                "rule_codes": rule_codes,
                "sync_job": {
                    "id": job.id,
                    "created": created,
                    "status": first_run.status,
                    "idempotency_reused": same_job.id == job.id and not created_second,
                },
                "counts": {"before": before_counts, "after": after_counts},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
