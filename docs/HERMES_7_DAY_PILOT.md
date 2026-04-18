# Hermes 7-Day Pilot Checklist

## Morning Check

1. Confirm stack is healthy:
   - `docker compose ps`
   - `curl http://127.0.0.1:${PLATFORM_PUBLIC_API_PORT:-18000}/health`
2. Run smoke check:
   - `PLATFORM_DATABASE_URL="$PLATFORM_DATABASE_URL" ./.venv/bin/python scripts/smoke_runtime.py`
3. Open Hermes dashboard and look at:
   - advertising CPL
   - incoming leads
   - lost leads
   - first response SLA breaches
   - active alerts
4. Confirm worker moved at least one tick in logs:
   - `docker compose logs --since=30m worker`

## Evening Check

1. Confirm all sync jobs are not stuck in `running` or repeated `retry`.
2. Confirm critical alerts are either acknowledged in operations or still correctly open.
3. Create backup:
   - `./scripts/backup_runtime.sh`
4. Save one-line ops note:
   - whether leads were answered
   - whether CPL spike was understood
   - whether any sync failure needs action next morning

## Mandatory Metrics

- `advertising.cpl`
- `advertising.spend`
- `leads_sales.incoming_leads`
- `leads_sales.lost_leads`
- `leads_sales.first_response_sla_breaches`

## Critical Alerts

- `lead.no_first_response`
  - critical when count > 0 for fresh inbound leads during working hours
- `marketing.cpl_above_threshold`
  - critical when high CPL persists for the current day and spend is still active
- `leads.lost_above_threshold`
  - critical when daily lost leads exceed threshold and reasons are not explained
- repeated sync failures
  - critical when the same integration remains in `retry` or `failed` after operator attention

## Escalation Triggers During Pilot

- API health endpoint fails
- worker does not progress for more than 30 minutes during business hours
- backup is not created by end of day
- Hermes dashboard stops showing advertising/leads widgets with current-day values
- PostgreSQL migration verification or smoke check fails after update
