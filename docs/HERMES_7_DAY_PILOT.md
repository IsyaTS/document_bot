# Hermes 7-Day Pilot Execution

Цель pilot: 7 дней подряд эксплуатировать текущий Hermes runtime как реальный operator loop, не расширяя систему и не рефакторя архитектуру.

## Daily Command Set

Использовать только эти команды:

1. health
   - `curl http://127.0.0.1:${PLATFORM_PUBLIC_API_PORT:-18000}/health`
2. verify runtime DB
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/verify_runtime_db.py'`
3. smoke runtime
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/smoke_runtime.py'`
4. ops report
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/hermes_ops_report.py'`
5. backup
   - `./scripts/backup_runtime.sh`

## Daily Focus Metrics

- `advertising.spend`
- `advertising.cpl`
- `leads_sales.incoming_leads`
- `leads_sales.lost_leads`
- `leads_sales.first_response_sla_breaches`

## Day 1

- morning:
  - run `health`
  - run `verify_runtime_db`
  - run `smoke_runtime`
  - record baseline numbers in `HERMES_PILOT_ISSUES.md`
- midday:
  - run `hermes_ops_report`
  - check if `hermes-avito-main` latest sync is fresh enough
  - note first obvious false positives
- evening:
  - run `hermes_ops_report`
  - run `backup_runtime`
  - record whether baseline signals were useful or noisy

## Day 2

- morning:
  - run `health`
  - run `verify_runtime_db`
  - compare top numbers vs Day 1
- midday:
  - run `hermes_ops_report`
  - inspect critical alerts and overdue tasks
  - note whether alert wording/SLA is clear enough for action
- evening:
  - run `backup_runtime`
  - write false positives and real useful signals

## Day 3

- morning:
  - run `health`
  - run `smoke_runtime`
  - confirm dashboard freshness still matches operator reality
- midday:
  - run `hermes_ops_report`
  - focus on sync status and stale-data risk
- evening:
  - run `backup_runtime`
  - log any sync drift, retries or unexplained data gaps

## Day 4

- morning:
  - run `health`
  - run `verify_runtime_db`
  - re-check top metrics and critical alerts
- midday:
  - run `hermes_ops_report`
  - focus on alert noise vs true positives
- evening:
  - run `backup_runtime`
  - write down any threshold tuning candidates

## Day 5

- morning:
  - run `health`
  - run `smoke_runtime`
  - check same-day tasks and overdue tasks
- midday:
  - run `hermes_ops_report`
  - focus on whether tasks are operationally actionable
- evening:
  - run `backup_runtime`
  - record any task/alert disconnects

## Day 6

- morning:
  - run `health`
  - run `verify_runtime_db`
  - compare current signals with earlier days
- midday:
  - run `hermes_ops_report`
  - focus on repeated issues and unresolved noise
- evening:
  - run `backup_runtime`
  - summarize what still feels unstable

## Day 7

- morning:
  - run `health`
  - run `smoke_runtime`
  - confirm the system is still usable without manual rescue
- midday:
  - run `hermes_ops_report`
  - prepare weekly pilot review from actual notes
- evening:
  - run `backup_runtime`
  - close `HERMES_PILOT_ISSUES.md`
  - fill `HERMES_WEEKLY_REVIEW_TEMPLATE.md`

## Success Criteria

- API, worker and PostgreSQL stay healthy for the full 7-day window
- `hermes-avito-main` does not remain stuck in `failed`
- dashboard metrics remain fresh enough for daily decisions
- critical alerts are seen and acted on the same day
- backup is created every day

## Failure Criteria

- health endpoint unavailable during business hours without immediate recovery
- sync repeatedly fails and owner cannot trust dashboard data
- first response alerts are missed operationally
- daily backup skipped
- operator cannot explain why a critical alert remained open

## Allowed Changes During Pilot

- tiny bug fixes
- threshold tuning
- runbook corrections
- alert policy corrections

## Not Allowed During Pilot

- new architecture
- new providers
- Obsidian
- heavy UI
- framework refactor
