# Hermes Runbook

## Canonical Runtime Target

- production runtime DB: PostgreSQL
- local compose PostgreSQL endpoint from host: `postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@127.0.0.1:5433/hermes_platform`
- internal compose PostgreSQL endpoint from containers: `postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@postgres:5432/hermes_platform`
- host API bind in compose: `http://127.0.0.1:${PLATFORM_PUBLIC_API_PORT:-18000}`
- legacy SQLite file `data/platform.sqlite3` should be kept only as migration fallback / cold snapshot after PostgreSQL cutover

## First-Time PostgreSQL Cutover

1. Copy `.env.example` to `.env` and set real values for:
   - `POSTGRES_PASSWORD`
   - `PLATFORM_SECRET_KEY`
   - `PLATFORM_INTERNAL_API_TOKEN`
   - `PLATFORM_CREDENTIALS_KEY` if you want a dedicated credentials key
2. Start PostgreSQL:
   - `docker compose up -d postgres`
3. Wait for DB:
   - `./.venv/bin/python scripts/wait_for_db.py --database-url "postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@127.0.0.1:5433/hermes_platform" --timeout 60`
4. Apply schema to PostgreSQL:
   - `PLATFORM_DATABASE_URL="postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@127.0.0.1:5433/hermes_platform" ./.venv/bin/alembic upgrade head`
5. Migrate current Hermes state from SQLite:
   - `./.venv/bin/python scripts/migrate_sqlite_to_postgres.py --source-url "sqlite+pysqlite:////opt/aidar/document_bot/data/platform.sqlite3" --target-url "postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@127.0.0.1:5433/hermes_platform"`
6. Verify migrated runtime DB:
   - `PLATFORM_DATABASE_URL="postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@127.0.0.1:5433/hermes_platform" ./.venv/bin/python scripts/verify_runtime_db.py`
7. Switch `.env` so `PLATFORM_DATABASE_URL` points to PostgreSQL.
8. Start full runtime:
   - `docker compose up -d api worker`

## Daily Start / Stop

- start all runtime services: `docker compose up -d`
- restart only runtime app: `docker compose restart api worker`
- stop runtime stack: `docker compose down`
- read API logs: `docker compose logs -f api`
- read worker logs: `docker compose logs -f worker`

## Health and Verification

- API health: `curl http://127.0.0.1:${PLATFORM_PUBLIC_API_PORT:-18000}/health`
- runtime DB verification: `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/verify_runtime_db.py'`
- smoke checks: `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/smoke_runtime.py'`
- ops report: `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/hermes_ops_report.py'`

## Daily Operations

- daily checklist: [HERMES_DAILY_OPERATIONS.md](/opt/aidar/document_bot/docs/HERMES_DAILY_OPERATIONS.md)
- alert ownership/SLA matrix: [HERMES_ALERT_POLICY.md](/opt/aidar/document_bot/docs/HERMES_ALERT_POLICY.md)
- pilot checklist: [HERMES_7_DAY_PILOT.md](/opt/aidar/document_bot/docs/HERMES_7_DAY_PILOT.md)
- pilot issues log: [HERMES_PILOT_ISSUES.md](/opt/aidar/document_bot/docs/HERMES_PILOT_ISSUES.md)
- weekly review template: [HERMES_WEEKLY_REVIEW_TEMPLATE.md](/opt/aidar/document_bot/docs/HERMES_WEEKLY_REVIEW_TEMPLATE.md)

## Backup

- create PostgreSQL backup: `./scripts/backup_runtime.sh`
- output is written to `backups/runtime/runtime_<timestamp>.sql.gz`
- keep at least:
  - latest 7 daily backups
  - 4 weekly backups

## Restore

1. Stop `api` and `worker`:
   - `docker compose stop api worker`
2. Restore backup:
   - `./scripts/restore_runtime.sh backups/runtime/runtime_<timestamp>.sql.gz`
3. Re-run verification:
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/verify_runtime_db.py'`
4. Start runtime again:
   - `docker compose up -d api worker`

## Rollback Flow

1. Stop `api` and `worker`:
   - `docker compose stop api worker`
2. Restore the latest known-good backup:
   - `./scripts/restore_runtime.sh backups/runtime/runtime_<timestamp>.sql.gz`
3. Re-verify DB:
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/verify_runtime_db.py'`
4. Restart runtime:
   - `docker compose up -d api worker`
5. Re-run smoke:
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/smoke_runtime.py'`

## Hermes Runtime Checks

- expected account slug: `hermes`
- expected operator email: `owner@hermes.local`
- expected main integration ref: `hermes-avito-main`
- expected active rule signals for current acceptance state:
  - `lead.no_first_response`
  - `marketing.cpl_above_threshold`
  - `leads.lost_above_threshold`

## Safe Update Sequence

1. `./scripts/backup_runtime.sh`
2. `docker compose pull` if you use prebuilt images, or rebuild locally:
   - `docker compose build`
3. Apply schema:
   - `docker compose run --rm api alembic upgrade head`
4. Restart runtime:
   - `docker compose up -d api worker`
5. Run smoke checks:
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/smoke_runtime.py'`
6. Run ops report:
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/hermes_ops_report.py'`
7. If smoke or ops report looks wrong, use rollback flow immediately
