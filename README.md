# Document Bot

Telegram-бот для документооборота: счет, договор, коммерческое предложение, ответ на претензию, акт, транспортная заявка, акт сверки и исходящее письмо.
Отдельно добавлены сценарии под двери: КП, смета замера, договор поставки и монтажа, акт замера, акт монтажа и гарантийный талон.

## Важно по секретам

Не храните токены и пароли в коде. Токен Telegram и данные МойСклад, отправленные в чат, нужно перевыпустить или сменить, затем положить новые значения только в локальный файл `.env`.

## Что уже есть

- Реквизиты ИП Батршин Айдар Мавлютович и ООО "Вектор плюс" в `data/companies.json`.
- Генерация `.docx` документов.
- Telegram-меню для выбора типа документа и организации.
- История созданных документов в SQLite.
- Быстрый мастер заполнения для основных документов.
- Полный ввод одним сообщением для тех, кому так быстрее.
- Подстановка данных из прошлого документа.
- Сохраненные контрагенты с повторным использованием.
- Опциональный поиск контрагента в МойСклад через `/moysklad`.
- Опциональная генерация делового текста через OpenAI для КП, претензий и писем.
- PDF-документы по умолчанию, DOCX сохраняется для правки.
- Счета с QR-кодом для оплаты по реквизитам организации.
- Каталог дверей и услуг в `data/door_catalog.json`.
- Поиск товара по неполному названию и автоподстановка цены из каталога.
- Изменение цены товара/услуги прямо из Telegram.
- Повтор старого КП с изменениями.
- Скачивание ранее созданных документов из истории.
- Быстрое редактирование готового документа: клиент, адрес, цена, добавление позиции.

## Запуск

```bash
cd /opt/aidar/document_bot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env
python main.py
```

Фоновый запуск через `tmux`:

```bash
cd /opt/aidar/document_bot
bash scripts/start_bot.sh
bash scripts/status_bot.sh
bash scripts/stop_bot.sh
```

## Работа в группе

Добавьте бота в группу и используйте команды:

```text
/menu
/id
/counterparties
/catalog стандарт
/product монтаж
/moysklad ООО Ромашка
```

Если бот молчит на обычные сообщения в группе, это нормально при включенном Telegram Privacy Mode. В этом режиме бот видит команды и упоминания. Пишите `/menu`, `/id` или упоминайте `@document07_bot`. Для реакции на любые сообщения группы нужно отключить Privacy Mode у бота через BotFather.

В `.env` укажите свежий токен:

```bash
TELEGRAM_BOT_TOKEN=новый_токен_telegram
OPENAI_API_KEY=ключ_openai_если_нужен
MOYSKLAD_LOGIN=логин_если_нужен
MOYSKLAD_PASSWORD=пароль_если_нужен
```

## Режимы заполнения

После выбора типа документа и организации бот предлагает:

- `Быстрый мастер` - короткие вопросы по шагам.
- `Полный ввод` - одно сообщение с данными.
- `Из прошлого документа` - подстановка последнего документа и ввод только изменений.
- `Из сохраненного контрагента` - подстановка клиента, ИНН, телефона и адреса.

Бот теперь сам сохраняет контрагентов из созданных документов. Их можно открыть через кнопку `Контрагенты` или команду `/counterparties`.
В разделе `Контрагенты` есть ручное добавление и импорт из `.txt`, `.csv` и `.xlsx`.

## Формат данных для полного ввода

Отправьте боту одним сообщением:

```text
Клиент: ООО Ромашка
ИНН: 1234567890
КПП: 123401001
Адрес: г. Уфа, ул. Примерная, 1
Предмет: перевозка груза по маршруту Уфа - Казань
Маршрут: Уфа - Казань
Адрес погрузки: г. Уфа, ул. Складская, 1
Адрес выгрузки: г. Казань, ул. Получателя, 10
Позиции: Перевозка груза | 1 | рейс | 55000
Сумма: 55000
НДС: Без НДС
Срок: до 5 рабочих дней
Оплата: 100% предоплата
Номер: 15
```

Позиции можно перечислять через `;`:

```text
Позиции: Перевозка | 1 | рейс | 55000; Погрузка | 2 | час | 3000
```

Для дверей можно писать короче, если позиция есть в каталоге:

```text
Клиент: Иванов Иван
Телефон: +7 900 000-00-00
Адрес объекта: г. Уфа, ул. Ленина, 5
Модель: входная дверь стандарт
Размер проема: 960 x 2050
Позиции: стандарт | 2; доставка | 1
Скидка: 5%
Оплата: 70% предоплата, 30% перед монтажом
Номер: 15
```

Бот найдет `стандарт` и `доставка` в каталоге и подставит цены. Стандартный монтаж можно не писать отдельной строкой, если он уже включен в стоимость двери. Если нужно вручную поменять цену только в конкретном документе, пишите полную позицию:

```text
Позиции: Входная дверь стандарт | 1 | шт. | 44500
```

Изменить цену в каталоге можно через кнопку `Каталог и цены` -> `Изменить цену`.

Для основных документов больше не нужен полный набор реквизитов. Достаточно:

- `КП`: клиент, адрес объекта, шаблон/модель, размер, позиции.
- `Смета`: клиент, адрес объекта, размер, позиции.
- `Акт`: клиент, адрес, позиции.
- `Счет`: клиент, позиции.

Остальные поля опциональны.

## Проверка генерации без Telegram

```bash
python scripts/generate_sample.py
```

Документы появятся в папке `generated`.

## Platform Runtime State

В проекте уже есть не только core foundation, но и рабочий platform runtime слой.
Telegram-бот продолжает жить как legacy adapter, а platform core/runtime развивается рядом и не ломает исходный сценарий.

### Implemented stages

- stage 2: multi-tenant core
- stage 3: business data foundation
- stage 4: integrations abstraction
- stage 5: rules + tasks loop
- stage 6: executive dashboard query layer
- stage 6.5: runtime surface + scheduler foundation
- provider hardening foundation
- stage 8.5: knowledge base and document intake foundation
- stage 8.6: people execution and KPI foundation
- stage 8.7: procurement, logistics and documents foundation
- stage 8.8: communication intelligence and owner guidance foundation
- stage 7.8: productization layer
- stage 7.9: execution brief and account actions
- stage 8.0: delivery layer and product polish
- stage 8.1: daily delivery and internal digest generation

### What is already working

- multi-tenant account/user/RBAC model
- canonical business tables for customers, leads, lead_events, deals, campaigns, ad_metrics, stock, purchases, banking and tasks
- integration layer with `integrations`, `integration_credentials`, `provider_tokens`, `sync_jobs`, `integration_logs`
- runtime API in `platform_runtime/app.py`
- scheduler/worker loop in `platform_runtime/worker.py`
- dashboard widgets: money, financial_result, leads_sales, advertising, stock, management, owner_panel
- rules engine with alerts, tasks, recommendations and deterministic dedupe
- `integration_entity_mappings`
- real sync execution path in `RuntimeIntegrationService.execute_job`

### Current provider status

- `generic_bank`: writes into canonical bank tables
- `moysklad`: writes into canonical business tables
- `avito`: writes into canonical ads/leads tables and affects dashboard + automation
- `telegram`, `whatsapp`, `google_sheets`: contracts exist, runtime implementation is not done yet

### Authoritative runtime DB

Для Hermes canonical production runtime target: PostgreSQL.

- host DSN after cutover: `postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@127.0.0.1:5433/hermes_platform`
- internal compose DSN: `postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@postgres:5432/hermes_platform`
- `data/platform.sqlite3` после cutover следует держать только как migration fallback / cold snapshot

Сейчас это canonical runtime DB для локального запуска. Она должна использоваться по умолчанию через:

```bash
PLATFORM_DATABASE_URL=postgresql+psycopg://hermes:${POSTGRES_PASSWORD}@127.0.0.1:5433/hermes_platform
```

Почему так:

- PostgreSQL является canonical runtime target после hardening cutover
- SQLite snapshots остаются как fallback / historical checkpoints
- acceptance snapshot больше не должен быть default runtime target

### Snapshot SQLite files

- `data/platform.sqlite3`: legacy SQLite snapshot / fallback
- `data/platform_avito_acceptance_v2.sqlite3`: latest Avito acceptance snapshot before promotion
- `data/platform_provider_acceptance.sqlite3`: provider-hardening acceptance snapshot
- `data/platform_stage3_acceptance.sqlite3`
- `data/platform_stage4_acceptance.sqlite3`
- `data/platform_stage5_acceptance.sqlite3`
- `data/platform_stage6_acceptance.sqlite3`
- `data/platform_stage65_acceptance.sqlite3`
- `data/platform_acceptance.sqlite3`: early core-only acceptance snapshot
- `data/platform_avito_acceptance_20260418.sqlite3`: промежуточный Avito файл, можно считать disposable snapshot

Эти acceptance-файлы нужны как исторические checkpoints. Их не нужно использовать как основной runtime target.

### Hermes Production Hardening

Для stage 6.8 добавлены production-minded assets без нового framework-рефакторинга:

- `Dockerfile`
- `docker-compose.yml`
- [docs/HERMES_RUNBOOK.md](/opt/aidar/document_bot/docs/HERMES_RUNBOOK.md)
- [docs/HERMES_7_DAY_PILOT.md](/opt/aidar/document_bot/docs/HERMES_7_DAY_PILOT.md)
- [docs/HERMES_PILOT_ISSUES.md](/opt/aidar/document_bot/docs/HERMES_PILOT_ISSUES.md)
- [docs/HERMES_DAILY_OPERATIONS.md](/opt/aidar/document_bot/docs/HERMES_DAILY_OPERATIONS.md)
- [docs/HERMES_ALERT_POLICY.md](/opt/aidar/document_bot/docs/HERMES_ALERT_POLICY.md)
- [docs/HERMES_WEEKLY_REVIEW_TEMPLATE.md](/opt/aidar/document_bot/docs/HERMES_WEEKLY_REVIEW_TEMPLATE.md)
- `scripts/wait_for_db.py`
- `scripts/migrate_sqlite_to_postgres.py`
- `scripts/verify_runtime_db.py`
- `scripts/smoke_runtime.py`
- `scripts/hermes_ops_report.py`
- `scripts/backup_runtime.sh`
- `scripts/restore_runtime.sh`
- `scripts/start_runtime_api.sh`
- `scripts/start_runtime_worker.sh`

Короткий operational flow:

1. `docker compose up -d postgres`
2. `alembic upgrade head` against PostgreSQL
3. `python scripts/migrate_sqlite_to_postgres.py ...`
4. switch `PLATFORM_DATABASE_URL` to PostgreSQL
5. `docker compose up -d api worker`
6. `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/verify_runtime_db.py'`
7. `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/smoke_runtime.py'`

### Admin App Product Surface

Server-rendered admin app lives in `platform_runtime/app.py` and now includes:

- global pages:
  - `/admin/portfolio`
  - `/admin/accounts`
  - `/admin/users`
  - `/admin/platform`
  - `/admin/super-admin`
- account-scoped pages:
  - `/admin/{account_slug}/dashboard`
  - `/admin/{account_slug}/brief`
  - `/admin/{account_slug}/delivery`
  - `/admin/{account_slug}/knowledge`
  - `/admin/{account_slug}/people`
  - `/admin/{account_slug}/integrations`
  - `/admin/{account_slug}/alerts-tasks`
  - `/admin/{account_slug}/ops-sync`
  - `/admin/{account_slug}/goals`
  - `/admin/{account_slug}/members`
  - `/admin/{account_slug}/settings`

Stage 7.8 adds:

- account settings for name/timezone/currency/branding/defaults
- account status / plan type / feature flags / soft limits foundation
- platform runtime visibility for app version, revision, DB, worker, backup, verify and smoke status
- hand-off-ready readiness blocks and next-step hints
- runtime status files in `data/runtime_status/`

Stage 7.9 adds:

- account-level execution brief page and JSON digest:
  - `GET /admin/{account_slug}/brief`
  - `GET /admin/{account_slug}/brief.json`
- account-level owner/operator execution defaults derived from account settings and memberships
- account-scoped execution actions:
  - `POST /admin/{account_slug}/alerts/{alert_id}/status`
  - `POST /admin/{account_slug}/alerts/{alert_id}/assign-default`
  - `POST /admin/{account_slug}/tasks/{task_id}/status`
  - `POST /admin/{account_slug}/tasks/{task_id}/assign-default`
- account drilldown links from dashboard, goals, alerts/tasks and ops pages into the execution brief
- execution-oriented account summary built from existing dashboard, ops, alerts/tasks and goals data, without new runtime framework

Stage 8.0 adds:

- account delivery and hand-off surface:
  - `GET /admin/{account_slug}/delivery`
  - `GET /admin/{account_slug}/delivery.json`
  - `GET /admin/{account_slug}/delivery.md`
  - `GET /admin/{account_slug}/delivery.txt`
- delivery pack derived from existing settings, readiness, brief, goals and ops data
- hand-off visibility for:
  - what is configured now
  - what still needs setup
  - health problems
  - integrations needing attention
  - owner actions
  - operator checklist
- product polish across the admin app:
  - cleaner active account context in sidebar
  - stronger empty-state hints
  - delivery links across dashboard, goals, integrations, alerts/tasks, ops, accounts and portfolio

Stage 8.3 adds:

- platform-level super-admin console:
  - `GET /admin/super-admin`
  - all accessible accounts with status, plan, readiness, sync health, owner/admin visibility, feature summary, soft-limit pressure and top issues
- platform-level lifecycle and commercial control actions:
  - disable / reactivate / archive account via status updates
  - change plan type
  - adjust soft limits
  - toggle feature flags
- minimal real feature enforcement:
  - `portfolio_console`
  - `owner_briefs`
  - `goals_tracking`
  - `integrations_setup`
  - `ops_console`
- clear blocked messages in handlers when a feature is outside plan, disabled for the account, or unavailable due to account status
- platform audit events:
  - `platform.account.status_changed`
  - `platform.account.plan_changed`
  - `platform.account.feature_flags_changed`
  - `platform.account.soft_limits_changed`

Stage 8.4 adds:

- thin Obsidian bridge on top of existing delivery/brief markdown
- account export from UI:
  - `POST /admin/{account_slug}/delivery/generate` with `export_obsidian=true`
- portfolio export from UI:
  - `POST /admin/portfolio/brief/generate` with `export_obsidian=true`
- internal/CLI generation also supports Obsidian export:
  - `python scripts/generate_runtime_delivery.py --account-slug hermes --actor-email owner@hermes.local --obsidian`
  - `python scripts/generate_runtime_delivery.py --portfolio --actor-email portfolio.owner@platform.local --obsidian`
- exported notes are written into a vault-compatible directory:
  - default: `data/runtime_obsidian_vault/Hermes Platform/`
  - account delivery notes: `Accounts/<slug>/`
  - portfolio notes: `Portfolio/`
- `Platform` page now shows the latest Obsidian export status

Obsidian operational note:

- runtime source of truth remains PostgreSQL + runtime API
- Obsidian receives markdown exports only
- easiest setup is to open `data/runtime_obsidian_vault/` as an Obsidian vault or link that folder into an existing vault
- config knobs:
  - `PLATFORM_OBSIDIAN_VAULT_PATH`
  - `PLATFORM_OBSIDIAN_EXPORT_SUBDIR`

Stage 8.5 adds:

- account-scoped knowledge base:
  - `GET /admin/{account_slug}/knowledge`
  - `POST /admin/{account_slug}/knowledge/save`
  - `POST /admin/{account_slug}/knowledge/{item_id}/status`
  - `GET /admin/{account_slug}/knowledge/{item_id}/download`
- canonical knowledge storage in `knowledge_items` with:
  - notes
  - SOPs / policies
  - customer notes
  - uploaded files
  - tags and linked customer / deal context
- read-only visibility for viewers and write flow for roles with `documents.manage`
- product-layer integration:
  - `knowledge_base` feature flag
  - `active_knowledge_items` soft-limit visibility
  - readiness next-step hint when account has no knowledge seeded
- uploaded files are stored under `data/runtime_knowledge_uploads/`

Stage 8.6 adds:

- account-scoped people execution page:
  - `GET /admin/{account_slug}/people`
  - `POST /admin/{account_slug}/people/employee/save`
  - `POST /admin/{account_slug}/people/tasks/create`
- employee registry on top of the existing `employees` table
- KPI snapshots derived from existing tasks and alerts:
  - open tasks
  - overdue tasks
  - completed 7d / 30d
  - average completion hours
  - open alerts pressure
- direct manual task creation for employees from the people console
- product-layer integration:
  - `people_execution` feature flag
  - `active_employees` soft-limit visibility
  - readiness hint when the account has no employees configured

Stage 8.7 adds:

- account-scoped operations page:
  - `GET /admin/{account_slug}/operations`
  - `POST /admin/{account_slug}/operations/warehouse/save`
  - `POST /admin/{account_slug}/operations/product/save`
  - `POST /admin/{account_slug}/operations/purchases/save`
  - `POST /admin/{account_slug}/operations/purchases/{purchase_id}/receive`
  - `POST /admin/{account_slug}/operations/documents/save`
  - `POST /admin/{account_slug}/operations/installations/save`
- minimal procurement and logistics foundation:
  - warehouse setup
  - product setup
  - purchase requests
  - stock receiving into canonical `stock_items` and `stock_movements`
  - installation requests
  - stagnant stock visibility
- document workflow foundation on top of canonical `documents`
- product-layer integration:
  - `operations_workflows` feature flag
  - `active_documents`, `open_installation_requests`, `open_purchase_requests` soft-limit visibility
  - readiness hint when the account has no product/warehouse setup

Stage 8.8 adds:

- account-scoped communications page:
  - `GET /admin/{account_slug}/communications`
  - `POST /admin/{account_slug}/communications/reviews/save`
  - `POST /admin/{account_slug}/communications/reviews/{review_id}/task`
- transcript review foundation for:
  - messages
  - calls
  - chats
  - email transcripts
- heuristic communication analysis tied to account data:
  - quality status
  - sentiment
  - follow-up required / clear
  - response-delay risk
  - lead first-response risk linkage
  - owner guidance recommendations
- direct follow-up task creation from communication reviews
- product-layer integration:
  - `communication_intelligence` feature flag
  - `communication_reviews` soft-limit visibility
  - readiness hint when the account has no communication reviews yet

Stage 8.1 adds:

- generated delivery snapshots for accounts and portfolio
- internal-token-protected report generation routes:
  - `POST /internal/reports/accounts/{account_slug}/delivery`
  - `POST /internal/reports/portfolio/brief`
- session-authenticated snapshot generation routes:
  - `POST /admin/{account_slug}/delivery/generate`
  - `POST /admin/portfolio/brief/generate`
- export routes for portfolio brief:
  - `GET /admin/portfolio/brief.md`
  - `GET /admin/portfolio/brief.txt`
- runtime delivery artifacts written to `data/runtime_delivery/`
- last delivery generation status recorded in `data/runtime_status/delivery_status.json`
- CLI generator:
  - `python scripts/generate_runtime_delivery.py --account-slug hermes --actor-email owner@hermes.local`
  - `python scripts/generate_runtime_delivery.py --portfolio --actor-email portfolio.owner@platform.local`

For stage 6.9 operational hardening:

- Hermes daily operator checklist is documented
- alert ownership/SLA matrix is documented
- lightweight ops visibility is available through:
  - `GET /api/admin/accounts/{account_id}/ops-summary`
  - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/hermes_ops_report.py'`

### Hermes Admin App

Current admin surface is a lightweight server-rendered FastAPI app, not a separate frontend stack.

Main pages:

- `/admin/login`
- `/admin`
- `/admin/portfolio`
- `/admin/accounts`
- `/admin/users`
- `/admin/{account_slug}/dashboard`
- `/admin/{account_slug}/brief`
- `/admin/{account_slug}/delivery`
- `/admin/{account_slug}/members`
- `/admin/{account_slug}/integrations`
- `/admin/{account_slug}/alerts-tasks`
- `/admin/{account_slug}/ops-sync`
- `/admin/{account_slug}/goals`

Current access flow:

- login with existing user email + password
- session cookie `hermes_admin_session`
- account chooser for users with more than one active membership
- explicit account switch in the sidebar
- logout via CSRF-protected `POST /admin/logout`
- bootstrap path for initial password / recovery:
  - `POST /admin/bootstrap-access`
  - one-time password claim page `/admin/password/claim?token=...`

Current auth hardening:

- password hashing is stored on the `users` table
- failed password attempts increment per user
- temporary lockout after repeated failures
- session auth version is checked against the user record, so password changes and user disable actions invalidate older sessions
- login, failed login, bootstrap reset issue, password claim and logout are audit-logged per accessible account membership

Current CSRF coverage:

- authenticated admin state-changing routes are protected by session CSRF token checks
- admin forms use hidden `csrf_token`
- admin JS actions send `X-CSRF-Token`
- covered flows include:
  - account create
  - user create/invite/reset/status
  - membership save/disable/remove
  - integration save/test/status/sync
  - goal save
  - logout

Current UI access behavior:

- `owner`: cross-account portfolio view at `/admin/portfolio`
- `owner` / `admin`: full dashboard, integrations, ops and goal management
- `owner` / `admin`: account onboarding page and membership management UI
- `owner` / `admin`: global user lifecycle page with invite, reset and disable flows
- `viewer`: dashboard + goals read-only + alerts/tasks visibility, but no integrations or ops management
- server-side permission checks remain enforced even if a restricted URL is opened directly

Current portfolio behavior:

- portfolio page is derived at read time from existing dashboard, goals and ops data
- visible only to users who have active `owner` memberships
- aggregates only the accounts where the actor is an `owner`
- shows:
  - health per account
  - available cash
  - revenue
  - net profit
  - incoming leads
  - critical alerts
  - overdue tasks
  - sync health
  - goals at risk
- includes portfolio-level rankings for:
  - highest risk accounts
  - broken sync accounts
  - critical goal deviations
  - alert/task pressure

Current owner action behavior:

- portfolio drilldown links go directly to:
  - account dashboard
  - filtered critical alerts
  - filtered overdue tasks
  - filtered broken sync view
  - goal deviations view
- owner-only portfolio actions:
  - `POST /admin/portfolio/accounts/{account_slug}/sync`
  - `POST /admin/portfolio/accounts/{account_slug}/alerts/{alert_id}/status`
  - `POST /admin/portfolio/accounts/{account_slug}/tasks/{task_id}/status`
- generated portfolio brief endpoint:
  - `GET /admin/portfolio/brief`
- portfolio brief includes:
  - daily brief
  - critical alerts digest
  - failed sync digest
  - goals at risk digest

Current account execution behavior:

- account execution brief is available at:
  - `GET /admin/{account_slug}/brief`
  - `GET /admin/{account_slug}/brief.json`
- account brief shows:
  - critical alerts
  - overdue tasks
  - failed sync jobs
  - goals at risk
  - must-do-now list
  - default owner and operator for execution routing
- account actions available from brief and alerts/tasks views:
  - acknowledge / dismiss / reopen alert
  - assign alert to default owner or operator
  - mark task open / done
  - assign task to default owner or operator
- account execution summary is derived from existing ops, goals and dashboard data and is intended to answer:
  - what matters now
  - who should react
  - where to go next

Current delivery behavior:

- account delivery page is available at:
  - `GET /admin/{account_slug}/delivery`
  - `GET /admin/{account_slug}/delivery.json`
  - `GET /admin/{account_slug}/delivery.md`
- `GET /admin/{account_slug}/delivery.txt`
- delivery page is intended for owner/admin hand-off and daily clarity
- delivery pack answers:
  - what is already configured
  - what is still missing
  - what is unhealthy right now
  - which integrations need attention
  - what the owner should do next
  - what the operator should work through next

Current delivery generation behavior:

- account delivery page can generate a saved snapshot from the UI
- portfolio page can generate a saved owner brief snapshot from the UI
- internal automation can generate the same snapshots through `PLATFORM_INTERNAL_API_TOKEN`
- generated artifacts are saved as `json`, `md` and `txt`
- platform visibility now shows the latest delivery snapshot status

Current account onboarding and lifecycle behavior:

- create a new account from `/admin/accounts`
- assign initial `owner` and optional `admin`
- creator is auto-added as `admin` when needed so the new account stays manageable through UI
- track onboarding completion by steps:
  - account created
  - owner assigned
  - admin or operator added
  - goal configured
  - integration configured
  - first sync completed
- manage members from `/admin/{account_slug}/members`
- integration lifecycle from `/admin/{account_slug}/integrations` now includes:
  - enable / disable / archive
  - credential rotate via merge mode
  - clear + replace via replace mode
  - recent sync job history and last rotation visibility

### Avito provider contract

Поддерживаемый concrete Avito live contract в текущем runtime:

- required credentials: `access_token`, `account_external_id`
- optional transport settings: `base_url`, `timeout_seconds`, `max_retries`, `backoff_seconds`
- optional query params: `campaigns_params`, `metrics_params`, `leads_params`
- optional source feed settings: `lead_source_feed_path`, `lead_source_feed_items_key`, `lead_source_feed_cursor_param`, `lead_source_feed_params`
- fixture mode: `fixture_payload`
- optional source/conversation enrichment payloads: `fixture_payload.lead_source_feed`, `lead_sources`

Поддерживаемые Avito fetch paths:

- campaigns: `/messaging/v1/accounts/{account_external_id}/campaigns`
- ad metrics: `/messaging/v1/accounts/{account_external_id}/campaigns/stats`
- leads: `/messaging/v1/accounts/{account_external_id}/leads`
- optional source/conversation feed: credential-configured `lead_source_feed_path` or fixture `lead_source_feed`

Canonical note:

- `Lead.source` остается canonical provider source (`avito`)
- channel/source detail, `source_status`, conversation timestamps and close/lost signals пишутся в lead metadata, customer notes, lead events и integration mappings, а не в отдельную схему

### Avito sync scope

Реально синкаются только эти сущности:

- `campaigns`
- `ad_metrics`
- `customers`
- `leads`
- `lead_events`
- `integration_entity_mappings`

Обязательные normalized records:

- `AdsCampaignRecord`: `external_id`, `source`, `name`, `status`
- `AdsMetricsRecord`: `campaign_external_id`, `metric_date`, `impressions`, `clicks`, `spend`
- `AdsLeadRecord`: `external_id`, `title`, `created_at`

Опциональные normalized fields:

- campaigns: `started_at`, `ended_at`, `budget_amount`, `currency`, `metadata`
- metrics: `leads_count`, `conversions_count`, `metadata`
- leads: `status`, `pipeline_stage`, `contact_name`, `phone`, `email`, `campaign_external_id`, `customer_external_id`, `first_response_due_at`, `first_responded_at`, `lost_reason`, `metadata`

Current field mapping hardening:

- campaigns fallbacks: `external_id|campaign_id|campaignId|id|itemId`, `name|title|campaign_name|ad_name|item_title`, `status|state|campaign_status|ad_status`, `started_at|start_date|startDate|created_at`, `ended_at|end_date|endDate|finished_at`, `budget_amount|budget|budget.limit|daily_budget`
- ad metrics fallbacks: `campaign_external_id|campaign_id|campaignId|id|itemId`, `metric_date|date|stats_date|day`, `impressions|views`, `clicks|contacts`, `spend|spent|cost`, `leads_count|contacts|leads|uniq_contacts`, `conversions_count|conversions|orders`
- leads fallbacks: `external_id|lead_id|leadId|id`, `title|ad_title|subject|item_title|campaign_name`, `created_at|createdAt|published_at|created|conversation.created_at`, `status|lead_status|source_status|state`, `pipeline_stage|stage|status`, `campaign_external_id|campaign_id|campaignId|item_id|itemId|ad_id`, `customer_external_id|contact_id|customer_id|customer.id|contact.id|user_id`
- contact fallbacks: direct `contact_name/phone/email` or nested `contact.*` / `customer.*`
- source feed fallbacks: `source|source_name|channel|origin`, `source_status|conversation_status|status`, `conversation_external_id|conversation_id|chat_id|dialog_id|conversation.id`, `conversation_created_at|conversation_started_at|first_message_at|conversation.created_at`, `last_message_at|updated_at|last_activity_at|conversation.last_message_at|last_incoming_message_at`, `closed_at|closedAt|lost_at|conversation.closed_at`, `lost_reason|close_reason|decline_reason`

### Avito cursor strategy

Runtime использует persisted section checkpoints в `sync_jobs.cursor_json` для `campaigns`, `ad_metrics` и `leads`. Для `leads` дополнительно хранится nested source feed state.

Для каждого section сохраняются:

- `status`: `running` или `completed`
- `next_cursor`
- `record_count`
- `exhausted`
- `stats`
- `window`
- `checkpoint_at`

Общее правило:

- `campaigns`: cursor нужен для безопасного resume/skip внутри текущего sync job
- `ad_metrics`: cursor привязан к зафиксированному sync window `date_from/date_to`
- `leads`: cursor привязан к тому же sync window и после enrichment пишет в `customers`, `leads`, `lead_events`
- `leads.source_feed`: nested cursor/state для optional source/conversation ingestion path

При retry того же job:

- уже `completed` sections не запускаются повторно
- незавершенный section продолжает работу в рамках сохраненного `window`
- nested source feed state сохраняется внутри `leads` checkpoint как `source_feed`
- canonical upsert/mapping path не допускает row explosion при повторном прогоне

### Failure and retry model

- partial failure после completed `campaigns`/`ad_metrics` оставляет их checkpoints в `sync_jobs.cursor_json`
- partial failure в source feed оставляет `leads.status=running`, а `campaigns`/`ad_metrics` остаются `completed`
- retry переводит job обратно в `running`, сохраняет `attempts_count`, но не пересинхронизирует уже completed sections
- duplicate prevention держится на canonical lookup + upsert + `integration_entity_mappings`
- lead event duplicate prevention идет не только через integration mapping, но и через canonical fallback `lead_id + event_type + event_at`
- account isolation обеспечивается `account_id` во всех canonical и integration таблицах

Это уже проверено на локальной runtime DB:

- simulated partial failure в Avito source feed перевёл job в `retry`
- повторный запуск завершил тот же job без повторного вызова `campaigns` и `ad_metrics`
- repeated rerun не меняет counts в `campaigns`, `ad_metrics`, `customers`, `leads`, а `lead_events` остаются идемпотентными после dedupe hardening
- counts по account `2` не изменились при sync integration account `1`

### Local platform commands

Поднять локальный runtime:

```bash
cd /opt/aidar/document_bot
source .venv/bin/activate
alembic upgrade head
python scripts/run_platform_api.py
```

Запустить один проход worker/scheduler:

```bash
python scripts/run_platform_runtime_worker.py
```

Создать первый аккаунт и администратора:

```bash
python scripts/bootstrap_platform_core.py \
  --account-slug hermes \
  --account-name "Hermes" \
  --admin-email owner@hermes.local \
  --admin-full-name "Hermes Owner"
```

### Current focus

Следующий рабочий приоритет: Avito hardening поверх уже существующего provider path.
Obsidian layer и heavy frontend пока не приоритетны.
