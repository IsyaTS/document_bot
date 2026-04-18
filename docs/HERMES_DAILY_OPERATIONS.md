# Hermes Daily Operations

## Morning Check

Цель: убедиться, что платформа жива, ночные/ранние sync отработали, и нет критичных дыр по лидам и деньгам.

1. Проверить инфраструктуру:
   - `docker compose ps`
   - `curl http://127.0.0.1:${PLATFORM_PUBLIC_API_PORT:-18000}/health`
2. Проверить runtime DB:
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/verify_runtime_db.py'`
3. Снять ops report:
   - `bash -lc 'set -a; source .env; set +a; ./.venv/bin/python scripts/hermes_ops_report.py'`
4. Проверить Hermes dashboard:
   - `advertising.cpl`
   - `advertising.spend`
   - `leads_sales.incoming_leads`
   - `leads_sales.lost_leads`
   - `leads_sales.first_response_sla_breaches`
5. Проверить critical alerts:
   - `lead.no_first_response`
   - `marketing.cpl_above_threshold`
   - `leads.lost_above_threshold`
   - `bank.balance_below_safe_threshold`
   - `inventory.stock_below_threshold`
   - `task.overdue_escalation`
6. Проверить same-day tasks:
   - все `high` и `critical`
   - все overdue

## Midday Sync Check

Цель: проверить, что интеграции не застряли в `retry/failed` и дневные решения принимаются на свежих данных.

1. Проверить last successful sync per integration:
   - `hermes-avito-main` обязательно
   - остальные Hermes integrations по мере включения
2. Проверить recent failed sync jobs:
   - если есть `retry`, убедиться, что причина понятна
   - если есть `failed`, это same-day action
3. Проверить worker logs за последние 2-3 часа:
   - `docker compose logs --since=3h worker`
4. При необходимости вручную trigger sync:
   - через `/api/integrations/{integration_id}/sync`
5. Перепроверить 3 метрики:
   - свежие лиды
   - SLA first response breaches
   - CPL выше порога или нет

## Evening Review

Цель: закрыть день, убедиться, что не остались неосмысленные риски на завтра.

1. Проверить open critical alerts.
2. Проверить overdue tasks и tasks с same-day SLA.
3. Проверить, что Hermes main integration не осталась в `retry/failed`.
4. Сделать backup:
   - `./scripts/backup_runtime.sh`
5. Записать короткий operator note:
   - были ли пропущенные лиды
   - был ли необъяснимый CPL spike
   - были ли sync сбои
   - что требует внимания утром

## Daily Required Jobs

- scheduler tick / worker loop должен работать весь день
- `hermes-avito-main` sync должен проходить ежедневно и фактически несколько раз в течение дня
- manual rerun требуется, если:
  - main integration stuck in `retry`
  - dashboard выглядит stale
  - operator исправил credentials / upstream issue и нужен immediate resync

## Daily Required Rule Coverage

Следующие rules должны фактически быть evaluable каждый день:

- `lead.no_first_response`
- `marketing.cpl_above_threshold`
- `leads.lost_above_threshold`
- `task.overdue_escalation`
- `bank.balance_below_safe_threshold`
- `inventory.stock_below_threshold`

## Critical Alerts

Считать критичными для daily ops:

- `lead.no_first_response`
- `marketing.cpl_above_threshold`
- `leads.lost_above_threshold`
- `bank.balance_below_safe_threshold`
- `inventory.stock_below_threshold`
- `task.overdue_escalation`

## Same-Day Reaction Tasks

Требуют реакции в тот же день:

- tasks, связанные с `lead.no_first_response`
- tasks, связанные с `marketing.cpl_above_threshold`
- любые overdue tasks с escalation
- любые tasks с `priority=critical`
