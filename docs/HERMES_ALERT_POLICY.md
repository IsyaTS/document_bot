# Hermes Alert Policy

## Policy Matrix

| Alert code | Owner | Severity target | Response SLA | Escalation policy | Close rule |
| --- | --- | --- | --- | --- | --- |
| `bank.balance_below_safe_threshold` | Owner / finance | critical | same business hour | if still open after 2h, escalate to owner direct action | manual close after balance is restored and next snapshot confirms it |
| `inventory.stock_below_threshold` | Operations / procurement | high | same day | if item blocks active sales, escalate to critical immediately | manual close after stock is replenished or threshold is intentionally changed |
| `lead.no_first_response` | Sales operator | critical during working hours | 30 min max | if still open after 30 more min, owner checks process failure | manual close only after first response is sent and lead state updated |
| `marketing.cpl_above_threshold` | Owner / marketing | high | same day | if spend continues and CPL stays above threshold for half day, pause/adjust campaign same day | manual close after next sync shows CPL back under threshold or campaign intentionally paused |
| `leads.lost_above_threshold` | Owner / sales | high | same day | if repeated 2 days in a row, mandatory root-cause review | manual close after reasons reviewed and next day signal normalizes |
| `task.overdue_escalation` | Task assignee, then owner | high -> critical on escalation | same day | each escalation increases urgency; escalated task must be addressed same day | manual close only when task is completed/cancelled correctly |

## Practical Interpretation

### `bank.balance_below_safe_threshold`

- Считается critical, потому что это прямой cash risk.
- Auto-close не делать.
- Если alert появился без понятной причины, оператор обязан проверить latest bank sync и balances.

### `inventory.stock_below_threshold`

- Для Hermes критичность зависит от того, влияет ли позиция на текущие продажи.
- Если stock issue не влияет на активные заказы, можно держать как `high`, но не игнорировать до следующего дня.

### `lead.no_first_response`

- Это самый быстрый сигнал операционной потери денег.
- Должен иметь reaction в тот же день, обычно сразу.
- Если alert открыт в рабочее время, owner должен трактовать это как process break, а не как “обычное предупреждение”.

### `marketing.cpl_above_threshold`

- Не every spike критичен, но same-day review обязателен.
- Если spend идёт, а CPL уже выше порога, без реакции это прямой burn.

### `leads.lost_above_threshold`

- Это сигнал о просадке в качестве обработки или в качестве трафика.
- Требует не только закрытия alert, но и фиксации причин losses.

### `task.overdue_escalation`

- Это сигнал дисциплины исполнения.
- Если escalated task остаётся без движения, owner должен считать это управленческой, а не технической проблемой.

## Auto-Close vs Manual Close

Для Hermes на текущем этапе policy такая:

- auto-close: не использовать как default для critical signals
- manual close: основной режим
- допустимый operational shortcut:
  - alert можно считать resolved только если underlying data уже нормализовалась на следующем sync/rule pass
  - но само закрытие всё равно лучше делать осознанно оператором
