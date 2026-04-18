from __future__ import annotations


DEFAULT_RULES = [
    {
        "code": "lead.no_first_response",
        "name": "Lead without first response",
        "rule_type": "lead_sla",
        "description": "Creates an alert and sales task when a lead has no first response after the configured threshold.",
        "config": {"assignee_role_code": "owner", "severity": "warning"},
        "thresholds": {"first_response_minutes": "30"},
    },
    {
        "code": "marketing.cpl_above_threshold",
        "name": "CPL above threshold",
        "rule_type": "marketing_efficiency",
        "description": "Creates an alert, recommendation and marketing task when CPL exceeds the configured threshold.",
        "config": {"assignee_role_code": "owner", "severity": "warning"},
        "thresholds": {"max_cpl": "1200"},
    },
    {
        "code": "inventory.stock_below_threshold",
        "name": "Stock below threshold",
        "rule_type": "inventory_control",
        "description": "Creates an alert, recommendation and procurement task when available stock falls below the minimum threshold.",
        "config": {"assignee_role_code": "owner", "severity": "critical"},
        "thresholds": {},
    },
    {
        "code": "task.overdue_escalation",
        "name": "Overdue task escalation",
        "rule_type": "task_control",
        "description": "Escalates overdue tasks deterministically.",
        "config": {"severity": "warning"},
        "thresholds": {"grace_minutes": "0"},
    },
    {
        "code": "bank.balance_below_safe_threshold",
        "name": "Bank balance below safe threshold",
        "rule_type": "banking_control",
        "description": "Creates an owner alert when the latest balance falls below the configured safety threshold.",
        "config": {"assignee_role_code": "owner", "severity": "critical"},
        "thresholds": {"safe_balance": "100000"},
    },
    {
        "code": "leads.lost_above_threshold",
        "name": "Lost leads above threshold",
        "rule_type": "lead_quality",
        "description": "Creates an owner alert when the number of lost leads in the day exceeds the configured threshold.",
        "config": {"assignee_role_code": "owner", "severity": "warning"},
        "thresholds": {"max_daily_lost_leads": "5"},
    },
]
