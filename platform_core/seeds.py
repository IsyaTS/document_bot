from __future__ import annotations


PERMISSION_DEFINITIONS = [
    {"code": "accounts.manage", "name": "Manage accounts", "module": "core"},
    {"code": "users.manage", "name": "Manage users", "module": "core"},
    {"code": "memberships.manage", "name": "Manage account memberships", "module": "core"},
    {"code": "audit.read", "name": "Read audit logs", "module": "core"},
    {"code": "dashboard.read", "name": "Read executive dashboard", "module": "dashboard"},
    {"code": "business.read", "name": "Read business data", "module": "business"},
    {"code": "business.write", "name": "Write business data", "module": "business"},
    {"code": "documents.manage", "name": "Manage documents", "module": "business"},
    {"code": "tasks.read", "name": "Read tasks", "module": "automation"},
    {"code": "tasks.manage", "name": "Manage tasks", "module": "automation"},
    {"code": "alerts.read", "name": "Read alerts", "module": "automation"},
    {"code": "rules.manage", "name": "Manage rules", "module": "automation"},
    {"code": "integrations.manage", "name": "Manage integrations", "module": "integrations"},
    {"code": "banking.read", "name": "Read banking data", "module": "banking"},
    {"code": "banking.manage", "name": "Manage banking connections", "module": "banking"},
]

ROLE_DEFINITIONS = [
    {"code": "owner", "name": "Owner", "description": "Full account control", "is_system": True},
    {"code": "admin", "name": "Admin", "description": "Operational administrator", "is_system": True},
    {"code": "operator", "name": "Operator", "description": "Daily operations", "is_system": True},
    {"code": "viewer", "name": "Viewer", "description": "Read-only access", "is_system": True},
]

ROLE_PERMISSION_MAP = {
    "owner": {definition["code"] for definition in PERMISSION_DEFINITIONS},
    "admin": {
        "users.manage",
        "memberships.manage",
        "audit.read",
        "dashboard.read",
        "business.read",
        "business.write",
        "documents.manage",
        "tasks.read",
        "tasks.manage",
        "alerts.read",
        "rules.manage",
        "integrations.manage",
        "banking.read",
        "banking.manage",
    },
    "operator": {
        "dashboard.read",
        "business.read",
        "business.write",
        "documents.manage",
        "tasks.read",
        "tasks.manage",
        "alerts.read",
        "banking.read",
    },
    "viewer": {
        "dashboard.read",
        "business.read",
        "tasks.read",
        "alerts.read",
        "banking.read",
    },
}
