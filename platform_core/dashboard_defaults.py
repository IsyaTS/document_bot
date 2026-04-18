from __future__ import annotations


DEFAULT_DASHBOARD = {
    "code": "executive_control_panel",
    "name": "Executive Control Panel",
    "settings": {"layout": "single_screen", "max_items_per_list": 5},
    "widgets": [
        {"key": "money", "title": "Деньги", "position": 10},
        {"key": "financial_result", "title": "Финрезультат", "position": 20},
        {"key": "leads_sales", "title": "Лиды и продажи", "position": 30},
        {"key": "advertising", "title": "Реклама", "position": 40},
        {"key": "stock", "title": "Склад", "position": 50},
        {"key": "management", "title": "Управление", "position": 60},
        {"key": "owner_panel", "title": "Панель собственника", "position": 70},
    ],
}
