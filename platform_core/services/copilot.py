from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.models import (
    Alert,
    CommunicationReview,
    CopilotReport,
    Document,
    KnowledgeItem,
    NotificationDispatch,
    Task,
)
from platform_core.runtime_obsidian import export_copilot_report_note
from platform_core.settings import load_platform_settings
from platform_core.services.business_os import BusinessOSService
from platform_core.services.dashboard import ExecutiveDashboardService
from platform_core.services.goals import GoalService
from platform_core.services.people import PeopleService
from platform_core.services.runtime import AdminQueryService, ResolvedRuntimeContext
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class CopilotAction:
    title: str
    reason: str
    owner: str
    priority: str
    target_area: str


class CopilotService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_reports(self, context: TenantContext, *, limit: int = 20) -> list[CopilotReport]:
        account_id = require_account_id(context)
        return self.session.execute(
            select(CopilotReport)
            .where(CopilotReport.account_id == account_id)
            .order_by(CopilotReport.created_at.desc(), CopilotReport.id.desc())
            .limit(max(1, limit))
        ).scalars().all()

    def get_report(self, context: TenantContext, report_id: int) -> CopilotReport:
        account_id = require_account_id(context)
        report = self.session.execute(
            select(CopilotReport).where(CopilotReport.account_id == account_id, CopilotReport.id == report_id)
        ).scalar_one_or_none()
        if report is None:
            raise ValueError("Copilot report not found in selected account.")
        return report

    def generate_account_report(
        self,
        runtime: ResolvedRuntimeContext,
        *,
        created_by_user_id: int | None,
        question_text: str | None = None,
    ) -> CopilotReport:
        now = datetime.now(timezone.utc)
        settings = load_platform_settings()
        account_settings = runtime.account.settings_json if isinstance(runtime.account.settings_json, dict) else {}
        grounding = self._build_grounding_payload(runtime)
        focus_areas = str(account_settings.get("copilot_focus_areas") or "").strip()

        generation_mode = "heuristic"
        model_name: str | None = None
        provider_response_id: str | None = None
        payload: dict[str, Any]
        if settings.openai_api_key:
            try:
                payload, provider_response_id = self._generate_openai_payload(
                    grounding=grounding,
                    question_text=question_text,
                    focus_areas=focus_areas,
                    model_name=settings.openai_model,
                    reasoning_effort=settings.openai_reasoning_effort,
                    api_key=settings.openai_api_key,
                )
                generation_mode = "openai"
                model_name = settings.openai_model
            except Exception as exc:
                payload = self._generate_heuristic_payload(runtime, grounding, question_text=question_text, focus_areas=focus_areas)
                payload["generation_warning"] = f"OpenAI generation failed, heuristic fallback used: {exc}"
                generation_mode = "heuristic_fallback"
                model_name = settings.openai_model
        else:
            payload = self._generate_heuristic_payload(runtime, grounding, question_text=question_text, focus_areas=focus_areas)

        markdown_text = self.render_markdown(runtime, payload)
        report = CopilotReport(
            account_id=runtime.account.id,
            created_by_user_id=created_by_user_id,
            scope="account",
            status="completed",
            generation_mode=generation_mode,
            model_name=model_name,
            provider_response_id=provider_response_id,
            question_text=(question_text or "").strip() or None,
            title=str(payload.get("title") or f"{runtime.account.name} Copilot Report"),
            summary_text=str(payload.get("executive_summary") or "").strip() or None,
            markdown_text=markdown_text,
            payload_json=payload,
        )
        self.session.add(report)
        self.session.flush()

        if bool(account_settings.get("export_copilot_to_obsidian", True)):
            export_info = export_copilot_report_note(
                account_slug=runtime.account.slug,
                account_name=runtime.account.name,
                generated_at=now.isoformat(),
                title=report.title,
                markdown_text=markdown_text,
                generation_mode=generation_mode,
                model_name=model_name,
            )
            report.obsidian_note_path = export_info["note_path"]
            report.payload_json = {
                **(report.payload_json or {}),
                "obsidian_export": export_info,
            }
            self.session.flush()
        return report

    def render_markdown(self, runtime: ResolvedRuntimeContext, payload: dict[str, Any]) -> str:
        actions = payload.get("recommended_actions") or []
        root_causes = payload.get("root_causes") or []
        questions = payload.get("questions_to_verify") or []
        signals = payload.get("supporting_signals") or []
        risks = payload.get("risks") or []
        lines = [
            f"# {payload.get('title') or runtime.account.name + ' Copilot Report'}",
            "",
            f"- account: {runtime.account.name} (`{runtime.account.slug}`)",
            f"- generated mode: {payload.get('generation_mode') or 'heuristic'}",
            f"- model: {payload.get('model_name') or 'n/a'}",
            "",
            "## Executive Summary",
            "",
            str(payload.get("executive_summary") or "No summary generated."),
            "",
            "## Root Causes",
        ]
        for item in root_causes:
            lines.append(f"- {item}")
        if not root_causes:
            lines.append("- No root causes identified.")
        lines.extend(["", "## Recommended Actions"])
        for item in actions:
            lines.append(
                f"- [{item.get('priority', 'normal')}] {item.get('title')} ({item.get('owner', 'owner')} -> {item.get('target_area', 'dashboard')}): {item.get('reason')}"
            )
        if not actions:
            lines.append("- No recommended actions generated.")
        lines.extend(["", "## Questions To Verify"])
        for item in questions:
            lines.append(f"- {item}")
        if not questions:
            lines.append("- No open questions.")
        lines.extend(["", "## Supporting Signals"])
        for item in signals:
            lines.append(f"- {item}")
        if not signals:
            lines.append("- No supporting signals listed.")
        lines.extend(["", "## Risks"])
        for item in risks:
            lines.append(f"- {item}")
        if not risks:
            lines.append("- No additional risks listed.")
        if payload.get("generation_warning"):
            lines.extend(["", "## Generation Warning", "", str(payload["generation_warning"])])
        return "\n".join(lines).strip() + "\n"

    def _build_grounding_payload(self, runtime: ResolvedRuntimeContext) -> dict[str, Any]:
        dashboard = ExecutiveDashboardService(self.session).get_dashboard(
            runtime.context,
            period_code=str((runtime.account.settings_json or {}).get("default_dashboard_period") or "today"),
        )
        widgets = {item["widget_key"]: item["payload"] for item in dashboard["widgets"]}
        ops = AdminQueryService(self.session).ops_summary(runtime.account.id)
        goals = GoalService(self.session).get_dashboard_goal_snapshot(runtime.context)
        knowledge = self.session.execute(
            select(KnowledgeItem)
            .where(KnowledgeItem.account_id == runtime.account.id, KnowledgeItem.status == "active")
            .order_by(KnowledgeItem.updated_at.desc(), KnowledgeItem.id.desc())
            .limit(8)
        ).scalars().all()
        communications = self.session.execute(
            select(CommunicationReview)
            .where(CommunicationReview.account_id == runtime.account.id)
            .order_by(CommunicationReview.created_at.desc(), CommunicationReview.id.desc())
            .limit(8)
        ).scalars().all()
        documents = self.session.execute(
            select(Document)
            .where(Document.account_id == runtime.account.id)
            .order_by(Document.created_at.desc(), Document.id.desc())
            .limit(8)
        ).scalars().all()
        alerts = self.session.execute(
            select(Alert)
            .where(Alert.account_id == runtime.account.id, Alert.status == "open")
            .order_by(Alert.updated_at.desc(), Alert.id.desc())
            .limit(8)
        ).scalars().all()
        tasks = self.session.execute(
            select(Task)
            .where(Task.account_id == runtime.account.id, Task.status == "open")
            .order_by(Task.updated_at.desc(), Task.id.desc())
            .limit(8)
        ).scalars().all()
        dispatches = self.session.execute(
            select(NotificationDispatch)
            .where(NotificationDispatch.account_id == runtime.account.id)
            .order_by(NotificationDispatch.created_at.desc(), NotificationDispatch.id.desc())
            .limit(8)
        ).scalars().all()
        inventory_rows = BusinessOSService(self.session).inventory_insights(runtime, stagnant_days=30)[:8]
        people_rows = PeopleService(self.session).employee_snapshots(runtime.context)[:8]

        return {
            "account": {
                "name": runtime.account.name,
                "slug": runtime.account.slug,
                "status": runtime.account.status,
                "plan_type": runtime.account.plan_type,
                "timezone": runtime.account.default_timezone,
                "currency": runtime.account.default_currency,
            },
            "dashboard": widgets,
            "ops_summary": {
                "failed_sync_jobs": len(ops["recent_failed_sync_jobs"]),
                "critical_alerts": len(ops["active_critical_alerts"]),
                "overdue_tasks": len(ops["overdue_tasks"]),
                "failed_rule_runs": len(ops["recent_failed_rule_runs"]),
                "integration_sync_status": [
                    {
                        "integration_id": row["integration"].id,
                        "external_ref": row["integration"].external_ref,
                        "provider_name": row["integration"].provider_name,
                        "latest_success_at": row["latest_success"].finished_at.isoformat() if row.get("latest_success") and row["latest_success"].finished_at else None,
                        "latest_failure_at": row["latest_failure"].finished_at.isoformat() if row.get("latest_failure") and row["latest_failure"].finished_at else None,
                    }
                    for row in ops["integration_sync_status"][:8]
                ],
            },
            "goals": [
                {
                    "title": item["goal"].title,
                    "summary_status": item["summary"]["status"],
                    "metrics": [
                        {
                            "metric_code": metric["metric_code"],
                            "target": metric["target"],
                            "actual": metric["actual"],
                            "delta": metric["delta"],
                            "status": metric["status"],
                        }
                        for metric in item["metrics"]
                    ],
                }
                for item in goals
            ],
            "alerts": [
                {"code": item.code, "severity": item.severity, "title": item.title, "description": item.description or ""}
                for item in alerts
            ],
            "tasks": [
                {
                    "id": item.id,
                    "title": item.title,
                    "priority": item.priority,
                    "due_at": item.due_at.isoformat() if item.due_at else None,
                    "related_entity_type": item.related_entity_type,
                }
                for item in tasks
            ],
            "communications": [
                {
                    "title": item.title,
                    "channel": item.channel,
                    "quality_status": item.quality_status,
                    "sentiment": item.sentiment,
                    "recommendations": (item.summary_json or {}).get("recommendations", []),
                }
                for item in communications
            ],
            "knowledge": [
                {
                    "title": item.title,
                    "summary": item.summary or "",
                    "body_excerpt": (item.body_text or "")[:500],
                    "item_type": item.item_type,
                }
                for item in knowledge
            ],
            "documents": [
                {
                    "document_type": item.document_type,
                    "document_number": item.document_number,
                    "status": item.status,
                    "total_amount": self._num(item.total_amount),
                }
                for item in documents
            ],
            "inventory": [
                {
                    "product": row.product.name,
                    "warehouse": row.warehouse.name,
                    "quantity_on_hand": self._num(row.stock_item.quantity_on_hand),
                    "min_quantity": self._num(row.stock_item.min_quantity),
                    "reorder_needed": row.reorder_needed,
                    "stagnant": row.stagnant,
                    "days_since_movement": row.days_since_movement,
                }
                for row in inventory_rows
            ],
            "people": [
                {
                    "full_name": item.employee.full_name,
                    "status": item.status,
                    "open_tasks": item.open_tasks,
                    "overdue_tasks": item.overdue_tasks,
                    "open_alerts": item.open_alerts,
                }
                for item in people_rows
            ],
            "dispatches": [
                {
                    "channel": item.channel,
                    "status": item.status,
                    "target_ref": item.target_ref,
                    "created_at": item.created_at.isoformat() if item.created_at else None,
                }
                for item in dispatches
            ],
        }

    def _generate_openai_payload(
        self,
        *,
        grounding: dict[str, Any],
        question_text: str | None,
        focus_areas: str,
        model_name: str,
        reasoning_effort: str,
        api_key: str,
    ) -> tuple[dict[str, Any], str | None]:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        instructions = (
            "You are an operations copilot for a business owner. "
            "Use only the provided business signals. Do not invent facts. "
            "Return valid JSON only with keys: "
            "title, executive_summary, root_causes, recommended_actions, questions_to_verify, supporting_signals, risks. "
            "Each recommended action must include title, reason, owner, priority, target_area. "
            "Valid owner values: owner, operator, sales, finance, warehouse, marketing. "
            "Valid priority values: critical, high, normal. "
            "Valid target_area values: dashboard, alerts_tasks, goals, communications, inventory, crm, payroll, operations, notifications, integrations, ops_sync, knowledge."
        )
        prompt = {
            "question": (question_text or "").strip() or "What should the owner do next based on current business signals?",
            "focus_areas": focus_areas,
            "grounding": grounding,
        }
        request_kwargs: dict[str, Any] = {
            "model": model_name,
            "instructions": instructions,
            "input": json.dumps(prompt, ensure_ascii=False),
        }
        if reasoning_effort:
            request_kwargs["reasoning"] = {"effort": reasoning_effort}
        response = client.responses.create(**request_kwargs)
        output_text = getattr(response, "output_text", "") or ""
        if not output_text:
            raise ValueError("OpenAI returned empty output_text.")
        payload = self._extract_json_payload(output_text)
        payload["generation_mode"] = "openai"
        payload["model_name"] = model_name
        return self._normalize_payload(payload), getattr(response, "id", None)

    def _generate_heuristic_payload(
        self,
        runtime: ResolvedRuntimeContext,
        grounding: dict[str, Any],
        *,
        question_text: str | None,
        focus_areas: str,
    ) -> dict[str, Any]:
        actions: list[CopilotAction] = []
        root_causes: list[str] = []
        supporting: list[str] = []
        risks: list[str] = []
        questions: list[str] = []

        ops = grounding["ops_summary"]
        if ops["critical_alerts"]:
            root_causes.append(f"{ops['critical_alerts']} critical alerts are open.")
            actions.append(CopilotAction("Work through critical alerts", "Open critical alerts still need owner/operator action.", "owner", "critical", "alerts_tasks"))
        if ops["overdue_tasks"]:
            root_causes.append(f"{ops['overdue_tasks']} tasks are overdue.")
            actions.append(CopilotAction("Clear overdue execution backlog", "Overdue tasks are blocking execution discipline.", "operator", "high", "alerts_tasks"))
        if ops["failed_sync_jobs"]:
            root_causes.append(f"{ops['failed_sync_jobs']} sync jobs failed recently.")
            actions.append(CopilotAction("Review broken sync paths", "Data freshness is at risk when recent sync jobs fail.", "operator", "high", "ops_sync"))

        goal_rows = grounding["goals"]
        at_risk_goals = [item for item in goal_rows if item["summary_status"] != "on_track"]
        if at_risk_goals:
            root_causes.append(f"{len(at_risk_goals)} goals are off track.")
            actions.append(CopilotAction("Review goal deviations", "Current plan vs fact has active deviations.", "owner", "critical", "goals"))
        for item in grounding["communications"]:
            if item["quality_status"] == "critical":
                supporting.append(f"Critical communication review: {item['title']}")
        if any(item["quality_status"] == "critical" for item in grounding["communications"]):
            actions.append(CopilotAction("Fix communication quality risks", "Customer-facing communication quality is hurting conversion and trust.", "sales", "high", "communications"))
        reorder_count = sum(1 for item in grounding["inventory"] if item["reorder_needed"])
        stagnant_count = sum(1 for item in grounding["inventory"] if item["stagnant"])
        if reorder_count:
            root_causes.append(f"{reorder_count} stock rows need reorder.")
            actions.append(CopilotAction("Launch reorder actions", "Inventory levels are at or below minimum thresholds.", "warehouse", "high", "inventory"))
        if stagnant_count:
            risks.append(f"{stagnant_count} stock rows are stagnant and may lock working capital.")

        dashboard = grounding["dashboard"]
        owner_panel = dashboard.get("owner_panel") or {}
        advertising = dashboard.get("advertising") or {}
        leads_sales = dashboard.get("leads_sales") or {}
        if advertising:
            supporting.append(
                f"Advertising spend {advertising.get('spend')} with leads {advertising.get('leads_count')} and CPL {advertising.get('cpl')}."
            )
        if leads_sales:
            supporting.append(
                f"Incoming leads {leads_sales.get('incoming_leads')} with response breaches {leads_sales.get('first_response_sla_breaches')}."
            )
        if not questions:
            questions.append("Which single blocked goal matters most this week?")
        if grounding["dispatches"] and any(item["status"] == "failed" for item in grounding["dispatches"]):
            risks.append("Recent outbound notification dispatches failed.")
            actions.append(CopilotAction("Repair notification delivery", "Owner/operator digests are failing to leave the platform.", "operator", "high", "notifications"))
        if not actions:
            actions.append(CopilotAction("Keep monitoring current account health", "Current signals do not show a severe cross-domain blocker.", "owner", "normal", "dashboard"))

        payload = {
            "title": f"{runtime.account.name} Copilot Report",
            "executive_summary": f"Grounded copilot review for {runtime.account.name}. Focus: {focus_areas or 'overall account health'}. Question: {(question_text or '').strip() or 'What should the owner do next?'}",
            "root_causes": root_causes[:5],
            "recommended_actions": [item.__dict__ for item in actions[:6]],
            "questions_to_verify": questions[:5],
            "supporting_signals": supporting[:8],
            "risks": risks[:5],
            "generation_mode": "heuristic",
            "model_name": None,
        }
        return self._normalize_payload(payload)

    def _normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_actions: list[dict[str, str]] = []
        for item in payload.get("recommended_actions") or []:
            if not isinstance(item, dict):
                continue
            normalized_actions.append(
                {
                    "title": str(item.get("title") or "Untitled action").strip() or "Untitled action",
                    "reason": str(item.get("reason") or "").strip() or "No reason provided.",
                    "owner": self._enum(str(item.get("owner") or "owner"), {"owner", "operator", "sales", "finance", "warehouse", "marketing"}, "owner"),
                    "priority": self._enum(str(item.get("priority") or "normal"), {"critical", "high", "normal"}, "normal"),
                    "target_area": self._enum(
                        str(item.get("target_area") or "dashboard"),
                        {"dashboard", "alerts_tasks", "goals", "communications", "inventory", "crm", "payroll", "operations", "notifications", "integrations", "ops_sync", "knowledge"},
                        "dashboard",
                    ),
                }
            )
        return {
            "title": str(payload.get("title") or "Copilot Report").strip() or "Copilot Report",
            "executive_summary": str(payload.get("executive_summary") or "").strip(),
            "root_causes": [str(item).strip() for item in payload.get("root_causes") or [] if str(item).strip()][:6],
            "recommended_actions": normalized_actions[:8],
            "questions_to_verify": [str(item).strip() for item in payload.get("questions_to_verify") or [] if str(item).strip()][:6],
            "supporting_signals": [str(item).strip() for item in payload.get("supporting_signals") or [] if str(item).strip()][:10],
            "risks": [str(item).strip() for item in payload.get("risks") or [] if str(item).strip()][:6],
            "generation_mode": str(payload.get("generation_mode") or "heuristic"),
            "model_name": payload.get("model_name"),
            **({"generation_warning": str(payload.get("generation_warning")).strip()} if payload.get("generation_warning") else {}),
        }

    def _extract_json_payload(self, text: str) -> dict[str, Any]:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError:
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start < 0 or end <= start:
                raise
            payload = json.loads(cleaned[start : end + 1])
        if not isinstance(payload, dict):
            raise ValueError("Copilot output is not a JSON object.")
        return payload

    def _enum(self, value: str, allowed: set[str], default: str) -> str:
        candidate = value.strip().lower()
        return candidate if candidate in allowed else default

    def _num(self, value: Decimal | None) -> float | None:
        if value is None:
            return None
        return float(Decimal(value))
