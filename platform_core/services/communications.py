from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.exceptions import PlatformCoreError, TenantContextError
from platform_core.models import CommunicationReview, Customer, Employee, Lead, Task, TaskEvent
from platform_core.tenancy import TenantContext, require_account_id


@dataclass(frozen=True)
class CommunicationAnalysis:
    quality_status: str
    sentiment: str
    next_step_present: bool
    follow_up_status: str
    summary_json: dict[str, object]


class CommunicationService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_reviews(self, context: TenantContext) -> list[CommunicationReview]:
        account_id = require_account_id(context)
        return self.session.execute(
            select(CommunicationReview)
            .where(CommunicationReview.account_id == account_id)
            .order_by(CommunicationReview.created_at.desc(), CommunicationReview.id.desc())
        ).scalars().all()

    def get_review(self, context: TenantContext, review_id: int) -> CommunicationReview:
        account_id = require_account_id(context)
        review = self.session.execute(
            select(CommunicationReview).where(
                CommunicationReview.account_id == account_id,
                CommunicationReview.id == review_id,
            )
        ).scalar_one_or_none()
        if review is None:
            raise TenantContextError("Communication review not found in selected account.")
        return review

    def create_review(
        self,
        context: TenantContext,
        *,
        created_by_user_id: int | None,
        customer_id: int | None,
        lead_id: int | None,
        employee_id: int | None,
        channel: str,
        direction: str,
        title: str,
        transcript_text: str,
        response_delay_minutes: int | None,
    ) -> CommunicationReview:
        account_id = require_account_id(context)
        cleaned_title = title.strip()
        cleaned_transcript = transcript_text.strip()
        if not cleaned_title:
            raise PlatformCoreError("Communication title is required.")
        if not cleaned_transcript:
            raise PlatformCoreError("Transcript text is required.")
        if customer_id is not None:
            self._customer(account_id, customer_id)
        lead = self._lead(account_id, lead_id) if lead_id is not None else None
        employee = self._employee(account_id, employee_id) if employee_id is not None else None
        analysis = self._analyze_transcript(
            transcript_text=cleaned_transcript,
            response_delay_minutes=response_delay_minutes,
            lead=lead,
        )
        review = CommunicationReview(
            account_id=account_id,
            created_by_user_id=created_by_user_id,
            customer_id=customer_id,
            lead_id=lead_id,
            employee_id=employee.id if employee is not None else None,
            channel=(channel or "message").strip() or "message",
            direction=(direction or "inbound").strip() or "inbound",
            title=cleaned_title,
            transcript_text=cleaned_transcript,
            source_kind="manual",
            quality_status=analysis.quality_status,
            sentiment=analysis.sentiment,
            response_delay_minutes=response_delay_minutes,
            next_step_present=analysis.next_step_present,
            follow_up_status=analysis.follow_up_status,
            summary_json=analysis.summary_json,
        )
        self.session.add(review)
        self.session.flush()
        return review

    def create_follow_up_task(
        self,
        context: TenantContext,
        *,
        review_id: int,
        created_by_user_id: int | None,
        assignee_user_id: int | None,
        assignee_employee_id: int | None,
        due_at: datetime | None,
    ) -> Task:
        review = self.get_review(context, review_id)
        task = Task(
            account_id=review.account_id,
            assignee_user_id=assignee_user_id,
            assignee_employee_id=assignee_employee_id,
            created_by_user_id=created_by_user_id,
            source="communications",
            title=f"Review communication: {review.title}",
            description=self._task_description(review),
            status="open",
            priority="high" if review.quality_status == "critical" else "normal",
            due_at=due_at,
            related_entity_type="communication_review",
            related_entity_id=str(review.id),
        )
        self.session.add(task)
        self.session.flush()
        self.session.add(
            TaskEvent(
                account_id=review.account_id,
                task_id=task.id,
                actor_user_id=created_by_user_id,
                event_type="task.created_from_communications_ui",
                event_at=datetime.now(timezone.utc),
                payload_json={"communication_review_id": review.id},
            )
        )
        self.session.flush()
        return task

    def _analyze_transcript(
        self,
        *,
        transcript_text: str,
        response_delay_minutes: int | None,
        lead: Lead | None,
    ) -> CommunicationAnalysis:
        text = transcript_text.lower()
        urgent_markers = [
            "срочно",
            "urgent",
            "жду",
            "почему не",
            "не ответили",
            "претенз",
            "жалоб",
            "angry",
            "problem",
            "refund",
            "cancel",
        ]
        positive_markers = ["спасибо", "thank you", "отлично", "понятно", "ok", "хорошо"]
        next_step_markers = ["завтра", "сегодня", "пришлю", "счет", "invoice", "монтаж", "достав", "созвон", "встреч", "оплат"]
        negative_hits = [marker for marker in urgent_markers if marker in text]
        positive_hits = [marker for marker in positive_markers if marker in text]
        next_step_hits = [marker for marker in next_step_markers if marker in text]
        unresolved_questions = transcript_text.count("?")
        next_step_present = bool(next_step_hits)
        follow_up_status = "required" if unresolved_questions > 0 and not next_step_present else "clear"
        delay_status = "healthy"
        if response_delay_minutes is not None:
            if response_delay_minutes >= 240:
                delay_status = "critical"
            elif response_delay_minutes >= 60:
                delay_status = "warning"
        lead_risk = False
        if lead is not None and lead.first_response_due_at is not None:
            due_at = lead.first_response_due_at
            if due_at.tzinfo is None:
                due_at = due_at.replace(tzinfo=timezone.utc)
            if lead.first_responded_at is None and due_at <= datetime.now(timezone.utc):
                lead_risk = True
        if negative_hits or lead_risk or delay_status == "critical":
            quality_status = "critical"
        elif unresolved_questions > 0 or delay_status == "warning" or not next_step_present:
            quality_status = "warning"
        else:
            quality_status = "healthy"
        if negative_hits:
            sentiment = "negative"
        elif positive_hits:
            sentiment = "positive"
        else:
            sentiment = "neutral"
        recommendations: list[str] = []
        if lead_risk:
            recommendations.append("First response SLA is already at risk for the linked lead.")
        if delay_status == "critical":
            recommendations.append("Response delay is critical. Owner or operator follow-up is required now.")
        elif delay_status == "warning":
            recommendations.append("Response delay is elevated. Confirm who owns the next reply.")
        if unresolved_questions > 0 and not next_step_present:
            recommendations.append("Conversation has open questions without a clear next step.")
        if negative_hits:
            recommendations.append("Customer tone shows friction. Use a direct recovery response and confirm next action.")
        if not recommendations:
            recommendations.append("Communication quality looks acceptable. Keep the promised next step visible.")
        return CommunicationAnalysis(
            quality_status=quality_status,
            sentiment=sentiment,
            next_step_present=next_step_present,
            follow_up_status=follow_up_status,
            summary_json={
                "negative_hits": negative_hits,
                "positive_hits": positive_hits,
                "next_step_hits": next_step_hits,
                "unresolved_questions": unresolved_questions,
                "lead_risk": lead_risk,
                "delay_status": delay_status,
                "recommendations": recommendations,
            },
        )

    def _task_description(self, review: CommunicationReview) -> str:
        recommendations = list((review.summary_json or {}).get("recommendations") or [])
        recommendation_text = " ".join(recommendations[:3]) if recommendations else "Review the transcript and close the loop."
        return f"{recommendation_text}\n\nTranscript:\n{review.transcript_text[:1200]}"

    def _customer(self, account_id: int, customer_id: int) -> Customer:
        customer = self.session.execute(
            select(Customer).where(Customer.account_id == account_id, Customer.id == customer_id)
        ).scalar_one_or_none()
        if customer is None:
            raise PlatformCoreError("Customer not found in selected account.")
        return customer

    def _lead(self, account_id: int, lead_id: int) -> Lead:
        lead = self.session.execute(
            select(Lead).where(Lead.account_id == account_id, Lead.id == lead_id)
        ).scalar_one_or_none()
        if lead is None:
            raise PlatformCoreError("Lead not found in selected account.")
        return lead

    def _employee(self, account_id: int, employee_id: int) -> Employee:
        employee = self.session.execute(
            select(Employee).where(Employee.account_id == account_id, Employee.id == employee_id)
        ).scalar_one_or_none()
        if employee is None:
            raise PlatformCoreError("Employee not found in selected account.")
        return employee
