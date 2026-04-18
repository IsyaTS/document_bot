from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


class RuleRunRequest(BaseModel):
    now: datetime | None = None


class SyncJobCreateRequest(BaseModel):
    job_type: str = "full_sync"
    idempotency_key: str = Field(..., min_length=3, max_length=128)
    scope: dict[str, object] = Field(default_factory=dict)
    execute_now: bool = False


class SchedulerRunRequest(BaseModel):
    run_rules: bool = True
    run_sync_jobs: bool = True


class GoalTargetPayload(BaseModel):
    metric_code: Literal[
        "revenue",
        "net_profit",
        "incoming_leads",
        "lost_leads",
        "cpl",
        "first_response_breaches",
        "low_stock_items",
        "available_cash",
    ]
    target_value: float
    direction: Literal["min", "max"] | None = None


class GoalCreateRequest(BaseModel):
    title: str = Field(..., min_length=3, max_length=255)
    description: str | None = None
    period_kind: Literal["day", "week", "month"]
    period_start: date | None = None
    period_end: date | None = None
    owner_user_id: int | None = None
    is_primary: bool = False
    status: Literal["draft", "active", "archived"] = "active"
    targets: list[GoalTargetPayload] = Field(default_factory=list)


class GoalUpdateRequest(BaseModel):
    title: str | None = Field(default=None, min_length=3, max_length=255)
    description: str | None = None
    period_kind: Literal["day", "week", "month"] | None = None
    period_start: date | None = None
    period_end: date | None = None
    owner_user_id: int | None = None
    is_primary: bool | None = None
    status: Literal["draft", "active", "archived"] | None = None
    targets: list[GoalTargetPayload] | None = None
