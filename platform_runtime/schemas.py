from __future__ import annotations

from datetime import datetime

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
