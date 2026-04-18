from __future__ import annotations

from dataclasses import dataclass

from platform_core.exceptions import TenantContextError


@dataclass(frozen=True)
class TenantContext:
    account_id: int
    actor_user_id: int | None = None
    source: str = "system"
    request_id: str | None = None
    role_code: str | None = None
    is_system: bool = False


def require_account_id(context: TenantContext) -> int:
    if context.account_id <= 0:
        raise TenantContextError("TenantContext.account_id must be a positive integer.")
    return context.account_id
