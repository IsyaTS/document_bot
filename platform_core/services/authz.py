from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.exceptions import AuthorizationError
from platform_core.models import AccountUser, Permission, Role, RolePermission
from platform_core.tenancy import TenantContext, require_account_id


class AuthorizationService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_permissions(self, context: TenantContext) -> set[str]:
        if context.is_system:
            return {"*"}

        if context.actor_user_id is None:
            raise AuthorizationError("Actor user is required for non-system authorization.")

        account_id = require_account_id(context)
        query = (
            select(Permission.code)
            .join(RolePermission, RolePermission.permission_id == Permission.id)
            .join(Role, Role.id == RolePermission.role_id)
            .join(AccountUser, AccountUser.role_id == Role.id)
            .where(
                AccountUser.account_id == account_id,
                AccountUser.user_id == context.actor_user_id,
                AccountUser.status == "active",
            )
        )
        return set(self.session.execute(query).scalars().all())

    def require(self, context: TenantContext, permission_code: str) -> None:
        permissions = self.list_permissions(context)
        if "*" in permissions or permission_code in permissions:
            return
        raise AuthorizationError(f"Permission denied: {permission_code}")
