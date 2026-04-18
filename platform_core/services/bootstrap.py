from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from platform_core.models import Permission, Role, RolePermission
from platform_core.seeds import PERMISSION_DEFINITIONS, ROLE_DEFINITIONS, ROLE_PERMISSION_MAP
from platform_core.services.accounts import AccountService, MembershipService, UserService
from platform_core.services.audit import AuditLogService
from platform_core.tenancy import TenantContext


@dataclass(frozen=True)
class BootstrapResult:
    account_id: int
    user_id: int
    membership_id: int
    account_created: bool
    user_created: bool
    membership_created: bool


class CoreBootstrapService:
    def __init__(self, session: Session, default_timezone: str) -> None:
        self.session = session
        self.default_timezone = default_timezone
        self.account_service = AccountService(session)
        self.user_service = UserService(session)
        self.membership_service = MembershipService(session)
        self.audit_service = AuditLogService(session)

    def ensure_role_catalog(self) -> None:
        permission_by_code = {
            permission.code: permission
            for permission in self.session.execute(select(Permission)).scalars().all()
        }
        for definition in PERMISSION_DEFINITIONS:
            if definition["code"] in permission_by_code:
                permission = permission_by_code[definition["code"]]
                permission.name = definition["name"]
                permission.module = definition["module"]
                continue
            permission = self._create_permission(definition["code"], definition["name"], definition["module"])
            permission_by_code[permission.code] = permission

        role_by_code = {role.code: role for role in self.session.execute(select(Role)).scalars().all()}
        for definition in ROLE_DEFINITIONS:
            if definition["code"] in role_by_code:
                role = role_by_code[definition["code"]]
                role.name = definition["name"]
                role.description = definition["description"]
                role.is_system = definition["is_system"]
                continue
            role = self._create_role(
                definition["code"],
                definition["name"],
                definition["description"],
                definition["is_system"],
            )
            role_by_code[role.code] = role

        existing_pairs = {
            (row.role_id, row.permission_id)
            for row in self.session.execute(select(RolePermission)).scalars().all()
        }
        for role_code, permission_codes in ROLE_PERMISSION_MAP.items():
            role = role_by_code[role_code]
            for permission_code in permission_codes:
                permission = permission_by_code[permission_code]
                pair = (role.id, permission.id)
                if pair in existing_pairs:
                    continue
                self._create_role_permission(role.id, permission.id)
                existing_pairs.add(pair)
        self.session.flush()

    def _create_permission(self, code: str, name: str, module: str) -> Permission:
        permission = Permission(code=code, name=name, module=module)
        try:
            with self.session.begin_nested():
                self.session.add(permission)
                self.session.flush()
            return permission
        except IntegrityError:
            existing = self.session.execute(select(Permission).where(Permission.code == code)).scalar_one()
            existing.name = name
            existing.module = module
            return existing

    def _create_role(self, code: str, name: str, description: str, is_system: bool) -> Role:
        role = Role(code=code, name=name, description=description, is_system=is_system)
        try:
            with self.session.begin_nested():
                self.session.add(role)
                self.session.flush()
            return role
        except IntegrityError:
            existing = self.session.execute(select(Role).where(Role.code == code)).scalar_one()
            existing.name = name
            existing.description = description
            existing.is_system = is_system
            return existing

    def _create_role_permission(self, role_id: int, permission_id: int) -> None:
        try:
            with self.session.begin_nested():
                self.session.add(RolePermission(role_id=role_id, permission_id=permission_id))
                self.session.flush()
        except IntegrityError:
            return

    def bootstrap_account(
        self,
        *,
        account_slug: str,
        account_name: str,
        admin_email: str,
        admin_full_name: str,
        membership_role_code: str = "owner",
    ) -> BootstrapResult:
        self.ensure_role_catalog()

        account, account_created = self.account_service.ensure_account(account_slug, account_name, self.default_timezone)
        user, user_created = self.user_service.ensure_user(admin_email, admin_full_name)
        membership, membership_created = self.membership_service.ensure_membership(account, user, membership_role_code)

        context = TenantContext(
            account_id=account.id,
            actor_user_id=user.id,
            source="bootstrap",
            role_code=membership_role_code,
            is_system=True,
        )
        self.audit_service.log(
            context,
            action="core.bootstrap.completed",
            entity_type="account",
            entity_id=str(account.id),
            details={
                "account_slug": account.slug,
                "admin_email": user.email,
                "membership_role_code": membership_role_code,
            },
        )

        return BootstrapResult(
            account_id=account.id,
            user_id=user.id,
            membership_id=membership.id,
            account_created=account_created,
            user_created=user_created,
            membership_created=membership_created,
        )
