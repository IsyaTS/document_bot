from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.models import Account, AccountUser, Role, User
from platform_core.exceptions import PlatformCoreError


class AccountService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_accounts(self) -> list[Account]:
        return self.session.execute(select(Account).order_by(Account.name.asc(), Account.slug.asc())).scalars().all()

    def get_by_slug(self, slug: str) -> Account | None:
        return self.session.execute(select(Account).where(Account.slug == slug)).scalar_one_or_none()

    def ensure_account(self, slug: str, name: str, default_timezone: str) -> tuple[Account, bool]:
        account = self.get_by_slug(slug)
        if account is not None:
            changed = False
            if account.name != name:
                account.name = name
                changed = True
            if account.default_timezone != default_timezone:
                account.default_timezone = default_timezone
                changed = True
            return account, changed

        account = Account(slug=slug, name=name, default_timezone=default_timezone)
        self.session.add(account)
        self.session.flush()
        return account, True

    def update_account(
        self,
        account: Account,
        *,
        name: str | None = None,
        default_timezone: str | None = None,
        default_currency: str | None = None,
        status: str | None = None,
        plan_type: str | None = None,
        settings_json: dict[str, object] | None = None,
        feature_flags_json: dict[str, object] | None = None,
        soft_limits_json: dict[str, object] | None = None,
    ) -> Account:
        if name is not None:
            account.name = name
        if default_timezone is not None:
            account.default_timezone = default_timezone
        if default_currency is not None:
            account.default_currency = default_currency
        if status is not None:
            account.status = status
        if plan_type is not None:
            account.plan_type = plan_type
        if settings_json is not None:
            account.settings_json = settings_json
        if feature_flags_json is not None:
            account.feature_flags_json = feature_flags_json
        if soft_limits_json is not None:
            account.soft_limits_json = soft_limits_json
        self.session.flush()
        return account


class UserService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_email(self, email: str) -> User | None:
        return self.session.execute(select(User).where(User.email == email)).scalar_one_or_none()

    def ensure_user(self, email: str, full_name: str) -> tuple[User, bool]:
        user = self.get_by_email(email)
        if user is not None:
            changed = False
            if user.full_name != full_name:
                user.full_name = full_name
                changed = True
            return user, changed

        user = User(email=email, full_name=full_name)
        self.session.add(user)
        self.session.flush()
        return user, True


class MembershipService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_memberships(self, account: Account) -> list[AccountUser]:
        return self.session.execute(
            select(AccountUser).where(AccountUser.account_id == account.id).order_by(AccountUser.id.asc())
        ).scalars().all()

    def get_membership(self, account: Account, membership_id: int) -> AccountUser:
        membership = self.session.execute(
            select(AccountUser).where(AccountUser.account_id == account.id, AccountUser.id == membership_id)
        ).scalar_one_or_none()
        if membership is None:
            raise PlatformCoreError("Membership not found in selected account.")
        return membership

    def ensure_membership(self, account: Account, user: User, role_code: str, status: str = "active") -> tuple[AccountUser, bool]:
        role = self.session.execute(select(Role).where(Role.code == role_code)).scalar_one()
        membership = self.session.execute(
            select(AccountUser).where(AccountUser.account_id == account.id, AccountUser.user_id == user.id)
        ).scalar_one_or_none()

        if membership is not None:
            changed = False
            if membership.role_id != role.id:
                membership.role = role
                changed = True
            if membership.status != status:
                membership.status = status
                changed = True
            return membership, changed

        membership = AccountUser(
            account_id=account.id,
            user_id=user.id,
            role_id=role.id,
            status=status,
            joined_at=datetime.now(timezone.utc),
        )
        self.session.add(membership)
        self.session.flush()
        return membership, True

    def update_membership(
        self,
        account: Account,
        membership_id: int,
        *,
        role_code: str | None = None,
        status: str | None = None,
    ) -> AccountUser:
        membership = self.get_membership(account, membership_id)
        next_role_code = role_code or (membership.role.code if membership.role is not None else None)
        next_status = status or membership.status
        self._ensure_owner_guard(account.id, membership, next_role_code=next_role_code, next_status=next_status)
        if role_code is not None and next_role_code is not None:
            membership.role = self.session.execute(select(Role).where(Role.code == next_role_code)).scalar_one()
        if status is not None:
            membership.status = status
        self.session.flush()
        return membership

    def disable_membership(self, account: Account, membership_id: int) -> AccountUser:
        return self.update_membership(account, membership_id, status="disabled")

    def remove_membership(self, account: Account, membership_id: int) -> None:
        membership = self.get_membership(account, membership_id)
        self._ensure_owner_guard(account.id, membership, next_role_code="removed", next_status="removed")
        self.session.delete(membership)
        self.session.flush()

    def _ensure_owner_guard(
        self,
        account_id: int,
        membership: AccountUser,
        *,
        next_role_code: str | None,
        next_status: str | None,
    ) -> None:
        current_role_code = membership.role.code if membership.role is not None else None
        if current_role_code != "owner":
            return
        if next_role_code == "owner" and next_status == "active":
            return
        active_owner_count = self.session.execute(
            select(AccountUser)
            .join(Role, Role.id == AccountUser.role_id)
            .where(
                AccountUser.account_id == account_id,
                AccountUser.status == "active",
                Role.code == "owner",
            )
        ).scalars().all()
        if len(active_owner_count) <= 1:
            raise PlatformCoreError("Account must keep at least one active owner.")
