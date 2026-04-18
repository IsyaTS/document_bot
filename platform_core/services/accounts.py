from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from platform_core.models import Account, AccountUser, Role, User


class AccountService:
    def __init__(self, session: Session) -> None:
        self.session = session

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
