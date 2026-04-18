from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from platform_core.exceptions import AuthorizationError, PlatformCoreError
from platform_core.models import User


@dataclass(frozen=True)
class AuthResult:
    ok: bool
    user: User | None
    reason: str


class PasswordHasher:
    algorithm = "scrypt"
    n = 1 << 14
    r = 8
    p = 1
    dklen = 64

    def hash_password(self, password: str) -> str:
        if len(password) < 8:
            raise PlatformCoreError("Password must be at least 8 characters long.")
        salt = secrets.token_bytes(16)
        digest = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=self.n,
            r=self.r,
            p=self.p,
            dklen=self.dklen,
        )
        salt_b64 = base64.urlsafe_b64encode(salt).decode("ascii")
        digest_b64 = base64.urlsafe_b64encode(digest).decode("ascii")
        return f"{self.algorithm}${self.n}${self.r}${self.p}${salt_b64}${digest_b64}"

    def verify_password(self, password: str, password_hash: str | None) -> bool:
        if not password_hash:
            return False
        try:
            algorithm, n, r, p, salt_b64, digest_b64 = password_hash.split("$", 5)
        except ValueError:
            return False
        if algorithm != self.algorithm:
            return False
        salt = base64.urlsafe_b64decode(salt_b64.encode("ascii"))
        expected = base64.urlsafe_b64decode(digest_b64.encode("ascii"))
        actual = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=int(n),
            r=int(r),
            p=int(p),
            dklen=len(expected),
        )
        return hmac.compare_digest(actual, expected)


class UserSecurityService:
    invite_ttl = timedelta(days=7)
    reset_ttl = timedelta(hours=4)
    lockout_threshold = 5
    lockout_duration = timedelta(minutes=15)

    def __init__(self, session: Session) -> None:
        self.session = session
        self._hasher = PasswordHasher()

    def hash_password(self, password: str) -> str:
        return self._hasher.hash_password(password)

    def list_users(self) -> list[User]:
        return self.session.execute(select(User).order_by(User.email.asc())).scalars().all()

    def get_user(self, user_id: int) -> User:
        user = self.session.get(User, user_id)
        if user is None:
            raise PlatformCoreError("User not found.")
        return user

    def get_by_email(self, email: str) -> User | None:
        normalized = email.strip().lower()
        if not normalized:
            return None
        return self.session.execute(select(User).where(User.email == normalized)).scalar_one_or_none()

    def authenticate(self, email: str, password: str, *, now: datetime | None = None) -> AuthResult:
        effective_now = now or datetime.now(timezone.utc)
        user = self.get_by_email(email)
        if user is None:
            return AuthResult(ok=False, user=None, reason="Invalid email or password.")
        if user.status != "active":
            return AuthResult(ok=False, user=user, reason="User account is not active.")
        if user.locked_until is not None and self._dt(user.locked_until) > effective_now:
            return AuthResult(ok=False, user=user, reason="Too many failed attempts. Try again later.")
        if not self._hasher.verify_password(password, user.password_hash):
            user.failed_login_attempts = int(user.failed_login_attempts or 0) + 1
            if user.failed_login_attempts >= self.lockout_threshold:
                user.locked_until = effective_now + self.lockout_duration
            self.session.flush()
            return AuthResult(ok=False, user=user, reason="Invalid email or password.")
        user.failed_login_attempts = 0
        user.locked_until = None
        user.last_login_at = effective_now
        self.session.flush()
        return AuthResult(ok=True, user=user, reason="ok")

    def create_or_update_user(
        self,
        *,
        email: str,
        full_name: str,
        status: str,
        user_id: int | None = None,
    ) -> tuple[User, bool]:
        normalized_email = email.strip().lower()
        if not normalized_email:
            raise PlatformCoreError("Email is required.")
        normalized_name = full_name.strip() or normalized_email
        if status not in {"invited", "active", "disabled"}:
            raise PlatformCoreError(f"Unsupported user status: {status}.")
        if user_id is not None:
            user = self.get_user(user_id)
            changed = False
            if normalized_email != user.email:
                raise PlatformCoreError("Existing user email cannot be changed.")
            if user.full_name != normalized_name:
                user.full_name = normalized_name
                changed = True
            if user.status != status:
                self.set_user_status(user, status)
                changed = True
            self.session.flush()
            return user, changed
        existing = self.get_by_email(normalized_email)
        if existing is not None:
            changed = False
            if existing.full_name != normalized_name:
                existing.full_name = normalized_name
                changed = True
            if existing.status != status:
                self.set_user_status(existing, status)
                changed = True
            self.session.flush()
            return existing, changed
        user = User(email=normalized_email, full_name=normalized_name, status=status)
        self.session.add(user)
        self.session.flush()
        return user, True

    def set_user_status(self, user: User, status: str) -> User:
        if status not in {"invited", "active", "disabled"}:
            raise PlatformCoreError(f"Unsupported user status: {status}.")
        if user.status == status:
            return user
        user.status = status
        if status == "disabled":
            user.locked_until = datetime.now(timezone.utc) + timedelta(days=3650)
            user.auth_version = int(user.auth_version or 1) + 1
        if status == "active":
            user.locked_until = None
        self.session.flush()
        return user

    def issue_invite(self, user: User, *, now: datetime | None = None) -> str:
        effective_now = now or datetime.now(timezone.utc)
        token, token_hash = self._issue_token()
        user.invite_token_hash = token_hash
        user.invite_sent_at = effective_now
        user.reset_token_hash = None
        user.reset_requested_at = None
        if user.status == "active" and not user.password_hash:
            user.status = "invited"
        elif user.status not in {"active", "disabled"}:
            user.status = "invited"
        self.session.flush()
        return token

    def issue_password_reset(self, user: User, *, now: datetime | None = None) -> str:
        effective_now = now or datetime.now(timezone.utc)
        if user.status == "disabled":
            raise PlatformCoreError("Cannot reset password for a disabled user.")
        token, token_hash = self._issue_token()
        user.reset_token_hash = token_hash
        user.reset_requested_at = effective_now
        self.session.flush()
        return token

    def claim_password(self, token: str, new_password: str, *, now: datetime | None = None) -> User:
        effective_now = now or datetime.now(timezone.utc)
        user, token_type = self._user_from_token(token)
        if token_type == "invite":
            if user.invite_sent_at is None or self._dt(user.invite_sent_at) + self.invite_ttl < effective_now:
                raise AuthorizationError("Invite link has expired.")
        if token_type == "reset":
            if user.reset_requested_at is None or self._dt(user.reset_requested_at) + self.reset_ttl < effective_now:
                raise AuthorizationError("Reset link has expired.")
        user.password_hash = self.hash_password(new_password)
        user.password_set_at = effective_now
        user.failed_login_attempts = 0
        user.locked_until = None
        user.auth_version = int(user.auth_version or 1) + 1
        if token_type == "invite":
            user.invite_token_hash = None
            user.invite_sent_at = None
            user.invite_accepted_at = effective_now
            if user.status != "disabled":
                user.status = "active"
        if token_type == "reset":
            user.reset_token_hash = None
            user.reset_requested_at = None
        self.session.flush()
        return user

    def token_claim_preview(self, token: str, *, now: datetime | None = None) -> tuple[User, str]:
        effective_now = now or datetime.now(timezone.utc)
        user, token_type = self._user_from_token(token)
        if token_type == "invite":
            if user.invite_sent_at is None or self._dt(user.invite_sent_at) + self.invite_ttl < effective_now:
                raise AuthorizationError("Invite link has expired.")
        if token_type == "reset":
            if user.reset_requested_at is None or self._dt(user.reset_requested_at) + self.reset_ttl < effective_now:
                raise AuthorizationError("Reset link has expired.")
        return user, token_type

    def _user_from_token(self, token: str) -> tuple[User, str]:
        token_hash = self._hash_token(token)
        user = self.session.execute(
            select(User).where(
                or_(
                    User.invite_token_hash == token_hash,
                    User.reset_token_hash == token_hash,
                )
            )
        ).scalar_one_or_none()
        if user is None:
            raise AuthorizationError("Password claim link is invalid.")
        if user.invite_token_hash == token_hash:
            return user, "invite"
        if user.reset_token_hash == token_hash:
            return user, "reset"
        raise AuthorizationError("Password claim link is invalid.")

    def _issue_token(self) -> tuple[str, str]:
        token = secrets.token_urlsafe(32)
        return token, self._hash_token(token)

    def _hash_token(self, token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _dt(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
