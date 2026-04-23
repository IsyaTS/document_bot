from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import threading
from typing import Any


class TelegramClientUnavailableError(RuntimeError):
    pass


class TelegramQrLoginError(RuntimeError):
    pass


@dataclass
class TelegramSessionIdentity:
    user_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    phone: str | None

    @property
    def display_name(self) -> str:
        full_name = " ".join(part for part in [self.first_name, self.last_name] if part).strip()
        if full_name:
            return full_name
        if self.username:
            return f"@{self.username}"
        if self.phone:
            return f"+{self.phone}"
        return str(self.user_id)

    def as_dict(self) -> dict[str, object]:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "phone": self.phone,
            "display_name": self.display_name,
        }


@dataclass
class TelegramQrLoginSnapshot:
    integration_id: int
    state: str
    qr_url: str | None = None
    expires_at: datetime | None = None
    error_message: str | None = None
    identity: TelegramSessionIdentity | None = None

    @property
    def connected(self) -> bool:
        return self.state == "connected"

    @property
    def pending(self) -> bool:
        return self.state == "pending"

    @property
    def requires_password(self) -> bool:
        return self.state == "password_required"


@dataclass
class _PendingQrEntry:
    integration_id: int
    client: Any
    qr_login: Any
    wait_task: asyncio.Task[Any] | None
    state: str = "pending"
    qr_url: str | None = None
    expires_at: datetime | None = None
    error_message: str | None = None
    identity: TelegramSessionIdentity | None = None
    session_string: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class TelegramQrLoginManager:
    def __init__(self) -> None:
        self._entries: dict[int, _PendingQrEntry] = {}
        self._lock = threading.RLock()

    async def refresh(self, *, integration_id: int, api_id: str, api_hash: str) -> TelegramQrLoginSnapshot:
        previous = self._pop(integration_id)
        if previous is not None:
            await _close_entry(previous)
        TelegramClient, StringSession, SessionPasswordNeededError = _telethon_runtime()
        del SessionPasswordNeededError
        client = TelegramClient(StringSession(), int(api_id), api_hash)
        await client.connect()
        qr_login = await client.qr_login()
        entry = _PendingQrEntry(
            integration_id=integration_id,
            client=client,
            qr_login=qr_login,
            wait_task=None,
            qr_url=str(qr_login.url),
            expires_at=_as_utc(getattr(qr_login, "expires", None)),
        )
        with self._lock:
            self._entries[integration_id] = entry
        entry.wait_task = asyncio.create_task(self._watch_login(integration_id), name=f"telegram-qr-{integration_id}")
        return self.snapshot(integration_id)

    def snapshot(self, integration_id: int) -> TelegramQrLoginSnapshot:
        with self._lock:
            entry = self._entries.get(integration_id)
            if entry is None:
                return TelegramQrLoginSnapshot(integration_id=integration_id, state="idle")
            return TelegramQrLoginSnapshot(
                integration_id=integration_id,
                state=entry.state,
                qr_url=entry.qr_url,
                expires_at=entry.expires_at,
                error_message=entry.error_message,
                identity=entry.identity,
            )

    async def consume_authorized_session(self, integration_id: int) -> tuple[str, TelegramSessionIdentity] | None:
        with self._lock:
            entry = self._entries.get(integration_id)
            if entry is None or entry.state != "connected" or not entry.session_string or entry.identity is None:
                return None
            self._entries.pop(integration_id, None)
        await _close_entry(entry)
        return entry.session_string, entry.identity

    async def clear(self, integration_id: int) -> None:
        entry = self._pop(integration_id)
        if entry is not None:
            await _close_entry(entry)

    async def submit_password(self, integration_id: int, password: str) -> TelegramQrLoginSnapshot:
        with self._lock:
            entry = self._entries.get(integration_id)
        if entry is None:
            raise TelegramQrLoginError("QR login session was not found. Refresh the QR code and try again.")
        if entry.state != "password_required":
            raise TelegramQrLoginError("Telegram account does not require a cloud password right now.")
        secret = str(password or "").strip()
        if not secret:
            raise TelegramQrLoginError("Telegram cloud password is required.")
        try:
            await entry.client.sign_in(password=secret)
            identity = await _load_identity(entry.client)
            entry.identity = identity
            entry.session_string = entry.client.session.save()
            entry.state = "connected"
            entry.error_message = None
            logger.info("telegram_qr_login password accepted integration_id=%s user_id=%s", integration_id, identity.user_id)
        except Exception as exc:
            entry.state = "password_required"
            entry.error_message = str(exc)
            logger.warning("telegram_qr_login password failed integration_id=%s error=%s", integration_id, exc)
            raise TelegramQrLoginError(str(exc)) from exc
        finally:
            entry.updated_at = datetime.now(timezone.utc)
            if entry.state == "connected":
                await entry.client.disconnect()
        return self.snapshot(integration_id)

    def _pop(self, integration_id: int) -> _PendingQrEntry | None:
        with self._lock:
            return self._entries.pop(integration_id, None)

    async def _watch_login(self, integration_id: int) -> None:
        TelegramClient, StringSession, SessionPasswordNeededError = _telethon_runtime()
        del TelegramClient, StringSession
        with self._lock:
            entry = self._entries.get(integration_id)
        if entry is None:
            return
        try:
            await entry.qr_login.wait()
            identity = await _load_identity(entry.client)
            entry.identity = identity
            entry.session_string = entry.client.session.save()
            entry.state = "connected"
            entry.error_message = None
            logger.info("telegram_qr_login connected integration_id=%s user_id=%s", integration_id, identity.user_id)
        except SessionPasswordNeededError:
            entry.state = "password_required"
            entry.error_message = "Для этого аккаунта включен облачный пароль Telegram."
            logger.info("telegram_qr_login password_required integration_id=%s", integration_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            entry.state = "failed"
            entry.error_message = str(exc)
            logger.exception("telegram_qr_login failed integration_id=%s", integration_id)
        finally:
            entry.updated_at = datetime.now(timezone.utc)
            if entry.state == "connected":
                await entry.client.disconnect()


telegram_qr_login_manager = TelegramQrLoginManager()
logger = logging.getLogger(__name__)


def describe_session_sync(*, api_id: str, api_hash: str, session_string: str) -> TelegramSessionIdentity:
    return _run_async_blocking(lambda: describe_session(api_id=api_id, api_hash=api_hash, session_string=session_string))


def send_saved_message_sync(
    *,
    api_id: str,
    api_hash: str,
    session_string: str,
    text: str,
    peer: str = "me",
) -> dict[str, object]:
    return _run_async_blocking(
        lambda: send_saved_message(api_id=api_id, api_hash=api_hash, session_string=session_string, text=text, peer=peer)
    )


async def describe_session(*, api_id: str, api_hash: str, session_string: str) -> TelegramSessionIdentity:
    TelegramClient, StringSession, SessionPasswordNeededError = _telethon_runtime()
    del SessionPasswordNeededError
    client = TelegramClient(StringSession(session_string), int(api_id), api_hash)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise TelegramQrLoginError("Telegram session is not authorized.")
        return await _load_identity(client)
    finally:
        await client.disconnect()


async def send_saved_message(
    *,
    api_id: str,
    api_hash: str,
    session_string: str,
    text: str,
    peer: str = "me",
) -> dict[str, object]:
    TelegramClient, StringSession, SessionPasswordNeededError = _telethon_runtime()
    del SessionPasswordNeededError
    client = TelegramClient(StringSession(session_string), int(api_id), api_hash)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise TelegramQrLoginError("Telegram session is not authorized.")
        message = await client.send_message(peer, text)
        identity = await _load_identity(client)
        return {
            "message_id": int(getattr(message, "id", 0) or 0),
            "sent_at": datetime.now(timezone.utc),
            "identity": identity.as_dict(),
        }
    finally:
        await client.disconnect()


async def _load_identity(client: Any) -> TelegramSessionIdentity:
    me = await client.get_me()
    if me is None:
        raise TelegramQrLoginError("Telegram account identity is unavailable.")
    return TelegramSessionIdentity(
        user_id=int(getattr(me, "id", 0) or 0),
        username=str(getattr(me, "username", "") or "").strip() or None,
        first_name=str(getattr(me, "first_name", "") or "").strip() or None,
        last_name=str(getattr(me, "last_name", "") or "").strip() or None,
        phone=str(getattr(me, "phone", "") or "").strip() or None,
    )


def _run_async_blocking(factory) -> Any:
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: asyncio.run(factory()))
        return future.result()


async def _close_entry(entry: _PendingQrEntry) -> None:
    if entry.wait_task and not entry.wait_task.done():
        entry.wait_task.cancel()
        try:
            await entry.wait_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    client = entry.client
    if client is not None and getattr(client, "is_connected", None):
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _telethon_runtime():
    try:
        from telethon import TelegramClient
        from telethon.errors import SessionPasswordNeededError
        from telethon.sessions import StringSession
    except ImportError as exc:
        raise TelegramClientUnavailableError(
            "Telethon is not installed. Add 'telethon' to the environment to use Telegram QR login."
        ) from exc
    return TelegramClient, StringSession, SessionPasswordNeededError
