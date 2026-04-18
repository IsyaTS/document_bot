from __future__ import annotations

from collections.abc import Generator

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from platform_core.db import create_session_factory
from platform_core.services.runtime import ResolvedRuntimeContext, RuntimeContextService
from platform_core.settings import PlatformSettings, load_platform_settings


_session_factory = create_session_factory()


def get_settings() -> PlatformSettings:
    return load_platform_settings()


def get_db_session() -> Generator[Session, None, None]:
    session = _session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def require_internal_token(
    settings: PlatformSettings = Depends(get_settings),
    x_internal_token: str | None = Header(default=None),
) -> None:
    if x_internal_token != settings.internal_api_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid internal API token.")


def get_runtime_context(
    request: Request,
    _: None = Depends(require_internal_token),
    session: Session = Depends(get_db_session),
    x_account_id: int | None = Header(default=None),
    x_account_slug: str | None = Header(default=None),
    x_actor_user_id: int | None = Header(default=None),
    x_actor_email: str | None = Header(default=None),
) -> ResolvedRuntimeContext:
    try:
        return RuntimeContextService(session).resolve(
            account_id=x_account_id,
            account_slug=x_account_slug,
            actor_user_id=x_actor_user_id,
            actor_email=x_actor_email,
            source="api",
            request_id=request.headers.get("x-request-id"),
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
