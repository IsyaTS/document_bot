from __future__ import annotations

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from platform_core.settings import PlatformSettings, load_platform_settings


NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def create_engine_from_settings(settings: PlatformSettings | None = None) -> Engine:
    resolved = settings or load_platform_settings()
    connect_args: dict[str, object] = {}
    if resolved.database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(
        resolved.database_url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def create_session_factory(settings: PlatformSettings | None = None) -> sessionmaker:
    engine = create_engine_from_settings(settings)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
