from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import MetaData, create_engine, inspect, select, text


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from platform_core.db import Base
from platform_core.settings import load_platform_settings
import platform_core.models  # noqa: F401


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copy runtime data from SQLite to PostgreSQL.")
    parser.add_argument(
        "--source-url",
        default=None,
        help="Source database URL. Defaults to the canonical SQLite runtime DB.",
    )
    parser.add_argument(
        "--target-url",
        default=None,
        help="Target PostgreSQL database URL. Defaults to PLATFORM_DATABASE_URL.",
    )
    parser.add_argument(
        "--sqlite-backup-dir",
        default=str(ROOT_DIR / "data" / "backups"),
        help="Directory for a pre-migration SQLite snapshot backup.",
    )
    parser.add_argument(
        "--force-clear-target",
        action="store_true",
        help="Delete existing rows in the target before copying. Use only on a dedicated empty target.",
    )
    return parser.parse_args()


def _default_source_url() -> str:
    sqlite_path = (ROOT_DIR / "data" / "platform.sqlite3").resolve()
    return f"sqlite+pysqlite:///{sqlite_path.as_posix()}"


def _sqlite_file_path(database_url: str) -> Path | None:
    prefix = "sqlite+pysqlite:///"
    if not database_url.startswith(prefix):
        return None
    return Path(database_url.removeprefix(prefix))


def _backup_sqlite(source_url: str, backup_dir: Path) -> Path | None:
    sqlite_path = _sqlite_file_path(source_url)
    if sqlite_path is None or not sqlite_path.exists():
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = backup_dir / f"{sqlite_path.stem}_pre_postgres_migration_{timestamp}.sqlite3"
    shutil.copy2(sqlite_path, target)
    return target


def _connect_args(database_url: str) -> dict[str, object]:
    return {"check_same_thread": False} if database_url.startswith("sqlite") else {}


def _table_names() -> list[str]:
    return [table.name for table in Base.metadata.sorted_tables]


def _assert_target_is_safe(target_engine, table_names: list[str], *, force_clear_target: bool) -> None:
    with target_engine.begin() as connection:
        non_empty = []
        for table_name in table_names:
            count = connection.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one()
            if int(count) > 0:
                non_empty.append((table_name, int(count)))
        if non_empty and not force_clear_target:
            formatted = ", ".join(f"{name}={count}" for name, count in non_empty[:10])
            raise SystemExit(
                "Target PostgreSQL database is not empty. "
                f"Use --force-clear-target only on a dedicated migration target. Found: {formatted}"
            )
        if non_empty and force_clear_target:
            for table_name in reversed(table_names):
                connection.execute(text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE'))


def _copy_rows(source_engine, target_engine, table_names: list[str]) -> dict[str, int]:
    source_metadata = MetaData()
    source_metadata.reflect(bind=source_engine)
    source_tables = source_metadata.tables
    copied: dict[str, int] = {}

    with source_engine.connect() as source_connection, target_engine.begin() as target_connection:
        for table_name in table_names:
            source_table = source_tables.get(table_name)
            target_table = Base.metadata.tables.get(table_name)
            if source_table is None or target_table is None:
                copied[table_name] = 0
                continue
            rows = [dict(row) for row in source_connection.execute(select(source_table)).mappings()]
            if rows:
                target_connection.execute(target_table.insert(), rows)
            copied[table_name] = len(rows)
        alembic_rows = [
            dict(row)
            for row in source_connection.execute(text('SELECT version_num FROM alembic_version')).mappings()
        ]
        target_connection.execute(text("DELETE FROM alembic_version"))
        if alembic_rows:
            target_connection.execute(text("INSERT INTO alembic_version (version_num) VALUES (:version_num)"), alembic_rows)
    return copied


def _sync_sequences(target_engine) -> None:
    inspector = inspect(target_engine)
    with target_engine.begin() as connection:
        for table_name in inspector.get_table_names():
            pk = inspector.get_pk_constraint(table_name).get("constrained_columns") or []
            if len(pk) != 1:
                continue
            column_name = pk[0]
            sequence_name = connection.execute(
                text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
                {"table_name": table_name, "column_name": column_name},
            ).scalar_one_or_none()
            if not sequence_name:
                continue
            connection.execute(
                text(
                    f"SELECT setval('{sequence_name}', "
                    f"COALESCE((SELECT MAX(\"{column_name}\") FROM \"{table_name}\"), 1), true)"
                )
            )


def _collect_counts(engine, table_names: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    with engine.connect() as connection:
        for table_name in table_names:
            counts[table_name] = int(connection.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one())
    return counts


def main() -> None:
    args = parse_args()
    settings = load_platform_settings()
    source_url = args.source_url or _default_source_url()
    target_url = args.target_url or settings.database_url
    if not target_url.startswith("postgresql+psycopg://"):
        raise SystemExit("Target URL must be PostgreSQL with psycopg, for example postgresql+psycopg://...")

    source_engine = create_engine(source_url, future=True, connect_args=_connect_args(source_url))
    target_engine = create_engine(target_url, future=True)
    table_names = _table_names()

    backup_path = _backup_sqlite(source_url, Path(args.sqlite_backup_dir))
    _assert_target_is_safe(target_engine, table_names, force_clear_target=args.force_clear_target)
    copied = _copy_rows(source_engine, target_engine, table_names)
    _sync_sequences(target_engine)

    source_counts = _collect_counts(source_engine, table_names)
    target_counts = _collect_counts(target_engine, table_names)
    mismatches = {
        table_name: {"source": source_counts[table_name], "target": target_counts[table_name]}
        for table_name in table_names
        if source_counts[table_name] != target_counts[table_name]
    }

    source_engine.dispose()
    target_engine.dispose()

    print(
        {
            "source_url": source_url,
            "target_url": target_url,
            "sqlite_backup": str(backup_path) if backup_path is not None else None,
            "copied_tables": copied,
            "mismatches": mismatches,
        }
    )
    if mismatches:
        raise SystemExit("Row count verification failed after SQLite -> PostgreSQL migration.")


if __name__ == "__main__":
    main()
