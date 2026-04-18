from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class Storage:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    doc_type TEXT NOT NULL,
                    company_key TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    raw_text TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS counterparties (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    inn TEXT NOT NULL DEFAULT '',
                    kpp TEXT NOT NULL DEFAULT '',
                    ogrn TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    email TEXT NOT NULL DEFAULT '',
                    manager TEXT NOT NULL DEFAULT '',
                    notes_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, name, inn)
                )
                """
            )
            self._ensure_column(conn, "documents", "raw_text", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "documents", "snapshot_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(conn, "documents", "counterparty_id", "INTEGER")

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def save_document(
        self,
        user_id: int,
        doc_type: str,
        company_key: str,
        filename: str,
        raw_text: str = "",
        snapshot: dict[str, Any] | None = None,
        counterparty_id: int | None = None,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO documents(user_id, doc_type, company_key, filename, raw_text, snapshot_json, counterparty_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, doc_type, company_key, filename, raw_text, json.dumps(snapshot or {}, ensure_ascii=False), counterparty_id),
            )
            return int(cursor.lastrowid)

    def recent_documents(self, user_id: int, limit: int = 10, doc_type: str | None = None) -> list[tuple[int, str, str, str]]:
        sql = """
            SELECT id, doc_type, filename, created_at
            FROM documents
            WHERE user_id = ?
        """
        params: list[Any] = [user_id]
        if doc_type:
            sql += " AND doc_type = ?"
            params.append(doc_type)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [(row[0], row[1], row[2], row[3]) for row in rows]

    def get_document(self, user_id: int, document_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, doc_type, company_key, filename, raw_text, snapshot_json, counterparty_id, created_at
                FROM documents
                WHERE user_id = ? AND id = ?
                """,
                (user_id, document_id),
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "doc_type": row[1],
            "company_key": row[2],
            "filename": row[3],
            "raw_text": row[4],
            "snapshot": json.loads(row[5] or "{}"),
            "counterparty_id": row[6],
            "created_at": row[7],
        }

    def last_document_snapshot(self, user_id: int, doc_type: str | None = None) -> dict[str, Any] | None:
        sql = """
            SELECT id, doc_type, company_key, raw_text, snapshot_json, counterparty_id, created_at
            FROM documents
            WHERE user_id = ?
        """
        params: list[Any] = [user_id]
        if doc_type:
            sql += " AND doc_type = ?"
            params.append(doc_type)
        sql += " ORDER BY id DESC LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "doc_type": row[1],
            "company_key": row[2],
            "raw_text": row[3],
            "snapshot": json.loads(row[4] or "{}"),
            "counterparty_id": row[5],
            "created_at": row[6],
        }

    def upsert_counterparty(self, user_id: int, payload: dict[str, Any]) -> int | None:
        name = str(payload.get("counterparty_name") or "").strip()
        if not name:
            return None
        inn = str(payload.get("counterparty_inn") or "").strip()
        values = (
            user_id,
            name,
            inn,
            str(payload.get("counterparty_kpp") or "").strip(),
            str(payload.get("counterparty_ogrn") or "").strip(),
            str(payload.get("counterparty_address") or payload.get("object_address") or "").strip(),
            str(payload.get("counterparty_phone") or "").strip(),
            str(payload.get("counterparty_email") or "").strip(),
            str(payload.get("counterparty_manager") or "").strip(),
            json.dumps(payload, ensure_ascii=False),
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO counterparties(
                    user_id, name, inn, kpp, ogrn, address, phone, email, manager, notes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, name, inn) DO UPDATE SET
                    kpp = excluded.kpp,
                    ogrn = excluded.ogrn,
                    address = excluded.address,
                    phone = excluded.phone,
                    email = excluded.email,
                    manager = excluded.manager,
                    notes_json = excluded.notes_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                values,
            )
            row = conn.execute(
                """
                SELECT id
                FROM counterparties
                WHERE user_id = ? AND name = ? AND inn = ?
                """,
                (user_id, name, inn),
            ).fetchone()
        return int(row[0]) if row else None

    def recent_counterparties(self, user_id: int, limit: int = 8) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, name, inn, kpp, address, phone, email, manager, notes_json, updated_at
                FROM counterparties
                WHERE user_id = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "inn": row[2],
                    "kpp": row[3],
                    "address": row[4],
                    "phone": row[5],
                    "email": row[6],
                    "manager": row[7],
                    "notes": json.loads(row[8] or "{}"),
                    "updated_at": row[9],
                }
            )
        return result

    def get_counterparty(self, user_id: int, counterparty_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, name, inn, kpp, ogrn, address, phone, email, manager, notes_json, updated_at
                FROM counterparties
                WHERE user_id = ? AND id = ?
                """,
                (user_id, counterparty_id),
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "name": row[1],
            "inn": row[2],
            "kpp": row[3],
            "ogrn": row[4],
            "address": row[5],
            "phone": row[6],
            "email": row[7],
            "manager": row[8],
            "notes": json.loads(row[9] or "{}"),
            "updated_at": row[10],
        }
