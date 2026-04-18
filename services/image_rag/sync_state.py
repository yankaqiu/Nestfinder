from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass(slots=True)
class ListingSyncState:
    listing_id: str
    model_name: str
    image_urls_hash: str
    image_count: int
    indexed_at: str | None
    last_error: str | None


class SyncStateStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def get_listing_state(self, *, listing_id: str, model_name: str) -> ListingSyncState | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT listing_id, model_name, image_urls_hash, image_count, indexed_at, last_error
                FROM listing_sync_state
                WHERE listing_id = ? AND model_name = ?
                """,
                [listing_id, model_name],
            ).fetchone()
        if row is None:
            return None
        return ListingSyncState(
            listing_id=row["listing_id"],
            model_name=row["model_name"],
            image_urls_hash=row["image_urls_hash"],
            image_count=row["image_count"],
            indexed_at=row["indexed_at"],
            last_error=row["last_error"],
        )

    def upsert_listing_state(
        self,
        *,
        listing_id: str,
        model_name: str,
        image_urls_hash: str,
        image_count: int,
        last_error: str | None = None,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO listing_sync_state (
                    listing_id,
                    model_name,
                    image_urls_hash,
                    image_count,
                    indexed_at,
                    last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(listing_id, model_name) DO UPDATE SET
                    image_urls_hash = excluded.image_urls_hash,
                    image_count = excluded.image_count,
                    indexed_at = excluded.indexed_at,
                    last_error = excluded.last_error
                """,
                [
                    listing_id,
                    model_name,
                    image_urls_hash,
                    image_count,
                    utc_now_iso(),
                    last_error,
                ],
            )
            connection.commit()

    def count_indexed_listings(self, *, model_name: str) -> int:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM listing_sync_state
                WHERE model_name = ? AND last_error IS NULL
                """,
                [model_name],
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def set_service_state(self, *, key: str, value: str | None) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO service_state (state_key, state_value)
                VALUES (?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value
                """,
                [key, value],
            )
            connection.commit()

    def get_service_state(self, key: str) -> str | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT state_value FROM service_state WHERE state_key = ?",
                [key],
            ).fetchone()
        return row["state_value"] if row is not None else None

    def _ensure_schema(self) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS listing_sync_state (
                    listing_id TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    image_urls_hash TEXT NOT NULL,
                    image_count INTEGER NOT NULL,
                    indexed_at TEXT,
                    last_error TEXT,
                    PRIMARY KEY (listing_id, model_name)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS service_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT
                )
                """
            )
            connection.commit()

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path)
        connection.row_factory = sqlite3.Row
        return connection
