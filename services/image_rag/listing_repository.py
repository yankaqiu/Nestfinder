from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from pathlib import Path

from app.db import get_connection


@dataclass(slots=True)
class ListingRecord:
    listing_id: str
    platform_id: str | None
    scrape_source: str | None
    images_json: str | None


class ListingRepository:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def list_listing_ids(self, *, limit: int | None = None) -> list[str]:
        query = "SELECT listing_id FROM listings ORDER BY listing_id ASC"
        params: list[int] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with get_connection(self._db_path) as connection:
            rows = connection.execute(query, params).fetchall()
        return [str(row["listing_id"]) for row in rows]

    def get_listing(self, listing_id: str) -> ListingRecord | None:
        with get_connection(self._db_path) as connection:
            row = connection.execute(
                """
                SELECT listing_id, platform_id, scrape_source, images_json
                FROM listings
                WHERE listing_id = ?
                """,
                [listing_id],
            ).fetchone()

        if row is None:
            return None

        return ListingRecord(
            listing_id=str(row["listing_id"]),
            platform_id=row["platform_id"],
            scrape_source=row["scrape_source"],
            images_json=row["images_json"],
        )

    def row_count(self) -> int:
        with sqlite3.connect(self._db_path) as connection:
            return int(connection.execute("SELECT COUNT(*) FROM listings").fetchone()[0])
