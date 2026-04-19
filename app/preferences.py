from __future__ import annotations

import os
import sqlite3
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def preferences_db_path() -> Path:
    configured = os.getenv("PREFERENCES_DB_PATH")
    if configured:
        return Path(configured)
    return _repo_root() / "data" / "preferences.db"


def _listings_db_path() -> Path:
    configured = os.getenv("LISTINGS_DB_PATH")
    if configured:
        return Path(configured)
    return _repo_root() / "data" / "listings.db"


@contextmanager
def _conn() -> Generator[sqlite3.Connection, None, None]:
    path = preferences_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


VALID_ACTIONS = {"view", "click", "favorite", "dismiss"}


def ensure_schema() -> None:
    with _conn() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS user_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id   TEXT,
                listing_id   TEXT NOT NULL,
                action       TEXT NOT NULL,
                query        TEXT,
                ts           TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_events_session_id ON user_events(session_id)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_events_listing_id ON user_events(listing_id)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS search_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id    TEXT,
                query         TEXT NOT NULL,
                result_count  INTEGER,
                ts            TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_search_log_session_id ON search_log(session_id)"
        )


ensure_schema()


def record_event(
    *,
    listing_id: str,
    action: str,
    query: str | None = None,
    session_id: str | None = None,
) -> int:
    ensure_schema()
    if action not in VALID_ACTIONS:
        raise ValueError(f"action must be one of {sorted(VALID_ACTIONS)}")
    with _conn() as connection:
        cursor = connection.execute(
            """
            INSERT INTO user_events (session_id, listing_id, action, query, ts)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                session_id,
                listing_id,
                action,
                query,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        return int(cursor.lastrowid or 0)


def get_events(
    *,
    session_id: str | None = None,
    listing_id: str | None = None,
    action: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    ensure_schema()
    clauses: list[str] = []
    params: list[Any] = []
    if session_id:
        clauses.append("session_id = ?")
        params.append(session_id)
    if listing_id:
        clauses.append("listing_id = ?")
        params.append(listing_id)
    if action:
        clauses.append("action = ?")
        params.append(action)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    with _conn() as connection:
        rows = connection.execute(
            f"SELECT * FROM user_events {where} ORDER BY ts DESC LIMIT ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def log_search(
    *,
    query: str,
    session_id: str | None = None,
    result_count: int = 0,
) -> None:
    ensure_schema()
    with _conn() as connection:
        connection.execute(
            """
            INSERT INTO search_log (session_id, query, result_count, ts)
            VALUES (?, ?, ?, ?)
            """,
            (
                session_id,
                query,
                result_count,
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def get_search_history(
    *,
    session_id: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    ensure_schema()
    where = ""
    params: list[Any] = []
    if session_id:
        where = "WHERE session_id = ?"
        params.append(session_id)
    params.append(limit)
    with _conn() as connection:
        rows = connection.execute(
            f"SELECT * FROM search_log {where} ORDER BY ts DESC LIMIT ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_recent_sessions(*, limit: int = 20) -> list[dict[str, Any]]:
    ensure_schema()
    with _conn() as connection:
        rows = connection.execute(
            """
            SELECT
                session_id,
                MAX(ts) AS last_seen_at,
                COUNT(*) AS activity_count
            FROM (
                SELECT session_id, ts FROM user_events WHERE session_id IS NOT NULL
                UNION ALL
                SELECT session_id, ts FROM search_log WHERE session_id IS NOT NULL
            )
            GROUP BY session_id
            ORDER BY last_seen_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def empty_user_profile(*, session_id: str | None = None) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "clicked_listing_ids": [],
        "favorite_listing_ids": [],
        "dismissed_listing_ids": [],
        "preferred_cities": [],
        "preferred_features": [],
        "price_range": None,
        "recent_searches": [],
    }


def build_user_profile(*, session_id: str | None = None) -> dict[str, Any]:
    ensure_schema()
    if not session_id:
        return empty_user_profile(session_id=session_id)

    click_events = get_events(session_id=session_id, action="click", limit=200)
    favorite_events = get_events(session_id=session_id, action="favorite", limit=200)
    dismissed = {
        event["listing_id"]
        for event in get_events(session_id=session_id, action="dismiss", limit=200)
    }

    clicked_ids = list({event["listing_id"] for event in click_events} - dismissed)
    favorite_ids = list({event["listing_id"] for event in favorite_events} - dismissed)
    liked_ids = list(set(clicked_ids) | set(favorite_ids))

    preferred_cities: Counter[str] = Counter()
    preferred_features: Counter[str] = Counter()
    prices: list[int] = []
    feature_columns = [
        "feature_balcony",
        "feature_elevator",
        "feature_parking",
        "feature_garage",
        "feature_fireplace",
        "feature_child_friendly",
        "feature_pets_allowed",
        "feature_new_build",
        "feature_wheelchair_accessible",
        "feature_minergie_certified",
        "feature_private_laundry",
    ]

    listings_db_path = _listings_db_path()
    if liked_ids and listings_db_path.exists():
        placeholders = ",".join("?" for _ in liked_ids)
        columns = ", ".join(["city", "price"] + feature_columns)
        connection = sqlite3.connect(str(listings_db_path))
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                f"SELECT {columns} FROM listings WHERE listing_id IN ({placeholders})",
                liked_ids,
            ).fetchall()
        finally:
            connection.close()

        for row in rows:
            city = row["city"]
            if city:
                preferred_cities[str(city).strip()] += 1
            price = row["price"]
            if price:
                prices.append(int(price))
            for column in feature_columns:
                if row[column]:
                    preferred_features[column.removeprefix("feature_")] += 1

    search_history = get_search_history(session_id=session_id, limit=10)
    return {
        "session_id": session_id,
        "clicked_listing_ids": clicked_ids,
        "favorite_listing_ids": favorite_ids,
        "dismissed_listing_ids": list(dismissed),
        "preferred_cities": [city for city, _ in preferred_cities.most_common(5)],
        "preferred_features": [feature for feature, _ in preferred_features.most_common(5)],
        "price_range": {"min": min(prices), "max": max(prices)} if prices else None,
        "recent_searches": [entry["query"] for entry in search_history],
    }
