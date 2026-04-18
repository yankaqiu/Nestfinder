from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator


def _db_path() -> Path:
    configured = os.getenv("PREFERENCES_DB_PATH")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / "data" / "preferences.db"


@contextmanager
def _conn() -> Generator[sqlite3.Connection, None, None]:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def _listings_db_path() -> Path:
    configured = os.getenv("LISTINGS_DB_PATH")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / "data" / "listings.db"


def _ensure_schema() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS user_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT,
                listing_id  TEXT    NOT NULL,
                action      TEXT    NOT NULL,
                query       TEXT,
                ts          TEXT    NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_session ON user_events(session_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_listing ON user_events(listing_id)")
        con.execute("""
            CREATE TABLE IF NOT EXISTS search_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT,
                query       TEXT    NOT NULL,
                result_count INTEGER,
                ts          TEXT    NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_search_session ON search_log(session_id)")


# Initialise on import
_ensure_schema()

VALID_ACTIONS = {"view", "click", "favorite", "dismiss"}


def record_event(
    *,
    listing_id: str,
    action: str,
    query: str | None = None,
    session_id: str | None = None,
) -> int:
    if action not in VALID_ACTIONS:
        raise ValueError(f"action must be one of {VALID_ACTIONS}")
    ts = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO user_events (session_id, listing_id, action, query, ts) VALUES (?,?,?,?,?)",
            (session_id, listing_id, action, query, ts),
        )
        return cur.lastrowid or 0


def get_events(
    *,
    session_id: str | None = None,
    listing_id: str | None = None,
    action: str | None = None,
    limit: int = 50,
) -> list[dict]:
    clauses: list[str] = []
    params: list[object] = []
    if session_id:
        clauses.append("session_id = ?")
        params.append(session_id)
    if listing_id:
        clauses.append("listing_id = ?")
        params.append(listing_id)
    if action:
        clauses.append("action = ?")
        params.append(action)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)
    with _conn() as con:
        rows = con.execute(
            f"SELECT * FROM user_events {where} ORDER BY ts DESC LIMIT ?", params
        ).fetchall()
    return [dict(r) for r in rows]


def log_search(*, query: str, session_id: str | None = None, result_count: int = 0) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            "INSERT INTO search_log (session_id, query, result_count, ts) VALUES (?,?,?,?)",
            (session_id, query, result_count, ts),
        )


def get_search_history(*, session_id: str | None = None, limit: int = 20) -> list[dict]:
    params: list[Any] = []
    where = ""
    if session_id:
        where = "WHERE session_id = ?"
        params.append(session_id)
    params.append(limit)
    with _conn() as con:
        rows = con.execute(
            f"SELECT * FROM search_log {where} ORDER BY ts DESC LIMIT ?", params
        ).fetchall()
    return [dict(r) for r in rows]


def build_user_profile(*, session_id: str | None = None) -> dict[str, Any]:
    """Aggregate click/favorite events into a structured preference profile."""
    positive_events = get_events(session_id=session_id, action="click", limit=200)
    positive_events += get_events(session_id=session_id, action="favorite", limit=200)
    dismissed = {
        e["listing_id"]
        for e in get_events(session_id=session_id, action="dismiss", limit=200)
    }

    clicked_ids = list({e["listing_id"] for e in positive_events} - dismissed)
    favorite_ids = list(
        {e["listing_id"] for e in positive_events if e["action"] == "favorite"} - dismissed
    )

    cities: Counter[str] = Counter()
    prices: list[int] = []
    features: Counter[str] = Counter()
    FEATURE_COLS = [
        "feature_balcony", "feature_elevator", "feature_parking", "feature_garage",
        "feature_fireplace", "feature_child_friendly", "feature_pets_allowed",
        "feature_new_build", "feature_wheelchair_accessible", "feature_minergie_certified",
        "feature_private_laundry",
    ]

    if clicked_ids:
        listings_db = _listings_db_path()
        if listings_db.exists():
            placeholders = ",".join("?" for _ in clicked_ids)
            cols = ", ".join(["city", "price"] + FEATURE_COLS)
            con = sqlite3.connect(str(listings_db))
            con.row_factory = sqlite3.Row
            try:
                rows = con.execute(
                    f"SELECT {cols} FROM listings WHERE listing_id IN ({placeholders})",
                    clicked_ids,
                ).fetchall()
            finally:
                con.close()

            for row in rows:
                if row["city"]:
                    cities[row["city"]] += 1
                if row["price"]:
                    prices.append(int(row["price"]))
                for col in FEATURE_COLS:
                    if row[col]:
                        features[col.replace("feature_", "")] += 1

    search_history = get_search_history(session_id=session_id, limit=10)

    return {
        "session_id": session_id,
        "clicked_listing_ids": clicked_ids,
        "favorite_listing_ids": favorite_ids,
        "dismissed_listing_ids": list(dismissed),
        "preferred_cities": [city for city, _ in cities.most_common(5)],
        "preferred_features": [f for f, _ in features.most_common(5)],
        "price_range": {"min": min(prices), "max": max(prices)} if prices else None,
        "recent_searches": [s["query"] for s in search_history],
    }


def favorite_listing_ids(*, session_id: str | None = None) -> list[str]:
    """Return listing IDs the user has favorited (and not since dismissed)."""
    events = get_events(session_id=session_id, action="favorite", limit=200)
    dismissed = {
        e["listing_id"]
        for e in get_events(session_id=session_id, action="dismiss", limit=200)
    }
    seen: list[str] = []
    for e in events:
        lid = e["listing_id"]
        if lid not in dismissed and lid not in seen:
            seen.append(lid)
    return seen
