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


_FEATURE_COLS = [
    "feature_balcony", "feature_elevator", "feature_parking", "feature_garage",
    "feature_fireplace", "feature_child_friendly", "feature_pets_allowed",
    "feature_new_build", "feature_wheelchair_accessible", "feature_minergie_certified",
]

_EVENT_STRENGTH: dict[str, float] = {
    "favorite": 3.0,
    "click":    1.0,
    "view":     1.0,
    "dismiss": -3.0,
}


def _ensure_schema() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS user_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     TEXT,
                session_id  TEXT,
                listing_id  TEXT    NOT NULL,
                action      TEXT    NOT NULL,
                query       TEXT,
                ts          TEXT    NOT NULL
            )
        """)
        # Add user_id column if upgrading from old schema (no-op if already exists)
        try:
            con.execute("ALTER TABLE user_events ADD COLUMN user_id TEXT")
        except Exception:
            pass
        con.execute("CREATE INDEX IF NOT EXISTS idx_user ON user_events(user_id)")
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
    user_id: str | None = None,
) -> int:
    if action not in VALID_ACTIONS:
        raise ValueError(f"action must be one of {VALID_ACTIONS}")
    ts = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO user_events (user_id, session_id, listing_id, action, query, ts) VALUES (?,?,?,?,?,?)",
            (user_id, session_id, listing_id, action, query, ts),
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


def build_user_profile(
    *,
    user_id: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Aggregate events into a recency-weighted preference profile."""
    now_iso = datetime.now(timezone.utc).isoformat()

    with _conn() as con:
        clauses: list[str] = []
        params: list[Any] = []
        if user_id:
            clauses.append("user_id = ?")
            params.append(user_id)
        elif session_id:
            clauses.append("session_id = ?")
            params.append(session_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = con.execute(
            f"""SELECT listing_id, action, ts,
                CASE
                    WHEN ts > datetime(?, '-1 day')  THEN 3.0
                    WHEN ts > datetime(?, '-7 days') THEN 2.0
                    ELSE 1.0
                END as recency_weight
            FROM user_events {where}
            ORDER BY ts DESC LIMIT 500""",
            [now_iso, now_iso] + params,
        ).fetchall()

    events = [dict(r) for r in rows]
    total_events = len(events)

    clicked_ids: list[str] = []
    favorite_ids: list[str] = []
    dismissed_ids: list[str] = []
    interacted_ids: list[str] = []

    for e in events:
        lid = e["listing_id"]
        if e["action"] == "favorite" and lid not in favorite_ids:
            favorite_ids.append(lid)
        if e["action"] == "dismiss" and lid not in dismissed_ids:
            dismissed_ids.append(lid)
        if e["action"] in ("click", "view", "favorite") and lid not in clicked_ids:
            clicked_ids.append(lid)
        if lid not in interacted_ids:
            interacted_ids.append(lid)

    dismissed_set = set(dismissed_ids)
    clicked_ids = [i for i in clicked_ids if i not in dismissed_set]
    favorite_ids = [i for i in favorite_ids if i not in dismissed_set]

    city_scores: dict[str, float] = {}
    feature_scores: dict[str, float] = {}
    prices: list[float] = []
    rooms_list: list[float] = []

    if interacted_ids:
        listings_db = _listings_db_path()
        if listings_db.exists():
            strength_by_id: dict[str, float] = {}
            for e in events:
                lid = e["listing_id"]
                s = _EVENT_STRENGTH.get(e["action"], 1.0) * float(e["recency_weight"])
                strength_by_id[lid] = strength_by_id.get(lid, 0.0) + s

            placeholders = ",".join("?" for _ in interacted_ids)
            cols = ", ".join(["listing_id", "city", "price", "rooms"] + _FEATURE_COLS)
            lcon = sqlite3.connect(str(listings_db))
            lcon.row_factory = sqlite3.Row
            try:
                lrows = lcon.execute(
                    f"SELECT {cols} FROM listings WHERE listing_id IN ({placeholders})",
                    interacted_ids,
                ).fetchall()
            finally:
                lcon.close()

            for row in lrows:
                lid = str(row["listing_id"])
                strength = strength_by_id.get(lid, 1.0)
                if strength <= 0:
                    continue
                if row["city"]:
                    city_scores[row["city"]] = city_scores.get(row["city"], 0.0) + strength
                if row["price"] and strength > 0:
                    prices.append(float(row["price"]))
                if row["rooms"] and strength > 0:
                    rooms_list.append(float(row["rooms"]))
                for col in _FEATURE_COLS:
                    if row[col]:
                        key = col.replace("feature_", "")
                        feature_scores[key] = feature_scores.get(key, 0.0) + strength

    total_city = sum(abs(v) for v in city_scores.values()) or 1.0
    preferred_cities = {k: round(v / total_city, 3) for k, v in city_scores.items() if v > 0}

    price_range: dict[str, float] | None = None
    if prices:
        sorted_prices = sorted(prices)
        price_range = {
            "min": round(min(prices) * 0.9),
            "max": round(max(prices) * 1.1),
            "median": round(sorted_prices[len(sorted_prices) // 2]),
        }

    search_history = get_search_history(session_id=session_id, limit=10)

    confidence = 0.0
    if total_events >= 30:
        confidence = 1.0
    elif total_events >= 10:
        confidence = 0.6
    elif total_events >= 3:
        confidence = 0.3

    return {
        "user_id": user_id,
        "session_id": session_id,
        "total_events": total_events,
        "confidence": confidence,
        "clicked_listing_ids": clicked_ids[:50],
        "favorite_listing_ids": favorite_ids,
        "dismissed_listing_ids": dismissed_ids,
        "preferred_cities": preferred_cities,
        "feature_affinities": {k: round(v, 3) for k, v in feature_scores.items() if v > 0},
        "preferred_features": sorted(feature_scores, key=lambda k: feature_scores[k], reverse=True)[:5],
        "price_range": price_range,
        "preferred_rooms": {
            "min": min(rooms_list) if rooms_list else None,
            "max": max(rooms_list) if rooms_list else None,
        },
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
