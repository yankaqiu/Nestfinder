from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator


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
