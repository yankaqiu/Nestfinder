"""Text feature extraction via Claude Haiku (async, crash-resilient).

Every result is appended to a JSONL checkpoint file immediately so that
a crash / rate-limit / ctrl-C never loses already-paid-for results.
On restart the checkpoint is read and already-extracted IDs are skipped.

Uses asyncio + semaphore for high-throughput parallel requests.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from pathlib import Path

import anthropic

logger = logging.getLogger(__name__)

CHECKPOINT_DIR = Path("data/checkpoints")
CHECKPOINT_FILE = CHECKPOINT_DIR / "text_features.jsonl"

_SYSTEM_PROMPT = (
    "Extract features from this real estate listing description. "
    "Return ONLY valid JSON, no markdown, no code fences, no explanation. "
    "Schema: "
    '{"balcony": bool, "elevator": bool, "parking": bool, "garden": bool, '
    '"washing_machine": bool, "dishwasher": bool, "fireplace": bool, '
    '"pets_allowed": bool, "furnished": bool, "minergie": bool, '
    '"wheelchair_accessible": bool, "cellar": bool}. '
    "Use false if not mentioned."
)

MAX_RETRIES = 5
BASE_BACKOFF = 2.0


def _parse_response(raw: str) -> dict[str, bool] | None:
    cleaned = re.sub(r"^```[a-z]*\n?|```$", "", raw.strip(), flags=re.M).strip()
    try:
        data = json.loads(cleaned)
        if isinstance(data, dict):
            return {k: bool(v) for k, v in data.items()}
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _load_checkpoint(path: Path) -> dict[str, dict]:
    """Load already-extracted results from the JSONL checkpoint."""
    done: dict[str, dict] = {}
    if not path.exists():
        return done
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                lid = record.get("listing_id")
                feats = record.get("features")
                if lid and feats is not None:
                    done[lid] = feats
            except json.JSONDecodeError:
                logger.warning("Corrupt checkpoint line %d, skipping", lineno)
    return done


class _CheckpointWriter:
    """Thread-safe, fsync'd JSONL writer."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(path, "a", encoding="utf-8")
        self._lock = asyncio.Lock()

    async def write(self, listing_id: str, features: dict) -> None:
        record = json.dumps({"listing_id": listing_id, "features": features},
                            ensure_ascii=True)
        async with self._lock:
            self._f.write(record + "\n")
            self._f.flush()
            os.fsync(self._f.fileno())

    def close(self) -> None:
        self._f.close()


async def _call_claude(
    client: anthropic.AsyncAnthropic,
    description: str,
    sem: asyncio.Semaphore,
) -> str | None:
    """Call Claude Haiku with semaphore throttling + exponential backoff."""
    async with sem:
        for attempt in range(MAX_RETRIES):
            try:
                response = await client.messages.create(
                    model="claude-haiku-4-5",
                    max_tokens=256,
                    system=[{
                        "type": "text",
                        "text": _SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": description}],
                )
                return response.content[0].text.strip()
            except anthropic.RateLimitError:
                wait = BASE_BACKOFF * (2 ** attempt)
                logger.warning("Rate limited, waiting %.1fs (attempt %d/%d)",
                               wait, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(wait)
            except anthropic.APIStatusError as exc:
                if exc.status_code >= 500:
                    wait = BASE_BACKOFF * (2 ** attempt)
                    logger.warning("Server error %d, retrying in %.1fs",
                                   exc.status_code, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.error("API error %d: %s", exc.status_code, exc.message)
                    return None
            except anthropic.APIConnectionError:
                wait = BASE_BACKOFF * (2 ** attempt)
                logger.warning("Connection error, retrying in %.1fs", wait)
                await asyncio.sleep(wait)
    logger.error("All %d retries exhausted", MAX_RETRIES)
    return None


async def _process_one(
    client: anthropic.AsyncAnthropic,
    sem: asyncio.Semaphore,
    listing_id: str,
    description: str,
    checkpoint: _CheckpointWriter,
    stats: dict,
    stats_lock: asyncio.Lock,
) -> tuple[str, str] | None:
    """Process a single listing: call API, write checkpoint, return DB update tuple."""
    desc_truncated = (description or "")[:1500]
    if len(desc_truncated) < 50:
        async with stats_lock:
            stats["skipped"] += 1
        return None

    raw = await _call_claude(client, desc_truncated, sem)
    if raw is None:
        async with stats_lock:
            stats["errors"] += 1
        return None

    features = _parse_response(raw)
    if features:
        await checkpoint.write(listing_id, features)
        async with stats_lock:
            stats["extracted"] += 1
            stats["_done"] += 1
        return (json.dumps(features, ensure_ascii=True), listing_id)
    else:
        async with stats_lock:
            stats["errors"] += 1
        logger.debug("Parse failed for %s: %s", listing_id, raw[:100])
        return None


def _flush_to_db(conn: sqlite3.Connection, updates: list[tuple[str, str]]) -> None:
    conn.executemany(
        "UPDATE listings SET text_features_json = ? WHERE listing_id = ?",
        updates,
    )
    conn.commit()


async def _run_async(
    conn: sqlite3.Connection,
    pending: list[tuple[str, str]],
    *,
    concurrency: int,
    batch_size: int,
    checkpoint_path: Path,
) -> dict[str, int]:
    """Core async extraction loop."""
    stats = {"extracted": 0, "errors": 0, "skipped": 0, "_done": 0}
    stats_lock = asyncio.Lock()
    total = len(pending)

    sem = asyncio.Semaphore(concurrency)
    client = anthropic.AsyncAnthropic()
    checkpoint = _CheckpointWriter(checkpoint_path)

    t_start = time.time()
    db_updates: list[tuple[str, str]] = []

    # Process in waves to avoid holding 22k futures in memory
    wave_size = concurrency * 4
    for wave_start in range(0, total, wave_size):
        wave = pending[wave_start : wave_start + wave_size]
        tasks = [
            _process_one(client, sem, lid, desc, checkpoint, stats, stats_lock)
            for lid, desc in wave
        ]
        results = await asyncio.gather(*tasks)

        for result in results:
            if result is not None:
                db_updates.append(result)

        if len(db_updates) >= batch_size:
            _flush_to_db(conn, db_updates)
            db_updates.clear()

        done = wave_start + len(wave)
        elapsed = time.time() - t_start
        rate = done / elapsed if elapsed > 0 else 0
        eta_s = (total - done) / rate if rate > 0 else 0
        pct = done / total * 100
        logger.info(
            "  [%d/%d] %.0f%%  |  %.1f req/s  |  ETA %.1f min  |  ok=%d err=%d",
            done, total, pct, rate, eta_s / 60,
            stats["extracted"], stats["errors"],
        )

    if db_updates:
        _flush_to_db(conn, db_updates)

    checkpoint.close()
    await client.close()

    elapsed = time.time() - t_start
    del stats["_done"]
    logger.info("Text extraction done in %.1f min: %s", elapsed / 60, stats)
    return stats


def enrich_text_features(
    conn: sqlite3.Connection,
    *,
    concurrency: int = 50,
    batch_size: int = 100,
    limit: int | None = None,
    checkpoint_path: Path | None = None,
) -> dict[str, int]:
    """Extract features from descriptions via Claude Haiku (async).

    - Runs `concurrency` parallel API calls (default 50).
    - Writes each result to a JSONL checkpoint file immediately.
    - On restart, skips listing IDs already in the checkpoint.
    - Flushes to SQLite every `batch_size` results.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set, skipping text extraction")
        return {"error": "no_api_key"}

    cp_path = checkpoint_path or CHECKPOINT_FILE
    cp_path.parent.mkdir(parents=True, exist_ok=True)

    already_done = _load_checkpoint(cp_path)
    logger.info("Loaded %d already-extracted results from checkpoint", len(already_done))

    query = """
        SELECT listing_id, description
        FROM listings
        WHERE text_features_json IS NULL
          AND description IS NOT NULL
          AND LENGTH(description) > 50
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query).fetchall()

    pending = [(lid, desc) for lid, desc in rows if lid not in already_done]
    skipped_from_cp = len(rows) - len(pending)

    if not pending and not already_done:
        logger.info("All listings already have text_features_json")
        return {"extracted": 0, "errors": 0, "skipped": 0}

    # Flush any checkpoint results missing from DB
    if already_done:
        db_ids_null = {r[0] for r in conn.execute(
            "SELECT listing_id FROM listings WHERE text_features_json IS NULL"
        ).fetchall()}
        backfill = [(json.dumps(feats), lid)
                    for lid, feats in already_done.items() if lid in db_ids_null]
        if backfill:
            logger.info("Flushing %d checkpoint results to DB that were missing", len(backfill))
            _flush_to_db(conn, backfill)

    if not pending:
        logger.info("Nothing left to extract (all recovered from checkpoint)")
        return {"extracted": 0, "errors": 0, "skipped": 0, "resumed": len(already_done)}

    logger.info("Processing %d listings with concurrency=%d (%d skipped from checkpoint)",
                len(pending), concurrency, skipped_from_cp)

    return asyncio.run(_run_async(
        conn, pending,
        concurrency=concurrency,
        batch_size=batch_size,
        checkpoint_path=cp_path,
    ))
