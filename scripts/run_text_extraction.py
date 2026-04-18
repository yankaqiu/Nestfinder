#!/usr/bin/env python3
"""Standalone, crash-resilient Claude text extraction runner.

Usage:
    python -m scripts.run_text_extraction                    # full run, auto-resumes
    python -m scripts.run_text_extraction --limit 100        # test with 100 listings
    python -m scripts.run_text_extraction --batch-size 25    # smaller DB flush batches
    python -m scripts.run_text_extraction --status           # just show progress
    python -m scripts.run_text_extraction --load-checkpoint  # load checkpoint → DB without calling API

Results are saved two ways:
  1. data/checkpoints/text_features.jsonl  (one line per listing, written immediately)
  2. SQLite text_features_json column      (flushed in batches)

If the script crashes, just re-run it -- it reads the checkpoint and skips
already-extracted listings automatically.
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("text_extract")

DEFAULT_DB = Path("data/listings.db")


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def show_status(db_path: Path) -> None:
    """Print current extraction progress without calling any API."""
    from app.enrichment.text_extraction import CHECKPOINT_FILE, _load_checkpoint

    conn = get_connection(db_path)
    total = conn.execute(
        "SELECT COUNT(*) FROM listings "
        "WHERE description IS NOT NULL AND LENGTH(description) > 50"
    ).fetchone()[0]
    in_db = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE text_features_json IS NOT NULL"
    ).fetchone()[0]
    checkpoint = _load_checkpoint(CHECKPOINT_FILE)
    remaining = total - max(in_db, len(checkpoint))

    print(f"\n{'='*55}")
    print(f"  TEXT EXTRACTION STATUS")
    print(f"{'='*55}")
    print(f"  Total listings with descriptions:  {total:>6,}")
    print(f"  In checkpoint file (JSONL):        {len(checkpoint):>6,}")
    print(f"  In SQLite (text_features_json):    {in_db:>6,}")
    print(f"  Remaining:                         {remaining:>6,}")
    print(f"  Checkpoint file: {CHECKPOINT_FILE}")
    print(f"{'='*55}\n")
    conn.close()


def load_checkpoint_to_db(db_path: Path) -> None:
    """Read checkpoint JSONL and flush any missing results into SQLite."""
    from app.enrichment.text_extraction import CHECKPOINT_FILE, _load_checkpoint

    conn = get_connection(db_path)
    checkpoint = _load_checkpoint(CHECKPOINT_FILE)
    if not checkpoint:
        print("No checkpoint data found.")
        conn.close()
        return

    null_ids = {r[0] for r in conn.execute(
        "SELECT listing_id FROM listings WHERE text_features_json IS NULL"
    ).fetchall()}

    to_write = [(json.dumps(feats), lid)
                for lid, feats in checkpoint.items() if lid in null_ids]

    if not to_write:
        print(f"All {len(checkpoint)} checkpoint entries already in DB.")
    else:
        conn.executemany(
            "UPDATE listings SET text_features_json = ? WHERE listing_id = ?",
            to_write,
        )
        conn.commit()
        print(f"Loaded {len(to_write)} results from checkpoint into DB "
              f"({len(checkpoint) - len(to_write)} were already there).")
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Claude Haiku text extraction with crash-resilient checkpointing",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB,
                        help="Path to listings SQLite DB")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max listings to process (useful for testing)")
    parser.add_argument("--concurrency", type=int, default=50,
                        help="Parallel API requests (default 50, max ~4000 rpm)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="How often to flush results to SQLite")
    parser.add_argument("--status", action="store_true",
                        help="Just show extraction progress, don't run")
    parser.add_argument("--load-checkpoint", action="store_true",
                        help="Load checkpoint JSONL → SQLite without calling API")
    args = parser.parse_args()

    if not args.db.exists():
        logger.error("DB not found at %s", args.db)
        return 1

    if args.status:
        show_status(args.db)
        return 0

    if args.load_checkpoint:
        load_checkpoint_to_db(args.db)
        return 0

    from app.enrichment.schema import add_enrichment_columns
    from app.enrichment.text_extraction import enrich_text_features

    conn = get_connection(args.db)
    add_enrichment_columns(conn)

    logger.info("Starting text extraction (limit=%s, concurrency=%d, batch_size=%d)",
                args.limit or "all", args.concurrency, args.batch_size)
    t0 = time.time()

    stats = enrich_text_features(
        conn,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        limit=args.limit,
    )

    elapsed = time.time() - t0
    logger.info("Finished in %.1f min — %s", elapsed / 60, stats)

    # Print final coverage
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    filled = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE text_features_json IS NOT NULL"
    ).fetchone()[0]
    pct = filled / total * 100 if total else 0
    print(f"\nFinal coverage: {filled:,}/{total:,} ({pct:.1f}%)")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
