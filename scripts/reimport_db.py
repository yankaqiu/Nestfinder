"""Re-import listings DB from scratch using the latest CSV files.

Drops the existing DB, re-creates the schema (with all v3.1 columns),
imports every CSV in raw_data/, adds enrichment columns, runs the
global_score enrichment, and prints a coverage report.

Usage:
    uv run python -m scripts.reimport_db                 # default paths
    uv run python -m scripts.reimport_db --db data/listings.db
    uv run python -m scripts.reimport_db --raw-data raw_data/
    uv run python -m scripts.reimport_db --only listings_enriched_v.3.1.csv
    uv run python -m scripts.reimport_db --keep-old      # backs up old DB

Idempotent: safe to re-run. The old DB is replaced (or backed up with --keep-old).
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("reimport")


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-import listings DB from CSV files")
    parser.add_argument(
        "--db", type=Path, default=None,
        help="Path to output DB (default: data/listings.db)",
    )
    parser.add_argument(
        "--raw-data", type=Path, default=None,
        help="Path to raw_data directory (default: raw_data/)",
    )
    parser.add_argument(
        "--only", type=str, default=None,
        help="Import only this CSV filename from raw_data/ (e.g. listings_enriched_v.3.1.csv)",
    )
    parser.add_argument(
        "--keep-old", action="store_true",
        help="Back up old DB to <name>.bak instead of deleting it",
    )
    parser.add_argument(
        "--skip-enrichment", action="store_true",
        help="Skip running global_score enrichment after import",
    )
    args = parser.parse_args()

    from app.config import get_settings
    settings = get_settings()

    db_path: Path = args.db or settings.db_path
    raw_data_dir: Path = args.raw_data or settings.raw_data_dir

    if not raw_data_dir.exists():
        logger.error("Raw data directory not found: %s", raw_data_dir)
        return 1

    if args.only:
        csv_paths = [raw_data_dir / args.only]
        for p in csv_paths:
            if not p.exists():
                logger.error("CSV file not found: %s", p)
                return 1
    else:
        csv_paths = sorted(p for p in raw_data_dir.glob("*.csv") if p.is_file())
        if not csv_paths:
            logger.error("No CSV files found in %s", raw_data_dir)
            return 1

    logger.info("CSV files to import:")
    for p in csv_paths:
        logger.info("  %s (%.1f MB)", p.name, p.stat().st_size / 1024 / 1024)

    # Handle existing DB
    if db_path.exists():
        if args.keep_old:
            bak = db_path.with_suffix(".db.bak")
            logger.info("Backing up old DB to %s", bak)
            shutil.copy2(db_path, bak)
        db_path.unlink()
        for wal in (db_path.with_suffix(".db-wal"), db_path.with_suffix(".db-shm")):
            if wal.exists():
                wal.unlink()
        logger.info("Removed old DB: %s", db_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Handle SRED transform if needed
    from app.harness.sred_transform import ensure_sred_normalized_csv
    ensure_sred_normalized_csv(raw_data_dir)
    if not args.only:
        csv_paths = sorted(p for p in raw_data_dir.glob("*.csv") if p.is_file())

    # Put v3.1 enriched CSV last so its data wins on INSERT OR REPLACE
    csv_paths.sort(key=lambda p: (1 if "enriched" in p.name.lower() else 0, p.name))

    # Create schema + import
    from app.harness.csv_import import create_schema, import_csvs, create_indexes
    from app.enrichment.schema import add_enrichment_columns

    conn = get_connection(db_path)

    t0 = time.time()
    logger.info("Creating schema...")
    create_schema(conn)

    logger.info("Importing %d CSV files...", len(csv_paths))
    import_csvs(conn, csv_paths)

    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    logger.info("Imported %d listings in %.1fs", total, time.time() - t0)

    logger.info("Creating indexes...")
    create_indexes(conn)

    logger.info("Adding enrichment columns...")
    add_enrichment_columns(conn)

    # Verify v3.1 data landed
    checks = {
        "nearest_stop_name": "transit stop names",
        "price_per_m2": "price per m²",
        "municipality_name": "municipality names",
        "population_density": "population density",
    }
    print(f"\n{'='*60}")
    print(f"  IMPORT SUMMARY  ({total:,} total listings)")
    print(f"{'='*60}")
    for col, label in checks.items():
        filled = conn.execute(
            f"SELECT COUNT(*) FROM listings WHERE {col} IS NOT NULL"
        ).fetchone()[0]
        pct = filled / total * 100 if total else 0
        status = "OK" if filled > 0 else "EMPTY"
        print(f"  {label:<30} {filled:>6} / {total} ({pct:5.1f}%) [{status}]")

    # Run global score enrichment
    if not args.skip_enrichment:
        logger.info("Running global_score enrichment...")
        t1 = time.time()
        from app.enrichment.global_score import enrich_global_score
        stats = enrich_global_score(conn, force=True)
        logger.info("Global score done in %.1fs: %s", time.time() - t1, stats)
    else:
        logger.info("Skipping enrichment (--skip-enrichment)")

    # Full coverage report
    from scripts.enrich_listings import step_report
    step_report(conn)

    conn.close()
    logger.info("Done. DB at %s", db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
