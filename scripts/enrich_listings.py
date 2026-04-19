"""Batch enrichment script for listings data.

Usage:
    python -m scripts.enrich_listings [--step STEP_NAME] [--db PATH]

Runs enrichment steps on the listings SQLite DB. Each step is idempotent
and tracks its own progress so it can be resumed.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("enrich")

DEFAULT_DB = Path("data/listings.db")


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def step_schema(conn: sqlite3.Connection) -> None:
    """Add enrichment columns to the DB schema."""
    from app.enrichment.schema import add_enrichment_columns
    add_enrichment_columns(conn)


def step_backfill(conn: sqlite3.Connection) -> None:
    """Backfill from existing data (orig_data_json, price metrics)."""
    from app.enrichment.backfill_existing import run_backfill
    stats = run_backfill(conn)
    logger.info("Backfill complete: %s", stats)


def step_geospatial(conn: sqlite3.Connection) -> None:
    """Enrich with municipality + lake distance via geo.admin.ch."""
    from app.enrichment.geospatial import enrich_geospatial
    stats = enrich_geospatial(conn)
    logger.info("Geospatial complete: %s", stats)


def step_text_extract(conn: sqlite3.Connection) -> None:
    """Extract features from descriptions via Claude Haiku."""
    from app.enrichment.text_extraction import enrich_text_features
    stats = enrich_text_features(conn)
    logger.info("Text extraction complete: %s", stats)


def step_neighborhood(conn: sqlite3.Connection) -> None:
    """Tag urban/rural from PLZ."""
    from app.enrichment.neighborhood import enrich_neighborhood
    stats = enrich_neighborhood(conn)
    logger.info("Neighborhood complete: %s", stats)


def step_global_score(conn: sqlite3.Connection) -> None:
    """Compute the global listing quality score."""
    from app.enrichment.global_score import enrich_global_score
    stats = enrich_global_score(conn)
    logger.info("Global score complete: %s", stats)


def step_report(conn: sqlite3.Connection) -> None:
    """Print a coverage report of enrichment columns."""
    enrichment_cols = [
        "floor_level", "year_built", "renovation_year", "is_furnished",
        "price_per_sqm", "price_vs_city_median",
        "municipality", "bfs_number", "lake_distance_m", "is_urban",
        "text_features_json",
        "nearest_stop_name", "nearest_stop_distance_m",
        "nearest_train_name", "nearest_train_distance_m",
        "nearest_hb_name", "nearest_hb_distance_m",
        "municipality_name", "district_name", "canton_name",
        "population_total", "population_density", "population_density_bucket",
        "price_per_m2", "price_per_m2_vs_municipality",
        "avg_price_per_m2_municipality", "avg_price_per_m2_district",
        "global_score", "score_value", "score_amenity", "score_location",
        "score_building", "score_completeness", "score_freshness",
        "score_transit",
    ]
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"  ENRICHMENT COVERAGE REPORT  ({total} total listings)")
    print(f"{'='*60}")
    for col in enrichment_cols:
        try:
            filled = conn.execute(
                f"SELECT COUNT(*) FROM listings WHERE {col} IS NOT NULL"
            ).fetchone()[0]
            pct = filled / total * 100 if total else 0
            bar = "#" * int(pct / 5) + " " * (20 - int(pct / 5))
            print(f"  {col:<30} [{bar}] {filled:>6} ({pct:5.1f}%)")
        except sqlite3.OperationalError:
            print(f"  {col:<30} [--- column missing ---]")
    print(f"{'='*60}\n")

    # City/canton coverage before vs after
    city_count = conn.execute("SELECT COUNT(*) FROM listings WHERE city IS NOT NULL").fetchone()[0]
    canton_count = conn.execute("SELECT COUNT(*) FROM listings WHERE canton IS NOT NULL").fetchone()[0]
    print(f"  City coverage:   {city_count}/{total} ({city_count/total*100:.1f}%)")
    print(f"  Canton coverage: {canton_count}/{total} ({canton_count/total*100:.1f}%)")
    print()


STEPS = {
    "schema": step_schema,
    "backfill": step_backfill,
    "geospatial": step_geospatial,
    "text_extract": step_text_extract,
    "neighborhood": step_neighborhood,
    "global_score": step_global_score,
    "report": step_report,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich listings DB")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument(
        "--step",
        choices=list(STEPS.keys()) + ["all"],
        default="all",
        help="Which enrichment step to run",
    )
    args = parser.parse_args()

    if not args.db.exists():
        logger.error("DB not found: %s", args.db)
        return 1

    conn = get_connection(args.db)

    if args.step == "all":
        steps_to_run = ["schema", "backfill", "neighborhood", "global_score", "report"]
    else:
        steps_to_run = [args.step]

    for step_name in steps_to_run:
        logger.info("Running step: %s", step_name)
        t0 = time.time()
        STEPS[step_name](conn)
        logger.info("Step %s completed in %.1fs", step_name, time.time() - t0)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
