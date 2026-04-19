"""Schema extensions for enriched listing data.

Adds new columns to the existing listings table via ALTER TABLE.
Safe to run multiple times -- skips columns that already exist.
"""
from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

ENRICHMENT_COLUMNS: list[tuple[str, str]] = [
    # --- Backfilled from orig_data_json (Step 0) ---
    ("floor_level", "INTEGER"),
    ("year_built", "INTEGER"),
    ("renovation_year", "INTEGER"),
    ("is_furnished", "INTEGER"),
    ("price_per_sqm", "REAL"),
    ("price_vs_city_median", "REAL"),

    # --- Text-extracted features (Step 2) ---
    ("text_features_json", "TEXT"),

    # --- Geospatial via geo.admin.ch (Step 4) ---
    ("municipality", "TEXT"),
    ("bfs_number", "INTEGER"),
    ("lake_distance_m", "INTEGER"),

    # --- Neighborhood metadata (Step 6) ---
    ("is_urban", "INTEGER"),

    # --- v3.1: transit proximity ---
    ("nearest_stop_name", "TEXT"),
    ("nearest_stop_distance_m", "REAL"),
    ("nearest_train_name", "TEXT"),
    ("nearest_train_distance_m", "REAL"),
    ("nearest_hb_name", "TEXT"),
    ("nearest_hb_distance_m", "REAL"),

    # --- v3.1: administrative geography ---
    ("municipality_code", "INTEGER"),
    ("district_code", "INTEGER"),
    ("canton_code", "INTEGER"),
    ("municipality_name", "TEXT"),
    ("district_name", "TEXT"),
    ("canton_name", "TEXT"),
    ("municipality_name_demo", "TEXT"),

    # --- v3.1: demographics ---
    ("population_total", "INTEGER"),
    ("area_ha", "REAL"),
    ("area_km2", "REAL"),
    ("population_density", "REAL"),
    ("population_density_bucket", "TEXT"),

    # --- v3.1: multi-level price benchmarking ---
    ("price_per_m2", "REAL"),
    ("avg_price_per_m2_municipality", "REAL"),
    ("avg_price_per_m2_district", "REAL"),
    ("avg_price_per_m2_canton", "REAL"),
    ("price_per_m2_vs_municipality", "REAL"),
    ("price_per_m2_vs_district", "REAL"),
    ("price_per_m2_vs_canton", "REAL"),
    ("price_per_m2_vs_municipality_label", "TEXT"),

    # --- Global quality score (Step 8) ---
    ("global_score", "REAL"),
    ("score_value", "REAL"),
    ("score_amenity", "REAL"),
    ("score_location", "REAL"),
    ("score_building", "REAL"),
    ("score_completeness", "REAL"),
    ("score_freshness", "REAL"),
    ("score_transit", "REAL"),
]


def add_enrichment_columns(conn: sqlite3.Connection) -> None:
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(listings)").fetchall()
    }
    added = 0
    for col_name, col_type in ENRICHMENT_COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}")
            added += 1
    conn.commit()
    if added:
        logger.info("Added %d enrichment columns to listings table", added)
    else:
        logger.info("All enrichment columns already present")
