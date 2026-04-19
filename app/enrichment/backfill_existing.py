"""Backfill enrichment columns from data already in the DB.

No external API calls -- just mining orig_data_json, location_address_json,
and computing derived metrics like price_per_sqm.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)


def _parse_floor(value: str | None) -> int | None:
    """Parse floor from Comparis MainData format like '3. Stock', 'EG', etc."""
    if not value or value == "nicht verfügbar":
        return None
    v = value.strip().lower()
    if v in ("eg", "erdgeschoss", "rez-de-chaussée", "ground floor"):
        return 0
    if v in ("ug", "untergeschoss", "sous-sol", "basement"):
        return -1
    m = re.search(r"(\d+)", v)
    return int(m.group(1)) if m else None


def _parse_year(value: str | None) -> int | None:
    if not value or value == "nicht verfügbar":
        return None
    m = re.search(r"(\d{4})", str(value))
    if m:
        year = int(m.group(1))
        if 1500 <= year <= 2030:
            return year
    return None


def _parse_bool_value(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "ja", "oui"):
        return 1
    if s in ("false", "0", "no", "nein", "non"):
        return 0
    return None


def _extract_main_data(orig_data: dict) -> dict[str, Any]:
    md_list = orig_data.get("MainData", [])
    if not isinstance(md_list, list):
        return {}
    return {
        item.get("Key"): item.get("Value")
        for item in md_list
        if isinstance(item, dict) and item.get("Key")
    }


def backfill_from_orig_data(conn: sqlite3.Connection) -> dict[str, int]:
    """Extract floor, year_built, renovation_year, is_furnished from orig_data_json."""
    stats = {"floor_filled": 0, "year_filled": 0, "renovation_filled": 0, "furnished_filled": 0}

    rows = conn.execute(
        "SELECT listing_id, orig_data_json FROM listings "
        "WHERE orig_data_json IS NOT NULL"
    ).fetchall()

    updates: list[tuple] = []
    for listing_id, raw_json in rows:
        try:
            orig = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError):
            continue

        md = _extract_main_data(orig)
        if not md:
            continue

        floor = _parse_floor(md.get("Floor"))
        year = _parse_year(md.get("YearOfConstruction"))
        reno = _parse_year(md.get("RenovationYear"))
        furnished = _parse_bool_value(md.get("IsFurnished"))

        if any(v is not None for v in (floor, year, reno, furnished)):
            updates.append((floor, year, reno, furnished, listing_id))
            if floor is not None:
                stats["floor_filled"] += 1
            if year is not None:
                stats["year_filled"] += 1
            if reno is not None:
                stats["renovation_filled"] += 1
            if furnished is not None:
                stats["furnished_filled"] += 1

    if updates:
        conn.executemany(
            "UPDATE listings SET floor_level = ?, year_built = ?, "
            "renovation_year = ?, is_furnished = ? WHERE listing_id = ?",
            updates,
        )
        conn.commit()

    logger.info("Backfilled from orig_data: %s", stats)
    return stats


def backfill_city_canton(conn: sqlite3.Connection) -> dict[str, int]:
    """Backfill missing city/canton from location_address_json."""
    stats = {"city_filled": 0, "canton_filled": 0}

    rows = conn.execute(
        "SELECT listing_id, location_address_json FROM listings "
        "WHERE (city IS NULL OR canton IS NULL) AND location_address_json IS NOT NULL"
    ).fetchall()

    updates: list[tuple] = []
    for listing_id, loc_json in rows:
        try:
            loc = json.loads(loc_json)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(loc, dict) or not loc:
            continue

        city = loc.get("City") or loc.get("city")
        canton = loc.get("canton") or loc.get("Canton") or loc.get("state")
        if city:
            city = city.strip()
        if canton:
            canton = canton.strip().upper()

        if city or canton:
            updates.append((city, canton, listing_id))
            if city:
                stats["city_filled"] += 1
            if canton:
                stats["canton_filled"] += 1

    if updates:
        conn.executemany(
            "UPDATE listings SET "
            "city = COALESCE(city, ?), "
            "canton = COALESCE(canton, ?) "
            "WHERE listing_id = ?",
            updates,
        )
        conn.commit()

    logger.info("Backfilled city/canton: %s", stats)
    return stats


def compute_price_metrics(conn: sqlite3.Connection) -> dict[str, int]:
    """Compute price_per_sqm and price_vs_city_median.

    Skips price_per_sqm for rows where v3.1 price_per_m2 is already present.
    """
    stats = {"price_per_sqm_filled": 0, "price_vs_median_filled": 0}

    conn.execute(
        "UPDATE listings SET price_per_sqm = ROUND(CAST(price AS REAL) / area, 2) "
        "WHERE price_per_sqm IS NULL AND price_per_m2 IS NULL "
        "AND price IS NOT NULL AND area IS NOT NULL AND area >= 10 "
        "AND price > 100 AND price < 50000"
    )
    stats["price_per_sqm_filled"] = conn.execute(
        "SELECT changes()"
    ).fetchone()[0]

    city_medians = conn.execute(
        "SELECT city, AVG(price_per_sqm) FROM listings "
        "WHERE city IS NOT NULL AND price_per_sqm IS NOT NULL "
        "AND price_per_sqm > 5 AND price_per_sqm < 100 "
        "GROUP BY city HAVING COUNT(*) >= 3"
    ).fetchall()

    for city, median in city_medians:
        if median and median > 0:
            conn.execute(
                "UPDATE listings SET price_vs_city_median = ROUND(price_per_sqm / ?, 3) "
                "WHERE city = ? AND price_per_sqm IS NOT NULL",
                (median, city),
            )

    stats["price_vs_median_filled"] = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE price_vs_city_median IS NOT NULL"
    ).fetchone()[0]

    conn.commit()
    logger.info("Price metrics: %s", stats)
    return stats


TEXT_FEATURE_TO_COLUMN = {
    "balcony": "feature_balcony",
    "elevator": "feature_elevator",
    "parking": "feature_parking",
    "fireplace": "feature_fireplace",
    "pets_allowed": "feature_pets_allowed",
    "wheelchair_accessible": "feature_wheelchair_accessible",
    "minergie": "feature_minergie_certified",
    "washing_machine": "feature_private_laundry",
    "furnished": "is_furnished",
}


def backfill_features_from_text(conn: sqlite3.Connection) -> dict[str, int]:
    """Fill NULL feature_* columns using text_features_json from Claude extraction.

    Only sets a column to 1 if it's currently NULL and Claude detected True.
    Never overwrites existing structured data.
    """
    stats: dict[str, int] = {}

    rows = conn.execute(
        "SELECT listing_id, text_features_json FROM listings "
        "WHERE text_features_json IS NOT NULL"
    ).fetchall()

    if not rows:
        logger.info("No text_features_json to backfill from")
        return stats

    for col_name in TEXT_FEATURE_TO_COLUMN.values():
        stats[col_name] = 0

    for listing_id, raw_json in rows:
        try:
            feats = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(feats, dict):
            continue

        sets = []
        for text_key, col_name in TEXT_FEATURE_TO_COLUMN.items():
            if feats.get(text_key) is True:
                sets.append(col_name)

        if sets:
            set_clause = ", ".join(f"{c} = COALESCE({c}, 1)" for c in sets)
            conn.execute(
                f"UPDATE listings SET {set_clause} WHERE listing_id = ?",
                (listing_id,),
            )
            for c in sets:
                stats[c] += 1

    conn.commit()
    total = sum(stats.values())
    logger.info("Backfilled %d feature values from text extraction: %s", total, stats)
    return stats


def run_backfill(conn: sqlite3.Connection) -> dict[str, Any]:
    """Run all backfill operations. Returns combined stats."""
    all_stats: dict[str, Any] = {}
    all_stats["orig_data"] = backfill_from_orig_data(conn)
    all_stats["city_canton"] = backfill_city_canton(conn)
    all_stats["price_metrics"] = compute_price_metrics(conn)
    all_stats["features_from_text"] = backfill_features_from_text(conn)
    return all_stats
