"""Neighborhood metadata enrichment via PLZ-based classification.

Tags each listing as urban/suburban/rural based on postal code.
No external API calls -- purely a static lookup.
"""
from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

# Major Swiss city center PLZs (BFS urban core municipalities)
URBAN_PLZS: set[str] = {
    # Zürich
    "8001", "8002", "8003", "8004", "8005", "8006", "8008", "8032", "8037",
    "8038", "8044", "8045", "8046", "8047", "8048", "8049", "8050", "8051",
    "8052", "8053", "8055", "8057", "8064",
    # Bern
    "3001", "3003", "3004", "3005", "3006", "3007", "3008", "3010", "3011",
    "3012", "3013", "3014", "3015",
    # Basel
    "4001", "4051", "4052", "4053", "4054", "4055", "4056", "4057", "4058",
    "4059",
    # Geneva
    "1201", "1202", "1203", "1204", "1205", "1206", "1207", "1208", "1209",
    # Lausanne
    "1003", "1004", "1005", "1006", "1007", "1010", "1012", "1018",
    # Luzern
    "6003", "6004", "6005", "6006",
    # St. Gallen
    "9000", "9008", "9010", "9011", "9012", "9014",
    # Winterthur
    "8400", "8401", "8402", "8404", "8405", "8406", "8408", "8409", "8410",
    # Lugano
    "6900", "6901", "6902", "6903",
    # Biel/Bienne
    "2501", "2502", "2503", "2504", "2505",
    # Thun
    "3600", "3604", "3608",
    # Fribourg
    "1700", "1701", "1708",
    # Neuchâtel
    "2000", "2001",
    # Sion
    "1950", "1951",
    # La Chaux-de-Fonds
    "2300", "2301",
}


def enrich_neighborhood(conn: sqlite3.Connection) -> dict[str, int]:
    """Set is_urban based on postal_code for all listings."""
    stats = {"urban": 0, "non_urban": 0, "no_plz": 0}

    rows = conn.execute(
        "SELECT listing_id, postal_code FROM listings WHERE is_urban IS NULL"
    ).fetchall()

    if not rows:
        logger.info("All listings already have is_urban set")
        return stats

    updates: list[tuple[int | None, str]] = []
    for lid, plz in rows:
        if not plz:
            stats["no_plz"] += 1
            continue
        is_urban = 1 if plz.strip() in URBAN_PLZS else 0
        updates.append((is_urban, lid))
        if is_urban:
            stats["urban"] += 1
        else:
            stats["non_urban"] += 1

    if updates:
        conn.executemany(
            "UPDATE listings SET is_urban = ? WHERE listing_id = ?",
            updates,
        )
        conn.commit()

    logger.info("Neighborhood enrichment done: %s", stats)
    return stats
