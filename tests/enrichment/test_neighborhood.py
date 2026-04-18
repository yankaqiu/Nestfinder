"""
Test: Step 6 -- Neighborhood metadata (PLZ-based urban/rural classification)

Demonstrates real impact:
  - 'lively' signal uses is_urban=1 to boost urban listings even when
    description doesn't mention restaurants or nightlife.
  - 'quiet' signal uses is_urban=0 to boost rural/suburban listings.
  - This covers ALL listings with a postal_code, not just those with keywords.

Run:  uv run python -m tests.enrichment.test_neighborhood
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("data/listings.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# Swiss urban municipalities (major cities) by PLZ prefix
# Source: BFS Gemeindetypologie + common knowledge
URBAN_PLZS = {
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
}


def test_urban_classification_logic():
    """Show how PLZ-based urban classification helps ranking signals."""
    conn = get_conn()

    print("=" * 70)
    print("TEST: Urban/rural classification by PLZ")
    print("=" * 70)

    # Count how many listings have postal codes
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    with_plz = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE postal_code IS NOT NULL"
    ).fetchone()[0]

    print(f"  Total listings:     {total}")
    print(f"  With postal_code:   {with_plz} ({with_plz/total*100:.1f}%)")

    # Classify current listings
    urban_count = 0
    rural_count = 0
    for row in conn.execute(
        "SELECT postal_code FROM listings WHERE postal_code IS NOT NULL"
    ).fetchall():
        if row["postal_code"] in URBAN_PLZS:
            urban_count += 1
        else:
            rural_count += 1

    print(f"  Urban (city center): {urban_count}")
    print(f"  Suburban/rural:      {rural_count}")
    print()

    # Show examples of urban listings without 'lively' keywords
    urban_no_keyword = conn.execute("""
        SELECT listing_id, title, postal_code, city
        FROM listings
        WHERE postal_code IN ({})
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%restaurant%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%café%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%nightlife%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%belebt%'
        LIMIT 5
    """.format(",".join(f"'{p}'" for p in list(URBAN_PLZS)[:30]))).fetchall()

    if urban_no_keyword:
        print("  Urban listings that DON'T mention restaurants/nightlife:")
        for r in urban_no_keyword:
            print(f"    [{r['postal_code']} {r['city'] or '?'}] {r['title'][:50]}")
            print(f"      -> is_urban=1 enables 'lively' signal. Regex would miss.")

    # Show rural listings without 'quiet' keywords
    rural_no_keyword = conn.execute("""
        SELECT listing_id, title, postal_code, city
        FROM listings
        WHERE postal_code IS NOT NULL
          AND postal_code NOT IN ({})
          AND city IS NOT NULL
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%ruhig%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%quiet%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%calme%'
        LIMIT 5
    """.format(",".join(f"'{p}'" for p in list(URBAN_PLZS)[:30]))).fetchall()

    if rural_no_keyword:
        print("\n  Non-urban listings that DON'T mention 'ruhig'/'quiet':")
        for r in rural_no_keyword:
            print(f"    [{r['postal_code']} {r['city']}] {r['title'][:50]}")
            print(f"      -> is_urban=0 enables 'quiet' signal. Regex would miss.")

    print()
    conn.close()
    return urban_count > 0 and rural_count > 0


def test_coverage_after_enrichment():
    """Show is_urban coverage after enrichment."""
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    filled = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE is_urban IS NOT NULL"
    ).fetchone()[0]

    print("=" * 70)
    print(f"NEIGHBORHOOD COVERAGE  ({total} total listings)")
    print("=" * 70)
    pct = filled / total * 100
    bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
    print(f"  is_urban  [{bar}] {filled:>6} ({pct:5.1f}%)")
    if filled == 0:
        print("  -> Run enrichment first: python -m scripts.enrich_listings --step neighborhood")
    print()
    conn.close()


if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        sys.exit(1)

    results = []
    results.append(("urban/rural classification", test_urban_classification_logic()))
    test_coverage_after_enrichment()

    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    for name, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}")

    sys.exit(0 if all(ok for _, ok in results) else 1)
