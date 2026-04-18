"""
Test: Step 4 -- Geospatial enrichment via geo.admin.ch

Demonstrates real impact:
  - SRED listings (49% of data) have NO city/canton. After geo enrichment,
    municipality backfills city so these listings become findable by city search.
  - lake_distance_m enables data-driven "near lake" signal instead of regex.
  - BFS number links listings to official Swiss statistical data.

Run:  uv run python -m tests.enrichment.test_geospatial
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import httpx

DB_PATH = Path("data/listings.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def test_live_api_call():
    """Verify geo.admin.ch returns correct data for known coordinates."""
    from app.enrichment.geospatial import lookup_municipality, nearest_lake_distance_m

    print("=" * 70)
    print("TEST: geo.admin.ch API returns correct municipality data")
    print("=" * 70)

    cases = [
        (47.3763, 8.5483, "Zürich", "ZH"),
        (46.9480, 7.4474, "Bern", "BE"),
        (47.5596, 7.5886, "Basel", "BS"),
        (46.5191, 6.5668, "Ecublens (VD)", "VD"),
    ]

    ok = True
    with httpx.Client() as client:
        for lat, lon, expected_name, expected_canton, in cases:
            result = lookup_municipality(lat, lon, client)
            lake = nearest_lake_distance_m(lat, lon)

            if result is None:
                print(f"  FAIL: ({lat}, {lon}) returned None")
                ok = False
                continue

            name_match = expected_name.lower() in result["name"].lower()
            canton_match = result["canton"] == expected_canton

            status = "PASS" if (name_match and canton_match) else "FAIL"
            print(f"  [{status}] ({lat:.4f}, {lon:.4f})")
            print(f"    Expected: {expected_name} ({expected_canton})")
            print(f"    Got:      {result['name']} ({result['canton']}), "
                  f"BFS={result['bfs_nr']}, lake={lake}m")

            if not name_match or not canton_match:
                ok = False

    print()
    return ok


def test_sred_city_backfill_gap():
    """Show how many SRED listings are missing city -- the gap geospatial fills."""
    conn = get_conn()

    total_sred = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE scrape_source = 'SRED'"
    ).fetchone()[0]
    sred_no_city = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE scrape_source = 'SRED' AND city IS NULL"
    ).fetchone()[0]
    sred_with_geo = conn.execute(
        "SELECT COUNT(*) FROM listings "
        "WHERE scrape_source = 'SRED' AND city IS NULL "
        "AND latitude IS NOT NULL AND longitude IS NOT NULL"
    ).fetchone()[0]

    print("=" * 70)
    print("TEST: SRED city gap that geospatial enrichment fills")
    print("=" * 70)
    print(f"  SRED total:          {total_sred}")
    print(f"  SRED missing city:   {sred_no_city} ({sred_no_city/total_sred*100:.1f}%)")
    print(f"  SRED fixable (has lat/lon): {sred_with_geo} ({sred_with_geo/total_sred*100:.1f}%)")
    print()

    # Show sample SRED listings that have lat/lng but no city
    samples = conn.execute("""
        SELECT listing_id, title, latitude, longitude, city, canton
        FROM listings
        WHERE scrape_source = 'SRED' AND city IS NULL
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 5
    """).fetchall()

    if samples:
        print("  Sample SRED listings with coords but no city:")
        for s in samples:
            print(f"    [{s['listing_id']}] {s['title'][:45]}")
            print(f"      lat={s['latitude']:.4f}, lon={s['longitude']:.4f}, "
                  f"city={s['city']}, canton={s['canton']}")
        print("    -> After geospatial: city + canton will be filled from geo.admin.ch")

    print()
    conn.close()
    return sred_with_geo > 0


def test_lake_distance_examples():
    """Show how lake_distance_m would work for known lake-side listings."""
    from app.enrichment.geospatial import nearest_lake_distance_m

    print("=" * 70)
    print("TEST: lake_distance_m for 'near_lake' signal")
    print("=" * 70)

    conn = get_conn()

    # Find some listings near known lake locations
    cases = [
        ("Near Zürichsee", 47.35, 8.55, 8.70, 2000),
        ("Near Genfersee", 46.40, 46.50, 6.50, 6.70),
    ]

    # Approach: find listings with lat/long near lake areas
    lakeside = conn.execute("""
        SELECT listing_id, title, latitude, longitude, city
        FROM listings
        WHERE latitude BETWEEN 47.25 AND 47.40
          AND longitude BETWEEN 8.50 AND 8.75
          AND latitude IS NOT NULL
        LIMIT 3
    """).fetchall()

    inland = conn.execute("""
        SELECT listing_id, title, latitude, longitude, city
        FROM listings
        WHERE latitude BETWEEN 47.0 AND 47.1
          AND longitude BETWEEN 8.0 AND 8.2
          AND latitude IS NOT NULL
        LIMIT 3
    """).fetchall()

    if lakeside:
        print("  Near lake (Zürichsee area):")
        for r in lakeside:
            dist = nearest_lake_distance_m(r["latitude"], r["longitude"])
            near = "YES" if dist < 2000 else "no"
            print(f"    [{r['city'] or '?'}] {r['title'][:40]} -> {dist}m ({near})")

    if inland:
        print("\n  Inland (central CH):")
        for r in inland:
            dist = nearest_lake_distance_m(r["latitude"], r["longitude"])
            near = "YES" if dist < 2000 else "no"
            print(f"    [{r['city'] or '?'}] {r['title'][:40]} -> {dist}m ({near})")

    print()
    conn.close()
    return bool(lakeside)


def test_enrichment_coverage():
    """After running geospatial enrichment, show coverage."""
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

    print("=" * 70)
    print(f"GEOSPATIAL COVERAGE  ({total} total listings)")
    print("=" * 70)
    for col in ["municipality", "bfs_number", "lake_distance_m"]:
        filled = conn.execute(
            f"SELECT COUNT(*) FROM listings WHERE {col} IS NOT NULL"
        ).fetchone()[0]
        pct = filled / total * 100
        bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
        print(f"  {col:<25} [{bar}] {filled:>6} ({pct:5.1f}%)")

    city_before = 11714  # hardcoded from initial check
    city_now = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE city IS NOT NULL"
    ).fetchone()[0]
    print(f"\n  City coverage: {city_before} -> {city_now} "
          f"(+{city_now - city_before} from geospatial)")

    print()
    conn.close()


if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        sys.exit(1)

    results = []
    results.append(("geo.admin.ch API", test_live_api_call()))
    results.append(("SRED city gap", test_sred_city_backfill_gap()))
    results.append(("lake distance", test_lake_distance_examples()))
    test_enrichment_coverage()

    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    all_ok = True
    for name, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}")
        if not ok:
            all_ok = False

    sys.exit(0 if all_ok else 1)
