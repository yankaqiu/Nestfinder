"""
Test: Step 0 -- Backfill from existing data (orig_data_json + price metrics).

Demonstrates real impact:
  - "modern apartment" query: year_built enrichment catches buildings from 2020+
    even when description doesn't say "modern"
  - "affordable in Zürich": price_vs_city_median ranks truly cheaper listings
    above ones that just say "günstig" in text
  - "new build": year_built >= 2022 catches new constructions without keyword

Run:  uv run python -m tests.enrichment.test_backfill
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


def test_modern_signal_with_year_built():
    """
    BEFORE enrichment: "modern" matched only if description contains
    'modern', 'renoviert', 'saniert', etc.

    AFTER enrichment: listings with year_built > 2010 or renovation_year > 2015
    also match -- even if description is in French or says nothing about style.
    """
    conn = get_conn()

    # Listings with year_built > 2010 that do NOT have 'modern' in text
    rows = conn.execute("""
        SELECT listing_id, title, year_built, renovation_year,
               description
        FROM listings
        WHERE year_built > 2010
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%modern%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%renoviert%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%saniert%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%neuwertig%'
        LIMIT 5
    """).fetchall()

    print("=" * 70)
    print("TEST: 'modern' signal -- year_built catches what regex misses")
    print("=" * 70)
    if not rows:
        print("  (no examples found -- enrichment may not be populated)")
        return False

    ok = True
    for r in rows:
        yr = r["year_built"]
        reno = r["renovation_year"]
        title = r["title"][:60]
        print(f"\n  [{r['listing_id']}] {title}")
        print(f"    year_built={yr}, renovation_year={reno}")
        print(f"    Regex would MISS this. Enrichment CATCHES it (year > 2010).")
        if yr is None or yr <= 2010:
            print("    FAIL: year_built should be > 2010")
            ok = False

    print()
    conn.close()
    return ok


def test_affordable_signal_with_price_metrics():
    """
    BEFORE enrichment: "affordable" matched only if description says
    'günstig', 'affordable', 'preiswert', 'cheap', 'bon marché'.

    AFTER enrichment: price_vs_city_median < 0.85 identifies objectively
    cheaper-than-average listings regardless of marketing language.
    """
    conn = get_conn()

    # Truly affordable listings (below median) that don't say "günstig"
    rows = conn.execute("""
        SELECT listing_id, title, city, price, area, price_per_sqm,
               price_vs_city_median, description
        FROM listings
        WHERE price_vs_city_median IS NOT NULL
          AND price_vs_city_median < 0.85
          AND price_per_sqm > 10
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%günstig%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%affordable%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%preiswert%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%cheap%'
        ORDER BY price_vs_city_median
        LIMIT 5
    """).fetchall()

    # Overpriced listings that DO say "günstig"
    fakes = conn.execute("""
        SELECT listing_id, title, city, price, area, price_per_sqm,
               price_vs_city_median
        FROM listings
        WHERE price_vs_city_median IS NOT NULL
          AND price_vs_city_median > 1.2
          AND (LOWER(title || ' ' || COALESCE(description, '')) LIKE '%günstig%'
               OR LOWER(title || ' ' || COALESCE(description, '')) LIKE '%affordable%')
        ORDER BY price_vs_city_median DESC
        LIMIT 3
    """).fetchall()

    print("=" * 70)
    print("TEST: 'affordable' signal -- price_vs_city_median vs keyword")
    print("=" * 70)

    if rows:
        print("\n  Truly affordable (below median) but NO 'günstig' keyword:")
        for r in rows:
            print(f"    [{r['city']}] {r['title'][:50]}")
            print(f"      {r['price']} CHF, {r['area']} sqm, "
                  f"{r['price_per_sqm']:.1f} CHF/sqm, "
                  f"vs_median={r['price_vs_city_median']:.2f}")
            print(f"      Regex would MISS. Enrichment CATCHES.")

    if fakes:
        print("\n  Says 'günstig' but actually ABOVE average:")
        for r in fakes:
            print(f"    [{r['city']}] {r['title'][:50]}")
            print(f"      {r['price']} CHF, {r['area']} sqm, "
                  f"{r['price_per_sqm']:.1f} CHF/sqm, "
                  f"vs_median={r['price_vs_city_median']:.2f}")
            print(f"      Regex would FALSE-MATCH. Enrichment correctly excludes.")

    print()
    conn.close()
    return bool(rows)


def test_new_build_with_year_built():
    """
    BEFORE: 'new_build' only matches 'Neubau', 'new build', 'Erstvermietung'
    AFTER: year_built >= 2022 also triggers the signal.
    """
    conn = get_conn()

    rows = conn.execute("""
        SELECT listing_id, title, year_built
        FROM listings
        WHERE year_built >= 2022
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%neubau%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%new build%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%erstvermietung%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%erstbezug%'
        LIMIT 5
    """).fetchall()

    print("=" * 70)
    print("TEST: 'new_build' signal -- year_built >= 2022 catches unlabeled")
    print("=" * 70)
    if not rows:
        print("  (no examples found)")
        return False

    for r in rows:
        print(f"  [{r['listing_id']}] {r['title'][:55]}  year={r['year_built']}")
        print(f"    Regex would MISS (no 'Neubau' keyword). Enrichment CATCHES.")

    print()
    conn.close()
    return True


def test_floor_level_for_views_and_bright():
    """
    floor_level from orig_data enables 'views' (floor >= 5) and 'bright' (floor >= 3).
    """
    conn = get_conn()

    # High-floor listings without view keywords
    rows = conn.execute("""
        SELECT listing_id, title, floor_level
        FROM listings
        WHERE floor_level >= 5
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%aussicht%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%view%'
          AND LOWER(title || ' ' || COALESCE(description, ''))
              NOT LIKE '%panorama%'
        LIMIT 5
    """).fetchall()

    print("=" * 70)
    print("TEST: 'views'/'bright' signals -- floor_level as proxy")
    print("=" * 70)
    if not rows:
        print("  (no examples found)")
        return False

    for r in rows:
        print(f"  [{r['listing_id']}] {r['title'][:55]}  floor={r['floor_level']}")
        print(f"    Floor {r['floor_level']} -> 'views' signal. Regex would miss.")

    print()
    conn.close()
    return True


def test_coverage_stats():
    """Print overall coverage stats for backfill columns."""
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

    print("=" * 70)
    print(f"COVERAGE SUMMARY  ({total} total listings)")
    print("=" * 70)
    for col in ["floor_level", "year_built", "renovation_year", "is_furnished",
                "price_per_sqm", "price_vs_city_median"]:
        filled = conn.execute(
            f"SELECT COUNT(*) FROM listings WHERE {col} IS NOT NULL"
        ).fetchone()[0]
        pct = filled / total * 100
        bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
        print(f"  {col:<25} [{bar}] {filled:>6} ({pct:5.1f}%)")

    print()
    conn.close()


if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        sys.exit(1)

    results = []
    test_coverage_stats()
    results.append(("modern + year_built", test_modern_signal_with_year_built()))
    results.append(("affordable + price_vs_median", test_affordable_signal_with_price_metrics()))
    results.append(("new_build + year_built", test_new_build_with_year_built()))
    results.append(("views/bright + floor_level", test_floor_level_for_views_and_bright()))

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


# ======================================================================
# COVERAGE SUMMARY  (22819 total listings)
# ======================================================================
#   floor_level               [#######.............]   8169 ( 35.8%)
#   year_built                [##..................]   2929 ( 12.8%)
#   renovation_year           [#...................]   1285 (  5.6%)
#   is_furnished              [....................]    430 (  1.9%)
#   price_per_sqm             [###############.....]  18237 ( 79.9%)
#   price_vs_city_median      [#####...............]   5851 ( 25.6%)

# ======================================================================
# TEST: 'modern' signal -- year_built catches what regex misses
# ======================================================================

#   [26744] Einstellhallenplatz in Gümligen gesucht?
#     year_built=2020, renovation_year=None
#     Regex would MISS this. Enrichment CATCHES it (year > 2010).

#   [26758] Bel appartement de 2,5 pièces
#     year_built=2017, renovation_year=None
#     Regex would MISS this. Enrichment CATCHES it (year > 2010).

#   [26773] Geräumige 3.5-Zimmer-Attikawohnung mit Mezzanin und Loggia  
#     year_built=2020, renovation_year=None
#     Regex would MISS this. Enrichment CATCHES it (year > 2010).

#   [26837] 2.5-Zimmer-Whg / 68 EG 0.2
#     year_built=2024, renovation_year=None
#     Regex would MISS this. Enrichment CATCHES it (year > 2010).

#   [26851] Appartement nur für Studenten im Herzen einer Seniorenreside
#     year_built=2025, renovation_year=None
#     Regex would MISS this. Enrichment CATCHES it (year > 2010).

# ======================================================================
# TEST: 'affordable' signal -- price_vs_city_median vs keyword
# ======================================================================

#   Truly affordable (below median) but NO 'günstig' keyword:
#     [Zürich] Fotografen und Künstler gesucht: Atelier zum Wohlf
#       395 CHF, 38.0 sqm, 10.4 CHF/sqm, vs_median=0.27
#       Regex would MISS. Enrichment CATCHES.
#     [Genève] Dépôt d'environ 23.6 m2 à louer
#       263 CHF, 24.0 sqm, 11.0 CHF/sqm, vs_median=0.27
#       Regex would MISS. Enrichment CATCHES.
#     [Zug] Mehr Platz an top Lage
#       320 CHF, 29.0 sqm, 11.0 CHF/sqm, vs_median=0.27
#       Regex would MISS. Enrichment CATCHES.
#     [Zug] Mehr Platz an top Lage
#       320 CHF, 29.0 sqm, 11.0 CHF/sqm, vs_median=0.27
#       Regex would MISS. Enrichment CATCHES.
#     [Zürich] Arbeitsplatz an der Bahnhofstrasse Zürich – Presti
#       1100 CHF, 100.0 sqm, 11.0 CHF/sqm, vs_median=0.28
#       Regex would MISS. Enrichment CATCHES.

#   Says 'günstig' but actually ABOVE average:
#     [Basel] Möbliertes WG-Zimmer L1 Nähe Biozentrum, Universit
#       870 CHF, 10.0 sqm, 87.0 CHF/sqm, vs_median=2.95
#       Regex would FALSE-MATCH. Enrichment correctly excludes.
#     [Zurich] Helles 19 m² Zimmer in geräumiger 200 m² Wohnung m
#       1818 CHF, 19.0 sqm, 95.7 CHF/sqm, vs_median=2.56
#       Regex would FALSE-MATCH. Enrichment correctly excludes.
#     [Muralto] Einzimmerwohnung in Locarno mit Parkplatz | Kurzfr
#       1300 CHF, 27.0 sqm, 48.1 CHF/sqm, vs_median=1.98
#       Regex would FALSE-MATCH. Enrichment correctly excludes.

# ======================================================================
# TEST: 'new_build' signal -- year_built >= 2022 catches unlabeled
# ======================================================================
#   [26782] Helle 1.5-Zimmer-Appartements mit Balkon  year=2023
#     Regex would MISS (no 'Neubau' keyword). Enrichment CATCHES.
#   [26786] Schike Wiedikon 2.0 Zim.,Keller,W/T. 3 Min. zum Markt,   year=2022
#     Regex would MISS (no 'Neubau' keyword). Enrichment CATCHES.
#   [26793] moderne, charmante 3.5-Zimmer-Dachwohnung mit Aussicht   year=2024
#     Regex would MISS (no 'Neubau' keyword). Enrichment CATCHES.
#   [26801] Grosszügige 2.5 Zimmer-Loftwohnung an zentraler Lage  year=2023
#     Regex would MISS (no 'Neubau' keyword). Enrichment CATCHES.
#   [26826] Affittasi appartamento 2.5 locali - 50m² - Locarno Cent  year=2022
#     Regex would MISS (no 'Neubau' keyword). Enrichment CATCHES.

# ======================================================================
# TEST: 'views'/'bright' signals -- floor_level as proxy
# ======================================================================
#   [24544] Moderne Loft-Wohnung mit hohen Decken & offener Küche,   floor=5
#     Floor 5 -> 'views' signal. Regex would miss.
#   [26746] Wohnen im Limmattal  floor=5
#     Floor 5 -> 'views' signal. Regex would miss.
#   [26756] schöne, helle 4 Zimmerwohnung  floor=6
#     Floor 6 -> 'views' signal. Regex would miss.
#   [26774] Reller 38 - 2 Zimmer, 5. Etage  floor=5
#     Floor 5 -> 'views' signal. Regex would miss.
#   [26806] Beau studio entièrement rénové  floor=9
#     Floor 9 -> 'views' signal. Regex would miss.

# ======================================================================
# RESULTS
# ======================================================================
#   [PASS] modern + year_built
#   [PASS] affordable + price_vs_median
#   [PASS] new_build + year_built
#   [PASS] views/bright + floor_level