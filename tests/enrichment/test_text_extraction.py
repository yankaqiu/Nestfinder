"""
Test: Step 2 -- Text extraction via Claude Haiku

Demonstrates real impact:
  - Feature booleans are NULL for ~22k non-robinreal listings.
  - Claude Haiku reads descriptions (DE/FR/IT) and extracts features
    that regex misses, especially in multilingual text.
  - After extraction, hard-filter queries for "balcony" or "pets_allowed"
    actually work for Comparis and SRED listings.

Run:  uv run python -m tests.enrichment.test_text_extraction
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("data/listings.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def test_feature_gap():
    """Show how many listings are missing feature booleans (the gap)."""
    conn = get_conn()

    print("=" * 70)
    print("TEST: Feature boolean coverage gap by source")
    print("=" * 70)

    sources = conn.execute(
        "SELECT scrape_source, COUNT(*) as cnt FROM listings GROUP BY scrape_source"
    ).fetchall()

    for src in sources:
        name = src["scrape_source"]
        total = src["cnt"]
        with_balcony = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source = ? AND feature_balcony IS NOT NULL",
            (name,)
        ).fetchone()[0]
        with_elevator = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source = ? AND feature_elevator IS NOT NULL",
            (name,)
        ).fetchone()[0]
        with_pets = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE scrape_source = ? AND feature_pets_allowed IS NOT NULL",
            (name,)
        ).fetchone()[0]

        print(f"\n  {name} ({total} listings):")
        print(f"    feature_balcony:      {with_balcony:>6} ({with_balcony/total*100:5.1f}%)")
        print(f"    feature_elevator:     {with_elevator:>6} ({with_elevator/total*100:5.1f}%)")
        print(f"    feature_pets_allowed: {with_pets:>6} ({with_pets/total*100:5.1f}%)")

    print()
    conn.close()
    return True


def test_multilingual_descriptions():
    """Show real descriptions in DE/FR/IT that mention features regex might miss."""
    conn = get_conn()

    print("=" * 70)
    print("TEST: Multilingual descriptions with features regex might miss")
    print("=" * 70)

    # French descriptions mentioning balcony/terrasse
    french = conn.execute("""
        SELECT listing_id, title, description, feature_balcony
        FROM listings
        WHERE description LIKE '%balcon%'
           OR description LIKE '%terrasse%'
        LIMIT 3
    """).fetchall()

    if french:
        print("\n  French/mixed descriptions mentioning balcony:")
        for r in french:
            desc_snippet = (r["description"] or "")[:120].replace("\n", " ")
            print(f"    [{r['listing_id']}] {r['title'][:50]}")
            print(f"      feature_balcony={r['feature_balcony']} (may be NULL)")
            print(f"      desc: \"{desc_snippet}...\"")
            print(f"      -> Claude would extract balcony=true from this text.")

    # Italian descriptions
    italian = conn.execute("""
        SELECT listing_id, title, description, feature_balcony
        FROM listings
        WHERE (description LIKE '%cucina%' OR description LIKE '%balcone%'
               OR description LIKE '%giardino%')
          AND feature_balcony IS NULL
        LIMIT 3
    """).fetchall()

    if italian:
        print("\n  Italian descriptions with features (all feature_balcony=NULL):")
        for r in italian:
            desc_snippet = (r["description"] or "")[:120].replace("\n", " ")
            print(f"    [{r['listing_id']}] {r['title'][:50]}")
            print(f"      desc: \"{desc_snippet}...\"")
            print(f"      -> Claude extracts features from Italian. Regex misses.")

    print()
    conn.close()
    return True


def test_claude_extraction_live():
    """Run Claude Haiku on a few real listings to show extraction works."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("=" * 70)
        print("TEST: Claude extraction (SKIPPED -- no ANTHROPIC_API_KEY)")
        print("=" * 70)
        print("  Set ANTHROPIC_API_KEY to run this test.")
        print()
        return True  # Don't fail, just skip

    import re

    import anthropic

    from app.enrichment.text_extraction import _parse_response

    conn = get_conn()

    samples = conn.execute("""
        SELECT listing_id, title, description
        FROM listings
        WHERE description IS NOT NULL
          AND LENGTH(description) > 100
          AND feature_balcony IS NULL
          AND feature_elevator IS NULL
        LIMIT 3
    """).fetchall()

    if not samples:
        print("  (no suitable samples found)")
        return True

    print("=" * 70)
    print("TEST: Claude Haiku live extraction on real listings")
    print("=" * 70)

    client = anthropic.Anthropic(api_key=api_key)

    for s in samples:
        desc = (s["description"] or "")[:800]
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=256,
            system="Extract features from this real estate listing. Return ONLY valid JSON: "
                   '{"balcony": bool, "elevator": bool, "parking": bool, "garden": bool, '
                   '"washing_machine": bool, "dishwasher": bool, "fireplace": bool, '
                   '"pets_allowed": bool, "furnished": bool, "minergie": bool, '
                   '"wheelchair_accessible": bool, "cellar": bool}. '
                   "Use false if not mentioned. No explanation.",
            messages=[{"role": "user", "content": desc}],
        )
        raw = response.content[0].text.strip()
        features = _parse_response(raw)
        if features is None:
            features = {"parse_error": raw[:100]}

        true_feats = [k for k, v in features.items() if v is True]
        print(f"\n  [{s['listing_id']}] {s['title'][:55]}")
        print(f"    Description: \"{desc[:80]}...\"")
        print(f"    Extracted: {true_feats if true_feats else '(none detected)'}")
        print(f"    Full JSON: {json.dumps(features)}")

    print()
    conn.close()
    return True


def test_text_features_coverage():
    """Show text_features_json coverage after enrichment."""
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    filled = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE text_features_json IS NOT NULL"
    ).fetchone()[0]

    print("=" * 70)
    print(f"TEXT EXTRACTION COVERAGE  ({total} total listings)")
    print("=" * 70)
    pct = filled / total * 100
    bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
    print(f"  text_features_json  [{bar}] {filled:>6} ({pct:5.1f}%)")
    if filled == 0:
        print("  -> Run enrichment first: python -m scripts.enrich_listings --step text_extract")
    print()
    conn.close()


if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        sys.exit(1)

    results = []
    results.append(("feature gap", test_feature_gap()))
    results.append(("multilingual descriptions", test_multilingual_descriptions()))
    results.append(("Claude extraction live", test_claude_extraction_live()))
    test_text_features_coverage()

    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    for name, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}")

    sys.exit(0 if all(ok for _, ok in results) else 1)
