"""Trace each benchmark case through the full pipeline and show
how many flats survive at each stage.

Usage:
    python -m benchmarks.pipeline_trace
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings
from app.core.hard_filters import HardFilterParams, search_listings
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts
from app.participant.ranking import rank_listings
from app.harness.search_service import to_hard_filter_params
from app.db import get_connection
from benchmarks.cases import BENCHMARK_CASES

settings = get_settings()
DB_PATH = settings.db_path


def _total_listings() -> int:
    with get_connection(DB_PATH) as conn:
        return conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]


def _hard_filter_no_limit(hard_facts) -> list[dict[str, Any]]:
    """Run hard filter with no limit/offset to get the real count."""
    params = to_hard_filter_params(hard_facts)
    params.limit = 999_999
    params.offset = 0
    return search_listings(DB_PATH, params)


def run_trace():
    total = _total_listings()
    sep = "=" * 110
    thin = "-" * 110

    print(f"\n{sep}")
    print(f"  PIPELINE TRACE  —  {total:,} total listings in DB")
    print(sep)

    for case in BENCHMARK_CASES:
        print(f"\n{thin}")
        print(f"  Case: {case.id}")
        print(f"  Query: {case.query[:90]}{'…' if len(case.query) > 90 else ''}")
        print(thin)

        # --- Stage 1: Hard-fact extraction ---
        try:
            hard_facts = extract_hard_facts(case.query)
        except Exception as e:
            print(f"  [!] Hard extraction FAILED: {e}")
            continue

        hard_dict = {k: v for k, v in hard_facts.__dict__.items()
                     if v is not None and k not in ("limit", "offset", "sort_by")}
        print(f"  Hard filters extracted : {hard_dict}")

        # --- Stage 2: Hard filtering (DB query) ---
        try:
            candidates = _hard_filter_no_limit(hard_facts)
        except Exception as e:
            print(f"  [!] Hard filter query FAILED: {e}")
            continue

        print(f"  After hard filtering   : {len(candidates):>6,} flats  (from {total:,})")

        # --- Stage 3: Soft-fact extraction ---
        try:
            soft_facts = extract_soft_facts(case.query)
        except Exception as e:
            print(f"  [!] Soft extraction FAILED: {e}")
            soft_facts = {"signals": {}, "raw_query": case.query}

        signals = soft_facts.get("signals", {})
        extras = {k: v for k, v in soft_facts.items()
                  if k not in ("signals", "raw_query")}
        print(f"  Soft signals           : {signals}")
        if extras:
            print(f"  Soft extras            : {extras}")

        # --- Stage 4: Soft filtering ---
        after_soft = filter_soft_facts(candidates, soft_facts)
        if len(after_soft) != len(candidates):
            print(f"  After soft filtering   : {len(after_soft):>6,} flats  (removed {len(candidates) - len(after_soft)})")
        else:
            print(f"  After soft filtering   : {len(after_soft):>6,} flats  (no-op — filter_soft_facts is stubbed)")

        # --- Stage 5: Ranking (top N) ---
        TOP_N = 50
        ranked = rank_listings(after_soft[:TOP_N], soft_facts)
        print(f"  Ranked (top {TOP_N})        : {len(ranked):>6,} flats returned to user")

        # Show a sample of top-ranked listings
        if ranked:
            print(f"  {'':>4}  Sample top-3:")
            for i, r in enumerate(ranked[:3]):
                l = r.listing
                price_str = f"CHF {l.price_chf:,.0f}" if l.price_chf else "no price"
                rooms_str = f"{l.rooms}r" if l.rooms else "?r"
                print(f"  {'':>6}  #{i+1}: {l.city or '?'}, {rooms_str}, {price_str} — {(l.title or '')[:50]}")

    print(f"\n{sep}")
    print("  DONE")
    print(sep)


if __name__ == "__main__":
    run_trace()
