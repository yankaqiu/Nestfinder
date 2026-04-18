"""Trace each benchmark case: show extracted filters, counts per stage,
and TOP-10 flats with scores + matched signals.

Usage:
    .venv/bin/python -m benchmarks.pipeline_trace
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings
from app.core.hard_filters import search_listings
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
    params = to_hard_filter_params(hard_facts)
    params.limit = 999_999
    params.offset = 0
    return search_listings(DB_PATH, params)


def _fmt_price(p) -> str:
    if p is None:
        return "—"
    try:
        return f"{int(p):,}"
    except (TypeError, ValueError):
        return str(p)


def run_trace():
    total = _total_listings()
    sep = "=" * 120

    print(f"\n{sep}")
    print(f"  PIPELINE TRACE  —  {total:,} total listings in DB")
    print(sep)

    for case in BENCHMARK_CASES:
        print(f"\n{'—'*120}")
        print(f"  [{case.id}]")
        print(f"  Query: {case.query}")
        print(f"{'—'*120}")

        # Stage 1: Extract
        try:
            hard_facts = extract_hard_facts(case.query)
        except Exception as e:
            print(f"  HARD EXTRACTION FAILED: {e}")
            continue

        hard_dict = {k: v for k, v in hard_facts.__dict__.items()
                     if v is not None and k not in ("limit", "offset", "sort_by")}
        print(f"  Hard filters : {hard_dict}")

        # Stage 2: DB query
        try:
            candidates = _hard_filter_no_limit(hard_facts)
        except Exception as e:
            print(f"  DB QUERY FAILED: {e}")
            continue

        # Stage 3: Soft extraction
        try:
            soft_facts = extract_soft_facts(case.query)
        except Exception:
            soft_facts = {"signals": {}, "raw_query": case.query}

        signals = soft_facts.get("signals", {})
        extras = {k: v for k, v in soft_facts.items() if k not in ("signals", "raw_query")}
        sig_str = ", ".join(f"{k}={v}" for k, v in signals.items()) if signals else "(none)"
        print(f"  Soft signals : {sig_str}")
        if extras:
            print(f"  Soft extras  : {extras}")

        # Stage 4: Soft filter
        after_soft = filter_soft_facts(candidates, soft_facts)
        removed = len(candidates) - len(after_soft)

        print(f"  Pipeline     : {total:,} → {len(candidates):,} (hard) → {len(after_soft):,} (soft filter, -{removed}) → top 10 ranked")

        # Stage 5: Rank
        ranked = rank_listings(after_soft, soft_facts)

        if not ranked:
            print(f"  TOP 10       : (no results)")
            continue

        # Print top 10
        print()
        print(f"  {'#':>4}  {'Score':>6}  {'City':<16} {'Rooms':>5} {'CHF':>8}  {'Matched signals':<35} Title")
        print(f"  {'─'*114}")
        for i, r in enumerate(ranked[:10]):
            l = r.listing
            rooms_s = f"{l.rooms}" if l.rooms else "—"
            price_s = _fmt_price(l.price_chf)
            reason = r.reason if r.reason != "hard filters only" else "—"
            title = (l.title or "")[:40]
            print(f"  {i+1:>4}  {r.score:>6.2f}  {(l.city or '?'):<16} {rooms_s:>5} {price_s:>8}  {reason:<35} {title}")

        print()

    print(sep)
    print("  DONE")
    print(sep)


if __name__ == "__main__":
    out_path = Path(__file__).resolve().parent / "TRACE_OUTPUT.txt"
    with open(out_path, "w") as f:
        old_stdout = sys.stdout
        sys.stdout = f
        try:
            run_trace()
        finally:
            sys.stdout = old_stdout
    print(f"Written to {out_path}")
