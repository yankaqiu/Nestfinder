"""Parametrized benchmark tests for hard-fact and soft-fact extraction.

Run with:
    pytest benchmarks/ -v
"""

from __future__ import annotations

import pytest

from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.soft_fact_extraction import extract_soft_facts

from benchmarks.cases import BENCHMARK_CASES, CASE_IDS, BenchmarkCase


# ---------------------------------------------------------------------------
# Hard-fact extraction benchmark
# ---------------------------------------------------------------------------

def _soft_cases() -> list[BenchmarkCase]:
    return [c for c in BENCHMARK_CASES if c.expected_soft_signals or c.expected_soft_extras]


def _soft_case_ids() -> list[str]:
    return [c.id for c in _soft_cases()]


@pytest.mark.parametrize("case", BENCHMARK_CASES, ids=CASE_IDS)
def test_hard_fact_extraction(case: BenchmarkCase) -> None:
    if case.xfail_hard:
        pytest.xfail(case.xfail_hard)

    result = extract_hard_facts(case.query)
    for key, expected in case.expected_hard.items():
        actual = getattr(result, key)
        if isinstance(expected, list) and isinstance(actual, list):
            assert set(actual) == set(expected), (
                f"[{case.id}] hard filter '{key}': expected {expected!r}, got {actual!r}"
            )
        else:
            assert actual == expected, (
                f"[{case.id}] hard filter '{key}': expected {expected!r}, got {actual!r}"
            )


# ---------------------------------------------------------------------------
# Soft-fact extraction benchmark
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("case", _soft_cases(), ids=_soft_case_ids())
def test_soft_fact_extraction(case: BenchmarkCase) -> None:
    if case.xfail_soft:
        pytest.xfail(case.xfail_soft)

    result = extract_soft_facts(case.query)
    signals = result.get("signals", {})

    for expected_signal in case.expected_soft_signals:
        assert expected_signal in signals, (
            f"[{case.id}] expected soft signal '{expected_signal}' not found. "
            f"Got signals: {list(signals.keys())}"
        )

    for key, expected in case.expected_soft_extras.items():
        actual = result.get(key)
        assert actual == expected, (
            f"[{case.id}] soft extra '{key}': expected {expected!r}, got {actual!r}"
        )


# ---------------------------------------------------------------------------
# Summary report (printed at session end)
# ---------------------------------------------------------------------------

def _run_all_and_report() -> str:
    """Run every case through both extractors and produce a summary table."""
    lines = [
        f"{'Case ID':<35} {'Hard':>6} {'Soft':>6}  Notes",
        "-" * 80,
    ]
    for case in BENCHMARK_CASES:
        hard_ok = True
        soft_ok = True

        try:
            result = extract_hard_facts(case.query)
            for key, expected in case.expected_hard.items():
                actual = getattr(result, key)
                if isinstance(expected, list) and isinstance(actual, list):
                    if set(actual) != set(expected):
                        hard_ok = False
                        break
                elif actual != expected:
                    hard_ok = False
                    break
        except Exception:
            hard_ok = False

        if case.expected_soft_signals or case.expected_soft_extras:
            try:
                sresult = extract_soft_facts(case.query)
                signals = sresult.get("signals", {})
                for sig in case.expected_soft_signals:
                    if sig not in signals:
                        soft_ok = False
                        break
                for k, v in case.expected_soft_extras.items():
                    if sresult.get(k) != v:
                        soft_ok = False
                        break
            except Exception:
                soft_ok = False
        else:
            soft_ok = True

        h = "PASS" if hard_ok else "FAIL"
        s = "PASS" if soft_ok else "FAIL"
        notes = case.notes[:40] if case.notes else ""
        lines.append(f"{case.id:<35} {h:>6} {s:>6}  {notes}")

    return "\n".join(lines)


def test_benchmark_summary_report(capsys: pytest.CaptureFixture[str]) -> None:
    """Print a human-readable summary table (always passes)."""
    report = _run_all_and_report()
    with capsys.disabled():
        print("\n\n" + "=" * 80)
        print("QUERY BENCHMARK SUMMARY")
        print("=" * 80)
        print(report)
        print("=" * 80 + "\n")
