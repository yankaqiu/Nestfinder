# Query Extraction Benchmarks

Curated queries that exercise the hard-fact and soft-fact extraction pipeline,
documenting known failure cases so you can iterate systematically.

## Quick start

```bash
# Run all benchmarks (from repo root)
pytest benchmarks/ -v

# Run only hard-fact tests
pytest benchmarks/test_extraction.py -k hard -v

# Run only soft-fact tests
pytest benchmarks/test_extraction.py -k soft -v
```

## File layout

```
benchmarks/
  cases.py            # BenchmarkCase dataclass + all curated cases (edit this!)
  test_extraction.py  # parametrized pytest tests + summary report
  conftest.py         # pytest discovery config
  README.md           # this file
```

**`cases.py`** is the file you edit most — add new queries, update expected
values, flip `xfail_hard`/`xfail_soft` to `None` when a fix lands.

## How to add a case

Add a new `BenchmarkCase(...)` to the `BENCHMARK_CASES` list in `cases.py`:

```python
BenchmarkCase(
    id="short_unique_id",
    query="The raw user query in any language",
    expected_hard={
        "city": ["Zürich"],
        "max_price": 2500,
        # only include fields you want to assert on
    },
    expected_soft_signals=["quiet", "balcony"],
    expected_soft_extras={"max_commute_minutes": 20},
    xfail_hard="reason if hard extraction is known-broken",  # or None
    xfail_soft="reason if soft extraction is known-broken",  # or None
    notes="Short description of what this case tests",
)
```

## Iteration roadmap

Fix the 9 known failures (`xfail`) in this order:

### Phase A — Add `bedroom` to room keyword regex

**File:** `app/participant/hard_fact_extraction.py`
**Change:** Add `bedrooms?` to `_ROOM_KW`
**Resolves:** `english_basel_bedrooms`, `family_basel_english`, `english_geneva_residential`

### Phase B — Add `budget up to` to max-price regex

**File:** `app/participant/hard_fact_extraction.py`
**Change:** Add `budget\s+(?:up\s+to|bis)` to `_MAX_PRICE_RE`
**Resolves:** `family_basel_english`, `english_geneva_residential`

### Phase C — Add missing city aliases

**File:** `app/participant/hard_fact_extraction.py`
**Change:** Add Wallisellen, Kilchberg, Rüschlikon, Thalwil, Oerlikon, Altstetten, Schlieren to `_CITY_ALIASES`
**Resolves:** `multi_city_german`, `family_lake_kilchberg`

### Phase D — Run soft extraction on both original + translated query

**File:** `app/participant/soft_fact_extraction.py`
**Change:** Run regex signals on the original query _and_ the translated query, merging results
**Resolves:** `german_zurich_3.5_rooms`, `family_lake_kilchberg`, `german_lausanne_epfl`

### Phase E — Expand public_transport regex

**File:** `app/participant/soft_fact_extraction.py`
**Change:** Add `good\s+transport`, `transport\s+access`, `well\s+connected` patterns
**Resolves:** `english_geneva_residential`

### Phase F — Implement soft filtering + ranking

**Files:** `app/participant/soft_filtering.py`, `app/participant/ranking.py`
**Change:** Use soft-fact signals to score and reorder candidates
**Resolves:** New benchmark cases (to be written)

## Workflow

1. Pick the next phase from the roadmap above
2. Make the code change in the `app/participant/` module
3. Run `pytest benchmarks/ -v`
4. Fixed `xfail` cases will flip to `xpass` — remove the `xfail_*` field from the case
5. Commit and move to the next phase
