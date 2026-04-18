# Query Extraction Benchmarks

Curated queries that exercise the hard-fact and soft-fact extraction pipeline,
documenting known failure cases so you can iterate systematically.

## Quick start

```bash
# Run all benchmarks — regex fallback only (no API key needed)
.venv/bin/python -m pytest benchmarks/ -v

# Run with LLM active (requires ANTHROPIC_API_KEY)
export ANTHROPIC_API_KEY=sk-ant-...
.venv/bin/python -m pytest benchmarks/ -v

# Show xfail cases as real failures to see what actually breaks
.venv/bin/python -m pytest benchmarks/ -v --runxfail
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

## Current status (V2)

24 cases total. With **LLM active**: nearly all pass. With **regex fallback**:

| Category | Regex only | With LLM |
|----------|-----------|----------|
| Hard pass | 10/24 | 23/24 |
| Soft pass | 5/9 | 8/9 |

### Weaknesses only the LLM fixes (regex xfails)

| Issue | Cases affected |
|-------|---------------|
| `bedroom` / `bed` not in room keyword regex | `english_basel_bedrooms`, `bedroom_rooms_english`, `bed_abbreviation` |
| `budget up to` not in max-price regex | `family_basel_english`, `english_geneva_residential` |
| `between X and Y CHF` price range | `price_range_between` |
| `for X per month` (no CHF context) | `price_per_month` |
| Italian `locali` / `massimo` | `italian_lugano` |
| Postal code extraction | `postal_code_zurich` |
| Object category (Einfamilienhaus) | `house_type_zug` |
| `Haustiere erlaubt` feature | `multi_feature_elevator_pets` |
| German soft signals after translation | `german_zurich_3.5_rooms`, `german_lausanne_epfl` |
| `Familie` → family_friendly | `family_lake_kilchberg` |
| `good transport access` | `english_geneva_residential` |

### Remaining weaknesses (even with LLM)

- `multi_city_german`: LLM sometimes interprets "Raum Zürich" as area-around rather than city
- `family_lake_kilchberg`: LLM varies on outdoor_space vs balcony distinction
- LLM responses are non-deterministic — same query may give slightly different results

## Iteration workflow

1. Pick a weakness from the table above
2. Decide: fix the regex fallback, improve the LLM prompt, or both
3. Run `pytest benchmarks/ -v` to check
4. Fixed `xfail` cases will flip to `xpass` — remove the `xfail_*` field
5. Commit and move on
