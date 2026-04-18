# Data Enrichment Pipeline

Offline batch enrichment that populates new columns in the `listings` SQLite table.
Run **once** after the database is bootstrapped from CSVs. Every step is idempotent
and resumable — safe to re-run after a crash.

## Quick start

```bash
# Run everything (schema + backfill + neighborhood + report)
python -m scripts.enrich_listings --step all

# Run individual steps
python -m scripts.enrich_listings --step schema
python -m scripts.enrich_listings --step backfill
python -m scripts.enrich_listings --step geospatial
python -m scripts.enrich_listings --step neighborhood
python -m scripts.enrich_listings --step text_extract
python -m scripts.enrich_listings --step report

# Standalone text extraction (async, crash-resilient, checkpointed)
python -m scripts.run_text_extraction                    # full run
python -m scripts.run_text_extraction --limit 100        # test with 100
python -m scripts.run_text_extraction --status           # check progress
python -m scripts.run_text_extraction --load-checkpoint  # replay JSONL → DB
```

---

## Modules

### `schema.py` — Column definitions

Adds 11 new columns to the `listings` table via `ALTER TABLE`. Safe to run
repeatedly (skips existing columns).

| Column | Type | Source |
|---|---|---|
| `floor_level` | INTEGER | backfill_existing |
| `year_built` | INTEGER | backfill_existing |
| `renovation_year` | INTEGER | backfill_existing |
| `is_furnished` | INTEGER | backfill_existing + text_extraction |
| `price_per_sqm` | REAL | backfill_existing (computed) |
| `price_vs_city_median` | REAL | backfill_existing (computed) |
| `text_features_json` | TEXT | text_extraction |
| `municipality` | TEXT | geospatial |
| `bfs_number` | INTEGER | geospatial |
| `lake_distance_m` | INTEGER | geospatial |
| `is_urban` | INTEGER | neighborhood |

---

### `backfill_existing.py` — Mine existing data (no API calls)

Extracts structured fields from JSON blobs already sitting in the DB and
computes derived metrics. Also backfills `feature_*` hard-filter columns
from Claude's text extraction results.

**Functions:**

| Function | What it does |
|---|---|
| `backfill_from_orig_data` | Parses `orig_data_json` → `floor_level`, `year_built`, `renovation_year`, `is_furnished` |
| `backfill_city_canton` | Parses `location_address_json` → fills NULL `city` / `canton` |
| `compute_price_metrics` | Computes `price_per_sqm` and `price_vs_city_median` (avg per city, ≥3 listings) |
| `backfill_features_from_text` | Copies Claude-extracted booleans into `feature_*` columns (e.g. `feature_balcony`, `feature_elevator`) so hard filters work |
| `run_backfill` | Orchestrates all of the above in order |

**Coverage results:**

| Column | Before | After | Gain |
|---|---|---|---|
| `floor_level` | 0 | 8,169 (36%) | +8,169 |
| `year_built` | 0 | 2,929 (13%) | +2,929 |
| `price_per_sqm` | 0 | 18,237 (80%) | +18,237 |
| `price_vs_city_median` | 0 | 13,576 (60%) | +13,576 |
| `city` | 19,604 (86%) | 22,818 (100%) | +3,214 |
| `feature_balcony` | 11,714 (51%) | 19,315 (85%) | +7,601 |
| `feature_parking` | 11,714 (51%) | 18,369 (81%) | +6,655 |
| `feature_elevator` | 11,714 (51%) | 14,333 (63%) | +2,619 |
| `feature_private_laundry` | 10,917 (48%) | 15,293 (67%) | +4,376 |

**How it's used in ranking:**

- `floor_level` → `bright` signal (≥3rd floor = likely bright), `views` signal (≥5th floor)
- `year_built` → `modern` signal (>2010), `new_build` signal (≥2022)
- `renovation_year` → `modern` signal (>2015), `well_maintained` signal (>2018)
- `is_furnished` → `furnished` signal
- `price_vs_city_median` → `affordable` signal (<0.85 = 15% below city average)
- `feature_*` columns → hard filters (SQL-level `WHERE feature_balcony = 1`)

---

### `text_extraction.py` — Claude Haiku feature extraction (async)

Sends each listing description to Claude Haiku and extracts 12 structured
boolean features. Works across German, French, and Italian descriptions.

**Extracted features:**
`balcony`, `elevator`, `parking`, `garden`, `washing_machine`, `dishwasher`,
`fireplace`, `pets_allowed`, `furnished`, `minergie`, `wheelchair_accessible`, `cellar`

**Architecture:**
- Async with configurable concurrency (default 50 parallel requests)
- JSONL checkpoint file at `data/checkpoints/text_features.jsonl`
- Every result fsynced to disk immediately — crash loses nothing
- On restart, reads checkpoint and skips already-done listings
- Exponential backoff on rate limits and server errors
- Prompt caching for the system prompt (saves ~40% on input cost)

**Coverage:** 22,124 / 22,819 (97%) — only 695 listings have no description

**Cost:** ~$16 at standard pricing, ~$8 with batch API

**Throughput:** ~46 req/s with concurrency=50 (full run in ~2 minutes)

**How it's used:**
1. Stored in `text_features_json` column as JSON
2. `hard_filters._parse_row()` merges true features into the `features` list
3. `ranking._has_feature()` checks the list for signals like `balcony`, `elevator`, `parking`, etc.
4. `backfill_features_from_text()` copies booleans into `feature_*` columns so SQL hard filters also work

---

### `geospatial.py` — Swiss federal geodata (async)

Calls `geo.admin.ch` Identify API to resolve coordinates into municipality,
BFS number, and canton. Also computes distance to nearest major Swiss lake.

**API:** `GET https://api3.geo.admin.ch/rest/services/api/MapServer/identify`
with layer `ch.swisstopo.swissboundaries3d-gemeinde-flaeche.fill`

**Architecture:**
- Async with configurable concurrency (default 25 parallel requests)
- Processes only `WHERE municipality IS NULL` — safe to re-run
- Batches DB writes every 100 rows
- Free API, no key needed

**Coverage:**

| Column | Before | After | Gain |
|---|---|---|---|
| `municipality` | 0 | 21,172 (93%) | +21,172 |
| `lake_distance_m` | 0 | 21,173 (93%) | +21,173 |
| `canton` | 15,835 (69%) | 21,261 (93%) | +5,426 |

**Lake distance note:** Uses haversine distance to lake center points, not
shorelines. Distances are therefore inflated (a listing 200m from the Zürichsee
shore might report ~5km). Still directionally useful for the `near_lake` signal.

**17 Swiss lakes indexed:** Zürichsee, Genfersee, Vierwaldstättersee, Thunersee,
Brienzersee, Bielersee, Neuenburgersee, Zugersee, Bodensee, Greifensee,
Sempachersee, Hallwilersee, Walensee, Murtensee, Lago Maggiore, Lago di Lugano,
Baldeggersee.

**How it's used in ranking:**
- `lake_distance_m` → `near_lake` signal (<2000m)
- `municipality` → backfills `city` for SRED listings that had coordinates but no city name
- `canton` → hard filter (`WHERE canton = 'ZH'`)

---

### `neighborhood.py` — Urban/rural classification

Static postal code lookup. Marks listings in ~200 known urban PLZs (Zürich,
Bern, Basel, Geneva, Lausanne, Winterthur, St. Gallen, Lucerne, Biel, Thun)
as urban.

**Coverage:** 11,693 / 22,819 (51%) — limited by listings missing `postal_code`

**How it's used in ranking:**
- `is_urban = 1` → `lively` signal (boosts city listings for nightlife/restaurant queries)
- `is_urban = 0` → `quiet` signal (boosts rural/suburban listings for quiet-seeking queries)

---

## Data flow diagram

```
User query
    │
    ▼
soft_fact_extraction.py ──→ {"signals": {"bright": 1.0, "balcony": 0.8, ...}}
    │
    ▼
hard_filters.py
    │  SQL: SELECT ... WHERE feature_balcony = 1 AND city = 'Zürich' ...
    │  Also SELECTs: floor_level, year_built, lake_distance_m, is_urban,
    │                price_vs_city_median, text_features_json, ...
    │
    │  _parse_row(): merges text_features_json booleans into features list
    │
    ▼
ranking.py
    │  For each candidate × each signal:
    │    1. Check enriched column (floor_level ≥ 3? lake_distance_m < 2000?)
    │    2. Check features list (balcony? elevator? from Claude extraction)
    │    3. Fall back to regex on title + description
    │
    │  score = sum of matched signal weights
    │
    ▼
Sorted results (highest score first)
```

## Adding a new signal (checklist)

1. **Enrichment** (optional): Add to Claude's extraction schema in `text_extraction.py`
2. **Matcher** in `ranking.py`: `m["signal_name"] = lambda c: ...`
3. **LLM menu** in `soft_fact_extraction.py`: Add to the system prompt signals dict with a default weight
4. **Regex fallback** in `soft_fact_extraction.py`: Add to `_SOFT_SIGNALS` list
5. Re-run text extraction for new listings if the signal uses Claude data
