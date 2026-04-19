# Data Enrichment Pipeline

Offline batch enrichment that populates new columns in the `listings` SQLite table.
Run **once** after the database is bootstrapped from CSVs. Every step is idempotent
and resumable — safe to re-run after a crash.

## Quick start

```bash
# RECOMMENDED: Full reimport from all CSVs (drops + rebuilds DB)
uv run python -m scripts.reimport_db

# Same but keep a .bak of the old DB
uv run python -m scripts.reimport_db --keep-old

# Import only the v3.1 enriched CSV
uv run python -m scripts.reimport_db --only listings_enriched_v.3.1.csv

# Quick reimport without global_score computation
uv run python -m scripts.reimport_db --skip-enrichment

# Run enrichment steps on an existing DB (no reimport)
uv run python -m scripts.enrich_listings --step all

# Run individual enrichment steps
uv run python -m scripts.enrich_listings --step schema
uv run python -m scripts.enrich_listings --step backfill
uv run python -m scripts.enrich_listings --step geospatial
uv run python -m scripts.enrich_listings --step neighborhood
uv run python -m scripts.enrich_listings --step global_score
uv run python -m scripts.enrich_listings --step report

# Standalone text extraction (async, crash-resilient, checkpointed)
uv run python -m scripts.run_text_extraction                    # full run
uv run python -m scripts.run_text_extraction --limit 100        # test with 100
uv run python -m scripts.run_text_extraction --status           # check progress
uv run python -m scripts.run_text_extraction --load-checkpoint  # replay JSONL → DB
```

### First-time setup for teammates

```bash
# 1. Install dependencies
uv sync --dev

# 2. Place raw_data/ in project root (CSVs including listings_enriched_v.3.1.csv)

# 3. Build the DB from scratch with all enrichment
uv run python -m scripts.reimport_db

# 4. Verify it worked (should show ~82% transit coverage, ~72% price benchmarking)
uv run python -m scripts.enrich_listings --step report

# 5. Start the debug server to explore results visually
uv run python -m benchmarks.debug_server
# → open http://127.0.0.1:8899/debug
```

### After pulling new code

If the schema changed (new columns added), the fastest way to get up to date:

```bash
uv run python -m scripts.reimport_db --keep-old
```

---

## Data sources

### v3.1 enriched CSV (`listings_enriched_v.3.1.csv`)

Pre-computed enrichment from external sources, imported directly as CSV columns.
Contains **26 new columns** across four categories:

**Transit proximity** — nearest public transport stop, train station, and
Hauptbahnhof with name + distance in meters:

| Column | Type | Example |
|---|---|---|
| `nearest_stop_name` | TEXT | "Zürich, Bahnhofstrasse/HB" |
| `nearest_stop_distance_m` | REAL | 142.5 |
| `nearest_train_name` | TEXT | "Zürich HB" |
| `nearest_train_distance_m` | REAL | 830.0 |
| `nearest_hb_name` | TEXT | "Zürich HB" |
| `nearest_hb_distance_m` | REAL | 830.0 |

**Administrative geography** — municipality, district, canton codes and names:

| Column | Type |
|---|---|
| `municipality_code` | INTEGER |
| `district_code` | INTEGER |
| `canton_code` | INTEGER |
| `municipality_name` | TEXT |
| `district_name` | TEXT |
| `canton_name` | TEXT |
| `municipality_name_demo` | TEXT |

**Demographics** — population and density per municipality:

| Column | Type | Example |
|---|---|---|
| `population_total` | INTEGER | 443,037 |
| `area_ha` | REAL | 8,781.0 |
| `area_km2` | REAL | 87.81 |
| `population_density` | REAL | 4,543.2 |
| `population_density_bucket` | TEXT | "Urban" |

**Multi-level price benchmarking** — price per m² compared against
municipality, district, and canton averages:

| Column | Type | Meaning |
|---|---|---|
| `price_per_m2` | REAL | listing's CHF/m² |
| `avg_price_per_m2_municipality` | REAL | municipality average |
| `avg_price_per_m2_district` | REAL | district average |
| `avg_price_per_m2_canton` | REAL | canton average |
| `price_per_m2_vs_municipality` | REAL | ratio (0.8 = 20% below avg) |
| `price_per_m2_vs_district` | REAL | ratio vs district |
| `price_per_m2_vs_canton` | REAL | ratio vs canton |
| `price_per_m2_vs_municipality_label` | TEXT | "Below average" etc. |

**Coverage** (25,546 total listings):
- Transit data: ~82.9%
- Municipality/demographics: ~82.3%
- Price benchmarking: ~72.7%

### Computed enrichment columns

Populated by the enrichment pipeline (`scripts/enrich_listings.py`):

| Column | Type | Source |
|---|---|---|
| `floor_level` | INTEGER | backfill_existing |
| `year_built` | INTEGER | backfill_existing |
| `renovation_year` | INTEGER | backfill_existing |
| `is_furnished` | INTEGER | backfill_existing + text_extraction |
| `price_per_sqm` | REAL | backfill_existing (computed, fallback when `price_per_m2` is NULL) |
| `price_vs_city_median` | REAL | backfill_existing (computed, fallback when `price_per_m2_vs_municipality` is NULL) |
| `text_features_json` | TEXT | text_extraction |
| `municipality` | TEXT | geospatial |
| `bfs_number` | INTEGER | geospatial |
| `lake_distance_m` | INTEGER | geospatial |
| `is_urban` | INTEGER | neighborhood (skipped when `population_density_bucket` exists) |

### Global quality score (7 dimensions)

| Column | Type | Weight | What it measures |
|---|---|---|---|
| `global_score` | REAL | — | Weighted composite of all dimensions |
| `score_value` | REAL | 0.20 | Price relative to municipality/city average |
| `score_amenity` | REAL | 0.18 | Count of positive feature flags |
| `score_location` | REAL | 0.17 | Urban flag, lake proximity |
| `score_building` | REAL | 0.13 | Construction/renovation year, floor level |
| `score_completeness` | REAL | 0.10 | How many fields are populated |
| `score_freshness` | REAL | 0.10 | How soon the listing is available |
| `score_transit` | REAL | 0.12 | 3-tier transit proximity (stop + train + HB) |

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

- `floor_level` → `bright` signal (binned: 2/4/6), `views` signal (binned: 3/5/8)
- `year_built` → `modern` signal (binned: 2005/2015/2020), `new_build` signal (≥2022)
- `renovation_year` → `modern` signal (binned: 2015/2020/2023), `well_maintained` signal (binned: 2010/2018/2022)
- `is_furnished` → `furnished` signal (binary)
- `price_vs_city_median` → `affordable` signal (fallback when municipality data missing)
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
- `lake_distance_m` → `near_lake` signal (binned: 5000/2000/800m)
- `municipality` → backfills `city` for SRED listings that had coordinates but no city name
- `canton` → hard filter (`WHERE canton = 'ZH'`)

---

### `neighborhood.py` — Urban/rural classification

Static postal code lookup. Marks listings in ~200 known urban PLZs (Zürich,
Bern, Basel, Geneva, Lausanne, Winterthur, St. Gallen, Lucerne, Biel, Thun)
as urban.

**Coverage:** 11,693 / 22,819 (51%) — limited by listings missing `postal_code`

**How it's used in ranking:**
- `is_urban = 1` → `lively` signal (fallback when `population_density` is missing)
- `is_urban = 0` → `quiet` signal (fallback when `population_density` is missing)
- Skipped entirely when v3.1 `population_density_bucket` is already set (more accurate)

---

## Data flow diagram

```
User query
    │
    ▼
soft_fact_extraction.py ──→ {"signals": {"bright": 1.0, "near_train": 0.9, ...}}
    │
    ▼
hard_filters.py
    │  SQL: SELECT ... WHERE feature_balcony = 1 AND city = 'Zürich' ...
    │  Also SELECTs: all v3.1 columns (transit, demographics, price benchmarks),
    │                floor_level, year_built, lake_distance_m, is_urban, ...
    │
    │  _parse_row(): merges text_features_json booleans into features list
    │
    ▼
soft_filtering.py
    │  Removes junk listings (parking spots, storage units)
    │  Deprioritizes value outliers (price_per_m2_vs_municipality > 1.5)
    │
    ▼
ranking.py  (binned scoring system)
    │  For each candidate × each signal:
    │    1. _bin3() on enriched column → returns 0.0 / 0.33 / 0.66 / 1.0
    │    2. Check features list (binary: 0.0 or 1.0)
    │    3. Fall back to regex on title + description (text fallback = 0.5)
    │    4. Take max() across all sources
    │
    │  score += weight × strength  (not just weight)
    │  + global_score bonus (7 dimensions including transit)
    │  + image RAG bonus
    │  + user preference bonus
    │
    ▼
Sorted results (highest score first)
```

## Binned scoring system

Signal matchers return a float in `[0.0, 1.0]` instead of `True`/`False`.

The `_bin3(value, t1, t2, t3)` function maps continuous data to 4 levels:

| Strength | Meaning | Example (`near_train`, thresholds 2000/800/300m) |
|---|---|---|
| 0.0 | No match | > 2000m |
| 0.33 | Weak match | ≤ 2000m |
| 0.66 | Good match | ≤ 800m |
| 1.0 | Strong match | ≤ 300m |

Binary signals (feature present/absent) still return `1.0` or `0.0`.

Text regex fallback returns `0.5` when matched (between weak and good).

The final contribution of a signal is `weight × strength`, so a "near_train"
signal with weight 0.9 and strength 0.66 contributes `0.594` to the score.

## All 45 signal matchers

### Binned signals (use v3.1 data)

| Signal | Data column | Thresholds | Direction |
|---|---|---|---|
| `public_transport` | `nearest_stop_distance_m` | 600 / 300 / 150m | lower = better |
| `near_train` | `nearest_train_distance_m` | 2000 / 800 / 300m | lower = better |
| `near_hauptbahnhof` | `nearest_hb_distance_m` | 5000 / 2000 / 800m | lower = better |
| `near_hb` | `nearest_hb_distance_m` | 5000 / 2000 / 800m | lower = better |
| `well_connected` | stop + train composite | — | composite |
| `near_lake` | `lake_distance_m` | 5000 / 2000 / 800m | lower = better |
| `quiet` | `population_density` | 1000 / 500 / 200 /km² | lower = better |
| `lively` | `population_density` | 1500 / 3000 / 4000 /km² | higher = better |
| `low_density` | `population_density` | 1500 / 800 / 300 /km² | lower = better |
| `high_density` | `population_density` | 1000 / 2000 / 4000 /km² | higher = better |
| `small_town` | `population_total` | 30k / 10k / 5k | lower = better |
| `large_municipality` | `population_total` | 20k / 50k / 100k | higher = better |
| `affordable` | `price_per_m2_vs_municipality` | 1.0 / 0.85 / 0.70 | lower = better |
| `good_value_local` | `price_per_m2_vs_municipality` | 1.0 / 0.85 / 0.70 | lower = better |
| `overpriced_warning` | `price_per_m2_vs_municipality` | 1.3 / 1.5 / 1.8 | higher = worse |
| `bright` | `floor_level` | 2 / 4 / 6 | higher = better |
| `views` | `floor_level` | 3 / 5 / 8 | higher = better |
| `modern` | `year_built` + `renovation_year` | 2005-2020 / 2015-2023 | higher = better |
| `well_maintained` | `renovation_year` | 2010 / 2018 / 2022 | higher = better |
| `spacious` | `area / rooms` ratio | 25 / 35 / 45 m²/room | higher = better |
| `family_friendly` | `distance_kindergarten` + `school` | 1500/800/400 + 2000/1000/500 | lower = better |
| `good_schools` | `distance_school_1` | 2000 / 1000 / 500m | lower = better |

### Binary signals (1.0 / 0.0)

`pets_allowed`, `furnished`, `elevator`, `balcony`, `parking`, `fireplace`,
`garden`, `dishwasher`, `cellar`, `washing_machine`, `minergie`, `new_build`,
`outdoor_space`, `private_laundry`, `modern_kitchen`, `modern_bathroom`,
`student`, `near_eth`, `near_epfl`, `specific_move_in`, `child_friendly`,
`green_area`

## Adding a new signal (checklist)

1. **Enrichment** (optional): Add to Claude's extraction schema in `text_extraction.py`,
   or use a v3.1 column if available
2. **Matcher** in `ranking.py`: `m["signal_name"] = lambda c: _bin3(...)` for continuous
   data, or `1.0 if ... else 0.0` for binary
3. **Sets**: Add to `VISUAL_SIGNALS` or `NON_VISUAL_SIGNALS` in `ranking.py`
4. **LLM menu** in `soft_fact_extraction.py`: Add to the system prompt signals dict with a default weight
5. **Regex fallback** in `soft_fact_extraction.py`: Add to `_SOFT_SIGNALS` list
6. Re-run text extraction for new listings if the signal uses Claude data

## Debug server

Visual step-by-step pipeline debugger with preset queries:

```bash
uv run python -m benchmarks.debug_server
# → http://127.0.0.1:8899/debug
```

Shows per-listing details including:
- Signal match strengths (binned scores)
- Global score breakdown (7 dimensions with transit)
- v3.1 enrichment data (transit proximity, demographics, price benchmarks)
- Image RAG matches and scores
- User preference bonuses
