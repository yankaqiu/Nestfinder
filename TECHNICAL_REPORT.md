# Nestfinder — Technical Report

## 1. Project Overview

**Nestfinder** is a Swiss real-estate listing search engine built for the Datathon 2026 challenge. It ingests ~22,800 property listings from multiple crawl sources, enriches them with geospatial and NLP-derived metadata, and exposes a natural-language search API that translates queries (German/French/English) into structured filters, soft-ranks results by preference signals, and returns scored listings via a FastAPI REST service.

**Tech stack:** Python 3.12, FastAPI, SQLite, Anthropic Claude Haiku 4.5, geo.admin.ch Swiss Federal API, httpx (async), Pydantic v2, Docker Compose, React + MapLibre (frontend widget).

---

## 2. Data Sources (22,819 listings total)

| Source | Count | Description |
|--------|-------|-------------|
| `robinreal` | 797 | Crawled listings with images |
| `sred` | 11,105 | SRED data with montage images + lat/lon |
| `structured_with_images` | 4,160 | Structured data with images |
| `structured_without_images` | 6,757 | Structured data without images |

Each CSV has 52 columns including `id`, `title`, `object_description`, `number_of_rooms`, `price`, `area`, `geo_lat`, `geo_lng`, `orig_data` (nested JSON with Features/MainData arrays), `images`, `location_address`, property booleans, and proximity distances.

---

## 3. End-to-End Pipeline

The system operates in two phases: **offline data preparation** and **online query serving**.

---

### PHASE A: Offline Data Preparation

#### Step 0 — SRED Normalization

**Module:** `app/harness/sred_transform.py`

The SRED bundle ships in a non-standard directory structure. `ensure_sred_normalized_csv()` transforms it into the standard 52-column CSV format expected by the importer, aligning column names and merging lat/lon data.

#### Step 1 — Database Bootstrap

**Modules:** `app/harness/bootstrap.py` → `app/harness/csv_import.py`

On first startup (or if `data/listings.db` doesn't exist):

1. **Schema creation** — Creates a `listings` table with 41 columns covering identity, text, location, pricing, rooms/area, dates, geo coordinates, proximity distances, 12 boolean feature columns, JSON blobs (`features_json`, `images_json`, `location_address_json`, `orig_data_json`, `raw_json`).

2. **CSV import** — Each CSV file is read via `csv.DictReader`. Every row passes through the **listing row parser** (`app/participant/listing_row_parser.py`), which performs:
   - **Text cleaning** — strips whitespace, normalizes "NULL" strings
   - **Price derivation** — cascading logic: `rent_gross` → `price` → `rent_net + rent_extra`
   - **Feature extraction from `orig_data`** — parses the nested JSON `Features` array (keys like `HasBalconies`, `HasLift`, `HasParkingIndoor`) and `MainData` array (keys like `PetsAllowed`, `IsNewBuilding`, `IsWheelchairAccessible`) into 12 boolean feature columns
   - **Location parsing** — extracts city/postal_code/canton from both flat columns and nested `location_address` JSON
   - **Type coercion** — rooms (float), area (float), lat/lon (float), distances (int), dates (ISO format)

3. **Index creation** — B-tree indexes on `city`, `postal_code`, `canton`, `price`, `rooms`, `latitude`, `longitude` for fast SQL filtering.

#### Step 2 — Enrichment Schema Extension

**Module:** `app/enrichment/schema.py`

Adds 11 new columns to the existing table via `ALTER TABLE`:

| Column | Type | Source |
|--------|------|--------|
| `floor_level` | INTEGER | Backfilled from orig_data |
| `year_built` | INTEGER | Backfilled from orig_data |
| `renovation_year` | INTEGER | Backfilled from orig_data |
| `is_furnished` | INTEGER | Backfilled from orig_data |
| `price_per_sqm` | REAL | Computed from price/area |
| `price_vs_city_median` | REAL | Ratio to city average |
| `text_features_json` | TEXT | Claude text extraction |
| `municipality` | TEXT | geo.admin.ch API |
| `bfs_number` | INTEGER | geo.admin.ch API |
| `lake_distance_m` | INTEGER | Haversine to nearest lake |
| `is_urban` | INTEGER | PLZ-based classification |

Idempotent — skips columns that already exist.

#### Step 3 — Backfill from Existing Data

**Module:** `app/enrichment/backfill_existing.py`

Four sub-steps that mine data already present in the DB (no external API calls):

| Sub-step | Source | Derived columns |
|----------|--------|-----------------|
| `backfill_from_orig_data` | `orig_data_json` → `MainData` array | `floor_level`, `year_built`, `renovation_year`, `is_furnished` |
| `backfill_city_canton` | `location_address_json` | `city`, `canton` (fills NULLs only) |
| `compute_price_metrics` | `price`, `area`, `city` | `price_per_sqm`, `price_vs_city_median` |
| `backfill_features_from_text` | `text_features_json` | Fills NULL `feature_*` columns using Claude-extracted booleans |

**Price metric logic:**
- `price_per_sqm = price / area` (only for area ≥ 10 m², price 100–50,000 CHF)
- `price_vs_city_median` = listing's price/sqm divided by the city's average price/sqm (cities with ≥3 listings only)

**Feature backfill priority:** structured CSV columns > `orig_data` JSON > Claude text extraction. Uses `COALESCE` to never overwrite higher-confidence data.

#### Step 4 — Geospatial Enrichment

**Module:** `app/enrichment/geospatial.py`

Uses the **geo.admin.ch** Swiss Federal API asynchronously:

- **Municipality lookup** — `GET /rest/services/api/MapServer/identify` with `ch.swisstopo.swissboundaries3d-gemeinde-flaeche.fill` layer. Returns municipality name, BFS number, and canton. Also backfills missing `city`/`canton`.
- **Lake distance** — Haversine distance to the nearest of 17 major Swiss lakes (Zürichsee, Genfersee, Vierwaldstättersee, Thunersee, Brienzersee, Bielersee, Neuenburgersee, Zugersee, Bodensee, Greifensee, Sempachersee, Hallwilersee, Walensee, Murtensee, Lago Maggiore, Lago di Lugano, Baldeggersee).
- **Concurrency** — 25 parallel async requests with semaphore throttling, exponential backoff (4 retries), batch commits every 100 rows.
- **Crash-safe** — only processes rows where `municipality IS NULL`, so partial runs can be resumed.

#### Step 5 — Text Feature Extraction

**Module:** `app/enrichment/text_extraction.py`

Uses **Claude Haiku 4.5** to extract structured features from listing descriptions:

- **Prompt** — Asks Claude to parse descriptions into a 12-field boolean JSON: `balcony`, `elevator`, `parking`, `garden`, `washing_machine`, `dishwasher`, `fireplace`, `pets_allowed`, `furnished`, `minergie`, `wheelchair_accessible`, `cellar`.
- **Concurrency** — 50 parallel async Claude API calls via `anthropic.AsyncAnthropic`.
- **Crash resilience** — Every result is immediately appended to a JSONL checkpoint file (`data/checkpoints/text_features.jsonl`) with `fsync`. On restart, already-extracted IDs are skipped. Missing DB records are backfilled from the checkpoint.
- **Descriptions are truncated** to 1,500 chars; listings with <50 char descriptions are skipped.

#### Step 6 — Neighborhood Classification

**Module:** `app/enrichment/neighborhood.py`

Static PLZ-based urban/non-urban tagging. A hand-curated set of ~100 postal codes covering major Swiss city centers (Zürich, Bern, Basel, Geneva, Lausanne, Luzern, St. Gallen, Winterthur, Lugano, Biel, Thun, Fribourg, Neuchâtel, Sion, La Chaux-de-Fonds) marks listings as `is_urban = 1`. All others get `is_urban = 0`. No external API calls.

#### Orchestration

**Script:** `scripts/enrich_listings.py`

Runs steps 2–6 sequentially against the SQLite database, with logging and stats output.

---

### PHASE B: Online Query Serving

The FastAPI app (`app/main.py`) starts via uvicorn, bootstraps the database on startup, mounts SRED image static files, and exposes the listing routes.

#### API Endpoints

| Endpoint | Method | Input | Purpose |
|----------|--------|-------|---------|
| `GET /health` | — | — | Health check |
| `POST /listings` | Natural language | `{"query": "...", "limit": 25}` | Full NL pipeline |
| `POST /listings/search/filter` | Structured | `{"hard_filters": {...}}` | Direct filter API |

#### The NL Query Pipeline

**Orchestrator:** `app/harness/search_service.py`

```
query_from_text(query, limit, offset)
  ├── 1. extract_hard_facts(query)     → HardFilters
  ├── 2. extract_soft_facts(query)     → {signals, max_commute_minutes, ...}
  ├── 3. filter_hard_facts(db, hard)   → list[dict]  (SQL query)
  ├── 4. filter_soft_facts(candidates) → list[dict]  (junk removal)
  └── 5. rank_listings(candidates, soft) → list[RankedListingResult]
```

##### Stage 1 — Translation

**Module:** `app/participant/translate.py`

Detects non-English queries using regex markers for German (`ich`, `suche`, `zimmer`, `wohnung`, `zürich`...) and French (`je`, `cherche`, `appartement`, `louer`...). If the query isn't English, sends it to **Claude Haiku 4.5** for translation. Falls back gracefully to the original text on API failure.

##### Stage 2 — Hard Fact Extraction

**Module:** `app/participant/hard_fact_extraction.py`

Extracts non-negotiable constraints from the (translated) query using a **two-tier approach**:

**LLM primary** — Claude Haiku 4.5 with a detailed system prompt containing:
- JSON schema with `city`, `postal_code`, `min_rooms`, `max_rooms`, `min_price`, `max_price`, `min_area_sqm`, `offer_type`, `features`
- Swiss-specific parsing rules (CHF with `'` separator, comma decimals, Zimmer→rooms)
- City canonical spellings (zurich→Zürich, genf→Genf, etc.)
- Key design decisions:
  - `offer_type` only set to "SALE" if explicitly stated (default is rent)
  - `object_category` excluded to avoid German/English value mismatches
  - `canton` excluded (city is sufficient for location filtering)
  - Only `elevator`, `pets_allowed`, `wheelchair_accessible` are hard features
  - Balcony, parking, garage, fireplace, garden are soft preferences — excluded from hard filters
  - Default `limit=100` to provide enough candidates for ranking

**Regex fallback** — 180+ city aliases, 50+ canton aliases, compiled regex patterns for rooms (range/exact/min/max), prices (CHF with Swiss formatting), area, features, offer type. Activated if the LLM call fails.

##### Stage 3 — Soft Fact Extraction

**Module:** `app/participant/soft_fact_extraction.py`

Extracts nice-to-have preferences with weighted importance scores. Same two-tier approach:

**LLM primary** — Claude Haiku extracts a `signals` dictionary mapping 35+ signal names to float weights (0.3–1.2):

| Signal | Weight | Signal | Weight |
|--------|--------|--------|--------|
| `bright` | 1.0 | `quiet` | 1.0 |
| `modern` | 0.8 | `views` | 0.6 |
| `near_lake` | 0.6 | `public_transport` | 1.0 |
| `short_commute` | 1.2 | `furnished` | 0.8 |
| `family_friendly` | 0.8 | `child_friendly` | 0.7 |
| `good_schools` | 0.7 | `green_area` | 0.6 |
| `balcony` | 0.8 | `parking` | 0.7 |
| `private_laundry` | 0.8 | `elevator` | 0.7 |
| `near_eth` | 1.2 | `near_epfl` | 1.2 |

Plus: `max_commute_minutes`, `commute_destination`, `preferred_min_area_sqm`.

**Regex fallback** — 35 compiled regex patterns covering German, English, and French terms.

##### Stage 4 — Soft Filtering (Junk Removal)

**Module:** `app/participant/soft_filtering.py`

Removes non-residential junk listings before ranking:
- **Title-based junk detection:** parking spots (`Parkplatz`, `Einstellplatz`, `Tiefgarage`), storage units (`Hobbyraum`, `Abstellraum`, `Lager`), garage boxes — unless residential signals also present in the title
- **Price filter:** listings under CHF 100 are removed
- **Safety net:** if filtering removes all candidates, the original list is preserved

##### Stage 5 — Ranking

**Module:** `app/participant/ranking.py`

The scoring engine matches each candidate against extracted soft signals using a **data-enriched + text-fallback** strategy.

For each of the 35+ signals, a matcher function checks:

1. **Enriched DB columns first** (data-driven, high confidence):

| Signal | Enriched check | Threshold |
|--------|---------------|-----------|
| `bright` | `floor_level >= 3` | 3rd floor+ |
| `modern` | `year_built > 2010` or `renovation_year > 2015` | — |
| `near_lake` | `lake_distance_m < 2000` | 2 km |
| `public_transport` | `distance_public_transport < 500` | 500 m |
| `affordable` | `price_vs_city_median < 0.85` | 15% below avg |
| `family_friendly` | `distance_kindergarten < 1000` or `distance_school_1 < 1500` | — |
| `good_schools` | `distance_school_1 < 1000` or `distance_school_2 < 1500` | — |
| `new_build` | `year_built >= 2022` | — |
| `lively` | `is_urban == 1` | — |
| `well_maintained` | `renovation_year > 2018` | — |
| `spacious` | `area >= 80` | 80 m² |
| `views` | `floor_level >= 5` | 5th floor+ |

2. **Text regex fallback** (for listings without enrichment data):
   - Multi-language regex patterns matching German, English, and French keywords in `title + description`

3. **Feature column checks** (from `features_json` / `text_features_json`):
   - `balcony`, `elevator`, `parking`, `garage`, `fireplace`, `garden`, `dishwasher`, `cellar`, `washing_machine`, `private_laundry`, `minergie`, `pets_allowed`, `furnished`

**Scoring formula:**
```
score = Σ (signal_weight for each matched signal)
      + 0.3 if area >= preferred_min_area_sqm
```

Results are sorted by descending score. Each result includes:
- `listing_id` — unique identifier
- `score` — float, rounded to 2 decimal places
- `reason` — comma-separated list of matched signal names (e.g., "quiet, balcony, public_transport, near_lake")
- `listing` — full `ListingData` object with 19 fields

---

## 4. Hard Filter SQL Engine

**Module:** `app/core/hard_filters.py`

The SQL query builder constructs dynamic `WHERE` clauses:

| Filter | SQL |
|--------|-----|
| `city` | `LOWER(city) IN (?)` (case-insensitive) |
| `postal_code` | `postal_code IN (?)` |
| `canton` | `UPPER(canton) = ?` |
| `min_price` / `max_price` | `price >= ?` / `price <= ?` |
| `min_rooms` / `max_rooms` | `rooms >= ?` / `rooms <= ?` |
| `offer_type` | `UPPER(offer_type) = ?` |
| `features` | Maps to `feature_* = 1` column checks |
| **Geo-radius** | Post-filter: Haversine distance ≤ `radius_km`, sorted by distance |

The SELECT pulls 34 columns including all enrichment fields (`floor_level`, `year_built`, `lake_distance_m`, `is_urban`, `text_features_json`, etc.) that feed into the ranking engine.

**Image URL extraction** merges `images.images[]` (URL objects) and `images.image_paths[]` (SRED local paths) into a unified `image_urls` list with `hero_image_url` set to the first entry.

---

## 5. Data Model

### Pydantic API Models (`app/models/schemas.py`)

| Model | Fields | Purpose |
|-------|--------|---------|
| `HardFilters` | 16 fields | Input: non-negotiable search constraints |
| `ListingsQueryRequest` | `query`, `limit`, `offset` | Input: NL search |
| `ListingsSearchRequest` | `hard_filters` | Input: structured search |
| `ListingData` | 19 fields | Output: single listing |
| `RankedListingResult` | `listing_id`, `score`, `reason`, `listing` | Output: scored listing |
| `ListingsResponse` | `listings`, `meta` | Output: response wrapper |

### SQLite Schema

**Base table:** 41 columns (from CSV import)
**Enrichment columns:** 11 additional columns (added via ALTER TABLE)
**Total:** 52 columns per listing

**Indexes:** `city`, `postal_code`, `canton`, `price`, `rooms`, `latitude`, `longitude`

---

## 6. Infrastructure

### Docker Compose

Two services:

| Service | Port | Role |
|---------|------|------|
| `api` | 8000 | Main FastAPI listing service |
| `mcp` | 8001 | Apps SDK server (MCP/ChatGPT connector) |

Both share the codebase via bind mount and the `listings_data` volume for the SQLite DB.

### Apps SDK

- **Backend** (`apps_sdk/server/`) — FastAPI proxy to the main API, serves the web widget
- **Frontend** (`apps_sdk/web/`) — React + Vite + TypeScript app with:
  - `ListingsMap.tsx` — MapLibre GL map showing listing locations
  - `RankedList.tsx` — Scrollable ranked results list
- **MCP integration** — Enables ChatGPT and other MCP clients to search listings

### Static File Serving

SRED images served at `/raw-data-images/` if the `raw_data/sred_images/` directory exists (~11,105 files, ~114 MB).

---

## 7. Testing & Benchmarks

| Suite | Location | Purpose |
|-------|----------|---------|
| `tests/test_pipeline.py` | Unit | End-to-end pipeline test |
| `tests/test_hard_filters.py` | Unit | SQL filter correctness |
| `tests/test_soft_filters.py` | Unit | Junk removal logic |
| `tests/test_schemas.py` | Unit | Pydantic model validation |
| `tests/enrichment/test_geospatial.py` | Unit | Municipality + lake enrichment |
| `tests/enrichment/test_neighborhood.py` | Unit | Urban/non-urban classification |
| `tests/enrichment/test_text_extraction.py` | Unit | Claude feature extraction |
| `tests/enrichment/test_backfill.py` | Unit | Data backfill correctness |
| `benchmarks/cases.py` | Benchmark | Real-world queries with expected results |
| `benchmarks/pipeline_trace.py` | Benchmark | Full pipeline execution tracer |
| `benchmarks/debug_server.py` | Debug | Interactive pipeline inspection server |

**Benchmark queries** (`raw_data/long_queries.md`): 6 realistic multi-constraint housing queries in German, English, and French, covering Zürich, Basel, Lausanne, and Geneva areas.

---

## 8. Key Design Decisions

1. **LLM + Regex dual-tier** — Every extraction step (translation, hard facts, soft facts) has a regex fallback so the system works offline or when the Anthropic API is down.

2. **Enrichment before ranking** — Offline enrichment (municipality, lake distance, floor level, price metrics, text features, urban classification) creates structured columns that the ranking engine uses as first-class signals, reducing reliance on noisy text matching at query time.

3. **Crash-resilient enrichment** — Geospatial and text extraction use idempotent queries (`WHERE column IS NULL`), JSONL checkpoints, and batch commits so partial runs can be resumed safely.

4. **Hard/soft separation** — Hard constraints become SQL WHERE clauses (fast, precise). Soft preferences become weighted scoring signals applied post-filter. This avoids over-constraining the result set while still respecting user priorities.

5. **Multi-language support** — The translation layer handles German, French, and English queries transparently, with regex extractors also supporting all three languages as fallback.

6. **Feature fusion** — Feature booleans come from three sources merged with `COALESCE` priority: structured CSV columns > `orig_data` JSON > Claude text extraction. This maximizes coverage without overwriting higher-confidence data.

7. **Junk filtering** — Parking spots, storage units, and sub-CHF-100 listings are removed before ranking to improve result quality, with a safety net that preserves the original list if everything would be filtered out.

---

## 9. Configuration

| Environment Variable | Default | Purpose |
|---------------------|---------|---------|
| `LISTINGS_RAW_DATA_DIR` | `raw_data` | Path to CSV source files |
| `LISTINGS_DB_PATH` | `data/listings.db` | SQLite database location |
| `LISTINGS_S3_BUCKET` | `crawl-data-...` | AWS S3 bucket for data |
| `LISTINGS_S3_REGION` | `eu-central-2` | AWS region |
| `ANTHROPIC_API_KEY` | — | Required for LLM features |
| `AWS_ACCESS_KEY_ID` | — | S3 data download |
| `AWS_SECRET_ACCESS_KEY` | — | S3 data download |

---

## 10. Repository Structure

```
Nestfinder/
├── app/
│   ├── main.py                          # FastAPI app + lifespan bootstrap
│   ├── config.py                        # Settings dataclass + env vars
│   ├── db.py                            # SQLite connection helper
│   ├── api/routes/listings.py           # REST endpoints
│   ├── core/
│   │   ├── hard_filters.py              # SQL query builder + geo-radius filter
│   │   └── s3.py                        # AWS S3 data download
│   ├── harness/
│   │   ├── bootstrap.py                 # DB init + CSV import orchestration
│   │   ├── csv_import.py                # Schema DDL + CSV→SQLite loader
│   │   ├── search_service.py            # Pipeline orchestrator
│   │   └── sred_transform.py            # SRED data normalization
│   ├── models/
│   │   └── schemas.py                   # Pydantic request/response models
│   ├── participant/
│   │   ├── translate.py                 # DE/FR→EN translation (Claude)
│   │   ├── hard_fact_extraction.py      # Hard constraint extraction (LLM+regex)
│   │   ├── soft_fact_extraction.py      # Soft preference extraction (LLM+regex)
│   │   ├── soft_filtering.py            # Junk listing removal
│   │   ├── ranking.py                   # Signal matching + scoring engine
│   │   └── listing_row_parser.py        # CSV row → DB tuple transformer
│   └── enrichment/
│       ├── schema.py                    # ALTER TABLE for enrichment columns
│       ├── backfill_existing.py         # Mine orig_data for floor/year/price
│       ├── geospatial.py                # geo.admin.ch municipality + lake dist
│       ├── text_extraction.py           # Claude feature extraction from descriptions
│       └── neighborhood.py              # PLZ-based urban/non-urban tagging
├── apps_sdk/                            # MCP + ChatGPT connector + React widget
├── scripts/                             # Offline enrichment + analysis scripts
├── benchmarks/                          # Pipeline traces + debug server
├── tests/                               # Unit + enrichment tests
├── Dockerfile + docker-compose.yml      # Containerized deployment
└── pyproject.toml                       # Dependencies + build config
```
