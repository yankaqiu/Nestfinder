"""Benchmark case definitions for the query extraction pipeline.

Each BenchmarkCase pairs a raw user query with expected extraction results.
Edit this file to add new cases or update expectations as the pipeline improves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BenchmarkCase:
    id: str
    query: str
    expected_hard: dict[str, Any]
    expected_soft_signals: list[str] = field(default_factory=list)
    expected_soft_extras: dict[str, Any] = field(default_factory=dict)
    xfail_hard: str | None = None
    xfail_soft: str | None = None
    notes: str = ""


BENCHMARK_CASES: list[BenchmarkCase] = [
    # 1 — Basic German query, exact rooms + max price + city
    BenchmarkCase(
        id="german_zurich_3.5_rooms",
        query="Ruhige 3.5-Zimmer Wohnung in Zürich unter CHF 2500",
        expected_hard={
            "city": ["Zürich"],
            "min_rooms": 3.5,
            "max_rooms": 3.5,
            "max_price": 2500,
        },
        expected_soft_signals=["quiet"],
        xfail_soft="'ruhig' translated to English before soft extraction; 'quiet' regex only matches untranslated",
        notes="Basic German: city + exact rooms + max price",
    ),

    # 2 — English query with CHF, rooms as 'bedroom'
    BenchmarkCase(
        id="english_basel_bedrooms",
        query="2 bedroom flat in Basel under CHF 3500",
        expected_hard={
            "city": ["Basel"],
            "max_price": 3500,
        },
        xfail_hard="'bedroom' not matched — extractor requires 'room/Zimmer/pièces'",
        notes="'bedroom' is not in the room-keyword regex",
    ),

    # 3 — Multi-city German query (from long_queries.md #1)
    BenchmarkCase(
        id="multi_city_german",
        query=(
            "Ich suche eine Wohnung im Raum Zürich, Dübendorf oder Wallisellen, "
            "idealerweise 2.5 bis 3.5 Zimmer, ab 70 m², Budget bis 3100 CHF, "
            "max 25 Minuten mit dem ÖV bis Stadelhofen"
        ),
        expected_hard={
            "city": ["Dübendorf", "Wallisellen"],
            "min_rooms": 2.5,
            "max_rooms": 3.5,
            "max_price": 3100,
        },
        expected_soft_signals=["public_transport"],
        expected_soft_extras={"max_commute_minutes": 25},
        xfail_hard="LLM may exclude Zürich ('Raum Zürich' = area around); regex includes all three + Zürich",
        notes="Tests multi-city + room range + price + commute",
    ),

    # 4 — Family search near lake (from long_queries.md #2)
    BenchmarkCase(
        id="family_lake_kilchberg",
        query=(
            "Wir suchen als Familie zu dritt etwas im Raum Kilchberg, Rüschlikon oder Thalwil, "
            "am liebsten nahe am See oder mit schneller Verbindung nach Zürich, "
            "mindestens 3.5 Zimmer, ab 90 m², Budget bis 4300 CHF, "
            "gern mit Balkon / Terrasse, Lift, Keller"
        ),
        expected_hard={
            "city": ["Kilchberg", "Rüschlikon", "Thalwil"],
            "min_rooms": 3.5,
            "max_price": 4300,
            "features": ["elevator"],
        },
        expected_soft_signals=["family_friendly", "near_lake", "balcony"],
        xfail_hard="Regex fallback also picks up Zürich from 'Verbindung nach Zürich'; LLM correctly excludes it",
        xfail_soft="Regex misses 'Familie'; LLM gets family_friendly but may vary on outdoor_space vs balcony",
        notes="Tests small-town names, family signals, elevator feature",
    ),

    # 5 — English Zurich area query (from long_queries.md #3)
    BenchmarkCase(
        id="english_zurich_area_commute",
        query=(
            "I'm looking for an apartment in the greater Zurich area, "
            "ideally somewhere like Oerlikon, Altstetten, or Schlieren, "
            "with at least 60 sqm, preferably 2 to 3 rooms, "
            "a commute under 30 minutes to Zurich HB door to door"
        ),
        expected_hard={
            "city": ["Altstetten", "Schlieren", "Oerlikon", "Zürich"],
            "min_rooms": 2.0,
            "max_rooms": 3.0,
        },
        expected_soft_signals=["near_hb"],
        expected_soft_extras={
            "preferred_min_area_sqm": 60,
            "max_commute_minutes": 30,
        },
        notes="Tests English NL with sub-area names, room range, commute extraction",
    ),

    # 6 — English Basel family (from long_queries.md #4)
    BenchmarkCase(
        id="family_basel_english",
        query=(
            "We are a family of 3 looking around Basel for something with 2 or 3 bedrooms, "
            "ideally 85 sqm or more, budget up to CHF 3500, in an area with good schools, "
            "quiet streets, and enough nearby amenities"
        ),
        expected_hard={
            "city": ["Basel"],
            "max_price": 3500,
        },
        expected_soft_signals=["good_schools", "quiet"],
        expected_soft_extras={"preferred_min_area_sqm": 85},
        xfail_hard="'bedrooms' not matched by room regex; 'budget up to' not matched by max-price regex",
        notes="'bedroom' and 'budget up to' are not recognized patterns",
    ),

    # 7 — German Lausanne near EPFL (from long_queries.md #5)
    BenchmarkCase(
        id="german_lausanne_epfl",
        query=(
            "Ich suche etwas Kleineres in Lausanne, möglichst in der Nähe von EPFL, "
            "gern möbliert, unter 2100 CHF, mit guter Anbindung"
        ),
        expected_hard={
            "city": ["Lausanne"],
            "max_price": 2100,
        },
        expected_soft_signals=["furnished", "near_epfl", "public_transport"],
        xfail_soft="'guter Anbindung' translated; English form not matched by public_transport regex",
        notes="German query mentioning EPFL (Lausanne). Tests furnished + EPFL signal.",
    ),

    # 8 — English Geneva (from long_queries.md #6)
    BenchmarkCase(
        id="english_geneva_residential",
        query=(
            "I'm looking for a place near Geneva city center but not right in the busiest part, "
            "ideally with 2 bedrooms, budget up to CHF 3600, good transport access"
        ),
        expected_hard={
            "city": ["Genf"],
            "max_price": 3600,
        },
        expected_soft_signals=["public_transport"],
        xfail_hard="'bedrooms' not matched; 'budget up to' not matched by max-price regex",
        xfail_soft="'good transport access' not matched by public_transport regex patterns",
        notes="Geneva alias + bedroom + budget pattern failures",
    ),

    # 9 — Studio shorthand
    BenchmarkCase(
        id="studio_lausanne",
        query="Studio in Lausanne under CHF 1500",
        expected_hard={
            "city": ["Lausanne"],
            "min_rooms": 1.0,
            "max_rooms": 1.5,
            "max_price": 1500,
        },
        notes="Tests studio shorthand → rooms 1.0-1.5",
    ),

    # 10 — Ambiguous canton vs city: Schwyz
    BenchmarkCase(
        id="ambiguous_schwyz",
        query="3-Zimmer Wohnung in Schwyz zu mieten",
        expected_hard={
            "min_rooms": 3.0,
            "max_rooms": 3.0,
            "offer_type": "RENT",
        },
        notes=(
            "Schwyz is both a city and canton. The extractor should pick one "
            "or both; currently city='Schwyz' and canton='SZ' may both be set."
        ),
    ),

    # 11 — Sale offer type
    BenchmarkCase(
        id="sale_zurich",
        query="Haus zu kaufen in Zürich, 5 Zimmer",
        expected_hard={
            "city": ["Zürich"],
            "min_rooms": 5.0,
            "max_rooms": 5.0,
            "offer_type": "SALE",
        },
        notes="Explicit 'kaufen' should trigger SALE offer type",
    ),

    # 12 — Edge case: vague query with no extractable constraints
    BenchmarkCase(
        id="vague_no_constraints",
        query="something nice and central",
        expected_hard={
            "city": None,
            "canton": None,
            "min_rooms": None,
            "max_rooms": None,
            "min_price": None,
            "max_price": None,
            "offer_type": None,
        },
        notes="No hard constraints should be extracted from a vague query",
    ),

    # ===================================================================
    # V2 — New weakness cases found by probing
    # ===================================================================

    # 13 — Price range: "between X and Y CHF" not captured
    BenchmarkCase(
        id="price_range_between",
        query="flat in Zurich between 1500 and 2500 CHF",
        expected_hard={
            "city": ["Zürich"],
            "min_price": 1500,
            "max_price": 2500,
        },
        xfail_hard="'between X and Y CHF' pattern not supported — only captures max_price from second number",
        notes="Price range with 'between' keyword",
    ),

    # 14 — "per month" price: treated as no price at all
    BenchmarkCase(
        id="price_per_month",
        query="apartment in Basel for 2000 per month",
        expected_hard={
            "city": ["Basel"],
            "max_price": 2000,
        },
        xfail_hard="'for X per month' not recognized as a price pattern — needs CHF context or 'for' pattern",
        notes="Price stated as 'for X per month' without CHF",
    ),

    # 15 — 'bedroom' still not in room regex
    BenchmarkCase(
        id="bedroom_rooms_english",
        query="2 bedroom apartment in Zurich under CHF 3000",
        expected_hard={
            "city": ["Zürich"],
            "min_rooms": 2.0,
            "max_rooms": 2.0,
            "max_price": 3000,
        },
        xfail_hard="'bedroom' not in _ROOM_KW regex — only 'room/Zimmer/pièces/zi' matched",
        notes="English 'bedroom' variant still unsupported",
    ),

    # 16 — Short 'bed' abbreviation
    BenchmarkCase(
        id="bed_abbreviation",
        query="3 bed flat in Lausanne",
        expected_hard={
            "city": ["Lausanne"],
            "min_rooms": 3.0,
            "max_rooms": 3.0,
        },
        xfail_hard="'bed' not in _ROOM_KW regex",
        notes="Informal 'bed' abbreviation for rooms",
    ),

    # 17 — Full French query
    BenchmarkCase(
        id="french_3_pieces_geneva",
        query="Je cherche un appartement 3 pièces à Genève, lumineux, moins de 2500 CHF",
        expected_hard={
            "city": ["Genf"],
            "min_rooms": 3.0,
            "max_rooms": 3.0,
            "max_price": 2500,
        },
        expected_soft_signals=["bright"],
        notes="Full French query: pièces + city alias + price + lumineux signal",
    ),

    # 18 — Italian query (unsupported language)
    BenchmarkCase(
        id="italian_lugano",
        query="Cerco un appartamento a Lugano, 3 locali, massimo 2000 CHF",
        expected_hard={
            "city": ["Lugano"],
            "min_rooms": 3.0,
            "max_rooms": 3.0,
            "max_price": 2000,
        },
        xfail_hard="Italian 'locali' not in room regex; 'massimo' not in max-price patterns",
        notes="Italian: 'locali' for rooms, 'massimo' for max — both unsupported",
    ),

    # 19 — Postal code extraction
    BenchmarkCase(
        id="postal_code_zurich",
        query="Wohnung in 8001 Zürich, 3 Zimmer",
        expected_hard={
            "city": ["Zürich"],
            "postal_code": ["8001"],
            "min_rooms": 3.0,
            "max_rooms": 3.0,
        },
        xfail_hard="Postal code extraction not implemented in regex fallback",
        notes="Swiss postal code (4 digits) before city name",
    ),

    # 20 — Area as hard constraint (mindestens X m²)
    BenchmarkCase(
        id="area_hard_constraint",
        query="mindestens 80 m² Wohnung in Zürich zu mieten",
        expected_hard={
            "city": ["Zürich"],
            "offer_type": "RENT",
        },
        expected_soft_extras={"preferred_min_area_sqm": 80},
        notes="Area only extracted as soft preference, not as hard filter",
    ),

    # 21 — Einfamilienhaus (house type / object category)
    BenchmarkCase(
        id="house_type_zug",
        query="Einfamilienhaus in Zug zu kaufen",
        expected_hard={
            "city": ["Zug"],
            "offer_type": "SALE",
        },
        xfail_hard="Regex fallback cannot extract object_category at all",
        notes="LLM returns object_category but regex path doesn't; value varies (house vs single_family_house)",
    ),

    # 22 — Multiple hard features (elevator + pets)
    BenchmarkCase(
        id="multi_feature_elevator_pets",
        query="Wohnung mit Lift und Haustiere erlaubt in Bern",
        expected_hard={
            "city": ["Bern"],
            "features": ["elevator", "pets_allowed"],
        },
        xfail_hard="'Haustiere erlaubt' not matched — regex expects 'haustier' or 'pets allowed' (no 'erlaubt')",
        notes="Multiple features: Lift works, but 'Haustiere erlaubt' fails",
    ),

    # 23 — "budget up to" price pattern (standalone CHF catches it)
    BenchmarkCase(
        id="budget_up_to",
        query="apartment in Bern, budget up to CHF 2800, 3 rooms",
        expected_hard={
            "city": ["Bern"],
            "max_price": 2800,
            "min_rooms": 3.0,
            "max_rooms": 3.0,
        },
        notes="'budget up to CHF X' caught by standalone _PRICE_AMOUNT_RE fallback",
    ),

    # 24 — Minimal single-word city query
    BenchmarkCase(
        id="minimal_city_only",
        query="Zürich",
        expected_hard={
            "city": ["Zürich"],
            "min_rooms": None,
            "max_rooms": None,
            "min_price": None,
            "max_price": None,
        },
        notes="Minimal query: just a city name, nothing else",
    ),
]

CASE_IDS = [c.id for c in BENCHMARK_CASES]
CASE_MAP = {c.id: c for c in BENCHMARK_CASES}
