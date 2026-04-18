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
            "city": ["Zürich", "Dübendorf"],
            "min_rooms": 2.5,
            "max_rooms": 3.5,
            "max_price": 3100,
        },
        expected_soft_signals=["public_transport"],
        expected_soft_extras={"max_commute_minutes": 25},
        xfail_hard="Wallisellen not in city alias map; multi-city extraction may be incomplete",
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
            "min_rooms": 3.5,
            "max_price": 4300,
            "features": ["elevator"],
        },
        expected_soft_signals=["family_friendly", "near_lake", "outdoor_space", "balcony"],
        xfail_hard="Kilchberg/Rüschlikon/Thalwil not in city alias map",
        xfail_soft="'Familie' translated away; family_friendly regex misses translated form",
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
            "city": ["Zürich"],
            "min_rooms": 2.0,
            "max_rooms": 3.0,
        },
        expected_soft_signals=["near_hb"],
        expected_soft_extras={
            "preferred_min_area_sqm": 60,
            "max_commute_minutes": 30,
        },
        notes="Tests English NL, room range, area as soft, commute extraction",
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
]

CASE_IDS = [c.id for c in BENCHMARK_CASES]
CASE_MAP = {c.id: c for c in BENCHMARK_CASES}
