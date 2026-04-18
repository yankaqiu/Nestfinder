from pathlib import Path

from app.models.schemas import HardFilters
from app.models.schemas import ListingData
from app.models.schemas import RankedListingResult
from app.harness.search_service import query_from_text
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts
from app.harness.search_service import to_hard_filter_params


def test_extract_hard_facts_returns_stub_structure() -> None:
    result = extract_hard_facts("3 room flat in zurich")

    assert isinstance(result, HardFilters)


def test_participant_soft_fact_modules_are_importable() -> None:
    candidates = [{"listing_id": "1", "title": "Example"}]

    soft_facts = extract_soft_facts("bright flat")
    filtered = filter_soft_facts(candidates, soft_facts)
    ranked = rank_listings(filtered, soft_facts)

    assert isinstance(soft_facts, dict)
    assert isinstance(filtered, list)
    assert all(item["listing_id"] in {"1"} for item in filtered)
    assert isinstance(ranked, list)
    assert ranked
    assert all(item.listing_id for item in ranked)
    assert all(isinstance(item.score, float) for item in ranked)


def test_harness_service_converts_hard_filters_to_search_params() -> None:
    filters = HardFilters(city=["Zurich"], features=["balcony"], limit=5, offset=2)

    params = to_hard_filter_params(filters)

    assert params.city == ["Zurich"]
    assert params.features == ["balcony"]
    assert params.limit == 5
    assert params.offset == 2


def test_query_from_text_applies_final_pagination_after_ranking(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, int] = {}

    monkeypatch.setattr(
        "app.harness.search_service.extract_hard_facts",
        lambda query: HardFilters(limit=5, offset=0),
    )
    monkeypatch.setattr(
        "app.harness.search_service.extract_soft_facts",
        lambda query: {"raw_query": query, "signals": {"bright": 1.0}},
    )

    def fake_filter_hard_facts(db_path: Path, hard_facts: HardFilters) -> list[dict[str, object]]:
        captured["limit"] = hard_facts.limit
        captured["offset"] = hard_facts.offset
        return [
            {"listing_id": "first", "title": "First"},
            {"listing_id": "second", "title": "Second"},
            {"listing_id": "third", "title": "Third"},
        ]

    monkeypatch.setattr("app.harness.search_service.filter_hard_facts", fake_filter_hard_facts)
    monkeypatch.setattr(
        "app.harness.search_service.filter_soft_facts",
        lambda candidates, soft_facts, **kwargs: candidates,
    )
    monkeypatch.setattr(
        "app.harness.search_service.rank_listings",
        lambda candidates, soft_facts: [
            RankedListingResult(
                listing_id="third",
                score=3.0,
                reason="third",
                listing=ListingData(id="third", title="Third"),
            ),
            RankedListingResult(
                listing_id="second",
                score=2.0,
                reason="second",
                listing=ListingData(id="second", title="Second"),
            ),
            RankedListingResult(
                listing_id="first",
                score=1.0,
                reason="first",
                listing=ListingData(id="first", title="First"),
            ),
        ],
    )

    response = query_from_text(
        db_path=tmp_path / "listings.db",
        query="bright apartment",
        limit=1,
        offset=1,
    )

    assert captured["offset"] == 0
    assert captured["limit"] >= 2
    assert [listing.listing_id for listing in response.listings] == ["second"]
