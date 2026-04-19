from pathlib import Path

from app.models.schemas import HardFilters
from app.models.schemas import ListingData
from app.models.schemas import RankedListingResult
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts
from app.harness import search_service
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


def test_query_from_text_paginates_after_ranking(monkeypatch) -> None:
    ranked_results = [
        RankedListingResult(
            listing_id=f"listing-{index}",
            score=float(100 - index),
            reason="test",
            listing=ListingData(id=f"listing-{index}", title=f"Listing {index}"),
        )
        for index in range(10)
    ]

    monkeypatch.setattr(search_service, "extract_hard_facts", lambda query: HardFilters(city=["Winterthur"]))
    monkeypatch.setattr(search_service, "extract_soft_facts", lambda query: {})
    monkeypatch.setattr(search_service, "filter_soft_facts", lambda candidates, soft_facts: candidates)
    monkeypatch.setattr(search_service, "build_user_profile", lambda session_id: {})
    monkeypatch.setattr(search_service, "rank_listings", lambda candidates, soft_facts, user_profile=None: ranked_results)

    observed: dict[str, int | None] = {}

    def fake_search_listings(db_path: Path, params) -> list[dict[str, object]]:
        observed["limit"] = params.limit
        observed["offset"] = params.offset
        return [{"listing_id": f"candidate-{index}", "title": f"Candidate {index}"} for index in range(10)]

    monkeypatch.setattr(search_service, "search_listings", fake_search_listings)

    logged: dict[str, int] = {}
    monkeypatch.setattr(
        search_service,
        "log_search",
        lambda query, session_id, result_count: logged.update({"result_count": result_count}),
    )

    response = search_service.query_from_text(
        db_path=Path("/tmp/unused.db"),
        query="bright flat in winterthur",
        limit=3,
        offset=2,
        session_id="sess-test",
    )

    assert observed == {"limit": None, "offset": 0}
    assert [item.listing_id for item in response.listings] == ["listing-2", "listing-3", "listing-4"]
    assert logged["result_count"] == 3


def test_query_from_filters_paginates_after_ranking(monkeypatch) -> None:
    ranked_results = [
        RankedListingResult(
            listing_id=f"listing-{index}",
            score=float(50 - index),
            reason="test",
            listing=ListingData(id=f"listing-{index}", title=f"Listing {index}"),
        )
        for index in range(8)
    ]

    monkeypatch.setattr(search_service, "extract_soft_facts", lambda query: {})
    monkeypatch.setattr(search_service, "filter_soft_facts", lambda candidates, soft_facts: candidates)
    monkeypatch.setattr(search_service, "build_user_profile", lambda session_id: {})
    monkeypatch.setattr(search_service, "rank_listings", lambda candidates, soft_facts, user_profile=None: ranked_results)

    observed: dict[str, int | None] = {}

    def fake_search_listings(db_path: Path, params) -> list[dict[str, object]]:
        observed["limit"] = params.limit
        observed["offset"] = params.offset
        return [{"listing_id": f"candidate-{index}", "title": f"Candidate {index}"} for index in range(8)]

    monkeypatch.setattr(search_service, "search_listings", fake_search_listings)

    response = search_service.query_from_filters(
        db_path=Path("/tmp/unused.db"),
        hard_facts=HardFilters(city=["Winterthur"], limit=2, offset=3),
        session_id="sess-test",
    )

    assert observed == {"limit": None, "offset": 0}
    assert [item.listing_id for item in response.listings] == ["listing-3", "listing-4"]


def test_query_from_text_caps_unfiltered_candidate_pool(monkeypatch) -> None:
    monkeypatch.setattr(search_service, "extract_hard_facts", lambda query: HardFilters())
    monkeypatch.setattr(search_service, "extract_soft_facts", lambda query: {})
    monkeypatch.setattr(search_service, "filter_soft_facts", lambda candidates, soft_facts: candidates)
    monkeypatch.setattr(search_service, "build_user_profile", lambda session_id: {})
    monkeypatch.setattr(search_service, "rank_listings", lambda candidates, soft_facts, user_profile=None: [])
    monkeypatch.setattr(search_service, "log_search", lambda query, session_id, result_count: None)

    observed: dict[str, int | None] = {}

    def fake_search_listings(db_path: Path, params) -> list[dict[str, object]]:
        observed["limit"] = params.limit
        observed["offset"] = params.offset
        return []

    monkeypatch.setattr(search_service, "search_listings", fake_search_listings)

    search_service.query_from_text(
        db_path=Path("/tmp/unused.db"),
        query="bright apartment with balcony",
        limit=25,
        offset=0,
        session_id="sess-test",
        max_unfiltered_candidates=1000,
    )

    assert observed == {"limit": 1000, "offset": 0}


def test_query_from_text_does_not_cap_when_hard_constraints_exist(monkeypatch) -> None:
    monkeypatch.setattr(search_service, "extract_hard_facts", lambda query: HardFilters(city=["Winterthur"]))
    monkeypatch.setattr(search_service, "extract_soft_facts", lambda query: {})
    monkeypatch.setattr(search_service, "filter_soft_facts", lambda candidates, soft_facts: candidates)
    monkeypatch.setattr(search_service, "build_user_profile", lambda session_id: {})
    monkeypatch.setattr(search_service, "rank_listings", lambda candidates, soft_facts, user_profile=None: [])
    monkeypatch.setattr(search_service, "log_search", lambda query, session_id, result_count: None)

    observed: dict[str, int | None] = {}

    def fake_search_listings(db_path: Path, params) -> list[dict[str, object]]:
        observed["limit"] = params.limit
        observed["offset"] = params.offset
        return []

    monkeypatch.setattr(search_service, "search_listings", fake_search_listings)

    search_service.query_from_text(
        db_path=Path("/tmp/unused.db"),
        query="apartment in winterthur",
        limit=25,
        offset=0,
        session_id="sess-test",
        max_unfiltered_candidates=1000,
    )

    assert observed == {"limit": None, "offset": 0}
