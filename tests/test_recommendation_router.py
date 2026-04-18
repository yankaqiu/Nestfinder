from pathlib import Path

from app.models.schemas import HardFilters, ListingData, ListingsResponse, RankedListingResult
from app.recommendation.registry import RecommendationRegistry
from app.recommendation.router import RecommendationRouter
from app.recommendation.strategies.llm_extraction_baseline import (
    LlmExtractionBaselineStrategy,
    extract_llm_hard_facts,
    extract_llm_soft_facts,
)


class StubStrategy:
    def __init__(self, strategy_id: str) -> None:
        self.strategy_id = strategy_id
        self.display_name = strategy_id.replace("_", " ").title()
        self.description = f"{strategy_id} description"

    def query_from_text(self, **kwargs: object) -> ListingsResponse:
        del kwargs
        return ListingsResponse(
            listings=[
                RankedListingResult(
                    listing_id=self.strategy_id,
                    score=1.0,
                    reason=self.strategy_id,
                    listing=ListingData(id=self.strategy_id, title=self.strategy_id),
                )
            ],
            meta={
                "strategy_id": self.strategy_id,
                "strategy_name": self.display_name,
            },
        )

    def query_from_filters(self, **kwargs: object) -> ListingsResponse:
        return self.query_from_text(**kwargs)


def test_router_uses_requested_strategy_and_exposes_pool() -> None:
    router = RecommendationRouter(
        RecommendationRegistry(
            strategies_by_id={
                "rule_based_image_rag": StubStrategy("rule_based_image_rag"),
                "llm_extraction_baseline": StubStrategy("llm_extraction_baseline"),
            }
        )
    )

    response = router.query_from_text(
        db_path=Path("unused.db"),
        query="bright apartment",
        limit=3,
        offset=0,
        strategy_id="llm_extraction_baseline",
    )

    assert response.meta["strategy_id"] == "llm_extraction_baseline"
    assert response.meta["strategy_selection_source"] == "request"
    assert response.meta["available_strategy_ids"] == [
        "rule_based_image_rag",
        "llm_extraction_baseline",
    ]


def test_router_uses_env_strategy_when_available(monkeypatch) -> None:
    monkeypatch.setenv("LISTINGS_RECOMMENDATION_STRATEGY", "rule_based_image_rag")
    router = RecommendationRouter(
        RecommendationRegistry(
            strategies_by_id={
                "rule_based_image_rag": StubStrategy("rule_based_image_rag"),
                "llm_extraction_baseline": StubStrategy("llm_extraction_baseline"),
            }
        )
    )

    response = router.query_from_text(
        db_path=Path("unused.db"),
        query="bright apartment",
        limit=3,
        offset=0,
    )

    assert response.meta["strategy_id"] == "rule_based_image_rag"
    assert response.meta["strategy_selection_source"] == "env"


def test_router_defaults_to_llm_strategy_when_env_missing(monkeypatch) -> None:
    monkeypatch.delenv("LISTINGS_RECOMMENDATION_STRATEGY", raising=False)
    router = RecommendationRouter(
        RecommendationRegistry(
            strategies_by_id={
                "rule_based_image_rag": StubStrategy("rule_based_image_rag"),
                "llm_extraction_baseline": StubStrategy("llm_extraction_baseline"),
            }
        )
    )

    response = router.query_from_text(
        db_path=Path("unused.db"),
        query="bright apartment",
        limit=3,
        offset=0,
    )

    assert response.meta["strategy_id"] == "llm_extraction_baseline"
    assert response.meta["strategy_selection_source"] == "default"


def test_llm_extraction_baseline_falls_back_to_rule_based_extractors(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.recommendation.strategies.llm_extraction_baseline.translate_to_english",
        lambda query: "bright apartment",
    )
    monkeypatch.setattr(
        "app.recommendation.strategies.llm_extraction_baseline._load_json_response",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        "app.recommendation.strategies.llm_extraction_baseline.extract_rule_based_hard_facts",
        lambda query: HardFilters(city=["Zürich"], limit=100),
    )
    monkeypatch.setattr(
        "app.recommendation.strategies.llm_extraction_baseline.extract_rule_based_soft_facts",
        lambda query: {"raw_query": query, "signals": {"bright": 1.0}},
    )

    hard_facts = extract_llm_hard_facts("helle wohnung")
    soft_facts = extract_llm_soft_facts("helle wohnung")

    assert hard_facts.city == ["Zürich"]
    assert hard_facts.limit == 100
    assert soft_facts["signals"] == {"bright": 1.0}


def test_llm_extraction_baseline_uses_shared_soft_filtering_and_ranking(monkeypatch) -> None:
    strategy = LlmExtractionBaselineStrategy()
    captured: dict[str, object] = {}

    def fake_filter(candidates, soft_facts, **kwargs):
        captured["filtered"] = {
            "candidates": candidates,
            "soft_facts": soft_facts,
            "kwargs": kwargs,
        }
        return [
            {
                "listing_id": "1",
                "title": "One",
                "_soft_score": 0.9,
                "_soft_reasons": ["bright"],
            }
        ]

    def fake_rank(candidates, soft_facts):
        captured["ranked"] = {
            "candidates": candidates,
            "soft_facts": soft_facts,
        }
        return [
            RankedListingResult(
                listing_id="1",
                score=1.0,
                reason="stub",
                listing=ListingData(id="1", title="One"),
            )
        ]

    monkeypatch.setattr(
        "app.recommendation.strategies.llm_extraction_baseline.filter_rule_based_soft_facts",
        fake_filter,
    )
    monkeypatch.setattr(
        "app.recommendation.strategies.llm_extraction_baseline.rank_rule_based_listings",
        fake_rank,
    )

    candidates = [{"listing_id": "1", "title": "One"}]
    soft_facts = {"raw_query": "bright apartment", "signals": {"bright": 1.0}}

    filtered = strategy.filter_soft_facts(
        candidates,
        soft_facts,
        limit=5,
        offset=2,
    )
    ranked = strategy.rank_listings(filtered, soft_facts)

    assert captured["filtered"] == {
        "candidates": candidates,
        "soft_facts": soft_facts,
        "kwargs": {"limit": 5, "offset": 2},
    }
    assert captured["ranked"] == {
        "candidates": filtered,
        "soft_facts": soft_facts,
    }
    assert ranked[0].listing_id == "1"
