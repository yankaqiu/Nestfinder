from __future__ import annotations

from typing import Any

from app.models.schemas import HardFilters, RankedListingResult
from app.participant.hard_fact_extraction import extract_hard_facts as extract_rule_based_hard_facts
from app.participant.ranking import rank_listings as rank_rule_based_listings
from app.participant.soft_fact_extraction import extract_soft_facts as extract_rule_based_soft_facts
from app.participant.soft_filtering import filter_soft_facts as filter_rule_based_soft_facts
from app.recommendation.base import RecommendationStrategy, SoftFacts


class RuleBasedImageRagStrategy(RecommendationStrategy):
    strategy_id = "rule_based_image_rag"
    display_name = "Rule-based + Image RAG"
    description = (
        "Rule-based hard and soft extraction, soft candidate pruning, and image reranking."
    )

    def extract_hard_facts(self, query: str) -> HardFilters:
        return extract_rule_based_hard_facts(query)

    def extract_soft_facts(self, query: str) -> SoftFacts:
        return extract_rule_based_soft_facts(query)

    def filter_soft_facts(
        self,
        candidates: list[dict[str, Any]],
        soft_facts: SoftFacts,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return filter_rule_based_soft_facts(
            candidates,
            soft_facts,
            limit=limit,
            offset=offset,
        )

    def rank_listings(
        self,
        candidates: list[dict[str, Any]],
        soft_facts: SoftFacts,
        user_profile: dict[str, Any] | None = None,
    ) -> list[RankedListingResult]:
        return rank_rule_based_listings(candidates, soft_facts, user_profile=user_profile)
