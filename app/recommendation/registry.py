from __future__ import annotations

from dataclasses import dataclass

from app.recommendation.base import RecommendationStrategy
from app.recommendation.strategies.image_rag_rule_based import RuleBasedImageRagStrategy
from app.recommendation.strategies.llm_extraction_baseline import (
    LlmExtractionBaselineStrategy,
)


@dataclass(slots=True)
class RecommendationRegistry:
    strategies_by_id: dict[str, RecommendationStrategy]

    def get(self, strategy_id: str) -> RecommendationStrategy:
        strategy = self.strategies_by_id.get(strategy_id)
        if strategy is None:
            available = ", ".join(self.ids())
            raise ValueError(
                f"Unknown recommendation strategy {strategy_id!r}. Available strategies: {available}"
            )
        return strategy

    def get_or_none(self, strategy_id: str) -> RecommendationStrategy | None:
        return self.strategies_by_id.get(strategy_id)

    def default(self, strategy_id: str) -> RecommendationStrategy:
        strategy = self.get_or_none(strategy_id)
        if strategy is not None:
            return strategy
        return next(iter(self.strategies_by_id.values()))

    def ids(self) -> list[str]:
        return list(self.strategies_by_id)


def build_recommendation_registry() -> RecommendationRegistry:
    strategies: list[RecommendationStrategy] = [
        RuleBasedImageRagStrategy(),
        LlmExtractionBaselineStrategy(),
    ]
    return RecommendationRegistry(
        strategies_by_id={strategy.strategy_id: strategy for strategy in strategies}
    )
