from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from app.models.schemas import HardFilters, ListingsResponse
from app.recommendation.base import RecommendationStrategy
from app.recommendation.registry import RecommendationRegistry, build_recommendation_registry

DEFAULT_RECOMMENDATION_STRATEGY_ID = "llm_extraction_baseline"


class RecommendationRouter:
    def __init__(
        self,
        registry: RecommendationRegistry,
        *,
        default_strategy_id: str = DEFAULT_RECOMMENDATION_STRATEGY_ID,
    ) -> None:
        self._registry = registry
        self._default_strategy_id = default_strategy_id

    def query_from_text(
        self,
        *,
        db_path: Path,
        query: str,
        limit: int,
        offset: int,
        strategy_id: str | None = None,
    ) -> ListingsResponse:
        strategy, selection_source = self.select_strategy(requested_strategy_id=strategy_id)
        response = strategy.query_from_text(
            db_path=db_path,
            query=query,
            limit=limit,
            offset=offset,
        )
        return self._with_router_meta(
            response,
            selection_source=selection_source,
        )

    def query_from_filters(
        self,
        *,
        db_path: Path,
        hard_facts: HardFilters | None,
        strategy_id: str | None = None,
    ) -> ListingsResponse:
        strategy, selection_source = self.select_strategy(requested_strategy_id=strategy_id)
        response = strategy.query_from_filters(
            db_path=db_path,
            hard_facts=hard_facts,
        )
        return self._with_router_meta(
            response,
            selection_source=selection_source,
        )

    def select_strategy(
        self,
        *,
        requested_strategy_id: str | None = None,
    ) -> tuple[RecommendationStrategy, str]:
        if requested_strategy_id:
            return self._registry.get(requested_strategy_id), "request"

        configured_strategy_id = os.getenv("LISTINGS_RECOMMENDATION_STRATEGY")
        if configured_strategy_id:
            configured_strategy = self._registry.get_or_none(configured_strategy_id)
            if configured_strategy is not None:
                return configured_strategy, "env"

        return self._registry.default(self._default_strategy_id), "default"

    def _with_router_meta(
        self,
        response: ListingsResponse,
        *,
        selection_source: str,
    ) -> ListingsResponse:
        meta = dict(response.meta)
        meta["available_strategy_ids"] = self._registry.ids()
        meta["strategy_selection_source"] = selection_source
        return response.model_copy(update={"meta": meta})


@lru_cache
def get_recommendation_router() -> RecommendationRouter:
    return RecommendationRouter(build_recommendation_registry())
