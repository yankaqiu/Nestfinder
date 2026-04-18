from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from app.models.schemas import HardFilters, ListingsResponse, RankedListingResult
from app.recommendation.utils import filter_hard_facts

DEFAULT_RERANK_CANDIDATE_POOL_SIZE = 100

SoftFacts = dict[str, Any]


class RecommendationStrategy(ABC):
    strategy_id: str
    display_name: str
    description: str = ""
    rerank_candidate_pool_size: int = DEFAULT_RERANK_CANDIDATE_POOL_SIZE

    def query_from_text(
        self,
        *,
        db_path: Path,
        query: str,
        limit: int,
        offset: int,
    ) -> ListingsResponse:
        raw_hard, soft_facts = asyncio.run(self._extract_parallel(query))
        hard_facts = self.prepare_hard_facts(raw_hard, limit=limit, offset=offset)
        return self._build_response(
            db_path=db_path,
            hard_facts=hard_facts,
            soft_facts=soft_facts,
            response_limit=limit,
            response_offset=offset,
        )

    def query_from_filters(
        self,
        *,
        db_path: Path,
        hard_facts: HardFilters | None,
    ) -> ListingsResponse:
        requested_hard_facts = hard_facts or HardFilters()
        prepared_hard_facts = self.prepare_hard_facts(
            requested_hard_facts,
            limit=requested_hard_facts.limit,
            offset=requested_hard_facts.offset,
        )
        return self._build_response(
            db_path=db_path,
            hard_facts=prepared_hard_facts,
            soft_facts=self.empty_soft_facts(),
            response_limit=requested_hard_facts.limit,
            response_offset=requested_hard_facts.offset,
        )

    def prepare_hard_facts(
        self,
        hard_facts: HardFilters,
        *,
        limit: int,
        offset: int,
    ) -> HardFilters:
        candidate_limit = max(hard_facts.limit, limit + offset, self.rerank_candidate_pool_size)
        return hard_facts.model_copy(
            update={
                "limit": candidate_limit,
                "offset": 0,
            }
        )

    def empty_soft_facts(self) -> SoftFacts:
        return {
            "raw_query": "",
            "signals": {},
        }

    def response_meta(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.display_name,
            "strategy_description": self.description,
        }

    def _build_response(
        self,
        *,
        db_path: Path,
        hard_facts: HardFilters,
        soft_facts: SoftFacts,
        response_limit: int,
        response_offset: int,
    ) -> ListingsResponse:
        candidates = filter_hard_facts(db_path, hard_facts)
        candidates = self.filter_soft_facts(
            candidates,
            soft_facts,
            limit=response_limit,
            offset=response_offset,
        )
        ranked = self.rank_listings(candidates, soft_facts)
        return ListingsResponse(
            listings=ranked[response_offset : response_offset + response_limit],
            meta=self.response_meta(),
        )

    async def _extract_parallel(self, query: str) -> tuple[HardFilters, SoftFacts]:
        loop = asyncio.get_event_loop()
        hard_task = loop.run_in_executor(None, self.extract_hard_facts, query)
        soft_task = loop.run_in_executor(None, self.extract_soft_facts, query)
        return await asyncio.gather(hard_task, soft_task)  # type: ignore[return-value]

    @abstractmethod
    def extract_hard_facts(self, query: str) -> HardFilters:
        raise NotImplementedError

    @abstractmethod
    def extract_soft_facts(self, query: str) -> SoftFacts:
        raise NotImplementedError

    @abstractmethod
    def filter_soft_facts(
        self,
        candidates: list[dict[str, Any]],
        soft_facts: SoftFacts,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def rank_listings(
        self,
        candidates: list[dict[str, Any]],
        soft_facts: SoftFacts,
    ) -> list[RankedListingResult]:
        raise NotImplementedError
