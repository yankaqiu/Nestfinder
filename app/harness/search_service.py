from __future__ import annotations

from pathlib import Path
from app.models.schemas import HardFilters, ListingsResponse
from app.recommendation.router import get_recommendation_router
from app.recommendation.utils import filter_hard_facts, to_hard_filter_params


def query_from_text(
    *,
    db_path: Path,
    query: str,
    limit: int,
    offset: int,
    strategy_id: str | None = None,
    user_id: str | None = None,
) -> ListingsResponse:
    return get_recommendation_router().query_from_text(
        db_path=db_path,
        query=query,
        limit=limit,
        offset=offset,
        strategy_id=strategy_id,
        user_id=user_id,
    )


def query_from_filters(
    *,
    db_path: Path,
    hard_facts: HardFilters | None,
    strategy_id: str | None = None,
) -> ListingsResponse:
    return get_recommendation_router().query_from_filters(
        db_path=db_path,
        hard_facts=hard_facts,
        strategy_id=strategy_id,
    )
