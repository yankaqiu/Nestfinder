from __future__ import annotations

import logging

from fastapi import APIRouter

from app.config import get_settings
from app.harness.search_service import query_from_filters, query_from_text
from app.models.schemas import (
    HealthResponse,
    ListingsQueryRequest,
    ListingsResponse,
    ListingsSearchRequest,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.post("/listings", response_model=ListingsResponse)
def listings(request: ListingsQueryRequest) -> ListingsResponse:
    settings = get_settings()
    logger.info(
        "API /listings query=%r limit=%s offset=%s",
        request.query,
        request.limit,
        request.offset,
    )
    response = query_from_text(
        db_path=settings.db_path,
        query=request.query,
        limit=request.limit,
        offset=request.offset,
        user_id=request.user_id,
    )
    logger.info(
        "API /listings returned %s listings via %s",
        len(response.listings),
        response.meta.get("strategy_id"),
    )
    return response


@router.post("/listings/search/filter", response_model=ListingsResponse)
def listings_search(request: ListingsSearchRequest) -> ListingsResponse:
    settings = get_settings()
    logger.info("API /listings/search/filter invoked")
    response = query_from_filters(
        db_path=settings.db_path,
        hard_facts=request.hard_filters,
    )
    logger.info(
        "API /listings/search/filter returned %s listings via %s",
        len(response.listings),
        response.meta.get("strategy_id"),
    )
    return response
