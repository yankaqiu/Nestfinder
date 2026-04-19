from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

from app.core.hard_filters import HardFilterParams, search_listings
from app.models.schemas import HardFilters, ListingsResponse
from app.preferences import build_user_profile, log_search
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts


def filter_hard_facts(db_path: Path, hard_facts: HardFilters) -> list[dict[str, Any]]:
    return search_listings(db_path, to_hard_filter_params(hard_facts))


def query_from_text(
    *,
    db_path: Path,
    query: str,
    limit: int,
    offset: int,
    session_id: str | None = None,
) -> ListingsResponse:
    effective_session_id = session_id or f"sess_{uuid4().hex[:16]}"
    hard_facts = extract_hard_facts(query)
    hard_facts.limit = limit
    hard_facts.offset = offset
    soft_facts = extract_soft_facts(query)
    candidates = filter_hard_facts(db_path, hard_facts)
    candidates = filter_soft_facts(candidates, soft_facts)
    user_profile = build_user_profile(session_id=effective_session_id)
    ranked = rank_listings(candidates, soft_facts, user_profile=user_profile)
    log_search(query=query, session_id=effective_session_id, result_count=len(ranked))
    return ListingsResponse(
        listings=ranked,
        meta={
            "query": query,
            "session_id": effective_session_id,
            "user_profile_applied": bool(
                user_profile.get("preferred_cities")
                or user_profile.get("preferred_features")
                or user_profile.get("clicked_listing_ids")
                or user_profile.get("favorite_listing_ids")
            ),
        },
    )


def query_from_filters(
    *,
    db_path: Path,
    hard_facts: HardFilters | None,
    session_id: str | None = None,
) -> ListingsResponse:
    structured_hard_facts = hard_facts or HardFilters()
    effective_session_id = session_id or f"sess_{uuid4().hex[:16]}"
    soft_facts = extract_soft_facts("")
    candidates = filter_hard_facts(db_path, structured_hard_facts)
    candidates = filter_soft_facts(candidates, soft_facts)
    user_profile = build_user_profile(session_id=effective_session_id)
    return ListingsResponse(
        listings=rank_listings(candidates, soft_facts, user_profile=user_profile),
        meta={
            "query": "",
            "session_id": effective_session_id,
            "user_profile_applied": bool(
                user_profile.get("preferred_cities")
                or user_profile.get("preferred_features")
                or user_profile.get("clicked_listing_ids")
                or user_profile.get("favorite_listing_ids")
            ),
        },
    )


def to_hard_filter_params(hard_facts: HardFilters) -> HardFilterParams:
    return HardFilterParams(
        city=hard_facts.city,
        postal_code=hard_facts.postal_code,
        canton=hard_facts.canton,
        min_price=hard_facts.min_price,
        max_price=hard_facts.max_price,
        min_rooms=hard_facts.min_rooms,
        max_rooms=hard_facts.max_rooms,
        latitude=hard_facts.latitude,
        longitude=hard_facts.longitude,
        radius_km=hard_facts.radius_km,
        features=hard_facts.features,
        offer_type=hard_facts.offer_type,
        object_category=hard_facts.object_category,
        limit=hard_facts.limit,
        offset=hard_facts.offset,
        sort_by=hard_facts.sort_by,
    )
