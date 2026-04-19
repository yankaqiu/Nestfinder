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


def filter_hard_facts(
    db_path: Path,
    hard_facts: HardFilters,
    *,
    paginate: bool = True,
    max_candidates: int | None = None,
) -> list[dict[str, Any]]:
    params = to_hard_filter_params(hard_facts)
    if not paginate:
        params.limit = max_candidates
        params.offset = 0
    return search_listings(db_path, params)


def query_from_text(
    *,
    db_path: Path,
    query: str,
    limit: int,
    offset: int,
    session_id: str | None = None,
    max_unfiltered_candidates: int | None = None,
) -> ListingsResponse:
    effective_session_id = session_id or f"sess_{uuid4().hex[:16]}"
    hard_facts = extract_hard_facts(query)
    soft_facts = extract_soft_facts(query)
    candidates = filter_hard_facts(
        db_path,
        hard_facts,
        paginate=False,
        max_candidates=_candidate_cap_for_hard_facts(
            hard_facts,
            max_unfiltered_candidates=max_unfiltered_candidates,
        ),
    )
    candidates = filter_soft_facts(candidates, soft_facts)
    user_profile = build_user_profile(session_id=effective_session_id)
    ranked = rank_listings(candidates, soft_facts, user_profile=user_profile)
    paged_ranked = _paginate_ranked(ranked, limit=limit, offset=offset)
    log_search(query=query, session_id=effective_session_id, result_count=len(paged_ranked))
    return ListingsResponse(
        listings=paged_ranked,
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
    max_unfiltered_candidates: int | None = None,
) -> ListingsResponse:
    structured_hard_facts = hard_facts or HardFilters()
    effective_session_id = session_id or f"sess_{uuid4().hex[:16]}"
    requested_limit = structured_hard_facts.limit
    requested_offset = structured_hard_facts.offset
    soft_facts = extract_soft_facts("")
    candidates = filter_hard_facts(
        db_path,
        structured_hard_facts,
        paginate=False,
        max_candidates=_candidate_cap_for_hard_facts(
            structured_hard_facts,
            max_unfiltered_candidates=max_unfiltered_candidates,
        ),
    )
    candidates = filter_soft_facts(candidates, soft_facts)
    user_profile = build_user_profile(session_id=effective_session_id)
    ranked = rank_listings(candidates, soft_facts, user_profile=user_profile)
    return ListingsResponse(
        listings=_paginate_ranked(ranked, limit=requested_limit, offset=requested_offset),
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


def _paginate_ranked(
    ranked: list[Any],
    *,
    limit: int,
    offset: int,
) -> list[Any]:
    start = max(offset, 0)
    return ranked[start : start + limit]


def _candidate_cap_for_hard_facts(
    hard_facts: HardFilters,
    *,
    max_unfiltered_candidates: int | None,
) -> int | None:
    if max_unfiltered_candidates is None:
        return None
    return max_unfiltered_candidates if not _has_effective_hard_constraints(hard_facts) else None


def _has_effective_hard_constraints(hard_facts: HardFilters) -> bool:
    return any(
        (
            hard_facts.city,
            hard_facts.postal_code,
            hard_facts.canton,
            hard_facts.district_name,
            hard_facts.municipality_name,
            hard_facts.min_price is not None,
            hard_facts.max_price is not None,
            hard_facts.min_rooms is not None,
            hard_facts.max_rooms is not None,
            hard_facts.latitude is not None,
            hard_facts.longitude is not None,
            hard_facts.radius_km is not None,
            hard_facts.features,
            hard_facts.offer_type,
            hard_facts.object_category,
        )
    )
