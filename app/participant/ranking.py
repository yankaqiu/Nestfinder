from __future__ import annotations

import json
from typing import Any

from app.models.schemas import ListingData, RankedListingResult
from app.participant.image_rag_client import search_image_rag


def rank_listings(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[RankedListingResult]:
    image_ranked = _rank_with_image_rag(candidates, soft_facts)
    if image_ranked is not None:
        return image_ranked

    return [
        RankedListingResult(
            listing_id=str(candidate["listing_id"]),
            score=1.0,
            reason="Matched hard filters; soft ranking stub.",
            listing=_to_listing_data(candidate),
        )
        for candidate in candidates
    ]


def _rank_with_image_rag(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[RankedListingResult] | None:
    query_text = str(soft_facts.get("raw_query") or "").strip()
    listing_ids = [str(candidate["listing_id"]) for candidate in candidates]
    if not query_text or not listing_ids:
        return None

    try:
        payload = search_image_rag(
            query_text=query_text,
            listing_ids=listing_ids,
            top_k=len(listing_ids),
        )
    except Exception:
        return None

    if not payload:
        return None

    scores_by_listing: dict[str, tuple[float, str | None]] = {}
    for item in payload.get("results", []):
        if not isinstance(item, dict):
            continue
        listing_id = str(item.get("listing_id") or "")
        if not listing_id:
            continue
        scores_by_listing[listing_id] = (
            float(item.get("score", 0.0)),
            item.get("best_image_url"),
        )

    if not scores_by_listing:
        return None

    ranked_candidates = sorted(
        candidates,
        key=lambda candidate: (
            -scores_by_listing.get(str(candidate["listing_id"]), (0.0, None))[0],
            str(candidate["listing_id"]),
        ),
    )

    ranked_results: list[RankedListingResult] = []
    for candidate in ranked_candidates:
        listing_id = str(candidate["listing_id"])
        score, best_image_url = scores_by_listing.get(listing_id, (0.0, None))
        listing = _to_listing_data(candidate)
        if best_image_url and not listing.hero_image_url:
            listing.hero_image_url = best_image_url
        if best_image_url and not listing.image_urls:
            listing.image_urls = [best_image_url]
        ranked_results.append(
            RankedListingResult(
                listing_id=listing_id,
                score=score,
                reason="Ranked by image similarity service." if score > 0 else "No image match from image similarity service.",
                listing=listing,
            )
        )

    return ranked_results


def _to_listing_data(candidate: dict[str, Any]) -> ListingData:
    return ListingData(
        id=str(candidate["listing_id"]),
        title=candidate["title"],
        description=candidate.get("description"),
        street=candidate.get("street"),
        city=candidate.get("city"),
        postal_code=candidate.get("postal_code"),
        canton=candidate.get("canton"),
        latitude=candidate.get("latitude"),
        longitude=candidate.get("longitude"),
        price_chf=candidate.get("price"),
        rooms=candidate.get("rooms"),
        living_area_sqm=_coerce_int(candidate.get("area")),
        available_from=candidate.get("available_from"),
        image_urls=_coerce_image_urls(candidate.get("image_urls")),
        hero_image_url=candidate.get("hero_image_url"),
        original_listing_url=candidate.get("original_url"),
        features=candidate.get("features") or [],
        offer_type=candidate.get("offer_type"),
        object_category=candidate.get("object_category"),
        object_type=candidate.get("object_type"),
    )


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _coerce_image_urls(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return None
