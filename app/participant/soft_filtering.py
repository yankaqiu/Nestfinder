from __future__ import annotations

from typing import Any

from app.participant.soft_fact_extraction import SOFT_SIGNAL_PATTERNS


DEFAULT_SOFT_FILTER_POOL_SIZE = 60

_FEATURE_SIGNAL_MAP: dict[str, tuple[str, ...]] = {
    "balcony": ("balcony",),
    "outdoor_space": ("balcony",),
    "parking": ("parking", "garage"),
    "fireplace": ("fireplace",),
    "child_friendly": ("child_friendly",),
    "family_friendly": ("child_friendly",),
    "pets_allowed": ("pets_allowed",),
    "private_laundry": ("private_laundry",),
    "new_build": ("new_build",),
    "minergie": ("minergie_certified",),
}

_PUBLIC_TRANSPORT_SIGNALS = {
    "public_transport",
    "short_commute",
    "near_hb",
    "near_eth",
    "near_epfl",
}
_SCHOOL_SIGNALS = {
    "good_schools",
    "child_friendly",
    "family_friendly",
}


def filter_soft_facts(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
    *,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict[str, Any]]:
    scored_candidates = _score_candidates(candidates, soft_facts)
    if not _has_active_soft_preferences(soft_facts):
        return scored_candidates

    requested_window = offset + (limit or 0)
    keep_count = min(
        len(scored_candidates),
        max(DEFAULT_SOFT_FILTER_POOL_SIZE, requested_window),
    )
    return scored_candidates[:keep_count]


def _score_candidates(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[dict[str, Any]]:
    signals = soft_facts.get("signals") or {}
    price_bounds = _numeric_bounds(candidates, "price")
    area_bounds = _numeric_bounds(candidates, "area")
    room_bounds = _numeric_bounds(candidates, "rooms")
    ranked: list[tuple[float, int, dict[str, Any]]] = []

    for index, candidate in enumerate(candidates):
        score, reasons = _score_candidate(
            candidate,
            signals=signals,
            soft_facts=soft_facts,
            price_bounds=price_bounds,
            area_bounds=area_bounds,
            room_bounds=room_bounds,
        )
        enriched_candidate = dict(candidate)
        enriched_candidate["_soft_score"] = round(score, 4)
        enriched_candidate["_soft_reasons"] = reasons
        ranked.append((score, index, enriched_candidate))

    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [candidate for _, _, candidate in ranked]


def _score_candidate(
    candidate: dict[str, Any],
    *,
    signals: dict[str, float],
    soft_facts: dict[str, Any],
    price_bounds: tuple[float, float] | None,
    area_bounds: tuple[float, float] | None,
    room_bounds: tuple[float, float] | None,
) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    text_blob = _candidate_text(candidate)
    features = {str(feature).lower() for feature in candidate.get("features") or []}

    for signal_name, weight in signals.items():
        if _matches_feature_signal(features, signal_name):
            score += weight
            reasons.append(_reason_label(signal_name))

        pattern = SOFT_SIGNAL_PATTERNS.get(signal_name)
        if pattern and pattern.search(text_blob):
            score += weight * 0.8
            reasons.append(_reason_label(signal_name))

        if signal_name in _PUBLIC_TRANSPORT_SIGNALS:
            transport_score = _distance_score(candidate.get("distance_public_transport"))
            if transport_score > 0:
                score += weight * transport_score
                reasons.append("near public transport")

        if signal_name in _SCHOOL_SIGNALS:
            school_score = _school_distance_score(candidate)
            if school_score > 0:
                score += weight * school_score
                reasons.append("close to schools")

        if signal_name == "affordable":
            affordability_score = _inverse_relative_score(candidate.get("price"), price_bounds)
            if affordability_score > 0:
                score += weight * affordability_score
                reasons.append("more affordable in this pool")

        if signal_name == "spacious":
            spacious_score = max(
                _relative_score(candidate.get("area"), area_bounds),
                _relative_score(candidate.get("rooms"), room_bounds),
            )
            if spacious_score > 0:
                score += weight * spacious_score
                reasons.append("more spacious in this pool")

    preferred_min_area = _coerce_float(soft_facts.get("preferred_min_area_sqm"))
    if preferred_min_area and preferred_min_area > 0:
        area = _coerce_float(candidate.get("area"))
        if area is not None:
            area_score = min(area / preferred_min_area, 1.0)
            if area_score > 0:
                score += area_score
                reasons.append(f"meets area preference ({int(preferred_min_area)} sqm)")

    if soft_facts.get("max_commute_minutes"):
        commute_score = _distance_score(candidate.get("distance_public_transport"))
        if commute_score > 0:
            score += commute_score * 0.6
            reasons.append("supports a shorter commute")

    return score, _dedupe_preserve_order(reasons)


def _has_active_soft_preferences(soft_facts: dict[str, Any]) -> bool:
    return bool(
        soft_facts.get("signals")
        or soft_facts.get("preferred_min_area_sqm")
        or soft_facts.get("max_commute_minutes")
        or soft_facts.get("commute_destination")
    )


def _candidate_text(candidate: dict[str, Any]) -> str:
    parts = [
        candidate.get("title"),
        candidate.get("description"),
        candidate.get("city"),
        candidate.get("object_category"),
        candidate.get("object_type"),
    ]
    parts.extend(candidate.get("features") or [])
    return " ".join(str(part) for part in parts if part).lower()


def _matches_feature_signal(features: set[str], signal_name: str) -> bool:
    return any(feature in features for feature in _FEATURE_SIGNAL_MAP.get(signal_name, ()))


def _numeric_bounds(
    candidates: list[dict[str, Any]],
    field_name: str,
) -> tuple[float, float] | None:
    values = [
        value
        for candidate in candidates
        if (value := _coerce_float(candidate.get(field_name))) is not None
    ]
    if not values:
        return None
    return min(values), max(values)


def _relative_score(value: Any, bounds: tuple[float, float] | None) -> float:
    numeric_value = _coerce_float(value)
    if numeric_value is None or bounds is None:
        return 0.0
    minimum, maximum = bounds
    if maximum <= minimum:
        return 0.0
    return (numeric_value - minimum) / (maximum - minimum)


def _inverse_relative_score(value: Any, bounds: tuple[float, float] | None) -> float:
    numeric_value = _coerce_float(value)
    if numeric_value is None or bounds is None:
        return 0.0
    minimum, maximum = bounds
    if maximum <= minimum:
        return 0.0
    return (maximum - numeric_value) / (maximum - minimum)


def _distance_score(value: Any) -> float:
    distance_meters = _coerce_float(value)
    if distance_meters is None:
        return 0.0
    if distance_meters <= 300:
        return 1.0
    if distance_meters <= 600:
        return 0.75
    if distance_meters <= 1000:
        return 0.5
    if distance_meters <= 1500:
        return 0.25
    return 0.0


def _school_distance_score(candidate: dict[str, Any]) -> float:
    nearby_scores = [
        _distance_score(candidate.get("distance_kindergarten")),
        _distance_score(candidate.get("distance_school_1")),
        _distance_score(candidate.get("distance_school_2")),
    ]
    return max(nearby_scores, default=0.0)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _reason_label(signal_name: str) -> str:
    return signal_name.replace("_", " ")


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped
