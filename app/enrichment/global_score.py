"""Global quality score for listings.

Computes a composite score in [0, 1] from six orthogonal dimensions:
  1. Value       – price relative to city median (cheaper = better value)
  2. Amenity     – count of positive feature flags
  3. Location    – urban flag, lake proximity, transit proximity
  4. Building    – construction/renovation year, floor level
  5. Completeness – how many fields are populated
  6. Freshness   – how soon the listing is available

The global_score acts as a tiebreaker when soft-signal scores are equal
and as a standalone quality indicator for hard-filter-only queries.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dimension weights (must sum to 1.0)
# ---------------------------------------------------------------------------
W_VALUE = 0.25
W_AMENITY = 0.20
W_LOCATION = 0.20
W_BUILDING = 0.15
W_COMPLETENESS = 0.10
W_FRESHNESS = 0.10

# Neutral defaults for missing data (avoids penalising sparse sources)
_NEUTRAL = 0.4


# ---------------------------------------------------------------------------
# Sub-score functions — each returns a float in [0, 1]
# ---------------------------------------------------------------------------

def _score_value(price_vs_median: float | None) -> float:
    """Lower than city median → higher score. 0.85 ratio is the sweet spot."""
    if price_vs_median is None:
        return _NEUTRAL
    # ratio < 0.7  → excellent value  → ~1.0
    # ratio = 1.0  → average          → ~0.5
    # ratio > 1.5  → expensive        → ~0.0
    return max(0.0, min(1.0, 1.0 - (price_vs_median - 0.5) * 1.0))


def _score_amenity(features_json: str | None, text_features_json: str | None) -> float:
    """More amenities → higher score. 5+ features = max."""
    count = 0

    if features_json:
        try:
            feats = json.loads(features_json)
            if isinstance(feats, list):
                count += len(feats)
        except (json.JSONDecodeError, TypeError):
            pass

    if text_features_json:
        try:
            tf = json.loads(text_features_json)
            if isinstance(tf, dict):
                count += sum(1 for v in tf.values() if v is True)
        except (json.JSONDecodeError, TypeError):
            pass

    return min(1.0, count / 5.0)


def _score_location(
    is_urban: int | None,
    lake_distance_m: int | None,
    dist_public_transport: float | None,
) -> float:
    """Composite of urbanity, lake proximity, and transit access."""
    urban_pts = 0.4 if is_urban == 1 else (0.15 if is_urban == 0 else 0.2)

    if lake_distance_m is not None:
        lake_pts = 0.3 * max(0.0, 1.0 - lake_distance_m / 10_000)
    else:
        lake_pts = 0.3 * 0.3

    if dist_public_transport is not None:
        transit_pts = 0.3 * max(0.0, 1.0 - min(dist_public_transport, 1000) / 1000)
    else:
        transit_pts = 0.3 * _NEUTRAL

    return min(1.0, urban_pts + lake_pts + transit_pts)


def _score_building(
    year_built: int | None,
    renovation_year: int | None,
    floor_level: int | None,
) -> float:
    """Newer/renovated buildings and higher floors score better."""
    effective_year = max(year_built or 0, renovation_year or 0)
    if effective_year > 0:
        age_score = max(0.0, min(1.0, (effective_year - 1950) / (2025 - 1950)))
    else:
        age_score = _NEUTRAL

    if floor_level is not None:
        if floor_level < 0:
            floor_score = 0.1
        else:
            floor_score = min(1.0, floor_level / 6.0)
    else:
        floor_score = 0.3

    return 0.6 * age_score + 0.4 * floor_score


_COMPLETENESS_FIELDS = [
    "price", "area", "rooms", "description", "latitude",
    "floor_level", "year_built", "available_from",
]


def _score_completeness(row: dict[str, Any]) -> float:
    """Listings with more populated fields are typically higher quality."""
    filled = sum(1 for f in _COMPLETENESS_FIELDS if row.get(f) is not None)

    has_images = False
    img_raw = row.get("images_json")
    if img_raw:
        try:
            img_data = json.loads(img_raw) if isinstance(img_raw, str) else img_raw
            images = img_data.get("images", []) if isinstance(img_data, dict) else []
            has_images = len(images) > 1
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
    if has_images:
        filled += 1

    has_features = bool(row.get("features_json") or row.get("text_features_json"))
    if has_features:
        filled += 1

    total_checks = len(_COMPLETENESS_FIELDS) + 2  # +images, +features
    return filled / total_checks


def _score_freshness(available_from: str | None) -> float:
    """Available sooner → more actionable → higher score."""
    if not available_from:
        return _NEUTRAL
    try:
        avail_date = date.fromisoformat(str(available_from))
    except (ValueError, TypeError):
        return _NEUTRAL

    today = date.today()
    days_until = (avail_date - today).days

    if days_until <= 0:
        return 1.0
    if days_until <= 30:
        return 0.8
    if days_until <= 90:
        return 0.5
    if days_until <= 180:
        return 0.3
    return 0.1


# ---------------------------------------------------------------------------
# Composite score
# ---------------------------------------------------------------------------

def compute_global_score(row: dict[str, Any]) -> dict[str, float]:
    """Compute all sub-scores and the weighted global score for a single listing.

    Returns dict with keys: global_score, score_value, score_amenity,
    score_location, score_building, score_completeness, score_freshness.
    """
    sv = _score_value(row.get("price_vs_city_median"))
    sa = _score_amenity(row.get("features_json"), row.get("text_features_json"))
    sl = _score_location(
        row.get("is_urban"),
        row.get("lake_distance_m"),
        row.get("distance_public_transport"),
    )
    sb = _score_building(
        row.get("year_built"),
        row.get("renovation_year"),
        row.get("floor_level"),
    )
    sc = _score_completeness(row)
    sf = _score_freshness(row.get("available_from"))

    global_s = (
        W_VALUE * sv
        + W_AMENITY * sa
        + W_LOCATION * sl
        + W_BUILDING * sb
        + W_COMPLETENESS * sc
        + W_FRESHNESS * sf
    )

    return {
        "global_score": round(global_s, 4),
        "score_value": round(sv, 4),
        "score_amenity": round(sa, 4),
        "score_location": round(sl, 4),
        "score_building": round(sb, 4),
        "score_completeness": round(sc, 4),
        "score_freshness": round(sf, 4),
    }


# ---------------------------------------------------------------------------
# Human-readable explanations
# ---------------------------------------------------------------------------

_VALUE_LABELS = [
    (0.85, "Excellent value — well below city median"),
    (0.65, "Good value — below city median"),
    (0.45, "Average price for the area"),
    (0.0, "Above-average price for the area"),
]

_AMENITY_LABELS = [
    (0.8, "Well-equipped — many amenities"),
    (0.4, "Some amenities included"),
    (0.0, "Few amenities listed"),
]

_LOCATION_LABELS = [
    (0.7, "Prime location — urban, well-connected"),
    (0.5, "Good location"),
    (0.3, "Moderate location"),
    (0.0, "Peripheral location"),
]

_BUILDING_LABELS = [
    (0.7, "Modern or recently renovated building"),
    (0.5, "Well-maintained building"),
    (0.3, "Older building"),
    (0.0, "Dated building"),
]

_FRESHNESS_LABELS = [
    (0.8, "Available now or very soon"),
    (0.5, "Available within a few months"),
    (0.3, "Available later"),
    (0.0, "Availability unclear"),
]


def _pick_label(score: float, labels: list[tuple[float, str]]) -> str:
    for threshold, label in labels:
        if score >= threshold:
            return label
    return labels[-1][1]


def _format_pct(ratio: float | None) -> str:
    if ratio is None:
        return ""
    diff = abs(1.0 - ratio) * 100
    if ratio < 1.0:
        return f" ({diff:.0f}% below median)"
    if ratio > 1.0:
        return f" ({diff:.0f}% above median)"
    return " (at median)"


def explain_score(
    scores: dict[str, float],
    candidate: dict[str, Any] | None = None,
) -> str:
    """Generate a human-readable explanation of why a listing got its score.

    Args:
        scores: Dict from compute_global_score (sub-scores + global_score).
        candidate: Optional raw listing dict for richer context (price, year, etc.).

    Returns:
        A short multi-sentence explanation suitable for showing to end users.
    """
    parts: list[str] = []

    sv = scores.get("score_value", 0)
    sa = scores.get("score_amenity", 0)
    sl = scores.get("score_location", 0)
    sb = scores.get("score_building", 0)
    sf = scores.get("score_freshness", 0)

    # Value
    value_text = _pick_label(sv, _VALUE_LABELS)
    if candidate:
        ratio = candidate.get("price_vs_city_median")
        if ratio is not None:
            value_text += _format_pct(ratio)
    parts.append(value_text)

    # Location — add specifics if available
    loc_text = _pick_label(sl, _LOCATION_LABELS)
    if candidate:
        extras = []
        if candidate.get("is_urban") == 1:
            extras.append("urban area")
        lake_d = candidate.get("lake_distance_m")
        if lake_d is not None and lake_d < 3000:
            extras.append(f"{lake_d:,}m from lake")
        pt_d = candidate.get("distance_public_transport")
        if pt_d is not None and pt_d < 500:
            extras.append(f"{int(pt_d)}m to public transport")
        if extras:
            loc_text += f" ({', '.join(extras)})"
    parts.append(loc_text)

    # Building — add year if available
    build_text = _pick_label(sb, _BUILDING_LABELS)
    if candidate:
        reno = candidate.get("renovation_year")
        yb = candidate.get("year_built")
        floor = candidate.get("floor_level")
        details = []
        if reno and reno > 0:
            details.append(f"renovated {reno}")
        elif yb and yb > 0:
            details.append(f"built {yb}")
        if floor is not None and floor > 0:
            details.append(f"floor {floor}")
        if details:
            build_text += f" ({', '.join(details)})"
    parts.append(build_text)

    # Amenities — only mention if noteworthy
    if sa >= 0.4:
        parts.append(_pick_label(sa, _AMENITY_LABELS))

    # Freshness — only mention if noteworthy
    if sf >= 0.7:
        parts.append(_pick_label(sf, _FRESHNESS_LABELS))

    return ". ".join(parts) + "."


# ---------------------------------------------------------------------------
# Batch enrichment entry point
# ---------------------------------------------------------------------------

_SELECT_QUERY = """
    SELECT
        listing_id,
        price_vs_city_median,
        features_json,
        text_features_json,
        is_urban,
        lake_distance_m,
        distance_public_transport,
        year_built,
        renovation_year,
        floor_level,
        price,
        area,
        rooms,
        description,
        latitude,
        available_from,
        images_json
    FROM listings
"""


def enrich_global_score(
    conn: sqlite3.Connection,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Compute and store global_score for all listings.

    Args:
        conn: SQLite connection.
        force: If True, recompute even for listings that already have a score.
    """
    query = _SELECT_QUERY
    if not force:
        query += " WHERE global_score IS NULL"

    rows = conn.execute(query).fetchall()
    if not rows:
        logger.info("All listings already have global_score")
        return {"scored": 0}

    total = len(rows)
    logger.info("Computing global_score for %d listings", total)

    updates: list[tuple] = []
    for row in rows:
        row_dict = dict(row)
        scores = compute_global_score(row_dict)
        updates.append((
            scores["global_score"],
            scores["score_value"],
            scores["score_amenity"],
            scores["score_location"],
            scores["score_building"],
            scores["score_completeness"],
            scores["score_freshness"],
            row_dict["listing_id"],
        ))

    batch_size = 500
    for i in range(0, len(updates), batch_size):
        conn.executemany(
            "UPDATE listings SET "
            "global_score = ?, score_value = ?, score_amenity = ?, "
            "score_location = ?, score_building = ?, "
            "score_completeness = ?, score_freshness = ? "
            "WHERE listing_id = ?",
            updates[i : i + batch_size],
        )
    conn.commit()

    avg_score = sum(u[0] for u in updates) / total if total else 0
    logger.info(
        "Global score done: %d listings scored, avg=%.3f", total, avg_score,
    )
    return {"scored": total, "avg_score": round(avg_score, 3)}
