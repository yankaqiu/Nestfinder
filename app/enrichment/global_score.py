"""Global quality score for listings.

Computes a composite score in [0, 1] from seven orthogonal dimensions:
  1. Value        – price relative to municipality/city median
  2. Amenity      – count of positive feature flags
  3. Location     – urban flag, lake proximity
  4. Building     – construction/renovation year, floor level
  5. Completeness – how many fields are populated
  6. Freshness    – how soon the listing is available
  7. Transit      – 3-tier transit proximity (stop, train, HB)

The global score acts as a subtle quality boost on top of the existing
soft-signal, user-preference, and image-rag ranking layers.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

W_VALUE = 0.20
W_AMENITY = 0.18
W_LOCATION = 0.17
W_BUILDING = 0.13
W_COMPLETENESS = 0.10
W_FRESHNESS = 0.10
W_TRANSIT = 0.12

_NEUTRAL = 0.4


def _bin3_gs(
    value: float | None,
    t1: float, t2: float, t3: float,
    *,
    lower_is_better: bool = True,
) -> float:
    """3-bin scorer for global_score module."""
    if value is None:
        return 0.0
    if lower_is_better:
        if value <= t3:
            return 1.0
        if value <= t2:
            return 0.66
        if value <= t1:
            return 0.33
    else:
        if value >= t3:
            return 1.0
        if value >= t2:
            return 0.66
        if value >= t1:
            return 0.33
    return 0.0


def _score_value(
    price_per_m2_vs_muni: float | None,
    price_vs_city_median: float | None,
) -> float:
    ratio = price_per_m2_vs_muni or price_vs_city_median
    if ratio is None:
        return _NEUTRAL
    return max(0.0, min(1.0, 1.0 - (ratio - 0.5)))


def _score_amenity(features_json: Any, text_features_json: Any) -> float:
    count = 0

    if features_json:
        try:
            feats = json.loads(features_json) if isinstance(features_json, str) else features_json
            if isinstance(feats, list):
                count += len(feats)
        except (json.JSONDecodeError, TypeError):
            pass

    if text_features_json:
        try:
            tf = json.loads(text_features_json) if isinstance(text_features_json, str) else text_features_json
            if isinstance(tf, dict):
                count += sum(1 for value in tf.values() if value is True)
        except (json.JSONDecodeError, TypeError):
            pass

    return min(1.0, count / 5.0)


def _score_location(
    is_urban: int | None,
    lake_distance_m: int | None,
    dist_public_transport: float | None,
) -> float:
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
    current_year = date.today().year
    effective_year = max(year_built or 0, renovation_year or 0)
    if effective_year > 0:
        age_score = max(0.0, min(1.0, (effective_year - 1950) / max(current_year - 1950, 1)))
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
    "price",
    "area",
    "rooms",
    "description",
    "latitude",
    "floor_level",
    "year_built",
    "available_from",
]


def _score_completeness(row: dict[str, Any]) -> float:
    filled = sum(1 for field in _COMPLETENESS_FIELDS if row.get(field) is not None)

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

    total_checks = len(_COMPLETENESS_FIELDS) + 2
    return filled / total_checks


def _score_freshness(available_from: str | None) -> float:
    if not available_from:
        return _NEUTRAL
    try:
        avail_date = date.fromisoformat(str(available_from))
    except (ValueError, TypeError):
        return _NEUTRAL

    days_until = (avail_date - date.today()).days
    if days_until <= 0:
        return 1.0
    if days_until <= 30:
        return 0.8
    if days_until <= 90:
        return 0.5
    if days_until <= 180:
        return 0.3
    return 0.1


def _score_transit(
    stop_m: float | None,
    train_m: float | None,
    hb_m: float | None,
) -> float:
    s_stop = _bin3_gs(stop_m, 600, 300, 150, lower_is_better=True)
    s_train = _bin3_gs(train_m, 2000, 800, 300, lower_is_better=True)
    s_hb = _bin3_gs(hb_m, 5000, 2000, 800, lower_is_better=True)
    if s_stop == 0 and s_train == 0 and s_hb == 0:
        return _NEUTRAL
    return 0.4 * s_stop + 0.35 * s_train + 0.25 * s_hb


def compute_global_score(row: dict[str, Any]) -> dict[str, float]:
    sv = _score_value(
        row.get("price_per_m2_vs_municipality"),
        row.get("price_vs_city_median"),
    )
    sa = _score_amenity(row.get("features_json") or row.get("features"), row.get("text_features_json"))
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
    st = _score_transit(
        row.get("nearest_stop_distance_m"),
        row.get("nearest_train_distance_m"),
        row.get("nearest_hb_distance_m"),
    )

    global_s = (
        W_VALUE * sv
        + W_AMENITY * sa
        + W_LOCATION * sl
        + W_BUILDING * sb
        + W_COMPLETENESS * sc
        + W_FRESHNESS * sf
        + W_TRANSIT * st
    )

    return {
        "global_score": round(global_s, 4),
        "score_value": round(sv, 4),
        "score_amenity": round(sa, 4),
        "score_location": round(sl, 4),
        "score_building": round(sb, 4),
        "score_completeness": round(sc, 4),
        "score_freshness": round(sf, 4),
        "score_transit": round(st, 4),
    }


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
    parts: list[str] = []

    sv = scores.get("score_value", 0.0)
    sa = scores.get("score_amenity", 0.0)
    sl = scores.get("score_location", 0.0)
    sb = scores.get("score_building", 0.0)
    sf = scores.get("score_freshness", 0.0)

    value_text = _pick_label(sv, _VALUE_LABELS)
    if candidate:
        value_text += _format_pct(
            candidate.get("price_per_m2_vs_municipality")
            or candidate.get("price_vs_city_median")
        )
        label = candidate.get("price_per_m2_vs_municipality_label")
        if label:
            value_text = str(label)
    parts.append(value_text)

    loc_text = _pick_label(sl, _LOCATION_LABELS)
    if candidate:
        extras: list[str] = []
        if candidate.get("is_urban") == 1:
            extras.append("urban area")
        lake_distance = candidate.get("lake_distance_m")
        if lake_distance is not None and lake_distance < 3000:
            extras.append(f"{int(lake_distance):,}m from lake")
        pt_distance = candidate.get("distance_public_transport")
        if pt_distance is not None and pt_distance < 500:
            extras.append(f"{int(pt_distance)}m to public transport")
        train_name = candidate.get("nearest_train_name")
        train_dist = candidate.get("nearest_train_distance_m")
        if train_name and train_dist is not None and train_dist < 1500:
            extras.append(f"{int(train_dist)}m to {train_name}")
        if extras:
            loc_text += f" ({', '.join(extras)})"
    parts.append(loc_text)

    build_text = _pick_label(sb, _BUILDING_LABELS)
    if candidate:
        details: list[str] = []
        renovation_year = candidate.get("renovation_year")
        year_built = candidate.get("year_built")
        floor_level = candidate.get("floor_level")
        if renovation_year and renovation_year > 0:
            details.append(f"renovated {renovation_year}")
        elif year_built and year_built > 0:
            details.append(f"built {year_built}")
        if floor_level is not None and floor_level > 0:
            details.append(f"floor {floor_level}")
        if details:
            build_text += f" ({', '.join(details)})"
    parts.append(build_text)

    if sa >= 0.4:
        parts.append(_pick_label(sa, _AMENITY_LABELS))
    if sf >= 0.7:
        parts.append(_pick_label(sf, _FRESHNESS_LABELS))

    return ". ".join(parts) + "."


_SELECT_QUERY = """
    SELECT
        listing_id,
        price_vs_city_median,
        price_per_m2_vs_municipality,
        features_json,
        text_features_json,
        is_urban,
        lake_distance_m,
        distance_public_transport,
        nearest_stop_distance_m,
        nearest_train_distance_m,
        nearest_hb_distance_m,
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
) -> dict[str, int | float]:
    query = _SELECT_QUERY
    if not force:
        query += " WHERE global_score IS NULL"

    rows = conn.execute(query).fetchall()
    if not rows:
        logger.info("All listings already have global_score")
        return {"scored": 0}

    logger.info("Computing global_score for %d listings", len(rows))
    updates: list[tuple] = []

    for row in rows:
        row_dict = dict(row)
        scores = compute_global_score(row_dict)
        updates.append(
            (
                scores["global_score"],
                scores["score_value"],
                scores["score_amenity"],
                scores["score_location"],
                scores["score_building"],
                scores["score_completeness"],
                scores["score_freshness"],
                scores["score_transit"],
                row_dict["listing_id"],
            )
        )

    batch_size = 500
    for i in range(0, len(updates), batch_size):
        conn.executemany(
            "UPDATE listings SET "
            "global_score = ?, score_value = ?, score_amenity = ?, "
            "score_location = ?, score_building = ?, "
            "score_completeness = ?, score_freshness = ?, score_transit = ? "
            "WHERE listing_id = ?",
            updates[i : i + batch_size],
        )
    conn.commit()

    avg_score = sum(update[0] for update in updates) / len(updates) if updates else 0.0
    logger.info("Global score done: %d listings scored, avg=%.3f", len(updates), avg_score)
    return {"scored": len(updates), "avg_score": round(avg_score, 3)}
