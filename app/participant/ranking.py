from __future__ import annotations

from dataclasses import dataclass
import json
import re
from datetime import date, timedelta
from typing import Any

from app.enrichment.global_score import compute_global_score, explain_score
from app.models.schemas import ListingData, RankedListingResult
from app.participant.image_rag_client import search_image_rag

MIN_IMAGE_BONUS_CAP = 0.15
DEFAULT_IMAGE_BONUS_CAP = 0.25
MAX_IMAGE_BONUS_CAP = 0.8
MAX_USER_PREFERENCE_BONUS = 0.35
USER_PREFERENCE_LISTING_BONUS = 0.18
USER_PREFERENCE_CITY_BONUS = 0.08
USER_PREFERENCE_PRICE_BONUS = 0.05
USER_PREFERENCE_FEATURE_BONUS = 0.04
MAX_USER_PREFERENCE_FEATURE_BONUS = 0.12
GLOBAL_SCORE_WEIGHT = 0.3

VISUAL_SIGNALS = {
    "bright",
    "views",
    "modern",
    "near_lake",
    "furnished",
    "green_area",
    "lively",
    "well_maintained",
    "outdoor_space",
    "balcony",
    "parking",
    "fireplace",
    "garden",
    "modern_kitchen",
    "modern_bathroom",
    "new_build",
}

NON_VISUAL_SIGNALS = {
    "quiet",
    "public_transport",
    "short_commute",
    "family_friendly",
    "child_friendly",
    "good_schools",
    "affordable",
    "spacious",
    "private_laundry",
    "elevator",
    "dishwasher",
    "cellar",
    "washing_machine",
    "minergie",
    "pets_allowed",
    "student",
    "near_eth",
    "near_epfl",
    "near_hb",
    "specific_move_in",
    "near_train",
    "near_hauptbahnhof",
    "well_connected",
    "good_value_local",
    "low_density",
    "high_density",
    "small_town",
    "large_municipality",
}


@dataclass(slots=True)
class CandidateRankBreakdown:
    candidate: dict[str, Any]
    matched: list[str]
    soft_score: float
    global_scores: dict[str, float]
    global_score_bonus: float
    preference_bonus: float
    preference_reasons: list[str]
    image_score: float
    image_bonus: float
    final_score: float
    best_image_url: str | None
    image_bonus_cap: float
    explanation: str

# ---------------------------------------------------------------------------
# Signal matchers: each maps a soft signal name to a function returning
# a float in [0.0, 1.0] representing match strength.
#   0.0  = no match
#   0.33 = weak match (bottom bin)
#   0.66 = good match (middle bin)
#   1.0  = strong match (top bin)
# Binary signals (feature present/absent) return 1.0 or 0.0.
#
# Strategy: prefer enriched DB columns (data-driven), fall back to
# text regex when enrichment data is missing (text fallback = 0.5).
# ---------------------------------------------------------------------------

_TEXT_FALLBACK = 0.5


def _text(c: dict) -> str:
    """Combined searchable text for a candidate."""
    return ((c.get("title") or "") + " " + (c.get("description") or "")).lower()


def _has_feature(c: dict, feat: str) -> bool:
    feats = c.get("features") or []
    return any(feat.lower() in str(f).lower() for f in feats)


def _get_float(c: dict, field: str) -> float | None:
    val = c.get(field)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _bin3(
    value: float | None,
    t1: float, t2: float, t3: float,
    *,
    lower_is_better: bool = True,
) -> float:
    """3-bin scorer. Returns 0.0, 0.33, 0.66, or 1.0."""
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


def _r(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.I)


_SIGNAL_MATCHERS: dict[str, Any] = {}


def _register_signals():
    """Build the signal matcher table. Each matcher returns float 0.0-1.0."""
    m = _SIGNAL_MATCHERS

    # --- Binned: bright (floor level + text) ---
    m["bright"] = lambda c: max(
        _bin3(_get_float(c, "floor_level"), 2, 4, 6, lower_is_better=False),
        _TEXT_FALLBACK if _r(
            r"\bhell[e]?\b|\bbright\b|\blumineu[xs]\b|\bsonnig\b|\bsunny\b|\blichtdurchflutet\b|\blicht\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: quiet (population density + text) ---
    m["quiet"] = lambda c: max(
        _bin3(_get_float(c, "population_density"), 1000, 500, 200, lower_is_better=True),
        _TEXT_FALLBACK if _r(
            r"\bruhig\b|\bquiet\b|\bcalme\b|\bstill[e]?\s+lage\b|\bruhige\s+lage\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: modern (year_built / renovation_year + text) ---
    m["modern"] = lambda c: max(
        _bin3(_get_float(c, "year_built"), 2005, 2015, 2020, lower_is_better=False),
        _bin3(_get_float(c, "renovation_year"), 2015, 2020, 2023, lower_is_better=False),
        _TEXT_FALLBACK if _r(
            r"\bmodern[e]?\b|\brenoviert\b|\bsaniert\b|\bneuwertig\b|\bcontemporary\b|\bstylish\b|\bdesign\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: views (floor level + text) ---
    m["views"] = lambda c: max(
        _bin3(_get_float(c, "floor_level"), 3, 5, 8, lower_is_better=False),
        _TEXT_FALLBACK if _r(
            r"\baussicht\b|\bview\b|\bvue\b|\bpanorama\b|\bseeblick\b|\bbergblick\b|\bweitsicht\b|\bfernblick\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: near_lake (lake_distance_m + text) ---
    m["near_lake"] = lambda c: max(
        _bin3(_get_float(c, "lake_distance_m"), 5000, 2000, 800, lower_is_better=True),
        _TEXT_FALLBACK if _r(
            r"\bseenähe\b|\bnear.*lake\b|\blac\b|\bseeblick\b|\bam\s+see\b|\blake\s+view\b|\bzürichsee\b|\bgenfersee\b|\bvierwaldstättersee\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: public_transport (nearest_stop_distance_m + text) ---
    m["public_transport"] = lambda c: max(
        _bin3(_get_float(c, "nearest_stop_distance_m"), 600, 300, 150, lower_is_better=True),
        _bin3(_get_float(c, "distance_public_transport"), 600, 300, 150, lower_is_better=True),
        _TEXT_FALLBACK if _r(
            r"\böv\b|\bpublic\s+transport\b|\btram\b|\bs-bahn\b|\bbus\s+station\b|\bbahnhof\b|\bmetro\b|\bgute\s+anbindung\b|\banbindung\b|\bverkehrsanbindung\b"
        ).search(_text(c)) else 0.0,
    )

    m["short_commute"] = m["public_transport"]

    # --- Binary: furnished ---
    m["furnished"] = lambda c: 1.0 if (
        (c.get("is_furnished") == 1)
        or _has_feature(c, "furnished")
        or bool(_r(
            r"\bmöbliert\b|\bfurnished\b|\bmeublé\b|\bwith\s+furniture\b"
        ).search(_text(c)))
    ) else 0.0

    # --- Binned: family_friendly (kindergarten + school distances) ---
    m["family_friendly"] = lambda c: max(
        _bin3(_get_float(c, "distance_kindergarten"), 1500, 800, 400, lower_is_better=True),
        _bin3(_get_float(c, "distance_school_1"), 2000, 1000, 500, lower_is_better=True),
        _TEXT_FALLBACK if (
            _has_feature(c, "child_friendly")
            or bool(_r(r"\bfamilie\b|\bfamily\b|\bfamilienfreundlich\b|\bfamily.friendly\b|\bkinder\b").search(_text(c)))
        ) else 0.0,
    )

    # --- Binary: child_friendly ---
    m["child_friendly"] = lambda c: 1.0 if (
        _has_feature(c, "child_friendly")
        or bool(_r(r"\bkinderfreundlich\b|\bchild.friendly\b|\bspielplatz\b|\bplayground\b|\bkinder\b").search(_text(c)))
    ) else max(
        _bin3(_get_float(c, "distance_kindergarten"), 1200, 600, 300, lower_is_better=True),
        0.0,
    )

    # --- Binned: good_schools (school distance) ---
    m["good_schools"] = lambda c: max(
        _bin3(_get_float(c, "distance_school_1"), 2000, 1000, 500, lower_is_better=True),
        _TEXT_FALLBACK if _r(r"\bschule\b|\bschool\b|\bécole\b|\bgute\s+schulen\b").search(_text(c)) else 0.0,
    )

    # --- Binary: green_area (text only) ---
    m["green_area"] = lambda c: 1.0 if bool(_r(
        r"\bpark\b|\bgarten\b|\bgarden\b|\bgrün[e]?\b|\bwald\b|\bnatur\b|\bforest\b|\bverdure\b"
    ).search(_text(c))) else 0.0

    # --- Binned: lively (population density + text) ---
    m["lively"] = lambda c: max(
        _bin3(_get_float(c, "population_density"), 1500, 3000, 4000, lower_is_better=False),
        _TEXT_FALLBACK if _r(
            r"\bbelebt\b|\blively\b|\bvibrant\b|\banimé\b|\bgastronomie\b|\brestaurant\b|\bcafé\b|\bcafe\b|\bausgang\b|\bnachtleben\b|\bbar[s]?\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: affordable (price_per_m2_vs_municipality, fallback city median + text) ---
    m["affordable"] = lambda c: max(
        _bin3(
            _get_float(c, "price_per_m2_vs_municipality") or _get_float(c, "price_vs_city_median"),
            1.0, 0.85, 0.70, lower_is_better=True,
        ),
        _TEXT_FALLBACK if _r(
            r"\bgünstig\b|\baffordable\b|\bpreiswert\b|\bcheap\b|\bbon\s+marché\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: spacious (area per room ratio + text) ---
    m["spacious"] = lambda c: max(
        _bin3(_area_per_room(c), 25, 35, 45, lower_is_better=False),
        _TEXT_FALLBACK if _r(
            r"\bgeräumig\b|\bspacious\b|\bgrosszügig\b|\bviel\s+platz\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: well_maintained (renovation year + text) ---
    m["well_maintained"] = lambda c: max(
        _bin3(_get_float(c, "renovation_year"), 2010, 2018, 2022, lower_is_better=False),
        _TEXT_FALLBACK if _r(
            r"\bgepflegt\b|\bwell.maintained\b|\bsauber\b|\bpropre\b|\brefurbished\b|\btop\s+zustand\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binary: outdoor_space ---
    m["outdoor_space"] = lambda c: 1.0 if (
        _has_feature(c, "balcony")
        or bool(_r(r"\bbalkon\b|\bbalcony\b|\bterr?asse\b|\bgarten\b|\bgarden\b|\bsitzplatz\b|\bloggia\b").search(_text(c)))
    ) else 0.0

    # --- Binary: balcony ---
    m["balcony"] = lambda c: 1.0 if (
        _has_feature(c, "balcony")
        or bool(_r(r"\bbalkon\b|\bbalcony\b|\bbalcon\b").search(_text(c)))
    ) else 0.0

    # --- Binary: parking ---
    m["parking"] = lambda c: 1.0 if (
        _has_feature(c, "parking") or _has_feature(c, "garage")
        or bool(_r(r"\bparkplatz\b|\bparking\b|\bgarage\b|\beinstellplatz\b|\btiefgarage\b").search(_text(c)))
    ) else 0.0

    # --- Binary: fireplace ---
    m["fireplace"] = lambda c: 1.0 if (
        _has_feature(c, "fireplace")
        or bool(_r(r"\bkamin\b|\bfireplace\b|\bcheminée\b").search(_text(c)))
    ) else 0.0

    # --- Binary: private_laundry ---
    m["private_laundry"] = lambda c: 1.0 if (
        _has_feature(c, "private_laundry") or _has_feature(c, "washing_machine")
        or bool(_r(r"\bwaschmaschine\b|\bwasher\b|\bwaschturm\b|\bprivate\s+laundry\b|\beigene\s+waschmaschine\b").search(_text(c)))
    ) else 0.0

    # --- Binary: elevator ---
    m["elevator"] = lambda c: 1.0 if (
        _has_feature(c, "elevator")
        or bool(_r(r"\blift\b|\belevator\b|\baufzug\b|\bascenseur\b").search(_text(c)))
    ) else 0.0

    # --- Binary: garden ---
    m["garden"] = lambda c: 1.0 if (
        _has_feature(c, "garden")
        or bool(_r(r"\bgarten\b|\bgarden\b|\bjardin\b|\bgiardino\b").search(_text(c)))
    ) else 0.0

    # --- Binary: dishwasher ---
    m["dishwasher"] = lambda c: 1.0 if (
        _has_feature(c, "dishwasher")
        or bool(_r(r"\bgeschirrspüler\b|\bdishwasher\b|\blave.vaisselle\b|\babwaschmaschine\b|\bspülmaschine\b").search(_text(c)))
    ) else 0.0

    # --- Binary: cellar ---
    m["cellar"] = lambda c: 1.0 if (
        _has_feature(c, "cellar")
        or bool(_r(r"\bkeller\b|\bcellar\b|\bcave\b|\bcantina\b|\bkellerabteil\b").search(_text(c)))
    ) else 0.0

    # --- Binary: washing_machine ---
    m["washing_machine"] = lambda c: 1.0 if (
        _has_feature(c, "washing_machine") or _has_feature(c, "private_laundry")
        or bool(_r(r"\bwaschmaschine\b|\bwashing\s+machine\b|\blave.linge\b|\bwaschturm\b|\bbuanderie\b").search(_text(c)))
    ) else 0.0

    # --- Binary: modern_kitchen ---
    m["modern_kitchen"] = lambda c: 1.0 if bool(_r(
        r"\bmoderne?\s+küche\b|\bmodern\s+kitchen\b|\beinbauküche\b|\bcuisine\s+équipée\b|\bkücheninsel\b"
    ).search(_text(c))) else 0.0

    # --- Binary: modern_bathroom ---
    m["modern_bathroom"] = lambda c: 1.0 if bool(_r(
        r"\bmoderne?\s+bad\b|\bmodern\s+bathroom\b|\bregendousche\b|\brain\s+shower\b"
    ).search(_text(c))) else 0.0

    # --- Binary: minergie ---
    m["minergie"] = lambda c: 1.0 if (
        _has_feature(c, "minergie")
        or bool(_r(r"\bminergie\b|\benergy.efficient\b|\bniedrigenergie\b").search(_text(c)))
    ) else 0.0

    # --- Binary: new_build ---
    m["new_build"] = lambda c: 1.0 if (
        (_get_float(c, "year_built") or 0) >= 2022
        or _has_feature(c, "new_build")
        or bool(_r(r"\bneubau\b|\bnew\s+build\b|\bnewly\s+built\b|\berstvermietung\b|\berst(e|-)bezug\b").search(_text(c)))
    ) else 0.0

    # --- Binary: pets_allowed ---
    m["pets_allowed"] = lambda c: 1.0 if (
        _has_feature(c, "pets_allowed")
        or bool(_r(r"\bhaustier\b|\bpets?\s+allowed\b|\bhund\b|\bkatze\b|\bdog\b|\bcat\b").search(_text(c)))
    ) else 0.0

    # --- Binary: student ---
    m["student"] = lambda c: 1.0 if bool(_r(
        r"\bstudent\b|\bstudenten\b|\bétudiants?\b|\bwg\b|\bwohngemeinschaft\b|\bshared\s+flat\b"
    ).search(_text(c))) else 0.0

    # --- Binary: near_eth, near_epfl ---
    m["near_eth"] = lambda c: 1.0 if bool(_r(r"\beth\b").search(_text(c))) else 0.0
    m["near_epfl"] = lambda c: 1.0 if bool(_r(r"\bepfl\b").search(_text(c))) else 0.0

    # --- Binned: near_hb (nearest_hb_distance_m + text) ---
    m["near_hb"] = lambda c: max(
        _bin3(_get_float(c, "nearest_hb_distance_m"), 5000, 2000, 800, lower_is_better=True),
        _TEXT_FALLBACK if _r(
            r"\bhb\b|\bhauptbahnhof\b|\bmain\s+station\b|\bgare\s+centrale\b|\bam\s+bahnhof\b|\bbeim\s+bahnhof\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binary: specific_move_in ---
    m["specific_move_in"] = lambda c: 1.0 if _check_available_soon(c) else 0.0

    # -----------------------------------------------------------------------
    # NEW v3.1 signals
    # -----------------------------------------------------------------------

    # --- Binned: near_train (nearest_train_distance_m) ---
    m["near_train"] = lambda c: max(
        _bin3(_get_float(c, "nearest_train_distance_m"), 2000, 800, 300, lower_is_better=True),
        _TEXT_FALLBACK if _r(
            r"\btrain\b|\bzug\b|\bs-bahn\b|\brain\s+station\b|\bbahnhof\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: near_hauptbahnhof (nearest_hb_distance_m) ---
    m["near_hauptbahnhof"] = lambda c: max(
        _bin3(_get_float(c, "nearest_hb_distance_m"), 5000, 2000, 800, lower_is_better=True),
        _TEXT_FALLBACK if _r(
            r"\bhauptbahnhof\b|\bhb\b|\bmain\s+station\b|\bgare\s+centrale\b"
        ).search(_text(c)) else 0.0,
    )

    # --- Binned: well_connected (stop + train composite) ---
    m["well_connected"] = lambda c: _well_connected_score(c)

    # --- Binned: good_value_local (price_per_m2_vs_municipality) ---
    m["good_value_local"] = lambda c: _bin3(
        _get_float(c, "price_per_m2_vs_municipality"),
        1.0, 0.85, 0.70, lower_is_better=True,
    )

    # --- Binned: low_density (population_density, lower = stronger) ---
    m["low_density"] = lambda c: _bin3(
        _get_float(c, "population_density"),
        1500, 800, 300, lower_is_better=True,
    )

    # --- Binned: high_density (population_density, higher = stronger) ---
    m["high_density"] = lambda c: _bin3(
        _get_float(c, "population_density"),
        1000, 2000, 4000, lower_is_better=False,
    )

    # --- Binned: small_town (population_total, lower = stronger) ---
    m["small_town"] = lambda c: _bin3(
        _get_float(c, "population_total"),
        30000, 10000, 5000, lower_is_better=True,
    )

    # --- Binned: large_municipality (population_total, higher = stronger) ---
    m["large_municipality"] = lambda c: _bin3(
        _get_float(c, "population_total"),
        20000, 50000, 100000, lower_is_better=False,
    )

    # --- Negative binned: overpriced_warning ---
    m["overpriced_warning"] = lambda c: _bin3(
        _get_float(c, "price_per_m2_vs_municipality"),
        1.3, 1.5, 1.8, lower_is_better=False,
    )


_register_signals()


def _well_connected_score(c: dict) -> float:
    """Composite transit connectivity: stop proximity + train proximity."""
    stop = _bin3(_get_float(c, "nearest_stop_distance_m"), 500, 300, 150, lower_is_better=True)
    train = _bin3(_get_float(c, "nearest_train_distance_m"), 1500, 800, 400, lower_is_better=True)
    if stop > 0 and train > 0:
        return min(1.0, 0.5 * stop + 0.5 * train)
    return max(stop, train) * 0.5


def _check_available_soon(c: dict) -> bool:
    avail = c.get("available_from")
    if not avail:
        return False
    try:
        avail_date = date.fromisoformat(str(avail))
        cutoff = date.today() + timedelta(days=90)
        return avail_date <= cutoff
    except (ValueError, TypeError):
        return False


def _area_per_room(c: dict) -> float | None:
    area = _get_float(c, "area")
    rooms = _get_float(c, "rooms")
    if area is None or rooms is None or rooms < 1:
        return None
    return area / rooms


def _area_gte(c: dict, min_sqm: float) -> bool:
    val = c.get("area")
    if val is None:
        return False
    try:
        return float(val) >= min_sqm
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _get_global_scores(candidate: dict[str, Any]) -> dict[str, float]:
    precomputed = candidate.get("global_score")
    if precomputed is not None:
        return {
            "global_score": float(precomputed),
            "score_value": float(candidate.get("score_value") or 0.0),
            "score_amenity": float(candidate.get("score_amenity") or 0.0),
            "score_location": float(candidate.get("score_location") or 0.0),
            "score_building": float(candidate.get("score_building") or 0.0),
            "score_completeness": float(candidate.get("score_completeness") or 0.0),
            "score_freshness": float(candidate.get("score_freshness") or 0.0),
            "score_transit": float(candidate.get("score_transit") or 0.0),
        }
    return compute_global_score(candidate)


def _global_score_bonus(global_scores: dict[str, float]) -> float:
    return global_scores.get("global_score", 0.0) * GLOBAL_SCORE_WEIGHT


def _score_candidate(candidate: dict, soft_facts: dict) -> tuple[float, list[str]]:
    """Score a candidate against extracted soft signals. Returns (score, matched_signals)."""
    signals = soft_facts.get("signals", {})
    if not signals and not soft_facts.get("preferred_min_area_sqm"):
        return 0.0, []

    score = 0.0
    matched: list[str] = []

    for signal_name, weight in signals.items():
        matcher = _SIGNAL_MATCHERS.get(signal_name)
        if matcher:
            strength = matcher(candidate)
            if strength > 0:
                score += weight * strength
                matched.append(signal_name)

    pref_area = soft_facts.get("preferred_min_area_sqm")
    if pref_area and _area_gte(candidate, pref_area):
        score += 0.3
        matched.append("area_pref")

    return score, matched


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def rank_listings(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
    user_profile: dict[str, Any] | None = None,
) -> list[RankedListingResult]:
    breakdowns = _build_rank_breakdowns(candidates, soft_facts, user_profile=user_profile)
    return [
        RankedListingResult(
            listing_id=str(breakdown.candidate["listing_id"]),
            score=round(breakdown.final_score, 4),
            reason=_reason_for_breakdown(breakdown),
            explanation=breakdown.explanation,
            listing=_to_listing_data(
                breakdown.candidate,
                hero_image_url_override=breakdown.best_image_url,
            ),
            global_scores=breakdown.global_scores,
        )
        for breakdown in breakdowns
    ]


def _build_rank_breakdowns(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
    user_profile: dict[str, Any] | None = None,
) -> list[CandidateRankBreakdown]:
    scored_candidates = _score_candidates(candidates, soft_facts)
    query_text = str(soft_facts.get("raw_query") or "").strip()
    listing_ids = [str(candidate["listing_id"]) for _, _, candidate in scored_candidates]
    scores_by_listing = _image_scores_by_listing(
        query_text=query_text,
        listing_ids=listing_ids,
    )
    max_image_score = max((score for score, _ in scores_by_listing.values()), default=0.0)
    image_bonus_cap = _image_bonus_cap(soft_facts)

    ranked_candidates: list[tuple[float, float, float, float, int, CandidateRankBreakdown]] = []
    for index, (soft_score, matched, candidate) in enumerate(scored_candidates):
        listing_id = str(candidate["listing_id"])
        global_scores = _get_global_scores(candidate)
        global_score_bonus = _global_score_bonus(global_scores)
        preference_bonus, preference_reasons = _user_preference_bonus(candidate, user_profile)
        image_score, best_image_url = scores_by_listing.get(listing_id, (0.0, None))
        image_bonus = _image_bonus(
            image_score=image_score,
            max_image_score=max_image_score,
            image_bonus_cap=image_bonus_cap,
        )
        breakdown = CandidateRankBreakdown(
            candidate=candidate,
            matched=matched,
            soft_score=soft_score,
            global_scores=global_scores,
            global_score_bonus=global_score_bonus,
            preference_bonus=preference_bonus,
            preference_reasons=preference_reasons,
            image_score=image_score,
            image_bonus=image_bonus,
            final_score=soft_score + global_score_bonus + preference_bonus + image_bonus,
            best_image_url=best_image_url,
            image_bonus_cap=image_bonus_cap,
            explanation=_build_explanation(
                candidate=candidate,
                global_scores=global_scores,
                matched=matched,
                preference_reasons=preference_reasons,
                image_score=image_score,
            ),
        )
        ranked_candidates.append(
            (
                breakdown.final_score,
                image_score,
                soft_score + preference_bonus,
                global_scores.get("global_score", 0.0),
                index,
                breakdown,
            )
        )

    ranked_candidates.sort(
        key=lambda item: (
            -item[0],
            -item[1],
            -item[2],
            -item[3],
            item[4],
        )
    )

    return [breakdown for _, _, _, _, _, breakdown in ranked_candidates]


def _score_candidates(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[tuple[float, list[str], dict[str, Any]]]:
    signals = soft_facts.get("signals", {})
    has_soft = bool(signals) or bool(soft_facts.get("preferred_min_area_sqm"))

    scored: list[tuple[float, list[str], dict[str, Any]]] = []
    for candidate in candidates:
        if has_soft:
            soft_score, matched = _score_candidate(candidate, soft_facts)
            scored.append((soft_score, matched, candidate))
        else:
            scored.append((0.0, [], candidate))

    if has_soft:
        scored.sort(key=lambda item: -item[0])

    return scored


def _image_scores_by_listing(
    *,
    query_text: str,
    listing_ids: list[str],
) -> dict[str, tuple[float, str | None]]:
    if not query_text or not listing_ids:
        return {}

    try:
        payload = search_image_rag(
            query_text=query_text,
            listing_ids=listing_ids,
            top_k=min(len(listing_ids), 500),
        )
    except Exception:
        return {}

    if not payload:
        return {}

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
    return scores_by_listing


def _fallback_reason(matched: list[str]) -> str:
    return ", ".join(matched) if matched else "hard filters only"


def _has_quality_reason(global_scores: dict[str, float]) -> bool:
    return global_scores.get("global_score", 0.0) >= 0.6


def _build_explanation(
    *,
    candidate: dict[str, Any],
    global_scores: dict[str, float],
    matched: list[str],
    preference_reasons: list[str],
    image_score: float,
) -> str:
    parts: list[str] = []
    if matched:
        parts.append(f"Matches your query on {', '.join(matched)}.")
    if preference_reasons:
        parts.append(f"Also aligned with your past preferences: {', '.join(preference_reasons)}.")
    if image_score > 0:
        parts.append("Listing photos also matched the visual style of your query.")
    parts.append(explain_score(global_scores, candidate))
    return " ".join(part.strip() for part in parts if part).strip()


def _image_rank_reason(
    matched: list[str],
    *,
    image_score: float,
    soft_score: float,
    has_quality_boost: bool,
    preference_bonus: float,
    preference_reasons: list[str],
) -> str:
    soft_reason = ", ".join(matched)
    preference_reason = ", ".join(preference_reasons)
    quality_suffix = " + quality boost" if has_quality_boost else ""

    if image_score > 0 and soft_score > 0 and preference_bonus > 0 and soft_reason and preference_reason:
        return f"soft match{quality_suffix} + user preference + image bonus: {soft_reason}; {preference_reason}"
    if image_score > 0 and soft_score > 0 and soft_reason:
        return f"soft match{quality_suffix} + image bonus: {soft_reason}"
    if preference_bonus > 0 and soft_score > 0 and soft_reason and preference_reason:
        return f"soft match{quality_suffix} + user preference: {soft_reason}; {preference_reason}"
    if image_score > 0 and soft_score > 0:
        return f"soft match{quality_suffix} + image bonus"
    if preference_bonus > 0 and soft_score > 0:
        return f"soft match{quality_suffix} + user preference"
    if image_score > 0 and preference_bonus > 0 and preference_reason:
        return f"image bonus{quality_suffix} + user preference: {preference_reason}"
    if image_score > 0:
        return f"image bonus{quality_suffix}"
    if preference_bonus > 0 and preference_reason:
        return f"user preference boost{quality_suffix}: {preference_reason}"
    if has_quality_boost:
        return "overall quality boost"
    return _fallback_reason(matched)


def _reason_for_breakdown(breakdown: CandidateRankBreakdown) -> str:
    if breakdown.image_bonus > 0 or breakdown.preference_bonus > 0 or _has_quality_reason(breakdown.global_scores):
        return _image_rank_reason(
            breakdown.matched,
            image_score=breakdown.image_score,
            soft_score=breakdown.soft_score,
            has_quality_boost=_has_quality_reason(breakdown.global_scores),
            preference_bonus=breakdown.preference_bonus,
            preference_reasons=breakdown.preference_reasons,
        )
    return _fallback_reason(breakdown.matched)


def _image_bonus(
    *,
    image_score: float,
    max_image_score: float,
    image_bonus_cap: float,
) -> float:
    return image_bonus_cap * _normalize_non_negative_score(image_score, max_image_score)


def _image_bonus_cap(soft_facts: dict[str, Any]) -> float:
    signals = soft_facts.get("signals", {})
    if not isinstance(signals, dict) or not signals:
        return DEFAULT_IMAGE_BONUS_CAP

    visual_weight = 0.0
    non_visual_weight = 0.0
    for signal_name, weight in signals.items():
        score = _coerce_non_negative_float(weight)
        if signal_name in VISUAL_SIGNALS:
            visual_weight += score
        elif signal_name in NON_VISUAL_SIGNALS:
            non_visual_weight += score

    if soft_facts.get("preferred_min_area_sqm"):
        non_visual_weight += 0.3
    if soft_facts.get("max_commute_minutes"):
        non_visual_weight += 0.5
    if soft_facts.get("commute_destination"):
        non_visual_weight += 0.3

    total_weight = visual_weight + non_visual_weight
    if total_weight <= 0:
        return DEFAULT_IMAGE_BONUS_CAP

    visual_ratio = visual_weight / total_weight
    return MIN_IMAGE_BONUS_CAP + (MAX_IMAGE_BONUS_CAP - MIN_IMAGE_BONUS_CAP) * visual_ratio


def _normalize_non_negative_score(score: float, max_score: float) -> float:
    if max_score <= 0:
        return 0.0
    return max(score, 0.0) / max_score


def _coerce_non_negative_float(value: Any) -> float:
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return 0.0


def _user_preference_bonus(
    candidate: dict[str, Any],
    user_profile: dict[str, Any] | None,
) -> tuple[float, list[str]]:
    if not user_profile:
        return 0.0, []

    dismissed_listing_ids = {
        str(listing_id)
        for listing_id in user_profile.get("dismissed_listing_ids", [])
        if listing_id
    }
    listing_id = str(candidate.get("listing_id") or "")
    if not listing_id or listing_id in dismissed_listing_ids:
        return 0.0, []

    bonus = 0.0
    reasons: list[str] = []
    favorite_listing_ids = {
        str(value)
        for value in user_profile.get("favorite_listing_ids", [])
        if value
    }
    clicked_listing_ids = {
        str(value)
        for value in user_profile.get("clicked_listing_ids", [])
        if value
    }
    if listing_id in favorite_listing_ids:
        bonus += USER_PREFERENCE_LISTING_BONUS
        reasons.append("favorited before")
    elif listing_id in clicked_listing_ids:
        bonus += USER_PREFERENCE_LISTING_BONUS * 0.6
        reasons.append("clicked before")

    preferred_cities = {
        str(city).strip().lower()
        for city in user_profile.get("preferred_cities", [])
        if city
    }
    city = str(candidate.get("city") or "").strip().lower()
    if city and city in preferred_cities:
        bonus += USER_PREFERENCE_CITY_BONUS
        reasons.append("preferred city")

    preferred_features = {
        str(feature).strip().lower()
        for feature in user_profile.get("preferred_features", [])
        if feature
    }
    candidate_features = {
        str(feature).strip().lower()
        for feature in candidate.get("features", [])
        if feature
    }
    shared_features = sorted(candidate_features & preferred_features)
    if shared_features:
        feature_bonus = min(
            MAX_USER_PREFERENCE_FEATURE_BONUS,
            USER_PREFERENCE_FEATURE_BONUS * len(shared_features),
        )
        bonus += feature_bonus
        reasons.extend(f"prefers {feature}" for feature in shared_features[:2])

    price_range = user_profile.get("price_range")
    price = candidate.get("price")
    if (
        isinstance(price_range, dict)
        and price is not None
        and price_range.get("min") is not None
        and price_range.get("max") is not None
    ):
        try:
            candidate_price = int(price)
            min_price = int(price_range["min"])
            max_price = int(price_range["max"])
        except (TypeError, ValueError):
            candidate_price = min_price = max_price = 0
        if min_price and max_price:
            if min_price <= candidate_price <= max_price:
                bonus += USER_PREFERENCE_PRICE_BONUS
                reasons.append("preferred price range")
            else:
                lower = int(min_price * 0.9)
                upper = int(max_price * 1.1)
                if lower <= candidate_price <= upper:
                    bonus += USER_PREFERENCE_PRICE_BONUS * 0.5
                    reasons.append("near preferred price range")

    return min(bonus, MAX_USER_PREFERENCE_BONUS), reasons


def _to_listing_data(
    candidate: dict[str, Any],
    hero_image_url_override: str | None = None,
) -> ListingData:
    image_urls = _coerce_image_urls(candidate.get("image_urls"))
    hero_image_url = candidate.get("hero_image_url") or hero_image_url_override
    if hero_image_url_override and not image_urls:
        image_urls = [hero_image_url_override]

    train_name = candidate.get("nearest_train_name")
    train_dist = candidate.get("nearest_train_distance_m")
    nearest_station = None
    if train_name and train_dist is not None:
        nearest_station = f"{int(train_dist)}m to {train_name}"

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
        image_urls=image_urls,
        hero_image_url=hero_image_url,
        original_listing_url=candidate.get("original_url"),
        features=candidate.get("features") or [],
        offer_type=candidate.get("offer_type"),
        object_category=candidate.get("object_category"),
        object_type=candidate.get("object_type"),
        price_per_m2=candidate.get("price_per_m2"),
        value_label=candidate.get("price_per_m2_vs_municipality_label"),
        district_name=candidate.get("district_name"),
        municipality_name=candidate.get("municipality_name"),
        nearest_station=nearest_station,
        population_density_bucket=candidate.get("population_density_bucket"),
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
