from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Any

from app.models.schemas import ListingData, RankedListingResult

# ---------------------------------------------------------------------------
# Signal matchers: each maps a soft signal name to a function that checks
# whether a candidate listing matches that signal.
# Returns True/False.
#
# Strategy: prefer enriched DB columns (data-driven), fall back to
# text regex when enrichment data is missing.
# ---------------------------------------------------------------------------

def _text(c: dict) -> str:
    """Combined searchable text for a candidate."""
    return ((c.get("title") or "") + " " + (c.get("description") or "")).lower()


def _has_feature(c: dict, feat: str) -> bool:
    feats = c.get("features") or []
    return any(feat.lower() in str(f).lower() for f in feats)


def _dist_lt(c: dict, field: str, threshold: float) -> bool | None:
    """Returns True if distance < threshold, False if >=, None if no data."""
    val = c.get(field)
    if val is None:
        return None
    try:
        return float(val) < threshold
    except (TypeError, ValueError):
        return None


def _enriched_lt(c: dict, field: str, threshold: float) -> bool | None:
    """Check enriched numeric column < threshold. None if missing."""
    val = c.get(field)
    if val is None:
        return None
    try:
        return float(val) < threshold
    except (TypeError, ValueError):
        return None


def _enriched_gt(c: dict, field: str, threshold: float) -> bool | None:
    val = c.get(field)
    if val is None:
        return None
    try:
        return float(val) > threshold
    except (TypeError, ValueError):
        return None


def _enriched_gte(c: dict, field: str, threshold: float) -> bool | None:
    val = c.get(field)
    if val is None:
        return None
    try:
        return float(val) >= threshold
    except (TypeError, ValueError):
        return None


_SIGNAL_MATCHERS: dict[str, Any] = {}


def _r(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.I)


def _register_signals():
    """Build the signal matcher table with enriched-data-first matching."""
    m = _SIGNAL_MATCHERS

    # --- Enriched + regex: bright ---
    # Higher floors get more light; text fallback for all
    m["bright"] = lambda c: (
        (_enriched_gte(c, "floor_level", 3) is True)
        or bool(_r(
            r"\bhell[e]?\b|\bbright\b|\blumineu[xs]\b|\bsonnig\b|\bsunny\b|\blichtdurchflutet\b|\blicht\b"
        ).search(_text(c)))
    )

    m["quiet"] = lambda c: (
        bool(_r(
            r"\bruhig\b|\bquiet\b|\bcalme\b|\bstill[e]?\s+lage\b|\bruhige\s+lage\b"
        ).search(_text(c)))
        or _dist_lt(c, "distance_shop", 500) is True
        or (c.get("is_urban") == 0)
    )

    # --- Enriched: modern (year_built > 2010 or renovation < 10 yrs) ---
    m["modern"] = lambda c: (
        (_enriched_gt(c, "year_built", 2010) is True)
        or (_enriched_gt(c, "renovation_year", 2015) is True)
        or bool(_r(
            r"\bmodern[e]?\b|\brenoviert\b|\bsaniert\b|\bneuwertig\b|\bcontemporary\b|\bstylish\b|\bdesign\b"
        ).search(_text(c)))
    )

    m["views"] = lambda c: (
        (_enriched_gte(c, "floor_level", 5) is True)
        or bool(_r(
            r"\baussicht\b|\bview\b|\bvue\b|\bpanorama\b|\bseeblick\b|\bbergblick\b|\bweitsicht\b|\bfernblick\b"
        ).search(_text(c)))
    )

    # --- Enriched: near_lake (lake_distance_m) + text fallback ---
    m["near_lake"] = lambda c: (
        (_enriched_lt(c, "lake_distance_m", 2000) is True)
        or bool(_r(
            r"\bseenähe\b|\bnear.*lake\b|\blac\b|\bseeblick\b|\bam\s+see\b|\blake\s+view\b|\bzürichsee\b|\bgenfersee\b|\bvierwaldstättersee\b"
        ).search(_text(c)))
    )

    m["public_transport"] = lambda c: (
        _dist_lt(c, "distance_public_transport", 500) is True
        or bool(_r(
            r"\böv\b|\bpublic\s+transport\b|\btram\b|\bs-bahn\b|\bbus\s+station\b|\bbahnhof\b|\bmetro\b|\bgute\s+anbindung\b|\banbindung\b|\bverkehrsanbindung\b"
        ).search(_text(c)))
    )

    m["short_commute"] = m["public_transport"]

    # --- Enriched: furnished (is_furnished column) ---
    m["furnished"] = lambda c: (
        (c.get("is_furnished") == 1)
        or _has_feature(c, "furnished")
        or bool(_r(
            r"\bmöbliert\b|\bfurnished\b|\bmeublé\b|\bwith\s+furniture\b"
        ).search(_text(c)))
    )

    m["family_friendly"] = lambda c: (
        _has_feature(c, "child_friendly")
        or _dist_lt(c, "distance_kindergarten", 1000) is True
        or _dist_lt(c, "distance_school_1", 1500) is True
        or bool(_r(r"\bfamilie\b|\bfamily\b|\bfamilienfreundlich\b|\bfamily.friendly\b|\bkinder\b").search(_text(c)))
    )

    m["child_friendly"] = lambda c: (
        _has_feature(c, "child_friendly")
        or _dist_lt(c, "distance_kindergarten", 800) is True
        or bool(_r(r"\bkinderfreundlich\b|\bchild.friendly\b|\bspielplatz\b|\bplayground\b|\bkinder\b").search(_text(c)))
    )

    m["good_schools"] = lambda c: (
        _dist_lt(c, "distance_school_1", 1000) is True
        or _dist_lt(c, "distance_school_2", 1500) is True
        or bool(_r(r"\bschule\b|\bschool\b|\bécole\b|\bgute\s+schulen\b").search(_text(c)))
    )

    m["green_area"] = lambda c: (
        bool(_r(
            r"\bpark\b|\bgarten\b|\bgarden\b|\bgrün[e]?\b|\bwald\b|\bnatur\b|\bforest\b|\bverdure\b"
        ).search(_text(c)))
    )

    m["lively"] = lambda c: (
        (c.get("is_urban") == 1)
        or bool(_r(
            r"\bbelebt\b|\blively\b|\bvibrant\b|\banimé\b|\bgastronomie\b|\brestaurant\b|\bcafé\b|\bcafe\b|\bausgang\b|\bnachtleben\b|\bbar[s]?\b"
        ).search(_text(c)))
    )

    # --- Enriched: affordable (price_vs_city_median) ---
    m["affordable"] = lambda c: (
        (_enriched_lt(c, "price_vs_city_median", 0.85) is True)
        or bool(_r(
            r"\bgünstig\b|\baffordable\b|\bpreiswert\b|\bcheap\b|\bbon\s+marché\b"
        ).search(_text(c)))
    )

    m["spacious"] = lambda c: _area_gte(c, 80) or bool(_r(
        r"\bgeräumig\b|\bspacious\b|\bgrosszügig\b|\bviel\s+platz\b"
    ).search(_text(c)))

    # --- Enriched: well_maintained (renovation_year recent) ---
    m["well_maintained"] = lambda c: (
        (_enriched_gt(c, "renovation_year", 2018) is True)
        or bool(_r(
            r"\bgepflegt\b|\bwell.maintained\b|\bsauber\b|\bpropre\b|\brefurbished\b|\btop\s+zustand\b"
        ).search(_text(c)))
    )

    m["outdoor_space"] = lambda c: (
        _has_feature(c, "balcony")
        or bool(_r(r"\bbalkon\b|\bbalcony\b|\bterr?asse\b|\bgarten\b|\bgarden\b|\bsitzplatz\b|\bloggia\b").search(_text(c)))
    )

    m["balcony"] = lambda c: (
        _has_feature(c, "balcony")
        or bool(_r(r"\bbalkon\b|\bbalcony\b|\bbalcon\b").search(_text(c)))
    )

    m["parking"] = lambda c: (
        _has_feature(c, "parking") or _has_feature(c, "garage")
        or bool(_r(r"\bparkplatz\b|\bparking\b|\bgarage\b|\beinstellplatz\b|\btiefgarage\b").search(_text(c)))
    )

    m["fireplace"] = lambda c: (
        _has_feature(c, "fireplace")
        or bool(_r(r"\bkamin\b|\bfireplace\b|\bcheminée\b").search(_text(c)))
    )

    m["private_laundry"] = lambda c: (
        _has_feature(c, "private_laundry") or _has_feature(c, "washing_machine")
        or bool(_r(r"\bwaschmaschine\b|\bwasher\b|\bwaschturm\b|\bprivate\s+laundry\b|\beigene\s+waschmaschine\b").search(_text(c)))
    )

    m["elevator"] = lambda c: (
        _has_feature(c, "elevator")
        or bool(_r(r"\blift\b|\belevator\b|\baufzug\b|\bascenseur\b").search(_text(c)))
    )

    m["garden"] = lambda c: (
        _has_feature(c, "garden")
        or bool(_r(r"\bgarten\b|\bgarden\b|\bjardin\b|\bgiardino\b").search(_text(c)))
    )

    m["dishwasher"] = lambda c: (
        _has_feature(c, "dishwasher")
        or bool(_r(r"\bgeschirrspüler\b|\bdishwasher\b|\blave.vaisselle\b|\babwaschmaschine\b|\bspülmaschine\b").search(_text(c)))
    )

    m["cellar"] = lambda c: (
        _has_feature(c, "cellar")
        or bool(_r(r"\bkeller\b|\bcellar\b|\bcave\b|\bcantina\b|\bkellerabteil\b").search(_text(c)))
    )

    m["washing_machine"] = lambda c: (
        _has_feature(c, "washing_machine") or _has_feature(c, "private_laundry")
        or bool(_r(r"\bwaschmaschine\b|\bwashing\s+machine\b|\blave.linge\b|\bwaschturm\b|\bbuanderie\b").search(_text(c)))
    )

    m["modern_kitchen"] = lambda c: bool(_r(
        r"\bmoderne?\s+küche\b|\bmodern\s+kitchen\b|\beinbauküche\b|\bcuisine\s+équipée\b|\bkücheninsel\b"
    ).search(_text(c)))

    m["modern_bathroom"] = lambda c: bool(_r(
        r"\bmoderne?\s+bad\b|\bmodern\s+bathroom\b|\bregendousche\b|\brain\s+shower\b"
    ).search(_text(c)))

    m["minergie"] = lambda c: (
        _has_feature(c, "minergie")
        or bool(_r(r"\bminergie\b|\benergy.efficient\b|\bniedrigenergie\b").search(_text(c)))
    )

    # --- Enriched: new_build (year_built >= 2022) ---
    m["new_build"] = lambda c: (
        (_enriched_gte(c, "year_built", 2022) is True)
        or _has_feature(c, "new_build")
        or bool(_r(r"\bneubau\b|\bnew\s+build\b|\bnewly\s+built\b|\berstvermietung\b|\berst(e|-)bezug\b").search(_text(c)))
    )

    m["pets_allowed"] = lambda c: (
        _has_feature(c, "pets_allowed")
        or bool(_r(r"\bhaustier\b|\bpets?\s+allowed\b|\bhund\b|\bkatze\b|\bdog\b|\bcat\b").search(_text(c)))
    )

    m["student"] = lambda c: bool(_r(
        r"\bstudent\b|\bstudenten\b|\bétudiants?\b|\bwg\b|\bwohngemeinschaft\b|\bshared\s+flat\b"
    ).search(_text(c)))

    m["near_eth"] = lambda c: bool(_r(r"\beth\b").search(_text(c)))
    m["near_epfl"] = lambda c: bool(_r(r"\bepfl\b").search(_text(c)))
    m["near_hb"] = lambda c: bool(_r(
        r"\bhb\b|\bhauptbahnhof\b|\bmain\s+station\b|\bgare\s+centrale\b|\bam\s+bahnhof\b|\bbeim\s+bahnhof\b"
    ).search(_text(c)))

    # --- Enriched: specific_move_in (available_from date matching) ---
    m["specific_move_in"] = lambda c: _check_available_soon(c)


_register_signals()


def _check_available_soon(c: dict) -> bool:
    """True if the listing is available within 3 months."""
    avail = c.get("available_from")
    if not avail:
        return False
    try:
        avail_date = date.fromisoformat(str(avail))
        cutoff = date.today() + timedelta(days=90)
        return avail_date <= cutoff
    except (ValueError, TypeError):
        return False


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

def _score_candidate(candidate: dict, soft_facts: dict) -> tuple[float, list[str]]:
    """Score a candidate against extracted soft signals. Returns (score, matched_signals)."""
    signals = soft_facts.get("signals", {})
    if not signals and not soft_facts.get("preferred_min_area_sqm"):
        return 0.0, []

    score = 0.0
    matched: list[str] = []

    for signal_name, weight in signals.items():
        matcher = _SIGNAL_MATCHERS.get(signal_name)
        if matcher and matcher(candidate):
            score += weight
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
) -> list[RankedListingResult]:
    signals = soft_facts.get("signals", {})
    has_soft = bool(signals) or bool(soft_facts.get("preferred_min_area_sqm"))

    scored: list[tuple[float, list[str], dict[str, Any]]] = []
    for candidate in candidates:
        if has_soft:
            s, m = _score_candidate(candidate, soft_facts)
            scored.append((s, m, candidate))
        else:
            scored.append((0.0, [], candidate))

    if has_soft:
        scored.sort(key=lambda x: -x[0])

    return [
        RankedListingResult(
            listing_id=str(cand["listing_id"]),
            score=round(sc, 2),
            reason=", ".join(matched) if matched else "hard filters only",
            listing=_to_listing_data(cand),
        )
        for sc, matched, cand in scored
    ]


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
