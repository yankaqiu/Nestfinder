from __future__ import annotations

import re
from typing import Any

from app.participant.translate import translate_to_english

# Each entry: (key, pattern, score_hint)
# score_hint = weight contribution when the keyword is present
SOFT_SIGNAL_SPECS: list[tuple[str, re.Pattern[str], float]] = [
    # Light / brightness
    ("bright", re.compile(r"\bhell\b|\bbright\b|\blumineux\b|\bluminous\b|\bsonnig\b|\bsunny\b", re.I), 1.0),

    # Quiet
    ("quiet", re.compile(r"\bruhig\b|\bquiet\b|\bcalme\b|\bsilent\b|\bstill\b", re.I), 1.0),

    # Modern
    ("modern", re.compile(r"\bmodern\b|\bneue?\b\s+(?:küche|bad|wohnung)|\brenoviert\b|\bsaniert\b|\bneuwertig\b|\bcontemporary\b", re.I), 0.8),

    # Good views
    ("views", re.compile(r"\baussicht\b|\bview\b|\bvue\b|\bpanorama\b|\bseeblick\b|\bbergblick\b|\bsee\b", re.I), 0.6),

    # Near water / lake
    ("near_lake", re.compile(r"\bseenähe\b|\bnear\s+(?:the\s+)?lake\b|\bnear\s+(?:the\s+)?sea\b|\blac\b|\bsee\b", re.I), 0.6),

    # Public transport proximity
    ("public_transport", re.compile(r"\böv\b|\bpublic\s+transport\b|\btransport\s+public\b|\bbus\b|\btram\b|\bsbahn\b|\bs-bahn\b|\bbahnhof\b|\bstation\b|\bmetro\b|\bverkehrsanbindung\b|\bgute\s+anbindung\b|\bwellconnected\b", re.I), 1.0),

    # Commute time constraint (soft: desired short commute)
    ("short_commute", re.compile(r"\d+\s*(?:minuten|minutes|min)\s*(?:pendelzeit|commute|fahrt|reise|door.to.door|zu\s+fuß|by\s+(?:public\s+transport|öv|foot|bike|tram|bus))|\bkurze\s+pendelzeit\b|\bkurzer\s+arbeitsweg\b", re.I), 1.2),

    # Furnished
    ("furnished", re.compile(r"\bmöbliert\b|\bfurnished\b|\bmeublé\b|\bwith\s+furniture\b", re.I), 0.8),

    # Family-friendly
    ("family_friendly", re.compile(r"\bfamilienfreundlich\b|\bfamily.friendly\b|\bfamille\b|\bkinder\b|\bchild\b|\bchildren\b", re.I), 0.8),

    # Good schools nearby
    ("good_schools", re.compile(r"\bgute\s+schulen\b|\bgood\s+schools?\b|\bschule\b|\bschool\b|\bécole\b|\bgym\b", re.I), 0.7),

    # Green / nature
    ("green_area", re.compile(r"\bgrün\b|\bpark\b|\bnatur\b|\bgreen\b|\bverdure\b|\bgarten\b|\bgarden\b|\bwald\b|\bforest\b", re.I), 0.6),

    # Lively / vibrant neighbourhood
    ("lively", re.compile(r"\bbelebt\b|\blively\b|\bvibrant\b|\banimé\b|\bgastronomie\b|\brestaurant\b|\bcafé\b|\bcafe\b|\bbar\b|\bleben\b|\bquartier\b", re.I), 0.6),

    # Affordable / cheap
    ("affordable", re.compile(r"\bgünstig\b|\baffordable\b|\bcheap\b|\bpreiswert\b|\bnicht\s+zu\s+teuer\b|\bbon\s+marché\b|\bpas\s+cher\b", re.I), 0.5),

    # Spacious
    ("spacious", re.compile(r"\bgeräumig\b|\bspacious\b|\blarge\b|\bbig\b|\bgroß\b|\bgrand\b|\bviel\s+platz\b", re.I), 0.6),

    # Well-maintained / clean building
    ("well_maintained", re.compile(r"\bgepflegt\b|\bwell.maintained\b|\bclean\b|\bsauber\b|\bpropre\b|\brefurbished\b|\bsaniert\b", re.I), 0.5),

    # Outdoor space
    ("outdoor_space", re.compile(r"\bbalkon\b|\bbalcony\b|\bterasse\b|\bterrasse\b|\bgarten\b|\bgarden\b|\baussenbereich\b|\boutdoor\b", re.I), 0.7),

    # Pet-friendly
    ("pets_allowed", re.compile(r"\bhaustier\b|\bpets?\s+allowed\b|\bcat\b|\bdog\b|\bkatze\b|\bhund\b", re.I), 0.5),

    # Balcony / terrace / outdoor space (duplicate of outdoor_space with balcony-specific weight)
    ("balcony", re.compile(r"\bbalkon\b|\bbalcony\b|\bbalcon\b", re.I), 0.8),

    # Parking / garage
    ("parking", re.compile(r"\bparkplatz\b|\bparking\b|\bgarage\b|\bparkierung\b|\beinstellplatz\b", re.I), 0.7),

    # Fireplace
    ("fireplace", re.compile(r"\bkamin\b|\bfireplace\b|\bcheminée\b|\bchemin[eé]e\b", re.I), 0.5),

    # Child-friendly
    ("child_friendly", re.compile(r"\bkinderfreundlich\b|\bchild.friendly\b|\bfamilienfreundlich\b|\bspielplatz\b|\bplayground\b", re.I), 0.7),

    # In-unit laundry
    ("private_laundry", re.compile(r"\bwaschmaschine\s+in\s+der\s+wohnung\b|\bprivate\s+laundry\b|\beigene\s+waschmaschine\b|\bwaschturm\b|\bwasher.dryer\b", re.I), 0.8),

    # Minergie / energy-efficient
    ("minergie", re.compile(r"\bminergie\b|\benergy.efficient\b|\benergiesparen\b|\bniedrigenergie\b", re.I), 0.6),

    # New build
    ("new_build", re.compile(r"\bneubau\b|\bnew\s+build\b|\bnewly\s+built\b|\bneubau\b", re.I), 0.6),

    # Student housing
    ("student", re.compile(r"\bstudent\b|\bstudenten\b|\bétudiants?\b|\bwg\b|\bwohngemeinschaft\b|\bshared\b", re.I), 0.4),

    # Proximity to specific places (ETH, EPFL, HB, etc.)
    ("near_eth", re.compile(r"\beth\s*(?:zürich|zentrum|hönggerberg)?\b|\beth\b", re.I), 1.2),
    ("near_epfl", re.compile(r"\bepfl\b", re.I), 1.2),
    ("near_hb", re.compile(r"\bhb\b|\bhauptbahnhof\b|\bmain\s+station\b|\bgare\s+centrale\b", re.I), 1.0),

    # Move-in flexibility (mentions a specific month = soft preference)
    ("specific_move_in", re.compile(
        r"\b(?:januar|februar|märz|april|mai|juni|juli|august|september|oktober|november|dezember"
        r"|january|february|march|april|may|june|july|august|september|october|november|december)\b",
        re.I,
    ), 0.3),

    # Nice kitchen
    ("modern_kitchen", re.compile(r"\bmoderne?\s+küche\b|\bmodern\s+kitchen\b|\beinbauküche\b|\bcuisine\s+équipée\b|\bküche\s+mit\s+\w+", re.I), 0.7),

    # Nice bathroom
    ("modern_bathroom", re.compile(r"\bmoderne?\s+bad\b|\bmodern\s+bathroom\b|\bbadezimmer\b|\bsalle\s+de\s+bain\b", re.I), 0.5),
]

SOFT_SIGNAL_PATTERNS: dict[str, re.Pattern[str]] = {
    key: pattern
    for key, pattern, _ in SOFT_SIGNAL_SPECS
}

# Extract numeric commute constraint if present (e.g. "20 minutes to ETH")
_COMMUTE_TIME_RE = re.compile(
    r"(\d+)\s*(?:minuten|minutes?|min)\s*(?:pendelzeit|commute|fahrt|reise|door.to.door|zu\s+fuß|by\s+(?:public\s+transport|öv|foot|bike|tram|bus|s.bahn))?",
    re.I,
)
_COMMUTE_DEST_RE = re.compile(
    r"(?:to|nach|zu|zur?|vers)\s+(eth|epfl|hb|hauptbahnhof|zentrum|center|centre|bahnhof\s+\w+|\w+\s+bahnhof)",
    re.I,
)

# Min area (sqm) as soft preference when no hard constraint was set
_AREA_SOFT_RE = re.compile(
    r"(\d+)\s*(?:m[²2]|qm|quadratmeter|sqm)",
    re.I,
)


def extract_soft_facts(query: str) -> dict[str, Any]:
    """Return a dict of soft preference signals derived from the query."""
    query = translate_to_english(query)
    signals: dict[str, float] = {}
    for key, pattern, weight in SOFT_SIGNAL_SPECS:
        if pattern.search(query):
            signals[key] = weight

    result: dict[str, Any] = {
        "raw_query": query,
        "signals": signals,
    }

    # Commute time constraint
    commute_minutes: int | None = None
    m = _COMMUTE_TIME_RE.search(query)
    if m:
        val = int(m.group(1))
        # Sanity check: only treat as commute if ≤120 minutes
        if 1 <= val <= 120:
            commute_minutes = val
    if commute_minutes:
        result["max_commute_minutes"] = commute_minutes

    # Commute destination
    m2 = _COMMUTE_DEST_RE.search(query)
    if m2:
        result["commute_destination"] = m2.group(1).strip()

    # Preferred area size (sqm) as soft signal
    area_matches = _AREA_SOFT_RE.findall(query)
    if area_matches:
        vals = [int(v) for v in area_matches if int(v) >= 20]
        if vals:
            result["preferred_min_area_sqm"] = max(vals)

    return result
