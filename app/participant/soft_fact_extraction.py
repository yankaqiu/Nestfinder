from __future__ import annotations

import json
import os
import re
from typing import Any

import anthropic

from app.participant.translate import translate_to_english

# ---------------------------------------------------------------------------
# Shared Anthropic client (lazy)
# ---------------------------------------------------------------------------
_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    return _client


# ---------------------------------------------------------------------------
# LLM extraction
# ---------------------------------------------------------------------------
_SOFT_SYSTEM_PROMPT = """\
You are a Swiss real-estate search assistant.
Extract soft preferences (nice-to-haves for ranking) from the user query.
Return ONLY a valid JSON object — no explanation, no markdown, no code fences.

JSON schema:
{
  "signals": {
    "bright": 1.0,
    "quiet": 1.0,
    "modern": 0.8,
    "views": 0.6,
    "near_lake": 0.6,
    "public_transport": 1.0,
    "short_commute": 1.2,
    "furnished": 0.8,
    "family_friendly": 0.8,
    "child_friendly": 0.7,
    "good_schools": 0.7,
    "green_area": 0.6,
    "lively": 0.6,
    "affordable": 0.5,
    "spacious": 0.6,
    "well_maintained": 0.5,
    "outdoor_space": 0.7,
    "balcony": 0.8,
    "parking": 0.7,
    "fireplace": 0.5,
    "private_laundry": 0.8,
    "modern_kitchen": 0.7,
    "modern_bathroom": 0.5,
    "minergie": 0.6,
    "new_build": 0.6,
    "pets_allowed": 0.5,
    "student": 0.4,
    "near_eth": 1.2,
    "near_epfl": 1.2,
    "near_hb": 1.0,
    "specific_move_in": 0.3
  },
  "max_commute_minutes": 30,
  "commute_destination": "ETH",
  "preferred_min_area_sqm": 70
}

Rules:
- Include ONLY signals that are actually present in the query
- Use the default weight values from the schema above for each signal
- max_commute_minutes: extract numeric commute limit (1-120 min only)
- commute_destination: the destination of the commute if named (ETH, EPFL, HB, Hauptbahnhof, etc.)
- preferred_min_area_sqm: minimum area preference in sqm (only if stated)
- Signals dictionary may be empty {} if no soft preferences found
- Return ONLY the JSON object
"""


def _llm_extract_soft(query: str) -> dict[str, Any]:
    client = _get_client()
    response = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=512,
        system=[
            {
                "type": "text",
                "text": _SOFT_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": query}],
    )
    raw = next((b.text.strip() for b in response.content if b.type == "text"), "")
    raw = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.M).strip()
    data = json.loads(raw)
    data["raw_query"] = query
    return data


# ---------------------------------------------------------------------------
# Regex fallback
# ---------------------------------------------------------------------------
_SOFT_SIGNALS: list[tuple[str, re.Pattern[str], float]] = [
    ("bright", re.compile(r"\bhell\b|\bbright\b|\blumineux\b|\bluminous\b|\bsonnig\b|\bsunny\b", re.I), 1.0),
    ("quiet", re.compile(r"\bruhig\b|\bquiet\b|\bcalme\b|\bsilent\b|\bstill\b", re.I), 1.0),
    ("modern", re.compile(r"\bmodern\b|\brenoviert\b|\bsaniert\b|\bneuwertig\b|\bcontemporary\b", re.I), 0.8),
    ("views", re.compile(r"\baussicht\b|\bview\b|\bvue\b|\bpanorama\b|\bseeblick\b|\bbergblick\b", re.I), 0.6),
    ("near_lake", re.compile(r"\bseenähe\b|\bnear\s+(?:the\s+)?lake\b|\bnear\s+(?:the\s+)?sea\b|\blac\b|\bsee\b", re.I), 0.6),
    ("public_transport", re.compile(r"\böv\b|\bpublic\s+transport\b|\btransport\s+public\b|\bbus\b|\btram\b|\bs-bahn\b|\bbahnhof\b|\bstation\b|\bmetro\b|\bgute\s+anbindung\b", re.I), 1.0),
    ("short_commute", re.compile(r"\d+\s*(?:minuten|minutes|min)\s*(?:pendelzeit|commute|fahrt|zu\s+fuß|by\s+(?:public\s+transport|öv|foot|bike|tram|bus))|\bkurze\s+pendelzeit\b", re.I), 1.2),
    ("furnished", re.compile(r"\bmöbliert\b|\bfurnished\b|\bmeublé\b|\bwith\s+furniture\b", re.I), 0.8),
    ("family_friendly", re.compile(r"\bfamilienfreundlich\b|\bfamily.friendly\b|\bfamille\b", re.I), 0.8),
    ("child_friendly", re.compile(r"\bkinderfreundlich\b|\bchild.friendly\b|\bkinder\b|\bchild\b|\bchildren\b|\bspielplatz\b|\bplayground\b", re.I), 0.7),
    ("good_schools", re.compile(r"\bgute\s+schulen\b|\bgood\s+schools?\b|\bschule\b|\bschool\b|\bécole\b", re.I), 0.7),
    ("green_area", re.compile(r"\bgrün\b|\bpark\b|\bnatur\b|\bgreen\b|\bverdure\b|\bgarten\b|\bgarden\b|\bwald\b|\bforest\b", re.I), 0.6),
    ("lively", re.compile(r"\bbelebt\b|\blively\b|\bvibrant\b|\banimé\b|\bgastronomie\b|\brestaurant\b|\bcafé\b|\bcafe\b|\bbar\b", re.I), 0.6),
    ("affordable", re.compile(r"\bgünstig\b|\baffordable\b|\bcheap\b|\bpreiswert\b|\bnicht\s+zu\s+teuer\b|\bbon\s+marché\b", re.I), 0.5),
    ("spacious", re.compile(r"\bgeräumig\b|\bspacious\b|\blarge\b|\bbig\b|\bgroß\b|\bgrand\b|\bviel\s+platz\b", re.I), 0.6),
    ("well_maintained", re.compile(r"\bgepflegt\b|\bwell.maintained\b|\bclean\b|\bsauber\b|\bpropre\b|\brefurbished\b", re.I), 0.5),
    ("outdoor_space", re.compile(r"\bbalkon\b|\bbalcony\b|\bterasse\b|\bterrasse\b|\baussenbereich\b|\boutdoor\b", re.I), 0.7),
    ("balcony", re.compile(r"\bbalkon\b|\bbalcony\b|\bbalcon\b", re.I), 0.8),
    ("parking", re.compile(r"\bparkplatz\b|\bparking\b|\bgarage\b|\bparkierung\b|\beinstellplatz\b", re.I), 0.7),
    ("fireplace", re.compile(r"\bkamin\b|\bfireplace\b|\bcheminée\b", re.I), 0.5),
    ("private_laundry", re.compile(r"\bwaschmaschine\s+in\s+der\s+wohnung\b|\bprivate\s+laundry\b|\beigene\s+waschmaschine\b|\bwaschturm\b|\bwasher.dryer\b", re.I), 0.8),
    ("modern_kitchen", re.compile(r"\bmoderne?\s+küche\b|\bmodern\s+kitchen\b|\beinbauküche\b|\bcuisine\s+équipée\b", re.I), 0.7),
    ("modern_bathroom", re.compile(r"\bmoderne?\s+bad\b|\bmodern\s+bathroom\b|\bbadezimmer\b|\bsalle\s+de\s+bain\b", re.I), 0.5),
    ("minergie", re.compile(r"\bminergie\b|\benergy.efficient\b|\bniedrigenergie\b", re.I), 0.6),
    ("new_build", re.compile(r"\bneubau\b|\bnew\s+build\b|\bnewly\s+built\b", re.I), 0.6),
    ("pets_allowed", re.compile(r"\bhaustier\b|\bpets?\s+allowed\b|\bcat\b|\bdog\b|\bkatze\b|\bhund\b", re.I), 0.5),
    ("student", re.compile(r"\bstudent\b|\bstudenten\b|\bétudiants?\b|\bwg\b|\bwohngemeinschaft\b|\bshared\b", re.I), 0.4),
    ("near_eth", re.compile(r"\beth\s*(?:zürich|zentrum|hönggerberg)?\b|\beth\b", re.I), 1.2),
    ("near_epfl", re.compile(r"\bepfl\b", re.I), 1.2),
    ("near_hb", re.compile(r"\bhb\b|\bhauptbahnhof\b|\bmain\s+station\b|\bgare\s+centrale\b", re.I), 1.0),
    ("specific_move_in", re.compile(
        r"\b(?:januar|februar|märz|april|mai|juni|juli|august|september|oktober|november|dezember"
        r"|january|february|march|april|may|june|july|august|september|october|november|december)\b",
        re.I,
    ), 0.3),
]

_COMMUTE_TIME_RE = re.compile(
    r"(\d+)\s*(?:minuten|minutes?|min)\s*(?:pendelzeit|commute|fahrt|zu\s+fuß|by\s+(?:public\s+transport|öv|foot|bike|tram|bus|s.bahn))?",
    re.I,
)
_COMMUTE_DEST_RE = re.compile(
    r"(?:to|nach|zu|zur?|vers)\s+(eth|epfl|hb|hauptbahnhof|zentrum|center|centre|bahnhof\s+\w+|\w+\s+bahnhof)",
    re.I,
)
_AREA_SOFT_RE = re.compile(r"(\d+)\s*(?:m[²2]|qm|quadratmeter|sqm)", re.I)


def _regex_extract_soft(query: str) -> dict[str, Any]:
    signals: dict[str, float] = {}
    for key, pattern, weight in _SOFT_SIGNALS:
        if pattern.search(query):
            signals[key] = weight

    result: dict[str, Any] = {"raw_query": query, "signals": signals}

    m = _COMMUTE_TIME_RE.search(query)
    if m:
        val = int(m.group(1))
        if 1 <= val <= 120:
            result["max_commute_minutes"] = val

    m2 = _COMMUTE_DEST_RE.search(query)
    if m2:
        result["commute_destination"] = m2.group(1).strip()

    area_matches = _AREA_SOFT_RE.findall(query)
    if area_matches:
        vals = [int(v) for v in area_matches if int(v) >= 20]
        if vals:
            result["preferred_min_area_sqm"] = max(vals)

    return result


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def extract_soft_facts(query: str) -> dict[str, Any]:
    translated = translate_to_english(query)
    try:
        return _llm_extract_soft(translated)
    except Exception:
        return _regex_extract_soft(translated)
