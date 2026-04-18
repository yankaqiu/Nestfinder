from __future__ import annotations

import json
import os
import re

import anthropic

from app.models.schemas import HardFilters
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
_HARD_SYSTEM_PROMPT = """\
You are a Swiss real-estate search assistant.
Extract ONLY the hard constraints (non-negotiable requirements) from the user query.
Return ONLY a valid JSON object — no explanation, no markdown, no code fences.
Omit any field that was not mentioned or cannot be confidently inferred.

JSON schema (all fields optional):
{
  "city": ["Zürich"],
  "canton": "ZH",
  "postal_code": ["8001"],
  "min_rooms": 2.5,
  "max_rooms": 3.5,
  "min_price": null,
  "max_price": 2800,
  "min_area_sqm": 70,
  "offer_type": "RENT",
  "features": ["elevator"],
  "object_category": ["apartment"]
}

Rules:
- "under / max / bis CHF X" → max_price: X  (integer CHF)
- "ab / from CHF X" → min_price: X
- "3-Zimmer" or "3-room" → min_rooms: 3.0, max_rooms: 3.0
- "2.5 to 3.5 Zimmer" → min_rooms: 2.5, max_rooms: 3.5
- "mindestens / at least 3 rooms" → min_rooms: 3.0
- "studio" → min_rooms: 1.0, max_rooms: 1.5
- German comma decimal: "2,5 Zimmer" = 2.5 rooms
- "mindestens X m²" → min_area_sqm: X
- offer_type: "RENT" for rental queries, "SALE" for purchase queries
- canton: single 2-letter code (ZH, BE, GE, VD, BS, BL, AG, TG, SG, LU, etc.)
- City canonical spellings: zurich→Zürich, genf→Genf, bern→Bern, basel→Basel,
  lausanne→Lausanne, luzern→Luzern, zug→Zug, winterthur→Winterthur
- features: include ONLY if user explicitly requires them (e.g. "must have", "brauche", "required")
  Valid hard features: elevator, pets_allowed, wheelchair_accessible
  Balcony, parking, garage, fireplace, garden are SOFT preferences — omit them from features
- Return ONLY the JSON object
"""


def _llm_extract_hard(query: str) -> HardFilters:
    client = _get_client()
    response = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=512,
        system=[
            {
                "type": "text",
                "text": _HARD_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": query}],
    )
    raw = next((b.text.strip() for b in response.content if b.type == "text"), "")
    # Strip accidental markdown fences
    raw = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.M).strip()
    data = json.loads(raw)
    # Drop null values and unknown fields; always return more candidates for ranking
    filtered = {k: v for k, v in data.items() if v is not None and k in HardFilters.model_fields}
    filtered.setdefault("limit", 100)
    return HardFilters(**filtered)


# ---------------------------------------------------------------------------
# Regex fallback (kept for offline / API-failure scenarios)
# ---------------------------------------------------------------------------
_CITY_ALIASES: dict[str, str] = {
    "zürich": "Zürich", "zurich": "Zürich", "zuerich": "Zürich",
    "genf": "Genf", "geneva": "Genf", "genève": "Genf", "geneve": "Genf",
    "bern": "Bern", "berne": "Bern",
    "basel": "Basel", "basle": "Basel",
    "lausanne": "Lausanne",
    "winterthur": "Winterthur",
    "st. gallen": "St. Gallen", "st gallen": "St. Gallen",
    "luzern": "Luzern", "lucerne": "Luzern",
    "zug": "Zug",
    "lugano": "Lugano",
    "biel": "Biel/Bienne", "bienne": "Biel/Bienne",
    "thun": "Thun",
    "köniz": "Köniz",
    "fribourg": "Fribourg",
    "freiburg": "Freiburg",
    "schaffhausen": "Schaffhausen",
    "uster": "Uster",
    "sion": "Sion", "sitten": "Sion",
    "neuchâtel": "Neuchâtel", "neuenburg": "Neuchâtel",
    "aarau": "Aarau",
    "rapperswil": "Rapperswil",
    "dübendorf": "Dübendorf",
    "dietikon": "Dietikon",
    "regensdorf": "Regensdorf",
    "horgen": "Horgen",
    "adliswil": "Adliswil",
    "küsnacht": "Küsnacht",
    "opfikon": "Opfikon",
    "muttenz": "Muttenz",
    "allschwil": "Allschwil",
    "oberwil": "Oberwil",
    "riehen": "Riehen",
    "binningen": "Binningen",
    "arlesheim": "Arlesheim",
    "olten": "Olten",
    "solothurn": "Solothurn",
    "grenchen": "Grenchen",
    "wil": "Wil",
    "kreuzlingen": "Kreuzlingen",
    "frauenfeld": "Frauenfeld",
    "weinfelden": "Weinfelden",
    "arbon": "Arbon",
    "romanshorn": "Romanshorn",
    "rorschach": "Rorschach",
    "gossau": "Gossau",
    "schwyz": "Schwyz",
    "sarnen": "Sarnen",
    "stans": "Stans",
    "altdorf": "Altdorf",
    "herisau": "Herisau",
    "appenzell": "Appenzell",
    "glarus": "Glarus",
    "chur": "Chur",
    "davos": "Davos",
    "arosa": "Arosa",
    "bellinzona": "Bellinzona",
    "locarno": "Locarno",
    "mendrisio": "Mendrisio",
    "chiasso": "Chiasso",
    "ascona": "Ascona",
    "carouge": "Carouge",
    "vernier": "Vernier",
    "meyrin": "Meyrin",
    "lancy": "Lancy",
    "onex": "Onex",
    "nyon": "Nyon",
    "morges": "Morges",
    "renens": "Renens",
    "prilly": "Prilly",
    "pully": "Pully",
    "ecublens": "Ecublens",
    "yverdon": "Yverdon-les-Bains",
    "monthey": "Monthey",
    "martigny": "Martigny",
    "visp": "Visp",
    "brig": "Brig",
    "kilchberg": "Kilchberg",
    "rüschlikon": "Rüschlikon",
    "thalwil": "Thalwil",
    "oerlikon": "Oerlikon",
    "altstetten": "Altstetten",
    "schlieren": "Schlieren",
    "wallisellen": "Wallisellen",
}

_CANTON_ALIASES: dict[str, str] = {
    "zürich": "ZH", "zurich": "ZH", "zh": "ZH",
    "bern": "BE", "berne": "BE", "be": "BE",
    "luzern": "LU", "lucerne": "LU", "lu": "LU",
    "uri": "UR", "ur": "UR",
    "schwyz": "SZ", "sz": "SZ",
    "obwalden": "OW", "ow": "OW",
    "nidwalden": "NW", "nw": "NW",
    "glarus": "GL", "gl": "GL",
    "zug": "ZG", "zg": "ZG",
    "fribourg": "FR", "freiburg": "FR", "fr": "FR",
    "solothurn": "SO", "so": "SO",
    "basel-stadt": "BS", "bs": "BS",
    "basel-landschaft": "BL", "bl": "BL",
    "schaffhausen": "SH", "sh": "SH",
    "appenzell ausserrhoden": "AR", "ar": "AR",
    "appenzell innerrhoden": "AI", "ai": "AI",
    "st. gallen": "SG", "sg": "SG",
    "graubünden": "GR", "grisons": "GR", "gr": "GR",
    "aargau": "AG", "ag": "AG",
    "thurgau": "TG", "tg": "TG",
    "ticino": "TI", "tessin": "TI", "ti": "TI",
    "vaud": "VD", "waadt": "VD", "vd": "VD",
    "valais": "VS", "wallis": "VS", "vs": "VS",
    "neuchâtel": "NE", "neuenburg": "NE", "ne": "NE",
    "genf": "GE", "genève": "GE", "geneva": "GE", "ge": "GE",
    "jura": "JU", "ju": "JU",
}

_FEATURE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\blift\b|\belevator\b|\bascenseur\b|\baufzug\b", re.I), "elevator"),
    (re.compile(r"\bhaustier\b|\bpets?\s+allowed\b|\banimals?\s+allowed\b", re.I), "pets_allowed"),
    (re.compile(r"\brollstuhl\b|\bbarrierefrei\b|\bwheelchair\b", re.I), "wheelchair_accessible"),
]

_RENT_RE = re.compile(r"\b(miete|mieten|vermieten|rent|rental|à\s+louer|louer)\b", re.I)
_SALE_RE = re.compile(r"\b(kauf|kaufen|sale|buy|zu\s+verkaufen|verkauf|acheter)\b", re.I)

_ROOM_KW = r"(?:zimmer|rooms?|pièces?|zi\b)"
_ROOMS_RANGE_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(?:to|bis|or|oder|–|-)\s*(\d+(?:[.,]\d+)?)\s*[-–]?\s*" + _ROOM_KW, re.I
)
_ROOMS_EXACT_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*[-–]?\s*" + _ROOM_KW, re.I)
_ROOMS_MIN_RE = re.compile(
    r"(?:mindestens|minimum|min(?:imum)?\.?|at\s+least|au\s+moins)\s+(\d+(?:[.,]\d+)?)\s*" + _ROOM_KW, re.I
)
_ROOMS_MAX_RE = re.compile(
    r"(?:maximal|maximum|max\.?|at\s+most|bis\s+zu)\s+(\d+(?:[.,]\d+)?)\s*" + _ROOM_KW, re.I
)
_STUDIO_RE = re.compile(r"\bstudio\b|\batelier\b", re.I)

_CHF = r"(?:chf|fr\.?|sfr\.?|franken)"
_MAX_PRICE_RE = re.compile(
    r"(?:under|unter|bis(?:\s+zu)?|max(?:imal)?\.?|maximum|au\s+maximum|pas\s+plus\s+de|moins\s+de)\s*"
    r"(?:" + _CHF + r"\s*)?(\d[\d'. ]*\d|\d{3,})(?:\s*(?:" + _CHF + r"))?",
    re.I,
)
_MIN_PRICE_RE = re.compile(
    r"(?:mindestens|minimum|min(?:imum)?\.?|ab|from|au\s+moins)\s*"
    r"(?:" + _CHF + r"\s*)?(\d[\d'. ]*\d|\d{3,})(?:\s*(?:" + _CHF + r"))?",
    re.I,
)
_PRICE_AMOUNT_RE = re.compile(
    r"(?:" + _CHF + r"\s*)(\d[\d'. ]*\d|\d+)|(\d[\d'. ]*\d|\d+)\s*(?:" + _CHF + r")", re.I
)
_MIN_AREA_RE = re.compile(
    r"(?:mindestens|minimum|min(?:imum)?\.?|at\s+least|au\s+moins|ab)\s*(\d+)\s*(?:m[²2]|qm|quadratmeter|sqm)",
    re.I,
)


def _parse_number(raw: str) -> float:
    return float(raw.replace("'", "").replace(" ", "").replace(",", "."))


def _regex_extract_hard(text: str) -> HardFilters:
    found_cities: list[str] = []
    lower = text.lower()
    for alias in sorted(_CITY_ALIASES, key=len, reverse=True):
        if re.search(r"\b" + re.escape(alias) + r"\b", lower):
            canonical = _CITY_ALIASES[alias]
            if canonical not in found_cities:
                found_cities.append(canonical)

    canton: str | None = None
    for alias in sorted(_CANTON_ALIASES, key=len, reverse=True):
        if re.search(r"\b" + re.escape(alias) + r"\b", lower):
            canton = _CANTON_ALIASES[alias]
            break
    if found_cities and canton:
        city_to_canton = {"zürich": "ZH", "genf": "GE", "bern": "BE", "basel": "BS",
                          "zug": "ZG", "luzern": "LU", "lausanne": "VD", "winterthur": "ZH"}
        if city_to_canton.get(found_cities[0].lower()) == canton:
            canton = None

    min_rooms = max_rooms = None
    if _STUDIO_RE.search(text):
        min_rooms, max_rooms = 1.0, 1.5
    m = _ROOMS_RANGE_RE.search(text)
    if m:
        lo, hi = _parse_number(m.group(1)), _parse_number(m.group(2))
        min_rooms, max_rooms = min(lo, hi), max(lo, hi)
    else:
        if (m := _ROOMS_MIN_RE.search(text)):
            min_rooms = _parse_number(m.group(1))
        if (m := _ROOMS_MAX_RE.search(text)):
            max_rooms = _parse_number(m.group(1))
        if min_rooms is None and max_rooms is None:
            matches = _ROOMS_EXACT_RE.findall(text)
            if matches:
                val = _parse_number(matches[0])
                min_rooms = max_rooms = val

    min_price = max_price = None
    if (m := _MAX_PRICE_RE.search(text)):
        val = int(_parse_number(m.group(1)))
        if val >= 200:
            max_price = val
    if (m := _MIN_PRICE_RE.search(text)):
        val = int(_parse_number(m.group(1)))
        if val >= 200:
            min_price = val
    if min_price is None and max_price is None:
        for m in _PRICE_AMOUNT_RE.finditer(text):
            raw = m.group(1) or m.group(2)
            if raw:
                val = int(_parse_number(raw))
                if val >= 200:
                    max_price = val
                    break

    features: list[str] = []
    for pattern, name in _FEATURE_PATTERNS:
        if pattern.search(text) and name not in features:
            features.append(name)

    offer_type = None
    if _SALE_RE.search(text):
        offer_type = "SALE"
    elif _RENT_RE.search(text):
        offer_type = "RENT"

    return HardFilters(
        city=found_cities or None,
        canton=canton,
        min_price=min_price,
        max_price=max_price,
        min_rooms=min_rooms,
        max_rooms=max_rooms,
        features=features or None,
        offer_type=offer_type,
        limit=100,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def extract_hard_facts(query: str) -> HardFilters:
    translated = translate_to_english(query)
    try:
        return _llm_extract_hard(translated)
    except Exception:
        return _regex_extract_hard(translated)
