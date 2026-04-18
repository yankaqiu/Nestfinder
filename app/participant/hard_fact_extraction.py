from __future__ import annotations

import re

from app.models.schemas import HardFilters
from app.participant.translate import translate_to_english

# Swiss cities mapped to canonical spellings used in the DB
_CITY_ALIASES: dict[str, str] = {
    "zürich": "Zürich",
    "zurich": "Zürich",
    "zuerich": "Zürich",
    "genf": "Genf",
    "geneva": "Genf",
    "genève": "Genf",
    "geneve": "Genf",
    "bern": "Bern",
    "berne": "Bern",
    "basel": "Basel",
    "basle": "Basel",
    "lausanne": "Lausanne",
    "winterthur": "Winterthur",
    "st. gallen": "St. Gallen",
    "st gallen": "St. Gallen",
    "luzern": "Luzern",
    "lucerne": "Luzern",
    "zug": "Zug",
    "lugano": "Lugano",
    "biel": "Biel/Bienne",
    "bienne": "Biel/Bienne",
    "thun": "Thun",
    "köniz": "Köniz",
    "fribourg": "Fribourg",
    "freiburg": "Freiburg",
    "schaffhausen": "Schaffhausen",
    "uster": "Uster",
    "sion": "Sion",
    "sitten": "Sion",
    "neuchâtel": "Neuchâtel",
    "neuenburg": "Neuchâtel",
    "aarau": "Aarau",
    "rapperswil": "Rapperswil",
    "frenkendorf": "Frenkendorf",
    "reinach": "Reinach",
    "kloten": "Kloten",
    "dübendorf": "Dübendorf",
    "dietikon": "Dietikon",
    "regensdorf": "Regensdorf",
    "horgen": "Horgen",
    "adliswil": "Adliswil",
    "küsnacht": "Küsnacht",
    "küssnacht": "Küssnacht",
    "embrach": "Embrach",
    "opfikon": "Opfikon",
    "illnau": "Illnau-Effretikon",
    "volketswil": "Volketswil",
    "bassersdorf": "Bassersdorf",
    "muttenz": "Muttenz",
    "allschwil": "Allschwil",
    "oberwil": "Oberwil",
    "riehen": "Riehen",
    "binningen": "Binningen",
    "arlesheim": "Arlesheim",
    "bettlach": "Bettlach",
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
    "arth": "Arth",
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
    "flims": "Flims",
    "thusis": "Thusis",
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
    "plan-les-ouates": "Plan-les-Ouates",
    "thônex": "Thônex",
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

# Hard features: only truly binary must-have constraints (not soft preferences)
_FEATURE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\blift\b|\belevator\b|\bascenseur\b|\baufzug\b", re.I), "elevator"),
    (re.compile(r"\bhaustier\b|\bpets?\s+allowed\b|\banimals?\s+allowed\b", re.I), "pets_allowed"),
    (re.compile(r"\brollstuhl\b|\bbarrierefrei\b|\bwheelchair\b", re.I), "wheelchair_accessible"),
]

# Offer type patterns
_RENT_RE = re.compile(r"\b(miete|mieten|vermieten|rent|rental|à\s+louer|louer)\b", re.I)
_SALE_RE = re.compile(r"\b(kauf|kaufen|sale|buy|zu\s+verkaufen|verkauf|acheter)\b", re.I)

# Room number patterns — room keyword is required to avoid matching price/time numbers
# e.g. "2.5-Zimmer", "3-room", "mindestens 2 Zimmer", "at least 3.5 rooms"
_ROOM_KW = r"(?:zimmer|rooms?|pièces?|zi\b)"
# Range: "2.5 to 3-room", "1 to 2 rooms", "3.5 bis 4.5 Zimmer", "3.5 or 4 rooms"
_ROOMS_RANGE_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(?:to|bis|or|oder|–|-)\s*(\d+(?:[.,]\d+)?)\s*[-–]?\s*" + _ROOM_KW,
    re.I,
)
_ROOMS_EXACT_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*[-–]?\s*" + _ROOM_KW,
    re.I,
)
_ROOMS_MIN_RE = re.compile(
    r"(?:mindestens|minimum|min(?:imum)?\.?|at\s+least|au\s+moins)\s+(\d+(?:[.,]\d+)?)\s*" + _ROOM_KW,
    re.I,
)
_ROOMS_MAX_RE = re.compile(
    r"(?:maximal|maximum|max\.?|at\s+most|bis\s+zu)\s+(\d+(?:[.,]\d+)?)\s*" + _ROOM_KW,
    re.I,
)
# studio / 1.5-Zimmer shorthand
_STUDIO_RE = re.compile(r"\bstudio\b|\batelier\b", re.I)

# Price patterns — always require CHF/fr context on at least one side to avoid
# matching commute times, areas, or room counts.
_CHF = r"(?:chf|fr\.?|sfr\.?|franken)"
# "under/max CHF 2800" or "under 2800 CHF"
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
# Standalone "CHF 1800" or "1800 CHF"
_PRICE_AMOUNT_RE = re.compile(
    r"(?:" + _CHF + r"\s*)(\d[\d'. ]*\d|\d+)|(\d[\d'. ]*\d|\d+)\s*(?:" + _CHF + r")",
    re.I,
)

# Area patterns: "mindestens 65 m²", "min 80m2", "80 Quadratmeter"
_MIN_AREA_RE = re.compile(
    r"(?:mindestens|minimum|min(?:imum)?\.?|at\s+least|au\s+moins|ab)\s*(\d+)\s*(?:m[²2]|qm|quadratmeter|sqm)",
    re.I,
)

# Move-in / availability patterns
_AVAIL_RE = re.compile(
    r"(?:ab|from|move.?in|verfügbar|available)\s+"
    r"(januar|februar|märz|april|mai|juni|juli|august|september|oktober|november|dezember"
    r"|january|february|march|april|may|june|july|august|september|october|november|december"
    r"|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)"
    r"(?:\s+(\d{4}))?",
    re.I,
)


def _parse_number(raw: str) -> float:
    """Remove thousands separators (apostrophe, space) and parse."""
    cleaned = raw.replace("'", "").replace(" ", "").replace(",", ".")
    return float(cleaned)


def _extract_cities(text: str) -> list[str] | None:
    found: list[str] = []
    lower = text.lower()
    # Try longest-match-first to handle "st. gallen" before "gallen"
    for alias in sorted(_CITY_ALIASES, key=len, reverse=True):
        if re.search(r"\b" + re.escape(alias) + r"\b", lower):
            canonical = _CITY_ALIASES[alias]
            if canonical not in found:
                found.append(canonical)
    return found or None


def _extract_canton(text: str) -> str | None:
    lower = text.lower()
    for alias in sorted(_CANTON_ALIASES, key=len, reverse=True):
        if re.search(r"\b" + re.escape(alias) + r"\b", lower):
            return _CANTON_ALIASES[alias]
    return None


def _extract_rooms(text: str) -> tuple[float | None, float | None]:
    """Returns (min_rooms, max_rooms)."""
    min_rooms: float | None = None
    max_rooms: float | None = None

    if _STUDIO_RE.search(text):
        min_rooms = 1.0
        max_rooms = 1.5

    # Explicit range wins over everything: "2.5 to 3-room", "3.5 or 4 rooms"
    m = _ROOMS_RANGE_RE.search(text)
    if m:
        lo = _parse_number(m.group(1))
        hi = _parse_number(m.group(2))
        min_rooms = min(lo, hi)
        max_rooms = max(lo, hi)
        return min_rooms, max_rooms

    m = _ROOMS_MIN_RE.search(text)
    if m:
        min_rooms = _parse_number(m.group(1))

    m = _ROOMS_MAX_RE.search(text)
    if m:
        max_rooms = _parse_number(m.group(1))

    # No qualifier found: treat the first exact room mention as both min and max
    if min_rooms is None and max_rooms is None:
        matches = _ROOMS_EXACT_RE.findall(text)
        if matches:
            val = _parse_number(matches[0])
            min_rooms = val
            max_rooms = val

    return min_rooms, max_rooms


def _extract_price(text: str) -> tuple[int | None, int | None]:
    """Returns (min_price, max_price)."""
    min_price: int | None = None
    max_price: int | None = None

    m = _MAX_PRICE_RE.search(text)
    if m:
        try:
            val = int(_parse_number(m.group(1)))
            # Require CHF context: either the prefix/suffix contains 'chf/fr' OR value ≥ 200
            if val >= 200:
                max_price = val
        except (ValueError, AttributeError):
            pass

    m = _MIN_PRICE_RE.search(text)
    if m:
        try:
            val = int(_parse_number(m.group(1)))
            if val >= 200:
                min_price = val
        except (ValueError, AttributeError):
            pass

    # Fallback: standalone CHF amounts not yet captured
    if min_price is None and max_price is None:
        for m in _PRICE_AMOUNT_RE.finditer(text):
            raw = m.group(1) or m.group(2)
            if raw:
                try:
                    val = int(_parse_number(raw))
                    if val >= 200:
                        max_price = val
                        break
                except ValueError:
                    pass

    return min_price, max_price


def _extract_features(text: str) -> list[str] | None:
    found: list[str] = []
    for pattern, name in _FEATURE_PATTERNS:
        if pattern.search(text):
            if name not in found:
                found.append(name)
    return found or None


def _extract_offer_type(text: str) -> str | None:
    if _SALE_RE.search(text):
        return "SALE"
    if _RENT_RE.search(text):
        return "RENT"
    return None


def extract_hard_facts(query: str) -> HardFilters:
    query = translate_to_english(query)
    cities = _extract_cities(query)
    canton = _extract_canton(query)

    # If a well-known city is found, don't also set the canton to avoid over-restriction
    # unless the canton was explicitly mentioned differently
    if cities and canton:
        city_lower = cities[0].lower()
        # If the canton was inferred from the city name itself, skip it
        city_to_canton = {
            "zürich": "ZH", "genf": "GE", "bern": "BE", "basel": "BS",
            "zug": "ZG", "luzern": "LU", "lausanne": "VD", "winterthur": "ZH",
        }
        if city_to_canton.get(city_lower) == canton:
            canton = None

    min_rooms, max_rooms = _extract_rooms(query)
    min_price, max_price = _extract_price(query)
    features = _extract_features(query)
    offer_type = _extract_offer_type(query)

    return HardFilters(
        city=cities,
        canton=canton,
        min_price=min_price,
        max_price=max_price,
        min_rooms=min_rooms,
        max_rooms=max_rooms,
        features=features,
        offer_type=offer_type,
        limit=100,  # return more candidates for downstream ranking
    )
