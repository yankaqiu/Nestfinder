from __future__ import annotations

import re

import anthropic

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic()
    return _client


# Quick heuristic: if query is already mostly ASCII-Latin, skip the API call.
_NON_LATIN_RE = re.compile(r"[^\x00-\x7Fàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿœ\s]")
_GERMAN_MARKERS = re.compile(
    r"\b(ich|suche|zimmer|wohnung|miete|möbliert|ruhig|hell|nähe|mindestens|maximal|"
    r"unter|zürich|winterthur|basel|bern|luzern|zug|lausanne|genf|studio|küche|bad)\b",
    re.I,
)
_FRENCH_MARKERS = re.compile(
    r"\b(je|cherche|appartement|louer|pièces?|lumineux|calme|près|au\s+moins|au\s+maximum|"
    r"zurich|genève|lausanne|berne|bâle|lugano|möbliert|cuisine|salle)\b",
    re.I,
)

# Cached system prompt with prompt-caching marker so repeated calls reuse it
_SYSTEM_PROMPT = (
    "You are a precise translation assistant. "
    "Your only job is to translate the user's real-estate search query into English. "
    "Output ONLY the translated query text — no explanation, no quotes, no commentary. "
    "If the query is already in English, return it unchanged."
)


def _is_english(query: str) -> bool:
    """Return True if the query is very likely already in English."""
    if _NON_LATIN_RE.search(query):
        return False
    if _GERMAN_MARKERS.search(query):
        return False
    if _FRENCH_MARKERS.search(query):
        return False
    return True


def translate_to_english(query: str) -> str:
    """Translate query to English using Claude Haiku; return original if already English.

    Falls back silently to the original query if the API call fails.
    """
    if _is_english(query):
        return query

    try:
        client = _get_client()
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=512,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": query}],
        )
        translated = next(
            (b.text.strip() for b in response.content if b.type == "text"), query
        )
        return translated or query
    except Exception:
        # Graceful degradation: regex extractors still work on German/French
        return query
