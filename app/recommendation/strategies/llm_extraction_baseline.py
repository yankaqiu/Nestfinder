from __future__ import annotations

import json
import os
import re
from typing import Any

import anthropic

from app.models.schemas import HardFilters, RankedListingResult
from app.participant.hard_fact_extraction import (
    extract_hard_facts as extract_rule_based_hard_facts,
)
from app.participant.ranking import rank_listings as rank_rule_based_listings
from app.participant.soft_fact_extraction import (
    extract_soft_facts as extract_rule_based_soft_facts,
)
from app.participant.soft_filtering import filter_soft_facts as filter_rule_based_soft_facts
from app.participant.translate import translate_to_english
from app.recommendation.base import RecommendationStrategy, SoftFacts

_client: anthropic.Anthropic | None = None

_HARD_SYSTEM_PROMPT = """\
You are a Swiss real-estate search assistant.
Extract ONLY the hard constraints (non-negotiable requirements) from the user query.
Return ONLY a valid JSON object.

JSON schema (all fields optional):
{
  "city": ["Zürich"],
  "canton": "ZH",
  "postal_code": ["8001"],
  "min_rooms": 2.5,
  "max_rooms": 3.5,
  "min_price": null,
  "max_price": 2800,
  "offer_type": "RENT",
  "features": ["elevator"],
  "object_category": ["apartment"]
}
"""

_SOFT_SYSTEM_PROMPT = """\
You are a Swiss real-estate search assistant.
Extract soft preferences (nice-to-haves for ranking) from the user query.
Return ONLY a valid JSON object.

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
"""


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    return _client


def _load_json_response(*, system_prompt: str, query: str) -> dict[str, Any]:
    response = _get_client().messages.create(
        model="claude-haiku-4-5",
        max_tokens=512,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": query}],
    )
    raw = next((block.text.strip() for block in response.content if block.type == "text"), "")
    cleaned = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.M).strip()
    payload = json.loads(cleaned)
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object from the extraction model.")
    return payload


def extract_llm_hard_facts(query: str) -> HardFilters:
    translated_query = translate_to_english(query)
    try:
        payload = _load_json_response(
            system_prompt=_HARD_SYSTEM_PROMPT,
            query=translated_query,
        )
        filtered_payload = {
            key: value
            for key, value in payload.items()
            if value is not None and key in HardFilters.model_fields
        }
        filtered_payload.setdefault("limit", 100)
        return HardFilters(**filtered_payload)
    except Exception:
        return extract_rule_based_hard_facts(translated_query)


def extract_llm_soft_facts(query: str) -> SoftFacts:
    translated_query = translate_to_english(query)
    try:
        payload = _load_json_response(
            system_prompt=_SOFT_SYSTEM_PROMPT,
            query=translated_query,
        )
        signals = payload.get("signals") or {}
        if not isinstance(signals, dict):
            raise ValueError("Expected a signals object in the soft-facts payload.")
        payload["signals"] = {
            str(key): float(value)
            for key, value in signals.items()
        }
        payload["raw_query"] = translated_query
        return payload
    except Exception:
        return extract_rule_based_soft_facts(translated_query)


class LlmExtractionBaselineStrategy(RecommendationStrategy):
    strategy_id = "llm_extraction_baseline"
    display_name = "LLM Extraction Baseline"
    description = (
        "LLM-assisted hard and soft extraction with soft filtering and image reranking."
    )

    def extract_hard_facts(self, query: str) -> HardFilters:
        return extract_llm_hard_facts(query)

    def extract_soft_facts(self, query: str) -> SoftFacts:
        return extract_llm_soft_facts(query)

    def filter_soft_facts(
        self,
        candidates: list[dict[str, Any]],
        soft_facts: SoftFacts,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return filter_rule_based_soft_facts(
            candidates,
            soft_facts,
            limit=limit,
            offset=offset,
        )

    def rank_listings(
        self,
        candidates: list[dict[str, Any]],
        soft_facts: SoftFacts,
        user_profile: dict[str, Any] | None = None,
    ) -> list[RankedListingResult]:
        return rank_rule_based_listings(candidates, soft_facts, user_profile=user_profile)
