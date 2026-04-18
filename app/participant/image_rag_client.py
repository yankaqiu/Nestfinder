from __future__ import annotations

import os
from typing import Any

import httpx


def get_image_rag_base_url() -> str | None:
    value = os.getenv("IMAGE_RAG_BASE_URL")
    if value is None:
        return None
    stripped = value.strip().rstrip("/")
    return stripped or None


def search_image_rag(
    *,
    query_text: str,
    listing_ids: list[str],
    top_k: int,
) -> dict[str, Any] | None:
    base_url = get_image_rag_base_url()
    if base_url is None or not query_text.strip() or not listing_ids:
        return None

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        response = client.post(
            "/search",
            json={
                "query_text": query_text,
                "listing_ids": listing_ids,
                "top_k": top_k,
            },
        )
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("Image RAG returned a non-dict payload.")
    if not isinstance(payload.get("results"), list):
        raise ValueError("Image RAG returned an invalid results list.")
    return payload
