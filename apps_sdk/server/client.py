from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(slots=True)
class ListingsApiClient:
    base_url: str

    async def search_listings(
        self,
        *,
        query: str,
        limit: int = 25,
        offset: int = 0,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
            response = await client.post(
                "/listings",
                json={
                    "query": query,
                    "limit": limit,
                    "offset": offset,
                    "session_id": session_id,
                },
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("listings"), list):
                raise ValueError("Listings API returned an invalid listings wrapper payload.")
            return payload


def get_listings_api_client() -> ListingsApiClient:
    return ListingsApiClient(
        base_url=os.getenv("APPS_SDK_LISTINGS_API_BASE_URL", "http://localhost:8000")
    )
