from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query_text: str = Field(min_length=1)
    listing_ids: list[str] = Field(default_factory=list)
    top_k: int = Field(default=20, ge=1, le=500)


class SearchResultItem(BaseModel):
    listing_id: str
    score: float
    best_image_url: str | None = None
    best_image_id: str | None = None


class SearchResponse(BaseModel):
    results: list[SearchResultItem]
    meta: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: str


class StatusResponse(BaseModel):
    status: str
    selected_device: str
    cuda_available: bool
    mps_available: bool
    mps_reason: str | None = None
    selected_model: str
    collection_name: str
    indexed_image_rows: int
    indexed_listing_rows: int
    startup_backfill_running: bool
    last_backfill_started_at: str | None = None
    last_backfill_completed_at: str | None = None


class SyncRequest(BaseModel):
    listing_ids: list[str] | None = None


class SyncResponse(BaseModel):
    indexed_now: int
    missing_image_count: int
    requested_count: int
    error_count: int
