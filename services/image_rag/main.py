from __future__ import annotations

import os

# Increase gRPC keepalive interval to stop Milvus Lite from closing the channel.
# Default is ~10 s which triggers ENHANCE_YOUR_CALM / too_many_pings from the
# embedded server. 5 minutes is well within typical idle timeouts.
os.environ.setdefault("GRPC_KEEPALIVE_TIME_MS", "300000")
os.environ.setdefault("GRPC_KEEPALIVE_TIMEOUT_MS", "20000")
os.environ.setdefault("GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS", "0")

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool

from services.image_rag.config import get_settings
from services.image_rag.schemas import (
    HealthResponse,
    SearchRequest,
    SearchResponse,
    StatusResponse,
    SyncRequest,
    SyncResponse,
)
from services.image_rag.service import ImageRagService


def create_app(service: ImageRagService | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        runtime_service = service or ImageRagService(get_settings())
        app.state.image_rag_service = runtime_service
        await runtime_service.startup()
        try:
            yield
        finally:
            await runtime_service.shutdown()

    app = FastAPI(
        title="Datathon 2026 Image RAG Service",
        lifespan=lifespan,
    )

    @app.get("/health", response_model=HealthResponse)
    async def health(request: Request) -> HealthResponse:
        payload = _get_service(request).health()
        return HealthResponse(**payload)

    @app.get("/status", response_model=StatusResponse)
    async def status(request: Request) -> StatusResponse:
        payload = await run_in_threadpool(_get_service(request).status)
        return StatusResponse(**payload)

    @app.post("/search", response_model=SearchResponse)
    async def search(request: Request, body: SearchRequest) -> SearchResponse:
        result = await run_in_threadpool(
            _get_service(request).search,
            query_text=body.query_text,
            listing_ids=body.listing_ids,
            top_k=body.top_k,
        )
        return SearchResponse(results=result.results, meta=result.meta)

    @app.post("/admin/sync", response_model=SyncResponse)
    async def sync(request: Request, body: SyncRequest) -> SyncResponse:
        summary = await run_in_threadpool(
            _get_service(request).sync,
            listing_ids=body.listing_ids,
        )
        return SyncResponse(
            indexed_now=summary.indexed_now,
            missing_image_count=summary.missing_image_count,
            requested_count=summary.requested_count,
            error_count=summary.error_count,
        )

    return app


def _get_service(request: Request) -> ImageRagService:
    return request.app.state.image_rag_service


app = create_app()
