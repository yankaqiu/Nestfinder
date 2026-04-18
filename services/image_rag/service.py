from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging

from app.harness.bootstrap import bootstrap_database

from services.image_rag.config import ImageRagSettings
from services.image_rag.device import detect_device, resolve_model_name
from services.image_rag.embedder import OpenClipEmbedder
from services.image_rag.image_loader import ImageLoader
from services.image_rag.listing_repository import ListingRepository
from services.image_rag.schemas import SearchResultItem
from services.image_rag.sync_manager import SyncManager, SyncSummary
from services.image_rag.sync_state import SyncStateStore
from services.image_rag.vector_store import MilvusLiteStore


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SearchOutput:
    results: list[SearchResultItem]
    meta: dict[str, object]


class ImageRagService:
    def __init__(self, settings: ImageRagSettings) -> None:
        self._settings = settings
        self._device_info = None
        self._selected_model = None
        self._repository = ListingRepository(settings.listings_db_path)
        self._state_store = SyncStateStore(settings.state_db_path)
        self._vector_store = MilvusLiteStore(
            uri=settings.db_uri,
            collection_name=settings.collection_name,
        )
        self._embedder: OpenClipEmbedder | None = None
        self._sync_manager: SyncManager | None = None
        self._backfill_task: asyncio.Task | None = None

    async def startup(self) -> None:
        bootstrap_database(
            db_path=self._settings.listings_db_path,
            raw_data_dir=self._settings.raw_data_dir,
        )

        self._device_info = detect_device(self._settings.device)
        self._selected_model = resolve_model_name(self._settings.model, self._device_info)
        self._print_startup_summary()

        self._embedder = OpenClipEmbedder(
            model_name=self._selected_model,
            device_info=self._device_info,
            image_batch_size=self._settings.resolved_image_batch_size(self._device_info.selected),
            query_batch_size=self._settings.query_batch_size,
        )
        self._vector_store.ensure_collection(dimension=self._embedder.dim)
        self._sync_manager = SyncManager(
            repository=self._repository,
            state_store=self._state_store,
            vector_store=self._vector_store,
            embedder=self._embedder,
            image_loader=ImageLoader(timeout_s=self._settings.image_download_timeout_s),
            raw_data_dir=self._settings.raw_data_dir,
            listings_db_path=self._settings.listings_db_path,
            max_workers=self._settings.sync_workers,
        )

        if self._settings.sync_on_start:
            self._backfill_task = asyncio.create_task(self._run_startup_backfill(), name="image-rag-backfill")

    async def shutdown(self) -> None:
        if self._backfill_task is not None:
            self._backfill_task.cancel()
            try:
                await self._backfill_task
            except asyncio.CancelledError:
                pass

    def health(self) -> dict[str, str]:
        return {"status": "ok"}

    def status(self) -> dict[str, object]:
        device_info = self._require_device_info()
        return {
            "status": "ok",
            "selected_device": device_info.selected,
            "cuda_available": device_info.cuda_available,
            "mps_available": device_info.mps_available,
            "mps_reason": device_info.mps_reason,
            "selected_model": self._require_selected_model(),
            "collection_name": self._vector_store.collection_name,
            "indexed_image_rows": self._vector_store.row_count(),
            "indexed_listing_rows": self._state_store.count_indexed_listings(
                model_name=self._require_selected_model()
            ),
            "startup_backfill_running": bool(self._backfill_task and not self._backfill_task.done()),
            "last_backfill_started_at": self._state_store.get_service_state("last_backfill_started_at"),
            "last_backfill_completed_at": self._state_store.get_service_state("last_backfill_completed_at"),
        }

    def search(self, *, query_text: str, listing_ids: list[str], top_k: int) -> SearchOutput:
        embedder = self._require_embedder()
        deduped_ids = list(dict.fromkeys(listing_ids))
        if not query_text.strip() or not deduped_ids:
            return SearchOutput(
                results=[],
                meta={
                    "model_name": self._require_selected_model(),
                    "device": self._require_device_info().selected,
                    "candidate_count": len(deduped_ids),
                    "indexed_now": 0,
                    "indexed_candidate_count": 0,
                    "missing_image_count": 0,
                },
            )

        indexed_ids = self._state_store.list_indexed_listing_ids(
            listing_ids=deduped_ids,
            model_name=self._require_selected_model(),
        )
        if not indexed_ids:
            return SearchOutput(
                results=[],
                meta={
                    "model_name": self._require_selected_model(),
                    "device": self._require_device_info().selected,
                    "candidate_count": len(deduped_ids),
                    "indexed_now": 0,
                    "indexed_candidate_count": 0,
                    "missing_image_count": len(deduped_ids),
                },
            )

        query_vector = embedder.encode_texts([query_text])[0]
        hits = self._vector_store.search(
            query_vector=query_vector,
            listing_ids=indexed_ids,
            model_name=self._require_selected_model(),
            top_k=top_k,
            chunk_size=self._settings.candidate_chunk_size,
            search_k_multiplier=self._settings.search_k_multiplier,
        )

        best_by_listing: dict[str, SearchResultItem] = {}
        for hit in hits:
            existing = best_by_listing.get(hit.listing_id)
            if existing is None or hit.score > existing.score:
                best_by_listing[hit.listing_id] = SearchResultItem(
                    listing_id=hit.listing_id,
                    score=hit.score,
                    best_image_url=hit.image_url,
                    best_image_id=hit.image_id,
                )

        results = sorted(
            best_by_listing.values(),
            key=lambda item: (-item.score, item.listing_id),
        )[:top_k]

        return SearchOutput(
            results=results,
            meta={
                "model_name": self._require_selected_model(),
                "device": self._require_device_info().selected,
                "candidate_count": len(deduped_ids),
                "indexed_now": 0,
                "indexed_candidate_count": len(indexed_ids),
                "missing_image_count": len(deduped_ids) - len(indexed_ids),
            },
        )

    def sync(self, *, listing_ids: list[str] | None = None) -> SyncSummary:
        sync_manager = self._require_sync_manager()
        if listing_ids is None:
            return sync_manager.sync_all(limit=self._settings.startup_limit)
        deduped_ids = list(dict.fromkeys(listing_ids))
        return sync_manager.ensure_indexed(deduped_ids)

    async def _run_startup_backfill(self) -> None:
        try:
            await asyncio.to_thread(self._require_sync_manager().sync_all, limit=self._settings.startup_limit)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Image RAG startup backfill failed.")

    def _print_startup_summary(self) -> None:
        device_info = self._require_device_info()
        model_name = self._require_selected_model()
        lines = [
            f"[image-rag] selected_device={device_info.selected}",
            f"[image-rag] cuda_available={device_info.cuda_available}",
            f"[image-rag] mps_available={device_info.mps_available}",
            f"[image-rag] gpu_available={device_info.gpu_available}",
        ]
        if not device_info.mps_available and device_info.mps_reason:
            lines.append(f"[image-rag] mps_reason={device_info.mps_reason}")
        lines.append(f"[image-rag] selected_model={model_name}")
        for line in lines:
            print(line)
            logger.info(line)

    def _require_device_info(self):
        if self._device_info is None:
            raise RuntimeError("Image RAG service has not been started yet.")
        return self._device_info

    def _require_selected_model(self) -> str:
        if self._selected_model is None:
            raise RuntimeError("Image RAG service has not been started yet.")
        return self._selected_model

    def _require_embedder(self) -> OpenClipEmbedder:
        if self._embedder is None:
            raise RuntimeError("Image RAG service has not been started yet.")
        return self._embedder

    def _require_sync_manager(self) -> SyncManager:
        if self._sync_manager is None:
            raise RuntimeError("Image RAG service has not been started yet.")
        return self._sync_manager
