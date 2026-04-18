from __future__ import annotations

from dataclasses import dataclass
import threading

from services.image_rag.embedder import OpenClipEmbedder
from services.image_rag.image_loader import ImageLoader
from services.image_rag.image_sources import image_urls_hash, resolve_listing_images
from services.image_rag.listing_repository import ListingRepository
from services.image_rag.sync_state import SyncStateStore, utc_now_iso
from services.image_rag.vector_store import MilvusLiteStore, VectorImageRecord


@dataclass(slots=True)
class SyncSummary:
    indexed_now: int = 0
    missing_image_count: int = 0
    requested_count: int = 0
    error_count: int = 0

    def merge(self, other: "SyncSummary") -> "SyncSummary":
        self.indexed_now += other.indexed_now
        self.missing_image_count += other.missing_image_count
        self.requested_count += other.requested_count
        self.error_count += other.error_count
        return self


class SyncManager:
    def __init__(
        self,
        *,
        repository: ListingRepository,
        state_store: SyncStateStore,
        vector_store: MilvusLiteStore,
        embedder: OpenClipEmbedder,
        image_loader: ImageLoader,
        raw_data_dir,
        listings_db_path,
    ) -> None:
        self._repository = repository
        self._state_store = state_store
        self._vector_store = vector_store
        self._embedder = embedder
        self._image_loader = image_loader
        self._raw_data_dir = raw_data_dir
        self._listings_db_path = listings_db_path
        self._active_syncs: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    @property
    def model_name(self) -> str:
        return self._embedder.model_name

    def ensure_indexed(self, listing_ids: list[str]) -> SyncSummary:
        summary = SyncSummary()
        for listing_id in listing_ids:
            summary.merge(self.sync_listing(listing_id))
        return summary

    def sync_all(self, *, limit: int | None = None) -> SyncSummary:
        self._state_store.set_service_state(key="last_backfill_started_at", value=utc_now_iso())
        summary = SyncSummary()
        listing_ids = self._repository.list_listing_ids(limit=limit)
        for listing_id in listing_ids:
            summary.merge(self.sync_listing(listing_id))
        self._state_store.set_service_state(key="last_backfill_completed_at", value=utc_now_iso())
        return summary

    def sync_listing(self, listing_id: str) -> SyncSummary:
        wait_event: threading.Event | None = None
        created_event: threading.Event | None = None

        with self._lock:
            if listing_id in self._active_syncs:
                wait_event = self._active_syncs[listing_id]
            else:
                created_event = threading.Event()
                self._active_syncs[listing_id] = created_event

        if wait_event is not None:
            wait_event.wait()
            return SyncSummary(requested_count=1)

        try:
            return self._sync_listing_internal(listing_id)
        finally:
            assert created_event is not None
            with self._lock:
                created_event.set()
                self._active_syncs.pop(listing_id, None)

    def _sync_listing_internal(self, listing_id: str) -> SyncSummary:
        summary = SyncSummary(requested_count=1)
        record = self._repository.get_listing(listing_id)
        if record is None:
            summary.missing_image_count += 1
            return summary

        images = resolve_listing_images(
            db_path=self._listings_db_path,
            raw_data_dir=self._raw_data_dir,
            record=record,
            model_name=self._embedder.model_name,
        )
        current_hash = image_urls_hash(images)
        prior_state = self._state_store.get_listing_state(
            listing_id=listing_id,
            model_name=self._embedder.model_name,
        )
        if prior_state and prior_state.image_urls_hash == current_hash and prior_state.last_error is None:
            return summary

        if not images:
            self._delete_existing_listing_vectors(listing_id)
            self._state_store.upsert_listing_state(
                listing_id=listing_id,
                model_name=self._embedder.model_name,
                image_urls_hash=current_hash,
                image_count=0,
                last_error=None,
            )
            summary.missing_image_count += 1
            return summary

        loaded_images = []
        loaded_refs = []
        for image in images:
            try:
                loaded_images.append(self._image_loader.load(image))
                loaded_refs.append(image)
            except Exception as exc:
                self._state_store.upsert_listing_state(
                    listing_id=listing_id,
                    model_name=self._embedder.model_name,
                    image_urls_hash=current_hash,
                    image_count=0,
                    last_error=str(exc),
                )

        if not loaded_images:
            self._delete_existing_listing_vectors(listing_id)
            summary.missing_image_count += 1
            summary.error_count += 1
            return summary

        embeddings = self._embedder.encode_images(loaded_images)
        records = [
            VectorImageRecord(
                row_id=image.image_id,
                listing_id=record.listing_id,
                image_id=image.image_id,
                image_url=image.image_url,
                scrape_source=record.scrape_source or "",
                model_name=self._embedder.model_name,
                model_version=self._embedder.model_version,
                embedding=embedding,
            )
            for image, embedding in zip(loaded_refs, embeddings, strict=True)
        ]

        self._vector_store.upsert(records)
        existing_ids = set(
            self._vector_store.list_row_ids_for_listing(
                listing_id=listing_id,
                model_name=self._embedder.model_name,
            )
        )
        active_ids = {record.row_id for record in records}
        stale_ids = sorted(existing_ids - active_ids)
        if stale_ids:
            self._vector_store.delete_ids(stale_ids)

        self._state_store.upsert_listing_state(
            listing_id=listing_id,
            model_name=self._embedder.model_name,
            image_urls_hash=current_hash,
            image_count=len(records),
            last_error=None,
        )
        summary.indexed_now += 1
        return summary

    def _delete_existing_listing_vectors(self, listing_id: str) -> None:
        existing_ids = self._vector_store.list_row_ids_for_listing(
            listing_id=listing_id,
            model_name=self._embedder.model_name,
        )
        if existing_ids:
            self._vector_store.delete_ids(existing_ids)
