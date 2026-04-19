from pathlib import Path

from services.image_rag.config import ImageRagSettings
from services.image_rag.image_sources import ResolvedImage
from services.image_rag.listing_repository import ListingRecord
from services.image_rag.service import ImageRagService
from services.image_rag.sync_manager import SyncManager
from services.image_rag.sync_state import SyncStateStore
from services.image_rag.vector_store import ImageSearchHit


class FakeRepository:
    def __init__(self, record: ListingRecord) -> None:
        self.record = record

    def get_listing(self, listing_id: str) -> ListingRecord | None:
        if listing_id == self.record.listing_id:
            return self.record
        return None

    def list_listing_ids(self, *, limit: int | None = None) -> list[str]:
        del limit
        return [self.record.listing_id]


class FakeVectorStore:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, object]] = {}
        self.search_listing_ids: list[str] | None = None

    def upsert(self, records) -> None:
        for record in records:
            self.records[record.row_id] = {
                "listing_id": record.listing_id,
                "model_name": record.model_name,
            }

    def list_row_ids_for_listing(self, *, listing_id: str, model_name: str) -> list[str]:
        return [
            row_id
            for row_id, payload in self.records.items()
            if payload["listing_id"] == listing_id and payload["model_name"] == model_name
        ]

    def delete_ids(self, ids: list[str]) -> None:
        for row_id in ids:
            self.records.pop(row_id, None)

    def search(
        self,
        *,
        query_vector,
        listing_ids,
        model_name,
        top_k,
        chunk_size,
        search_k_multiplier,
    ):
        del query_vector, model_name, top_k, chunk_size, search_k_multiplier
        self.search_listing_ids = list(listing_ids)
        return [
            ImageSearchHit(
                listing_id=listing_ids[0],
                image_id="img-1",
                image_url="https://example.com/1.jpg",
                score=0.9,
            )
        ]


class FakeEmbedder:
    def __init__(self) -> None:
        self.model_name = "test-model"
        self.model_version = "1.0"
        self.image_encode_calls = 0
        self.text_encode_calls = 0

    def encode_images(self, images) -> list[list[float]]:
        self.image_encode_calls += 1
        return [[float(index + 1), 0.0] for index, _ in enumerate(images)]

    def encode_texts(self, texts) -> list[list[float]]:
        self.text_encode_calls += 1
        return [[1.0, 0.0] for _ in texts]


class FakeImageLoader:
    def load(self, image: ResolvedImage):
        return image.image_url


def test_sync_manager_skips_unchanged_listing(tmp_path: Path, monkeypatch) -> None:
    record = ListingRecord(
        listing_id="listing-1",
        platform_id="platform-1",
        scrape_source="COMPARIS",
        images_json='{"images":[{"url":"https://example.com/1.jpg"}]}',
    )

    monkeypatch.setattr(
        "services.image_rag.sync_manager.resolve_listing_images",
        lambda **kwargs: [
            ResolvedImage(
                image_id="listing-1:image-1",
                image_url="https://example.com/1.jpg",
            )
        ],
    )

    embedder = FakeEmbedder()
    sync_manager = SyncManager(
        repository=FakeRepository(record),
        state_store=SyncStateStore(tmp_path / "state.db"),
        vector_store=FakeVectorStore(),
        embedder=embedder,
        image_loader=FakeImageLoader(),
        raw_data_dir=tmp_path,
        listings_db_path=tmp_path / "listings.db",
        max_workers=2,
    )

    first = sync_manager.sync_listing("listing-1")
    second = sync_manager.sync_listing("listing-1")

    assert first.indexed_now == 1
    assert second.indexed_now == 0
    assert embedder.image_encode_calls == 1


def test_image_rag_search_uses_only_preindexed_listings(tmp_path: Path) -> None:
    settings = ImageRagSettings(
        raw_data_dir=tmp_path,
        listings_db_path=tmp_path / "listings.db",
        db_uri=str(tmp_path / "milvus.db"),
        state_db_path=tmp_path / "state.db",
        collection_name="listing_images",
        device="cpu",
        model="auto",
        sync_on_start=False,
        startup_limit=None,
        image_batch_size_override=None,
        query_batch_size=1,
        sync_workers=2,
        candidate_chunk_size=500,
        search_k_multiplier=3,
        image_download_timeout_s=20.0,
    )
    service = ImageRagService(settings)
    service._device_info = type(
        "DeviceInfo",
        (),
        {
            "selected": "cpu",
            "cuda_available": False,
            "mps_available": False,
            "mps_reason": None,
            "gpu_available": False,
        },
    )()
    service._selected_model = "test-model"
    service._embedder = FakeEmbedder()
    service._sync_manager = object()
    state_store = SyncStateStore(tmp_path / "state.db")
    state_store.upsert_listing_state(
        listing_id="indexed-listing",
        model_name="test-model",
        image_urls_hash="hash-1",
        image_count=2,
        last_error=None,
    )
    state_store.upsert_listing_state(
        listing_id="missing-images",
        model_name="test-model",
        image_urls_hash="hash-2",
        image_count=0,
        last_error=None,
    )
    service._state_store = state_store
    vector_store = FakeVectorStore()
    service._vector_store = vector_store

    result = service.search(
        query_text="bright apartment",
        listing_ids=["indexed-listing", "missing-images", "not-processed"],
        top_k=5,
    )

    assert vector_store.search_listing_ids == ["indexed-listing"]
    assert result.meta["indexed_now"] == 0
    assert result.meta["indexed_candidate_count"] == 1
    assert result.meta["missing_image_count"] == 2
    assert [item.listing_id for item in result.results] == ["indexed-listing"]
