from pathlib import Path

from services.image_rag.image_sources import ResolvedImage
from services.image_rag.listing_repository import ListingRecord
from services.image_rag.sync_manager import SyncManager
from services.image_rag.sync_state import SyncStateStore


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


class FakeEmbedder:
    def __init__(self) -> None:
        self.model_name = "test-model"
        self.model_version = "1.0"
        self.image_encode_calls = 0

    def encode_images(self, images) -> list[list[float]]:
        self.image_encode_calls += 1
        return [[float(index + 1), 0.0] for index, _ in enumerate(images)]


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
    )

    first = sync_manager.sync_listing("listing-1")
    second = sync_manager.sync_listing("listing-1")

    assert first.indexed_now == 1
    assert second.indexed_now == 0
    assert embedder.image_encode_calls == 1
