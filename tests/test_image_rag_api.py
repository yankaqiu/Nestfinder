from pathlib import Path

from fastapi.testclient import TestClient

from services.image_rag.main import create_app


class FakeImageRagService:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def startup(self) -> None:
        self.started = True

    async def shutdown(self) -> None:
        self.stopped = True

    def health(self) -> dict[str, str]:
        return {"status": "ok"}

    def status(self) -> dict[str, object]:
        return {
            "status": "ok",
            "selected_device": "cpu",
            "cuda_available": False,
            "mps_available": False,
            "mps_reason": "MPS not available because the current PyTorch install was not built with MPS enabled.",
            "selected_model": "timm/ViT-B-16-SigLIP-i18n-256",
            "collection_name": "listing_images",
            "indexed_image_rows": 12,
            "indexed_listing_rows": 5,
            "startup_backfill_running": False,
            "last_backfill_started_at": None,
            "last_backfill_completed_at": None,
        }

    def search(self, *, query_text: str, listing_ids: list[str], top_k: int):
        del query_text, listing_ids, top_k
        return type(
            "SearchOutput",
            (),
            {
                "results": [
                    {
                        "listing_id": "listing-1",
                        "score": 0.87,
                        "best_image_url": "https://example.com/1.jpg",
                        "best_image_id": "listing-1:abc",
                    }
                ],
                "meta": {
                    "model_name": "timm/ViT-B-16-SigLIP-i18n-256",
                    "device": "cpu",
                    "candidate_count": 1,
                    "indexed_now": 0,
                    "missing_image_count": 0,
                },
            },
        )()

    def sync(self, *, listing_ids: list[str] | None = None):
        del listing_ids
        return type(
            "SyncSummary",
            (),
            {
                "indexed_now": 2,
                "missing_image_count": 1,
                "requested_count": 3,
                "error_count": 0,
            },
        )()


def test_image_rag_api_endpoints() -> None:
    fake_service = FakeImageRagService()
    app = create_app(service=fake_service)

    with TestClient(app) as client:
        health = client.get("/health")
        status = client.get("/status")
        search = client.post(
            "/search",
            json={
                "query_text": "bright balcony apartment",
                "listing_ids": ["listing-1"],
                "top_k": 10,
            },
        )
        sync = client.post("/admin/sync", json={"listing_ids": ["listing-1", "listing-2"]})

    assert fake_service.started is True
    assert fake_service.stopped is True
    assert health.status_code == 200
    assert status.status_code == 200
    assert search.status_code == 200
    assert sync.status_code == 200
    assert status.json()["selected_model"] == "timm/ViT-B-16-SigLIP-i18n-256"
    assert search.json()["results"][0]["listing_id"] == "listing-1"
    assert sync.json()["indexed_now"] == 2
