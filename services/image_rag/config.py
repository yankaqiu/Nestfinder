from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_optional_int(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return int(value)


@dataclass(slots=True)
class ImageRagSettings:
    raw_data_dir: Path
    listings_db_path: Path
    db_uri: str
    state_db_path: Path
    collection_name: str
    device: str
    model: str
    sync_on_start: bool
    startup_limit: int | None
    image_batch_size_override: int | None
    query_batch_size: int
    sync_workers: int
    candidate_chunk_size: int
    search_k_multiplier: int
    image_download_timeout_s: float

    def resolved_image_batch_size(self, device_name: str) -> int:
        if self.image_batch_size_override is not None:
            return self.image_batch_size_override
        if device_name == "mps":
            return 8
        return 4


def get_settings() -> ImageRagSettings:
    repo_root = _repo_root()
    raw_data_dir = Path(os.getenv("LISTINGS_RAW_DATA_DIR", repo_root / "raw_data"))
    listings_db_path = Path(os.getenv("LISTINGS_DB_PATH", repo_root / "data" / "listings.db"))
    image_rag_dir = repo_root / "data" / "image-rag"

    return ImageRagSettings(
        raw_data_dir=raw_data_dir,
        listings_db_path=listings_db_path,
        db_uri=os.getenv("IMAGE_RAG_DB_URI", str(image_rag_dir / "milvus.db")),
        state_db_path=Path(os.getenv("IMAGE_RAG_STATE_DB", image_rag_dir / "state.db")),
        collection_name=os.getenv("IMAGE_RAG_COLLECTION_NAME", "listing_images"),
        device=os.getenv("IMAGE_RAG_DEVICE", "auto"),
        model=os.getenv("IMAGE_RAG_MODEL", "auto"),
        sync_on_start=_env_bool("IMAGE_RAG_SYNC_ON_START", True),
        startup_limit=_env_optional_int("IMAGE_RAG_STARTUP_LIMIT"),
        image_batch_size_override=_env_optional_int("IMAGE_RAG_IMAGE_BATCH_SIZE"),
        query_batch_size=int(os.getenv("IMAGE_RAG_QUERY_BATCH_SIZE", "1")),
        sync_workers=int(os.getenv("IMAGE_RAG_SYNC_WORKERS", "4")),
        candidate_chunk_size=int(os.getenv("IMAGE_RAG_CANDIDATE_CHUNK_SIZE", "500")),
        search_k_multiplier=int(os.getenv("IMAGE_RAG_SEARCH_K_MULTIPLIER", "3")),
        image_download_timeout_s=float(os.getenv("IMAGE_RAG_IMAGE_DOWNLOAD_TIMEOUT_S", "20")),
    )
