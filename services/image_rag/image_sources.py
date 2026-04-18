from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

from app.core.s3 import get_image_urls_by_listing_id

from services.image_rag.listing_repository import ListingRecord


@dataclass(slots=True)
class ResolvedImage:
    image_id: str
    image_url: str
    local_path: Path | None = None


def resolve_listing_images(
    *,
    db_path: Path,
    raw_data_dir: Path,
    record: ListingRecord,
    model_name: str,
) -> list[ResolvedImage]:
    raw_urls = _extract_image_urls(record.images_json)
    if not raw_urls:
        raw_urls = get_image_urls_by_listing_id(db_path=db_path, listing_id=record.listing_id)

    resolved: list[ResolvedImage] = []
    seen_urls: set[str] = set()
    for image_url in raw_urls:
        if image_url in seen_urls:
            continue
        seen_urls.add(image_url)
        local_path = _local_path_for_image(raw_data_dir=raw_data_dir, image_url=image_url)
        image_id = _build_image_id(record.listing_id, model_name, image_url)
        resolved.append(
            ResolvedImage(
                image_id=image_id,
                image_url=image_url,
                local_path=local_path,
            )
        )
    return resolved


def image_urls_hash(images: list[ResolvedImage]) -> str:
    payload = "\n".join(image.image_url for image in images)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _extract_image_urls(images_json: str | None) -> list[str]:
    if not images_json:
        return []
    try:
        parsed = json.loads(images_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, dict):
        return []

    urls: list[str] = []
    for item in parsed.get("images", []) or []:
        if isinstance(item, dict) and item.get("url"):
            urls.append(str(item["url"]))
        elif isinstance(item, str) and item:
            urls.append(item)
    for item in parsed.get("image_paths", []) or []:
        if isinstance(item, str) and item:
            urls.append(item)
    return urls


def _local_path_for_image(*, raw_data_dir: Path, image_url: str) -> Path | None:
    if not image_url.startswith("/raw-data-images/"):
        return None
    filename = image_url.rsplit("/", 1)[-1]
    local_path = raw_data_dir / "sred_images" / filename
    if local_path.exists():
        return local_path
    return None


def _build_image_id(listing_id: str, model_name: str, image_url: str) -> str:
    digest = hashlib.sha256(f"{listing_id}|{model_name}|{image_url}".encode("utf-8")).hexdigest()
    return f"{listing_id}:{digest}"
