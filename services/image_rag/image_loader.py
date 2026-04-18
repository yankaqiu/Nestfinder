from __future__ import annotations

from io import BytesIO
from pathlib import Path

import httpx
from PIL import Image

from services.image_rag.image_sources import ResolvedImage


class ImageLoader:
    def __init__(self, *, timeout_s: float) -> None:
        self._timeout_s = timeout_s

    def load(self, image: ResolvedImage) -> Image.Image:
        if image.local_path is not None:
            return self._load_local(image.local_path)
        return self._load_remote(image.image_url)

    def _load_local(self, path: Path) -> Image.Image:
        with path.open("rb") as handle:
            return Image.open(handle).convert("RGB")

    def _load_remote(self, url: str) -> Image.Image:
        with httpx.Client(timeout=self._timeout_s, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
        return Image.open(BytesIO(response.content)).convert("RGB")
