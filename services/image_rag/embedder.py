from __future__ import annotations

from contextlib import nullcontext
from typing import Any

try:
    import open_clip
except Exception:  # pragma: no cover - startup failure path
    open_clip = None  # type: ignore[assignment]

try:
    import torch
except Exception:  # pragma: no cover - startup failure path
    torch = None  # type: ignore[assignment]

from PIL import Image

from services.image_rag.device import DeviceInfo


class OpenClipEmbedder:
    def __init__(
        self,
        *,
        model_name: str,
        device_info: DeviceInfo,
        image_batch_size: int,
        query_batch_size: int,
    ) -> None:
        if open_clip is None or torch is None:
            raise RuntimeError(
                "Image RAG dependencies are missing. Install torch and open-clip-torch first."
            )

        self.model_name = model_name
        self.model_version = getattr(open_clip, "__version__", "unknown")
        self._device_info = device_info
        self._device = device_info.selected
        self._dtype = torch.float16 if self._device == "cuda" else torch.float32
        self._image_batch_size = image_batch_size
        self._query_batch_size = query_batch_size

        model_ref = f"hf-hub:{model_name}"
        self._model, _, self._preprocess = open_clip.create_model_and_transforms(model_ref)
        self._tokenizer = open_clip.get_tokenizer(model_ref)
        self._model.to(device=self._device, dtype=self._dtype)
        self._model.eval()
        self.dim = len(self.encode_texts(["dimension probe"])[0])

    def encode_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        embeddings: list[list[float]] = []
        for batch in _batch(texts, self._query_batch_size):
            tokens = self._tokenizer(batch)
            tokens = _move_to_device(tokens, self._device)
            with torch.inference_mode():
                with self._autocast_context():
                    encoded = self._model.encode_text(tokens, normalize=True)
            embeddings.extend(encoded.float().cpu().tolist())
        return embeddings

    def encode_images(self, images: list[Image.Image]) -> list[list[float]]:
        if not images:
            return []

        embeddings: list[list[float]] = []
        for batch in _batch(images, self._image_batch_size):
            inputs = [self._preprocess(image) for image in batch]
            tensor = torch.stack(inputs).to(device=self._device, dtype=self._dtype)
            with torch.inference_mode():
                with self._autocast_context():
                    encoded = self._model.encode_image(tensor, normalize=True)
            embeddings.extend(encoded.float().cpu().tolist())
        return embeddings

    def _autocast_context(self):
        if self._device == "cuda":
            return torch.autocast(device_type="cuda", dtype=torch.float16)
        return nullcontext()


def _batch(items: list[Any], batch_size: int) -> list[list[Any]]:
    return [items[index : index + batch_size] for index in range(0, len(items), batch_size)]


def _move_to_device(tokens: Any, device: str) -> Any:
    if hasattr(tokens, "to"):
        return tokens.to(device)
    if isinstance(tokens, dict):
        return {key: value.to(device) if hasattr(value, "to") else value for key, value in tokens.items()}
    return tokens
