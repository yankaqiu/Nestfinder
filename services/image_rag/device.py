from __future__ import annotations

from dataclasses import dataclass

try:
    import torch
except Exception:  # pragma: no cover - exercised in tests with stubs
    torch = None  # type: ignore[assignment]


DEFAULT_GPU_MODEL = "timm/ViT-B-16-SigLIP2"
DEFAULT_CPU_MODEL = "timm/ViT-B-16-SigLIP-i18n-256"


@dataclass(slots=True)
class DeviceInfo:
    selected: str
    cuda_available: bool
    mps_available: bool
    mps_reason: str | None
    gpu_available: bool


def detect_device(requested: str) -> DeviceInfo:
    requested_normalized = requested.strip().lower()
    torch_module = torch

    cuda_available = bool(torch_module and torch_module.cuda.is_available())
    mps_built = bool(torch_module and hasattr(torch_module.backends, "mps") and torch_module.backends.mps.is_built())
    mps_available = bool(torch_module and hasattr(torch_module.backends, "mps") and torch_module.backends.mps.is_available())

    mps_reason: str | None = None
    if not mps_available:
        if not torch_module:
            mps_reason = "PyTorch is not installed."
        elif not mps_built:
            mps_reason = "MPS not available because the current PyTorch install was not built with MPS enabled."
        else:
            mps_reason = "MPS not available because the current macOS version is not 12.3+ and/or this machine does not have an MPS-enabled device."

    if requested_normalized == "auto":
        if cuda_available:
            selected = "cuda"
        elif mps_available:
            selected = "mps"
        else:
            selected = "cpu"
    else:
        if requested_normalized == "cuda" and not cuda_available:
            raise RuntimeError("IMAGE_RAG_DEVICE=cuda was requested, but CUDA is not available.")
        if requested_normalized == "mps" and not mps_available:
            raise RuntimeError(mps_reason or "IMAGE_RAG_DEVICE=mps was requested, but MPS is not available.")
        if requested_normalized not in {"cpu", "cuda", "mps"}:
            raise RuntimeError(f"Unsupported IMAGE_RAG_DEVICE value: {requested}")
        selected = requested_normalized

    return DeviceInfo(
        selected=selected,
        cuda_available=cuda_available,
        mps_available=mps_available,
        mps_reason=mps_reason,
        gpu_available=selected in {"cuda", "mps"},
    )


def resolve_model_name(requested_model: str, device_info: DeviceInfo) -> str:
    if requested_model.strip().lower() != "auto":
        return requested_model
    if device_info.selected in {"cuda", "mps"}:
        return DEFAULT_GPU_MODEL
    return DEFAULT_CPU_MODEL
