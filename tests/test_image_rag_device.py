from services.image_rag.device import detect_device, resolve_model_name


class _FakeMpsBackend:
    def __init__(self, *, built: bool, available: bool) -> None:
        self._built = built
        self._available = available

    def is_built(self) -> bool:
        return self._built

    def is_available(self) -> bool:
        return self._available


class _FakeCudaBackend:
    def __init__(self, *, available: bool) -> None:
        self._available = available

    def is_available(self) -> bool:
        return self._available


class _FakeTorch:
    def __init__(self, *, cuda_available: bool, mps_built: bool, mps_available: bool) -> None:
        self.cuda = _FakeCudaBackend(available=cuda_available)
        self.backends = type("Backends", (), {"mps": _FakeMpsBackend(built=mps_built, available=mps_available)})()


def test_detect_device_prefers_mps_when_cuda_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.image_rag.device.torch",
        _FakeTorch(cuda_available=False, mps_built=True, mps_available=True),
    )

    device_info = detect_device("auto")

    assert device_info.selected == "mps"
    assert device_info.mps_available is True
    assert resolve_model_name("auto", device_info) == "timm/ViT-B-16-SigLIP2"


def test_detect_device_reports_mps_reason_when_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.image_rag.device.torch",
        _FakeTorch(cuda_available=False, mps_built=False, mps_available=False),
    )

    device_info = detect_device("auto")

    assert device_info.selected == "cpu"
    assert device_info.mps_available is False
    assert "not built with mps enabled" in (device_info.mps_reason or "").lower()
    assert resolve_model_name("auto", device_info) == "timm/ViT-B-16-SigLIP-i18n-256"
