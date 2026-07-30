from __future__ import annotations

import inspect
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import parking_spot_monitor.detector_adapter as detector_adapter
from parking_spot_monitor.detector_adapter import SharedLazyDetector, adapt_detector
from parking_spot_monitor.detection import VehicleDetection


DETECTION = VehicleDetection(class_name="car", confidence=0.9, bbox=(1, 2, 3, 4))


def test_adapter_inspects_legacy_signature_only_during_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signature_calls = 0
    real_signature = inspect.signature

    def counted(value: object) -> inspect.Signature:
        nonlocal signature_calls
        signature_calls += 1
        return real_signature(value)

    monkeypatch.setattr(detector_adapter.inspect, "signature", counted)

    class LegacyDetector:
        def __init__(self) -> None:
            self.calls: list[tuple[Path, float | None]] = []

        def detect(
            self,
            frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
        ) -> list[VehicleDetection]:
            self.calls.append((Path(frame_path), confidence_threshold))
            return [DETECTION]

    raw = LegacyDetector()
    adapter = adapt_detector(raw)
    construction_signature_calls = signature_calls

    assert adapter.detect_path(
        Path("one.jpg"),
        confidence_threshold=0.1,
        inference_image_size=640,
    ) == [DETECTION]
    assert adapter.detect_path(
        Path("two.jpg"),
        confidence_threshold=0.2,
        inference_image_size=320,
    ) == [DETECTION]
    assert raw.calls == [(Path("one.jpg"), 0.1), (Path("two.jpg"), 0.2)]
    assert construction_signature_calls == 1
    assert signature_calls == construction_signature_calls


def test_adapter_uses_compatible_in_memory_detector_without_path_fallback() -> None:
    class ModernDetector:
        def detect(self, _frame_path: str | Path, **_kwargs: object) -> list[VehicleDetection]:
            raise AssertionError("path fallback should not run")

        def detect_image(
            self,
            image: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            assert image == "decoded-image"
            assert confidence_threshold == 0.2
            assert inference_image_size == 320
            return [DETECTION]

    adapter = adapt_detector(ModernDetector())

    assert adapter.detect_image_if_supported(
        "decoded-image",
        confidence_threshold=0.2,
        inference_image_size=320,
    ) == [DETECTION]


def test_adapter_returns_none_for_incompatible_in_memory_method() -> None:
    class LegacyDetector:
        __slots__ = ()

        def detect(self, _frame_path: str | Path, **_kwargs: object) -> list[VehicleDetection]:
            return [DETECTION]

        def detect_image(self, _image: object) -> list[VehicleDetection]:
            raise AssertionError("incompatible image method should not run")

    adapter = adapt_detector(LegacyDetector())

    assert adapter.detect_image_if_supported(
        "decoded-image",
        confidence_threshold=0.2,
        inference_image_size=320,
    ) is None


@pytest.mark.parametrize("method_name", ["detect", "detect_image"])
def test_adapter_does_not_retry_internal_type_error(method_name: str) -> None:
    class FailingDetector:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            self.calls.append("detect")
            raise TypeError("internal path bug")

        def detect_image(
            self,
            _image: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            self.calls.append("detect_image")
            raise TypeError("internal image bug")

    raw = FailingDetector()
    adapter = adapt_detector(raw)

    with pytest.raises(TypeError, match=f"internal {'path' if method_name == 'detect' else 'image'} bug"):
        if method_name == "detect":
            adapter.detect_path(Path("frame.jpg"), confidence_threshold=0.1, inference_image_size=640)
        else:
            adapter.detect_image_if_supported("image", confidence_threshold=0.1, inference_image_size=640)

    assert raw.calls == [method_name]


def test_shared_lazy_detector_constructs_one_backend_across_threads() -> None:
    constructed: list[object] = []
    construction_started = threading.Event()
    release_construction = threading.Event()

    class Detector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            return [DETECTION]

    def factory() -> Detector:
        construction_started.set()
        assert release_construction.wait(2)
        backend = Detector()
        constructed.append(backend)
        return backend

    shared = SharedLazyDetector(factory)
    assert shared.loaded is False

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                shared.detect_path,
                Path(name),
                confidence_threshold=0.1,
                inference_image_size=640,
            )
            for name in ("runtime.jpg", "incident.jpg")
        ]
        assert construction_started.wait(2)
        release_construction.set()
        assert [future.result(timeout=2) for future in futures] == [[DETECTION], [DETECTION]]

    assert shared.loaded is True
    assert len(constructed) == 1


def test_shared_lazy_detector_retries_factory_after_failed_load() -> None:
    attempts = 0

    class Detector:
        def detect(self, _frame_path: str | Path, **_kwargs: object) -> list[VehicleDetection]:
            return [DETECTION]

    def factory() -> Detector:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("load failed")
        return Detector()

    shared = SharedLazyDetector(factory)

    with pytest.raises(RuntimeError, match="load failed"):
        shared.detect_path(Path("first.jpg"), confidence_threshold=0.1, inference_image_size=640)
    assert shared.loaded is False
    assert shared.detect_path(
        Path("second.jpg"),
        confidence_threshold=0.1,
        inference_image_size=640,
    ) == [DETECTION]
    assert attempts == 2
