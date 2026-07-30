from __future__ import annotations

import gc
import inspect
import weakref
from collections.abc import Callable

import pytest

import parking_spot_monitor.runtime_detector_capabilities as capabilities


class _NonWeakrefablePathDetector:
    __slots__ = ()

    def detect(
        self,
        frame_path: object,
        *,
        confidence_threshold: float | None = None,
        inference_image_size: int | None = None,
    ) -> list[object]:
        return []


class _UnhashablePathDetector:
    __hash__ = None

    def detect(
        self,
        frame_path: object,
        *,
        confidence_threshold: float | None = None,
        inference_image_size: int | None = None,
    ) -> list[object]:
        return []


def _record_signature_calls(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    real_signature = capabilities.inspect.signature
    calls = [0]

    def counting_signature(callable_object: object) -> object:
        calls[0] += 1
        return real_signature(callable_object)

    monkeypatch.setattr(capabilities.inspect, "signature", counting_signature)
    return calls


def test_path_capability_cache_releases_dead_detector_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Detector:
        def detect(
            self,
            frame_path: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[object]:
            return []

    calls = _record_signature_calls(monkeypatch)
    gc.collect()
    cache_size_before = len(capabilities._DETECT_CAPABILITY_CACHE)
    detector = Detector()
    detector_reference = weakref.ref(detector)

    assert capabilities.detect_accepts_inference_image_size(detector) is True
    assert capabilities.detect_accepts_inference_image_size(detector) is True
    assert calls[0] == 1
    assert len(capabilities._DETECT_CAPABILITY_CACHE) == cache_size_before + 1

    del detector
    gc.collect()
    assert detector_reference() is None
    assert len(capabilities._DETECT_CAPABILITY_CACHE) == cache_size_before


@pytest.mark.parametrize(
    ("detector_type", "expected_signature_calls"),
    [
        pytest.param(_NonWeakrefablePathDetector, 2, id="non-weakrefable-recomputes"),
        pytest.param(_UnhashablePathDetector, 1, id="weakrefable-unhashable-caches"),
    ],
)
def test_path_capability_cache_handles_detector_object_constraints(
    monkeypatch: pytest.MonkeyPatch,
    detector_type: Callable[[], object],
    expected_signature_calls: int,
) -> None:
    calls = _record_signature_calls(monkeypatch)
    detector = detector_type()

    assert capabilities.detect_accepts_inference_image_size(detector) is True
    assert capabilities.detect_accepts_inference_image_size(detector) is True
    assert calls[0] == expected_signature_calls


def test_path_capability_invalidates_when_class_method_changes() -> None:
    class Detector:
        def detect(
            self,
            frame_path: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[object]:
            return []

    detector = Detector()
    assert capabilities.detect_accepts_inference_image_size(detector) is True

    def detect_without_image_size(
        self: Detector,
        frame_path: object,
        *,
        confidence_threshold: float | None = None,
    ) -> list[object]:
        return []

    Detector.detect = detect_without_image_size  # type: ignore[assignment]
    assert capabilities.detect_accepts_inference_image_size(detector) is False


def test_in_memory_capability_invalidates_when_class_method_changes() -> None:
    class Detector:
        def detect_image(self, image: object) -> list[object]:
            return []

    detector = Detector()
    assert capabilities.compatible_detect_image(detector) is None

    def compatible_detect_image(
        self: Detector,
        image: object,
        *,
        confidence_threshold: float | None = None,
        inference_image_size: int | None = None,
    ) -> list[object]:
        return []

    Detector.detect_image = compatible_detect_image  # type: ignore[assignment]
    assert capabilities.compatible_detect_image(detector) is not None


@pytest.mark.parametrize(
    "removed_keyword",
    [pytest.param("confidence_threshold"), pytest.param("inference_image_size")],
)
def test_in_memory_capability_invalidates_after_in_place_code_mutation(
    removed_keyword: str,
) -> None:
    class Detector:
        def detect_image(
            self,
            image: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[object]:
            return []

    def missing_confidence_threshold(
        self: Detector,
        image: object,
        *,
        inference_image_size: int | None = None,
    ) -> list[object]:
        return []

    def missing_inference_image_size(
        self: Detector,
        image: object,
        *,
        confidence_threshold: float | None = None,
    ) -> list[object]:
        return []

    replacement = (
        missing_confidence_threshold
        if removed_keyword == "confidence_threshold"
        else missing_inference_image_size
    )
    detector = Detector()
    assert capabilities.compatible_detect_image(detector) is not None

    Detector.detect_image.__code__ = replacement.__code__
    Detector.detect_image.__kwdefaults__ = replacement.__kwdefaults__
    assert capabilities.compatible_detect_image(detector) is None


@pytest.mark.parametrize(
    "missing_keyword",
    [pytest.param("confidence_threshold"), pytest.param("inference_image_size")],
)
def test_in_memory_capability_requires_both_named_keywords(missing_keyword: str) -> None:
    if missing_keyword == "confidence_threshold":
        class Detector:
            def detect_image(
                self,
                image: object,
                *,
                inference_image_size: int | None = None,
            ) -> list[object]:
                return []
    else:
        class Detector:  # type: ignore[no-redef]
            def detect_image(
                self,
                image: object,
                *,
                confidence_threshold: float | None = None,
            ) -> list[object]:
                return []

    assert capabilities.compatible_detect_image(Detector()) is None


def test_in_memory_capability_accepts_var_keyword_signature() -> None:
    class Detector:
        def detect_image(self, image: object, **kwargs: object) -> list[object]:
            return []

    assert capabilities.compatible_detect_image(Detector()) is not None


@pytest.mark.parametrize(
    "default_kind",
    [pytest.param("positional"), pytest.param("keyword")],
)
def test_in_memory_capability_invalidates_when_default_presence_changes(
    default_kind: str,
) -> None:
    if default_kind == "positional":
        class Detector:
            def detect_image(
                self,
                image: object,
                optional_mode: str = "normal",
                *,
                confidence_threshold: float | None = None,
                inference_image_size: int | None = None,
            ) -> list[object]:
                return []

        def remove_default() -> None:
            Detector.detect_image.__defaults__ = ()
    else:
        class Detector:  # type: ignore[no-redef]
            def detect_image(
                self,
                image: object,
                *,
                confidence_threshold: float | None = None,
                inference_image_size: int | None = None,
                optional_mode: str = "normal",
            ) -> list[object]:
                return []

        def remove_default() -> None:  # type: ignore[no-redef]
            assert Detector.detect_image.__kwdefaults__ is not None
            del Detector.detect_image.__kwdefaults__["optional_mode"]

    detector = Detector()
    assert capabilities.compatible_detect_image(detector) is not None
    remove_default()
    assert capabilities.compatible_detect_image(detector) is None


def test_in_memory_capability_recomputes_custom_signature() -> None:
    class Detector:
        def detect_image(
            self,
            image: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[object]:
            return []

    def missing_inference_image_size(
        self: Detector,
        image: object,
        *,
        confidence_threshold: float | None = None,
    ) -> list[object]:
        return []

    detector = Detector()
    assert capabilities.compatible_detect_image(detector) is not None
    Detector.detect_image.__signature__ = inspect.signature(  # type: ignore[attr-defined]
        missing_inference_image_size
    )
    assert capabilities.compatible_detect_image(detector) is None


@pytest.mark.parametrize(
    "descriptor_kind",
    [pytest.param("staticmethod"), pytest.param("classmethod")],
)
def test_descriptor_capability_caches_and_invalidates(
    monkeypatch: pytest.MonkeyPatch,
    descriptor_kind: str,
) -> None:
    if descriptor_kind == "staticmethod":
        class Detector:
            @staticmethod
            def detect_image(
                image: object,
                *,
                confidence_threshold: float | None = None,
                inference_image_size: int | None = None,
            ) -> list[object]:
                return []
    else:
        class Detector:  # type: ignore[no-redef]
            @classmethod
            def detect_image(
                cls,
                image: object,
                *,
                confidence_threshold: float | None = None,
                inference_image_size: int | None = None,
            ) -> list[object]:
                return []

    calls = _record_signature_calls(monkeypatch)
    detector = Detector()
    assert capabilities.compatible_detect_image(detector) is not None
    assert capabilities.compatible_detect_image(detector) is not None
    assert calls[0] == 1

    if descriptor_kind == "staticmethod":
        def incompatible_detect_image(
            image: object,
            *,
            confidence_threshold: float | None = None,
        ) -> list[object]:
            return []

        Detector.detect_image = staticmethod(incompatible_detect_image)  # type: ignore[method-assign]
    else:
        def incompatible_detect_image(  # type: ignore[no-redef]
            cls: type[object],
            image: object,
            *,
            inference_image_size: int | None = None,
        ) -> list[object]:
            return []

        Detector.detect_image = classmethod(incompatible_detect_image)  # type: ignore[method-assign]

    assert capabilities.compatible_detect_image(detector) is None
    assert calls[0] == 2


def test_in_memory_capability_recomputes_custom_getattribute_callable() -> None:
    class SwitchingDetector:
        def __init__(self) -> None:
            self.compatible = True

        def __getattribute__(self, name: str) -> object:
            if name == "detect_image":
                target = (
                    "_compatible_detect_image"
                    if object.__getattribute__(self, "compatible")
                    else "_incompatible_detect_image"
                )
                return object.__getattribute__(self, target)
            return object.__getattribute__(self, name)

        def detect_image(self, image: object, **kwargs: object) -> list[object]:
            raise AssertionError("effective callable routing was bypassed")

        def _compatible_detect_image(
            self,
            image: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[object]:
            return []

        def _incompatible_detect_image(self, image: object) -> list[object]:
            return []

    detector = SwitchingDetector()
    assert capabilities.compatible_detect_image(detector) is not None
    detector.compatible = False
    assert capabilities.compatible_detect_image(detector) is None


def test_callable_fingerprint_does_not_retain_replaced_method_closure() -> None:
    class Detector:
        def detect(self, frame_path: object, **kwargs: object) -> list[object]:
            return []

    detector = Detector()
    detector_reference = weakref.ref(detector)

    def build_capturing_detect(captured: Detector) -> object:
        def capturing_detect(
            self: Detector,
            frame_path: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[object]:
            assert captured is not None
            return []

        return capturing_detect

    capturing_detect = build_capturing_detect(detector)
    Detector.detect = capturing_detect  # type: ignore[assignment]
    gc.collect()
    cache_size_before = len(capabilities._DETECT_CAPABILITY_CACHE)
    assert capabilities.detect_accepts_inference_image_size(detector) is True
    assert len(capabilities._DETECT_CAPABILITY_CACHE) == cache_size_before + 1

    def replacement_detect(
        self: Detector,
        frame_path: object,
        *,
        confidence_threshold: float | None = None,
    ) -> list[object]:
        return []

    Detector.detect = replacement_detect  # type: ignore[assignment]
    del capturing_detect
    del detector
    gc.collect()
    assert detector_reference() is None
    assert len(capabilities._DETECT_CAPABILITY_CACHE) == cache_size_before


def test_path_capability_cache_uses_identity_not_custom_equality() -> None:
    class EqualDetector:
        def __eq__(self, other: object) -> bool:
            return isinstance(other, EqualDetector)

        def __hash__(self) -> int:
            return 1

    class WithImageSize(EqualDetector):
        def detect(
            self,
            frame_path: object,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[object]:
            return []

    class WithoutImageSize(EqualDetector):
        def detect(
            self,
            frame_path: object,
            *,
            confidence_threshold: float | None = None,
        ) -> list[object]:
            return []

    with_image_size = WithImageSize()
    without_image_size = WithoutImageSize()
    with_reference = weakref.ref(with_image_size)
    without_reference = weakref.ref(without_image_size)

    assert capabilities.detect_accepts_inference_image_size(with_image_size) is True
    assert capabilities.detect_accepts_inference_image_size(without_image_size) is False
    assert with_reference() is with_image_size
    assert without_reference() is without_image_size
    assert id(with_image_size) in capabilities._DETECT_CAPABILITY_CACHE
    assert id(without_image_size) in capabilities._DETECT_CAPABILITY_CACHE
