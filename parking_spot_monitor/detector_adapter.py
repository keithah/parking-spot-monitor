from __future__ import annotations

import inspect
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from parking_spot_monitor.detection import VehicleDetection


class DetectorRunner(Protocol):
    def detect_path(
        self,
        frame_path: Path,
        *,
        confidence_threshold: float,
        inference_image_size: int | None,
    ) -> list[VehicleDetection]: ...

    def detect_image_if_supported(
        self,
        image: object,
        *,
        confidence_threshold: float,
        inference_image_size: int | None,
    ) -> list[VehicleDetection] | None: ...


@dataclass(frozen=True, slots=True)
class DetectorCallPlan:
    path_accepts_image_size: bool
    image_method: Callable[..., object] | None


class DetectorAdapter:
    """Construction-time call plan for legacy and modern detector objects."""

    def __init__(self, detector: object) -> None:
        path_method = getattr(detector, "detect")
        if not callable(path_method):
            raise TypeError("detector.detect must be callable")
        self.raw = detector
        self._path_method = path_method
        self._plan = DetectorCallPlan(
            path_accepts_image_size=_accepts_keyword(path_method, "inference_image_size"),
            image_method=_compatible_image_method(detector),
        )

    def detect_path(
        self,
        frame_path: Path,
        *,
        confidence_threshold: float,
        inference_image_size: int | None,
    ) -> list[VehicleDetection]:
        kwargs: dict[str, object] = {"confidence_threshold": confidence_threshold}
        if self._plan.path_accepts_image_size:
            kwargs["inference_image_size"] = inference_image_size
        return list(self._path_method(frame_path, **kwargs))

    def detect_image_if_supported(
        self,
        image: object,
        *,
        confidence_threshold: float,
        inference_image_size: int | None,
    ) -> list[VehicleDetection] | None:
        image_method = self._plan.image_method
        if image_method is None:
            return None
        return list(
            image_method(
                image,
                confidence_threshold=confidence_threshold,
                inference_image_size=inference_image_size,
            )
        )


class SharedLazyDetector:
    """Own one lazily constructed detector adapter across runtime consumers."""

    def __init__(self, factory: Callable[[], object]) -> None:
        self._factory = factory
        self._lock = threading.Lock()
        self._adapter: DetectorAdapter | None = None

    @property
    def loaded(self) -> bool:
        return self._adapter is not None

    def _get(self) -> DetectorAdapter:
        adapter = self._adapter
        if adapter is None:
            with self._lock:
                adapter = self._adapter
                if adapter is None:
                    adapter = adapt_detector(self._factory())
                    self._adapter = adapter
        return adapter

    def detect_path(
        self,
        frame_path: Path,
        *,
        confidence_threshold: float,
        inference_image_size: int | None,
    ) -> list[VehicleDetection]:
        return self._get().detect_path(
            frame_path,
            confidence_threshold=confidence_threshold,
            inference_image_size=inference_image_size,
        )

    def detect_image_if_supported(
        self,
        image: object,
        *,
        confidence_threshold: float,
        inference_image_size: int | None,
    ) -> list[VehicleDetection] | None:
        return self._get().detect_image_if_supported(
            image,
            confidence_threshold=confidence_threshold,
            inference_image_size=inference_image_size,
        )


def adapt_detector(detector: object) -> DetectorAdapter:
    if isinstance(detector, DetectorAdapter):
        return detector
    return DetectorAdapter(detector)


def _accepts_keyword(candidate: Callable[..., Any], keyword: str) -> bool:
    try:
        signature = inspect.signature(candidate)
    except (TypeError, ValueError):
        return False
    parameter = signature.parameters.get(keyword)
    if parameter is not None and parameter.kind in {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    }:
        return True
    return any(
        item.kind is inspect.Parameter.VAR_KEYWORD
        for item in signature.parameters.values()
    )


def _compatible_image_method(detector: object) -> Callable[..., object] | None:
    candidate = getattr(detector, "detect_image", None)
    if not callable(candidate):
        return None
    try:
        inspect.signature(candidate).bind(
            object(),
            confidence_threshold=None,
            inference_image_size=None,
        )
    except (TypeError, ValueError):
        return None
    return candidate
