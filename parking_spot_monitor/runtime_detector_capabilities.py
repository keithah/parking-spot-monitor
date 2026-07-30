from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any
from weakref import ReferenceType, ref


_CapabilityCache = dict[int, tuple[ReferenceType[Any], ReferenceType[Any], bool]]
_DETECT_CAPABILITY_CACHE: _CapabilityCache = {}
_DETECT_IMAGE_CAPABILITY_CACHE: _CapabilityCache = {}


def detect_accepts_inference_image_size(detector: Any) -> bool:
    """Return whether the effective path detector accepts the image-size keyword."""

    _detect, supported = _method_capability(
        detector,
        method_name="detect",
        cache=_DETECT_CAPABILITY_CACHE,
        predicate=_accepts_inference_image_size,
    )
    return supported


def compatible_detect_image(detector: Any) -> Callable[..., Any] | None:
    """Return a callable supporting the complete in-memory invocation contract."""

    candidate, supported = _method_capability(
        detector,
        method_name="detect_image",
        cache=_DETECT_IMAGE_CAPABILITY_CACHE,
        predicate=_accepts_in_memory_call,
    )
    return candidate if supported else None


def _method_capability(
    detector: Any,
    *,
    method_name: str,
    cache: _CapabilityCache,
    predicate: Callable[[Callable[..., Any]], bool],
) -> tuple[Callable[..., Any] | None, bool]:
    candidate = getattr(detector, method_name, None)
    if not callable(candidate):
        return None, False

    class_descriptor = _stable_class_descriptor(detector, method_name)
    if class_descriptor is None:
        return candidate, predicate(candidate)
    try:
        fingerprint = ref(class_descriptor)
    except TypeError:
        return candidate, predicate(candidate)

    detector_id = id(detector)
    cached = cache.get(detector_id)
    if cached is not None and cached[0]() is detector and cached[1]() is class_descriptor:
        return candidate, cached[2]

    supported = predicate(candidate)
    try:
        detector_reference = ref(
            detector,
            lambda expired, detector_id=detector_id, cache=cache: _expire_capability(
                cache, detector_id, expired
            ),
        )
    except TypeError:
        return candidate, supported
    cache[detector_id] = (detector_reference, fingerprint, supported)
    return candidate, supported


def _stable_class_descriptor(detector: Any, method_name: str) -> object | None:
    try:
        class_descriptor = inspect.getattr_static(type(detector), method_name)
        effective_descriptor = inspect.getattr_static(detector, method_name)
    except AttributeError:
        return None
    if effective_descriptor is not class_descriptor:
        return None
    if (
        inspect.isfunction(class_descriptor)
        or inspect.ismethoddescriptor(class_descriptor)
        or isinstance(class_descriptor, (classmethod, staticmethod))
    ):
        return class_descriptor
    return None


def _accepts_inference_image_size(candidate: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(candidate)
    except (TypeError, ValueError):
        return False
    parameter = signature.parameters.get("inference_image_size")
    if parameter is not None and parameter.kind in {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    }:
        return True
    return any(
        item.kind is inspect.Parameter.VAR_KEYWORD for item in signature.parameters.values()
    )


def _accepts_in_memory_call(candidate: Callable[..., Any]) -> bool:
    try:
        inspect.signature(candidate).bind(
            object(),
            confidence_threshold=None,
            inference_image_size=None,
        )
    except (TypeError, ValueError):
        return False
    return True


def _expire_capability(
    cache: _CapabilityCache,
    detector_id: int,
    expired: ReferenceType[Any],
) -> None:
    cached = cache.get(detector_id)
    if cached is not None and cached[0] is expired:
        del cache[detector_id]
