from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from weakref import ReferenceType, ref


@dataclass(frozen=True, slots=True)
class _CallableFingerprint:
    function: ReferenceType[Any]
    code: ReferenceType[Any]
    descriptor_kind: str
    positional_default_count: int
    keyword_default_names: frozenset[str]

    def matches(self, other: _CallableFingerprint) -> bool:
        return (
            self.function() is other.function()
            and self.code() is other.code()
            and self.descriptor_kind == other.descriptor_kind
            and self.positional_default_count == other.positional_default_count
            and self.keyword_default_names == other.keyword_default_names
        )


@dataclass(frozen=True, slots=True)
class _CapabilityEntry:
    detector: ReferenceType[Any]
    callable: _CallableFingerprint
    supported: bool


_CapabilityCache = dict[int, _CapabilityEntry]
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

    fingerprint = _stable_callable_fingerprint(detector, method_name, candidate)
    if fingerprint is None:
        return candidate, predicate(candidate)

    detector_id = id(detector)
    cached = cache.get(detector_id)
    if cached is not None and cached.detector() is detector and cached.callable.matches(fingerprint):
        return candidate, cached.supported

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
    cache[detector_id] = _CapabilityEntry(detector_reference, fingerprint, supported)
    return candidate, supported


def _stable_callable_fingerprint(
    detector: Any,
    method_name: str,
    candidate: Callable[..., Any],
) -> _CallableFingerprint | None:
    try:
        class_descriptor = inspect.getattr_static(type(detector), method_name)
        effective_descriptor = inspect.getattr_static(detector, method_name)
    except AttributeError:
        return None
    if effective_descriptor is not class_descriptor:
        return None

    if isinstance(class_descriptor, staticmethod):
        function = class_descriptor.__func__
        descriptor_kind = "staticmethod"
        if candidate is not function:
            return None
    elif isinstance(class_descriptor, classmethod):
        function = class_descriptor.__func__
        descriptor_kind = "classmethod"
        if not inspect.ismethod(candidate) or candidate.__func__ is not function:
            return None
    elif inspect.isfunction(class_descriptor):
        function = class_descriptor
        descriptor_kind = "method"
        if (
            not inspect.ismethod(candidate)
            or candidate.__func__ is not function
            or candidate.__self__ is not detector
        ):
            return None
    else:
        return None

    if "__signature__" in function.__dict__ or "__wrapped__" in function.__dict__:
        return None
    try:
        function_reference = ref(function)
        code_reference = ref(function.__code__)
    except (AttributeError, TypeError):
        return None
    return _CallableFingerprint(
        function=function_reference,
        code=code_reference,
        descriptor_kind=descriptor_kind,
        positional_default_count=len(function.__defaults__ or ()),
        keyword_default_names=frozenset((function.__kwdefaults__ or {}).keys()),
    )


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
    if cached is not None and cached.detector is expired:
        del cache[detector_id]
