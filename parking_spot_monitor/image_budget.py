"""Bounded JPEG encoding with deterministic dimension and quality selection."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from io import BytesIO
from numbers import Real
from typing import Any

from PIL import Image


class ImageBudgetError(ValueError):
    """A safe failure raised when JPEG budget inputs or output are invalid."""


@dataclass(frozen=True, slots=True)
class JpegBudgetResult:
    """An immutable JPEG payload and the selection work used to produce it."""

    data: bytes
    width: int
    height: int
    quality: int
    attempts: int


def encode_jpeg_under_budget(
    image: Image.Image,
    *,
    max_bytes: int,
    initial_max_dimension: int,
    min_dimension: int,
    dimension_scale: float,
    qualities: Iterable[int],
    resampling: Any,
) -> JpegBudgetResult:
    """Encode the largest, highest-quality configured JPEG within ``max_bytes``."""

    _require_positive_integer("max_bytes", max_bytes)
    _require_positive_integer("initial_max_dimension", initial_max_dimension)
    _require_positive_integer("min_dimension", min_dimension)
    if initial_max_dimension < min_dimension:
        raise ImageBudgetError("initial_max_dimension must be greater than or equal to min_dimension")
    if (
        isinstance(dimension_scale, bool)
        or not isinstance(dimension_scale, Real)
        or not math.isfinite(float(dimension_scale))
        or not 0 < dimension_scale < 1
    ):
        raise ImageBudgetError("dimension_scale must be greater than 0 and less than 1")

    normalized_qualities = _normalize_qualities(qualities)
    source_width, source_height = _image_size(image)
    source_max_dimension = max(source_width, source_height)
    current_dimension = min(source_max_dimension, initial_max_dimension)
    minimum_dimension = min(source_max_dimension, min_dimension)
    lowest_quality = normalized_qualities[-1]
    attempts = 0

    with BytesIO() as buffer:
        while True:
            target_size = _bounded_dimensions(source_width, source_height, current_dimension)
            candidate = image if target_size == (source_width, source_height) else image.resize(target_size, resampling)
            try:
                lowest_data = _attempt(candidate, buffer, lowest_quality)
                attempts += 1
                if len(lowest_data) <= max_bytes:
                    quality, data, quality_attempts = _highest_viable_quality(
                        candidate,
                        buffer,
                        normalized_qualities,
                        max_bytes=max_bytes,
                        lowest_data=lowest_data,
                    )
                    attempts += quality_attempts
                    width, height = candidate.size
                    return JpegBudgetResult(
                        data=data,
                        width=width,
                        height=height,
                        quality=quality,
                        attempts=attempts,
                    )
            finally:
                if candidate is not image:
                    candidate.close()

            if current_dimension == minimum_dimension:
                break
            scaled_dimension = int(current_dimension * dimension_scale)
            current_dimension = max(minimum_dimension, min(current_dimension - 1, scaled_dimension))

    raise ImageBudgetError("image could not be encoded under byte budget")


def _highest_viable_quality(
    image: Image.Image,
    buffer: BytesIO,
    qualities: tuple[int, ...],
    *,
    max_bytes: int,
    lowest_data: bytes,
) -> tuple[int, bytes, int]:
    low_index = 0
    high_index = len(qualities) - 1
    best_quality = qualities[high_index]
    best_data = lowest_data
    attempts = 0

    while low_index < high_index:
        middle_index = (low_index + high_index) // 2
        quality = qualities[middle_index]
        data = _attempt(image, buffer, quality)
        attempts += 1
        if len(data) <= max_bytes:
            high_index = middle_index
            best_quality = quality
            best_data = data
        else:
            low_index = middle_index + 1

    return best_quality, best_data, attempts


def _attempt(image: Image.Image, buffer: BytesIO, quality: int) -> bytes:
    buffer.seek(0)
    buffer.truncate(0)
    _encode_jpeg(image, buffer, quality)
    return buffer.getvalue()


def _encode_jpeg(image: Image.Image, buffer: BytesIO, quality: int) -> None:
    image.save(buffer, format="JPEG", quality=quality, optimize=True)


def _normalize_qualities(qualities: Iterable[int]) -> tuple[int, ...]:
    try:
        configured = tuple(qualities)
    except TypeError as exc:
        raise ImageBudgetError("qualities must contain at least one quality") from exc
    if not configured:
        raise ImageBudgetError("qualities must contain at least one quality")
    if any(
        isinstance(quality, bool) or not isinstance(quality, int) or not 1 <= quality <= 100
        for quality in configured
    ):
        raise ImageBudgetError("qualities must contain integers from 1 through 100")
    return tuple(sorted(set(configured), reverse=True))


def _image_size(image: Image.Image) -> tuple[int, int]:
    try:
        width, height = image.size
    except (AttributeError, TypeError, ValueError) as exc:
        raise ImageBudgetError("image dimensions must be positive integers") from exc
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in (width, height)):
        raise ImageBudgetError("image dimensions must be positive integers")
    return width, height


def _require_positive_integer(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ImageBudgetError(f"{name} must be a positive integer")


def _bounded_dimensions(width: int, height: int, max_dimension: int) -> tuple[int, int]:
    scale = min(1.0, max_dimension / max(width, height))
    return max(1, int(width * scale)), max(1, int(height * scale))
