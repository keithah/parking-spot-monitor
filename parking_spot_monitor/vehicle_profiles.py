from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Sequence

from PIL import Image, UnidentifiedImageError

THUMBNAIL_SIZE = (32, 32)
HASH_SIZE = (8, 8)
RGB_HISTOGRAM_BINS_PER_CHANNEL = 4
RGB_HISTOGRAM_LENGTH = RGB_HISTOGRAM_BINS_PER_CHANNEL**3
HASH_BITS = HASH_SIZE[0] * HASH_SIZE[1]


class VehicleProfileDescriptorError(RuntimeError):
    """Raised when a crop cannot be converted into a safe visual descriptor."""


class MatchStatus(StrEnum):
    """Conservative profile matching outcomes for one occupied crop."""

    MATCHED = "matched"
    NEW_PROFILE = "new_profile"
    UNKNOWN = "unknown"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class VehicleDescriptor:
    """Deterministic, dependency-light visual summary of an occupied crop."""

    width: int
    height: int
    aspect_ratio: float
    rgb_histogram: tuple[float, ...]
    average_hash: int
    hash_bits: int = HASH_BITS


@dataclass(frozen=True)
class VehicleProfileRecord:
    """Stored local visual profile summary used as a match candidate."""

    profile_id: str
    descriptor: VehicleDescriptor
    sample_count: int = 1
    quarantined: bool = False


@dataclass(frozen=True)
class MatchCandidate:
    """Scored candidate profile for diagnostics and conservative selection."""

    profile_id: str
    descriptor: VehicleDescriptor
    distance: float
    confidence: float
    sample_count: int = 1


@dataclass(frozen=True)
class MatchResult:
    """Profile matching result; unknown/ambiguous results do not choose a profile."""

    status: MatchStatus
    profile_id: str | None
    confidence: float
    distance: float | None
    reason: str
    best_candidate: MatchCandidate | None = None
    second_candidate: MatchCandidate | None = None


ProfileLike = VehicleProfileRecord | MatchCandidate


def extract_vehicle_descriptor(crop_path: str | Path) -> VehicleDescriptor:
    """Extract a deterministic local descriptor from a readable JPEG crop.

    Only Pillow is used: the JPEG is converted to RGB, resized to a small fixed
    thumbnail, summarized as a normalized coarse RGB histogram, and hashed with
    an 8x8 average hash. Failures raise messages containing only a sanitized
    basename and a short reason.
    """

    path = Path(crop_path)
    safe_name = _safe_basename(path)
    try:
        with Image.open(path) as opened:
            if opened.format != "JPEG":
                raise VehicleProfileDescriptorError(_error_message(safe_name, "input must be a JPEG"))
            image = opened.convert("RGB")
            width, height = image.size
            if width <= 0 or height <= 0:
                raise VehicleProfileDescriptorError(_error_message(safe_name, "image has invalid dimensions"))
            thumbnail = image.resize(THUMBNAIL_SIZE, Image.Resampling.BILINEAR)
            hash_image = image.resize(HASH_SIZE, Image.Resampling.BILINEAR).convert("L")
    except VehicleProfileDescriptorError:
        raise
    except FileNotFoundError as exc:
        raise VehicleProfileDescriptorError(_error_message(safe_name, "file is missing")) from exc
    except (UnidentifiedImageError, OSError) as exc:
        raise VehicleProfileDescriptorError(_error_message(safe_name, "file is unreadable")) from exc

    histogram = _normalized_rgb_histogram(thumbnail)
    average_hash = _average_hash(hash_image)
    aspect_ratio = _safe_ratio(width, height)
    if not math.isfinite(aspect_ratio):
        raise VehicleProfileDescriptorError(_error_message(safe_name, "image has invalid dimensions"))
    return VehicleDescriptor(
        width=width,
        height=height,
        aspect_ratio=aspect_ratio,
        rgb_histogram=histogram,
        average_hash=average_hash,
    )


def descriptor_distance(left: VehicleDescriptor, right: VehicleDescriptor) -> float:
    """Return a finite normalized distance in [0.0, 1.0] between descriptors."""

    _validate_descriptor(left)
    _validate_descriptor(right)
    histogram_distance = _histogram_l1_distance(left.rgb_histogram, right.rgb_histogram)
    hash_distance = _hash_hamming_distance(left.average_hash, right.average_hash, left.hash_bits, right.hash_bits)
    aspect_distance = _aspect_ratio_distance(left.aspect_ratio, right.aspect_ratio)
    distance = (0.55 * histogram_distance) + (0.35 * hash_distance) + (0.10 * aspect_distance)
    return _clamp_unit(distance)


def match_confidence(distance: float) -> float:
    """Convert a normalized distance to a finite confidence in [0.0, 1.0]."""

    if not math.isfinite(distance):
        return 0.0
    return _clamp_unit(1.0 - distance)


def score_match_candidate(descriptor: VehicleDescriptor, profile: ProfileLike) -> MatchCandidate:
    """Score one profile-like candidate against a descriptor."""

    distance = descriptor_distance(descriptor, profile.descriptor)
    return MatchCandidate(
        profile_id=profile.profile_id,
        descriptor=profile.descriptor,
        distance=distance,
        confidence=match_confidence(distance),
        sample_count=profile.sample_count,
    )


def match_vehicle_profile(
    descriptor: VehicleDescriptor,
    candidates: Sequence[ProfileLike],
    *,
    match_distance_threshold: float = 0.24,
    new_profile_distance_threshold: float = 0.45,
    separation_margin: float = 0.08,
    min_match_confidence: float = 0.76,
) -> MatchResult:
    """Select a local profile conservatively, failing uncertain cases closed.

    Empty candidates request a new profile. A best candidate is matched only when
    it is close enough, confident enough, and sufficiently separated from the
    second-best candidate. Clearly distant crops request a new profile. All
    threshold or margin uncertainty returns an unknown/ambiguous outcome without
    choosing a profile id.
    """

    _validate_descriptor(descriptor)
    thresholds = (match_distance_threshold, new_profile_distance_threshold, separation_margin, min_match_confidence)
    if not all(math.isfinite(value) for value in thresholds):
        raise ValueError("profile match thresholds must be finite")
    if match_distance_threshold < 0 or new_profile_distance_threshold < 0 or separation_margin < 0:
        raise ValueError("profile match thresholds must be non-negative")

    if not candidates:
        return MatchResult(
            status=MatchStatus.NEW_PROFILE,
            profile_id=None,
            confidence=0.0,
            distance=None,
            reason="no-candidates",
        )

    scored = sorted((score_match_candidate(descriptor, profile) for profile in candidates), key=lambda item: item.distance)
    best = scored[0]
    second = scored[1] if len(scored) > 1 else None
    best_separation = math.inf if second is None else second.distance - best.distance

    if best.distance <= match_distance_threshold and best.confidence >= min_match_confidence:
        if best_separation >= separation_margin:
            return MatchResult(
                status=MatchStatus.MATCHED,
                profile_id=best.profile_id,
                confidence=best.confidence,
                distance=best.distance,
                reason="best-candidate-separated",
                best_candidate=best,
                second_candidate=second,
            )
        return MatchResult(
            status=MatchStatus.AMBIGUOUS,
            profile_id=None,
            confidence=best.confidence,
            distance=best.distance,
            reason="best-candidate-not-separated",
            best_candidate=best,
            second_candidate=second,
        )

    if best.distance >= new_profile_distance_threshold:
        return MatchResult(
            status=MatchStatus.NEW_PROFILE,
            profile_id=None,
            confidence=best.confidence,
            distance=best.distance,
            reason="best-candidate-distant",
            best_candidate=best,
            second_candidate=second,
        )

    return MatchResult(
        status=MatchStatus.UNKNOWN,
        profile_id=None,
        confidence=best.confidence,
        distance=best.distance,
        reason="best-candidate-threshold-uncertain",
        best_candidate=best,
        second_candidate=second,
    )


def _normalized_rgb_histogram(image: Image.Image) -> tuple[float, ...]:
    counts = [0] * RGB_HISTOGRAM_LENGTH
    for red, green, blue in image.getdata():
        red_bin = min(RGB_HISTOGRAM_BINS_PER_CHANNEL - 1, red * RGB_HISTOGRAM_BINS_PER_CHANNEL // 256)
        green_bin = min(RGB_HISTOGRAM_BINS_PER_CHANNEL - 1, green * RGB_HISTOGRAM_BINS_PER_CHANNEL // 256)
        blue_bin = min(RGB_HISTOGRAM_BINS_PER_CHANNEL - 1, blue * RGB_HISTOGRAM_BINS_PER_CHANNEL // 256)
        index = (red_bin * RGB_HISTOGRAM_BINS_PER_CHANNEL * RGB_HISTOGRAM_BINS_PER_CHANNEL) + (
            green_bin * RGB_HISTOGRAM_BINS_PER_CHANNEL
        ) + blue_bin
        counts[index] += 1
    total = sum(counts)
    if total <= 0:
        raise VehicleProfileDescriptorError("vehicle descriptor failed: image has no pixels")
    return tuple(count / total for count in counts)


def _average_hash(image: Image.Image) -> int:
    pixels = list(image.getdata())
    if len(pixels) != HASH_BITS:
        raise VehicleProfileDescriptorError("vehicle descriptor failed: invalid hash dimensions")
    average = sum(float(pixel) for pixel in pixels) / len(pixels)
    value = 0
    for pixel in pixels:
        value = (value << 1) | (1 if float(pixel) >= average else 0)
    return value


def _histogram_l1_distance(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    if len(left) != RGB_HISTOGRAM_LENGTH or len(right) != RGB_HISTOGRAM_LENGTH:
        raise ValueError("vehicle descriptor histogram has invalid length")
    distance = sum(abs(a - b) for a, b in zip(left, right, strict=True)) / 2.0
    return _clamp_unit(distance)


def _hash_hamming_distance(left_hash: int, right_hash: int, left_bits: int, right_bits: int) -> float:
    if left_bits <= 0 or right_bits <= 0 or left_bits != right_bits:
        raise ValueError("vehicle descriptor hash has invalid size")
    mask = (1 << left_bits) - 1
    distance = ((left_hash & mask) ^ (right_hash & mask)).bit_count() / left_bits
    return _clamp_unit(distance)


def _aspect_ratio_distance(left: float, right: float) -> float:
    if not math.isfinite(left) or not math.isfinite(right) or left <= 0 or right <= 0:
        raise ValueError("vehicle descriptor aspect ratio must be finite and positive")
    return _clamp_unit(abs(left - right) / max(left, right))


def _validate_descriptor(descriptor: VehicleDescriptor) -> None:
    if descriptor.width <= 0 or descriptor.height <= 0:
        raise ValueError("vehicle descriptor dimensions must be positive")
    if not math.isfinite(descriptor.aspect_ratio) or descriptor.aspect_ratio <= 0:
        raise ValueError("vehicle descriptor aspect ratio must be finite and positive")
    if len(descriptor.rgb_histogram) != RGB_HISTOGRAM_LENGTH:
        raise ValueError("vehicle descriptor histogram has invalid length")
    if not all(math.isfinite(value) and value >= 0 for value in descriptor.rgb_histogram):
        raise ValueError("vehicle descriptor histogram must contain finite non-negative values")
    histogram_sum = sum(descriptor.rgb_histogram)
    if not math.isfinite(histogram_sum) or histogram_sum <= 0:
        raise ValueError("vehicle descriptor histogram must have a finite positive sum")
    if descriptor.hash_bits <= 0 or descriptor.average_hash < 0:
        raise ValueError("vehicle descriptor hash must be non-negative with positive size")


def _safe_ratio(width: int, height: int) -> float:
    if height <= 0:
        return math.inf
    return width / height


def _clamp_unit(value: float) -> float:
    if not math.isfinite(value):
        return 0.0 if value < 0 else 1.0
    return max(0.0, min(1.0, value))


def _safe_basename(path: Path) -> str:
    name = path.name or "<unknown>"
    redacted = re.sub(r"(?i)(access[_-]?token|token|secret|password|apikey|api[_-]?key|key)=?[^._-]*", r"\1=<redacted>", name)
    redacted = re.sub(r"[A-Za-z0-9_-]{24,}", "<redacted>", redacted)
    return redacted[:96]


def _error_message(safe_name: str, reason: str) -> str:
    return f"vehicle descriptor failed for {safe_name!r}: {reason}"
