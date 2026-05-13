from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Protocol

STATUS_ESTIMATED = "estimated"
STATUS_INSUFFICIENT_HISTORY = "insufficient_history"

REASON_UNKNOWN_PROFILE = "unknown-profile"
REASON_INSUFFICIENT_SAMPLES = "insufficient-samples"
REASON_INVALID_HISTORY = "invalid-history"
REASON_HIGH_VARIANCE = "high-variance"

CONFIDENCE_LOW = "low"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_HIGH = "high"

_MINUTES_PER_DAY = 24 * 60
_DWELL_BUCKET_SECONDS = 300
_LEAVE_BUCKET_MINUTES = 15
_MAX_DWELL_RANGE_SECONDS = 8 * 60 * 60
_MAX_DWELL_RELATIVE_SPREAD = 2.5
_MAX_LEAVE_WINDOW_MINUTES = 6 * 60


class VehicleHistorySessionLike(Protocol):
    """Metadata-only vehicle-history fields consumed by the pure estimator."""

    profile_id: str | None
    profile_confidence: float | None
    duration_seconds: int | None
    ended_at: str | None


@dataclass(frozen=True)
class DwellRange:
    """Estimated dwell range in seconds, with a typical point estimate."""

    lower_seconds: int
    upper_seconds: int
    typical_seconds: int


@dataclass(frozen=True)
class LeaveTimeWindow:
    """Estimated leave-time window as minutes since local/recorded-day midnight."""

    start_minute: int
    end_minute: int
    typical_minute: int
    crosses_midnight: bool


@dataclass(frozen=True)
class VehicleHistoryEstimate:
    """Stable S05-ready result shape for repeat-vehicle history estimates."""

    status: str
    reason: str | None
    profile_id: str | None
    sample_count: int
    confidence: str
    dwell_range: DwellRange | None
    leave_time_window: LeaveTimeWindow | None


@dataclass(frozen=True)
class _Sample:
    duration_seconds: int
    leave_minute: int


def estimate_vehicle_history(
    profile_id: str | None,
    sessions: Iterable[VehicleHistorySessionLike],
    *,
    min_samples: int = 2,
    min_profile_confidence: float = 0.76,
) -> VehicleHistoryEstimate:
    """Estimate dwell and usual leave time from already-loaded closed sessions.

    The function is intentionally pure and metadata-only: it reads no archive
    files, image paths, crop bytes, OCR text, RTSP URLs, tokens, and emits no
    logs. Malformed or weak history is excluded from qualifying evidence and
    represented through stable no-estimate status/reason values.
    """

    normalized_profile_id = _normalize_profile_id(profile_id)
    if normalized_profile_id is None:
        return _no_estimate(None, REASON_UNKNOWN_PROFILE, sample_count=0)

    threshold = float(min_profile_confidence)
    if not math.isfinite(threshold):
        threshold = 1.0
    threshold = max(0.0, threshold)
    required_samples = max(1, min_samples)

    candidate_count = 0
    invalid_candidate_count = 0
    samples: list[_Sample] = []
    for session in sessions:
        if getattr(session, "profile_id", None) != normalized_profile_id:
            continue
        candidate_count += 1
        sample = _qualifying_sample(session, min_profile_confidence=threshold)
        if sample is None:
            invalid_candidate_count += 1
            continue
        samples.append(sample)

    sample_count = len(samples)
    if sample_count < required_samples:
        reason = REASON_INVALID_HISTORY if candidate_count > 0 and sample_count == 0 and invalid_candidate_count > 0 else REASON_INSUFFICIENT_SAMPLES
        return _no_estimate(normalized_profile_id, reason, sample_count=sample_count)

    durations = sorted(sample.duration_seconds for sample in samples)
    leave_minutes = sorted(sample.leave_minute for sample in samples)
    dwell_spread = durations[-1] - durations[0]
    leave_start, leave_end, leave_width = _minimal_circular_window(leave_minutes)
    typical_duration_raw = _median(durations)

    if _has_high_variance(durations, dwell_spread=dwell_spread, typical_duration=typical_duration_raw, leave_width=leave_width):
        return _no_estimate(normalized_profile_id, REASON_HIGH_VARIANCE, sample_count=sample_count)

    dwell_range = _dwell_range(durations)
    leave_time_window = _leave_time_window(leave_start, leave_end, leave_width)
    return VehicleHistoryEstimate(
        status=STATUS_ESTIMATED,
        reason=None,
        profile_id=normalized_profile_id,
        sample_count=sample_count,
        confidence=_confidence(sample_count, dwell_spread=dwell_spread, leave_width=leave_width),
        dwell_range=dwell_range,
        leave_time_window=leave_time_window,
    )


def _qualifying_sample(session: VehicleHistorySessionLike, *, min_profile_confidence: float) -> _Sample | None:
    duration = getattr(session, "duration_seconds", None)
    if isinstance(duration, bool) or not isinstance(duration, int) or duration < 0:
        return None
    ended_at = getattr(session, "ended_at", None)
    if not isinstance(ended_at, str) or _parse_timestamp(ended_at) is None:
        return None
    confidence = getattr(session, "profile_confidence", None)
    if isinstance(confidence, bool) or not isinstance(confidence, int | float):
        return None
    confidence_float = float(confidence)
    if not math.isfinite(confidence_float) or confidence_float < min_profile_confidence:
        return None
    parsed = _parse_timestamp(ended_at)
    if parsed is None:
        return None
    return _Sample(duration_seconds=duration, leave_minute=(parsed.hour * 60) + parsed.minute)


def _normalize_profile_id(profile_id: str | None) -> str | None:
    if not isinstance(profile_id, str):
        return None
    text = profile_id.strip()
    return text or None


def _parse_timestamp(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _dwell_range(durations: list[int]) -> DwellRange:
    if len(durations) <= 4:
        lower_raw = durations[0]
        upper_raw = durations[-1]
    else:
        lower_raw = _percentile(durations, 0.20)
        upper_raw = _percentile(durations, 0.80)
    typical_raw = _median(durations)
    return DwellRange(
        lower_seconds=_floor_bucket(lower_raw, _DWELL_BUCKET_SECONDS),
        upper_seconds=_ceil_bucket(upper_raw, _DWELL_BUCKET_SECONDS),
        typical_seconds=_nearest_bucket(typical_raw, _DWELL_BUCKET_SECONDS),
    )


def _leave_time_window(start_minute: int, end_minute: int, width: int) -> LeaveTimeWindow:
    start = _floor_bucket(start_minute, _LEAVE_BUCKET_MINUTES) % _MINUTES_PER_DAY
    end = _ceil_bucket(end_minute, _LEAVE_BUCKET_MINUTES) % _MINUTES_PER_DAY
    if width == 0:
        end = start
    typical = (start_minute + (width / 2.0)) % _MINUTES_PER_DAY
    typical_minute = _nearest_bucket(typical, _LEAVE_BUCKET_MINUTES) % _MINUTES_PER_DAY
    return LeaveTimeWindow(
        start_minute=start,
        end_minute=end,
        typical_minute=typical_minute,
        crosses_midnight=start > end,
    )


def _minimal_circular_window(minutes: list[int]) -> tuple[int, int, int]:
    if len(minutes) == 1:
        minute = minutes[0]
        return (minute, minute, 0)
    largest_gap = -1
    gap_index = 0
    for index, minute in enumerate(minutes):
        next_minute = minutes[(index + 1) % len(minutes)]
        if index == len(minutes) - 1:
            next_minute += _MINUTES_PER_DAY
        gap = next_minute - minute
        if gap > largest_gap:
            largest_gap = gap
            gap_index = index
    start = minutes[(gap_index + 1) % len(minutes)]
    end = minutes[gap_index]
    if end < start:
        end += _MINUTES_PER_DAY
    return (start, end, end - start)


def _has_high_variance(durations: list[int], *, dwell_spread: int, typical_duration: float, leave_width: int) -> bool:
    if dwell_spread > _MAX_DWELL_RANGE_SECONDS:
        return True
    if typical_duration > 0 and dwell_spread > (typical_duration * _MAX_DWELL_RELATIVE_SPREAD):
        return True
    return leave_width > _MAX_LEAVE_WINDOW_MINUTES


def _confidence(sample_count: int, *, dwell_spread: int, leave_width: int) -> str:
    if sample_count >= 8 and dwell_spread <= 2 * 60 * 60 and leave_width <= 90:
        return CONFIDENCE_HIGH
    if sample_count >= 3 and dwell_spread <= 4 * 60 * 60 and leave_width <= 3 * 60:
        return CONFIDENCE_MEDIUM
    return CONFIDENCE_LOW


def _median(values: list[int]) -> float:
    midpoint = len(values) // 2
    if len(values) % 2 == 1:
        return float(values[midpoint])
    return (values[midpoint - 1] + values[midpoint]) / 2.0


def _percentile(values: list[int], fraction: float) -> float:
    if len(values) == 1:
        return float(values[0])
    position = (len(values) - 1) * fraction
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return float(values[lower_index])
    weight = position - lower_index
    return (values[lower_index] * (1.0 - weight)) + (values[upper_index] * weight)


def _floor_bucket(value: float, bucket: int) -> int:
    return int(math.floor(value / bucket) * bucket)


def _ceil_bucket(value: float, bucket: int) -> int:
    return int(math.ceil(value / bucket) * bucket)


def _nearest_bucket(value: float, bucket: int) -> int:
    return int(round(value / bucket) * bucket)


def _no_estimate(profile_id: str | None, reason: str, *, sample_count: int) -> VehicleHistoryEstimate:
    return VehicleHistoryEstimate(
        status=STATUS_INSUFFICIENT_HISTORY,
        reason=reason,
        profile_id=profile_id,
        sample_count=sample_count,
        confidence=CONFIDENCE_LOW,
        dwell_range=None,
        leave_time_window=None,
    )
