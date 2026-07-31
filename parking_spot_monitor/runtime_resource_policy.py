from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping, Protocol

from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState


RuntimeResourceReason = Literal[
    "adaptive-disabled",
    "degraded",
    "transition-settle",
    "unknown",
    "occupied",
    "partial-streak",
    "weak-presence",
    "settling",
    "stable",
]


class _RuntimeCadenceSettings(Protocol):
    frame_interval_seconds: float
    occupied_frame_interval_seconds: float
    adaptive_polling_enabled: bool
    stable_frame_interval_seconds: float
    stable_settle_frames: int


class _OccupancyThresholdSettings(Protocol):
    confirm_frames: int
    release_frames: int


class RuntimeResourceSettings(Protocol):
    runtime: _RuntimeCadenceSettings
    occupancy: _OccupancyThresholdSettings


class RuntimeOccupancyState(Protocol):
    state_by_spot: Mapping[str, SpotOccupancyState]


@dataclass(frozen=True, slots=True)
class RuntimeResourcePolicyState:
    stable_success_count: int = 0
    last_verification_at: float | None = None
    last_overlay_at: float | None = None


@dataclass(frozen=True, slots=True)
class RuntimeResourceDecision:
    interval_seconds: float
    reason: RuntimeResourceReason
    stable_success_count: int


def decide_runtime_interval(
    settings: RuntimeResourceSettings,
    runtime_state: RuntimeOccupancyState,
    *,
    previous_stable_success_count: int,
    frame_had_transition: bool,
    frame_has_weak_presence: bool,
    degraded: bool,
) -> RuntimeResourceDecision:
    """Choose the next successful-loop cadence from current frame evidence."""

    active_interval = settings.runtime.frame_interval_seconds
    adaptive_enabled = settings.runtime.adaptive_polling_enabled
    if degraded:
        return _active_decision(
            active_interval, "degraded" if adaptive_enabled else "adaptive-disabled"
        )
    if frame_had_transition:
        return _active_decision(
            active_interval,
            "transition-settle" if adaptive_enabled else "adaptive-disabled",
        )

    states = tuple(runtime_state.state_by_spot.values())
    if not states or any(state.status is OccupancyStatus.UNKNOWN for state in states):
        return _active_decision(
            active_interval, "unknown" if adaptive_enabled else "adaptive-disabled"
        )
    if adaptive_enabled and any(
        state.status is OccupancyStatus.OCCUPIED for state in states
    ):
        return _active_decision(
            settings.runtime.occupied_frame_interval_seconds,
            "occupied",
        )
    if any(
        _has_partial_streak(
            state,
            confirm_frames=settings.occupancy.confirm_frames,
            release_frames=settings.occupancy.release_frames,
        )
        for state in states
    ):
        return _active_decision(
            active_interval,
            "partial-streak" if adaptive_enabled else "adaptive-disabled",
        )
    if frame_has_weak_presence:
        return _active_decision(
            active_interval,
            "weak-presence" if adaptive_enabled else "adaptive-disabled",
        )

    settle_frames = settings.runtime.stable_settle_frames
    stable_success_count = max(previous_stable_success_count, 0) + 1
    if not adaptive_enabled:
        return RuntimeResourceDecision(
            interval_seconds=active_interval,
            reason="adaptive-disabled",
            stable_success_count=stable_success_count,
        )
    if stable_success_count < settle_frames:
        return RuntimeResourceDecision(
            interval_seconds=active_interval,
            reason="settling",
            stable_success_count=stable_success_count,
        )
    return RuntimeResourceDecision(
        interval_seconds=settings.runtime.stable_frame_interval_seconds,
        reason="stable",
        stable_success_count=stable_success_count,
    )


def verification_due(
    *,
    now_monotonic: float,
    last_verification_at: float | None,
    interval_seconds: float,
) -> bool:
    """Return whether optional high-resolution verification is due."""

    return _periodic_deadline_due(
        now_monotonic=now_monotonic,
        last_completed_at=last_verification_at,
        interval_seconds=interval_seconds,
    )


def artifact_due(
    *,
    now_monotonic: float,
    last_written_at: float | None,
    interval_seconds: float,
    transition: bool,
) -> bool:
    """Return whether a periodic or transition-forced artifact should be written."""

    return transition or _periodic_deadline_due(
        now_monotonic=now_monotonic,
        last_completed_at=last_written_at,
        interval_seconds=interval_seconds,
    )


def remaining_sleep_seconds(
    *,
    interval_seconds: float,
    iteration_started_at: float,
    now_monotonic: float,
) -> float:
    """Subtract elapsed work from an iteration deadline and clamp the result."""

    bounded_interval = max(interval_seconds, 0.0)
    elapsed = max(now_monotonic - iteration_started_at, 0.0)
    return max(bounded_interval - elapsed, 0.0)


def _active_decision(
    interval_seconds: float,
    reason: RuntimeResourceReason,
) -> RuntimeResourceDecision:
    return RuntimeResourceDecision(
        interval_seconds=interval_seconds,
        reason=reason,
        stable_success_count=0,
    )


def _has_partial_streak(
    state: SpotOccupancyState,
    *,
    confirm_frames: int,
    release_frames: int,
) -> bool:
    return (
        0 < state.hit_streak < confirm_frames
        or 0 < state.miss_streak < release_frames
    )


def _periodic_deadline_due(
    *,
    now_monotonic: float,
    last_completed_at: float | None,
    interval_seconds: float,
) -> bool:
    if interval_seconds <= 0:
        return False
    if last_completed_at is None:
        return True
    return now_monotonic - last_completed_at >= interval_seconds
