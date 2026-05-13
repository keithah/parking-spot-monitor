from __future__ import annotations

from dataclasses import dataclass

import pytest

from parking_spot_monitor.vehicle_estimates import estimate_vehicle_history


@dataclass(frozen=True)
class Session:
    profile_id: str | None
    profile_confidence: float | None
    duration_seconds: object
    ended_at: str | None


def session(
    *,
    profile_id: str | None = "prof-car",
    profile_confidence: float | None = 0.95,
    duration_seconds: object = 3600,
    ended_at: str | None = "2026-05-18T17:30:00Z",
) -> Session:
    return Session(
        profile_id=profile_id,
        profile_confidence=profile_confidence,
        duration_seconds=duration_seconds,
        ended_at=ended_at,
    )


def test_estimates_dwell_and_leave_window_from_qualifying_closed_sessions() -> None:
    result = estimate_vehicle_history(
        "prof-car",
        [
            session(duration_seconds=3300, ended_at="2026-05-18T17:20:00Z"),
            session(duration_seconds=3600, ended_at="2026-05-19T17:35:00Z"),
            session(duration_seconds=3900, ended_at="2026-05-20T17:45:00Z"),
            session(duration_seconds=4200, ended_at="2026-05-21T17:30:00Z"),
            session(profile_id="prof-other", duration_seconds=30_000, ended_at="2026-05-21T02:00:00Z"),
        ],
    )

    assert result.status == "estimated"
    assert result.reason is None
    assert result.profile_id == "prof-car"
    assert result.sample_count == 4
    assert result.confidence == "medium"
    assert result.dwell_range is not None
    assert result.dwell_range.lower_seconds <= 3300
    assert result.dwell_range.upper_seconds >= 4200
    assert result.dwell_range.typical_seconds == 3600
    assert result.leave_time_window is not None
    assert result.leave_time_window.start_minute <= 17 * 60 + 20
    assert result.leave_time_window.end_minute >= 17 * 60 + 45
    assert result.leave_time_window.crosses_midnight is False


@pytest.mark.parametrize("profile_id", [None, "", "   "])
def test_unknown_profile_id_returns_stable_no_estimate(profile_id: str | None) -> None:
    result = estimate_vehicle_history(profile_id, [session()])

    assert result.status == "insufficient_history"
    assert result.reason == "unknown-profile"
    assert result.profile_id is None
    assert result.sample_count == 0
    assert result.confidence == "low"
    assert result.dwell_range is None
    assert result.leave_time_window is None


def test_one_qualifying_sample_is_insufficient() -> None:
    result = estimate_vehicle_history("prof-car", [session(duration_seconds=1800)])

    assert result.status == "insufficient_history"
    assert result.reason == "insufficient-samples"
    assert result.sample_count == 1
    assert result.dwell_range is None
    assert result.leave_time_window is None


def test_invalid_history_is_excluded_without_exceptions() -> None:
    result = estimate_vehicle_history(
        "prof-car",
        [
            session(duration_seconds=None),
            session(duration_seconds=-1),
            session(duration_seconds=1.5),
            session(ended_at=None),
            session(ended_at="not a timestamp"),
        ],
    )

    assert result.status == "insufficient_history"
    assert result.reason == "invalid-history"
    assert result.sample_count == 0


def test_low_or_non_finite_confidence_and_mismatched_profile_are_excluded() -> None:
    result = estimate_vehicle_history(
        "prof-car",
        [
            session(profile_confidence=0.75),
            session(profile_confidence=float("nan")),
            session(profile_confidence=float("inf")),
            session(profile_id="prof-other"),
            session(duration_seconds=2400, ended_at="2026-05-18T08:00:00Z"),
            session(duration_seconds=2700, ended_at="2026-05-19T08:15:00Z"),
        ],
    )

    assert result.status == "estimated"
    assert result.sample_count == 2
    assert result.dwell_range is not None
    assert result.dwell_range.lower_seconds <= 2400
    assert result.dwell_range.upper_seconds >= 2700


def test_high_variance_history_fails_closed() -> None:
    result = estimate_vehicle_history(
        "prof-car",
        [
            session(duration_seconds=600, ended_at="2026-05-18T08:00:00Z"),
            session(duration_seconds=720, ended_at="2026-05-19T08:10:00Z"),
            session(duration_seconds=40_000, ended_at="2026-05-20T22:30:00Z"),
        ],
    )

    assert result.status == "insufficient_history"
    assert result.reason == "high-variance"
    assert result.sample_count == 3
    assert result.dwell_range is None
    assert result.leave_time_window is None


def test_leave_time_window_handles_midnight_crossing() -> None:
    result = estimate_vehicle_history(
        "prof-car",
        [
            session(duration_seconds=1800, ended_at="2026-05-18T23:50:00Z"),
            session(duration_seconds=2100, ended_at="2026-05-19T00:05:00Z"),
            session(duration_seconds=2400, ended_at="2026-05-20T00:15:00Z"),
        ],
    )

    assert result.status == "estimated"
    assert result.leave_time_window is not None
    assert result.leave_time_window.crosses_midnight is True
    assert result.leave_time_window.start_minute >= 23 * 60
    assert result.leave_time_window.end_minute <= 15
