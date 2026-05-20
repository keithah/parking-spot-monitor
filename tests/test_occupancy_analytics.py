from __future__ import annotations

from dataclasses import dataclass

import pytest


NOW = "2026-05-20T12:00:00Z"

@dataclass(frozen=True)
class Session:
    session_id: str
    spot_id: str
    started_at: str
    ended_at: str | None
    duration_seconds: int | None


def analyze_occupancy(sessions: list[object], *, window: str = "7d", now: str = NOW, sparse_threshold: int = 3):
    from parking_spot_monitor.occupancy_analytics import analyze_occupancy as real_analyze_occupancy

    return real_analyze_occupancy(
        sessions,
        window=window,
        now=now,
        sparse_threshold=sparse_threshold,
    )


def session(
    session_id: str,
    spot_id: str,
    started_at: str,
    ended_at: str | None,
    duration_seconds: int | None,
) -> Session:
    return Session(
        session_id=session_id,
        spot_id=spot_id,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
    )


def test_spot_metrics_include_closed_and_active_sessions_across_two_spots() -> None:
    result = analyze_occupancy(
        [
            session("a-today", "driveway", "2026-05-20T08:00:00Z", "2026-05-20T10:00:00Z", 7_200),
            session("a-active", "driveway", "2026-05-20T11:00:00Z", None, None),
            session("b-yesterday", "curb", "2026-05-19T09:00:00Z", "2026-05-19T10:30:00Z", 5_400),
            session("b-old", "curb", "2026-05-12T12:00:00Z", "2026-05-12T13:00:00Z", 3_600),
        ],
        window="7d",
        sparse_threshold=2,
    )

    assert result.window.label == "7d"
    assert result.window.started_at == "2026-05-13T12:00:00Z"
    assert result.window.ended_at == NOW
    assert result.session_count == 3
    assert result.closed_session_count == 2
    assert result.active_session_count == 1
    assert result.current_occupied_spot_count == 1

    driveway = result.spots["driveway"]
    assert driveway.spot_id == "driveway"
    assert driveway.session_count == 2
    assert driveway.closed_session_count == 1
    assert driveway.active_session_count == 1
    assert driveway.currently_occupied is True
    assert driveway.occupied_duration_seconds == 10_800
    assert driveway.average_dwell_seconds == 7_200
    assert driveway.longest_session_seconds == 7_200
    assert driveway.first_seen_at == "2026-05-20T08:00:00Z"
    assert driveway.last_seen_at == NOW

    curb = result.spots["curb"]
    assert curb.session_count == 1
    assert curb.closed_session_count == 1
    assert curb.active_session_count == 0
    assert curb.currently_occupied is False
    assert curb.occupied_duration_seconds == 5_400
    assert curb.average_dwell_seconds == 5_400
    assert curb.longest_session_seconds == 5_400
    assert curb.first_seen_at == "2026-05-19T09:00:00Z"
    assert curb.last_seen_at == "2026-05-19T10:30:00Z"


@pytest.mark.parametrize(
    ("window", "expected_started_at", "expected_session_ids"),
    [
        ("today", "2026-05-20T00:00:00Z", {"today", "active"}),
        ("7d", "2026-05-13T12:00:00Z", {"today", "active", "six-days"}),
        ("30d", "2026-04-20T12:00:00Z", {"today", "active", "six-days", "twenty-days"}),
        ("all", None, {"today", "active", "six-days", "twenty-days", "forty-days"}),
    ],
)
def test_simple_windows_select_expected_sessions(
    window: str,
    expected_started_at: str | None,
    expected_session_ids: set[str],
) -> None:
    result = analyze_occupancy(
        [
            session("today", "driveway", "2026-05-20T07:00:00Z", "2026-05-20T08:00:00Z", 3_600),
            session("active", "driveway", "2026-05-20T11:30:00Z", None, None),
            session("six-days", "driveway", "2026-05-14T09:00:00Z", "2026-05-14T10:00:00Z", 3_600),
            session("twenty-days", "driveway", "2026-04-30T09:00:00Z", "2026-04-30T10:00:00Z", 3_600),
            session("forty-days", "driveway", "2026-04-10T09:00:00Z", "2026-04-10T10:00:00Z", 3_600),
        ],
        window=window,
        sparse_threshold=1,
    )

    assert result.window.label == window
    assert result.window.started_at == expected_started_at
    assert result.window.ended_at == NOW
    assert result.included_session_ids == tuple(sorted(expected_session_ids))
    assert result.spots["driveway"].session_count == len(expected_session_ids)


def test_no_data_window_returns_empty_metrics_with_sparse_caveat() -> None:
    result = analyze_occupancy(
        [session("old", "driveway", "2026-05-19T09:00:00Z", "2026-05-19T10:00:00Z", 3_600)],
        window="today",
    )

    assert result.session_count == 0
    assert result.closed_session_count == 0
    assert result.active_session_count == 0
    assert result.current_occupied_spot_count == 0
    assert result.spots == {}
    assert result.diagnostics == (
        {"code": "no-data-window", "message": "No vehicle-history sessions overlap the selected window."},
    )


def test_sparse_data_diagnostic_is_reported_per_spot_and_globally() -> None:
    result = analyze_occupancy(
        [session("only", "driveway", "2026-05-20T09:00:00Z", "2026-05-20T10:00:00Z", 3_600)],
        window="today",
        sparse_threshold=3,
    )

    assert result.session_count == 1
    assert result.diagnostics == (
        {
            "code": "sparse-data",
            "message": "Only 1 qualifying vehicle-history session is available; analytics may be noisy.",
            "qualifying_session_count": 1,
            "sparse_threshold": 3,
        },
    )
    assert result.spots["driveway"].diagnostics == (
        {
            "code": "sparse-spot-data",
            "message": "Only 1 qualifying session is available for spot driveway.",
            "qualifying_session_count": 1,
            "sparse_threshold": 3,
        },
    )


def test_malformed_sessions_are_excluded_with_diagnostics() -> None:
    result = analyze_occupancy(
        [
            {"session_id": "bad-start", "spot_id": "driveway", "started_at": "not-a-date", "ended_at": "2026-05-20T10:00:00Z", "duration_seconds": 3_600},
            {"session_id": "bad-duration", "spot_id": "driveway", "started_at": "2026-05-20T08:00:00Z", "ended_at": "2026-05-20T09:00:00Z", "duration_seconds": -1},
            {"session_id": "missing-spot", "started_at": "2026-05-20T08:00:00Z", "ended_at": "2026-05-20T09:00:00Z", "duration_seconds": 3_600},
            session("good", "driveway", "2026-05-20T10:00:00Z", "2026-05-20T11:00:00Z", 3_600),
        ],
        window="today",
        sparse_threshold=1,
    )

    assert result.session_count == 1
    assert result.invalid_session_count == 3
    assert result.included_session_ids == ("good",)
    assert result.spots["driveway"].occupied_duration_seconds == 3_600
    assert result.diagnostics == (
        {
            "code": "malformed-history",
            "message": "3 vehicle-history sessions were ignored because they were malformed.",
            "invalid_session_count": 3,
        },
    )


def test_unknown_windows_fail_closed() -> None:
    with pytest.raises(ValueError, match="unsupported occupancy analytics window"):
        analyze_occupancy([], window="yesterday")
