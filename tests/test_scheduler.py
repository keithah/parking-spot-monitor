from __future__ import annotations

from datetime import datetime, timezone

import pytest

from parking_spot_monitor.config import QuietWindowConfig
from parking_spot_monitor.scheduler import (
    QuietWindowEventType,
    evaluate_quiet_windows,
    quiet_window_notice_events,
)


def street_sweeping_window() -> QuietWindowConfig:
    return QuietWindowConfig(
        name="street_sweeping",
        timezone="America/Los_Angeles",
        recurrence="monthly_weekday",
        weekdays=["monday"],
        ordinals=[1, 3],
        start="13:00",
        end="15:00",
        reminder_minutes_before=60,
    )


@pytest.mark.parametrize(
    "now,expected_active,expected_id",
    [
        (datetime(2026, 5, 4, 13, 0, tzinfo=timezone.utc), False, None),  # 06:00 PDT
        (datetime(2026, 5, 4, 20, 0, tzinfo=timezone.utc), True, "street_sweeping:2026-05-04:13:00-15:00"),
        (datetime(2026, 5, 4, 22, 0, tzinfo=timezone.utc), False, None),
        (datetime(2026, 5, 11, 20, 30, tzinfo=timezone.utc), False, None),
        (datetime(2026, 5, 18, 20, 30, tzinfo=timezone.utc), True, "street_sweeping:2026-05-18:13:00-15:00"),
        (datetime(2026, 5, 25, 20, 30, tzinfo=timezone.utc), False, None),
    ],
)
def test_monthly_weekday_quiet_window_matches_first_and_third_monday_local_time(
    now: datetime, expected_active: bool, expected_id: str | None
) -> None:
    status = evaluate_quiet_windows([street_sweeping_window()], now)

    assert status.active is expected_active
    assert status.active_window_id == expected_id
    assert status.window_name == ("street_sweeping" if expected_active else None)


def test_local_timezone_aware_input_is_supported() -> None:
    from zoneinfo import ZoneInfo

    status = evaluate_quiet_windows(
        [street_sweeping_window()],
        datetime(2026, 5, 18, 13, 30, tzinfo=ZoneInfo("America/Los_Angeles")),
    )

    assert status.active is True
    assert status.active_window_id == "street_sweeping:2026-05-18:13:00-15:00"


def test_start_and_end_notice_ids_are_stable_and_deduplicated() -> None:
    active_status = evaluate_quiet_windows([street_sweeping_window()], datetime(2026, 5, 18, 20, 1, tzinfo=timezone.utc))
    inactive_status = evaluate_quiet_windows([street_sweeping_window()], datetime(2026, 5, 18, 22, 1, tzinfo=timezone.utc))

    started = quiet_window_notice_events(previous_active_window_ids=set(), current=active_status)
    duplicate_started = quiet_window_notice_events(
        previous_active_window_ids={active_status.active_window_id}, current=active_status
    )
    ended = quiet_window_notice_events(previous_active_window_ids={active_status.active_window_id}, current=inactive_status)
    duplicate_ended = quiet_window_notice_events(previous_active_window_ids=set(), current=inactive_status)

    assert [event.event_type for event in started] == [QuietWindowEventType.STARTED]
    assert started[0].event_id == "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00"
    assert started[0].window_id == "street_sweeping:2026-05-18:13:00-15:00"
    assert [event.to_dict()["event_type"] for event in started] == ["quiet-window-started"]

    assert duplicate_started == []
    assert [event.event_type for event in ended] == [QuietWindowEventType.ENDED]
    assert ended[0].event_id == "quiet-window-ended:street_sweeping:2026-05-18:13:00-15:00"
    assert duplicate_ended == []


def test_upcoming_notice_id_is_stable_and_deduplicated() -> None:
    upcoming_status = evaluate_quiet_windows([street_sweeping_window()], datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc))
    duplicate_status = evaluate_quiet_windows([street_sweeping_window()], datetime(2026, 5, 18, 19, 30, tzinfo=timezone.utc))
    active_status = evaluate_quiet_windows([street_sweeping_window()], datetime(2026, 5, 18, 20, 0, tzinfo=timezone.utc))

    upcoming = quiet_window_notice_events(previous_active_window_ids=set(), current=upcoming_status)
    duplicate_upcoming = quiet_window_notice_events(
        previous_active_window_ids=set(),
        current=duplicate_status,
        emitted_notice_ids={"quiet-window-upcoming:street_sweeping:2026-05-18:13:00-15:00:60m"},
    )
    active = quiet_window_notice_events(previous_active_window_ids=set(), current=active_status)

    assert upcoming_status.active is False
    assert [event.event_type for event in upcoming] == [QuietWindowEventType.UPCOMING]
    assert upcoming[0].event_id == "quiet-window-upcoming:street_sweeping:2026-05-18:13:00-15:00:60m"
    assert upcoming[0].window_id == "street_sweeping:2026-05-18:13:00-15:00"
    assert upcoming[0].reminder_minutes_before == 60
    assert upcoming[0].to_dict() == {
        "event_type": "quiet-window-upcoming",
        "event_id": "quiet-window-upcoming:street_sweeping:2026-05-18:13:00-15:00:60m",
        "window_id": "street_sweeping:2026-05-18:13:00-15:00",
        "reminder_minutes_before": 60,
    }
    assert duplicate_upcoming == []
    assert [event.event_type for event in active] == [QuietWindowEventType.STARTED]


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate_quiet_windows([street_sweeping_window()], datetime(2026, 5, 18, 13, 30))
