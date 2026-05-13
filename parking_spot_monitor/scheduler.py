from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from parking_spot_monitor.config import QuietWindowConfig

WEEKDAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


class QuietWindowEventType(StrEnum):
    """Stable event names consumed by runtime logging and notification code."""

    UPCOMING = "quiet-window-upcoming"
    STARTED = "quiet-window-started"
    ENDED = "quiet-window-ended"


@dataclass(frozen=True)
class QuietWindowStatus:
    """Pure schedule evaluation result for the current frame time."""

    active: bool
    active_window_id: str | None = None
    window_name: str | None = None
    active_window_ids: frozenset[str] = field(default_factory=frozenset)
    upcoming_window_reminders: dict[str, int] = field(default_factory=dict)

    @property
    def window_id(self) -> str | None:
        return self.active_window_id

    @property
    def suppressed_reason(self) -> str | None:
        if not self.active:
            return None
        return f"quiet_window:{self.active_window_id}" if self.active_window_id else "quiet_window"


@dataclass(frozen=True)
class QuietWindowNoticeEvent:
    """Serializable quiet-window start/end notice payload."""

    event_type: QuietWindowEventType
    event_id: str
    window_id: str
    reminder_minutes_before: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event_type": self.event_type.value,
            "event_id": self.event_id,
            "window_id": self.window_id,
        }
        if self.reminder_minutes_before is not None:
            payload["reminder_minutes_before"] = self.reminder_minutes_before
        return payload


def evaluate_quiet_windows(windows: Iterable[QuietWindowConfig], now: datetime) -> QuietWindowStatus:
    """Evaluate configured quiet windows at a timezone-aware instant.

    Work is O(number of windows), performs no filesystem or network I/O, and uses
    stdlib zoneinfo for local civil-time conversion.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")

    active: list[tuple[str, str]] = []
    upcoming: dict[str, int] = {}
    for window in windows:
        window_id = _active_window_id(window, now)
        if window_id is not None:
            active.append((window.name, window_id))
            continue
        upcoming_window_id = _upcoming_window_id(window, now)
        if upcoming_window_id is not None and window.reminder_minutes_before is not None:
            upcoming[upcoming_window_id] = window.reminder_minutes_before

    if not active:
        return QuietWindowStatus(active=False, upcoming_window_reminders=upcoming)

    first_name, first_id = active[0]
    return QuietWindowStatus(
        active=True,
        active_window_id=first_id,
        window_name=first_name,
        active_window_ids=frozenset(window_id for _, window_id in active),
        upcoming_window_reminders=upcoming,
    )


def quiet_window_notice_events(
    *, previous_active_window_ids: Iterable[str], current: QuietWindowStatus, emitted_notice_ids: Iterable[str] = ()
) -> list[QuietWindowNoticeEvent]:
    """Return deterministic upcoming/start/end notices by comparing persisted markers."""

    previous = set(previous_active_window_ids)
    current_ids = set(current.active_window_ids)
    emitted = set(emitted_notice_ids)
    events: list[QuietWindowNoticeEvent] = []

    for window_id, minutes_before in sorted(current.upcoming_window_reminders.items()):
        event_id = f"{QuietWindowEventType.UPCOMING.value}:{window_id}:{minutes_before}m"
        if event_id not in emitted:
            events.append(
                QuietWindowNoticeEvent(
                    event_type=QuietWindowEventType.UPCOMING,
                    event_id=event_id,
                    window_id=window_id,
                    reminder_minutes_before=minutes_before,
                )
            )

    for window_id in sorted(current_ids - previous):
        events.append(
            QuietWindowNoticeEvent(
                event_type=QuietWindowEventType.STARTED,
                event_id=f"{QuietWindowEventType.STARTED.value}:{window_id}",
                window_id=window_id,
            )
        )

    for window_id in sorted(previous - current_ids):
        events.append(
            QuietWindowNoticeEvent(
                event_type=QuietWindowEventType.ENDED,
                event_id=f"{QuietWindowEventType.ENDED.value}:{window_id}",
                window_id=window_id,
            )
        )

    return events


def _active_window_id(window: QuietWindowConfig, now: datetime) -> str | None:
    local_now = now.astimezone(ZoneInfo(window.timezone))
    if not _matches_recurrence(window, local_now):
        return None

    start_minutes = _minutes_since_midnight(window.start)
    end_minutes = _minutes_since_midnight(window.end)
    current_minutes = local_now.hour * 60 + local_now.minute
    if not (start_minutes <= current_minutes < end_minutes):
        return None

    return _window_id(window, local_now)


def _upcoming_window_id(window: QuietWindowConfig, now: datetime) -> str | None:
    if window.reminder_minutes_before is None:
        return None
    local_now = now.astimezone(ZoneInfo(window.timezone))
    if not _matches_recurrence(window, local_now):
        return None

    start_minutes = _minutes_since_midnight(window.start)
    current_minutes = local_now.hour * 60 + local_now.minute
    reminder_start = start_minutes - window.reminder_minutes_before
    if not (reminder_start <= current_minutes < start_minutes):
        return None
    return _window_id(window, local_now)


def _matches_recurrence(window: QuietWindowConfig, local_now: datetime) -> bool:
    if window.recurrence != "monthly_weekday":
        return False
    if local_now.weekday() not in {WEEKDAY_INDEX[weekday] for weekday in window.weekdays}:
        return False
    return _ordinal_in_month(local_now) in set(window.ordinals)


def _window_id(window: QuietWindowConfig, local_now: datetime) -> str:
    return f"{window.name}:{local_now.date().isoformat()}:{window.start}-{window.end}"


def _ordinal_in_month(value: datetime) -> int:
    return ((value.day - 1) // 7) + 1


def _minutes_since_midnight(value: str) -> int:
    hours, minutes = value.split(":", 1)
    return int(hours) * 60 + int(minutes)
