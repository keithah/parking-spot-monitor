"""Shared Matrix timestamp formatting helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from parking_spot_monitor.matrix_support import _require_non_empty

DISPLAY_TIMEZONE = ZoneInfo("America/Los_Angeles")


def parse_observed_at(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = _require_non_empty("observed_at", value)
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def format_observed_at(value: object) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return _require_non_empty("observed_at", str(value))


def display_observed_at(value: object) -> str:
    observed_at = parse_observed_at(value)
    if observed_at is None:
        return format_observed_at(value).replace("Z", "+00:00")
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        return _format_display_datetime(observed_at)
    return _format_display_datetime(observed_at.astimezone(DISPLAY_TIMEZONE))


def _format_display_datetime(value: datetime) -> str:
    time_text = _format_12_hour_time(value.hour, value.minute, value.second)
    timezone_name = value.tzname() if value.tzinfo is not None and value.utcoffset() is not None else ""
    suffix = f" {timezone_name}" if timezone_name else ""
    return f"{value:%Y-%m-%d} {time_text}{suffix}"


def _format_12_hour_time(hour: int, minute: int, second: int | None = None) -> str:
    suffix = "AM" if hour < 12 else "PM"
    display_hour = hour % 12 or 12
    if second is None:
        return f"{display_hour}:{minute:02d} {suffix}"
    return f"{display_hour}:{minute:02d}:{second:02d} {suffix}"
