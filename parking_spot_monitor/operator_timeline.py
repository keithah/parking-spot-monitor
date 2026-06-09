from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from parking_spot_monitor.logging import redact_diagnostic_text

DISPLAY_TIMEZONE = ZoneInfo("America/Los_Angeles")


def parse_incident_time(value: str, *, now: datetime) -> datetime:
    text = redact_diagnostic_text(value).strip().lower()
    try:
        parsed = datetime.fromisoformat(text.replace("z", "+00:00"))
    except ValueError:
        parsed = _parse_local_clock_time(text, now=now)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=DISPLAY_TIMEZONE)
    return parsed.astimezone(timezone.utc)


def nearest_timeline_frame(data_dir: Path, target_time: datetime) -> tuple[Path, datetime] | None:
    frames_dir = data_dir / "timeline" / "frames"
    try:
        candidates = frames_dir.glob("*.jpg")
    except OSError:
        return None
    nearest: tuple[Path, datetime] | None = None
    for path in candidates:
        frame_time = timeline_frame_time(path)
        if frame_time is None:
            continue
        if nearest is None or abs(frame_time - target_time) < abs(nearest[1] - target_time):
            nearest = (path, frame_time)
    return nearest


def timeline_frame_time(path: Path) -> datetime | None:
    try:
        return datetime.strptime(path.stem, "%Y%m%dT%H%M00Z").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_local_clock_time(text: str, *, now: datetime) -> datetime:
    compact = text.replace(" ", "")
    suffix = None
    if compact.endswith("am") or compact.endswith("pm"):
        suffix = compact[-2:]
        compact = compact[:-2]
    if ":" not in compact:
        raise ValueError("incident time must be ISO or h:mmam/pm")
    hour_text, minute_text = compact.split(":", 1)
    hour = int(hour_text)
    minute = int(minute_text)
    if suffix == "pm" and hour != 12:
        hour += 12
    if suffix == "am" and hour == 12:
        hour = 0
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("incident time is out of range")
    local_now = now.astimezone(DISPLAY_TIMEZONE)
    candidate = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate - local_now > timedelta(hours=1):
        candidate -= timedelta(days=1)
    return candidate
