"""Matrix alert text formatting and stable event identifiers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from parking_spot_monitor.logging import redact_diagnostic_text
from parking_spot_monitor.matrix_support import MatrixError, _require_non_empty
from parking_spot_monitor.matrix_time import DISPLAY_TIMEZONE, display_observed_at, format_observed_at

MONITOR_STARTED_EVENT_TYPE = "parking-monitor-started"
MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE = "parking-monitor-shutdown-requested"
LIFECYCLE_EVENT_TYPES = frozenset({MONITOR_STARTED_EVENT_TYPE, MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE})
OPEN_SPOT_EVENT_TYPE = "occupancy-open-event"
OCCUPIED_SPOT_EVENT_TYPE = "occupancy-occupied-event"
OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE = "owner-vehicle-quiet-window-alert"

def monitor_lifecycle_event_id(
    *,
    event_type: str,
    observed_at: str,
    signal: str | None = None,
) -> str:
    if signal:
        return f"{event_type}:{signal}:{observed_at}"
    return f"{event_type}:{observed_at}"


def monitor_lifecycle_event(
    event_type: str,
    observed_at: datetime,
    *,
    signal: str | None = None,
) -> dict[str, Any]:
    observed_at_text = format_observed_at(observed_at)
    event: dict[str, Any] = {
        "event_type": event_type,
        "observed_at": observed_at_text,
        "event_id": monitor_lifecycle_event_id(event_type=event_type, observed_at=observed_at_text, signal=signal),
    }
    if signal is not None:
        event["signal"] = signal
    return event


def open_spot_event_id(event: Mapping[str, Any]) -> str:
    """Return the stable Matrix transaction base for a confirmed open event."""

    event_type = _require_non_empty("event_type", str(event.get("event_type", OPEN_SPOT_EVENT_TYPE)))
    spot_id = _require_non_empty("spot_id", str(event.get("spot_id", "")))
    observed_at = format_observed_at(event.get("observed_at"))
    return f"{event_type}:{spot_id}:{observed_at}"


def occupied_spot_event_id(event: Mapping[str, Any]) -> str:
    """Return the stable Matrix transaction base for a confirmed occupied event."""

    event_type = _require_non_empty("event_type", str(event.get("event_type", OCCUPIED_SPOT_EVENT_TYPE)))
    spot_id = _require_non_empty("spot_id", str(event.get("spot_id", "")))
    observed_at = format_observed_at(event.get("observed_at"))
    return f"{event_type}:{spot_id}:{observed_at}"


def format_open_spot_alert(event: Mapping[str, Any]) -> str:
    """Return deterministic Matrix text for a confirmed parking-open event."""

    spot_id = _require_non_empty("spot_id", redact_diagnostic_text(event.get("spot_id", "")))
    observed_at = display_observed_at(event.get("observed_at"))
    return f"Parking spot open: {spot_id} at {observed_at}"




def format_occupied_spot_alert(event: Mapping[str, Any]) -> str:
    """Return deterministic Matrix text for a confirmed parking-occupied event.

    The formatter is intentionally metadata-only: it never opens snapshot files
    and only reads an allowlist of alert-safe fields from the provided mapping.
    """

    spot_id = _require_non_empty("spot_id", redact_diagnostic_text(event.get("spot_id", "")))
    observed_at = display_observed_at(event.get("observed_at"))
    vehicle = _mapping_field(event, "likely_vehicle")
    estimate = _mapping_field(event, "vehicle_history_estimate") or _mapping_field(event, "history_estimate")

    label = _safe_text(_first_present(vehicle, "label", "vehicle_label", "display_label"), default="unknown vehicle")
    profile_id = _safe_text(
        _first_present(vehicle, "profile_id") or _first_present(estimate, "profile_id") or event.get("profile_id"),
        default="unknown",
    )
    match_status = _safe_text(
        _first_present(vehicle, "match_status", "status") or event.get("match_status"),
        default="unknown",
    )
    match_confidence = _safe_text(
        _first_present(vehicle, "confidence", "profile_confidence") or event.get("profile_confidence"),
        default="unknown",
    )

    lines = [f"Parking spot occupied: {spot_id} at {observed_at}"]

    estimate_status = _safe_text(_first_present(estimate, "status"), default="insufficient_history")
    sample_count = _int_field(estimate, "sample_count", default=0)
    estimate_confidence = _safe_text(_first_present(estimate, "confidence"), default="unknown")
    estimate_has_high_signal = estimate_status == "estimated" and sample_count >= 3 and estimate_confidence not in {"low", "unknown"}
    has_useful_vehicle_context = _has_meaningful_vehicle_label(label, profile_id) or estimate_has_high_signal
    if not has_useful_vehicle_context:
        return "\n".join(lines)

    lines.extend(
        [
            f"Likely vehicle: {label} (profile {profile_id})",
            f"Match: {match_status}, confidence {match_confidence}",
        ]
    )

    if estimate_status == "estimated":
        dwell_range = _mapping_field(estimate, "dwell_range")
        leave_window = _mapping_field(estimate, "leave_time_window")
        lines.append(f"Estimated dwell: {_format_dwell_range(dwell_range)}")
        lines.append(f"Usual leave window: {_format_leave_window(leave_window)}")
    else:
        reason = _safe_text(_first_present(estimate, "reason"), default="insufficient-history")
        lines.append(f"Estimate unavailable: {reason}")
    lines.append(f"History: {sample_count} {_plural('sample', sample_count)}, estimate confidence {estimate_confidence}")
    return "\n".join(lines)


def _mapping_field(source: object, name: str) -> Mapping[str, Any]:
    if isinstance(source, Mapping):
        value = source.get(name)
        if isinstance(value, Mapping):
            return value
        value = getattr(value, "__dict__", None)
        if isinstance(value, Mapping):
            return value
    return {}


def _first_present(source: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = source.get(name)
        if value is not None:
            return value
    return None


def _safe_text(value: object, *, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")
    text = redact_diagnostic_text(value).strip()
    return text or default


def _int_field(source: Mapping[str, Any], name: str, *, default: int) -> int:
    value = source.get(name)
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


def _format_dwell_range(dwell_range: Mapping[str, Any]) -> str:
    lower = _int_field(dwell_range, "lower_seconds", default=0)
    upper = _int_field(dwell_range, "upper_seconds", default=0)
    typical = _int_field(dwell_range, "typical_seconds", default=0)
    return f"{_format_duration(lower)}–{_format_duration(upper)} (typical {_format_duration(typical)})"


def _format_duration(seconds: int) -> str:
    seconds = max(0, seconds)
    total_minutes = int(round(seconds / 60))
    if total_minutes < 60:
        return f"{total_minutes} {_plural('min', total_minutes)}"
    hours, minutes = divmod(total_minutes, 60)
    hour_text = f"{hours} {_plural('hr', hours)}"
    if minutes == 0:
        return hour_text
    return f"{hour_text} {minutes} {_plural('min', minutes)}"


def _format_leave_window(leave_window: Mapping[str, Any]) -> str:
    start = _int_field(leave_window, "start_minute", default=0)
    end = _int_field(leave_window, "end_minute", default=0)
    typical = _int_field(leave_window, "typical_minute", default=0)
    crosses_midnight = bool(leave_window.get("crosses_midnight"))
    suffix = "; crosses midnight" if crosses_midnight else ""
    return f"{_format_minute_of_day(start)}–{_format_minute_of_day(end)} (typical {_format_minute_of_day(typical)}{suffix})"


def _format_minute_of_day(value: int) -> str:
    minute = value % (24 * 60)
    return _format_12_hour_time(minute // 60, minute % 60)


def _format_12_hour_time(hour: int, minute: int, second: int | None = None) -> str:
    suffix = "AM" if hour < 12 else "PM"
    display_hour = hour % 12 or 12
    if second is None:
        return f"{display_hour}:{minute:02d} {suffix}"
    return f"{display_hour}:{minute:02d}:{second:02d} {suffix}"


def _has_meaningful_vehicle_label(label: str, profile_id: str) -> bool:
    normalized_label = label.strip().lower()
    normalized_profile = profile_id.strip().lower()
    if normalized_label in {"", "unknown", "unknown vehicle"}:
        return False
    if normalized_profile and normalized_label == normalized_profile:
        return False
    if normalized_label.startswith("prof_sess-"):
        return False
    return True


def _plural(word: str, count: int) -> str:
    if word == "min":
        return word
    return word if count == 1 else f"{word}s"


def _occupied_snapshot_body(*, spot_id: str, observed_at: object) -> str:
    return f"Raw occupied full-frame snapshot for {redact_diagnostic_text(spot_id)} at {display_observed_at(observed_at)}"



def owner_vehicle_quiet_window_event_id(event: Mapping[str, Any]) -> str:
    spot_id = _require_non_empty("spot_id", redact_diagnostic_text(event.get("spot_id", "")))
    profile_id = _safe_text(event.get("profile_id"), default="unknown")
    window_id = _safe_text(event.get("window_id"), default="quiet-window")
    return f"{OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE}:{spot_id}:{profile_id}:{window_id}"


def format_owner_vehicle_quiet_window_alert(event: Mapping[str, Any]) -> str:
    spot_id = _require_non_empty("spot_id", redact_diagnostic_text(event.get("spot_id", "")))
    observed_at = display_observed_at(event.get("observed_at"))
    window_id = _safe_text(event.get("window_id"), default="street cleaning")
    owner_vehicle = _mapping_field(event, "owner_vehicle")
    label = _safe_text(owner_vehicle.get("label") or event.get("label"), default="your car")
    return f"Street cleaning alert: {label} is parked in {spot_id} at {observed_at} during {window_id}."

def format_quiet_window_notice(event: Mapping[str, Any]) -> str:
    """Return deterministic Matrix text for a street-sweeping start/end notice."""

    event_type = _require_non_empty("event_type", str(event.get("event_type", "")))
    window_id = _require_non_empty("window_id", str(event.get("window_id", "")))
    if event_type == "quiet-window-upcoming":
        minutes_before = _int_field(event, "reminder_minutes_before", default=0)
        lead_time = _format_lead_time(minutes_before)
        return f"Street sweeping starts in {lead_time}: {window_id}"
    if event_type == "quiet-window-started":
        verb = "started"
    elif event_type == "quiet-window-ended":
        verb = "ended"
    else:
        verb = event_type
    return f"Street sweeping {verb}: {window_id}"


def format_lifecycle_notice(event: Mapping[str, Any]) -> str:
    event_type = _require_non_empty("event_type", str(event.get("event_type", "")))
    observed_at = display_observed_at(event.get("observed_at"))
    if event_type == MONITOR_STARTED_EVENT_TYPE:
        return f"Parking monitor started at {observed_at}."
    if event_type == MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE:
        signal_name = _safe_text(event.get("signal"), default="shutdown signal")
        return f"Parking monitor shutdown requested by {signal_name} at {observed_at}."
    raise MatrixError(
        "Matrix lifecycle event type is unsupported",
        error_type="unsupported_lifecycle_event",
        event_type=event_type,
        event_id=str(event.get("event_id", "")),
    )


def _format_lead_time(minutes: int) -> str:
    if minutes == 60:
        return "1 hour"
    if minutes > 0 and minutes % 60 == 0:
        return f"{minutes // 60} hours"
    return f"{minutes} minutes"


def format_live_proof_text(*, observed_at: object, selected_mode: object) -> str:
    observed_text = display_observed_at(observed_at)
    mode_text = str(getattr(selected_mode, "value", selected_mode))
    return f"LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at {observed_text} (decode mode: {mode_text})."


def format_live_proof_image_body(*, observed_at: object) -> str:
    observed_text = display_observed_at(observed_at)
    return f"LIVE PROOF / TEST IMAGE: raw full-frame camera snapshot captured at {observed_text}."


def live_proof_event_id(observed_at: object) -> str:
    return f"live-proof:{format_observed_at(observed_at)}"

