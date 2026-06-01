from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value

MAX_REPLY_BYTES = 4096
MAX_FILE_BYTES = 256_000
MAX_LATEST_IMAGE_BYTES = 300_000
MAX_WHO_MATRIX_IMAGE_BYTES = MAX_LATEST_IMAGE_BYTES
WHO_MATRIX_SNAPSHOT_FILENAME = "who_latest.jpg"
INCIDENT_MATRIX_SNAPSHOT_TEMPLATE = "incident_{spot_id}.jpg"
DISPLAY_TIMEZONE = ZoneInfo("America/Los_Angeles")
WHO_MATRIX_INITIAL_MAX_DIMENSION = 960
WHO_MATRIX_MIN_DIMENSION = 320
WHO_MATRIX_JPEG_QUALITIES = (85, 75, 65, 55, 45, 35)
MAX_LINES_PER_SECTION = 24
STALE_INTERVAL_MULTIPLIER = 3
STALE_MIN_SECONDS = 60

LoadState = Literal["available", "unavailable", "error"]
FreshnessState = Literal["fresh", "stale", "unknown"]


@dataclass(frozen=True)
class BoundedJsonLoad:
    state: LoadState
    payload: Mapping[str, Any] | None = None
    error_type: str | None = None


@dataclass(frozen=True)
class HealthSummary:
    state: LoadState
    status: str = "unavailable"
    freshness: FreshnessState = "unknown"
    updated_at: datetime | None = None
    updated_age: str = "unknown"
    iteration: str = "unknown"
    last_frame_age: str = "unknown"
    frame_interval_seconds: float | str = "unknown"
    selected_decode_mode: str = "unknown"
    consecutive_capture_failures: int = 0
    consecutive_detection_failures: int = 0
    error_type: str | None = None


@dataclass(frozen=True)
class SpotSummary:
    spot_id: str
    status: str
    hit_streak: int
    miss_streak: int
    open_event_emitted: bool


@dataclass(frozen=True)
class StateSummary:
    state: LoadState
    spots: tuple[SpotSummary, ...]
    active_quiet_window_count: int = 0
    quiet_window_notice_count: int = 0
    owner_quiet_window_alert_count: int = 0
    error_type: str | None = None


@dataclass(frozen=True)
class LatestSnapshotValidation:
    state: LoadState
    path: Path | None = None
    info: dict[str, int | str] | None = None
    freshness: FreshnessState = "unknown"
    age: str = "unknown"
    error_type: str | None = None


@dataclass(frozen=True)
class LatestSnapshotResponse:
    """Bounded Matrix-ready latest snapshot response without image bytes."""

    text: str
    image_path: Path | None
    image_info: dict[str, int | str] | None


def summarize_health(
    *,
    settings: RuntimeSettings,
    health_path: str | Path,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> HealthSummary:
    observed_now = utc_now(now)
    loaded = load_bounded_json_object(Path(health_path), label="health", logger=logger)
    frame_interval = getattr(getattr(settings, "runtime", None), "frame_interval_seconds", "unknown")
    if loaded.state != "available" or loaded.payload is None:
        return HealthSummary(state=loaded.state, frame_interval_seconds=frame_interval, error_type=loaded.error_type)

    payload = loaded.payload
    updated_at = _parse_time(payload.get("updated_at"))
    freshness = _freshness(updated_at, observed_now, frame_interval)
    capture = mapping_value(payload.get("capture"))
    last_frame_at = _parse_time(_first_present(payload, "last_frame_at") or _first_present(capture, "last_success_at"))
    iteration = payload.get("iteration")
    return HealthSummary(
        state="available",
        status=text_value(payload.get("status"), default="unavailable"),
        freshness=freshness,
        updated_at=updated_at,
        updated_age=age_label(updated_at, observed_now),
        iteration=str(iteration) if isinstance(iteration, int) and not isinstance(iteration, bool) else "unknown",
        last_frame_age=age_label(last_frame_at, observed_now),
        frame_interval_seconds=frame_interval,
        selected_decode_mode=text_value(
            _first_present(payload, "selected_decode_mode") or _first_present(capture, "selected_decode_mode"),
            default="unknown",
        ),
        consecutive_capture_failures=int_value(payload.get("consecutive_capture_failures")),
        consecutive_detection_failures=int_value(payload.get("consecutive_detection_failures")),
    )


def summarize_state(*, settings: RuntimeSettings, state_path: str | Path, logger: StructuredLogger | None = None) -> StateSummary:
    configured = spot_ids(settings)
    fallback_spots = tuple(SpotSummary(spot_id=spot_id, status="open", hit_streak=0, miss_streak=0, open_event_emitted=False) for spot_id in configured)
    loaded = load_bounded_json_object(Path(state_path), label="state", logger=logger)
    if loaded.state != "available" or loaded.payload is None:
        return StateSummary(state=loaded.state, spots=fallback_spots, error_type=loaded.error_type)

    payload = loaded.payload
    raw_spots = payload.get("spots")
    if not isinstance(raw_spots, Mapping):
        return StateSummary(state="error", spots=fallback_spots, error_type="schema_error")

    spots: list[SpotSummary] = []
    for spot_id in configured:
        raw = raw_spots.get(spot_id)
        spot = mapping_value(raw)
        spots.append(
            SpotSummary(
                spot_id=spot_id,
                status=_spot_status(spot.get("status")),
                hit_streak=int_value(spot.get("hit_streak")),
                miss_streak=int_value(spot.get("miss_streak")),
                open_event_emitted=spot.get("open_event_emitted") is True,
            )
        )
    return StateSummary(
        state="available",
        spots=tuple(spots),
        active_quiet_window_count=_bounded_count(payload.get("active_quiet_window_ids")),
        quiet_window_notice_count=_bounded_count(payload.get("quiet_window_notice_ids")),
        owner_quiet_window_alert_count=_bounded_count(payload.get("owner_quiet_window_alert_ids")),
    )


def load_bounded_json_object(path: Path, *, label: str, logger: StructuredLogger | None) -> BoundedJsonLoad:
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        log_load_problem(logger, label=label, reason="missing", error_type="FileNotFoundError")
        return BoundedJsonLoad(state="unavailable", error_type="missing")
    except OSError as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        log_load_problem(logger, label=label, reason="stat_error", error_type=error_type)
        return BoundedJsonLoad(state="unavailable", error_type=error_type)
    if size > MAX_FILE_BYTES:
        log_load_problem(logger, label=label, reason="too_large", error_type="file_too_large", byte_size=size)
        return BoundedJsonLoad(state="error", error_type="file_too_large")
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as exc:
        log_load_problem(logger, label=label, reason="json_parse", error_type=exc.__class__.__name__)
        return BoundedJsonLoad(state="unavailable", error_type="JSONDecodeError")
    except OSError as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        log_load_problem(logger, label=label, reason="read_error", error_type=error_type)
        return BoundedJsonLoad(state="unavailable", error_type=error_type)
    if not isinstance(payload, Mapping):
        log_load_problem(logger, label=label, reason="schema", error_type="non_object_payload")
        return BoundedJsonLoad(state="error", error_type="non_object_payload")
    return BoundedJsonLoad(state="available", payload=dict(payload))


def format_health_line(health: HealthSummary) -> str:
    if health.state == "available":
        stale = " stale" if health.freshness == "stale" else ""
        return f"Health: {health.status}{stale} (updated {health.updated_age})"
    suffix = f" ({health.error_type})" if health.error_type else ""
    return f"Health: unavailable{suffix}"


def _freshness(updated_at: datetime | None, now: datetime, frame_interval: object) -> FreshnessState:
    if updated_at is None:
        return "unknown"
    try:
        interval = float(frame_interval)
    except (TypeError, ValueError):
        interval = 300.0
    allowed = max(STALE_MIN_SECONDS, interval * STALE_INTERVAL_MULTIPLIER)
    return "stale" if (now - updated_at).total_seconds() > allowed else "fresh"


def log_load_problem(logger: StructuredLogger | None, **fields: Any) -> None:
    if logger is None:
        return
    logger.warning("matrix-operator-runtime-load", **redact_diagnostic_value(fields))


def spot_items(settings: RuntimeSettings) -> list[tuple[str, Any]]:
    spots = getattr(settings, "spots", None)
    if spots is None:
        return []
    return [("left_spot", spots.left_spot), ("right_spot", spots.right_spot)]


def spot_ids(settings: RuntimeSettings) -> list[str]:
    names = [spot_id for spot_id, _spot in spot_items(settings)]
    return names or ["left_spot", "right_spot"]


def _spot_status(value: Any) -> str:
    text = text_value(value, default="unknown")
    if text in {"empty", "open", "unknown"}:
        return "open"
    if text == "occupied":
        return "occupied"
    return "unavailable"


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def age_label(value: datetime | None, now: datetime) -> str:
    if value is None:
        return "unknown"
    seconds = max(0, int((now - value).total_seconds()))
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


def bounded_reply(lines: Sequence[str]) -> str:
    rendered = "\n".join(bounded_lines(lines))
    encoded = rendered.encode("utf-8")
    if len(encoded) <= MAX_REPLY_BYTES:
        return rendered
    return encoded[: MAX_REPLY_BYTES - 3].decode("utf-8", errors="ignore") + "..."


def bounded_lines(lines: Sequence[str]) -> list[str]:
    bounded = [redact_diagnostic_text(line) for line in lines[: MAX_LINES_PER_SECTION * 3]]
    if len(lines) > len(bounded):
        bounded.append("... truncated")
    return bounded


def utc_now(value: datetime | None) -> datetime:
    selected = value if value is not None else datetime.now(timezone.utc)
    if selected.tzinfo is None:
        return selected.replace(tzinfo=timezone.utc)
    return selected.astimezone(timezone.utc)


def mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_present(mapping: Mapping[str, Any], key: str) -> Any:
    value = mapping.get(key)
    return value if value not in (None, "") else None


def text_value(value: Any, *, default: str = "unknown") -> str:
    if value is None:
        return default
    if isinstance(value, bool):
        return "true" if value else "false"
    text = redact_diagnostic_text(value)
    return text[:160] if text else default


def int_value(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _bounded_count(value: Any) -> int:
    if not isinstance(value, list | tuple | set | frozenset):
        return 0
    return min(len(value), MAX_LINES_PER_SECTION)


def crop_label(enabled: Any) -> str:
    return "crop enabled" if bool(enabled) else "crop disabled"


def list_label(value: Any) -> str:
    if not isinstance(value, list | tuple):
        return "none"
    items = [text_value(item) for item in value[:8]]
    suffix = ", ..." if len(value) > len(items) else ""
    return ", ".join(items) + suffix if items else "none"


def matrix_token_present(matrix: Mapping[str, Any]) -> bool:
    token = mapping_value(matrix.get("matrix_token"))
    return token.get("present") is True
