from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any, Literal

from PIL import Image, UnidentifiedImageError

from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.capture import CaptureError, capture_latest
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.incident_review import build_incident_replay
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.matrix import MatrixCommandResponse
from parking_spot_monitor.occupancy_analytics import analyze_occupancy
from parking_spot_monitor.paths import resolve_runtime_paths

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


def build_latest_snapshot_response(
    *,
    settings: RuntimeSettings,
    latest_path: str | Path,
    health_path: str | Path,
    state_path: str | Path,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> LatestSnapshotResponse:
    """Build a bounded, redacted latest snapshot summary from local runtime files only."""

    observed_now = _utc_now(now)
    snapshot = _validate_latest_snapshot(Path(latest_path), now=observed_now, logger=logger)
    health = summarize_health(settings=settings, health_path=health_path, now=observed_now, logger=logger)
    state = summarize_state(settings=settings, state_path=state_path, logger=logger)

    available = snapshot.state == "available" and snapshot.path is not None and snapshot.info is not None
    heading = "Parking monitor latest" if available else "Parking monitor latest unavailable"
    lines = [heading, _format_latest_snapshot_line(snapshot), _format_health_line(health)]
    if health.state == "available":
        lines.append(
            "Runtime: iteration "
            f"{health.iteration}; last frame {health.last_frame_age}; decode mode {health.selected_decode_mode}; "
            f"capture failures {health.consecutive_capture_failures}; detection failures {health.consecutive_detection_failures}"
        )

    if state.state == "available":
        lines.append("Spot decisions:")
        for spot in state.spots[:MAX_LINES_PER_SECTION]:
            lines.append(f"- {spot.spot_id}: {spot.status}; hits {spot.hit_streak}; misses {spot.miss_streak}")
    else:
        suffix = f" ({state.error_type})" if state.error_type else ""
        lines.append(f"State: unavailable{suffix}")
        for spot in state.spots[:MAX_LINES_PER_SECTION]:
            lines.append(f"- {spot.spot_id}: {spot.status}")

    return LatestSnapshotResponse(
        text=_bounded_reply(lines),
        image_path=snapshot.path if available else None,
        image_info=dict(snapshot.info) if available and snapshot.info is not None else None,
    )


def build_who_snapshot_response(
    *,
    settings: RuntimeSettings,
    data_dir: str | Path,
    base_text: str,
    capture_func: Any = capture_latest,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> MatrixCommandResponse:
    """Build a Matrix who reply enriched by one fresh raw capture when available.

    This helper intentionally performs only capture and JPEG validation. It does
    not run detector/model inference and does not read or mutate occupancy
    state.
    """

    observed_now = _utc_now(now)
    try:
        capture = capture_func(settings, Path(data_dir), logger=logger)
        latest_path = Path(capture.latest_path)
        snapshot = _prepare_who_snapshot_for_matrix(latest_path, data_dir=Path(data_dir), now=observed_now, logger=logger)
    except CaptureError as exc:
        reason = redact_diagnostic_text(exc.reason or exc.__class__.__name__)
        _log_snapshot_failure(logger, reason=reason, error_type=exc.__class__.__name__)
        return MatrixCommandResponse(text=_prepend_who_snapshot_line(base_text, _who_snapshot_unavailable_line(reason)))
    except Exception as exc:
        reason = redact_diagnostic_text(exc.__class__.__name__)
        _log_snapshot_failure(logger, reason=reason, error_type=exc.__class__.__name__)
        return MatrixCommandResponse(text=_prepend_who_snapshot_line(base_text, _who_snapshot_unavailable_line(reason)))

    if snapshot.state != "available" or snapshot.path is None or snapshot.info is None:
        reason = redact_diagnostic_text(snapshot.error_type or "unavailable")
        _log_snapshot_failure(logger, reason=reason, error_type="invalid_snapshot")
        return MatrixCommandResponse(text=_prepend_who_snapshot_line(base_text, _who_snapshot_unavailable_line(reason)))

    return MatrixCommandResponse(
        text=_prepend_who_snapshot_line(base_text, f"Snapshot: fresh capture at {_display_time(getattr(capture, 'timestamp', None))}"),
        image_path=snapshot.path,
        image_info=dict(snapshot.info),
    )


def build_incident_review_response(
    *,
    data_dir: str | Path,
    spot_id: str,
    time_text: str,
    settings: RuntimeSettings | None = None,
    state_path: str | Path | None = None,
    detector: Any | None = None,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> MatrixCommandResponse:
    """Build a local incident review from retained timeline frames and decision memory."""

    root = Path(data_dir)
    safe_spot = _safe_incident_spot_id(spot_id)
    observed_now = _utc_now(now)
    target_time = _parse_incident_time(time_text, now=observed_now)
    heading_time = _display_local_time(target_time)
    lines = [f"Incident review: {safe_spot} around {heading_time}"]
    nearest = _nearest_timeline_frame(root, target_time)
    if nearest is None:
        lines.extend([
            "Nearest retained frame: unavailable",
            "No retained timeline frames were found.",
            "No detector, camera, Matrix send, or state mutation was run.",
        ])
        return MatrixCommandResponse(text=_bounded_reply(lines))

    frame_path, frame_time = nearest
    delta_seconds = abs(int((frame_time - target_time).total_seconds()))
    lines.append(f"Nearest retained frame: {_display_local_time(frame_time)} ({delta_seconds}s from requested time)")
    replay = build_incident_replay(
        settings=settings,
        frame_path=frame_path,
        frame_time=frame_time,
        requested_spot_id=safe_spot,
        state_path=state_path,
        detector=detector,
    )
    if replay.unavailable_reason == "corrupt_frame":
        lines.extend(replay.lines)
        return MatrixCommandResponse(text=_bounded_reply(lines))
    lines.extend(replay.lines)
    lines.append("Recent local decision memory:")
    why_lines = format_operator_why_reply(data_dir=root, spot_id=safe_spot, logger=logger).splitlines()
    memory_lines = why_lines[1:7] if len(why_lines) > 1 else why_lines[:1]
    lines.extend(memory_lines or ["No recent decision memory for this spot."])

    snapshot = _prepare_incident_snapshot_for_matrix(frame_path, data_dir=root, spot_id=safe_spot, now=observed_now, logger=logger)
    if snapshot.state != "available" or snapshot.path is None or snapshot.info is None:
        lines.append(f"Frame attachment unavailable: {snapshot.error_type or 'unavailable'}")
        return MatrixCommandResponse(text=_bounded_reply(lines))
    return MatrixCommandResponse(text=_bounded_reply(lines), image_path=snapshot.path, image_info=dict(snapshot.info))


def _safe_incident_spot_id(value: str) -> str:
    text = redact_diagnostic_text(value).strip()
    if text not in {"left_spot", "right_spot"}:
        raise ValueError("invalid spot id")
    return text


def _parse_incident_time(value: str, *, now: datetime) -> datetime:
    text = redact_diagnostic_text(value).strip().lower()
    try:
        parsed = datetime.fromisoformat(text.replace("z", "+00:00"))
    except ValueError:
        parsed = _parse_local_clock_time(text, now=now)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=DISPLAY_TIMEZONE)
    return parsed.astimezone(timezone.utc)


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


def _nearest_timeline_frame(data_dir: Path, target_time: datetime) -> tuple[Path, datetime] | None:
    frames_dir = data_dir / "timeline" / "frames"
    try:
        candidates = list(frames_dir.glob("*.jpg"))
    except OSError:
        return None
    nearest: tuple[Path, datetime] | None = None
    for path in candidates:
        frame_time = _timeline_frame_time(path)
        if frame_time is None:
            continue
        if nearest is None or abs(frame_time - target_time) < abs(nearest[1] - target_time):
            nearest = (path, frame_time)
    return nearest


def _timeline_frame_time(path: Path) -> datetime | None:
    try:
        return datetime.strptime(path.stem, "%Y%m%dT%H%M00Z").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _prepare_incident_snapshot_for_matrix(path: Path, *, data_dir: Path, spot_id: str, now: datetime, logger: StructuredLogger | None) -> LatestSnapshotValidation:
    destination = data_dir / INCIDENT_MATRIX_SNAPSHOT_TEMPLATE.format(spot_id=spot_id)
    try:
        return _resize_who_snapshot_for_matrix(path, destination=destination, now=now, logger=logger)
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        _log_snapshot_failure(logger, reason="incident_resize_failed", error_type=error_type)
        return LatestSnapshotValidation(state="error", error_type="resize failed")


def _display_local_time(value: datetime) -> str:
    local = value.astimezone(DISPLAY_TIMEZONE)
    hour = local.hour % 12 or 12
    suffix = "AM" if local.hour < 12 else "PM"
    return f"{local.year:04d}-{local.month:02d}-{local.day:02d} {hour}:{local.minute:02d} {suffix} PDT"

def format_operator_status_reply(
    *,
    settings: RuntimeSettings,
    health_path: str | Path,
    state_path: str | Path,
    matrix_outbox_path: str | Path | None = None,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, secret-free Matrix status reply from local runtime files."""

    observed_now = _utc_now(now)
    health = summarize_health(settings=settings, health_path=health_path, now=observed_now, logger=logger)
    state = summarize_state(settings=settings, state_path=state_path, logger=logger)
    outbox_lines = _matrix_outbox_status_lines(matrix_outbox_path, logger=logger)

    lines = ["Parking monitor status", _format_health_line(health)]
    if health.state == "available":
        lines.append(
            "Loop: iteration "
            f"{health.iteration}; last frame {health.last_frame_age}; frame interval {health.frame_interval_seconds}s; "
            f"decode mode {health.selected_decode_mode}"
        )
        lines.append(
            "Failures: capture failures "
            f"{health.consecutive_capture_failures}; detection failures {health.consecutive_detection_failures}"
        )

    if state.state == "available":
        lines.append("Spots:")
        for spot in state.spots[:MAX_LINES_PER_SECTION]:
            emitted = "yes" if spot.open_event_emitted else "no"
            lines.append(
                f"- {spot.spot_id}: {spot.status}; hit streak {spot.hit_streak}; miss streak {spot.miss_streak}; open event emitted {emitted}"
            )
        lines.append(
            "Quiet windows: active "
            f"{state.active_quiet_window_count}; notices {state.quiet_window_notice_count}; owner alerts {state.owner_quiet_window_alert_count}"
        )
    else:
        suffix = f" ({state.error_type})" if state.error_type else ""
        lines.append(f"State: unavailable{suffix}")
        for spot in state.spots[:MAX_LINES_PER_SECTION]:
            lines.append(f"- {spot.spot_id}: {spot.status}")

    lines.append("Matrix outbox:")
    lines.extend(outbox_lines)

    return _bounded_reply(lines)


def format_operator_config_reply(
    *,
    settings: RuntimeSettings,
    data_dir: str | Path,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, secret-free Matrix config reply from loaded settings only."""

    del now, logger
    summary = redact_diagnostic_value(settings.sanitized_summary())
    paths = resolve_runtime_paths(settings, data_dir)
    detection = _mapping(summary.get("detection"))
    stream = _mapping(summary.get("stream"))
    occupancy = _mapping(summary.get("occupancy"))
    storage = _mapping(summary.get("storage"))
    runtime = _mapping(summary.get("runtime"))
    matrix = _mapping(summary.get("matrix"))

    lines = [
        "Parking monitor config",
        "Detection: "
        f"model {_text(detection.get('model'))}; confidence threshold {_text(detection.get('confidence_threshold'))}; "
        f"inference image size {_text(detection.get('inference_image_size'), default='default')}; "
        f"{_crop_label(detection.get('spot_crop_inference'))}; crop margin {_int(detection.get('spot_crop_margin_px'))}px",
        "Suppression/classes: "
        f"open suppression threshold {_text(detection.get('open_suppression_min_confidence'))}; "
        f"open suppression classes {_list_label(detection.get('open_suppression_classes'))}; "
        f"vehicle classes {_list_label(detection.get('vehicle_classes'))}",
        "Occupancy: "
        f"iou threshold {_text(occupancy.get('iou_threshold'))}; confirm frames {_text(occupancy.get('confirm_frames'))}; "
        f"release frames {_text(occupancy.get('release_frames'))}; min bbox area {_text(detection.get('min_bbox_area_px'))}; "
        f"min polygon overlap {_text(detection.get('min_polygon_overlap_ratio'))}",
        "Runtime: "
        f"frame interval {_text(runtime.get('frame_interval_seconds'))}s; frame {_text(stream.get('frame_width'))}x{_text(stream.get('frame_height'))}; "
        f"reconnect {_text(stream.get('reconnect_seconds'))}s",
        "Paths: "
        f"data {paths.data_dir}; state {paths.state_file}; health {paths.health_file}; snapshots {paths.snapshots_dir}",
        f"Storage: retention {_text(storage.get('snapshot_retention_count'))} snapshots",
        "Matrix: "
        f"command prefix {_text(matrix.get('command_prefix'))}; authorized senders {_text(matrix.get('command_authorized_senders_count'), default='0')}; "
        f"token {'configured' if _matrix_token_present(matrix) else 'missing'}",
        "Spots:",
    ]

    for spot_id, spot in _spot_items(settings)[:MAX_LINES_PER_SECTION]:
        lines.append(f"- {spot_id}: {spot.name} ({len(spot.polygon)} points)")

    if settings.quiet_windows:
        lines.append("Quiet windows:")
        for window in settings.quiet_windows[:MAX_LINES_PER_SECTION]:
            lines.append(f"- quiet window {window.name}: {window.start}-{window.end} {window.timezone}")
    else:
        lines.append("Quiet windows: none")

    return _bounded_reply(lines)



def format_operator_confidence_reply(
    *,
    settings: RuntimeSettings,
    data_dir: str | Path,
    health_path: str | Path,
    state_path: str | Path,
    matrix_outbox_path: str | Path | None = None,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, read-only confidence report from local artifacts only."""

    observed_now = _utc_now(now)
    root = Path(data_dir)
    health_load = _load_bounded_json_object(Path(health_path), label="health", logger=logger)
    state = summarize_state(settings=settings, state_path=state_path, logger=logger)
    timeline = _summarize_timeline_frames(root / "timeline" / "frames", now=observed_now, logger=logger)
    memory = _load_confidence_memory(root, logger=logger)

    lines = [
        "Parking confidence report",
        "Confidence is conservative and artifact-derived; it is not a calibrated model score.",
        "Spot stability:",
    ]
    confirm_frames = _threshold_value(getattr(getattr(settings, "occupancy", None), "confirm_frames", None), default=1)
    release_frames = _threshold_value(getattr(getattr(settings, "occupancy", None), "release_frames", None), default=confirm_frames)
    state_by_spot = {spot.spot_id: spot for spot in state.spots}
    for spot_id in _spot_ids(settings)[:MAX_LINES_PER_SECTION]:
        spot = state_by_spot.get(spot_id, SpotSummary(spot_id=spot_id, status="unknown", hit_streak=0, miss_streak=0, open_event_emitted=False))
        stability = _classify_spot_stability(spot, confirm_frames=confirm_frames, release_frames=release_frames, state_available=state.state == "available")
        lines.append(
            f"- {spot_id}: {stability}; state {spot.status}; hit streak {spot.hit_streak}/{confirm_frames}; miss streak {spot.miss_streak}/{release_frames}"
        )
    if state.state != "available":
        suffix = f" ({state.error_type})" if state.error_type else ""
        lines.append(f"State artifacts: unavailable{suffix}; configured spot fallbacks shown.")

    lines.append("Weak evidence:")
    lines.extend(_confidence_memory_lines(memory, _spot_ids(settings)))
    lines.append("Timeline health:")
    lines.extend(timeline)
    lines.append("Matrix delivery:")
    lines.extend(_matrix_delivery_lines(health_load, memory))
    lines.extend(_matrix_outbox_status_lines(matrix_outbox_path, logger=logger))
    lines.append("Read-only: no detector, camera, media upload, alert emission, or state mutation was run.")
    return _bounded_reply(lines)



def format_operator_analytics_reply(
    *,
    data_dir: str | Path,
    window: str = "7d",
    now: datetime | None = None,
    detector: Any | None = None,
    health_path: str | Path | None = None,
    state_path: str | Path | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Format bounded spot-level historical occupancy analytics from local vehicle-history JSON only."""

    del detector, health_path, state_path
    observed_now = _utc_now(now)
    root = Path(data_dir)
    closed_sessions, closed_invalid = _load_vehicle_history_session_dicts(root / "vehicle-history" / "sessions" / "closed", logger=logger)
    active_sessions, active_invalid = _load_vehicle_history_session_dicts(root / "vehicle-history" / "sessions" / "active", logger=logger)
    invalid_placeholders = [{} for _ in range(closed_invalid + active_invalid)]
    try:
        result = analyze_occupancy(
            [*closed_sessions, *invalid_placeholders],
            active_sessions=active_sessions,
            window=window,
            now=observed_now,
        )
    except ValueError:
        safe_window = _text(window, default="unknown")
        return _bounded_reply([
            "Parking occupancy analytics unavailable",
            f"Window: {safe_window}",
            "Unsupported window; use today, 7d, 30d, or all.",
            "No detector, camera, Matrix media upload, alert emission, or state mutation was run.",
        ])

    lines = [
        "Parking occupancy analytics",
        f"Window: {result.window.label} ({result.window.started_at or 'beginning'} to {result.window.ended_at})",
        "Source: local vehicle-history sessions; sparse local history can make these metrics noisy.",
        (
            f"Totals: sessions {result.session_count}; closed {result.closed_session_count}; "
            f"active {result.active_session_count}; occupied spots {result.current_occupied_spot_count}."
        ),
    ]

    if result.spots:
        lines.append("Spots:")
        for metric in list(result.spots.values())[:MAX_LINES_PER_SECTION]:
            state = "currently occupied" if metric.currently_occupied else "currently open"
            dwell = _duration_label(metric.average_dwell_seconds) if metric.average_dwell_seconds is not None else "n/a"
            longest = _duration_label(metric.longest_session_seconds) if metric.longest_session_seconds is not None else "n/a"
            lines.append(
                f"- {metric.spot_id}: sessions {metric.session_count}; active {metric.active_session_count}; {state}; "
                f"occupied {_duration_label(metric.occupied_duration_seconds)}; average dwell {dwell}; longest {longest}"
            )
    else:
        lines.append("No vehicle-history sessions overlap the selected window.")

    diagnostics = [*result.diagnostics]
    for metric in result.spots.values():
        diagnostics.extend(metric.diagnostics)
    if diagnostics:
        lines.append("Caveats:")
        seen: set[str] = set()
        for diagnostic in diagnostics[:MAX_LINES_PER_SECTION]:
            message = _text(_mapping(diagnostic).get("message"), default="analytics caveat unavailable")
            if message in seen:
                continue
            seen.add(message)
            lines.append(f"- {message}")
    if closed_invalid + active_invalid:
        count = closed_invalid + active_invalid
        noun = "session" if count == 1 else "sessions"
        lines.append(f"- {count} malformed vehicle-history {noun} ignored during local archive scan.")

    lines.append("Read-only: local vehicle-history JSON was scanned; no detector, camera, Matrix media upload, alert emission, or state mutation was run.")
    lines.append("No detector, camera, Matrix media upload, alert emission, or state mutation was run.")
    return _bounded_reply(lines)


def _load_vehicle_history_session_dicts(directory: Path, *, logger: StructuredLogger | None) -> tuple[list[Mapping[str, Any]], int]:
    try:
        paths = sorted(path for path in directory.glob("*.json") if path.is_file())
    except OSError as exc:
        _log_load_problem(logger, label="vehicle_history", reason="scan_error", error_type=exc.__class__.__name__)
        return [], 1
    records: list[Mapping[str, Any]] = []
    invalid_count = 0
    for path in paths[: MAX_LINES_PER_SECTION * 20]:
        try:
            if path.stat().st_size > MAX_FILE_BYTES:
                invalid_count += 1
                continue
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            invalid_count += 1
            continue
        if isinstance(payload, Mapping):
            records.append(dict(payload))
        else:
            invalid_count += 1
    if len(paths) > MAX_LINES_PER_SECTION * 20:
        invalid_count += len(paths) - (MAX_LINES_PER_SECTION * 20)
    return records, invalid_count


def _duration_label(seconds: int | None) -> str:
    if seconds is None:
        return "n/a"
    remaining = max(0, int(seconds))
    if remaining < 60:
        return f"{remaining}s"
    minutes = remaining // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 48:
        extra_minutes = minutes % 60
        return f"{hours}h {extra_minutes}m" if extra_minutes else f"{hours}h"
    days = hours // 24
    extra_hours = hours % 24
    return f"{days}d {extra_hours}h" if extra_hours else f"{days}d"


def _matrix_outbox_status_lines(matrix_outbox_path: str | Path | None, *, logger: StructuredLogger | None) -> list[str]:
    """Return concise, redacted Matrix outbox lines from LocalOutbox summary fields only."""

    if matrix_outbox_path is None:
        return ["- outbox status unavailable (path not configured)."]
    path = Path(matrix_outbox_path)
    try:
        summary = LocalOutbox(path).status_summary()
    except FileNotFoundError:
        _log_load_problem(logger, label="matrix_outbox", reason="missing", error_type="missing")
        return ["- outbox empty (file missing)."]
    except Exception as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        _log_load_problem(logger, label="matrix_outbox", reason="summary_error", error_type=error_type)
        return [f"- outbox status unavailable ({error_type})."]
    return _format_matrix_outbox_summary_lines(summary)


def _format_matrix_outbox_summary_lines(summary: Mapping[str, Any]) -> list[str]:
    total = _int(summary.get("total"))
    counts = _mapping(summary.get("counts_by_state"))
    state_order = ("pending", "retrying", "delivered", "failed", "dead_lettered")
    if total == 0:
        lines = ["- outbox empty."]
    else:
        count_text = ", ".join(f"{state.replace('_', '-')} {_int(counts.get(state))}" for state in state_order)
        lines = [f"- outbox total {total}: {count_text}."]
        phase_counts = _outbox_phase_counts(summary)
        if phase_counts:
            lines.append("- phase states: " + "; ".join(_format_phase_count(phase, states) for phase, states in phase_counts[:6]) + ".")
        retry_reasons = _reason_count_line("retry reasons", summary.get("retry_reason_counts"))
        if retry_reasons is not None:
            lines.append(retry_reasons)
        dead_reasons = _reason_count_line("dead-letter reasons", summary.get("dead_letter_reason_counts"))
        if dead_reasons is not None:
            lines.append(dead_reasons)
        lines.extend(_outbox_item_lines(summary))

    recovery = _mapping(summary.get("recovery"))
    quarantined = _int(recovery.get("quarantined_count"))
    recovered = _int(recovery.get("recovered_count"))
    if quarantined or recovered:
        reasons = _mapping(recovery.get("reason_counts"))
        reason_text = _format_reason_counts(reasons) if reasons else "none"
        lines.append(f"- recovery: recovered {recovered}; quarantined {quarantined}; reasons {reason_text}.")
    return lines[:MAX_LINES_PER_SECTION]


def _outbox_phase_counts(summary: Mapping[str, Any]) -> list[tuple[str, dict[str, int]]]:
    counts: dict[str, dict[str, int]] = {}
    items = summary.get("items")
    if not isinstance(items, list):
        return []
    for item in items[:MAX_LINES_PER_SECTION]:
        if not isinstance(item, Mapping):
            continue
        phases = item.get("phases")
        if not isinstance(phases, list):
            continue
        for phase_item in phases[:6]:
            phase_map = _mapping(phase_item)
            phase = _text(phase_map.get("phase"), default="unknown")
            state = _text(phase_map.get("state"), default="unknown")
            if phase == "unknown" or state == "unknown":
                continue
            state_counts = counts.setdefault(phase, {})
            state_counts[state] = state_counts.get(state, 0) + 1
    return sorted(counts.items())


def _format_phase_count(phase: str, states: Mapping[str, int]) -> str:
    state_order = ("pending", "delivered", "failed")
    rendered = ", ".join(f"{state} {_int(states.get(state))}" for state in state_order if _int(states.get(state)))
    return f"{_text(phase)} {rendered or 'none'}"


def _reason_count_line(label: str, value: Any) -> str | None:
    counts = _mapping(value)
    if not counts:
        return None
    return f"- {label}: {_format_reason_counts(counts)}."


def _format_reason_counts(counts: Mapping[str, Any]) -> str:
    return ", ".join(f"{_text(key)} {_int(value)}" for key, value in list(counts.items())[:8]) or "none"


def _outbox_item_lines(summary: Mapping[str, Any]) -> list[str]:
    items = summary.get("items")
    if not isinstance(items, list):
        return []
    lines: list[str] = []
    for item in items[: min(5, MAX_LINES_PER_SECTION)]:
        if not isinstance(item, Mapping):
            continue
        state = _text(item.get("state"))
        phase = _text(item.get("phase"))
        parts = [f"state {state}", f"phase {phase}"]
        retry_reason = _text(item.get("retry_reason"), default="")
        dead_reason = _text(item.get("dead_letter_reason"), default="")
        if retry_reason:
            parts.append(f"retry {retry_reason}")
        if dead_reason:
            parts.append(f"dead-letter {dead_reason}")
        phase_states = []
        phases = item.get("phases")
        if isinstance(phases, list):
            for phase_item in phases[:3]:
                phase_map = _mapping(phase_item)
                phase_states.append(f"{_text(phase_map.get('phase'))}={_text(phase_map.get('state'))}")
        if phase_states:
            parts.append("phases " + ", ".join(phase_states))
        lines.append("- record: " + "; ".join(parts) + ".")
    if len(items) > len(lines):
        lines.append(f"- records truncated: showing {len(lines)} of {len(items)}.")
    return lines

def _threshold_value(value: Any, *, default: int) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else max(1, default)


def _classify_spot_stability(spot: SpotSummary, *, confirm_frames: int, release_frames: int, state_available: bool) -> str:
    if not state_available or spot.status not in {"open", "occupied"}:
        return "unavailable"
    if spot.status == "occupied":
        if spot.hit_streak >= confirm_frames:
            return "stable occupied"
        return "weak occupied"
    if spot.miss_streak >= release_frames:
        return "stable open"
    return "weak open"


def _load_confidence_memory(data_dir: Path, *, logger: StructuredLogger | None) -> Any:
    from parking_spot_monitor.operator_decision_memory import decision_memory_path, load_decision_memory

    return load_decision_memory(decision_memory_path(data_dir), logger=logger)


def _confidence_memory_lines(memory: Any, spot_ids: Sequence[str]) -> list[str]:
    if getattr(memory, "state", None) != "available":
        suffix = f" ({_text(getattr(memory, 'error_type', None))})" if getattr(memory, "error_type", None) else ""
        return [f"- decision memory unavailable{suffix}; continuing with state and timeline artifacts."]

    weak_kinds = {"confidence_dip", "rejected_evidence", "miss", "suppression"}
    records = list(getattr(memory, "records", ()) or ())
    lines: list[str] = []
    for spot_id in spot_ids[:MAX_LINES_PER_SECTION]:
        matches = [record for record in records if getattr(record, "spot_id", None) == spot_id and getattr(record, "kind", "") in weak_kinds]
        if not matches:
            lines.append(f"- {spot_id}: no recent weak evidence records.")
            continue
        snippets = []
        for record in matches[-3:]:
            summary = _text(getattr(record, "summary", ""), default="no summary")
            snippets.append(f"{_text(getattr(record, 'kind', 'unknown'))}: {summary}")
        lines.append(f"- {spot_id}: " + "; ".join(snippets))
    return lines or ["- no configured spots available for weak-evidence summary."]


def _summarize_timeline_frames(frames_dir: Path, *, now: datetime, logger: StructuredLogger | None) -> list[str]:
    try:
        paths = [path for path in frames_dir.iterdir() if path.is_file()]
    except FileNotFoundError:
        _log_load_problem(logger, label="timeline", reason="missing", error_type="missing")
        return ["- unavailable (missing timeline frames directory)."]
    except OSError as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        _log_load_problem(logger, label="timeline", reason="scan_error", error_type=error_type)
        return [f"- unavailable ({error_type})."]

    parsed = [_timeline_frame_time(path) for path in paths[: MAX_LINES_PER_SECTION * 20]]
    valid_times = sorted(time for time in parsed if time is not None)
    ignored = len([time for time in parsed if time is None])
    if not valid_times:
        suffix = f"; ignored {ignored} unparseable filename(s)" if ignored else ""
        return [f"- no timestamped retained frames found{suffix}."]
    oldest = valid_times[0]
    newest = valid_times[-1]
    newest_age = _age_label(newest, now)
    return [
        f"- retained timestamped frames {len(valid_times)}; newest {newest_age}; oldest {_age_label(oldest, now)}.",
        f"- filename scan only; image bytes were not opened; ignored {ignored} unparseable filename(s).",
    ]


def _matrix_delivery_lines(health_load: BoundedJsonLoad, memory: Any) -> list[str]:
    lines: list[str] = []
    if health_load.state != "available" or health_load.payload is None:
        suffix = f" ({health_load.error_type})" if health_load.error_type else ""
        lines.append(f"- health unavailable{suffix}; Matrix error status unknown.")
    else:
        matrix_error = _mapping(health_load.payload.get("last_matrix_error"))
        if matrix_error:
            error_type = _text(matrix_error.get("error_type") or matrix_error.get("type") or matrix_error.get("status"), default="error")
            lines.append(f"- last Matrix error: {error_type}.")
        else:
            lines.append("- no last Matrix error recorded in health.")

    if getattr(memory, "state", None) == "available":
        records = [record for record in getattr(memory, "records", ()) if getattr(record, "kind", "") in {"command_outcome", "alert"}]
        if records:
            record = records[-1]
            lines.append(f"- recent delivery memory: {_text(getattr(record, 'kind', 'unknown'))}: {_text(getattr(record, 'summary', ''), default='no summary')}.")
        else:
            lines.append("- no recent command or alert outcome memory.")
    else:
        lines.append("- delivery memory unavailable.")
    return lines

def format_operator_why_reply(
    *,
    data_dir: str | Path,
    spot_id: str,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, redacted decision-memory explanation for one spot."""

    from parking_spot_monitor.operator_decision_memory import decision_memory_path, format_why_reply

    return format_why_reply(decision_memory_path(data_dir), spot_id, logger=logger)


def format_operator_recent_reply(
    *,
    data_dir: str | Path,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, redacted recent decision-memory timeline."""

    from parking_spot_monitor.operator_decision_memory import decision_memory_path, format_recent_reply

    return format_recent_reply(decision_memory_path(data_dir), logger=logger)


def format_detection_lab_run_reply(
    *,
    data_dir: str | Path,
    kind: str,
    manager: Any | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Start a bounded local detection-lab job and return a text-only Matrix reply."""

    lab_manager = _detection_lab_manager(data_dir, manager=manager, logger=logger)
    try:
        if kind == "replay":
            job = lab_manager.start_replay()
        elif kind == "tuning":
            job = lab_manager.start_tuning()
        else:
            return _bounded_reply(["Detection lab run unavailable", "Error: invalid_job_kind; use replay or tuning", "No detector, camera, shell, or live occupancy work was run by this reply path."])
    except Exception as exc:
        _log_lab_problem(logger, reason="start_failed", error_type=exc.__class__.__name__)
        return _bounded_reply(["Detection lab run unavailable", f"Error: {redact_diagnostic_text(exc.__class__.__name__)}", "No detector, camera, shell, or live occupancy work was run by this reply path."])

    return _bounded_reply([
        "Detection lab job started",
        f"Job: {job.job_id}",
        f"Kind: {job.kind}",
        "Status: queued or blocked; use !parking lab status latest for the persisted redacted status.",
        "Inputs: fixed local detection-lab files under the runtime data directory.",
    ])


def format_detection_lab_status_reply(
    *,
    data_dir: str | Path,
    job_id: str = "latest",
    manager: Any | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, redacted detection-lab job status from local artifacts."""

    lab_manager = _detection_lab_manager(data_dir, manager=manager, logger=logger)
    try:
        status = lab_manager.summarize(job_id or "latest")
    except Exception as exc:
        code = _text(getattr(exc, "code", None), default=redact_diagnostic_text(exc.__class__.__name__))
        message = _text(getattr(exc, "message", None) or str(exc), default="unavailable")
        _log_lab_problem(logger, reason="status_unavailable", error_type=exc.__class__.__name__, error_code=code)
        return _bounded_reply([
            "Detection lab status unavailable",
            f"Lookup: {_text(job_id or 'latest')}",
            f"Error: {code}; {message}",
            "No detector, camera, shell, or live occupancy work was run by this reply path.",
        ])

    return _bounded_reply(_format_lab_status_lines(status))

def summarize_health(
    *,
    settings: RuntimeSettings,
    health_path: str | Path,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> HealthSummary:
    observed_now = _utc_now(now)
    loaded = _load_bounded_json_object(Path(health_path), label="health", logger=logger)
    frame_interval = getattr(getattr(settings, "runtime", None), "frame_interval_seconds", "unknown")
    if loaded.state != "available" or loaded.payload is None:
        return HealthSummary(state=loaded.state, frame_interval_seconds=frame_interval, error_type=loaded.error_type)

    payload = loaded.payload
    updated_at = _parse_time(payload.get("updated_at"))
    freshness = _freshness(updated_at, observed_now, frame_interval)
    capture = _mapping(payload.get("capture"))
    last_frame_at = _parse_time(_first_present(payload, "last_frame_at") or _first_present(capture, "last_success_at"))
    iteration = payload.get("iteration")
    return HealthSummary(
        state="available",
        status=_text(payload.get("status"), default="unavailable"),
        freshness=freshness,
        updated_at=updated_at,
        updated_age=_age_label(updated_at, observed_now),
        iteration=str(iteration) if isinstance(iteration, int) and not isinstance(iteration, bool) else "unknown",
        last_frame_age=_age_label(last_frame_at, observed_now),
        frame_interval_seconds=frame_interval,
        selected_decode_mode=_text(
            _first_present(payload, "selected_decode_mode") or _first_present(capture, "selected_decode_mode"),
            default="unknown",
        ),
        consecutive_capture_failures=_int(payload.get("consecutive_capture_failures")),
        consecutive_detection_failures=_int(payload.get("consecutive_detection_failures")),
    )


def summarize_state(*, settings: RuntimeSettings, state_path: str | Path, logger: StructuredLogger | None = None) -> StateSummary:
    configured = _spot_ids(settings)
    fallback_spots = tuple(SpotSummary(spot_id=spot_id, status="open", hit_streak=0, miss_streak=0, open_event_emitted=False) for spot_id in configured)
    loaded = _load_bounded_json_object(Path(state_path), label="state", logger=logger)
    if loaded.state != "available" or loaded.payload is None:
        return StateSummary(state=loaded.state, spots=fallback_spots, error_type=loaded.error_type)

    payload = loaded.payload
    raw_spots = payload.get("spots")
    if not isinstance(raw_spots, Mapping):
        return StateSummary(state="error", spots=fallback_spots, error_type="schema_error")

    spots: list[SpotSummary] = []
    for spot_id in configured:
        raw = raw_spots.get(spot_id)
        spot = _mapping(raw)
        spots.append(
            SpotSummary(
                spot_id=spot_id,
                status=_spot_status(spot.get("status")),
                hit_streak=_int(spot.get("hit_streak")),
                miss_streak=_int(spot.get("miss_streak")),
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


def _prepend_who_snapshot_line(base_text: str, snapshot_line: str) -> str:
    lines = base_text.splitlines()
    if not lines:
        return _bounded_multiline_reply(["Parking monitor who", snapshot_line])
    return _bounded_multiline_reply([lines[0], snapshot_line, "", *lines[1:]])


def _bounded_multiline_reply(lines: Sequence[str]) -> str:
    rendered = "\n".join(redact_diagnostic_text(line) for line in lines[: MAX_LINES_PER_SECTION * 3])
    encoded = rendered.encode("utf-8")
    if len(encoded) <= MAX_REPLY_BYTES:
        return rendered
    return encoded[: MAX_REPLY_BYTES - 3].decode("utf-8", errors="ignore") + "..."


def _who_snapshot_unavailable_line(reason: str) -> str:
    safe_reason = redact_diagnostic_text(reason)[:120] or "unavailable"
    return f"Snapshot: fresh capture unavailable ({safe_reason}); no live state was changed."


def _display_time(value: object) -> str:
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return redact_diagnostic_text(value)[:80] or "unknown"
    if parsed is None:
        return "unknown"
    return _utc_now(parsed).isoformat().replace("+00:00", "Z")


def _log_snapshot_failure(logger: StructuredLogger | None, **fields: Any) -> None:
    if logger is not None:
        logger.warning("operator-who-snapshot-unavailable", **redact_diagnostic_value(fields))

def _prepare_who_snapshot_for_matrix(path: Path, *, data_dir: Path, now: datetime, logger: StructuredLogger | None) -> LatestSnapshotValidation:
    snapshot = _validate_latest_snapshot(path, now=now, logger=logger)
    if snapshot.state == "available":
        return snapshot
    if snapshot.error_type != "too large":
        return snapshot

    destination = data_dir / WHO_MATRIX_SNAPSHOT_FILENAME
    try:
        return _resize_who_snapshot_for_matrix(path, destination=destination, now=now, logger=logger)
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        _log_snapshot_failure(logger, reason="resize_failed", error_type=error_type)
        return LatestSnapshotValidation(state="error", error_type="resize failed")


def _resize_who_snapshot_for_matrix(path: Path, *, destination: Path, now: datetime, logger: StructuredLogger | None) -> LatestSnapshotValidation:
    with Image.open(path) as image:
        working = image.convert("RGB")
    width, height = working.size
    if width <= 0 or height <= 0:
        raise ValueError("invalid image dimensions")

    max_dimension = min(max(width, height), WHO_MATRIX_INITIAL_MAX_DIMENSION)
    resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
    destination.parent.mkdir(parents=True, exist_ok=True)
    while max_dimension >= WHO_MATRIX_MIN_DIMENSION:
        resized = working.copy()
        resized.thumbnail((max_dimension, max_dimension), resampling)
        for quality in WHO_MATRIX_JPEG_QUALITIES:
            resized.save(destination, format="JPEG", quality=quality, optimize=True)
            stat = destination.stat()
            if stat.st_size <= MAX_WHO_MATRIX_IMAGE_BYTES:
                output_width, output_height = resized.size
                _log_who_snapshot_resized(
                    logger,
                    source_path=path,
                    destination_path=destination,
                    source_width=width,
                    source_height=height,
                    output_width=output_width,
                    output_height=output_height,
                    byte_size=stat.st_size,
                    quality=quality,
                )
                return LatestSnapshotValidation(
                    state="available",
                    path=destination,
                    info={"mimetype": "image/jpeg", "size": stat.st_size, "w": output_width, "h": output_height},
                    freshness="fresh",
                    age=_age_label(datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc), now),
                )
        max_dimension = int(max_dimension * 0.85)
    raise ValueError("resized image exceeds Matrix upload budget")


def _log_who_snapshot_resized(logger: StructuredLogger | None, **fields: Any) -> None:
    if logger is not None:
        logger.info("operator-who-snapshot-resized", **redact_diagnostic_value(fields))


def _validate_latest_snapshot(path: Path, *, now: datetime, logger: StructuredLogger | None) -> LatestSnapshotValidation:
    if path.name != "latest.jpg":
        error_type = "debug overlay" if "debug" in path.name.lower() else "invalid latest path"
        _log_latest_snapshot_problem(logger, reason="invalid_name", error_type=error_type)
        return LatestSnapshotValidation(state="unavailable", error_type=error_type)
    try:
        stat = path.stat()
    except FileNotFoundError:
        _log_latest_snapshot_problem(logger, reason="missing", error_type="missing")
        return LatestSnapshotValidation(state="unavailable", error_type="missing")
    except OSError as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        _log_latest_snapshot_problem(logger, reason="stat_error", error_type=error_type)
        return LatestSnapshotValidation(state="unavailable", error_type=error_type)
    if not path.is_file():
        _log_latest_snapshot_problem(logger, reason="not_file", error_type="not a file")
        return LatestSnapshotValidation(state="unavailable", error_type="not a file")
    if stat.st_size > MAX_LATEST_IMAGE_BYTES:
        _log_latest_snapshot_problem(logger, reason="too_large", error_type="too large", byte_size=stat.st_size)
        return LatestSnapshotValidation(state="error", error_type="too large")
    try:
        with Image.open(path) as image:
            image.verify()
        with Image.open(path) as image:
            width, height = image.size
            image_format = image.format
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        _log_latest_snapshot_problem(logger, reason="invalid_jpeg", error_type="invalid JPEG", exception_type=exc.__class__.__name__)
        return LatestSnapshotValidation(state="error", error_type="invalid JPEG")
    if image_format != "JPEG" or width <= 0 or height <= 0:
        _log_latest_snapshot_problem(logger, reason="invalid_jpeg_metadata", error_type="invalid JPEG")
        return LatestSnapshotValidation(state="error", error_type="invalid JPEG")
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    return LatestSnapshotValidation(
        state="available",
        path=path,
        info={"mimetype": "image/jpeg", "size": stat.st_size, "w": width, "h": height},
        freshness="fresh",
        age=_age_label(mtime, now),
    )


def _format_latest_snapshot_line(snapshot: LatestSnapshotValidation) -> str:
    if snapshot.state == "available" and snapshot.info is not None:
        return (
            "Snapshot: fresh raw latest.jpg; "
            f"{snapshot.info['w']}x{snapshot.info['h']}; size {snapshot.info['size']} bytes; modified {snapshot.age}"
        )
    suffix = f": {snapshot.error_type}" if snapshot.error_type else ""
    return f"Snapshot: unavailable{suffix}"


def _log_latest_snapshot_problem(logger: StructuredLogger | None, **fields: Any) -> None:
    if logger is None:
        return
    logger.warning("matrix-latest-snapshot-invalid", **redact_diagnostic_value(fields))


def _log_lab_problem(logger: StructuredLogger | None, **fields: Any) -> None:
    if logger is None:
        return
    logger.warning("matrix-detection-lab-unavailable", **redact_diagnostic_value(fields))


def _detection_lab_manager(data_dir: str | Path, *, manager: Any | None, logger: StructuredLogger | None) -> Any:
    if manager is not None:
        return manager
    from parking_spot_monitor.detection_lab import DetectionLabManager

    return DetectionLabManager(data_dir, logger=logger)


def _format_lab_status_lines(status: Mapping[str, Any]) -> list[str]:
    lines = [
        "Detection lab status",
        f"Job: {_text(status.get('job_id'))}",
        f"Kind: {_text(status.get('kind'))}",
        f"Status: {_text(status.get('status'))}; phase {_text(status.get('phase'))}",
    ]
    if status.get("created_at") or status.get("updated_at"):
        lines.append(f"Timestamps: created {_text(status.get('created_at'))}; updated {_text(status.get('updated_at'))}")
    if status.get("report_path"):
        lines.append(f"Report: {_text(status.get('report_path'))}")
    error = _mapping(status.get("error"))
    if error:
        lines.append(f"Error: {_text(error.get('code'))}; {_text(error.get('message'))}")
    summary = _mapping(status.get("summary"))
    if summary:
        lines.append("Summary:")
        for line in _format_lab_summary_lines(summary):
            lines.append(line)
    return lines


def _format_lab_summary_lines(summary: Mapping[str, Any]) -> list[str]:
    lines: list[str] = []
    status_counts = _mapping(summary.get("status_counts"))
    if status_counts:
        counts = ", ".join(f"{_text(key)}={_int(value)}" for key, value in list(status_counts.items())[:8])
        lines.append(f"- status counts: {counts}")
    coverage = _mapping(summary.get("coverage"))
    if coverage:
        lines.append(
            "- coverage: assessed "
            f"{_int(coverage.get('assessed_frames'))}; blocked {_int(coverage.get('blocked_frames'))}; "
            f"not assessed {_int(coverage.get('not_assessed_frames'))}"
        )
    threshold = _mapping(summary.get("shared_threshold_sufficiency"))
    if threshold:
        lines.append(f"- threshold: {_text(threshold.get('verdict'))}; {_text(threshold.get('rationale'), default='')}")
    if summary.get("decision"):
        lines.append(f"- decision: {_text(summary.get('decision'))}; {_text(summary.get('decision_rationale'), default='')}")
    deltas = _mapping(summary.get("metric_delta_totals"))
    if deltas:
        rendered = ", ".join(f"{_text(key)}={_int(value)}" for key, value in list(deltas.items())[:8])
        lines.append(f"- metric deltas: {rendered}")
    redaction = _mapping(summary.get("redaction"))
    if redaction:
        findings = redaction.get("findings")
        finding_count = len(findings) if isinstance(findings, list) else 0
        lines.append(f"- redaction: passed {str(redaction.get('passed') is True).lower()}; findings {finding_count}")
    if summary.get("missing_inputs"):
        missing = summary.get("missing_inputs")
        if isinstance(missing, list):
            lines.append("- missing fixed inputs: " + ", ".join(_text(item) for item in missing[:8]))
    if not lines:
        lines.append("- no report summary available yet")
    return lines[:MAX_LINES_PER_SECTION]

def _load_bounded_json_object(path: Path, *, label: str, logger: StructuredLogger | None) -> BoundedJsonLoad:
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        _log_load_problem(logger, label=label, reason="missing", error_type="FileNotFoundError")
        return BoundedJsonLoad(state="unavailable", error_type="missing")
    except OSError as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        _log_load_problem(logger, label=label, reason="stat_error", error_type=error_type)
        return BoundedJsonLoad(state="unavailable", error_type=error_type)
    if size > MAX_FILE_BYTES:
        _log_load_problem(logger, label=label, reason="too_large", error_type="file_too_large", byte_size=size)
        return BoundedJsonLoad(state="error", error_type="file_too_large")
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as exc:
        _log_load_problem(logger, label=label, reason="json_parse", error_type=exc.__class__.__name__)
        return BoundedJsonLoad(state="unavailable", error_type="JSONDecodeError")
    except OSError as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        _log_load_problem(logger, label=label, reason="read_error", error_type=error_type)
        return BoundedJsonLoad(state="unavailable", error_type=error_type)
    if not isinstance(payload, Mapping):
        _log_load_problem(logger, label=label, reason="schema", error_type="non_object_payload")
        return BoundedJsonLoad(state="error", error_type="non_object_payload")
    return BoundedJsonLoad(state="available", payload=dict(payload))


def _format_health_line(health: HealthSummary) -> str:
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


def _log_load_problem(logger: StructuredLogger | None, **fields: Any) -> None:
    if logger is None:
        return
    logger.warning("matrix-operator-runtime-load", **redact_diagnostic_value(fields))


def _spot_items(settings: RuntimeSettings) -> list[tuple[str, Any]]:
    spots = getattr(settings, "spots", None)
    if spots is None:
        return []
    return [("left_spot", spots.left_spot), ("right_spot", spots.right_spot)]


def _spot_ids(settings: RuntimeSettings) -> list[str]:
    names = [spot_id for spot_id, _spot in _spot_items(settings)]
    return names or ["left_spot", "right_spot"]


def _spot_status(value: Any) -> str:
    text = _text(value, default="unknown")
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


def _age_label(value: datetime | None, now: datetime) -> str:
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


def _bounded_reply(lines: Sequence[str]) -> str:
    rendered = redact_diagnostic_text("\n".join(_bounded_lines(lines)))
    encoded = rendered.encode("utf-8")
    if len(encoded) <= MAX_REPLY_BYTES:
        return rendered
    return encoded[: MAX_REPLY_BYTES - 3].decode("utf-8", errors="ignore") + "..."


def _bounded_lines(lines: Sequence[str]) -> list[str]:
    bounded = [redact_diagnostic_text(line) for line in lines[: MAX_LINES_PER_SECTION * 3]]
    if len(lines) > len(bounded):
        bounded.append("... truncated")
    return bounded


def _utc_now(value: datetime | None) -> datetime:
    selected = value if value is not None else datetime.now(timezone.utc)
    if selected.tzinfo is None:
        return selected.replace(tzinfo=timezone.utc)
    return selected.astimezone(timezone.utc)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_present(mapping: Mapping[str, Any], key: str) -> Any:
    value = mapping.get(key)
    return value if value not in (None, "") else None


def _text(value: Any, *, default: str = "unknown") -> str:
    if value is None:
        return default
    if isinstance(value, bool):
        return "true" if value else "false"
    text = redact_diagnostic_text(value)
    return text[:160] if text else default


def _int(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _bounded_count(value: Any) -> int:
    if not isinstance(value, list | tuple | set | frozenset):
        return 0
    return min(len(value), MAX_LINES_PER_SECTION)


def _crop_label(enabled: Any) -> str:
    return "crop enabled" if bool(enabled) else "crop disabled"


def _list_label(value: Any) -> str:
    if not isinstance(value, list | tuple):
        return "none"
    items = [_text(item) for item in value[:8]]
    suffix = ", ..." if len(value) > len(items) else ""
    return ", ".join(items) + suffix if items else "none"


def _matrix_token_present(matrix: Mapping[str, Any]) -> bool:
    token = _mapping(matrix.get("matrix_token"))
    return token.get("present") is True
