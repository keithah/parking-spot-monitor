from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.capture import CaptureError, capture_latest
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.incident_review import build_incident_replay
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.matrix_models import MatrixCommandResponse
from parking_spot_monitor.paths import resolve_runtime_paths
from parking_spot_monitor.operator_cockpit_shared import (
    DISPLAY_TIMEZONE,
    INCIDENT_MATRIX_SNAPSHOT_TEMPLATE,
    MAX_LINES_PER_SECTION,
    MAX_LATEST_IMAGE_BYTES,
    MAX_REPLY_BYTES,
    MAX_WHO_MATRIX_IMAGE_BYTES,
    WHO_MATRIX_INITIAL_MAX_DIMENSION,
    WHO_MATRIX_JPEG_QUALITIES,
    WHO_MATRIX_MIN_DIMENSION,
    WHO_MATRIX_SNAPSHOT_FILENAME,
    LatestSnapshotResponse,
    LatestSnapshotValidation,
    _age_label,
    _bounded_lines,
    _bounded_reply,
    _format_health_line,
    _utc_now,
    summarize_health,
    summarize_state,
)


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
    from parking_spot_monitor.operator_cockpit import format_operator_why_reply

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
