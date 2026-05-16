from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.paths import resolve_runtime_paths

MAX_REPLY_BYTES = 4096
MAX_FILE_BYTES = 256_000
MAX_LATEST_IMAGE_BYTES = 300_000
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


def format_operator_status_reply(
    *,
    settings: RuntimeSettings,
    health_path: str | Path,
    state_path: str | Path,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, secret-free Matrix status reply from local runtime files."""

    observed_now = _utc_now(now)
    health = summarize_health(settings=settings, health_path=health_path, now=observed_now, logger=logger)
    state = summarize_state(settings=settings, state_path=state_path, logger=logger)

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
