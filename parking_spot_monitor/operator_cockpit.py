from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.occupancy_analytics import analyze_occupancy
from parking_spot_monitor.paths import resolve_runtime_paths
from parking_spot_monitor.operator_cockpit_shared import (
    MAX_FILE_BYTES,
    MAX_LINES_PER_SECTION,
    BoundedJsonLoad,
    SpotSummary,
    _age_label,
    _bounded_reply,
    _crop_label,
    _format_health_line,
    _int,
    _list_label,
    _load_bounded_json_object,
    _log_load_problem,
    _mapping,
    _matrix_token_present,
    _spot_ids,
    _spot_items,
    _text,
    _utc_now,
    summarize_health,
    summarize_state,
)
from parking_spot_monitor.operator_cockpit_snapshots import (
    LatestSnapshotResponse,
    LatestSnapshotValidation,
    _display_time,
    _nearest_timeline_frame,
    _parse_incident_time,
    _timeline_frame_time,
    build_incident_review_response,
    build_latest_snapshot_response,
    build_who_snapshot_response,
)



















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
        f"Window: {result.window.label}",
        f"Range: {result.window.started_at or 'beginning'} → {result.window.ended_at}",
        "Source: local vehicle-history sessions; sparse local history can make these metrics noisy.",
        "",
        "Totals",
        f"- Sessions: {result.session_count}",
        f"- Closed: {result.closed_session_count}",
        f"- Active: {result.active_session_count}",
        f"- Currently occupied spots: {result.current_occupied_spot_count}",
    ]

    spot_labels = {metric.spot_id: _analytics_spot_label(metric.spot_id) for metric in result.spots.values()}
    if result.spots:
        lines.extend(["", "Spots"])
        for index, metric in enumerate(list(result.spots.values())[:MAX_LINES_PER_SECTION]):
            if index:
                lines.append("")
            state = "occupied" if metric.currently_occupied else "open"
            dwell = _duration_label(metric.average_dwell_seconds) if metric.average_dwell_seconds is not None else "n/a"
            longest = _duration_label(metric.longest_session_seconds) if metric.longest_session_seconds is not None else "n/a"
            lines.extend([
                spot_labels.get(metric.spot_id, "unknown_spot"),
                f"- Sessions: {metric.session_count}",
                f"- Active: {metric.active_session_count}",
                f"- Status: {state}",
                f"- Occupied: {_duration_label(metric.occupied_duration_seconds)}",
                f"- Average dwell: {dwell}",
                f"- Longest dwell: {longest}",
            ])
    else:
        lines.extend(["", "No vehicle-history sessions overlap the selected window."])

    diagnostics = [*result.diagnostics]
    for metric in result.spots.values():
        diagnostics.extend(metric.diagnostics)
    if diagnostics:
        lines.append("Caveats:")
        seen: set[str] = set()
        for diagnostic in diagnostics[:MAX_LINES_PER_SECTION]:
            message = _analytics_diagnostic_message(_mapping(diagnostic).get("message"), spot_labels)
            if message in seen:
                continue
            seen.add(message)
            lines.append(f"- {message}")
    if closed_invalid + active_invalid:
        count = closed_invalid + active_invalid
        noun = "session" if count == 1 else "sessions"
        lines.append(f"- {count} malformed vehicle-history {noun} ignored during local archive scan.")

    lines.extend([
        "",
        "Read-only",
        "Scanned local vehicle-history JSON only. No detector, camera, Matrix media upload, alert emission, or state mutation was run.",
    ])
    return _bounded_reply(lines)


_SAFE_ANALYTICS_SPOT_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")


def _analytics_spot_label(value: object) -> str:
    text = _text(value, default="unknown_spot")[:80]
    if not _SAFE_ANALYTICS_SPOT_RE.fullmatch(text):
        return "unknown_spot"
    if ".." in text:
        return "unknown_spot"
    return text


def _analytics_diagnostic_message(value: object, spot_labels: Mapping[str, str]) -> str:
    message = _text(value, default="analytics caveat unavailable")
    for raw, safe in sorted(spot_labels.items(), key=lambda item: len(item[0]), reverse=True):
        if raw and raw != safe:
            message = message.replace(raw, safe)
    return message


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
