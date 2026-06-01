from __future__ import annotations

from datetime import datetime
from pathlib import Path

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_value
from parking_spot_monitor.operator_cockpit_analytics import format_operator_analytics_reply
from parking_spot_monitor.operator_cockpit_confidence import format_operator_confidence_reply
from parking_spot_monitor.operator_cockpit_lab import format_detection_lab_run_reply, format_detection_lab_status_reply
from parking_spot_monitor.operator_cockpit_memory import format_operator_recent_reply, format_operator_why_reply
from parking_spot_monitor.operator_cockpit_outbox import matrix_outbox_status_lines
from parking_spot_monitor.operator_cockpit_shared import (
    MAX_LINES_PER_SECTION,
    bounded_reply,
    crop_label,
    format_health_line,
    int_value,
    list_label,
    mapping_value,
    matrix_token_present,
    spot_items,
    text_value,
    summarize_health,
    summarize_state,
    utc_now,
)
from parking_spot_monitor.operator_cockpit_snapshots import (
    LatestSnapshotResponse,
    LatestSnapshotValidation,
    build_incident_review_response,
    build_latest_snapshot_response,
    build_who_snapshot_response,
    display_time,
)
from parking_spot_monitor.paths import resolve_runtime_paths


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

    observed_now = utc_now(now)
    health = summarize_health(settings=settings, health_path=health_path, now=observed_now, logger=logger)
    state = summarize_state(settings=settings, state_path=state_path, logger=logger)
    outbox_lines = matrix_outbox_status_lines(matrix_outbox_path, logger=logger)

    lines = ["Parking monitor status", format_health_line(health)]
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

    return bounded_reply(lines)


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
    detection = mapping_value(summary.get("detection"))
    stream = mapping_value(summary.get("stream"))
    occupancy = mapping_value(summary.get("occupancy"))
    storage = mapping_value(summary.get("storage"))
    runtime = mapping_value(summary.get("runtime"))
    matrix = mapping_value(summary.get("matrix"))

    lines = [
        "Parking monitor config",
        "Detection: "
        f"model {text_value(detection.get('model'))}; confidence threshold {text_value(detection.get('confidence_threshold'))}; "
        f"inference image size {text_value(detection.get('inference_image_size'), default='default')}; "
        f"{crop_label(detection.get('spot_crop_inference'))}; crop margin {int_value(detection.get('spot_crop_margin_px'))}px",
        "Suppression/classes: "
        f"open suppression threshold {text_value(detection.get('open_suppression_min_confidence'))}; "
        f"open suppression classes {list_label(detection.get('open_suppression_classes'))}; "
        f"vehicle classes {list_label(detection.get('vehicle_classes'))}",
        "Occupancy: "
        f"iou threshold {text_value(occupancy.get('iou_threshold'))}; confirm frames {text_value(occupancy.get('confirm_frames'))}; "
        f"release frames {text_value(occupancy.get('release_frames'))}; min bbox area {text_value(detection.get('min_bbox_area_px'))}; "
        f"min polygon overlap {text_value(detection.get('min_polygon_overlap_ratio'))}",
        "Runtime: "
        f"frame interval {text_value(runtime.get('frame_interval_seconds'))}s; frame {text_value(stream.get('frame_width'))}x{text_value(stream.get('frame_height'))}; "
        f"reconnect {text_value(stream.get('reconnect_seconds'))}s",
        "Paths: "
        f"data {paths.data_dir}; state {paths.state_file}; health {paths.health_file}; snapshots {paths.snapshots_dir}",
        f"Storage: retention {text_value(storage.get('snapshot_retention_count'))} snapshots",
        "Matrix: "
        f"command prefix {text_value(matrix.get('command_prefix'))}; authorized senders {text_value(matrix.get('command_authorized_senders_count'), default='0')}; "
        f"token {'configured' if matrix_token_present(matrix) else 'missing'}",
        "Spots:",
    ]

    for spot_id, spot in spot_items(settings)[:MAX_LINES_PER_SECTION]:
        lines.append(f"- {spot_id}: {spot.name} ({len(spot.polygon)} points)")

    if settings.quiet_windows:
        lines.append("Quiet windows:")
        for window in settings.quiet_windows[:MAX_LINES_PER_SECTION]:
            lines.append(f"- quiet window {window.name}: {window.start}-{window.end} {window.timezone}")
    else:
        lines.append("Quiet windows: none")

    return bounded_reply(lines)
