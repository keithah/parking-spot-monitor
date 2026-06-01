from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.operator_cockpit_outbox import matrix_outbox_status_lines
from parking_spot_monitor.operator_cockpit_shared import (
    MAX_LINES_PER_SECTION,
    BoundedJsonLoad,
    SpotSummary,
    age_label,
    bounded_reply,
    load_bounded_json_object,
    log_load_problem,
    mapping_value,
    spot_ids,
    summarize_state,
    text_value,
    utc_now,
)
from parking_spot_monitor.operator_timeline import timeline_frame_time


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

    observed_now = utc_now(now)
    root = Path(data_dir)
    health_load = load_bounded_json_object(Path(health_path), label="health", logger=logger)
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
    for spot_id in spot_ids(settings)[:MAX_LINES_PER_SECTION]:
        spot = state_by_spot.get(spot_id, SpotSummary(spot_id=spot_id, status="unknown", hit_streak=0, miss_streak=0, open_event_emitted=False))
        stability = _classify_spot_stability(spot, confirm_frames=confirm_frames, release_frames=release_frames, state_available=state.state == "available")
        lines.append(
            f"- {spot_id}: {stability}; state {spot.status}; hit streak {spot.hit_streak}/{confirm_frames}; miss streak {spot.miss_streak}/{release_frames}"
        )
    if state.state != "available":
        suffix = f" ({state.error_type})" if state.error_type else ""
        lines.append(f"State artifacts: unavailable{suffix}; configured spot fallbacks shown.")

    lines.append("Weak evidence:")
    lines.extend(_confidence_memory_lines(memory, spot_ids(settings)))
    lines.append("Timeline health:")
    lines.extend(timeline)
    lines.append("Matrix delivery:")
    lines.extend(_matrix_delivery_lines(health_load, memory))
    lines.extend(matrix_outbox_status_lines(matrix_outbox_path, logger=logger))
    lines.append("Read-only: no detector, camera, media upload, alert emission, or state mutation was run.")
    return bounded_reply(lines)


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
        suffix = f" ({text_value(getattr(memory, 'error_type', None))})" if getattr(memory, "error_type", None) else ""
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
            summary = text_value(getattr(record, "summary", ""), default="no summary")
            snippets.append(f"{text_value(getattr(record, 'kind', 'unknown'))}: {summary}")
        lines.append(f"- {spot_id}: " + "; ".join(snippets))
    return lines or ["- no configured spots available for weak-evidence summary."]


def _summarize_timeline_frames(frames_dir: Path, *, now: datetime, logger: StructuredLogger | None) -> list[str]:
    try:
        paths = [path for path in frames_dir.iterdir() if path.is_file()]
    except FileNotFoundError:
        log_load_problem(logger, label="timeline", reason="missing", error_type="missing")
        return ["- unavailable (missing timeline frames directory)."]
    except OSError as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        log_load_problem(logger, label="timeline", reason="scan_error", error_type=error_type)
        return [f"- unavailable ({error_type})."]

    parsed = [timeline_frame_time(path) for path in paths[: MAX_LINES_PER_SECTION * 20]]
    valid_times = sorted(time for time in parsed if time is not None)
    ignored = len([time for time in parsed if time is None])
    if not valid_times:
        suffix = f"; ignored {ignored} unparseable filename(s)" if ignored else ""
        return [f"- no timestamped retained frames found{suffix}."]
    oldest = valid_times[0]
    newest = valid_times[-1]
    newest_age = age_label(newest, now)
    return [
        f"- retained timestamped frames {len(valid_times)}; newest {newest_age}; oldest {age_label(oldest, now)}.",
        f"- filename scan only; image bytes were not opened; ignored {ignored} unparseable filename(s).",
    ]


def _matrix_delivery_lines(health_load: BoundedJsonLoad, memory: Any) -> list[str]:
    lines: list[str] = []
    if health_load.state != "available" or health_load.payload is None:
        suffix = f" ({health_load.error_type})" if health_load.error_type else ""
        lines.append(f"- health unavailable{suffix}; Matrix error status unknown.")
    else:
        matrix_error = mapping_value(health_load.payload.get("last_matrix_error"))
        if matrix_error:
            error_type = text_value(matrix_error.get("error_type") or matrix_error.get("type") or matrix_error.get("status"), default="error")
            lines.append(f"- last Matrix error: {error_type}.")
        else:
            lines.append("- no last Matrix error recorded in health.")

    if getattr(memory, "state", None) == "available":
        records = [record for record in getattr(memory, "records", ()) if getattr(record, "kind", "") in {"command_outcome", "alert"}]
        if records:
            record = records[-1]
            lines.append(f"- recent delivery memory: {text_value(getattr(record, 'kind', 'unknown'))}: {text_value(getattr(record, 'summary', ''), default='no summary')}.")
        else:
            lines.append("- no recent command or alert outcome memory.")
    else:
        lines.append("- delivery memory unavailable.")
    return lines
