from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.matrix_alerts import _first_present, _int_field, _occupied_snapshot_body, _safe_text
from parking_spot_monitor.matrix_models import MatrixCommandParseError, MatrixCommandResponse
from parking_spot_monitor.operator_cockpit import (
    build_incident_review_response,
    build_latest_snapshot_response,
    format_detection_lab_run_reply,
    format_detection_lab_status_reply,
    format_operator_analytics_reply,
    format_operator_confidence_reply,
    format_operator_config_reply,
    format_operator_recent_reply,
    format_operator_status_reply,
    format_operator_why_reply,
)

@dataclass(frozen=True)
class MatrixOperatorCockpitContext:
    """Immutable local runtime inputs for read-only Matrix cockpit commands."""

    settings: Any
    data_dir: Path
    health_path: Path
    state_path: Path
    matrix_outbox_path: Path | None = None
    latest_path: Path | None = None
    snapshots_dir: Path | None = None
    detection_lab_manager: Any | None = None
    incident_detector: Any | None = None

    def status_reply(self, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        return MatrixCommandResponse(
            text=format_operator_status_reply(
                settings=self.settings,
                health_path=self.health_path,
                state_path=self.state_path,
                matrix_outbox_path=self.matrix_outbox_path,
                logger=logger,
            )
        )

    def config_reply(self, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        return MatrixCommandResponse(text=format_operator_config_reply(settings=self.settings, data_dir=self.data_dir, logger=logger))

    def latest_reply(self, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        if self.latest_path is None:
            return MatrixCommandResponse(text="Parking monitor latest unavailable: latest.jpg path is not configured")
        latest = build_latest_snapshot_response(
            settings=self.settings,
            latest_path=self.latest_path,
            health_path=self.health_path,
            state_path=self.state_path,
            logger=logger,
        )
        return MatrixCommandResponse(text=latest.text, image_path=latest.image_path, image_info=latest.image_info)

    def why_reply(self, spot_id: str, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        return MatrixCommandResponse(text=format_operator_why_reply(data_dir=self.data_dir, spot_id=spot_id, logger=logger))

    def recent_reply(self, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        return MatrixCommandResponse(text=format_operator_recent_reply(data_dir=self.data_dir, logger=logger))

    def confidence_reply(self, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        return MatrixCommandResponse(
            text=format_operator_confidence_reply(
                settings=self.settings,
                data_dir=self.data_dir,
                health_path=self.health_path,
                state_path=self.state_path,
                matrix_outbox_path=self.matrix_outbox_path,
                logger=logger,
            )
        )

    def analytics_reply(self, window: str, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        return MatrixCommandResponse(text=format_operator_analytics_reply(data_dir=self.data_dir, window=window, logger=logger))

    def incident_review_reply(
        self,
        *,
        spot_id: str,
        incident_time: str,
        logger: StructuredLogger | None = None,
    ) -> MatrixCommandResponse:
        return build_incident_review_response(
            settings=self.settings,
            data_dir=self.data_dir,
            state_path=self.state_path,
            spot_id=spot_id,
            time_text=incident_time,
            detector=self.incident_detector,
            logger=logger,
        )

    def lab_run_reply(self, kind: str, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        return MatrixCommandResponse(
            text=format_detection_lab_run_reply(
                data_dir=self.data_dir,
                kind=kind,
                manager=self.detection_lab_manager,
                logger=logger,
            )
        )

    def lab_status_reply(self, job_id: str, *, logger: StructuredLogger | None = None) -> MatrixCommandResponse:
        return MatrixCommandResponse(
            text=format_detection_lab_status_reply(
                data_dir=self.data_dir,
                job_id=job_id,
                manager=self.detection_lab_manager,
                logger=logger,
            )
        )

def _active_spot_assignments_with_runtime_status(
    assignments: Sequence[Mapping[str, Any]],
    *,
    cockpit_context: MatrixOperatorCockpitContext | None,
    logger: StructuredLogger | None = None,
) -> list[dict[str, Any]]:
    enriched = [dict(assignment, status="occupied") for assignment in assignments]
    if cockpit_context is None:
        return enriched

    configured_spot_ids = _configured_spot_ids(getattr(cockpit_context, "settings", None))
    if not configured_spot_ids:
        return enriched
    try:
        from parking_spot_monitor.occupancy import OccupancyStatus
        from parking_spot_monitor.state import load_runtime_state

        runtime_state = load_runtime_state(cockpit_context.state_path, configured_spot_ids, logger=logger)
    except Exception:
        return enriched

    by_spot = {str(item.get("spot_id")): item for item in enriched}
    for spot_id in configured_spot_ids:
        state = runtime_state.state_by_spot.get(spot_id)
        if state is None:
            continue
        if spot_id in by_spot:
            by_spot[spot_id]["last_status_changed_at"] = state.last_status_changed_at
            continue
        status = "open" if state.status in {OccupancyStatus.EMPTY, OccupancyStatus.UNKNOWN} else "occupied"
        by_spot[spot_id] = {
            "spot_id": spot_id,
            "status": status,
            "last_status_changed_at": state.last_status_changed_at,
        }
    return [by_spot[spot_id] for spot_id in sorted(by_spot)]

def _configured_spot_ids(settings: object) -> list[str]:
    spots = getattr(settings, "spots", None)
    if spots is None:
        return []
    return [spot_id for spot_id in ("left_spot", "right_spot") if getattr(spots, spot_id, None) is not None]

def _format_active_spot_assignments_reply(assignments: Sequence[Mapping[str, Any]], *, now: object | None = None) -> str:
    if not assignments:
        return "No active parking sessions."
    observed_now = _parse_display_time(now) or datetime.now(timezone.utc)
    lines = ["Active parking sessions:"]
    for assignment in assignments:
        spot_id = _safe_text(assignment.get("spot_id"), default="unknown_spot")
        status = _safe_text(assignment.get("status"), default="occupied")
        duration = _duration_suffix(_first_present(assignment, "started_at", "last_status_changed_at"), observed_now)
        if status in {"open", "empty"}:
            lines.append(f"{spot_id}: open{duration}")
            continue
        session_id = _safe_text(assignment.get("session_id"), default="unknown_session")
        profile_label = _safe_text(assignment.get("profile_label"), default="unknown vehicle")
        confidence_text = _confidence_text(assignment.get("profile_confidence"))
        sample_count = assignment.get("profile_sample_count")
        sample_text = "unknown" if not isinstance(sample_count, int) else str(sample_count)
        if assignment.get("profile_id") is None:
            lines.append(f"{spot_id}: occupied{duration} — unknown vehicle — session {session_id}")
        else:
            lines.append(f"{spot_id}: occupied{duration} — {profile_label} — confidence {confidence_text} — samples {sample_text} — session {session_id}")
    return "\n".join(lines)

def _confidence_text(value: object) -> str:
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return f"{float(value):.2f}"
    return "unknown"

def _duration_suffix(since: object, now: datetime) -> str:
    started = _parse_display_time(since)
    if started is None:
        return ""
    seconds = int(max(0, (now - started).total_seconds()))
    return f" for {_human_duration(seconds)}"

def _parse_display_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

def _human_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds} sec"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min"
    hours, remaining_minutes = divmod(minutes, 60)
    if hours < 24:
        if remaining_minutes:
            return f"{hours} hr {remaining_minutes} min"
        return f"{hours} hr"
    days, remaining_hours = divmod(hours, 24)
    if remaining_hours:
        return f"{days} day{'s' if days != 1 else ''} {remaining_hours} hr"
    return f"{days} day{'s' if days != 1 else ''}"
