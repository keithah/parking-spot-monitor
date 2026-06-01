from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import load_decision_memory
from parking_spot_monitor.operator_feedback_models import AlertEvidenceCandidate, SpotState, optional_feedback_text


def resolve_latest_alert_candidate(path: str | Path, spot_id: str, *, logger: StructuredLogger | None = None) -> AlertEvidenceCandidate | None:
    """Return the newest alert memory record for a spot with a recognized reported state."""

    loaded = load_decision_memory(path, logger=logger)
    if loaded.state != "available":
        return None
    for record in reversed(loaded.records):
        if record.kind != "alert" or record.spot_id != spot_id:
            continue
        details = record.details if isinstance(record.details, Mapping) else {}
        if details.get("outcome") != "sent":
            continue
        event_type = details.get("event_type")
        reported_state = reported_state_from_event_type(event_type)
        if reported_state is None:
            continue
        return AlertEvidenceCandidate(
            spot_id=spot_id,
            reported_state=reported_state,
            reported_at=record.observed_at,
            alert_event_type=optional_feedback_text(event_type, limit=120),
            alert_event_id=optional_feedback_text(details.get("event_id"), limit=180),
            snapshot_path=optional_feedback_text(details.get("retained_snapshot_path") or details.get("snapshot_path"), limit=240),
        )
    return None


def reported_state_from_event_type(value: object) -> SpotState | None:
    text = str(value or "")
    if text == "occupancy-occupied-event":
        return "occupied"
    if text == "occupancy-open-event":
        return "open"
    return None
