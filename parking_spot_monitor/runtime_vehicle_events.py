from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from parking_spot_monitor.detection import DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.matrix import (
    OCCUPIED_SPOT_EVENT_TYPE,
    OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
    owner_vehicle_quiet_window_event_id,
)
from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus
from parking_spot_monitor.owner_vehicles import load_owner_vehicle_registry
from parking_spot_monitor.runtime_health import safe_error_context as _safe_error_context
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

OWNER_VEHICLE_MIN_PROFILE_CONFIDENCE = 0.95


@dataclass(frozen=True)
class VehicleHistoryEventResult:
    errors: list[dict[str, Any]]
    occupied_alerts: list[dict[str, Any]]


def _owner_vehicle_quiet_window_alerts(
    history_archive: VehicleHistoryArchive | None,
    *,
    quiet_status: Any,
    observed_at: datetime,
    emitted_alert_ids: set[str],
    configured_spot_ids: Sequence[str],
    logger: StructuredLogger,
) -> list[dict[str, Any]]:
    if history_archive is None or not getattr(quiet_status, "active", False):
        return []
    window_id = getattr(quiet_status, "active_window_id", None)
    if not isinstance(window_id, str) or not window_id:
        return []
    configured = set(configured_spot_ids)
    alerts: list[dict[str, Any]] = []
    try:
        registry = load_owner_vehicle_registry(history_archive.root / "owner-vehicles.json")
        sessions = history_archive.load_active_sessions()
    except Exception as exc:
        logger.warning(
            "owner-vehicle-alert-scan-failed",
            phase="owner-vehicle",
            action="load-owner-registry",
            error_type=type(exc).__name__,
            error_message=redact_diagnostic_text(exc),
        )
        return []
    for session in sessions:
        if session.spot_id not in configured:
            continue
        owner = registry.owner_for_profile(session.profile_id)
        if owner is None:
            continue
        if not _owner_vehicle_profile_confidence_is_high_enough(session.profile_confidence):
            logger.info(
                "owner-vehicle-alert-skipped",
                reason="profile-confidence-too-low",
                spot_id=session.spot_id,
                session_id=session.session_id,
                profile_id=session.profile_id,
                profile_confidence=session.profile_confidence,
                min_profile_confidence=OWNER_VEHICLE_MIN_PROFILE_CONFIDENCE,
            )
            continue
        payload = {
            "event_type": OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
            "spot_id": session.spot_id,
            "observed_at": observed_at.isoformat(),
            "window_id": window_id,
            "profile_id": owner.profile_id,
            "session_id": session.session_id,
            "owner_vehicle": owner.to_alert_payload(),
        }
        event_id = owner_vehicle_quiet_window_event_id(payload)
        if event_id in emitted_alert_ids:
            continue
        payload["event_id"] = event_id
        alerts.append(payload)
    return alerts

def _owner_vehicle_profile_confidence_is_high_enough(confidence: float | None) -> bool:
    if confidence is None:
        return False
    try:
        value = float(confidence)
    except (TypeError, ValueError):
        return False
    return math.isfinite(value) and value >= OWNER_VEHICLE_MIN_PROFILE_CONFIDENCE

def _occupancy_history_event_id(event: OccupancyEvent) -> str:
    payload = event.to_dict()
    return str(payload.get("event_id") or f"{event.event_type.value}:{event.spot_id}:{event.observed_at}")

def _record_vehicle_history_events(
    history_archive: VehicleHistoryArchive | None,
    events: Sequence[OccupancyEvent],
    *,
    detection_result: DetectionFilterResult | None = None,
    snapshot_path: str | None = None,
    logger: StructuredLogger,
) -> VehicleHistoryEventResult:
    history_errors: list[dict[str, Any]] = []
    occupied_alerts: list[dict[str, Any]] = []
    if history_archive is None:
        return VehicleHistoryEventResult(errors=history_errors, occupied_alerts=occupied_alerts)
    for event in events:
        if event.event_type is not OccupancyEventType.STATE_CHANGED:
            logger.info(
                "vehicle-session-lifecycle-ignored",
                event_type=event.event_type.value,
                spot_id=event.spot_id,
                reason="not-state-changed",
            )
            continue
        previous_status = event.previous_status
        new_status = event.new_status
        if previous_status is not OccupancyStatus.OCCUPIED and new_status is OccupancyStatus.OCCUPIED:
            logger.info(
                "vehicle-session-lifecycle-attempt",
                action="start",
                spot_id=event.spot_id,
                event_id=_occupancy_history_event_id(event),
            )
            try:
                record = history_archive.start_session(event)
            except Exception as exc:  # preserve Matrix/open-alert delivery when archive recording fails
                context = _safe_error_context(
                    "vehicle-history",
                    exc,
                    extra={
                        "action": "start",
                        "event_type": event.event_type.value,
                        "spot_id": event.spot_id,
                        "event_id": _occupancy_history_event_id(event),
                    },
                )
                history_errors.append(context)
                logger.error("vehicle-history-record-failed", **context)
            else:
                logger.info(
                    "vehicle-session-lifecycle-recorded",
                    action="start",
                    spot_id=event.spot_id,
                    session_id=record.session_id,
                )
                accepted = None
                if detection_result is not None:
                    spot_detection = detection_result.by_spot.get(event.spot_id)
                    if spot_detection is not None:
                        accepted = spot_detection.accepted
                source_frame_path = snapshot_path
                if accepted is None or source_frame_path is None:
                    context = _safe_error_context(
                        "vehicle-history",
                        RuntimeError("accepted occupied candidate or source frame missing"),
                        extra={
                            "action": "attach-images",
                            "image_phase": "image-capture",
                            "event_type": event.event_type.value,
                            "spot_id": event.spot_id,
                            "event_id": _occupancy_history_event_id(event),
                            "session_id": record.session_id,
                        },
                    )
                    history_errors.append(context)
                    logger.error("vehicle-history-record-failed", **context)
                else:
                    try:
                        image_record = history_archive.attach_occupied_images(
                            session_id=record.session_id,
                            source_frame_path=source_frame_path,
                            bbox=accepted.bbox,
                        )
                    except Exception as exc:  # keep the session lifecycle recorded when image capture fails
                        context = _safe_error_context(
                            "vehicle-history",
                            exc,
                            extra={
                                "action": "attach-images",
                                "image_phase": "image-capture",
                                "event_type": event.event_type.value,
                                "spot_id": event.spot_id,
                                "event_id": _occupancy_history_event_id(event),
                                "session_id": record.session_id,
                            },
                        )
                        history_errors.append(context)
                        logger.error("vehicle-history-record-failed", **context)
                    else:
                        logger.info(
                            "vehicle-session-images-attached",
                            action="attach-images",
                            spot_id=event.spot_id,
                            session_id=image_record.session_id,
                            occupied_snapshot_attached=image_record.occupied_snapshot_path is not None,
                            occupied_crop_attached=image_record.occupied_crop_path is not None,
                        )
                        profile_assignment = None
                        if image_record.occupied_crop_path is not None:
                            try:
                                profile_assignment = history_archive.match_or_create_profile(session_id=record.session_id)
                            except Exception as exc:  # keep the session lifecycle and image archive when profile matching fails
                                context = _safe_error_context(
                                    "vehicle-history",
                                    exc,
                                    extra={
                                        "action": "match-profile",
                                        "profile_phase": "profile-match",
                                        "event_type": event.event_type.value,
                                        "spot_id": event.spot_id,
                                        "event_id": _occupancy_history_event_id(event),
                                        "session_id": record.session_id,
                                    },
                                )
                                history_errors.append(context)
                                logger.error("vehicle-history-record-failed", **context)
                            else:
                                logger.info(
                                    "vehicle-session-profile-matched",
                                    action="match-profile",
                                    spot_id=event.spot_id,
                                    session_id=profile_assignment.session_id,
                                    match_status=profile_assignment.status,
                                    profile_id=profile_assignment.profile_id,
                                    profile_confidence=profile_assignment.profile_confidence,
                                )
                        occupied_alert = _occupied_alert_payload(
                            history_archive,
                            event,
                            session_id=record.session_id,
                            image_record=image_record,
                            profile_assignment=profile_assignment,
                            logger=logger,
                        )
                        if occupied_alert is not None:
                            occupied_alerts.append(occupied_alert)
            continue
        if previous_status is OccupancyStatus.OCCUPIED and new_status is OccupancyStatus.EMPTY:
            logger.info(
                "vehicle-session-lifecycle-attempt",
                action="close",
                spot_id=event.spot_id,
                event_id=_occupancy_history_event_id(event),
            )
            try:
                record = history_archive.close_session(event)
            except Exception as exc:  # preserve Matrix/open-alert delivery when archive recording fails
                context = _safe_error_context(
                    "vehicle-history",
                    exc,
                    extra={
                        "action": "close",
                        "event_type": event.event_type.value,
                        "spot_id": event.spot_id,
                        "event_id": _occupancy_history_event_id(event),
                    },
                )
                history_errors.append(context)
                logger.error("vehicle-history-record-failed", **context)
            else:
                logger.info(
                    "vehicle-session-lifecycle-recorded",
                    action="close",
                    spot_id=event.spot_id,
                    session_id=None if record is None else record.session_id,
                    result="noop" if record is None else "closed",
                )
            continue
        logger.info(
            "vehicle-session-lifecycle-ignored",
            event_type=event.event_type.value,
            spot_id=event.spot_id,
            previous_status=None if previous_status is None else previous_status.value,
            new_status=None if new_status is None else new_status.value,
            reason="not-lifecycle-transition",
        )
    return VehicleHistoryEventResult(errors=history_errors, occupied_alerts=occupied_alerts)

def _occupied_alert_payload(
    history_archive: VehicleHistoryArchive,
    event: OccupancyEvent,
    *,
    session_id: str,
    image_record: Any,
    profile_assignment: Any | None,
    logger: StructuredLogger,
) -> dict[str, Any] | None:
    occupied_snapshot_path = getattr(image_record, "occupied_snapshot_path", None)
    if not isinstance(occupied_snapshot_path, str) or not occupied_snapshot_path.strip():
        logger.info(
            "vehicle-history-occupied-alert-skipped",
            event_type=OCCUPIED_SPOT_EVENT_TYPE,
            spot_id=event.spot_id,
            event_id=_occupancy_history_event_id(event),
            session_id=session_id,
            reason="missing-occupied-snapshot",
        )
        return None

    profile_id = getattr(profile_assignment, "profile_id", None)
    profile_confidence = getattr(profile_assignment, "profile_confidence", None)
    match_status = getattr(profile_assignment, "status", None)
    match_reason = getattr(profile_assignment, "reason", None)

    label = _profile_label_for_alert(history_archive, profile_id, logger=logger, spot_id=event.spot_id, session_id=session_id)
    estimate = _estimate_for_alert(history_archive, session_id, logger=logger, spot_id=event.spot_id)

    payload: dict[str, Any] = {
        "event_type": OCCUPIED_SPOT_EVENT_TYPE,
        "spot_id": event.spot_id,
        "observed_at": event.observed_at,
        "source_timestamp": event.source_timestamp,
        "event_id": _occupancy_history_event_id(event),
        "session_id": session_id,
        "profile_id": profile_id,
        "profile_label": label,
        "profile_confidence": profile_confidence,
        "match_status": match_status,
        "match_reason": match_reason,
        "occupied_snapshot_path": occupied_snapshot_path,
        "likely_vehicle": {
            "label": label or profile_id or "unknown vehicle",
            "profile_id": profile_id,
            "profile_confidence": profile_confidence,
            "confidence": profile_confidence,
            "match_status": match_status,
            "match_reason": match_reason,
        },
        "vehicle_history_estimate": estimate,
    }
    return payload

def _profile_label_for_alert(
    history_archive: VehicleHistoryArchive,
    profile_id: object,
    *,
    logger: StructuredLogger,
    spot_id: str,
    session_id: str,
) -> str | None:
    if not isinstance(profile_id, str) or not profile_id.strip():
        return None
    try:
        label = history_archive.effective_label(profile_id)
    except Exception as exc:
        logger.warning(
            "vehicle-history-profile-label-failed",
            phase="vehicle-history",
            action="effective-label",
            spot_id=spot_id,
            session_id=session_id,
            error_type=type(exc).__name__,
            error_message=redact_diagnostic_text(exc),
        )
        return None
    return label if isinstance(label, str) and label.strip() else None

def _estimate_for_alert(
    history_archive: VehicleHistoryArchive,
    session_id: str,
    *,
    logger: StructuredLogger,
    spot_id: str,
) -> dict[str, Any]:
    try:
        estimate = history_archive.estimate_for_session(session_id)
    except Exception as exc:
        logger.warning(
            "vehicle-history-estimate-failed",
            phase="vehicle-history",
            action="estimate-for-session",
            spot_id=spot_id,
            session_id=session_id,
            error_type=type(exc).__name__,
            error_message=redact_diagnostic_text(exc),
        )
        return {
            "status": "insufficient_history",
            "reason": "estimate-error",
            "profile_id": None,
            "sample_count": 0,
            "confidence": "unknown",
            "dwell_range": None,
            "leave_time_window": None,
        }
    return _vehicle_history_estimate_payload(estimate)

def _vehicle_history_estimate_payload(estimate: Any) -> dict[str, Any]:
    return {
        "status": getattr(estimate, "status", "insufficient_history"),
        "reason": getattr(estimate, "reason", None),
        "profile_id": getattr(estimate, "profile_id", None),
        "sample_count": getattr(estimate, "sample_count", 0),
        "confidence": getattr(estimate, "confidence", "unknown"),
        "dwell_range": _dataclass_like_payload(getattr(estimate, "dwell_range", None)),
        "leave_time_window": _dataclass_like_payload(getattr(estimate, "leave_time_window", None)),
    }

def _dataclass_like_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    fields = getattr(value, "__dataclass_fields__", None)
    if isinstance(fields, dict):
        return {name: getattr(value, name) for name in fields}
    if isinstance(value, Mapping):
        return dict(value)
    return None
