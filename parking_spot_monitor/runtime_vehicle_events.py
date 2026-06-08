from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Generic, TypeVar

from parking_spot_monitor.detection import DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.matrix_alerts import (
    OCCUPIED_SPOT_EVENT_TYPE,
    OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
    owner_vehicle_quiet_window_event_id,
)
from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus
from parking_spot_monitor.owner_vehicles import load_owner_vehicle_registry
from parking_spot_monitor.runtime_health import safe_error_context as _safe_error_context
from parking_spot_monitor.scheduler import QuietWindowStatus
from parking_spot_monitor.vehicle_history_alert_payloads import (
    likely_vehicle_payload,
    vehicle_history_estimate_error_payload,
    vehicle_history_estimate_payload,
)
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive
from parking_spot_monitor.vehicle_history_models import ProfileAssignment, SessionRecord

OWNER_VEHICLE_MIN_PROFILE_CONFIDENCE = 0.95
_T = TypeVar("_T")


@dataclass(frozen=True)
class VehicleHistoryEventResult:
    errors: list[dict[str, Any]]
    occupied_alerts: list[dict[str, Any]]


@dataclass(frozen=True)
class _VehicleHistoryStepResult(Generic[_T]):
    value: _T | None
    errors: list[dict[str, Any]]


def _owner_vehicle_quiet_window_alerts(
    history_archive: VehicleHistoryArchive | None,
    *,
    quiet_status: QuietWindowStatus,
    observed_at: datetime,
    emitted_alert_ids: set[str],
    configured_spot_ids: Sequence[str],
    logger: StructuredLogger,
) -> list[dict[str, Any]]:
    if history_archive is None or not quiet_status.active:
        return []
    window_id = quiet_status.active_window_id
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
            result = _record_vehicle_history_start(
                history_archive,
                event,
                detection_result=detection_result,
                snapshot_path=snapshot_path,
                logger=logger,
            )
            history_errors.extend(result.errors)
            occupied_alerts.extend(result.occupied_alerts)
            continue
        if previous_status is OccupancyStatus.OCCUPIED and new_status is OccupancyStatus.EMPTY:
            history_errors.extend(_record_vehicle_history_close(history_archive, event, logger=logger))
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


def _record_vehicle_history_start(
    history_archive: VehicleHistoryArchive,
    event: OccupancyEvent,
    *,
    detection_result: DetectionFilterResult | None,
    snapshot_path: str | None,
    logger: StructuredLogger,
) -> VehicleHistoryEventResult:
    errors: list[dict[str, Any]] = []
    occupied_alerts: list[dict[str, Any]] = []
    event_id = _occupancy_history_event_id(event)
    logger.info("vehicle-session-lifecycle-attempt", action="start", spot_id=event.spot_id, event_id=event_id)
    try:
        record = history_archive.start_session(event)
    except Exception as exc:  # preserve Matrix/open-alert delivery when archive recording fails
        context = _vehicle_history_error_context(exc, action="start", event=event, event_id=event_id)
        errors.append(context)
        logger.error("vehicle-history-record-failed", **context)
        return VehicleHistoryEventResult(errors=errors, occupied_alerts=occupied_alerts)

    logger.info("vehicle-session-lifecycle-recorded", action="start", spot_id=event.spot_id, session_id=record.session_id)
    accepted = _accepted_detection_for_event(detection_result, event)
    if accepted is None or snapshot_path is None:
        context = _vehicle_history_error_context(
            RuntimeError("accepted occupied candidate or source frame missing"),
            action="attach-images",
            event=event,
            event_id=event_id,
            session_id=record.session_id,
            image_phase="image-capture",
        )
        errors.append(context)
        logger.error("vehicle-history-record-failed", **context)
        return VehicleHistoryEventResult(errors=errors, occupied_alerts=occupied_alerts)

    image_record = _attach_occupied_images(
        history_archive,
        event,
        session_id=record.session_id,
        source_frame_path=snapshot_path,
        bbox=accepted.bbox,
        event_id=event_id,
        logger=logger,
    )
    errors.extend(image_record.errors)
    if image_record.value is None:
        return VehicleHistoryEventResult(errors=errors, occupied_alerts=occupied_alerts)

    profile_assignment = _match_vehicle_profile_for_session(
        history_archive,
        event,
        session_id=record.session_id,
        image_record=image_record.value,
        event_id=event_id,
        logger=logger,
    )
    errors.extend(profile_assignment.errors)
    occupied_alert = _occupied_alert_payload(
        history_archive,
        event,
        session_id=record.session_id,
        image_record=image_record.value,
        profile_assignment=profile_assignment.value,
        logger=logger,
    )
    if occupied_alert is not None:
        occupied_alerts.append(occupied_alert)
    return VehicleHistoryEventResult(errors=errors, occupied_alerts=occupied_alerts)


def _record_vehicle_history_close(
    history_archive: VehicleHistoryArchive,
    event: OccupancyEvent,
    *,
    logger: StructuredLogger,
) -> list[dict[str, Any]]:
    event_id = _occupancy_history_event_id(event)
    logger.info("vehicle-session-lifecycle-attempt", action="close", spot_id=event.spot_id, event_id=event_id)
    try:
        record = history_archive.close_session(event)
    except Exception as exc:  # preserve Matrix/open-alert delivery when archive recording fails
        context = _vehicle_history_error_context(exc, action="close", event=event, event_id=event_id)
        logger.error("vehicle-history-record-failed", **context)
        return [context]
    logger.info(
        "vehicle-session-lifecycle-recorded",
        action="close",
        spot_id=event.spot_id,
        session_id=None if record is None else record.session_id,
        result="noop" if record is None else "closed",
    )
    return []


def _accepted_detection_for_event(detection_result: DetectionFilterResult | None, event: OccupancyEvent) -> Any | None:
    if detection_result is None:
        return None
    spot_detection = detection_result.by_spot.get(event.spot_id)
    return None if spot_detection is None else spot_detection.accepted


def _attach_occupied_images(
    history_archive: VehicleHistoryArchive,
    event: OccupancyEvent,
    *,
    session_id: str,
    source_frame_path: str,
    bbox: Any,
    event_id: str,
    logger: StructuredLogger,
) -> _VehicleHistoryStepResult[SessionRecord]:
    try:
        image_record = history_archive.attach_occupied_images(
            session_id=session_id,
            source_frame_path=source_frame_path,
            bbox=bbox,
        )
    except Exception as exc:  # keep the session lifecycle recorded when image capture fails
        context = _vehicle_history_error_context(
            exc,
            action="attach-images",
            event=event,
            event_id=event_id,
            session_id=session_id,
            image_phase="image-capture",
        )
        logger.error("vehicle-history-record-failed", **context)
        return _VehicleHistoryStepResult(value=None, errors=[context])
    logger.info(
        "vehicle-session-images-attached",
        action="attach-images",
        spot_id=event.spot_id,
        session_id=image_record.session_id,
        occupied_snapshot_attached=image_record.occupied_snapshot_path is not None,
        occupied_crop_attached=image_record.occupied_crop_path is not None,
    )
    return _VehicleHistoryStepResult(value=image_record, errors=[])


def _match_vehicle_profile_for_session(
    history_archive: VehicleHistoryArchive,
    event: OccupancyEvent,
    *,
    session_id: str,
    image_record: SessionRecord,
    event_id: str,
    logger: StructuredLogger,
) -> _VehicleHistoryStepResult[ProfileAssignment]:
    if image_record.occupied_crop_path is None:
        return _VehicleHistoryStepResult(value=None, errors=[])
    try:
        profile_assignment = history_archive.match_or_create_profile(session_id=session_id)
    except Exception as exc:  # keep the session lifecycle and image archive when profile matching fails
        context = _vehicle_history_error_context(
            exc,
            action="match-profile",
            event=event,
            event_id=event_id,
            session_id=session_id,
            profile_phase="profile-match",
        )
        logger.error("vehicle-history-record-failed", **context)
        return _VehicleHistoryStepResult(value=None, errors=[context])
    logger.info(
        "vehicle-session-profile-matched",
        action="match-profile",
        spot_id=event.spot_id,
        session_id=profile_assignment.session_id,
        match_status=profile_assignment.status,
        profile_id=profile_assignment.profile_id,
        profile_confidence=profile_assignment.profile_confidence,
    )
    return _VehicleHistoryStepResult(value=profile_assignment, errors=[])


def _vehicle_history_error_context(
    error: BaseException,
    *,
    action: str,
    event: OccupancyEvent,
    event_id: str,
    session_id: str | None = None,
    image_phase: str | None = None,
    profile_phase: str | None = None,
) -> dict[str, Any]:
    extra: dict[str, Any] = {
        "action": action,
        "event_type": event.event_type.value,
        "spot_id": event.spot_id,
        "event_id": event_id,
    }
    if session_id is not None:
        extra["session_id"] = session_id
    if image_phase is not None:
        extra["image_phase"] = image_phase
    if profile_phase is not None:
        extra["profile_phase"] = profile_phase
    return _safe_error_context("vehicle-history", error, extra=extra)


def _occupied_alert_payload(
    history_archive: VehicleHistoryArchive,
    event: OccupancyEvent,
    *,
    session_id: str,
    image_record: SessionRecord,
    profile_assignment: ProfileAssignment | None,
    logger: StructuredLogger,
) -> dict[str, Any] | None:
    occupied_snapshot_path = image_record.occupied_snapshot_path
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

    profile_id = None if profile_assignment is None else profile_assignment.profile_id
    profile_confidence = None if profile_assignment is None else profile_assignment.profile_confidence
    match_status = None if profile_assignment is None else profile_assignment.status
    match_reason = None if profile_assignment is None else profile_assignment.reason

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
        "likely_vehicle": likely_vehicle_payload(profile_assignment, label),
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
        return vehicle_history_estimate_error_payload()
    return vehicle_history_estimate_payload(estimate)
