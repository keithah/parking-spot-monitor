from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_alerts import (
    OCCUPIED_SPOT_EVENT_TYPE,
    OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
)
from parking_spot_monitor.matrix_dispatch import RuntimeMatrixDelivery, dispatch_matrix_event
from parking_spot_monitor.operator_decision_memory import DecisionMemoryRecord, append_decision_memory_records
from parking_spot_monitor.runtime_decision_memory import build_runtime_state_memory_records
from parking_spot_monitor.runtime_frame_plan import build_runtime_frame_plan
from parking_spot_monitor.runtime_health import safe_error_context as _safe_error_context
from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleRuntimeCache
from parking_spot_monitor.runtime_vehicle_events import _record_vehicle_history_events
from parking_spot_monitor.state import RuntimeState, save_runtime_state
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive


@dataclass(frozen=True)
class FrameUpdateResult:
    runtime_state: RuntimeState
    matrix_errors: list[dict[str, Any]]
    transition_occurred: bool
    state_save_error: dict[str, Any] | None = None
    history_errors: list[dict[str, Any]] | None = None


def _update_runtime_state_for_frame(
    *,
    settings: RuntimeSettings,
    runtime_state: RuntimeState,
    detection_result: DetectionFilterResult,
    observed_at: datetime,
    snapshot_path: str,
    logger: StructuredLogger,
    matrix_delivery: RuntimeMatrixDelivery | None,
    state_path: Path,
    configured_spot_ids: Sequence[str],
    history_archive: VehicleHistoryArchive | None = None,
    owner_vehicle_cache: OwnerVehicleRuntimeCache | None = None,
    decision_memory_path: Path | None = None,
    pending_decision_records: Sequence[DecisionMemoryRecord] = (),
) -> FrameUpdateResult:
    matrix_errors: list[dict[str, Any]] = []
    frame_plan = build_runtime_frame_plan(
        settings=settings,
        runtime_state=runtime_state,
        detection_result=detection_result,
        observed_at=observed_at,
        snapshot_path=snapshot_path,
        configured_spot_ids=configured_spot_ids,
        history_archive=history_archive,
        logger=logger,
        owner_vehicle_cache=owner_vehicle_cache,
    )
    state_save_error: dict[str, Any] | None = None

    for pending_payload in frame_plan.pending_notice_payloads:
        payload = dict(pending_payload)
        event_name = str(payload.pop("event_type"))
        logger.info(event_name, **payload)
        matrix_error = dispatch_matrix_event(
            matrix_delivery,
            event_name,
            payload | {"event_type": event_name},
            logger=logger,
            decision_memory_path=decision_memory_path,
        )
        if matrix_error is not None:
            matrix_errors.append(matrix_error)

    if decision_memory_path is not None:
        frame_records = [
            *pending_decision_records,
            *build_runtime_state_memory_records(
                previous_state=runtime_state,
                next_state=frame_plan.occupancy_update.state_by_spot,
                detection_result=detection_result,
                quiet_status=frame_plan.quiet_status,
                observed_at=observed_at,
                configured_spot_ids=configured_spot_ids,
                presence_by_spot=frame_plan.presence_by_spot,
            ),
        ]
        append_decision_memory_records(decision_memory_path, frame_records, logger=logger)
    history_result = _record_vehicle_history_events(
        history_archive,
        frame_plan.occupancy_update.events,
        detection_result=detection_result,
        snapshot_path=snapshot_path,
        logger=logger,
    )
    history_errors = history_result.errors

    for owner_alert in frame_plan.owner_alerts:
        event_name = str(owner_alert.get("event_type", OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE))
        logger.info(event_name, **{key: value for key, value in owner_alert.items() if key != "event_type"})
        matrix_error = dispatch_matrix_event(
            matrix_delivery,
            event_name,
            owner_alert,
            logger=logger,
            decision_memory_path=decision_memory_path,
        )
        if matrix_error is not None:
            matrix_errors.append(matrix_error)

    for occupied_alert in history_result.occupied_alerts:
        matrix_error = dispatch_matrix_event(
            matrix_delivery,
            str(occupied_alert.get("event_type", OCCUPIED_SPOT_EVENT_TYPE)),
            occupied_alert,
            logger=logger,
            decision_memory_path=decision_memory_path,
        )
        if matrix_error is not None:
            matrix_errors.append(matrix_error)

    for event in frame_plan.occupancy_update.events:
        payload = event.to_dict()
        event_name = str(payload.pop("event_type"))
        logger.info(event_name, **payload)
        matrix_error = dispatch_matrix_event(
            matrix_delivery,
            event_name,
            payload | {"event_type": event_name},
            logger=logger,
            decision_memory_path=decision_memory_path,
        )
        if matrix_error is not None:
            matrix_errors.append(matrix_error)

    try:
        save_runtime_state(state_path, frame_plan.runtime_state, logger=logger)
    except Exception as exc:
        state_save_error = _safe_error_context("state-save", exc)

    return FrameUpdateResult(
        runtime_state=frame_plan.runtime_state,
        matrix_errors=matrix_errors,
        transition_occurred=bool(frame_plan.occupancy_update.events),
        state_save_error=state_save_error,
        history_errors=history_errors,
    )
