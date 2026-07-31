from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy import OccupancyUpdate, update_occupancy
from parking_spot_monitor.runtime_detection_geometry import candidate_in_configured_frame
from parking_spot_monitor.runtime_log_aggregation import RuntimeLogAggregator
from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleSnapshotProvider
from parking_spot_monitor.runtime_presence import _log_missed_occupied_spot_diagnostics, presence_by_spot
from parking_spot_monitor.runtime_vehicle_events import _owner_vehicle_quiet_window_alerts
from parking_spot_monitor.scheduler import QuietWindowStatus, evaluate_quiet_windows, quiet_window_notice_events
from parking_spot_monitor.state import RuntimeState
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive


@dataclass(frozen=True)
class RuntimeFramePlan:
    quiet_status: QuietWindowStatus
    pending_notice_payloads: list[dict[str, Any]]
    presence_by_spot: Mapping[str, Any]
    occupancy_update: OccupancyUpdate
    owner_alerts: list[dict[str, Any]]
    runtime_state: RuntimeState


def build_runtime_frame_plan(
    *,
    settings: RuntimeSettings,
    runtime_state: RuntimeState,
    detection_result: DetectionFilterResult,
    observed_at: datetime,
    snapshot_path: str,
    configured_spot_ids: Sequence[str],
    history_archive: VehicleHistoryArchive | None,
    logger: StructuredLogger,
    owner_vehicle_snapshot_provider: OwnerVehicleSnapshotProvider,
    log_aggregator: RuntimeLogAggregator | None = None,
) -> RuntimeFramePlan:
    quiet_status = evaluate_quiet_windows(settings.quiet_windows, observed_at)
    pending_notice_payloads = _pending_quiet_window_notice_payloads(
        previous_active_window_ids=runtime_state.active_quiet_window_ids,
        current=quiet_status,
        emitted_notice_ids=runtime_state.quiet_window_notice_ids,
    )
    emitted_notice_ids = set(runtime_state.quiet_window_notice_ids)
    for payload in pending_notice_payloads:
        event_id = payload.get("event_id")
        if isinstance(event_id, str) and event_id:
            emitted_notice_ids.add(event_id)

    _log_missed_occupied_spot_diagnostics(
        logger,
        runtime_state=runtime_state,
        detection_result=detection_result,
        open_suppression_classes=settings.detection.open_suppression_classes,
        min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
        log_aggregator=log_aggregator,
    )
    spot_presence = presence_by_spot(
        detection_result,
        open_suppression_classes=settings.detection.open_suppression_classes,
        min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
    )
    occupancy_update = update_occupancy(
        runtime_state.state_by_spot,
        {
            spot_id: candidate_in_configured_frame(
                spot_result.accepted,
                scale=detection_result.coordinate_scale,
            )
            for spot_id, spot_result in detection_result.by_spot.items()
        },
        settings.occupancy,
        observed_at.isoformat(),
        quiet_status,
        snapshot_path,
        configured_spot_ids=configured_spot_ids,
        presence_by_spot=spot_presence,
    )
    owner_alert_ids = set(runtime_state.owner_quiet_window_alert_ids)
    owner_alerts = _owner_vehicle_quiet_window_alerts(
        history_archive,
        quiet_status=quiet_status,
        observed_at=observed_at,
        emitted_alert_ids=owner_alert_ids,
        configured_spot_ids=configured_spot_ids,
        logger=logger,
        owner_vehicle_snapshot_provider=owner_vehicle_snapshot_provider,
    )
    for owner_alert in owner_alerts:
        event_id = owner_alert.get("event_id")
        if isinstance(event_id, str) and event_id:
            owner_alert_ids.add(event_id)

    updated_state = RuntimeState(
        state_by_spot=occupancy_update.state_by_spot,
        active_quiet_window_ids=quiet_status.active_window_ids,
        quiet_window_notice_ids=frozenset(emitted_notice_ids),
        owner_quiet_window_alert_ids=frozenset(owner_alert_ids),
    )
    return RuntimeFramePlan(
        quiet_status=quiet_status,
        pending_notice_payloads=pending_notice_payloads,
        presence_by_spot=spot_presence,
        occupancy_update=occupancy_update,
        owner_alerts=owner_alerts,
        runtime_state=updated_state,
    )


def _pending_quiet_window_notice_payloads(
    *,
    previous_active_window_ids: frozenset[str],
    current: QuietWindowStatus,
    emitted_notice_ids: frozenset[str],
) -> list[dict[str, Any]]:
    emitted_ids = set(emitted_notice_ids)
    payloads: list[dict[str, Any]] = []
    for notice in quiet_window_notice_events(
        previous_active_window_ids=previous_active_window_ids,
        current=current,
        emitted_notice_ids=emitted_notice_ids,
    ):
        if notice.event_id in emitted_ids:
            continue
        payloads.append(notice.to_dict())
        emitted_ids.add(notice.event_id)
    return payloads
