from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor import runtime_loop_resources, runtime_matrix_commands
from parking_spot_monitor.capture import CaptureError, StreamProfileCapture
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detector_adapter import DetectorRunner
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_alerts import MONITOR_STARTED_EVENT_TYPE, monitor_lifecycle_event
from parking_spot_monitor.matrix_dispatch import RuntimeMatrixDelivery, dispatch_matrix_event
from parking_spot_monitor.paths import resolve_runtime_paths
from parking_spot_monitor.runtime_frame import capture_and_detect_runtime_frame
from parking_spot_monitor.runtime_frame_outcome import prepare_runtime_frame_loop_result
from parking_spot_monitor.runtime_health import RuntimeLoopHealthState, observed_at
from parking_spot_monitor.runtime_health_cache import VehicleHistoryHealthSnapshotCache
from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleRuntimeCache
from parking_spot_monitor.runtime_commands import RuntimeMatrixCommandService, _poll_matrix_commands_once
from parking_spot_monitor.runtime_decision_memory import build_detection_memory_records
from parking_spot_monitor.runtime_detection import _configured_spot_polygons
from parking_spot_monitor.runtime_resource_policy import RuntimeResourcePolicyState
from parking_spot_monitor.runtime_state_update import _update_runtime_state_for_frame
from parking_spot_monitor.runtime_lifecycle import ShutdownState, monitor_signal_handlers, return_if_shutdown_requested
from parking_spot_monitor.state import load_runtime_state
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

VEHICLE_HISTORY_HEALTH_CACHE_SECONDS = 300
def run_capture_loop(
    settings: RuntimeSettings,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    capture: StreamProfileCapture,
    overlay: Callable[..., Any],
    detector_factory: Callable[[RuntimeSettings], Any],
    matrix_delivery: RuntimeMatrixDelivery | None,
    history_archive: VehicleHistoryArchive | None = None,
    matrix_command_service: RuntimeMatrixCommandService | None = None,
    sleep: Callable[[float], None],
    wait: Callable[[float], bool] | None = None,
    shutdown_state: ShutdownState | None = None,
    max_iterations: int | None = None,
    now: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    startup_retention_failure_count: int = 0,
) -> int:
    iteration = 0
    detector: DetectorRunner | None = None
    spot_ids = list(_configured_spot_polygons(settings).keys())
    state_path = data_dir / "state.json"
    runtime_paths = resolve_runtime_paths(settings, data_dir)
    runtime_state = load_runtime_state(state_path, spot_ids, logger=logger)
    effective_history_archive = (
        history_archive
        if history_archive is not None
        else VehicleHistoryArchive(data_dir / "vehicle-history", logger=logger)
    )
    now_fn = now if now is not None else lambda: datetime.now(timezone.utc)
    health_state = RuntimeLoopHealthState(retention_failure_count=startup_retention_failure_count)
    resource_policy_state = RuntimeResourcePolicyState()
    matrix_command_poll_state = runtime_matrix_commands.MatrixCommandPollState()
    decision_memory_path = data_dir / "operator-decision-memory.json"
    shutdown_state = shutdown_state if shutdown_state is not None else ShutdownState()
    if wait is not None:
        wait_for_shutdown = wait
    elif sleep is time.sleep:
        wait_for_shutdown = shutdown_state.wait
    else:
        def wait_for_shutdown(timeout_seconds: float) -> bool:
            sleep(timeout_seconds)
            return shutdown_state.requested
    vehicle_history_health = VehicleHistoryHealthSnapshotCache(
        effective_history_archive,
        now=now_fn,
        ttl_seconds=VEHICLE_HISTORY_HEALTH_CACHE_SECONDS,
    )
    archive_root = Path(getattr(effective_history_archive, "root", runtime_paths.vehicle_history_dir))
    owner_vehicle_cache = OwnerVehicleRuntimeCache(archive_root / "owner-vehicles.json", logger=logger)
    outbox_health_provider = getattr(matrix_delivery, "outbox_health_summary", None)
    if not callable(outbox_health_provider):
        outbox_health_provider = None

    def write_current_health(*, status: str, iteration: int) -> None:
        runtime_loop_resources.write_current_loop_health(
            settings,
            logger=logger,
            health_state=health_state,
            status=status,
            iteration=iteration,
            vehicle_history_health=vehicle_history_health,
            runtime_paths=runtime_paths,
            outbox_health_provider=outbox_health_provider,
        )

    with monitor_signal_handlers(shutdown_state, logger=logger):
        startup_lifecycle_error = dispatch_matrix_event(
            matrix_delivery,
            MONITOR_STARTED_EVENT_TYPE,
            monitor_lifecycle_event(MONITOR_STARTED_EVENT_TYPE, now_fn()),
            logger=logger,
            decision_memory_path=decision_memory_path,
        )
        if startup_lifecycle_error is not None:
            logger.warning(
                "lifecycle-notice-delivery-degraded",
                event_type=MONITOR_STARTED_EVENT_TYPE,
                error_type=startup_lifecycle_error.get("error_type"),
            )
        startup_status = "degraded" if health_state.retention_failure_count else "starting"
        write_current_health(status=startup_status, iteration=iteration)
        while max_iterations is None or iteration < max_iterations:
            shutdown_exit = return_if_shutdown_requested(
                shutdown_state=shutdown_state,
                matrix_delivery=matrix_delivery,
                now_fn=now_fn,
                logger=logger,
                decision_memory_path=decision_memory_path,
                iteration=iteration,
            )
            if shutdown_exit is not None:
                return shutdown_exit
            iteration += 1
            iteration_started_at = monotonic()
            logger.debug("capture-loop-iteration", iteration=iteration, data_dir=str(data_dir))
            try:
                frame_attempt = capture_and_detect_runtime_frame(
                    settings,
                    data_dir,
                    capture=capture,
                    detector=detector,
                    detector_factory=detector_factory,
                    runtime_state=runtime_state,
                    logger=logger,
                    mode="runtime-loop",
                    iteration=iteration,
                    periodic_verification_due=runtime_loop_resources.periodic_verification_due(
                        settings,
                        resource_policy_state,
                        now_monotonic=iteration_started_at,
                    ),
                )
                frame_result = prepare_runtime_frame_loop_result(
                    frame_attempt,
                    health_state=health_state,
                    logger=logger,
                    iteration=iteration,
                )
                detector = frame_result.detector
                result = frame_result.capture
                detection_result = frame_result.detection
                transition_occurred = False
                frame_has_weak_presence = False
                overlay_written = False
                if detection_result is not None:
                    health_state.record_processed_frame(timestamp=result.timestamp, selected_mode=result.selected_mode)
                    pending_decision_records = build_detection_memory_records(
                        detection_result,
                        observed_at=result.timestamp,
                        mode="runtime-loop",
                        iteration=iteration,
                    )
                    health_state.record_detection_success()
                    frame_observed_at = observed_at(result.timestamp, now_fn)
                    frame_update = _update_runtime_state_for_frame(
                        settings=settings,
                        runtime_state=runtime_state,
                        detection_result=detection_result,
                        observed_at=frame_observed_at,
                        snapshot_path=str(result.latest_path),
                        logger=logger,
                        matrix_delivery=matrix_delivery,
                        state_path=state_path,
                        configured_spot_ids=spot_ids,
                        history_archive=effective_history_archive,
                        owner_vehicle_snapshot_provider=owner_vehicle_cache,
                        decision_memory_path=decision_memory_path,
                        pending_decision_records=pending_decision_records,
                    )
                    runtime_state = frame_update.runtime_state
                    transition_occurred = frame_update.transition_occurred
                    frame_has_weak_presence = runtime_loop_resources.frame_has_weak_presence(
                        settings, detection_result
                    )
                    health_state.record_frame_update(
                        matrix_errors=frame_update.matrix_errors,
                        history_errors=frame_update.history_errors,
                        state_save_error=frame_update.state_save_error,
                    )
                    if matrix_command_service is not None:
                        command_poll_due_at = monotonic()
                        if runtime_matrix_commands.command_poll_due(
                            settings.matrix,
                            matrix_command_poll_state,
                            command_poll_due_at,
                        ):
                            command_outcome = _poll_matrix_commands_once(
                                matrix_command_service,
                                logger=logger,
                                iteration=iteration,
                                decision_memory_path=decision_memory_path,
                            )
                            command_poll_completed_at = monotonic()
                            matrix_command_poll_state = runtime_matrix_commands.record_command_poll_result(
                                settings.matrix,
                                matrix_command_poll_state,
                                command_poll_completed_at,
                                failed=command_outcome.transport_failed,
                            )
                            health_state.record_command_result(command_outcome.health_error)
                    overlay_written = runtime_loop_resources.record_primary_frame_artifacts(
                        settings,
                        frame_result.primary_capture,
                        data_dir,
                        logger=logger,
                        overlay=overlay,
                        iteration=iteration,
                        policy_state=resource_policy_state,
                        now_monotonic=iteration_started_at,
                        transition_occurred=transition_occurred,
                    )
                logger.info("capture-loop-frame-written", iteration=iteration, **result.diagnostics())
                write_current_health(status=health_state.status(), iteration=iteration)
                iteration_finished_at = monotonic()
                policy_update = runtime_loop_resources.advance_resource_policy(
                    settings,
                    runtime_state,
                    resource_policy_state,
                    transition_occurred=transition_occurred,
                    weak_presence=frame_has_weak_presence,
                    degraded=health_state.status() != "ok",
                    verification_succeeded=detection_result is not None and frame_result.escalated,
                    overlay_written=overlay_written,
                    completed_at=iteration_finished_at,
                )
                resource_policy_state = policy_update.state
                sleep_seconds = runtime_loop_resources.paced_sleep_seconds(
                    settings,
                    policy_update.decision,
                    iteration_started_at=iteration_started_at,
                    now_monotonic=iteration_finished_at,
                )
                logger.debug(
                    "capture-loop-paced",
                    iteration=iteration,
                    sleep_seconds=sleep_seconds,
                    cadence_reason=policy_update.decision.reason,
                )
                wait_for_shutdown(sleep_seconds)
                shutdown_exit = return_if_shutdown_requested(
                    shutdown_state=shutdown_state,
                    matrix_delivery=matrix_delivery,
                    now_fn=now_fn,
                    logger=logger,
                    decision_memory_path=decision_memory_path,
                    iteration=iteration,
                )
                if shutdown_exit is not None:
                    return shutdown_exit
            except CaptureError as exc:
                health_state.record_capture_failure(exc, iteration=iteration)
                resource_policy_state = runtime_loop_resources.reset_stable_successes(resource_policy_state)
                backoff_seconds = settings.stream.reconnect_seconds
                logger.error("capture-loop-failure", iteration=iteration, backoff_seconds=backoff_seconds, **exc.diagnostics())
                write_current_health(status="down", iteration=iteration)
                wait_for_shutdown(backoff_seconds)
                shutdown_exit = return_if_shutdown_requested(
                    shutdown_state=shutdown_state,
                    matrix_delivery=matrix_delivery,
                    now_fn=now_fn,
                    logger=logger,
                    decision_memory_path=decision_memory_path,
                    iteration=iteration,
                )
                if shutdown_exit is not None:
                    return shutdown_exit
        return 0
