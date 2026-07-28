from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor.capture import CaptureError, StreamProfileCapture
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_alerts import MONITOR_STARTED_EVENT_TYPE, monitor_lifecycle_event
from parking_spot_monitor.matrix_dispatch import dispatch_matrix_event
from parking_spot_monitor.paths import resolve_runtime_paths
from parking_spot_monitor.runtime_frame import capture_and_detect_runtime_frame
from parking_spot_monitor.runtime_frame_outcome import prepare_runtime_frame_loop_result
from parking_spot_monitor.runtime_health import RuntimeLoopHealthState, observed_at, safe_error_context, write_loop_health
from parking_spot_monitor.runtime_health_cache import VehicleHistoryHealthSnapshotCache
from parking_spot_monitor.runtime_commands import _poll_matrix_commands_once
from parking_spot_monitor.runtime_detection import _configured_spot_polygons, record_detection_memory_records
from parking_spot_monitor.runtime_overlay import _write_overlay_for_capture
from parking_spot_monitor.runtime_state_update import _update_runtime_state_for_frame
from parking_spot_monitor.runtime_lifecycle import ShutdownState, monitor_signal_handlers, return_if_shutdown_requested
from parking_spot_monitor.state import load_runtime_state
from parking_spot_monitor.timeline_buffer import record_timeline_frame
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

MATRIX_OUTBOX_MAX_RECORDS_PER_ITERATION = 1
VEHICLE_HISTORY_HEALTH_CACHE_SECONDS = 300


def drain_matrix_outbox_if_available(
    matrix_delivery: Any | None,
    *,
    logger: StructuredLogger,
    iteration: int,
    trigger: str,
    max_records: int | None = None,
) -> dict[str, Any] | None:
    drain = getattr(matrix_delivery, "drain_outbox", None)
    if drain is None or not callable(drain):
        return None
    logger.info("matrix-outbox-runtime-drain-attempt", trigger=trigger, iteration=iteration)
    try:
        try:
            result = drain(max_records=max_records) if max_records is not None else drain()
        except TypeError:
            if max_records is None:
                raise
            result = drain()
    except Exception as exc:
        context = safe_error_context(
            "matrix-outbox",
            exc,
            extra={"trigger": trigger, "iteration": iteration},
        )
        logger.warning("matrix-outbox-runtime-drain-failed", **context)
        return context
    logger.info(
        "matrix-outbox-runtime-drain-succeeded",
        trigger=trigger,
        iteration=iteration,
        attempted_count=getattr(result, "attempted_count", None),
        delivered_count=getattr(result, "delivered_count", None),
        retrying_count=getattr(result, "retrying_count", None),
    )
    retrying_count = getattr(result, "retrying_count", None)
    if isinstance(retrying_count, int) and retrying_count > 0:
        return {
            "phase": "matrix-outbox",
            "error_type": "retrying_records",
            "message": "matrix outbox has retrying records",
            "trigger": trigger,
            "iteration": iteration,
            "retrying_count": retrying_count,
        }
    return None


def run_capture_loop(
    settings: RuntimeSettings,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    capture: StreamProfileCapture,
    overlay: Callable[..., Any],
    detector_factory: Callable[[RuntimeSettings], Any],
    matrix_delivery: Any | None,
    history_archive: VehicleHistoryArchive | None = None,
    matrix_command_service: Any | None = None,
    sleep: Callable[[float], None],
    max_iterations: int | None = None,
    now: Callable[[], datetime] | None = None,
    startup_retention_failure_count: int = 0,
) -> int:
    iteration = 0
    detector: Any | None = None
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
    startup_outbox_error = drain_matrix_outbox_if_available(matrix_delivery, logger=logger, iteration=iteration, trigger="startup")
    health_state.record_matrix_result(startup_outbox_error)
    decision_memory_path = data_dir / "operator-decision-memory.json"
    shutdown_state = ShutdownState()
    vehicle_history_health = VehicleHistoryHealthSnapshotCache(
        effective_history_archive,
        now=now_fn,
        ttl_seconds=VEHICLE_HISTORY_HEALTH_CACHE_SECONDS,
    )
    outbox_health_provider = getattr(matrix_delivery, "outbox_health_summary", None)
    if not callable(outbox_health_provider):
        outbox_health_provider = None

    def write_current_health(*, status: str, iteration: int) -> None:
        write_loop_health(
            settings,
            logger=logger,
            status=status,
            iteration=iteration,
            last_frame_at=health_state.last_frame_at,
            selected_decode_mode=health_state.selected_decode_mode,
            capture_last_success_at=health_state.capture_last_success_at,
            capture_selected_decode_mode=health_state.capture_selected_decode_mode,
            consecutive_capture_failures=health_state.consecutive_capture_failures,
            consecutive_detection_failures=health_state.consecutive_detection_failures,
            last_matrix_error=health_state.last_matrix_error,
            last_error=health_state.last_error,
            retention_failure_count=health_state.retention_failure_count,
            state_save_error=health_state.state_save_error,
            matrix_command_failure_count=health_state.matrix_command_failure_count,
            last_matrix_command_error=health_state.last_matrix_command_error,
            vehicle_history_failure_count=health_state.vehicle_history_failure_count,
            last_vehicle_history_error=health_state.last_vehicle_history_error,
            vehicle_history=vehicle_history_health.snapshot(
                force=health_state.last_vehicle_history_error is not None
                or health_state.vehicle_history_failure_count > 0
            ),
            matrix_outbox_file=runtime_paths.matrix_outbox_file,
            matrix_outbox_summary_provider=outbox_health_provider,
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
        startup_status = "degraded" if health_state.retention_failure_count or startup_outbox_error is not None else "starting"
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
            logger.debug("capture-loop-iteration", iteration=iteration, data_dir=str(data_dir))
            try:
                outbox_error = drain_matrix_outbox_if_available(
                    matrix_delivery,
                    logger=logger,
                    iteration=iteration,
                    trigger="iteration",
                    max_records=MATRIX_OUTBOX_MAX_RECORDS_PER_ITERATION,
                )
                health_state.record_matrix_result(outbox_error)
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
                if detection_result is not None:
                    health_state.record_processed_frame(timestamp=result.timestamp, selected_mode=result.selected_mode)
                    record_detection_memory_records(
                        decision_memory_path,
                        detection_result,
                        observed_at=result.timestamp,
                        logger=logger,
                        mode="runtime-loop",
                        iteration=iteration,
                    )
                    _write_overlay_for_capture(settings, result.latest_path, data_dir, logger=logger, overlay=overlay)
                    timeline_result = record_timeline_frame(result.latest_path, data_dir=data_dir, observed_at=result.timestamp)
                    logger.debug("timeline-frame-retained", iteration=iteration, **timeline_result.diagnostics())
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
                        decision_memory_path=decision_memory_path,
                    )
                    runtime_state = frame_update.runtime_state
                    health_state.record_frame_update(
                        matrix_errors=frame_update.matrix_errors,
                        history_errors=frame_update.history_errors,
                        state_save_error=frame_update.state_save_error,
                    )
                    command_error = _poll_matrix_commands_once(
                        matrix_command_service,
                        logger=logger,
                        iteration=iteration,
                        decision_memory_path=decision_memory_path,
                    )
                    health_state.record_command_result(command_error)
                logger.info("capture-loop-frame-written", iteration=iteration, **result.diagnostics())
                write_current_health(status=health_state.status(), iteration=iteration)
                logger.debug("capture-loop-paced", iteration=iteration, sleep_seconds=settings.runtime.frame_interval_seconds)
                sleep(settings.runtime.frame_interval_seconds)
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
                backoff_seconds = settings.stream.reconnect_seconds
                logger.error("capture-loop-failure", iteration=iteration, backoff_seconds=backoff_seconds, **exc.diagnostics())
                write_current_health(status="down", iteration=iteration)
                sleep(backoff_seconds)
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
