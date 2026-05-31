"""Long-running capture / detect / notify runtime loop."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor.capture import CaptureError
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionError
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix import MONITOR_STARTED_EVENT_TYPE, monitor_lifecycle_event
from parking_spot_monitor.matrix_dispatch import dispatch_matrix_event
from parking_spot_monitor.paths import resolve_runtime_paths
from parking_spot_monitor.runtime_health import (
    format_health_timestamp,
    health_status_for_loop,
    observed_at,
    safe_error_context,
    write_loop_health,
)
from parking_spot_monitor.runtime_lifecycle import (
    ShutdownState,
    monitor_signal_handlers,
    return_if_shutdown_requested,
)
from parking_spot_monitor.state import load_runtime_state
from parking_spot_monitor.timeline_buffer import record_timeline_frame
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive


def drain_matrix_outbox_if_available(
    matrix_delivery: Any | None,
    *,
    logger: StructuredLogger,
    iteration: int,
    trigger: str,
) -> dict[str, Any] | None:
    drain = getattr(matrix_delivery, "drain_outbox", None)
    if drain is None or not callable(drain):
        return None
    logger.info("matrix-outbox-runtime-drain-attempt", trigger=trigger, iteration=iteration)
    try:
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
    capture: Callable[[RuntimeSettings, str | Path], Any],
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
    from parking_spot_monitor import __main__ as runtime_main

    iteration = 0
    detector: Any | None = None
    spot_ids = list(runtime_main._configured_spot_polygons(settings).keys())
    state_path = data_dir / "state.json"
    runtime_paths = resolve_runtime_paths(settings, data_dir)
    runtime_state = load_runtime_state(state_path, spot_ids, logger=logger)
    effective_history_archive = (
        history_archive if history_archive is not None else VehicleHistoryArchive(data_dir / "vehicle-history", logger=logger)
    )
    now_fn = now if now is not None else lambda: datetime.now(timezone.utc)
    consecutive_capture_failures = 0
    consecutive_detection_failures = 0
    last_frame_at: str | None = None
    selected_decode_mode: str | None = None
    last_matrix_error: dict[str, Any] | None = None
    last_error: dict[str, Any] | None = None
    state_save_error: dict[str, Any] | None = None
    last_vehicle_history_error: dict[str, Any] | None = None
    vehicle_history_failure_count = 0
    retention_failure_count = startup_retention_failure_count
    startup_outbox_error = drain_matrix_outbox_if_available(matrix_delivery, logger=logger, iteration=iteration, trigger="startup")
    if startup_outbox_error is not None:
        last_matrix_error = startup_outbox_error
        last_error = startup_outbox_error
    decision_memory_path = data_dir / "operator-decision-memory.json"
    shutdown_state = ShutdownState()
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
        write_loop_health(
            settings,
            logger=logger,
            status="degraded" if retention_failure_count or startup_outbox_error is not None else "starting",
            iteration=iteration,
            last_frame_at=last_frame_at,
            selected_decode_mode=selected_decode_mode,
            consecutive_capture_failures=consecutive_capture_failures,
            consecutive_detection_failures=consecutive_detection_failures,
            last_matrix_error=last_matrix_error,
            last_error=last_error,
            retention_failure_count=retention_failure_count,
            state_save_error=state_save_error,
            vehicle_history_failure_count=vehicle_history_failure_count,
            last_vehicle_history_error=last_vehicle_history_error,
            vehicle_history=effective_history_archive.health_snapshot(),
            matrix_outbox_file=runtime_paths.matrix_outbox_file,
        )
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
            logger.info("capture-loop-iteration", iteration=iteration, data_dir=str(data_dir))
            try:
                outbox_error = drain_matrix_outbox_if_available(matrix_delivery, logger=logger, iteration=iteration, trigger="iteration")
                if outbox_error is not None:
                    last_matrix_error = outbox_error
                    last_error = outbox_error
                result = capture(settings, data_dir)
                consecutive_capture_failures = 0
                last_frame_at = format_health_timestamp(result.timestamp)
                selected_decode_mode = str(result.selected_mode.value if hasattr(result.selected_mode, "value") else result.selected_mode)
                runtime_main._write_overlay_for_capture(settings, result.latest_path, data_dir, logger=logger, overlay=overlay)
                timeline_result = record_timeline_frame(result.latest_path, data_dir=data_dir, observed_at=result.timestamp)
                logger.info("timeline-frame-retained", iteration=iteration, **timeline_result.diagnostics())
                try:
                    if detector is None:
                        detector = detector_factory(settings)
                    detection_result = runtime_main._process_detection_for_capture(
                        settings,
                        detector,
                        result.latest_path,
                        frame_timestamp=result.timestamp,
                        logger=logger,
                        mode="runtime-loop",
                        iteration=iteration,
                        decision_memory_path=decision_memory_path,
                    )
                except DetectionError as exc:
                    consecutive_detection_failures += 1
                    last_error = safe_error_context("detection", exc, extra={"iteration": iteration})
                    logger.error("detection-frame-failed", mode="runtime-loop", iteration=iteration, **exc.diagnostics())
                else:
                    consecutive_detection_failures = 0
                    last_error = None
                    frame_observed_at = observed_at(result.timestamp, now_fn)
                    frame_update = runtime_main._update_runtime_state_for_frame(
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
                    if frame_update.matrix_errors:
                        last_matrix_error = frame_update.matrix_errors[-1]
                        last_error = last_matrix_error
                    if frame_update.history_errors:
                        vehicle_history_failure_count += len(frame_update.history_errors)
                        last_vehicle_history_error = frame_update.history_errors[-1]
                        last_error = last_vehicle_history_error
                    state_save_error = frame_update.state_save_error
                    if state_save_error is not None:
                        last_error = state_save_error
                    command_error = runtime_main._poll_matrix_commands_once(
                        matrix_command_service,
                        logger=logger,
                        iteration=iteration,
                        decision_memory_path=decision_memory_path,
                    )
                    if command_error is not None:
                        vehicle_history_failure_count += 1
                        last_vehicle_history_error = command_error
                        last_error = command_error
                logger.info("capture-loop-frame-written", iteration=iteration, **result.diagnostics())
                status = health_status_for_loop(
                    consecutive_capture_failures=consecutive_capture_failures,
                    consecutive_detection_failures=consecutive_detection_failures,
                    last_matrix_error=last_matrix_error,
                    state_save_error=state_save_error,
                    retention_failure_count=retention_failure_count,
                    vehicle_history_failure_count=vehicle_history_failure_count,
                    last_vehicle_history_error=last_vehicle_history_error,
                )
                write_loop_health(
                    settings,
                    logger=logger,
                    status=status,
                    iteration=iteration,
                    last_frame_at=last_frame_at,
                    selected_decode_mode=selected_decode_mode,
                    consecutive_capture_failures=consecutive_capture_failures,
                    consecutive_detection_failures=consecutive_detection_failures,
                    last_matrix_error=last_matrix_error,
                    last_error=last_error,
                    retention_failure_count=retention_failure_count,
                    state_save_error=state_save_error,
                    vehicle_history_failure_count=vehicle_history_failure_count,
                    last_vehicle_history_error=last_vehicle_history_error,
                    vehicle_history=effective_history_archive.health_snapshot(),
                    matrix_outbox_file=runtime_paths.matrix_outbox_file,
                )
                logger.info("capture-loop-paced", iteration=iteration, sleep_seconds=settings.runtime.frame_interval_seconds)
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
                consecutive_capture_failures += 1
                last_error = safe_error_context("capture", exc, extra={"iteration": iteration})
                backoff_seconds = settings.stream.reconnect_seconds
                logger.error("capture-loop-failure", iteration=iteration, backoff_seconds=backoff_seconds, **exc.diagnostics())
                write_loop_health(
                    settings,
                    logger=logger,
                    status="down",
                    iteration=iteration,
                    last_frame_at=last_frame_at,
                    selected_decode_mode=selected_decode_mode,
                    consecutive_capture_failures=consecutive_capture_failures,
                    consecutive_detection_failures=consecutive_detection_failures,
                    last_matrix_error=last_matrix_error,
                    last_error=last_error,
                    retention_failure_count=retention_failure_count,
                    state_save_error=state_save_error,
                    vehicle_history_failure_count=vehicle_history_failure_count,
                    last_vehicle_history_error=last_vehicle_history_error,
                    vehicle_history=effective_history_archive.health_snapshot(),
                    matrix_outbox_file=runtime_paths.matrix_outbox_file,
                )
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
