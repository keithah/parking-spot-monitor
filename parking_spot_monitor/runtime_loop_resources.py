from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from parking_spot_monitor.capture import FrameCaptureResult
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.paths import RuntimePaths
from parking_spot_monitor.runtime_health import (
    RuntimeLoopHealthState,
    write_loop_health,
)
from parking_spot_monitor.runtime_health_cache import VehicleHistoryHealthSnapshotCache
from parking_spot_monitor.runtime_overlay import write_overlay_for_capture
from parking_spot_monitor.runtime_presence import presence_by_spot
from parking_spot_monitor.runtime_resource_policy import (
    RuntimeResourceDecision,
    RuntimeResourcePolicyState,
    artifact_due,
    decide_runtime_interval,
    remaining_sleep_seconds,
    verification_due,
)
from parking_spot_monitor.state import RuntimeState
from parking_spot_monitor.timeline_buffer import record_timeline_frame

@dataclass(frozen=True)
class ResourcePolicyUpdate:
    state: RuntimeResourcePolicyState
    decision: RuntimeResourceDecision


def write_current_loop_health(
    settings: RuntimeSettings,
    *,
    logger: StructuredLogger,
    health_state: RuntimeLoopHealthState,
    status: str,
    iteration: int,
    vehicle_history_health: VehicleHistoryHealthSnapshotCache,
    runtime_paths: RuntimePaths,
    outbox_health_provider: Callable[[], Any] | None,
) -> None:
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


def periodic_verification_due(
    settings: RuntimeSettings,
    state: RuntimeResourcePolicyState,
    *,
    now_monotonic: float,
) -> bool:
    return (
        state.stable_success_count >= settings.runtime.stable_settle_frames
        and verification_due(
            now_monotonic=now_monotonic,
            last_verification_at=state.last_verification_at,
            interval_seconds=settings.stream.escalation_verification_seconds,
        )
    )


def frame_has_weak_presence(
    settings: RuntimeSettings,
    detection_result: DetectionFilterResult,
) -> bool:
    spot_presence = presence_by_spot(
        detection_result,
        open_suppression_classes=settings.detection.open_suppression_classes,
        min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
    )
    return any(
        spot_presence.get(spot_id, False) and spot_result.accepted is None
        for spot_id, spot_result in detection_result.by_spot.items()
    )


def record_primary_frame_artifacts(
    settings: RuntimeSettings,
    primary_capture: FrameCaptureResult,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    overlay: Callable[..., Any],
    iteration: int,
    policy_state: RuntimeResourcePolicyState,
    now_monotonic: float,
    transition_occurred: bool,
) -> bool:
    timeline_result = record_timeline_frame(
        primary_capture.latest_path,
        data_dir=data_dir,
        observed_at=primary_capture.timestamp,
    )
    logger.debug("timeline-frame-retained", iteration=iteration, **timeline_result.diagnostics())
    if not artifact_due(
        now_monotonic=now_monotonic,
        last_written_at=policy_state.last_overlay_at,
        interval_seconds=settings.runtime.debug_overlay_interval_seconds,
        transition=transition_occurred,
    ):
        return False
    return write_overlay_for_capture(
        settings,
        primary_capture.latest_path,
        data_dir,
        logger=logger,
        overlay=overlay,
    )


def advance_resource_policy(
    settings: RuntimeSettings,
    runtime_state: RuntimeState,
    previous: RuntimeResourcePolicyState,
    *,
    transition_occurred: bool,
    weak_presence: bool,
    degraded: bool,
    verification_succeeded: bool,
    overlay_written: bool,
    completed_at: float,
) -> ResourcePolicyUpdate:
    decision = decide_runtime_interval(
        settings,
        runtime_state,
        previous_stable_success_count=previous.stable_success_count,
        frame_had_transition=transition_occurred,
        frame_has_weak_presence=weak_presence,
        degraded=degraded,
    )
    return ResourcePolicyUpdate(
        state=RuntimeResourcePolicyState(
            stable_success_count=decision.stable_success_count,
            last_verification_at=(
                completed_at if verification_succeeded else previous.last_verification_at
            ),
            last_overlay_at=completed_at if overlay_written else previous.last_overlay_at,
        ),
        decision=decision,
    )


def paced_sleep_seconds(
    settings: RuntimeSettings,
    decision: RuntimeResourceDecision,
    *,
    iteration_started_at: float,
    now_monotonic: float,
) -> float:
    if (
        not settings.runtime.adaptive_polling_enabled
        or settings.runtime.stable_frame_interval_seconds
        == settings.runtime.frame_interval_seconds
    ):
        return decision.interval_seconds
    return remaining_sleep_seconds(
        interval_seconds=decision.interval_seconds,
        iteration_started_at=iteration_started_at,
        now_monotonic=now_monotonic,
    )


def reset_stable_successes(state: RuntimeResourcePolicyState) -> RuntimeResourcePolicyState:
    return RuntimeResourcePolicyState(
        stable_success_count=0,
        last_verification_at=state.last_verification_at,
        last_overlay_at=state.last_overlay_at,
    )
