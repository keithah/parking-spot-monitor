from __future__ import annotations

from dataclasses import dataclass

from parking_spot_monitor.capture import CaptureError, FrameCaptureResult
from parking_spot_monitor.detection import DetectionError, DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.runtime_health import RuntimeLoopHealthState
from parking_spot_monitor.runtime_log_aggregation import RuntimeLogAggregator


@dataclass(frozen=True)
class RuntimeFrameCaptureFailed:
    error: CaptureError
@dataclass(frozen=True)
class RuntimeFrameDetectionFailed:
    primary_capture: FrameCaptureResult
    capture: FrameCaptureResult
    detector: object | None
    error: DetectionError
@dataclass(frozen=True)
class RuntimeFrameCaptureEscalationFailed:
    capture: FrameCaptureResult
    detector: object
    error: CaptureError
@dataclass(frozen=True)
class RuntimeFrameDetected:
    primary_capture: FrameCaptureResult
    capture: FrameCaptureResult
    detector: object
    detection: DetectionFilterResult
    escalated: bool


RuntimeFrameAttempt = RuntimeFrameCaptureFailed | RuntimeFrameDetectionFailed | RuntimeFrameCaptureEscalationFailed | RuntimeFrameDetected

@dataclass(frozen=True)
class RuntimeFrameLoopResult:
    primary_capture: FrameCaptureResult
    capture: FrameCaptureResult
    detector: object | None
    detection: DetectionFilterResult | None = None
    escalated: bool = False


def prepare_runtime_frame_loop_result(
    frame_attempt: RuntimeFrameAttempt,
    *,
    health_state: RuntimeLoopHealthState,
    logger: StructuredLogger,
    iteration: int,
    log_aggregator: RuntimeLogAggregator | None = None,
) -> RuntimeFrameLoopResult:
    # Preserve capture-success health side effects before escalation errors propagate.
    if isinstance(frame_attempt, RuntimeFrameCaptureFailed):
        raise frame_attempt.error

    if isinstance(frame_attempt, RuntimeFrameCaptureEscalationFailed):
        _record_capture_success(health_state, frame_attempt.capture)
        raise frame_attempt.error

    if isinstance(frame_attempt, RuntimeFrameDetectionFailed):
        _record_capture_success(health_state, frame_attempt.capture)
        health_state.record_detection_failure(frame_attempt.error, iteration=iteration)
        error_type = frame_attempt.error.diagnostics().get("error_type", "DetectionError")
        first_or_transition = log_aggregator is None or log_aggregator.record_failure("detection", error_type)
        log_failure = logger.error if first_or_transition else logger.debug
        log_failure(
            "detection-frame-failed",
            mode="runtime-loop",
            iteration=iteration,
            **frame_attempt.error.diagnostics(),
        )
        return RuntimeFrameLoopResult(primary_capture=frame_attempt.primary_capture, capture=frame_attempt.capture, detector=frame_attempt.detector)

    _record_capture_success(health_state, frame_attempt.capture)
    return RuntimeFrameLoopResult(
        primary_capture=frame_attempt.primary_capture,
        capture=frame_attempt.capture,
        detector=frame_attempt.detector,
        detection=frame_attempt.detection,
        escalated=frame_attempt.escalated,
    )


def _record_capture_success(health_state: RuntimeLoopHealthState, capture: FrameCaptureResult) -> None:
    health_state.record_capture_success(timestamp=capture.timestamp, selected_mode=capture.selected_mode)
