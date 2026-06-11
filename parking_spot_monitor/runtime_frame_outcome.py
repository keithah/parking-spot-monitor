from __future__ import annotations

from dataclasses import dataclass

from parking_spot_monitor.capture import CaptureError, FrameCaptureResult
from parking_spot_monitor.detection import DetectionError, DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.runtime_health import RuntimeLoopHealthState


@dataclass(frozen=True)
class RuntimeFrameCaptureFailed:
    error: CaptureError


@dataclass(frozen=True)
class RuntimeFrameDetectionFailed:
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
    capture: FrameCaptureResult
    detector: object
    detection: DetectionFilterResult


RuntimeFrameAttempt = (
    RuntimeFrameCaptureFailed
    | RuntimeFrameDetectionFailed
    | RuntimeFrameCaptureEscalationFailed
    | RuntimeFrameDetected
)


@dataclass(frozen=True)
class RuntimeFrameLoopResult:
    capture: FrameCaptureResult
    detector: object | None
    detection: DetectionFilterResult | None = None


def prepare_runtime_frame_loop_result(
    frame_attempt: RuntimeFrameAttempt,
    *,
    health_state: RuntimeLoopHealthState,
    logger: StructuredLogger,
    iteration: int,
) -> RuntimeFrameLoopResult:
    # Capture errors are carried back as outcome objects rather than propagating
    # directly so that health side-effects (recording the last successful capture
    # on the escalation path) run here before the loop's CaptureError handler sees them.
    if isinstance(frame_attempt, RuntimeFrameCaptureFailed):
        raise frame_attempt.error

    if isinstance(frame_attempt, RuntimeFrameCaptureEscalationFailed):
        _record_capture_success(health_state, frame_attempt.capture)
        raise frame_attempt.error

    if isinstance(frame_attempt, RuntimeFrameDetectionFailed):
        _record_capture_success(health_state, frame_attempt.capture)
        health_state.record_detection_failure(frame_attempt.error, iteration=iteration)
        logger.error(
            "detection-frame-failed",
            mode="runtime-loop",
            iteration=iteration,
            **frame_attempt.error.diagnostics(),
        )
        return RuntimeFrameLoopResult(capture=frame_attempt.capture, detector=frame_attempt.detector)

    _record_capture_success(health_state, frame_attempt.capture)
    return RuntimeFrameLoopResult(
        capture=frame_attempt.capture,
        detector=frame_attempt.detector,
        detection=frame_attempt.detection,
    )


def _record_capture_success(health_state: RuntimeLoopHealthState, capture: FrameCaptureResult) -> None:
    health_state.record_capture_success(timestamp=capture.timestamp, selected_mode=capture.selected_mode)
