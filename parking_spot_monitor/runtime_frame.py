from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from parking_spot_monitor.capture import CaptureError, StreamProfileCapture
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionError
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.runtime_frame_outcome import (
    RuntimeFrameAttempt,
    RuntimeFrameCaptureEscalationFailed,
    RuntimeFrameCaptureFailed,
    RuntimeFrameDetected,
    RuntimeFrameDetectionFailed,
)
from parking_spot_monitor.runtime_stream_escalation import (
    StreamEscalationCaptureFailed,
    StreamEscalationDetectionFailed,
    detect_with_stream_escalation,
)
from parking_spot_monitor.state import RuntimeState


def capture_and_detect_runtime_frame(
    settings: RuntimeSettings,
    data_dir: Path,
    *,
    capture: StreamProfileCapture,
    detector: object | None,
    detector_factory: Callable[[RuntimeSettings], object],
    runtime_state: RuntimeState,
    logger: StructuredLogger,
    mode: str,
    iteration: int,
) -> RuntimeFrameAttempt:
    try:
        primary_result = capture(settings, data_dir)
    except CaptureError as exc:
        return RuntimeFrameCaptureFailed(error=exc)

    try:
        detector = detector if detector is not None else detector_factory(settings)
        outcome = detect_with_stream_escalation(
            settings,
            data_dir,
            capture=capture,
            detector=detector,
            runtime_state=runtime_state,
            primary_result=primary_result,
            logger=logger,
            mode=mode,
            iteration=iteration,
        )
    except DetectionError as exc:
        return RuntimeFrameDetectionFailed(capture=primary_result, detector=detector, error=exc)

    if isinstance(outcome, StreamEscalationCaptureFailed):
        return RuntimeFrameCaptureEscalationFailed(
            capture=outcome.last_successful_capture,
            detector=detector,
            error=outcome.error,
        )
    if isinstance(outcome, StreamEscalationDetectionFailed):
        return RuntimeFrameDetectionFailed(capture=outcome.last_successful_capture, detector=detector, error=outcome.error)

    return RuntimeFrameDetected(
        capture=outcome.final_capture,
        detector=detector,
        detection=outcome.detection,
    )
