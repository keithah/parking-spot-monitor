from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from parking_spot_monitor.capture import CaptureError, StreamProfileCapture
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionError
from parking_spot_monitor.detector_adapter import DetectorRunner, SharedLazyDetector, adapt_detector
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
    detector: DetectorRunner | None,
    detector_factory: Callable[[RuntimeSettings], object],
    runtime_state: RuntimeState,
    logger: StructuredLogger,
    mode: str,
    iteration: int,
    periodic_verification_due: bool = False,
) -> RuntimeFrameAttempt:
    try:
        primary_result = capture(settings, data_dir)
    except CaptureError as exc:
        return RuntimeFrameCaptureFailed(error=exc)

    try:
        if detector is None:
            raw_detector = detector_factory(settings)
            detector = raw_detector if isinstance(raw_detector, SharedLazyDetector) else adapt_detector(raw_detector)
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
            periodic_verification_due=periodic_verification_due,
        )
    except DetectionError as exc:
        return RuntimeFrameDetectionFailed(primary_capture=primary_result, capture=primary_result, detector=detector, error=exc)

    if isinstance(outcome, StreamEscalationCaptureFailed):
        return RuntimeFrameCaptureEscalationFailed(
            capture=outcome.last_successful_capture,
            detector=detector,
            error=outcome.error,
        )
    if isinstance(outcome, StreamEscalationDetectionFailed):
        return RuntimeFrameDetectionFailed(
            primary_capture=primary_result,
            capture=outcome.last_successful_capture,
            detector=detector,
            error=outcome.error,
        )

    return RuntimeFrameDetected(
        primary_capture=outcome.primary_capture,
        capture=outcome.final_capture,
        detector=detector,
        detection=outcome.detection,
        escalated=outcome.escalated,
    )
