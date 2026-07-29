from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from parking_spot_monitor.capture import CaptureError, FrameCaptureResult, StreamProfileCapture
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionError, DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
from parking_spot_monitor.runtime_detection import _process_detection_for_capture
from parking_spot_monitor.state import RuntimeState


@dataclass(frozen=True)
class StreamDetectionResult:
    primary_capture: FrameCaptureResult
    final_capture: FrameCaptureResult
    detection: DetectionFilterResult
    escalated: bool


@dataclass(frozen=True)
class StreamEscalationCaptureFailed:
    last_successful_capture: FrameCaptureResult
    error: CaptureError


@dataclass(frozen=True)
class StreamEscalationDetectionFailed:
    last_successful_capture: FrameCaptureResult
    error: DetectionError


StreamEscalationOutcome = StreamDetectionResult | StreamEscalationCaptureFailed | StreamEscalationDetectionFailed


def detect_with_stream_escalation(
    settings: RuntimeSettings,
    data_dir: Path,
    *,
    capture: StreamProfileCapture,
    detector: object,
    runtime_state: RuntimeState,
    primary_result: FrameCaptureResult,
    logger: StructuredLogger,
    mode: str,
    iteration: int,
    periodic_verification_due: bool = False,
) -> StreamEscalationOutcome:
    primary_detection = _process_detection_for_capture(
        settings,
        detector,
        primary_result.latest_path,
        frame_timestamp=primary_result.timestamp,
        logger=logger,
        mode=mode,
        iteration=iteration,
        frame_geometry=primary_result.frame_geometry,
    )
    escalation_profile = settings.stream.escalation_profile
    if escalation_profile is None:
        return StreamDetectionResult(
            primary_capture=primary_result,
            final_capture=primary_result,
            detection=primary_detection,
            escalated=False,
        )
    escalation_reason = _stream_escalation_reason(
        settings,
        runtime_state,
        primary_detection,
        periodic_verification_due=periodic_verification_due,
    )
    if escalation_reason is None:
        return StreamDetectionResult(
            primary_capture=primary_result,
            final_capture=primary_result,
            detection=primary_detection,
            escalated=False,
        )

    logger.info(
        "stream-profile-escalated",
        from_profile=primary_result.frame_geometry.stream_profile,
        to_profile=escalation_profile,
        reason=escalation_reason,
        min_authoritative_confidence=settings.stream.escalation_min_confidence,
        iteration=iteration,
    )
    try:
        high_result = capture(settings, data_dir, stream_profile=escalation_profile)
    except CaptureError as exc:
        return StreamEscalationCaptureFailed(last_successful_capture=primary_result, error=exc)
    try:
        high_detection = _process_detection_for_capture(
            settings,
            detector,
            high_result.latest_path,
            frame_timestamp=high_result.timestamp,
            logger=logger,
            mode=mode,
            iteration=iteration,
            frame_geometry=high_result.frame_geometry,
        )
    except DetectionError as exc:
        return StreamEscalationDetectionFailed(last_successful_capture=high_result, error=exc)
    return StreamDetectionResult(
        primary_capture=primary_result,
        final_capture=high_result,
        detection=high_detection,
        escalated=True,
    )


def _stream_escalation_reason(
    settings: RuntimeSettings,
    runtime_state: RuntimeState,
    detection_result: DetectionFilterResult,
    *,
    periodic_verification_due: bool,
) -> str | None:
    if periodic_verification_due:
        return "periodic-verification"

    min_confidence = settings.stream.escalation_min_confidence
    for spot_id, spot_result in detection_result.by_spot.items():
        if spot_result.accepted is not None and spot_result.accepted.confidence < min_confidence:
            prior = runtime_state.state_by_spot.get(spot_id, SpotOccupancyState())
            if prior.status is not OccupancyStatus.OCCUPIED:
                return "weak-transition-candidate"

    for spot_id, spot_state in runtime_state.state_by_spot.items():
        if spot_state.status is not OccupancyStatus.OCCUPIED:
            continue
        spot_result = detection_result.by_spot.get(spot_id)
        if spot_result is not None and spot_result.accepted is not None:
            continue
        if spot_state.miss_streak + 1 >= settings.occupancy.release_frames:
            return "release-transition-candidate"
    return None
