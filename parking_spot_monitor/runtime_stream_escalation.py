from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from parking_spot_monitor.capture import CaptureError, FrameCaptureResult, StreamProfileCapture
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionError, DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy import OccupancyStatus
from parking_spot_monitor.runtime_detection import _process_detection_for_capture
from parking_spot_monitor.state import RuntimeState


@dataclass(frozen=True)
class StreamDetectionResult:
    final_capture: FrameCaptureResult
    detection: DetectionFilterResult


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
        return StreamDetectionResult(final_capture=primary_result, detection=primary_detection)
    if not _should_escalate_stream_result(settings, runtime_state, primary_detection):
        return StreamDetectionResult(final_capture=primary_result, detection=primary_detection)

    logger.info(
        "stream-profile-escalated",
        from_profile=primary_result.frame_geometry.stream_profile,
        to_profile=escalation_profile,
        reason="weak-primary-detection",
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
    return StreamDetectionResult(final_capture=high_result, detection=high_detection)


def _should_escalate_stream_result(
    settings: RuntimeSettings,
    runtime_state: RuntimeState,
    detection_result: DetectionFilterResult,
) -> bool:
    min_confidence = settings.stream.escalation_min_confidence
    for spot_result in detection_result.by_spot.values():
        if spot_result.accepted is not None and spot_result.accepted.confidence < min_confidence:
            return True

    for spot_id, spot_state in runtime_state.state_by_spot.items():
        if spot_state.status is not OccupancyStatus.OCCUPIED:
            continue
        spot_result = detection_result.by_spot.get(spot_id)
        if spot_result is not None and spot_result.accepted is not None:
            continue
        if spot_state.miss_streak + 1 >= settings.occupancy.release_frames:
            return True
    return False
