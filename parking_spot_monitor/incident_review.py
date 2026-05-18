from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import (
    DetectionFilterResult,
    RejectedDetection,
    SpotDetectionCandidate,
    VehicleDetection,
    filter_spot_detections,
)
from parking_spot_monitor.logging import redact_diagnostic_text
from parking_spot_monitor.occupancy import OccupancyStatus, QuietWindowStatus, SpotOccupancyState, update_occupancy
from parking_spot_monitor.state import RuntimeState, StateSchemaError, _state_from_json


class IncidentDetector(Protocol):
    def detect(
        self,
        frame_path: str | Path,
        *,
        confidence_threshold: float | None = None,
        inference_image_size: int | None = None,
    ) -> Sequence[VehicleDetection]: ...


@dataclass(frozen=True)
class IncidentFrameEvidence:
    path: Path
    observed_at: datetime
    byte_size: int
    width: int
    height: int


@dataclass(frozen=True)
class IncidentReplayResult:
    """Bounded, redacted local replay evidence for one retained incident frame."""

    frame: IncidentFrameEvidence | None
    lines: tuple[str, ...]
    unavailable_reason: str | None = None
    detector_error_type: str | None = None
    state_error_type: str | None = None
    image_upload_allowed: bool = False


@dataclass(frozen=True)
class _ReplayStateLoad:
    state: RuntimeState
    error_type: str | None = None


@dataclass(frozen=True)
class _SpotReplaySummary:
    spot_id: str
    prior: SpotOccupancyState
    after: SpotOccupancyState
    accepted: SpotDetectionCandidate | None
    rejected: tuple[RejectedDetection, ...] = field(default_factory=tuple)
    event_count: int = 0


def build_incident_replay(
    *,
    settings: RuntimeSettings | None,
    frame_path: str | Path,
    frame_time: datetime,
    requested_spot_id: str,
    state_path: str | Path | None,
    detector: IncidentDetector | None,
) -> IncidentReplayResult:
    """Replay detector and occupancy evidence without mutating runtime state.

    This function intentionally performs only local reads and one optional detector
    call for the selected retained frame. It never calls capture, Matrix delivery,
    state persistence, alerting, or history writers.
    """

    frame = _validate_frame(frame_path, frame_time=frame_time)
    if frame is None:
        return IncidentReplayResult(
            frame=None,
            lines=(
                "Nearest retained frame: unavailable",
                "Incident replay unavailable: corrupt_frame",
                "No detector, camera, Matrix send, or state mutation was run.",
            ),
            unavailable_reason="corrupt_frame",
            image_upload_allowed=False,
        )

    if settings is None:
        return IncidentReplayResult(
            frame=frame,
            lines=(
                "Detector replay unavailable: settings_unavailable",
                "State simulation unavailable: settings_unavailable",
                "No detector, camera, Matrix send, or state mutation was run.",
            ),
            unavailable_reason="settings_unavailable",
            image_upload_allowed=True,
        )

    spot_ids = list(_configured_spot_polygons(settings).keys())
    detections, detector_error_type = _run_detector(settings=settings, detector=detector, frame=frame)
    detection_result = None if detections is None else _filter_detections(settings=settings, frame=frame, detections=detections)
    state_load = _load_state_without_side_effects(state_path, spot_ids=spot_ids)

    lines: list[str] = []
    if detector_error_type is not None:
        lines.append(f"Detector replay unavailable: {detector_error_type}")
    elif detection_result is None or not detections:
        lines.append("Detector replay: no vehicle evidence")
    else:
        lines.append("Detector replay:")

    if detection_result is not None:
        lines.extend(_format_detection_lines(detection_result, requested_spot_id=requested_spot_id))

    if state_load.error_type is not None:
        lines.append(f"Runtime state unavailable: {state_load.error_type}; simulated from unknown/default state")

    if detection_result is None:
        candidates_by_spot = {spot_id: None for spot_id in spot_ids}
        presence_by_spot: dict[str, bool] = {}
        rejected_by_spot: dict[str, tuple[RejectedDetection, ...]] = {spot_id: () for spot_id in spot_ids}
    else:
        candidates_by_spot = {spot_id: result.accepted for spot_id, result in detection_result.by_spot.items()}
        presence_by_spot = _presence_by_spot(settings=settings, detection_result=detection_result)
        rejected_by_spot = {spot_id: tuple(result.rejected) for spot_id, result in detection_result.by_spot.items()}

    update = update_occupancy(
        state_load.state.state_by_spot,
        candidates_by_spot,
        settings.occupancy,
        observed_at=frame.observed_at.isoformat().replace("+00:00", "Z"),
        quiet_window_status=QuietWindowStatus(active=False),
        snapshot_path=str(frame.path.name),
        configured_spot_ids=spot_ids,
        presence_by_spot=presence_by_spot,
    )
    lines.append("State simulation:")
    for summary in _spot_summaries(
        spot_ids=spot_ids,
        prior_state=state_load.state.state_by_spot,
        next_state=update.state_by_spot,
        candidates_by_spot=candidates_by_spot,
        rejected_by_spot=rejected_by_spot,
        event_counts=_event_counts(update.events),
    ):
        lines.append(_format_state_line(summary))
    lines.append("Simulation only: no live state was changed")

    return IncidentReplayResult(
        frame=frame,
        lines=tuple(lines),
        detector_error_type=detector_error_type,
        state_error_type=state_load.error_type,
        image_upload_allowed=True,
    )


def _validate_frame(frame_path: str | Path, *, frame_time: datetime) -> IncidentFrameEvidence | None:
    path = Path(frame_path)
    if path.suffix.lower() not in {".jpg", ".jpeg"}:
        return None
    try:
        with Image.open(path) as image:
            image.verify()
        with Image.open(path) as image:
            width, height = image.size
        byte_size = path.stat().st_size
    except (OSError, UnidentifiedImageError, ValueError):
        return None
    return IncidentFrameEvidence(path=path, observed_at=frame_time.astimezone(timezone.utc), byte_size=byte_size, width=width, height=height)


def _run_detector(
    *,
    settings: RuntimeSettings,
    detector: IncidentDetector | None,
    frame: IncidentFrameEvidence,
) -> tuple[list[VehicleDetection] | None, str | None]:
    if detector is None:
        return None, "detector_unavailable"
    kwargs: dict[str, Any] = {"confidence_threshold": settings.detection.confidence_threshold}
    if settings.detection.inference_image_size is not None:
        kwargs["inference_image_size"] = settings.detection.inference_image_size
    try:
        return list(detector.detect(frame.path, **kwargs)), None
    except Exception as exc:  # safe degradation boundary for Matrix command replies
        return None, redact_diagnostic_text(exc.__class__.__name__) or "detector_error"


def _filter_detections(*, settings: RuntimeSettings, frame: IncidentFrameEvidence, detections: Sequence[VehicleDetection]) -> DetectionFilterResult:
    return filter_spot_detections(
        detections,
        spots=_configured_spot_polygons(settings),
        allowed_classes=settings.detection.vehicle_classes,
        confidence_threshold=settings.detection.confidence_threshold,
        min_bbox_area_px=settings.detection.min_bbox_area_px,
        min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
        source_frame_path=str(frame.path.name),
        source_timestamp=frame.observed_at.isoformat().replace("+00:00", "Z"),
    )


def _load_state_without_side_effects(state_path: str | Path | None, *, spot_ids: Sequence[str]) -> _ReplayStateLoad:
    if state_path is None:
        return _ReplayStateLoad(RuntimeState.default(spot_ids), "state_path_unavailable")
    path = Path(state_path)
    if not path.exists():
        return _ReplayStateLoad(RuntimeState.default(spot_ids), "missing")
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return _ReplayStateLoad(_state_from_json(payload, list(spot_ids)))
    except json.JSONDecodeError:
        return _ReplayStateLoad(RuntimeState.default(spot_ids), "JSONDecodeError")
    except (OSError, StateSchemaError) as exc:
        return _ReplayStateLoad(RuntimeState.default(spot_ids), redact_diagnostic_text(exc.__class__.__name__) or "state_error")


def _configured_spot_polygons(settings: RuntimeSettings) -> dict[str, list[tuple[float, float]]]:
    return {
        "left_spot": [(float(point.x), float(point.y)) for point in settings.spots.left_spot.polygon],
        "right_spot": [(float(point.x), float(point.y)) for point in settings.spots.right_spot.polygon],
    }


def _format_detection_lines(result: DetectionFilterResult, *, requested_spot_id: str) -> list[str]:
    lines: list[str] = []
    for spot_id in _ordered_spot_ids(result.by_spot.keys(), requested_spot_id=requested_spot_id):
        spot_result = result.by_spot[spot_id]
        if spot_result.accepted is not None:
            candidate = spot_result.accepted
            lines.append(
                f"- {spot_id}: accepted {redact_diagnostic_text(candidate.class_name)} confidence {candidate.confidence:.2f} "
                f"overlap {candidate.overlap_ratio:.2f}"
            )
            continue
        if spot_result.rejected:
            rejection = _top_rejection(spot_result.rejected)
            if spot_id == requested_spot_id:
                lines.append(
                    f"- {spot_id}: accepted {redact_diagnostic_text(rejection.detection.class_name)} "
                    f"confidence {rejection.detection.confidence:.2f}? no; rejected {rejection.reason.value}"
                )
            else:
                lines.append(f"- {spot_id}: rejected {rejection.reason.value}")
        else:
            lines.append(f"- {spot_id}: no vehicle evidence")
    return lines


def _presence_by_spot(*, settings: RuntimeSettings, detection_result: DetectionFilterResult) -> dict[str, bool]:
    presence: dict[str, bool] = {}
    allowed = set(settings.detection.open_suppression_classes or settings.detection.vehicle_classes)
    min_conf = settings.detection.open_suppression_min_confidence
    for spot_id, spot_result in detection_result.by_spot.items():
        presence[spot_id] = any(
            rejected.detection.class_name in allowed and rejected.detection.confidence >= min_conf for rejected in spot_result.rejected
        )
    return presence


def _spot_summaries(
    *,
    spot_ids: Sequence[str],
    prior_state: Mapping[str, SpotOccupancyState],
    next_state: Mapping[str, SpotOccupancyState],
    candidates_by_spot: Mapping[str, SpotDetectionCandidate | None],
    rejected_by_spot: Mapping[str, tuple[RejectedDetection, ...]],
    event_counts: Mapping[str, int],
) -> list[_SpotReplaySummary]:
    summaries: list[_SpotReplaySummary] = []
    for spot_id in spot_ids:
        summaries.append(
            _SpotReplaySummary(
                spot_id=spot_id,
                prior=prior_state.get(spot_id, SpotOccupancyState()),
                after=next_state.get(spot_id, SpotOccupancyState()),
                accepted=candidates_by_spot.get(spot_id),
                rejected=rejected_by_spot.get(spot_id, ()),
                event_count=event_counts.get(spot_id, 0),
            )
        )
    return summaries


def _format_state_line(summary: _SpotReplaySummary) -> str:
    prior = OccupancyStatus(summary.prior.status).value
    after = OccupancyStatus(summary.after.status).value
    if summary.accepted is not None:
        if prior == after:
            implication = f"{prior} would remain {after}"
        else:
            implication = f"{prior} would become {after}"
    elif summary.prior.miss_streak != summary.after.miss_streak:
        implication = f"{prior} would increment miss streak to {summary.after.miss_streak}"
    else:
        implication = f"{prior} would remain {after}"
    if summary.event_count:
        implication += f"; would emit {summary.event_count} event(s)"
    return f"- {summary.spot_id}: {implication}"


def _event_counts(events: Sequence[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in events:
        spot_id = str(getattr(event, "spot_id", ""))
        if spot_id:
            counts[spot_id] = counts.get(spot_id, 0) + 1
    return counts


def _top_rejection(rejections: Sequence[RejectedDetection]) -> RejectedDetection:
    return sorted(
        rejections,
        key=lambda rejection: (
            -float(rejection.detection.confidence),
            str(rejection.reason.value),
        ),
    )[0]


def _ordered_spot_ids(spot_ids: Sequence[str] | Any, *, requested_spot_id: str) -> list[str]:
    ordered = list(spot_ids)
    return sorted(ordered, key=lambda spot_id: (spot_id != requested_spot_id, spot_id))
