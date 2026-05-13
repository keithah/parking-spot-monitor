from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
import math
from os import PathLike
from typing import Any, Iterable, Mapping, Sequence

from parking_spot_monitor.geometry import (
    PolygonInput,
    bbox_area,
    bbox_centroid,
    bbox_polygon_overlap_ratio,
    point_in_polygon,
)
from parking_spot_monitor.logging import redact_diagnostic_text

BBoxTuple = tuple[float, float, float, float]


class DetectionError(RuntimeError):
    """Safe detector diagnostic without traceback or secret-bearing payloads."""

    def __init__(
        self,
        message: str,
        *,
        model_path: str,
        phase: str,
        error_type: str,
        frame_path: str | None = None,
    ) -> None:
        sanitized_message = _sanitize_error_message(message)
        super().__init__(sanitized_message)
        self.model_path = _sanitize_error_message(model_path)
        self.frame_path = _sanitize_error_message(frame_path) if frame_path is not None else None
        self.phase = phase
        self.error_type = error_type
        self.message = sanitized_message

    def __str__(self) -> str:
        parts = [
            f"phase={self.phase}",
            f"model_path={self.model_path}",
        ]
        if self.frame_path is not None:
            parts.append(f"frame_path={self.frame_path}")
        parts.extend([f"error_type={self.error_type}", f"message={self.message}"])
        return "detection error (" + ", ".join(parts) + ")"

    def diagnostics(self) -> dict[str, str]:
        """Return structured fields that are safe for JSON-line logs."""

        diagnostic = {
            "phase": self.phase,
            "model_path": self.model_path,
            "error_type": self.error_type,
            "message": self.message,
        }
        if self.frame_path is not None:
            diagnostic["frame_path"] = self.frame_path
        return diagnostic


class RejectionReason(StrEnum):
    """Stable reason codes for detector diagnostics."""

    CLASS_NOT_ALLOWED = "class_not_allowed"
    CONFIDENCE_TOO_LOW = "confidence_too_low"
    AREA_TOO_SMALL = "area_too_small"
    CENTROID_OUTSIDE = "centroid_outside"
    OVERLAP_TOO_LOW = "overlap_too_low"


@dataclass(frozen=True)
class VehicleDetection:
    """Detector-neutral vehicle bbox emitted by a model adapter."""

    class_name: str
    confidence: float
    bbox: Sequence[float]

    def __post_init__(self) -> None:
        normalized_bbox = _normalize_bbox(self.bbox)
        normalized_confidence = float(self.confidence)
        if not math.isfinite(normalized_confidence) or not 0 <= normalized_confidence <= 1:
            raise ValueError("confidence must be finite and between 0 and 1")
        object.__setattr__(self, "bbox", normalized_bbox)
        object.__setattr__(self, "confidence", normalized_confidence)


@dataclass(frozen=True)
class SpotDetectionCandidate:
    """A detection accepted for a configured parking spot."""

    spot_id: str
    class_name: str
    confidence: float
    bbox: BBoxTuple
    bbox_area_px: float
    centroid: tuple[float, float]
    overlap_ratio: float
    source_frame_path: str | None = None
    source_timestamp: Any | None = None


@dataclass(frozen=True)
class RejectedDetection:
    """A detection rejected for one spot with a machine-readable reason."""

    spot_id: str
    detection: VehicleDetection
    reason: RejectionReason
    bbox_area_px: float | None = None
    centroid: tuple[float, float] | None = None
    overlap_ratio: float | None = None


@dataclass(frozen=True)
class SpotDetectionResult:
    """Accepted and rejected detections for one spot."""

    spot_id: str
    accepted: SpotDetectionCandidate | None
    rejected: list[RejectedDetection]


@dataclass(frozen=True)
class DetectionFilterResult:
    """Pure filtering output ready for later structured runtime logging."""

    by_spot: dict[str, SpotDetectionResult]
    rejection_counts: dict[RejectionReason, int]


@dataclass(frozen=True)
class _EvaluatedCandidate:
    candidate: SpotDetectionCandidate
    detection: VehicleDetection


class UltralyticsVehicleDetector:
    """Lazy Ultralytics YOLO adapter that emits detector-neutral vehicles."""

    def __init__(self, model_path: str, *, yolo_class: Callable[[str], Any] | None = None) -> None:
        self.model_path = str(model_path)
        try:
            yolo = yolo_class if yolo_class is not None else _load_ultralytics_yolo()
            self._model = yolo(self.model_path)
        except DetectionError:
            raise
        except Exception as exc:
            raise _detection_error_from_exception(
                exc,
                model_path=self.model_path,
                phase="model_load",
            ) from exc

    def detect(self, frame_path: str | PathLike[str], *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
        """Run one model prediction and normalize all result batches."""

        safe_frame_path = str(frame_path)
        predict_kwargs: dict[str, Any] = {"source": safe_frame_path, "verbose": False}
        if confidence_threshold is not None:
            predict_kwargs["conf"] = confidence_threshold

        try:
            results = self._model.predict(**predict_kwargs)
            return _normalize_ultralytics_results(results)
        except DetectionError:
            raise
        except Exception as exc:
            raise _detection_error_from_exception(
                exc,
                model_path=self.model_path,
                phase="predict",
                frame_path=safe_frame_path,
            ) from exc


def filter_spot_detections(
    detections: Iterable[VehicleDetection],
    *,
    spots: Mapping[str, PolygonInput],
    allowed_classes: Iterable[str],
    confidence_threshold: float,
    min_bbox_area_px: float,
    min_polygon_overlap_ratio: float,
    source_frame_path: str | None = None,
    source_timestamp: Any | None = None,
) -> DetectionFilterResult:
    """Filter detector-neutral bboxes into deterministic per-spot candidates.

    This function is intentionally pure: it performs no image/model I/O and emits
    no logs. Callers can log the returned accepted candidates and aggregate
    rejection counts without per-detection log spam.
    """

    allowed_class_set = set(allowed_classes)
    _validate_thresholds(
        confidence_threshold=confidence_threshold,
        min_bbox_area_px=min_bbox_area_px,
        min_polygon_overlap_ratio=min_polygon_overlap_ratio,
    )

    by_spot: dict[str, SpotDetectionResult] = {}
    rejection_counter: Counter[RejectionReason] = Counter()
    normalized_detections = list(detections)

    for spot_id, polygon in spots.items():
        candidates: list[_EvaluatedCandidate] = []
        rejected: list[RejectedDetection] = []

        for vehicle in normalized_detections:
            candidate_or_rejection = _evaluate_detection_for_spot(
                vehicle,
                spot_id=spot_id,
                polygon=polygon,
                allowed_classes=allowed_class_set,
                confidence_threshold=confidence_threshold,
                min_bbox_area_px=min_bbox_area_px,
                min_polygon_overlap_ratio=min_polygon_overlap_ratio,
                source_frame_path=source_frame_path,
                source_timestamp=source_timestamp,
            )
            if isinstance(candidate_or_rejection, RejectedDetection):
                rejected.append(candidate_or_rejection)
                rejection_counter[candidate_or_rejection.reason] += 1
            else:
                candidates.append(candidate_or_rejection)

        accepted: SpotDetectionCandidate | None = None
        if candidates:
            selected = sorted(candidates, key=_candidate_sort_key)[0]
            accepted = selected.candidate

        by_spot[spot_id] = SpotDetectionResult(spot_id=spot_id, accepted=accepted, rejected=rejected)

    return DetectionFilterResult(by_spot=by_spot, rejection_counts=dict(rejection_counter))


def _evaluate_detection_for_spot(
    detection: VehicleDetection,
    *,
    spot_id: str,
    polygon: PolygonInput,
    allowed_classes: set[str],
    confidence_threshold: float,
    min_bbox_area_px: float,
    min_polygon_overlap_ratio: float,
    source_frame_path: str | None = None,
    source_timestamp: Any | None = None,
) -> _EvaluatedCandidate | RejectedDetection:
    area = bbox_area(detection.bbox)
    membership_bbox = _lower_half_bbox(detection.bbox)
    centroid = bbox_centroid(membership_bbox)

    if detection.class_name not in allowed_classes:
        return RejectedDetection(spot_id=spot_id, detection=detection, reason=RejectionReason.CLASS_NOT_ALLOWED, bbox_area_px=area, centroid=centroid)
    if detection.confidence < confidence_threshold:
        return RejectedDetection(spot_id=spot_id, detection=detection, reason=RejectionReason.CONFIDENCE_TOO_LOW, bbox_area_px=area, centroid=centroid)
    if area < min_bbox_area_px:
        return RejectedDetection(spot_id=spot_id, detection=detection, reason=RejectionReason.AREA_TOO_SMALL, bbox_area_px=area, centroid=centroid)
    if not point_in_polygon(centroid, polygon):
        return RejectedDetection(spot_id=spot_id, detection=detection, reason=RejectionReason.CENTROID_OUTSIDE, bbox_area_px=area, centroid=centroid)

    overlap_ratio = bbox_polygon_overlap_ratio(membership_bbox, polygon)
    if overlap_ratio < min_polygon_overlap_ratio:
        return RejectedDetection(
            spot_id=spot_id,
            detection=detection,
            reason=RejectionReason.OVERLAP_TOO_LOW,
            bbox_area_px=area,
            centroid=centroid,
            overlap_ratio=overlap_ratio,
        )

    candidate = SpotDetectionCandidate(
        spot_id=spot_id,
        class_name=detection.class_name,
        confidence=detection.confidence,
        bbox=detection.bbox,
        bbox_area_px=area,
        centroid=centroid,
        overlap_ratio=overlap_ratio,
        source_frame_path=source_frame_path,
        source_timestamp=source_timestamp,
    )
    return _EvaluatedCandidate(candidate=candidate, detection=detection)


def _lower_half_bbox(bbox: BBoxTuple) -> BBoxTuple:
    x_min, y_min, x_max, y_max = bbox
    return (x_min, y_min + ((y_max - y_min) / 2.0), x_max, y_max)


def _load_ultralytics_yolo() -> Callable[[str], Any]:
    from ultralytics import YOLO

    return YOLO


def _normalize_ultralytics_results(results: Any) -> list[VehicleDetection]:
    detections: list[VehicleDetection] = []
    for result in _as_result_batches(results):
        boxes = getattr(result, "boxes", None)
        if boxes is None:
            continue
        xyxy = _plain_sequence(getattr(boxes, "xyxy"))
        conf = _plain_sequence(getattr(boxes, "conf"))
        cls = _plain_sequence(getattr(boxes, "cls"))
        names = getattr(result, "names", {})
        if not (len(xyxy) == len(conf) == len(cls)):
            raise ValueError("YOLO boxes xyxy/conf/cls lengths do not match")
        for bbox, confidence, class_id in zip(xyxy, conf, cls, strict=True):
            detections.append(
                VehicleDetection(
                    class_name=_class_name_for_id(names, class_id),
                    confidence=float(_scalar_value(confidence)),
                    bbox=tuple(float(_scalar_value(value)) for value in _plain_sequence(bbox)),
                )
            )
    return detections


def _as_result_batches(results: Any) -> list[Any]:
    if results is None:
        return []
    if isinstance(results, (list, tuple)):
        return list(results)
    return [results]


def _plain_sequence(value: Any) -> Any:
    value = _tensor_to_plain(value)
    if isinstance(value, tuple):
        return list(value)
    return value


def _tensor_to_plain(value: Any) -> Any:
    for method in ("detach", "cpu", "numpy"):
        if hasattr(value, method):
            value = getattr(value, method)()
    if hasattr(value, "tolist"):
        value = value.tolist()
    return value


def _scalar_value(value: Any) -> Any:
    value = _tensor_to_plain(value)
    if hasattr(value, "item"):
        return value.item()
    return value


def _class_name_for_id(names: Any, class_id: Any) -> str:
    numeric_class_id = int(float(_scalar_value(class_id)))
    if isinstance(names, Mapping):
        name = names.get(numeric_class_id, names.get(str(numeric_class_id)))
    elif isinstance(names, Sequence) and not isinstance(names, (str, bytes)):
        name = names[numeric_class_id] if 0 <= numeric_class_id < len(names) else None
    else:
        name = None
    return str(name) if name is not None else f"unknown_{numeric_class_id}"


def _detection_error_from_exception(
    exc: Exception,
    *,
    model_path: str,
    phase: str,
    frame_path: str | None = None,
) -> DetectionError:
    return DetectionError(
        str(exc) or exc.__class__.__name__,
        model_path=model_path,
        frame_path=frame_path,
        phase=phase,
        error_type=exc.__class__.__name__,
    )


def _sanitize_error_message(message: str | None) -> str:
    return redact_diagnostic_text(message)[:500]


def _candidate_sort_key(evaluated: _EvaluatedCandidate) -> tuple[float, float, BBoxTuple, str]:
    candidate = evaluated.candidate
    return (-candidate.confidence, -candidate.overlap_ratio, candidate.bbox, candidate.class_name)


def _normalize_bbox(bbox: Sequence[float]) -> BBoxTuple:
    # Reuse geometry validation so malformed detector output fails safely before
    # it can become an accepted candidate.
    bbox_area(bbox)
    return (float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))


def _validate_thresholds(
    *,
    confidence_threshold: float,
    min_bbox_area_px: float,
    min_polygon_overlap_ratio: float,
) -> None:
    if not 0 <= confidence_threshold <= 1:
        raise ValueError("confidence_threshold must be between 0 and 1")
    if min_bbox_area_px <= 0:
        raise ValueError("min_bbox_area_px must be greater than 0")
    if not 0 <= min_polygon_overlap_ratio <= 1:
        raise ValueError("min_polygon_overlap_ratio must be between 0 and 1")
