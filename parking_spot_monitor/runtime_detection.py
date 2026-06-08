from __future__ import annotations

import inspect
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from parking_spot_monitor.config import RuntimeSettings, SpotConfig
from parking_spot_monitor.detection import (
    DetectionError,
    DetectionFilterResult,
    SpotDetectionCandidate,
    crop_region_for_polygon,
    filter_spot_detections,
    translate_crop_detection,
)
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.runtime_decision_memory import _append_detection_memory_records


def _process_detection_for_capture(
    settings: RuntimeSettings,
    detector: Any,
    latest_path: Path,
    *,
    frame_timestamp: Any | None = None,
    logger: StructuredLogger,
    mode: str,
    iteration: int | None = None,
    decision_memory_path: Path | None = None,
) -> DetectionFilterResult:
    actual_frame_size = _image_size(latest_path)
    configured_frame_size = (settings.stream.frame_width, settings.stream.frame_height)
    frame_size_mismatch = actual_frame_size is not None and actual_frame_size != configured_frame_size
    scale = _frame_scale(configured_frame_size=configured_frame_size, actual_frame_size=actual_frame_size)
    spot_polygons = _configured_spot_polygons(settings, scale=scale)
    full_frame_detections = _detect_vehicles_for_frame(settings, detector, latest_path)
    spot_crop_detections = _detect_spot_crop_vehicles_for_frame(
        settings,
        detector,
        latest_path,
        spot_polygons=spot_polygons,
        actual_frame_size=actual_frame_size,
    )
    detections = [*full_frame_detections, *spot_crop_detections]
    result = filter_spot_detections(
        detections,
        spots=spot_polygons,
        allowed_classes=settings.detection.vehicle_classes,
        confidence_threshold=settings.detection.confidence_threshold,
        min_bbox_area_px=_scaled_min_bbox_area(settings.detection.min_bbox_area_px, scale=scale),
        min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
        source_frame_path=str(latest_path),
        source_timestamp=frame_timestamp,
    )
    fields: dict[str, Any] = {
        "mode": mode,
        "frame_path": str(latest_path),
        "spot_ids": list(result.by_spot.keys()),
        "detection_count": len(detections),
        "full_frame_detection_count": len(full_frame_detections),
        "spot_crop_inference_enabled": settings.detection.spot_crop_inference,
        "spot_crop_detection_count": len(spot_crop_detections),
        "accepted_count": sum(1 for spot in result.by_spot.values() if spot.accepted is not None),
        "accepted_by_spot": _accepted_by_spot(result),
        "rejection_counts": _stringify_rejection_counts(result),
        "thresholds": {
            "confidence_threshold": settings.detection.confidence_threshold,
            "min_bbox_area_px": _scaled_min_bbox_area(settings.detection.min_bbox_area_px, scale=scale),
            "configured_min_bbox_area_px": settings.detection.min_bbox_area_px,
            "min_polygon_overlap_ratio": settings.detection.min_polygon_overlap_ratio,
        },
        "actual_frame_size": _frame_size_dict(actual_frame_size),
        "configured_frame_size": _frame_size_dict(configured_frame_size),
        "frame_size_mismatch": frame_size_mismatch,
        "candidate_summaries": _candidate_summaries(result),
    }
    if iteration is not None:
        fields["iteration"] = iteration
    logger.info("detection-frame-processed", **fields)
    _append_detection_memory_records(
        decision_memory_path,
        result,
        observed_at=frame_timestamp,
        logger=logger,
        mode=mode,
        iteration=iteration,
    )
    return result

def _detect_spot_crop_vehicles_for_frame(
    settings: RuntimeSettings,
    detector: Any,
    latest_path: Path,
    *,
    spot_polygons: Mapping[str, Sequence[tuple[float, float]]],
    actual_frame_size: tuple[int, int] | None,
) -> list[Any]:
    if not settings.detection.spot_crop_inference or actual_frame_size is None:
        return []

    try:
        from PIL import Image

        translated: list[Any] = []
        with tempfile.TemporaryDirectory(prefix="spot-crops-", dir=str(latest_path.parent)) as temp_dir:
            temp_root = Path(temp_dir)
            with Image.open(latest_path) as image:
                for spot_id, polygon in spot_polygons.items():
                    region = crop_region_for_polygon(
                        polygon,
                        frame_size=actual_frame_size,
                        margin_px=settings.detection.spot_crop_margin_px,
                        spot_id=spot_id,
                    )
                    crop_path = temp_root / f"{spot_id}.jpg"
                    image.crop((region.left, region.top, region.right, region.bottom)).save(crop_path, format="JPEG")
                    translated.extend(
                        translate_crop_detection(detection, offset_x=region.left, offset_y=region.top)
                        for detection in _detect_vehicles_for_frame(settings, detector, crop_path)
                    )
        return translated
    except DetectionError:
        raise

def _detect_vehicles_for_frame(settings: RuntimeSettings, detector: Any, latest_path: Path) -> list[Any]:
    kwargs: dict[str, Any] = {
        "confidence_threshold": min(
            settings.detection.confidence_threshold,
            settings.detection.open_suppression_min_confidence,
        )
    }
    if _detect_accepts_inference_image_size(detector):
        kwargs["inference_image_size"] = settings.detection.inference_image_size
    return detector.detect(latest_path, **kwargs)

def _detect_accepts_inference_image_size(detector: Any) -> bool:
    detect = getattr(detector, "detect", None)
    try:
        signature = inspect.signature(detect)
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD or name == "inference_image_size"
        for name, parameter in signature.parameters.items()
    )

def _configured_spot_polygons(
    settings: RuntimeSettings, *, scale: tuple[float, float] = (1.0, 1.0)
) -> dict[str, list[tuple[float, float]]]:
    return {
        "left_spot": _spot_polygon(settings.spots.left_spot, scale=scale),
        "right_spot": _spot_polygon(settings.spots.right_spot, scale=scale),
    }

def _spot_polygon(spot: SpotConfig, *, scale: tuple[float, float] = (1.0, 1.0)) -> list[tuple[float, float]]:
    scale_x, scale_y = scale
    return [(point.x * scale_x, point.y * scale_y) for point in spot.polygon]

def _image_size(path: Path) -> tuple[int, int] | None:
    try:
        from PIL import Image

        with Image.open(path) as image:
            return image.size
    except Exception:
        return None

def _frame_scale(
    *, configured_frame_size: tuple[int, int], actual_frame_size: tuple[int, int] | None
) -> tuple[float, float]:
    if actual_frame_size is None:
        return (1.0, 1.0)
    configured_width, configured_height = configured_frame_size
    actual_width, actual_height = actual_frame_size
    if configured_width <= 0 or configured_height <= 0:
        return (1.0, 1.0)
    return (actual_width / configured_width, actual_height / configured_height)

def _scaled_min_bbox_area(value: float, *, scale: tuple[float, float]) -> float:
    return float(value) * scale[0] * scale[1]

def _frame_size_dict(size: tuple[int, int] | None) -> dict[str, int] | None:
    if size is None:
        return None
    return {"width": int(size[0]), "height": int(size[1])}

def _stringify_rejection_counts(result: DetectionFilterResult) -> dict[str, int]:
    return {str(reason): count for reason, count in result.rejection_counts.items()}

def _accepted_by_spot(result: DetectionFilterResult) -> dict[str, bool]:
    return {spot_id: spot.accepted is not None for spot_id, spot in result.by_spot.items()}

def _candidate_summaries(result: DetectionFilterResult) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for spot in result.by_spot.values():
        if spot.accepted is not None:
            summaries.append(_candidate_summary(spot.accepted))
    return summaries

def _candidate_summary(candidate: SpotDetectionCandidate) -> dict[str, Any]:
    return {
        "bbox": list(candidate.bbox),
        "bbox_area_px": candidate.bbox_area_px,
        "centroid": list(candidate.centroid),
        "class_name": candidate.class_name,
        "confidence": candidate.confidence,
        "overlap_ratio": candidate.overlap_ratio,
        "source_frame_path": candidate.source_frame_path,
        "source_timestamp": candidate.source_timestamp,
        "spot_id": candidate.spot_id,
    }
