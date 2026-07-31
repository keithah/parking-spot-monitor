from __future__ import annotations

import tempfile
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any

from parking_spot_monitor.capture import FrameGeometry
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import (
    DetectionFilterResult,
    crop_region_for_polygon,
    filter_spot_detections,
    translate_crop_detection,
)
from parking_spot_monitor.detector_adapter import DetectorRunner
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.runtime_detection_support import (
    accepted_by_spot as _accepted_by_spot,
    candidate_summaries as _candidate_summaries,
    configured_spot_polygons as _configured_spot_polygons,
    frame_scale as _frame_scale,
    frame_size_dict as _frame_size_dict,
    image_size as _image_size,
    open_image_with_size as _open_image_with_size,
    scaled_min_bbox_area as _scaled_min_bbox_area,
    stringify_rejection_counts as _stringify_rejection_counts,
)


def _process_detection_for_capture(
    settings: RuntimeSettings,
    detector: DetectorRunner,
    latest_path: Path,
    *,
    frame_timestamp: Any | None = None,
    logger: StructuredLogger,
    mode: str,
    iteration: int | None = None,
    frame_geometry: FrameGeometry,
) -> DetectionFilterResult:
    if settings.detection.spot_crop_inference:
        crop_source, actual_frame_size = _open_image_with_size(latest_path)
    else:
        crop_source = None
        actual_frame_size = _image_size(latest_path)
    try:
        full_frame_detections = _detect_vehicles_for_frame(settings, detector, latest_path)
        spot_crop_detections = (
            _detect_spot_crop_vehicles_for_frame(settings, detector, latest_path, crop_source, actual_frame_size)
            if crop_source is not None and actual_frame_size is not None
            else []
        )
    finally:
        if crop_source is not None:
            crop_source.close()
    expected_size = frame_geometry.expected_size
    effective_frame_size = actual_frame_size or expected_size
    configured_frame_size = (settings.stream.frame_width, settings.stream.frame_height)
    frame_size_mismatch = effective_frame_size is not None and effective_frame_size != configured_frame_size
    scale = _frame_scale(configured_frame_size=configured_frame_size, actual_frame_size=effective_frame_size)
    spot_polygons = _configured_spot_polygons(settings, scale=scale)
    detections = [*full_frame_detections, *spot_crop_detections]
    result = replace(
        filter_spot_detections(
            detections,
            spots=spot_polygons,
            allowed_classes=settings.detection.vehicle_classes,
            confidence_threshold=settings.detection.confidence_threshold,
            min_bbox_area_px=_scaled_min_bbox_area(
                settings.detection.min_bbox_area_px, scale=scale
            ),
            min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
            source_frame_path=str(latest_path),
            source_timestamp=frame_timestamp,
        ),
        coordinate_scale=scale,
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
        "stream_profile": frame_geometry.stream_profile,
        "expected_frame_size": _frame_size_dict(expected_size),
        "effective_frame_size": _frame_size_dict(effective_frame_size),
        "configured_frame_size": _frame_size_dict(configured_frame_size),
        "frame_size_mismatch": frame_size_mismatch,
    }
    routine_runtime_frame = mode == "runtime-loop"
    detail_level = "DEBUG" if routine_runtime_frame else "INFO"
    if logger.is_enabled_for(detail_level):
        fields["candidate_summaries"] = _candidate_summaries(result)
    if iteration is not None:
        fields["iteration"] = iteration
    log_frame = logger.debug if routine_runtime_frame else logger.info
    log_frame("detection-frame-processed", **fields)
    return result


def _detect_spot_crop_vehicles_for_frame(
    settings: RuntimeSettings,
    detector: DetectorRunner,
    latest_path: Path,
    image: Any,
    actual_frame_size: tuple[int, int],
) -> list[Any]:
    configured_frame_size = (settings.stream.frame_width, settings.stream.frame_height)
    scale = _frame_scale(
        configured_frame_size=configured_frame_size,
        actual_frame_size=actual_frame_size,
    )
    spot_polygons = _configured_spot_polygons(settings, scale=scale)
    translated: list[Any] = []
    image_method_supported = True
    for spot_id, polygon in spot_polygons.items():
        region = crop_region_for_polygon(
            polygon,
            frame_size=actual_frame_size,
            margin_px=settings.detection.spot_crop_margin_px,
            spot_id=spot_id,
        )
        with closing(image.crop((region.left, region.top, region.right, region.bottom))) as crop:
            detections = detector.detect_image_if_supported(
                crop,
                confidence_threshold=_detector_confidence_threshold(settings),
                inference_image_size=settings.detection.inference_image_size,
            )
            if detections is None:
                image_method_supported = False
                break
            translated.extend(
                translate_crop_detection(detection, offset_x=region.left, offset_y=region.top)
                for detection in detections
            )
    if image_method_supported:
        return translated

    translated.clear()
    with tempfile.TemporaryDirectory(prefix="spot-crops-", dir=str(latest_path.parent)) as temp_dir:
        temp_root = Path(temp_dir)
        for spot_id, polygon in spot_polygons.items():
            region = crop_region_for_polygon(
                polygon,
                frame_size=actual_frame_size,
                margin_px=settings.detection.spot_crop_margin_px,
                spot_id=spot_id,
            )
            crop_path = temp_root / f"{spot_id}.jpg"
            with closing(image.crop((region.left, region.top, region.right, region.bottom))) as crop:
                crop.save(crop_path, format="JPEG")
            translated.extend(
                translate_crop_detection(detection, offset_x=region.left, offset_y=region.top)
                for detection in _detect_vehicles_for_frame(settings, detector, crop_path)
            )
    return translated


def _detect_vehicles_for_frame(settings: RuntimeSettings, detector: DetectorRunner, latest_path: Path) -> list[Any]:
    return detector.detect_path(
        latest_path,
        confidence_threshold=_detector_confidence_threshold(settings),
        inference_image_size=settings.detection.inference_image_size,
    )


def _detector_confidence_threshold(settings: RuntimeSettings) -> float:
    return min(
        settings.detection.confidence_threshold,
        settings.detection.open_suppression_min_confidence,
    )
