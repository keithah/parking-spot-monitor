from __future__ import annotations

from pathlib import Path
from typing import Any

from parking_spot_monitor.config import RuntimeSettings, SpotConfig
from parking_spot_monitor.detection import DetectionFilterResult, SpotDetectionCandidate


def configured_spot_polygons(
    settings: RuntimeSettings, *, scale: tuple[float, float] = (1.0, 1.0)
) -> dict[str, list[tuple[float, float]]]:
    return {
        "left_spot": _spot_polygon(settings.spots.left_spot, scale=scale),
        "right_spot": _spot_polygon(settings.spots.right_spot, scale=scale),
    }


def _spot_polygon(spot: SpotConfig, *, scale: tuple[float, float]) -> list[tuple[float, float]]:
    scale_x, scale_y = scale
    return [(point.x * scale_x, point.y * scale_y) for point in spot.polygon]


def image_size(path: Path) -> tuple[int, int] | None:
    try:
        from PIL import Image

        with Image.open(path) as image:
            return image.size
    except Exception:
        return None


def open_image_with_size(path: Path) -> tuple[Any | None, tuple[int, int] | None]:
    from PIL import Image

    try:
        image = Image.open(path)
    except Exception:
        return None, None
    try:
        return image, image.size
    except Exception:
        image.close()
        return None, None


def frame_scale(
    *, configured_frame_size: tuple[int, int], actual_frame_size: tuple[int, int] | None
) -> tuple[float, float]:
    if actual_frame_size is None:
        return (1.0, 1.0)
    configured_width, configured_height = configured_frame_size
    actual_width, actual_height = actual_frame_size
    if configured_width <= 0 or configured_height <= 0:
        return (1.0, 1.0)
    return (actual_width / configured_width, actual_height / configured_height)


def scaled_min_bbox_area(value: float, *, scale: tuple[float, float]) -> float:
    return float(value) * scale[0] * scale[1]


def frame_size_dict(size: tuple[int, int] | None) -> dict[str, int] | None:
    if size is None:
        return None
    return {"width": int(size[0]), "height": int(size[1])}


def stringify_rejection_counts(result: DetectionFilterResult) -> dict[str, int]:
    return {str(reason): count for reason, count in result.rejection_counts.items()}


def accepted_by_spot(result: DetectionFilterResult) -> dict[str, bool]:
    return {spot_id: spot.accepted is not None for spot_id, spot in result.by_spot.items()}


def candidate_summaries(result: DetectionFilterResult) -> list[dict[str, Any]]:
    return [
        _candidate_summary(spot.accepted)
        for spot in result.by_spot.values()
        if spot.accepted is not None
    ]


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
