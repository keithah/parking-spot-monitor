from __future__ import annotations

from dataclasses import replace

from parking_spot_monitor.detection import SpotDetectionCandidate
from parking_spot_monitor.geometry import bbox_area


def candidate_in_configured_frame(
    candidate: SpotDetectionCandidate | None,
    *,
    scale: tuple[float, float],
) -> SpotDetectionCandidate | None:
    """Return occupancy evidence in the configured primary-frame coordinates."""

    if candidate is None or scale == (1.0, 1.0):
        return candidate
    scale_x, scale_y = scale
    if scale_x <= 0 or scale_y <= 0:
        return candidate
    x_min, y_min, x_max, y_max = candidate.bbox
    bbox = (
        x_min / scale_x,
        y_min / scale_y,
        x_max / scale_x,
        y_max / scale_y,
    )
    return replace(
        candidate,
        bbox=bbox,
        bbox_area_px=bbox_area(bbox),
        centroid=(
            candidate.centroid[0] / scale_x,
            candidate.centroid[1] / scale_y,
        ),
    )
