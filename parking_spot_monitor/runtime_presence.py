from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from parking_spot_monitor.detection import DetectionFilterResult, RejectionReason
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy import OccupancyStatus
from parking_spot_monitor.state import RuntimeState
from parking_spot_monitor.runtime_log_aggregation import RuntimeLogAggregator


def presence_by_spot(
    result: DetectionFilterResult,
    *,
    open_suppression_classes: Sequence[str] | None = None,
    min_polygon_overlap_ratio: float = 0.0,
) -> dict[str, bool]:
    """Return weak vehicle-presence evidence that suppresses open alerts.

    Accepted candidates confirm normal occupancy. Rejections that still prove a
    vehicle-like object is inside the spot should prevent release/open alerts,
    but they must not confirm a new occupied state on their own. Centroid-outside
    rejections are excluded so driveway cars and passing traffic outside the spot
    do not keep spots occupied forever.
    """

    suppression_classes = set(open_suppression_classes or ())
    presence: dict[str, bool] = {}
    for spot_id, spot_result in result.by_spot.items():
        presence[spot_id] = spot_result.accepted is not None or any(
            _rejection_suppresses_open(
                rejected,
                open_suppression_classes=suppression_classes,
                min_polygon_overlap_ratio=min_polygon_overlap_ratio,
            )
            for rejected in spot_result.rejected
        )
    return presence


def _rejection_suppresses_open(
    rejected: Any,
    *,
    open_suppression_classes: set[str],
    min_polygon_overlap_ratio: float,
) -> bool:
    suppressing_reasons = {
        RejectionReason.AREA_TOO_SMALL,
        RejectionReason.OVERLAP_TOO_LOW,
        RejectionReason.CONFIDENCE_TOO_LOW,
    }
    if rejected.reason is RejectionReason.CENTROID_OUTSIDE:
        return False
    if rejected.reason is RejectionReason.CLASS_NOT_ALLOWED and rejected.detection.class_name not in open_suppression_classes:
        return False
    if rejected.reason not in suppressing_reasons and rejected.reason is not RejectionReason.CLASS_NOT_ALLOWED:
        return False
    overlap = getattr(rejected, "overlap_ratio", None)
    return overlap is None or float(overlap) >= min_polygon_overlap_ratio


def _log_missed_occupied_spot_diagnostics(
    logger: StructuredLogger,
    *,
    runtime_state: RuntimeState,
    detection_result: DetectionFilterResult,
    open_suppression_classes: Sequence[str],
    min_polygon_overlap_ratio: float,
    log_aggregator: RuntimeLogAggregator | None = None,
) -> None:
    suppression_classes = set(open_suppression_classes)
    for spot_id, spot_result in detection_result.by_spot.items():
        prior = runtime_state.state_by_spot.get(spot_id)
        if prior is None or prior.status is not OccupancyStatus.OCCUPIED or spot_result.accepted is not None:
            continue
        best = _best_rejected_detection(spot_result.rejected)
        if best is None:
            if log_aggregator is not None:
                log_aggregator.record_diagnostic("no-rejection")
            log_diagnostic = logger.debug if log_aggregator is not None else logger.info
            log_diagnostic("spot-detection-miss-diagnostic", spot_id=spot_id, prior_status="occupied", best_rejected=None, suppressing_presence=False)
            continue
        if log_aggregator is not None:
            log_aggregator.record_diagnostic(f"{best.reason}:{best.detection.class_name}")
        log_diagnostic = logger.debug if log_aggregator is not None else logger.info
        log_diagnostic(
            "spot-detection-miss-diagnostic",
            spot_id=spot_id,
            prior_status="occupied",
            best_rejected={
                "class_name": best.detection.class_name,
                "confidence": best.detection.confidence,
                "reason": str(best.reason),
                "bbox_area_px": best.bbox_area_px,
                "centroid": list(best.centroid) if best.centroid is not None else None,
                "overlap_ratio": best.overlap_ratio,
            },
            suppressing_presence=_rejection_suppresses_open(
                best,
                open_suppression_classes=suppression_classes,
                min_polygon_overlap_ratio=min_polygon_overlap_ratio,
            ),
        )

def _best_rejected_detection(rejections: Sequence[Any]) -> Any | None:
    if not rejections:
        return None
    return sorted(
        rejections,
        key=lambda rejected: (
            float(getattr(rejected, "overlap_ratio", 0.0) or 0.0),
            float(getattr(rejected.detection, "confidence", 0.0) or 0.0),
            float(getattr(rejected, "bbox_area_px", 0.0) or 0.0),
        ),
        reverse=True,
    )[0]
