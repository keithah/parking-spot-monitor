from __future__ import annotations

from dataclasses import asdict

from parking_spot_monitor.vehicle_estimates import STATUS_INSUFFICIENT_HISTORY, VehicleHistoryEstimate
from parking_spot_monitor.vehicle_history_models import ProfileAssignment


def likely_vehicle_payload(assignment: ProfileAssignment | None, label: str | None) -> dict[str, object]:
    if assignment is None:
        return {
            "label": label or "unknown vehicle",
            "profile_id": None,
            "profile_confidence": None,
            "confidence": None,
            "match_status": None,
            "match_reason": None,
        }
    return {
        "label": label or assignment.profile_id or "unknown vehicle",
        "profile_id": assignment.profile_id,
        "profile_confidence": assignment.profile_confidence,
        "confidence": assignment.profile_confidence,
        "match_status": assignment.status,
        "match_reason": assignment.reason,
    }


def vehicle_history_estimate_payload(estimate: VehicleHistoryEstimate) -> dict[str, object]:
    return {
        "status": estimate.status,
        "reason": estimate.reason,
        "profile_id": estimate.profile_id,
        "sample_count": estimate.sample_count,
        "confidence": estimate.confidence,
        "dwell_range": None if estimate.dwell_range is None else asdict(estimate.dwell_range),
        "leave_time_window": None if estimate.leave_time_window is None else asdict(estimate.leave_time_window),
    }


def vehicle_history_estimate_error_payload() -> dict[str, object]:
    return {
        "status": STATUS_INSUFFICIENT_HISTORY,
        "reason": "estimate-error",
        "profile_id": None,
        "sample_count": 0,
        "confidence": "unknown",
        "dwell_range": None,
        "leave_time_window": None,
    }
