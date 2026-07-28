from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from parking_spot_monitor.detection import DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import (
    DecisionMemoryRecord,
    append_decision_memory_record,
    make_decision_memory_record,
)
from parking_spot_monitor.state import RuntimeState


def build_detection_memory_records(
    result: DetectionFilterResult,
    *,
    observed_at: Any | None,
    mode: str,
    iteration: int | None,
) -> list[DecisionMemoryRecord]:
    from parking_spot_monitor.runtime_detection import _candidate_summary
    from parking_spot_monitor.runtime_presence import _best_rejected_detection

    records: list[DecisionMemoryRecord] = []
    for spot_id, spot_result in result.by_spot.items():
        accepted = spot_result.accepted
        rejected = list(spot_result.rejected)
        common_details: dict[str, Any] = {
            "mode": mode,
            "iteration": iteration,
            "rejected_count": len(rejected),
            "weak_evidence_count": len(rejected),
            "rejection_reasons": _rejection_reason_counts(rejected),
        }
        if accepted is not None:
            record = make_decision_memory_record(
                "accepted_evidence",
                spot_id=spot_id,
                observed_at=observed_at,
                summary=f"accepted {accepted.class_name} evidence confidence={accepted.confidence:.2f}",
                details=common_details | {"candidate": _candidate_summary(accepted)},
            )
        elif rejected:
            best = _best_rejected_detection(rejected)
            record = make_decision_memory_record(
                "rejected_evidence",
                spot_id=spot_id,
                observed_at=observed_at,
                summary="no accepted candidate; rejected vehicle-like evidence present",
                details=common_details | {"best_rejected": _rejected_summary(best) if best is not None else None},
            )
        else:
            record = make_decision_memory_record(
                "miss",
                spot_id=spot_id,
                observed_at=observed_at,
                summary="no vehicle evidence for configured spot",
                details=common_details,
            )
        records.append(record)
    return records


def build_runtime_state_memory_records(
    *,
    previous_state: RuntimeState,
    next_state: Mapping[str, Any],
    detection_result: DetectionFilterResult,
    quiet_status: Any,
    observed_at: Any,
    configured_spot_ids: Sequence[str],
    presence_by_spot: Mapping[str, bool],
) -> list[DecisionMemoryRecord]:
    records: list[DecisionMemoryRecord] = []
    for spot_id in configured_spot_ids:
        prior = previous_state.state_by_spot.get(spot_id)
        current = next_state.get(spot_id)
        spot_result = detection_result.by_spot.get(spot_id)
        accepted = None if spot_result is None else spot_result.accepted
        has_presence = bool(presence_by_spot.get(spot_id))
        kind = "accepted_evidence" if accepted is not None else "suppression" if has_presence else "miss"
        prior_status = None if prior is None else prior.status.value
        current_status = getattr(getattr(current, "status", None), "value", None)
        hit_streak = getattr(current, "hit_streak", None)
        miss_streak = getattr(current, "miss_streak", None)
        reason = "accepted-candidate" if accepted is not None else "weak-open-suppression" if has_presence else "no-presence-evidence"
        records.append(make_decision_memory_record(
            kind,
            spot_id=spot_id,
            observed_at=observed_at,
            summary=f"runtime state {prior_status or 'unknown'} -> {current_status or 'unknown'} ({reason})",
            details={
                "previous_status": prior_status,
                "new_status": current_status,
                "hit_streak": hit_streak,
                "miss_streak": miss_streak,
                "reason": reason,
                "quiet_window_active": bool(getattr(quiet_status, "active", False)),
                "suppressed_reason": getattr(quiet_status, "suppressed_reason", None),
            },
        ))
    return records

def _append_lab_outcome_memory(
    path: Path,
    status_payload: Mapping[str, Any],
    *,
    data_dir: Path,
    logger: StructuredLogger,
) -> None:
    job_id = status_payload.get("job_id")
    kind = status_payload.get("kind")
    status = status_payload.get("status")
    phase = status_payload.get("phase")
    summary_payload = status_payload.get("summary") if isinstance(status_payload.get("summary"), Mapping) else {}
    error_payload = status_payload.get("error") if isinstance(status_payload.get("error"), Mapping) else {}
    report_name = status_payload.get("report_path") if isinstance(status_payload.get("report_path"), str) else None
    report_path = None
    if isinstance(job_id, str) and report_name:
        report_path = str(Path("detection-lab") / "jobs" / job_id / report_name)
    details: dict[str, Any] = {
        "job_id": job_id,
        "kind": kind,
        "status": status,
        "phase": phase,
        "created_at": status_payload.get("created_at"),
        "updated_at": status_payload.get("updated_at"),
        "report_path": report_path,
        "status_counts": summary_payload.get("status_counts"),
        "coverage": summary_payload.get("coverage"),
        "decision": summary_payload.get("decision"),
        "metric_delta_totals": summary_payload.get("metric_delta_totals"),
        "error_code": error_payload.get("code"),
        "error_message": error_payload.get("message"),
    }
    if summary_payload.get("shared_threshold_sufficiency") is not None:
        details["shared_threshold_sufficiency"] = summary_payload.get("shared_threshold_sufficiency")
    if summary_payload.get("redaction") is not None:
        details["redaction"] = summary_payload.get("redaction")
    if summary_payload.get("missing_inputs") is not None:
        details["missing_inputs"] = summary_payload.get("missing_inputs")
    _append_decision_memory(
        path,
        "lab_outcome",
        spot_id=None,
        observed_at=status_payload.get("updated_at"),
        summary=f"detection lab {kind or 'unknown'} {status or 'unknown'}",
        details={key: value for key, value in details.items() if value is not None},
        logger=logger,
    )
    logger.info(
        "detection-lab-outcome-recorded",
        phase="detection-lab",
        job_id=job_id,
        kind=kind,
        status=status,
        lab_dir=str(data_dir / "detection-lab"),
    )

def _append_decision_memory(
    path: Path,
    kind: str,
    *,
    spot_id: str | None,
    observed_at: Any | None,
    summary: str,
    details: Mapping[str, Any],
    logger: StructuredLogger,
) -> None:
    append_decision_memory_record(
        path,
        make_decision_memory_record(kind, observed_at=observed_at, spot_id=spot_id, summary=summary, details=details),
        logger=logger,
    )

def _rejection_reason_counts(rejections: Sequence[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rejected in rejections:
        reason = str(getattr(rejected, "reason", "unknown"))
        counts[reason] = counts.get(reason, 0) + 1
    return counts

def _rejected_summary(rejected: Any) -> dict[str, Any]:
    return {
        "class_name": getattr(rejected.detection, "class_name", None),
        "confidence": getattr(rejected.detection, "confidence", None),
        "reason": str(getattr(rejected, "reason", "unknown")),
        "bbox_area_px": getattr(rejected, "bbox_area_px", None),
        "overlap_ratio": getattr(rejected, "overlap_ratio", None),
    }
