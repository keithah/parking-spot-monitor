from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.operator_cockpit_shared import MAX_LINES_PER_SECTION, bounded_reply, int_value, mapping_value, text_value


def format_detection_lab_run_reply(
    *,
    data_dir: str | Path,
    kind: str,
    manager: Any | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Start a bounded local detection-lab job and return a text-only Matrix reply."""

    lab_manager = _detection_lab_manager(data_dir, manager=manager, logger=logger)
    try:
        if kind == "replay":
            job = lab_manager.start_replay()
        elif kind == "tuning":
            job = lab_manager.start_tuning()
        else:
            return bounded_reply(["Detection lab run unavailable", "Error: invalid_job_kind; use replay or tuning", "No detector, camera, shell, or live occupancy work was run by this reply path."])
    except Exception as exc:
        _log_lab_problem(logger, reason="start_failed", error_type=exc.__class__.__name__)
        return bounded_reply(["Detection lab run unavailable", f"Error: {redact_diagnostic_text(exc.__class__.__name__)}", "No detector, camera, shell, or live occupancy work was run by this reply path."])

    return bounded_reply([
        "Detection lab job started",
        f"Job: {job.job_id}",
        f"Kind: {job.kind}",
        "Status: queued or blocked; use !parking lab status latest for the persisted redacted status.",
        "Inputs: fixed local detection-lab files under the runtime data directory.",
    ])


def format_detection_lab_status_reply(
    *,
    data_dir: str | Path,
    job_id: str = "latest",
    manager: Any | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, redacted detection-lab job status from local artifacts."""

    lab_manager = _detection_lab_manager(data_dir, manager=manager, logger=logger)
    try:
        status = lab_manager.summarize(job_id or "latest")
    except Exception as exc:
        code = text_value(getattr(exc, "code", None), default=redact_diagnostic_text(exc.__class__.__name__))
        message = text_value(getattr(exc, "message", None) or str(exc), default="unavailable")
        _log_lab_problem(logger, reason="status_unavailable", error_type=exc.__class__.__name__, error_code=code)
        return bounded_reply([
            "Detection lab status unavailable",
            f"Lookup: {text_value(job_id or 'latest')}",
            f"Error: {code}; {message}",
            "No detector, camera, shell, or live occupancy work was run by this reply path.",
        ])

    return bounded_reply(_format_lab_status_lines(status))


def _log_lab_problem(logger: StructuredLogger | None, **fields: Any) -> None:
    if logger is None:
        return
    logger.warning("matrix-detection-lab-unavailable", **redact_diagnostic_value(fields))


def _detection_lab_manager(data_dir: str | Path, *, manager: Any | None, logger: StructuredLogger | None) -> Any:
    if manager is not None:
        return manager
    from parking_spot_monitor.detection_lab import DetectionLabManager

    return DetectionLabManager(data_dir, logger=logger)


def _format_lab_status_lines(status: Mapping[str, Any]) -> list[str]:
    lines = [
        "Detection lab status",
        f"Job: {text_value(status.get('job_id'))}",
        f"Kind: {text_value(status.get('kind'))}",
        f"Status: {text_value(status.get('status'))}; phase {text_value(status.get('phase'))}",
    ]
    if status.get("created_at") or status.get("updated_at"):
        lines.append(f"Timestamps: created {text_value(status.get('created_at'))}; updated {text_value(status.get('updated_at'))}")
    if status.get("report_path"):
        lines.append(f"Report: {text_value(status.get('report_path'))}")
    error = mapping_value(status.get("error"))
    if error:
        lines.append(f"Error: {text_value(error.get('code'))}; {text_value(error.get('message'))}")
    summary = mapping_value(status.get("summary"))
    if summary:
        lines.append("Summary:")
        for line in _format_lab_summary_lines(summary):
            lines.append(line)
    return lines


def _format_lab_summary_lines(summary: Mapping[str, Any]) -> list[str]:
    lines: list[str] = []
    status_counts = mapping_value(summary.get("status_counts"))
    if status_counts:
        counts = ", ".join(f"{text_value(key)}={int_value(value)}" for key, value in list(status_counts.items())[:8])
        lines.append(f"- status counts: {counts}")
    coverage = mapping_value(summary.get("coverage"))
    if coverage:
        lines.append(
            "- coverage: assessed "
            f"{int_value(coverage.get('assessed_frames'))}; blocked {int_value(coverage.get('blocked_frames'))}; "
            f"not assessed {int_value(coverage.get('not_assessed_frames'))}"
        )
    threshold = mapping_value(summary.get("shared_threshold_sufficiency"))
    if threshold:
        lines.append(f"- threshold: {text_value(threshold.get('verdict'))}; {text_value(threshold.get('rationale'), default='')}")
    if summary.get("decision"):
        lines.append(f"- decision: {text_value(summary.get('decision'))}; {text_value(summary.get('decision_rationale'), default='')}")
    deltas = mapping_value(summary.get("metric_delta_totals"))
    if deltas:
        rendered = ", ".join(f"{text_value(key)}={int_value(value)}" for key, value in list(deltas.items())[:8])
        lines.append(f"- metric deltas: {rendered}")
    redaction = mapping_value(summary.get("redaction"))
    if redaction:
        findings = redaction.get("findings")
        finding_count = len(findings) if isinstance(findings, list) else 0
        lines.append(f"- redaction: passed {str(redaction.get('passed') is True).lower()}; findings {finding_count}")
    if summary.get("missing_inputs"):
        missing = summary.get("missing_inputs")
        if isinstance(missing, list):
            lines.append("- missing fixed inputs: " + ", ".join(text_value(item) for item in missing[:8]))
    if not lines:
        lines.append("- no report summary available yet")
    return lines[:MAX_LINES_PER_SECTION]
