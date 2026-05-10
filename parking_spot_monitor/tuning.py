from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Mapping, Sequence

from parking_spot_monitor.replay import (
    LabelManifest,
    ReplayEvaluationConfig,
    ReplayReportError,
    build_replay_report,
    evaluate_manifest,
    scan_report_redactions,
)


TUNING_REPORT_SCHEMA_VERSION = "parking-spot-monitor.tuning-report.v1"
_COUNTED_METRIC_KEYS = ("tp", "tn", "fp", "fn", "blocked", "not_assessed")
_ERROR_METRIC_KEYS = ("fp", "fn")


class TuningDecision(StrEnum):
    KEEP_SHARED_THRESHOLDS = "keep_shared_thresholds"
    APPLY_SHARED_TUNING = "apply_shared_tuning"
    NEEDS_PER_SPOT_THRESHOLDS = "needs_per_spot_thresholds"
    BLOCKED = "blocked"


def build_tuning_comparison_report(
    manifest: LabelManifest | Mapping[str, Any],
    *,
    baseline_config: ReplayEvaluationConfig | Mapping[str, Any],
    proposed_config: ReplayEvaluationConfig | Mapping[str, Any],
    created_at: str | None = None,
) -> dict[str, Any]:
    """Compare baseline/proposed replay configs against the same manifest.

    The builder is intentionally pure: callers provide an already parsed (or
    parseable) label manifest and two replay configs, and the function delegates
    all replay semantics to ``evaluate_manifest``. The returned object is
    deterministic and JSON-serializable so CLI/reporting layers can persist it
    directly as ``tuning-report.json``.
    """

    parsed_manifest = manifest if isinstance(manifest, LabelManifest) else LabelManifest.model_validate(manifest)
    baseline_result = evaluate_manifest(parsed_manifest, baseline_config)
    proposed_result = evaluate_manifest(parsed_manifest, proposed_config)
    baseline_report = build_replay_report(baseline_result, created_at=created_at)
    proposed_report = build_replay_report(proposed_result, created_at=created_at)

    metric_deltas = _metric_deltas(baseline_report.get("metrics_by_spot", {}), proposed_report.get("metrics_by_spot", {}))
    event_deltas = _event_deltas(baseline_report.get("event_findings", []), proposed_report.get("event_findings", []))
    blocked_reasons = _combined_reasons(baseline_report, proposed_report, "blocked_reasons")
    not_covered_reasons = _combined_reasons(baseline_report, proposed_report, "not_covered_reasons")
    status_counts = {"baseline": baseline_report.get("status_counts", {}), "proposed": proposed_report.get("status_counts", {})}

    report: dict[str, Any] = {
        "schema_version": TUNING_REPORT_SCHEMA_VERSION,
        "created_at": created_at,
        "case_ids": [case.case_id for case in parsed_manifest.cases],
        "baseline": _comparison_side(baseline_report),
        "proposed": _comparison_side(proposed_report),
        "baseline_thresholds": baseline_report.get("config_thresholds", {}),
        "proposed_thresholds": proposed_report.get("config_thresholds", {}),
        "metric_deltas": metric_deltas,
        "event_deltas": event_deltas,
        "status_counts": status_counts,
        "blocked_reasons": blocked_reasons,
        "not_covered_reasons": not_covered_reasons,
    }
    pre_decision_scan = _combined_redaction_scan(report, baseline_report, proposed_report)
    decision, rationale = _decide(
        metric_deltas=metric_deltas,
        proposed_report=proposed_report,
        status_counts=status_counts,
        blocked_reasons=blocked_reasons,
        not_covered_reasons=not_covered_reasons,
        redaction_scan=pre_decision_scan,
    )
    report["decision"] = decision.value
    report["decision_rationale"] = rationale
    report["redaction_scan"] = _combined_redaction_scan(report, baseline_report, proposed_report)
    if report["redaction_scan"]["passed"] is False:
        report["decision"] = TuningDecision.BLOCKED.value
        report["decision_rationale"] = "redaction findings prevent safe publication of tuning evidence"
    return _json_round_trip(report)


def render_tuning_report_markdown(report: Mapping[str, Any]) -> str:
    """Render a publication-safe Markdown tuning report from report data."""

    jsonable = _json_round_trip(report)
    redaction = jsonable.get("redaction_scan", {}) or {}
    if redaction.get("passed") is False:
        raise ReplayReportError("rendered Markdown contains unsafe content", path=",".join(redaction.get("findings", [])))

    metric_totals = jsonable.get("metric_deltas", {}).get("totals", {})
    event_deltas = jsonable.get("event_deltas", {}) or {}
    lines = [
        "# Tuning Comparison Report",
        "",
        "## Decision",
        f"- Decision: **{jsonable.get('decision', 'unknown')}**",
        f"- Rationale: {jsonable.get('decision_rationale', 'no rationale')}",
        f"- Schema version: `{jsonable.get('schema_version', 'unknown')}`",
        f"- Case IDs: {_comma_list(jsonable.get('case_ids', []))}",
        "",
        "## Thresholds Compared",
        f"- Baseline: `{_inline_json(jsonable.get('baseline_thresholds', {}))}`",
        f"- Proposed: `{_inline_json(jsonable.get('proposed_thresholds', {}))}`",
        "",
        "## Metric Deltas",
        "Deltas are proposed minus baseline; negative FP/FN values are improvements.",
        f"- True positives: {metric_totals.get('tp', 0)}",
        f"- True negatives: {metric_totals.get('tn', 0)}",
        f"- False positives: {metric_totals.get('fp', 0)}",
        f"- False negatives: {metric_totals.get('fn', 0)}",
        "",
        "| Spot | TP | TN | FP | FN | Blocked | Not covered |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for spot_id, metric in sorted((jsonable.get("metric_deltas", {}).get("by_spot", {}) or {}).items()):
        lines.append(
            f"| `{spot_id}` | {metric.get('tp', 0)} | {metric.get('tn', 0)} | {metric.get('fp', 0)} | {metric.get('fn', 0)} | {metric.get('blocked', 0)} | {metric.get('not_assessed', 0)} |"
        )

    lines.extend(
        [
            "",
            "## Event Deltas",
            f"- Baseline findings: {event_deltas.get('baseline_count', 0)}",
            f"- Proposed findings: {event_deltas.get('proposed_count', 0)}",
            f"- Added findings: {len(event_deltas.get('added', []))}",
            f"- Removed findings: {len(event_deltas.get('removed', []))}",
            "",
            "## Coverage and Safety",
            f"- Status counts: `{_inline_json(jsonable.get('status_counts', {}))}`",
            f"- Blocked reasons: {_comma_list(jsonable.get('blocked_reasons', []))}",
            f"- Not-covered reasons: {_comma_list(jsonable.get('not_covered_reasons', []))}",
            f"- Redaction passed: {redaction.get('passed', False)}",
            f"- Redaction findings: {_comma_list(redaction.get('findings', []))}",
        ]
    )
    markdown = "\n".join(lines) + "\n"
    final_scan = scan_report_redactions(markdown)
    if not final_scan["passed"]:
        raise ReplayReportError("rendered Markdown contains unsafe content", path=",".join(final_scan["findings"]))
    return markdown


def _comparison_side(replay_report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "config_thresholds": replay_report.get("config_thresholds", {}),
        "metrics_by_spot": replay_report.get("metrics_by_spot", {}),
        "coverage": replay_report.get("coverage", {}),
        "status_counts": replay_report.get("status_counts", {}),
        "shared_threshold_sufficiency": replay_report.get("shared_threshold_sufficiency", {}),
        "redaction_scan": replay_report.get("redaction_scan", {}),
    }


def _metric_deltas(baseline_metrics: Any, proposed_metrics: Any) -> dict[str, Any]:
    baseline = baseline_metrics if isinstance(baseline_metrics, Mapping) else {}
    proposed = proposed_metrics if isinstance(proposed_metrics, Mapping) else {}
    by_spot: dict[str, dict[str, int]] = {}
    totals = {key: 0 for key in _COUNTED_METRIC_KEYS}
    for spot_id in sorted(set(baseline) | set(proposed)):
        delta: dict[str, int] = {}
        for key in _COUNTED_METRIC_KEYS:
            value = int((proposed.get(spot_id, {}) or {}).get(key, 0)) - int((baseline.get(spot_id, {}) or {}).get(key, 0))
            delta[key] = value
            totals[key] += value
        by_spot[str(spot_id)] = delta
    return {"by_spot": by_spot, "totals": totals}


def _event_deltas(baseline_findings: Any, proposed_findings: Any) -> dict[str, Any]:
    baseline = [_event_identity(item) for item in baseline_findings if isinstance(item, Mapping) and _is_operator_event_finding(item)] if isinstance(baseline_findings, Sequence) and not isinstance(baseline_findings, str) else []
    proposed = [_event_identity(item) for item in proposed_findings if isinstance(item, Mapping) and _is_operator_event_finding(item)] if isinstance(proposed_findings, Sequence) and not isinstance(proposed_findings, str) else []
    baseline_keys = {_event_key(item) for item in baseline}
    proposed_keys = {_event_key(item) for item in proposed}
    return {
        "baseline_count": len(baseline),
        "proposed_count": len(proposed),
        "added": [item for item in proposed if _event_key(item) not in baseline_keys],
        "removed": [item for item in baseline if _event_key(item) not in proposed_keys],
    }


def _is_operator_event_finding(item: Mapping[str, Any]) -> bool:
    return str(item.get("event_type", "")).startswith("occupancy-open")


def _event_identity(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "case_id": item.get("case_id"),
        "scenario_id": item.get("scenario_id"),
        "frame_id": item.get("frame_id"),
        "spot_id": item.get("spot_id"),
        "event_type": item.get("event_type"),
        "finding": item.get("finding"),
    }


def _event_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    return (item.get("case_id"), item.get("scenario_id"), item.get("frame_id"), item.get("spot_id"), item.get("event_type"), item.get("finding"))


def _combined_reasons(baseline_report: Mapping[str, Any], proposed_report: Mapping[str, Any], key: str) -> list[str]:
    reasons: set[str] = set()
    for replay_report in (baseline_report, proposed_report):
        for case in replay_report.get("cases", []) or []:
            if isinstance(case, Mapping):
                reasons.update(str(reason) for reason in case.get(key, []) or [])
        coverage = replay_report.get("coverage", {}) or {}
        if isinstance(coverage, Mapping):
            reasons.update(str(reason) for reason in coverage.get(key, []) or [])
    return sorted(reasons)


def _combined_redaction_scan(report: Mapping[str, Any], baseline_report: Mapping[str, Any], proposed_report: Mapping[str, Any]) -> dict[str, Any]:
    findings: set[str] = set()
    for scan in (
        baseline_report.get("redaction_scan", {}) or {},
        proposed_report.get("redaction_scan", {}) or {},
        scan_report_redactions(json.dumps(report, sort_keys=True, separators=(",", ":"))),
    ):
        if isinstance(scan, Mapping):
            findings.update(str(finding) for finding in scan.get("findings", []) or [])
    return {
        "passed": not findings,
        "findings": sorted(findings),
        "reason": "no_forbidden_report_content" if not findings else "forbidden_report_content_detected",
    }


def _decide(
    *,
    metric_deltas: Mapping[str, Any],
    proposed_report: Mapping[str, Any],
    status_counts: Mapping[str, Any],
    blocked_reasons: Sequence[str],
    not_covered_reasons: Sequence[str],
    redaction_scan: Mapping[str, Any],
) -> tuple[TuningDecision, str]:
    if redaction_scan.get("passed") is False:
        return TuningDecision.BLOCKED, "redaction findings prevent safe publication of tuning evidence"
    if blocked_reasons or not_covered_reasons or _has_blocked_or_not_covered_counts(status_counts):
        return TuningDecision.BLOCKED, "blocked or not-covered replay evidence prevents a safe tuning decision"

    totals = metric_deltas.get("totals", {}) or {}
    fp_delta = int(totals.get("fp", 0))
    fn_delta = int(totals.get("fn", 0))
    blocked_delta = int(totals.get("blocked", 0))
    improved_error_count = fp_delta + fn_delta < 0
    no_new_safety_regressions = fp_delta <= 0 and fn_delta <= 0 and blocked_delta <= 0

    proposed_sufficiency = (proposed_report.get("shared_threshold_sufficiency", {}) or {}).get("verdict")
    if proposed_sufficiency == "insufficient" and no_new_safety_regressions and _has_spot_divergent_errors(proposed_report.get("metrics_by_spot", {})):
        return TuningDecision.NEEDS_PER_SPOT_THRESHOLDS, "residual false-positive/false-negative errors diverge by spot under shared proposed thresholds"

    if improved_error_count and no_new_safety_regressions and proposed_sufficiency == "sufficient":
        return TuningDecision.APPLY_SHARED_TUNING, "proposed shared thresholds reduce false-positive/false-negative evidence without new safety regressions"

    return TuningDecision.KEEP_SHARED_THRESHOLDS, "proposed shared thresholds do not improve false-positive/false-negative evidence"


def _has_blocked_or_not_covered_counts(status_counts: Mapping[str, Any]) -> bool:
    for side in ("baseline", "proposed"):
        counts = status_counts.get(side, {}) or {}
        if isinstance(counts, Mapping) and (int(counts.get("blocked", 0)) or int(counts.get("not_covered", 0))):
            return True
    return False


def _has_spot_divergent_errors(metrics: Any) -> bool:
    if not isinstance(metrics, Mapping):
        return False
    covered_spots = []
    error_spots = []
    for spot_id, metric in metrics.items():
        if not isinstance(metric, Mapping):
            continue
        counted = sum(int(metric.get(key, 0)) for key in ("tp", "tn", "fp", "fn"))
        errors = sum(int(metric.get(key, 0)) for key in _ERROR_METRIC_KEYS)
        if counted:
            covered_spots.append(spot_id)
        if errors:
            error_spots.append(spot_id)
    return bool(error_spots) and len(error_spots) < len(covered_spots)


def _json_round_trip(value: Any) -> dict[str, Any]:
    return json.loads(json.dumps(value, sort_keys=True))


def _inline_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _comma_list(values: Any) -> str:
    if not values:
        return "None"
    if isinstance(values, str):
        return values
    return ", ".join(f"`{value}`" for value in values)
