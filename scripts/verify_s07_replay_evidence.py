#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parking_spot_monitor.replay import ReplayReportError, scan_report_redactions

REPORT_JSON = "s07-evidence-report.json"
REPORT_MARKDOWN = "s07-evidence-report.md"
DEFAULT_REQUIRED_TAGS = ("real_capture", "bottom_driveway", "passing_traffic", "threshold_decision")
REAL_SEMANTIC_REQUIRED_TAGS = frozenset({"real_capture", "bottom_driveway", "passing_traffic"})
PASSING_TUNING_DECISIONS = {"keep_shared_thresholds", "apply_shared_tuning"}
NON_PASSING_TUNING_DECISIONS = {"needs_per_spot_thresholds"}
COUNTED_METRIC_KEYS = ("tp", "tn", "fp", "fn")
ERROR_METRIC_KEYS = ("fp", "fn")


class EvidenceVerifierError(Exception):
    """Safe verifier error that can still be rendered into the output report."""

    def __init__(self, code: str, message: str, *, path: Path | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.path = path

    def reason(self) -> str:
        if self.path is None:
            return f"{self.code}: {self.message}"
        return f"{self.code}: {self.message} ({self.path.name})"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify S07 replay/tuning evidence coverage without reading private labels.")
    parser.add_argument("--replay-report", required=True, help="Path to replay-report.json produced by replay_calibration_cases.py.")
    parser.add_argument(
        "--tuning-report",
        help="Optional path to tuning-report.json produced by compare_calibration_tuning.py. Missing tuning evidence blocks publication without preventing replay coverage reporting.",
    )
    parser.add_argument("--output-dir", required=True, help="Directory where S07 evidence JSON/Markdown reports are written.")
    parser.add_argument(
        "--required-tags",
        nargs="+",
        default=list(DEFAULT_REQUIRED_TAGS),
        help="Semantic tags required across replay case/scenario summaries.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    try:
        report = build_evidence_report(
            replay_report_path=Path(args.replay_report),
            tuning_report_path=Path(args.tuning_report) if args.tuning_report else None,
            required_tags=_normalize_tags(args.required_tags),
        )
        markdown = render_evidence_markdown(report)
        json_path, markdown_path = write_reports(output_dir, report, markdown)
    except OSError:
        _print_diagnostic(
            {"status": "error", "code": "OUTPUT_WRITE_FAILED", "message": "S07 evidence report files could not be written", "path": output_dir.name},
            stream=sys.stderr,
        )
        return 2
    except Exception:
        _print_diagnostic({"status": "error", "code": "INTERNAL_ERROR", "message": "unexpected S07 evidence verifier failure"}, stream=sys.stderr)
        return 1

    _print_diagnostic(
        {
            "status": report["status"],
            "outputs": {"json": str(json_path), "markdown": str(markdown_path)},
            "blocker_reasons": report["blocker_reasons"],
            "gap_reasons": report["gap_reasons"],
        },
        stream=sys.stdout,
    )
    return 0


def build_evidence_report(*, replay_report_path: Path, tuning_report_path: Path | None, required_tags: Sequence[str]) -> dict[str, Any]:
    blocker_reasons: list[str] = []
    gap_reasons: list[str] = []

    replay_report: dict[str, Any] = {}
    tuning_report: dict[str, Any] = {}
    try:
        replay_report = _load_json_report(replay_report_path, expected_name="replay report")
    except EvidenceVerifierError as exc:
        blocker_reasons.append(exc.reason())
    if tuning_report_path is None:
        blocker_reasons.append("tuning report not provided; threshold decision evidence is unavailable")
    else:
        try:
            tuning_report = _load_json_report(tuning_report_path, expected_name="tuning report")
        except EvidenceVerifierError as exc:
            blocker_reasons.append(exc.reason())

    replay_assessment = assess_replay_report(replay_report, required_tags=required_tags) if replay_report else _empty_replay_assessment(required_tags)
    tuning_assessment = assess_tuning_report(tuning_report) if tuning_report else _empty_tuning_assessment()

    blocker_reasons.extend(replay_assessment["blocker_reasons"])
    blocker_reasons.extend(tuning_assessment["blocker_reasons"])
    gap_reasons.extend(replay_assessment["gap_reasons"])
    gap_reasons.extend(tuning_assessment["gap_reasons"])

    report: dict[str, Any] = {
        "schema_version": "parking-spot-monitor.s07-evidence-report.v1",
        "status": _status_from_reasons(blocker_reasons, gap_reasons),
        "required_tags": replay_assessment["required_tags"],
        "tag_coverage": replay_assessment["tag_coverage"],
        "missing_tags": replay_assessment["missing_tags"],
        "per_spot_coverage": replay_assessment["per_spot_coverage"],
        "evidence_accounting": replay_assessment["evidence_accounting"],
        "replay_redaction": replay_assessment["redaction"],
        "blocked_cases": replay_assessment["blocked_cases"],
        "failed_cases": replay_assessment["failed_cases"],
        "tuning_decision": tuning_assessment["decision"],
        "tuning_status": tuning_assessment["status"],
        "threshold_verdict": tuning_assessment["threshold_verdict"],
        "blocker_reasons": _unique_sorted(blocker_reasons),
        "gap_reasons": _unique_sorted(gap_reasons),
        "source_files": {"replay_report": replay_report_path.name, "tuning_report": tuning_report_path.name if tuning_report_path is not None else None},
    }
    report["blockers"] = report["blocker_reasons"]
    report["findings"] = _unique_sorted([*report["blocker_reasons"], *report["gap_reasons"]])
    report["redaction_scan"] = scan_report_redactions(json.dumps(report, sort_keys=True, separators=(",", ":")))
    if not report["redaction_scan"]["passed"]:
        report["status"] = "blocked"
        report["blocker_reasons"] = _unique_sorted([*report["blocker_reasons"], "S07 report redaction scan found unsafe content"])
        report["blockers"] = report["blocker_reasons"]
        report["findings"] = _unique_sorted([*report["blocker_reasons"], *report["gap_reasons"]])
    return report


def assess_replay_report(report: Mapping[str, Any], *, required_tags: Sequence[str]) -> dict[str, Any]:
    blocker_reasons: list[str] = []
    gap_reasons: list[str] = []

    if not isinstance(report.get("cases"), list):
        blocker_reasons.append("replay report missing cases list")
    if not isinstance(report.get("metrics_by_spot"), Mapping):
        blocker_reasons.append("replay report missing metrics_by_spot object")

    redaction = _redaction_status(report.get("redaction_scan"))
    if not redaction["passed"]:
        blocker_reasons.append("replay report redaction scan did not pass")

    cases = report.get("cases", []) if isinstance(report.get("cases"), list) else []
    all_tags = _collect_replay_tags(cases)
    has_real_semantic_evidence = REAL_SEMANTIC_REQUIRED_TAGS.issubset(all_tags)
    tag_coverage = {tag: tag in all_tags for tag in required_tags}
    missing_tags = [tag for tag, present in tag_coverage.items() if not present]
    if missing_tags:
        missing_message = "missing required semantic tags: " + ", ".join(missing_tags)
        gap_reasons.append(missing_message)
        missing_real_semantic_tags = sorted(tag for tag in missing_tags if tag in REAL_SEMANTIC_REQUIRED_TAGS)
        if missing_real_semantic_tags:
            blocker_reasons.append(
                "missing real semantic evidence tags; strict S07 coverage cannot pass without: "
                + ", ".join(missing_real_semantic_tags)
            )

    blocked_cases = _case_summaries_by_status(cases, "blocked")
    failed_cases = _case_summaries_by_status(cases, "failed")
    if blocked_cases:
        blocker_reasons.append("replay report contains blocked cases")

    metrics_by_spot = report.get("metrics_by_spot", {}) if isinstance(report.get("metrics_by_spot"), Mapping) else {}
    configured_spots = _configured_spots(report, metrics_by_spot)
    if len(configured_spots) < 2:
        gap_reasons.append("replay report does not expose both configured spot ids")

    per_spot_coverage = {
        str(spot_id): _spot_coverage(metric, has_real_semantic_evidence=has_real_semantic_evidence)
        for spot_id, metric in sorted(metrics_by_spot.items())
        if isinstance(metric, Mapping)
    }
    missing_assessed_spots = [spot_id for spot_id in configured_spots if not per_spot_coverage.get(spot_id, {}).get("has_assessed_evidence", False)]
    if missing_assessed_spots:
        gap_reasons.append("spots missing assessed real evidence: " + ", ".join(missing_assessed_spots))

    missing_error_keys = [str(spot_id) for spot_id, metric in metrics_by_spot.items() if isinstance(metric, Mapping) and any(key not in metric for key in ERROR_METRIC_KEYS)]
    if missing_error_keys:
        gap_reasons.append("spots missing explicit FP/FN accounting: " + ", ".join(sorted(missing_error_keys)))

    return {
        "required_tags": list(required_tags),
        "tag_coverage": tag_coverage,
        "missing_tags": missing_tags,
        "per_spot_coverage": per_spot_coverage,
        "evidence_accounting": _evidence_accounting(metrics_by_spot),
        "redaction": redaction,
        "blocked_cases": blocked_cases,
        "failed_cases": failed_cases,
        "blocker_reasons": blocker_reasons,
        "gap_reasons": gap_reasons,
    }


def assess_tuning_report(report: Mapping[str, Any]) -> dict[str, Any]:
    blocker_reasons: list[str] = []
    decision = report.get("decision")
    if not isinstance(decision, str):
        blocker_reasons.append("tuning report missing decision")
        decision = "unknown"

    redaction = _redaction_status(report.get("redaction_scan"))
    if not redaction["passed"]:
        blocker_reasons.append("tuning report redaction scan did not pass")

    blocker_reasons.extend(_tuning_safety_reasons(report))
    if decision in NON_PASSING_TUNING_DECISIONS:
        blocker_reasons.append("tuning decision requires per-spot thresholds before S07 evidence can pass")
    elif decision not in PASSING_TUNING_DECISIONS:
        blocker_reasons.append(f"unsupported tuning decision: {decision}")

    return {
        "decision": decision,
        "status": "safe" if not blocker_reasons else "unsafe",
        "threshold_verdict": _threshold_verdict(decision),
        "blocker_reasons": blocker_reasons,
        "gap_reasons": [],
    }


def render_evidence_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# S07 Replay/Tuning Evidence Report",
        "",
        "## Status",
        f"- Status: **{report.get('status', 'blocked')}**",
        f"- Threshold verdict: **{report.get('threshold_verdict', 'unknown')}**",
        f"- Tuning decision: `{report.get('tuning_decision', 'unknown')}`",
        f"- Tuning status: `{report.get('tuning_status', 'unknown')}`",
        "",
        "## Required Semantic Tags",
    ]
    tag_coverage = report.get("tag_coverage", {}) if isinstance(report.get("tag_coverage"), Mapping) else {}
    for tag in report.get("required_tags", []) or []:
        lines.append(f"- `{tag}`: {'present' if tag_coverage.get(tag) else 'missing'}")

    accounting = report.get("evidence_accounting", {}) if isinstance(report.get("evidence_accounting"), Mapping) else {}
    totals = accounting.get("totals", {}) if isinstance(accounting.get("totals"), Mapping) else {}
    lines.extend(
        [
            "",
            "## Evidence Accounting",
            f"- True positives: {totals.get('tp', 0)}",
            f"- True negatives: {totals.get('tn', 0)}",
            f"- False positives: {totals.get('fp', 0)}",
            f"- False negatives: {totals.get('fn', 0)}",
            "",
            "## Per-Spot Coverage",
            "| Spot | Assessed | Counted observations | TP | TN | FP | FN | Blocked | Not covered |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    per_spot = report.get("per_spot_coverage", {}) if isinstance(report.get("per_spot_coverage"), Mapping) else {}
    for spot_id, coverage in sorted(per_spot.items()):
        if not isinstance(coverage, Mapping):
            continue
        lines.append(
            f"| `{spot_id}` | {coverage.get('has_assessed_evidence', False)} | {coverage.get('counted_observations', 0)} | {coverage.get('tp', 0)} | {coverage.get('tn', 0)} | {coverage.get('fp', 0)} | {coverage.get('fn', 0)} | {coverage.get('blocked', 0)} | {coverage.get('not_assessed', 0)} |"
        )

    replay_redaction = report.get("replay_redaction") if isinstance(report.get("replay_redaction"), Mapping) else {}
    lines.extend(
        [
            "",
            "## Replay Safety",
            f"- Redaction passed: {replay_redaction.get('passed', False)}",
            f"- Blocked cases: {_case_list(report.get('blocked_cases', []))}",
            f"- Failed cases: {_case_list(report.get('failed_cases', []))}",
            "",
            "## Reasons",
            f"- Blockers: {_comma_list(report.get('blocker_reasons', []))}",
            f"- Coverage gaps: {_comma_list(report.get('gap_reasons', []))}",
            f"- Missing tags: {_comma_list(report.get('missing_tags', []))}",
        ]
    )
    markdown = "\n".join(lines) + "\n"
    scan = scan_report_redactions(markdown)
    if not scan["passed"]:
        raise ReplayReportError("rendered S07 evidence Markdown contains unsafe content", path=",".join(scan.get("findings", [])))
    return markdown


def write_reports(output_dir: Path, report: Mapping[str, Any], markdown: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / REPORT_JSON
    markdown_path = output_dir / REPORT_MARKDOWN
    json_tmp = output_dir / f".{REPORT_JSON}.tmp"
    markdown_tmp = output_dir / f".{REPORT_MARKDOWN}.tmp"
    try:
        json_tmp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_tmp.write_text(markdown, encoding="utf-8")
        json_tmp.replace(json_path)
        markdown_tmp.replace(markdown_path)
    finally:
        for tmp in (json_tmp, markdown_tmp):
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
    return json_path, markdown_path


def _load_json_report(path: Path, *, expected_name: str) -> dict[str, Any]:
    if not path.is_file():
        raise EvidenceVerifierError("REPORT_NOT_FOUND", f"{expected_name} could not be read", path=path)
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise EvidenceVerifierError("REPORT_INVALID_JSON", f"{expected_name} is not valid JSON", path=path) from exc
    except OSError as exc:
        raise EvidenceVerifierError("REPORT_NOT_FOUND", f"{expected_name} could not be read", path=path) from exc
    if not isinstance(loaded, dict):
        raise EvidenceVerifierError("REPORT_MALFORMED", f"{expected_name} root must be an object", path=path)
    return loaded


def _empty_replay_assessment(required_tags: Sequence[str]) -> dict[str, Any]:
    return {
        "required_tags": list(required_tags),
        "tag_coverage": {tag: False for tag in required_tags},
        "missing_tags": list(required_tags),
        "per_spot_coverage": {},
        "evidence_accounting": {"totals": {key: 0 for key in (*COUNTED_METRIC_KEYS, "blocked", "not_assessed")}, "by_spot": {}},
        "redaction": {"passed": False, "findings": [], "reason": "missing_replay_report"},
        "blocked_cases": [],
        "failed_cases": [],
        "blocker_reasons": [],
        "gap_reasons": [],
    }


def _empty_tuning_assessment() -> dict[str, Any]:
    return {"decision": "unknown", "status": "unsafe", "threshold_verdict": "blocked", "blocker_reasons": [], "gap_reasons": []}


def _redaction_status(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {"passed": False, "findings": ["missing_redaction_scan"], "reason": "missing_redaction_scan"}
    return {
        "passed": value.get("passed") is True,
        "findings": sorted(str(finding) for finding in value.get("findings", []) or []),
        "reason": str(value.get("reason", "unknown")),
    }


def _collect_replay_tags(cases: Sequence[Any]) -> set[str]:
    tags: set[str] = set()
    for case in cases:
        if not isinstance(case, Mapping):
            continue
        tags.update(_normalize_tags(case.get("tags", [])))
        scenario_tags = case.get("scenario_tags", {})
        if isinstance(scenario_tags, Mapping):
            for values in scenario_tags.values():
                tags.update(_normalize_tags(values if isinstance(values, Sequence) and not isinstance(values, str) else []))
    return tags


def _normalize_tags(tags: Sequence[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in tags:
        if not isinstance(item, str):
            continue
        tag = item.strip().lower().replace(" ", "_")
        if tag and tag not in seen:
            normalized.append(tag)
            seen.add(tag)
    return normalized


def _case_summaries_by_status(cases: Sequence[Any], status: str) -> list[dict[str, Any]]:
    summaries = []
    for index, case in enumerate(cases):
        if not isinstance(case, Mapping) or case.get("status") != status:
            continue
        summaries.append(
            {
                "case_id": str(case.get("case_id", f"case-{index}")),
                "reasons": sorted(str(reason) for reason in case.get("blocked_reasons", []) or case.get("not_covered_reasons", []) or []),
            }
        )
    return summaries


def _configured_spots(report: Mapping[str, Any], metrics_by_spot: Mapping[str, Any]) -> list[str]:
    spot_ids = report.get("spot_ids")
    if isinstance(spot_ids, list) and all(isinstance(item, str) for item in spot_ids):
        return sorted(spot_ids)
    return sorted(str(spot_id) for spot_id in metrics_by_spot)


def _spot_coverage(metric: Mapping[str, Any], *, has_real_semantic_evidence: bool) -> dict[str, Any]:
    values = {key: _safe_int(metric.get(key, 0)) for key in (*COUNTED_METRIC_KEYS, "blocked", "not_assessed")}
    counted = sum(values[key] for key in COUNTED_METRIC_KEYS)
    return {**values, "counted_observations": counted, "has_assessed_evidence": has_real_semantic_evidence and counted > 0}


def _evidence_accounting(metrics_by_spot: Mapping[str, Any]) -> dict[str, Any]:
    by_spot: dict[str, dict[str, int]] = {}
    totals = {key: 0 for key in (*COUNTED_METRIC_KEYS, "blocked", "not_assessed")}
    for spot_id, metric in sorted(metrics_by_spot.items()):
        if not isinstance(metric, Mapping):
            continue
        values = {key: _safe_int(metric.get(key, 0)) for key in totals}
        by_spot[str(spot_id)] = values
        for key, value in values.items():
            totals[key] += value
    return {"totals": totals, "by_spot": by_spot}


def _tuning_safety_reasons(report: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if report.get("blocked_reasons"):
        reasons.append("tuning report contains blocked reasons")
    if report.get("not_covered_reasons"):
        reasons.append("tuning report contains not-covered reasons")
    status_counts = report.get("status_counts", {})
    if isinstance(status_counts, Mapping):
        for side, counts in status_counts.items():
            if not isinstance(counts, Mapping):
                continue
            if _safe_int(counts.get("blocked", 0)) > 0:
                reasons.append(f"tuning {side} evidence contains blocked cases")
            if _safe_int(counts.get("not_covered", 0)) > 0:
                reasons.append(f"tuning {side} evidence contains not-covered cases")
    return reasons


def _threshold_verdict(decision: str) -> str:
    if decision == "keep_shared_thresholds":
        return "shared_thresholds_sufficient"
    if decision == "apply_shared_tuning":
        return "shared_tuning_supported"
    if decision == "needs_per_spot_thresholds":
        return "per_spot_thresholds_required"
    return "blocked"


def _status_from_reasons(blocker_reasons: Sequence[str], gap_reasons: Sequence[str]) -> str:
    if blocker_reasons:
        return "blocked"
    if gap_reasons:
        return "coverage_gap"
    return "passed"


def _safe_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _unique_sorted(values: Sequence[str]) -> list[str]:
    return sorted(set(str(value) for value in values if value))


def _comma_list(values: Any) -> str:
    if not values:
        return "None"
    if isinstance(values, str):
        return values
    return ", ".join(f"`{value}`" for value in values)


def _case_list(values: Any) -> str:
    if not values:
        return "None"
    if not isinstance(values, Sequence) or isinstance(values, str):
        return "Unknown"
    ids = [str(value.get("case_id", "unknown")) for value in values if isinstance(value, Mapping)]
    return _comma_list(ids)


def _print_diagnostic(payload: Mapping[str, Any], *, stream: Any) -> None:
    print(json.dumps(payload, sort_keys=True), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
