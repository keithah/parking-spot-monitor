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

from parking_spot_monitor.replay import scan_report_redactions
from scripts import verify_alert_soak, verify_live_proof

DEFAULT_REPLAY_REPORT = Path("data/s07-replay-evidence/replay/replay-report.json")
DEFAULT_TUNING_REPORT = Path("data/s07-replay-evidence/tuning/tuning-report.json")
DEFAULT_LIVE_PROOF_RESULT = Path("data/live-proof-result.json")
DEFAULT_ALERT_SOAK_RESULT = Path("data/alert-soak-result.json")
DEFAULT_S07_EVIDENCE_REPORT = Path("data/s07-replay-evidence/coverage/s07-evidence-report.json")
DEFAULT_OUTPUT_DIR = Path("data/m002-validation")
PACKAGE_JSON = "m002-validation-package.json"
PACKAGE_MARKDOWN = "m002-validation-package.md"
S10_ACQUISITION_CONTRACT = Path("data/s07-replay-evidence/evidence-notes.md")
PASS_STATUSES = {"passed"}
NON_STRICT_ACCEPTABLE_STATUSES = {"coverage_gap"}
FINAL_STATUS_ORDER = ("blocked", "failed", "coverage_gap")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assemble publication-safe M002 validation package from local evidence reports.")
    parser.add_argument("--replay-report", default=str(DEFAULT_REPLAY_REPORT), help="Path to replay-report.json from scripts/replay_calibration_cases.py.")
    parser.add_argument("--tuning-report", default=str(DEFAULT_TUNING_REPORT), help="Path to tuning-report.json from scripts/compare_calibration_tuning.py.")
    parser.add_argument("--live-proof-result", default=str(DEFAULT_LIVE_PROOF_RESULT), help="Path to live-proof-result.json.")
    parser.add_argument("--alert-soak-result", default=str(DEFAULT_ALERT_SOAK_RESULT), help="Path to alert-soak-result.json.")
    parser.add_argument("--s07-evidence-report", default=str(DEFAULT_S07_EVIDENCE_REPORT), help="Path to S07 replay/tuning evidence coverage report.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to write M002 validation package artifacts.")
    parser.add_argument(
        "--allow-residual-risk",
        default=None,
        help="Explicit acceptance wording for a non-strict closure such as an honest coverage gap.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    package = assemble_package(
        replay_report_path=Path(args.replay_report),
        tuning_report_path=Path(args.tuning_report),
        live_proof_result_path=Path(args.live_proof_result),
        alert_soak_result_path=Path(args.alert_soak_result),
        s07_evidence_report_path=Path(args.s07_evidence_report),
        allow_residual_risk=args.allow_residual_risk,
    )
    markdown = render_markdown(package)
    safety = scan_publication_safety(markdown)
    package["publication_safety"]["markdown"] = safety
    if safety["passed"] is not True:
        package["final_status"] = "failed"
        package["final_reason"] = "rendered validation package failed publication-safety scan"
        markdown = render_markdown(package)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / PACKAGE_JSON).write_text(json.dumps(package, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / PACKAGE_MARKDOWN).write_text(markdown, encoding="utf-8")
    return 0 if package["final_status"] != "failed" else 1


def load_json(path: Path) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not path.is_file():
        return None, evidence_result(
            artifact=str(path),
            status="blocked",
            reason="artifact path is missing",
            requirement_implications=["M002 validation blocked until the named artifact is generated"],
        )
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None, evidence_result(
            artifact=str(path),
            status="blocked",
            reason="artifact JSON is malformed",
            requirement_implications=["M002 validation blocked until artifact JSON can be parsed"],
        )
    except OSError:
        return None, evidence_result(
            artifact=str(path),
            status="blocked",
            reason="artifact path could not be read",
            requirement_implications=["M002 validation blocked until the named artifact is readable"],
        )
    if not isinstance(loaded, dict):
        return None, evidence_result(
            artifact=str(path),
            status="blocked",
            reason="artifact JSON root is not an object",
            requirement_implications=["M002 validation blocked until artifact JSON has the expected object shape"],
        )
    return loaded, None


def assemble_package(
    *,
    replay_report_path: Path,
    tuning_report_path: Path,
    live_proof_result_path: Path,
    alert_soak_result_path: Path,
    s07_evidence_report_path: Path,
    allow_residual_risk: str | None,
) -> dict[str, Any]:
    evidence_specs = {
        "replay": (replay_report_path, classify_replay),
        "tuning": (tuning_report_path, classify_tuning),
        "live_proof": (live_proof_result_path, classify_live_proof),
        "alert_soak": (alert_soak_result_path, classify_alert_soak),
        "s07_coverage": (s07_evidence_report_path, classify_s07_coverage),
    }
    evidence: dict[str, dict[str, Any]] = {}
    for name, (path, classifier) in evidence_specs.items():
        loaded, load_error = load_json(path)
        evidence[name] = load_error if load_error is not None else classifier(loaded or {}, artifact_label=str(path))

    final_status, final_reason = classify_final_status(evidence, allow_residual_risk=allow_residual_risk)
    requirement_implications = _unique(
        implication for item in evidence.values() for implication in item.get("requirement_implications", []) if isinstance(implication, str)
    )
    requirement_coverage = build_requirement_coverage(evidence)
    validation_boundaries = build_validation_boundaries(evidence)
    package = {
        "schema_version": "parking-spot-monitor.m002-validation-package.v1",
        "final_status": final_status,
        "final_reason": final_reason,
        "evidence": evidence,
        "requirement_implications": requirement_implications,
        "requirement_coverage": requirement_coverage,
        "validation_boundaries": validation_boundaries,
        "no_change_decision": _no_change_decision(evidence.get("tuning", {})),
        "residual_risk": allow_residual_risk if final_status == "residual_risk_accepted" else None,
        "publication_safety": {
            "package": scan_publication_safety(
                json.dumps(
                    _publication_scan_payload(evidence, requirement_implications, requirement_coverage, validation_boundaries),
                    sort_keys=True,
                )
            ),
        },
    }
    if package["publication_safety"]["package"]["passed"] is not True:
        package["final_status"] = "failed"
        package["final_reason"] = "validation package failed publication-safety scan"
    return package


def classify_final_status(evidence: Mapping[str, Mapping[str, Any]], *, allow_residual_risk: str | None) -> tuple[str, str]:
    statuses = {name: str(item.get("status", "unknown")) for name, item in evidence.items()}
    if all(status in PASS_STATUSES for status in statuses.values()):
        return "validated", "all required evidence surfaces passed strict validation"
    if allow_residual_risk and all(status in PASS_STATUSES | NON_STRICT_ACCEPTABLE_STATUSES for status in statuses.values()):
        return "residual_risk_accepted", "explicit residual-risk note accepted for non-strict closure"
    for status in FINAL_STATUS_ORDER:
        failed = [name for name, value in statuses.items() if value == status]
        if failed:
            return status, f"{status} evidence: {', '.join(failed)}"
    return "failed", "one or more evidence surfaces did not pass strict validation"


def classify_replay(report: Mapping[str, Any], *, artifact_label: str) -> dict[str, Any]:
    redaction_failure = _redaction_failure(report, "replay report redaction scan failed", artifact_label)
    if redaction_failure is not None:
        return redaction_failure
    counts = _counts(report.get("status_counts"))
    if counts.get("blocked", 0) > 0:
        return evidence_result(
            artifact=artifact_label,
            status="blocked",
            reason="replay evidence contains blocked cases",
            counts=counts,
            requirement_implications=["Replay calibration coverage is blocked by missing or invalid evidence"],
        )
    if counts.get("failed", 0) > 0:
        return evidence_result(
            artifact=artifact_label,
            status="failed",
            reason="replay evidence contains failed cases",
            counts=counts,
            requirement_implications=["Replay calibration evidence does not support M002 closure"],
        )
    verdict = _nested_str(report, "shared_threshold_sufficiency", "verdict")
    if counts.get("not_covered", 0) > 0 or verdict in {"not_covered", "inconclusive"}:
        return evidence_result(
            artifact=artifact_label,
            status="coverage_gap",
            reason="replay evidence is not fully covered",
            counts=counts,
            requirement_implications=["Replay evidence has coverage gaps that remain unvalidated"],
        )
    return evidence_result(
        artifact=artifact_label,
        status="passed",
        reason="replay evidence passed with clean redaction scan",
        counts=counts,
        requirement_implications=["Replay calibration evidence supports shared-threshold validation"],
    )


def classify_tuning(report: Mapping[str, Any], *, artifact_label: str) -> dict[str, Any]:
    redaction_failure = _redaction_failure(report, "tuning report redaction scan failed", artifact_label)
    if redaction_failure is not None:
        return redaction_failure
    decision = str(report.get("decision", "unknown"))
    counts = report.get("status_counts") if isinstance(report.get("status_counts"), dict) else {}
    if decision == "blocked":
        return evidence_result(
            artifact=artifact_label,
            status="blocked",
            reason="tuning comparison is blocked",
            decision=decision,
            counts=counts,
            requirement_implications=["Tuning decision blocked by replay evidence gaps"],
        )
    if decision == "needs_per_spot_thresholds":
        return evidence_result(
            artifact=artifact_label,
            status="blocked",
            reason="per-spot threshold follow-up required before M002 closure",
            decision=decision,
            counts=counts,
            requirement_implications=["R019 follow-up required"],
        )
    if decision in {"keep_shared_thresholds", "apply_shared_tuning"}:
        return evidence_result(
            artifact=artifact_label,
            status="passed",
            reason=f"tuning decision `{decision}` is acceptable for M002 closure",
            decision=decision,
            counts=counts,
            requirement_implications=["Shared-threshold tuning decision is explicit and publication-safe"],
        )
    return evidence_result(
        artifact=artifact_label,
        status="blocked",
        reason="tuning report decision is unsupported",
        decision=decision,
        counts=counts,
        requirement_implications=["M002 validation blocked until tuning decision is recognized"],
    )


def classify_live_proof(result: Mapping[str, Any], *, artifact_label: str) -> dict[str, Any]:
    try:
        normalized = verify_live_proof.normalize_result_contract(result)
        outcome = verify_live_proof.validate_result(normalized, allow_preflight_blocker=False, artifact_root=Path(artifact_label).parent)
    except verify_live_proof.VerificationError as exc:
        status = "blocked" if _is_live_preflight(result) else "failed"
        return evidence_result(
            artifact=artifact_label,
            status=status,
            reason=exc.public_reason,
            verifier_state="verifier_error",
            requirement_implications=["R003/R015 remain unvalidated"],
            publication_safety=_redaction_counts(result),
        )
    if outcome.accepted and outcome.state == "success":
        return evidence_result(
            artifact=artifact_label,
            status="passed",
            reason="live-proof verifier accepted strict success",
            verifier_state=outcome.state,
            requirement_implications=["R003/R015 validated"],
            publication_safety=_redaction_counts(normalized),
        )
    return evidence_result(
        artifact=artifact_label,
        status="blocked" if outcome.state == "preflight_failed" else "failed",
        reason=outcome.state,
        verifier_state=outcome.state,
        requirement_implications=["R003/R015 remain unvalidated"],
        publication_safety=_redaction_counts(normalized),
    )


def classify_alert_soak(result: Mapping[str, Any], *, artifact_label: str) -> dict[str, Any]:
    try:
        normalized = verify_alert_soak.normalize_result_contract(result)
        outcome = verify_alert_soak.validate_result(normalized, allow_coverage_gap=False, allow_preflight_blocker=False)
    except verify_alert_soak.VerificationError as exc:
        status = "blocked" if str(result.get("status")) == verify_alert_soak.PREFLIGHT_STATUS else "failed"
        evidence = evidence_result(
            artifact=artifact_label,
            status=status,
            reason=exc.public_reason,
            verifier_state="verifier_error",
            requirement_implications=["S08 strict live soak validation failed or incomplete"],
            publication_safety=_redaction_counts(result),
        )
        evidence["missing_inputs"] = _safe_str_list(result.get("missing_inputs"))
        return evidence
    if outcome.accepted and outcome.state == verify_alert_soak.SUCCESS_STATUS:
        evidence = evidence_result(
            artifact=artifact_label,
            status="passed",
            reason="alert-soak verifier accepted strict success",
            verifier_state=outcome.state,
            requirement_implications=["S08 strict live soak validation complete"],
            publication_safety=_redaction_counts(normalized),
        )
        evidence["missing_inputs"] = []
        evidence["verifier_findings"] = list(outcome.findings)
        return evidence
    if outcome.state == verify_alert_soak.COVERAGE_GAP_STATUS:
        evidence = evidence_result(
            artifact=artifact_label,
            status="coverage_gap",
            reason="no organic occupancy-open-event alerts were observed",
            verifier_state=outcome.state,
            requirement_implications=["S08 strict live soak validation remains incomplete"],
            publication_safety=_redaction_counts(normalized),
        )
        evidence["missing_inputs"] = []
        evidence["verifier_findings"] = list(outcome.findings)
        return evidence
    evidence = evidence_result(
        artifact=artifact_label,
        status="blocked" if outcome.state == "preflight_blocked" else "failed",
        reason=outcome.state,
        verifier_state=outcome.state,
        requirement_implications=["S08 strict live soak validation failed or incomplete"],
        publication_safety=_redaction_counts(normalized),
    )
    evidence["missing_inputs"] = _safe_str_list(normalized.get("missing_inputs"))
    evidence["verifier_findings"] = list(outcome.findings)
    return evidence



def classify_s07_coverage(report: Mapping[str, Any], *, artifact_label: str) -> dict[str, Any]:
    redaction_failure = _redaction_failure(report, "S07 evidence report redaction scan failed", artifact_label)
    if redaction_failure is not None:
        redaction_failure["requirement_implications"] = ["R018/R019/R028 remain unvalidated because S07 coverage report failed publication safety"]
        return redaction_failure

    raw_status = str(report.get("status", "unknown"))
    status = raw_status if raw_status in {"passed", "coverage_gap", "blocked", "failed"} else "blocked"
    blocker_reasons = _safe_str_list(report.get("blocker_reasons"))
    gap_reasons = _safe_str_list(report.get("gap_reasons"))
    missing_tags = _safe_str_list(report.get("missing_tags"))
    tag_coverage = report.get("tag_coverage") if isinstance(report.get("tag_coverage"), Mapping) else {}
    evidence_accounting = report.get("evidence_accounting") if isinstance(report.get("evidence_accounting"), Mapping) else {}
    reason = _s07_reason(status=status, blocker_reasons=blocker_reasons, gap_reasons=gap_reasons)
    implications = ["S07 replay/tuning evidence passed strict coverage"] if status == "passed" else ["S07 replay/tuning evidence does not strictly validate R018/R019/R028"]
    result = evidence_result(
        artifact=artifact_label,
        status=status,
        reason=reason,
        counts=_s07_counts(evidence_accounting),
        decision=str(report.get("tuning_decision", "unknown")),
        verifier_state=raw_status,
        requirement_implications=implications,
        publication_safety=_redaction_counts(report),
    )
    result.update(
        {
            "missing_tags": missing_tags,
            "tag_coverage": {str(key): value is True for key, value in tag_coverage.items()},
            "gap_reasons": gap_reasons,
            "blocker_reasons": blocker_reasons,
            "threshold_verdict": str(report.get("threshold_verdict", "unknown")),
            "tuning_status": str(report.get("tuning_status", "unknown")),
        }
    )
    return result


def build_validation_boundaries(evidence: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    s07 = evidence.get("s07_coverage", {})
    s07_status = str(s07.get("status", "blocked"))
    missing_tags = _safe_str_list(s07.get("missing_tags"))
    s10_blockers = _safe_str_list(s07.get("blocker_reasons")) or _safe_str_list(s07.get("gap_reasons"))
    if s07_status == "passed":
        s10_status = "strict_evidence_available"
        s10_summary = "S10 strict semantic replay evidence is available for R018/R019/R028."
    else:
        s10_status = "evidence_gap"
        s10_summary = "S10 strict semantic replay evidence is missing; R018/R019/R028 must remain non-validated until real captured labels are acquired."
    return {
        "s10_strict_replay_gap": {
            "status": s10_status,
            "summary": s10_summary,
            "blocked_requirements": ["R018", "R019", "R028"] if s07_status != "passed" else [],
            "missing_semantic_tags": missing_tags,
            "blocker_reasons": s10_blockers,
            "acquisition_contract": str(S10_ACQUISITION_CONTRACT),
            "required_acquisition": [
                "publication-safe real captured replay cases with detector-neutral bounding boxes",
                "explicit expected states for left_spot and right_spot",
                "captured-evidence-backed real_capture, bottom_driveway, passing_traffic, and threshold_decision tags",
            ],
        },
        "s11_live_proof_boundary": {
            "status": "separate_responsibility",
            "summary": "S11 remains responsible for unattended live proof and Matrix delivery evidence; S10 replay gaps must not be closed with S11 live-proof artifacts.",
            "not_a_substitute_for": ["R018", "R019", "R028"],
        },
        "s11_alert_soak_boundary": _build_s11_alert_soak_boundary(evidence.get("alert_soak", {})),
    }


def _build_s11_alert_soak_boundary(alert_soak: Mapping[str, Any]) -> dict[str, Any]:
    status = str(alert_soak.get("status", "blocked"))
    verifier_state = str(alert_soak.get("verifier_state") or status)
    missing_inputs = _safe_str_list(alert_soak.get("missing_inputs"))
    if status == "passed" and verifier_state == verify_alert_soak.SUCCESS_STATUS:
        return {
            "status": "strict_success",
            "summary": "S11 strict Matrix/RTSP alert-soak success is available from the alert-soak verifier.",
            "verifier_state": verifier_state,
            "blocked_requirements": [],
            "missing_inputs": [],
            "live_soak_implication": "Strict live alert-soak evidence can support S11 closure only; it is not a substitute for S10 semantic replay evidence.",
        }

    boundary_status = verifier_state if verifier_state != "verifier_error" else status
    reason = str(alert_soak.get("reason", "alert-soak evidence did not pass strict verification"))
    if verifier_state == "preflight_blocked":
        summary = "S11 alert-soak evidence is preflight-blocked and cannot validate strict Matrix/RTSP alert-soak success."
    elif status == "coverage_gap":
        summary = "S11 alert-soak evidence has no-alert coverage gaps and cannot validate strict Matrix/RTSP alert-soak success."
    else:
        summary = "S11 alert-soak evidence did not pass strict verification and cannot validate strict Matrix/RTSP alert-soak success."
    return {
        "status": boundary_status,
        "summary": summary,
        "verifier_state": verifier_state,
        "blocked_requirements": ["R003", "R015"],
        "missing_inputs": missing_inputs,
        "blocker_reason": reason,
        "live_soak_implication": "Current strict live alert-soak evidence is documentation-only until the alert-soak verifier accepts strict success.",
    }


def build_requirement_coverage(evidence: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    s07 = evidence.get("s07_coverage", {})
    tuning = evidence.get("tuning", {})
    s07_status = str(s07.get("status", "blocked"))
    missing_tags = _safe_str_list(s07.get("missing_tags"))
    tag_coverage = s07.get("tag_coverage") if isinstance(s07.get("tag_coverage"), Mapping) else {}
    s07_source = str(s07.get("artifact", DEFAULT_S07_EVIDENCE_REPORT))

    if s07_status == "passed":
        r018_status = "passed"
        r018_rationale = "S07 replay/tuning evidence report passed strict semantic and FP/FN coverage checks."
        r018_blocked = None
    elif s07_status == "coverage_gap":
        r018_status = "coverage_gap"
        r018_rationale = "S07 replay/tuning evidence report found semantic-tag or FP/FN accounting gaps."
        r018_blocked = _join_reasons(s07.get("gap_reasons"))
    elif s07_status in {"blocked", "failed"}:
        r018_status = s07_status
        r018_rationale = "S07 replay/tuning evidence report did not produce strict validated coverage."
        r018_blocked = _join_reasons(s07.get("blocker_reasons")) or str(s07.get("reason", "S07 coverage report unavailable"))
    else:
        r018_status = "blocked"
        r018_rationale = "S07 replay/tuning evidence report status is unsupported."
        r018_blocked = "unsupported S07 coverage status"

    tuning_decision = str(tuning.get("decision", "unknown"))
    tuning_acceptable = tuning_decision in {"keep_shared_thresholds", "apply_shared_tuning"} and tuning.get("status") == "passed"
    threshold_tag_present = tag_coverage.get("threshold_decision") is True
    if tuning_acceptable and s07_status == "passed" and threshold_tag_present:
        r019_status = "validated"
        r019_rationale = "Shared-threshold tuning decision is acceptable and S07 threshold-decision coverage passed strict validation."
        r019_blocked = None
    elif tuning.get("status") in {"blocked", "failed"} and not tuning_acceptable:
        r019_status = str(tuning.get("status"))
        r019_rationale = "Shared-threshold tuning decision is not acceptable for M002 closure."
        r019_blocked = str(tuning.get("reason", "tuning decision unavailable"))
    else:
        r019_status = "coverage_gap"
        r019_rationale = "Tuning decision is acceptable only at smoke level until strict S07 threshold-decision evidence passes."
        r019_blocked = _join_reasons(s07.get("gap_reasons")) or "strict S07 threshold-decision coverage is not passed"

    if s07_status == "passed" and tag_coverage.get("bottom_driveway") is True:
        r028_status = "passed"
        r028_rationale = "S07 strict semantic coverage includes bottom_driveway evidence."
        r028_blocked = None
    elif s07_status in {"blocked", "failed"}:
        r028_status = s07_status
        r028_rationale = "Bottom-driveway coverage cannot be validated because S07 coverage did not pass."
        r028_blocked = _join_reasons(s07.get("blocker_reasons")) or str(s07.get("reason", "S07 coverage report unavailable"))
    else:
        r028_status = "coverage_gap"
        r028_rationale = "M001 regression evidence is not sufficient for R028; S07 bottom_driveway semantic coverage is required."
        r028_blocked = "missing bottom_driveway semantic tag" if "bottom_driveway" in missing_tags or tag_coverage.get("bottom_driveway") is not True else _join_reasons(s07.get("gap_reasons"))

    return {
        "R018": _requirement_record(r018_status, s07_source, r018_rationale, missing_tags, r018_blocked),
        "R019": _requirement_record(r019_status, f"{tuning.get('artifact', 'tuning-report.json')} + {s07_source}", r019_rationale, missing_tags, r019_blocked),
        "R028": _requirement_record(r028_status, s07_source, r028_rationale, missing_tags, r028_blocked),
        "R020": _requirement_record("deferred", "M002 validation scope", "Setup documentation updates are deferred outside this M002 validation package.", [], "setup docs deferred"),
        "R021": _requirement_record("out_of_scope", "M002 validation scope", "Encrypted Matrix room support is unsupported and not claimed by M002 validation.", [], "encrypted Matrix unsupported"),
        "R022": _requirement_record("out_of_scope", "M002 validation scope", "Historical occupancy storage is outside the M002 validation scope.", [], "historical occupancy storage outside M002"),
    }


def _requirement_record(status: str, evidence_source: str, rationale: str, missing_tags: Sequence[str], blocked_or_deferred_reason: str | None) -> dict[str, Any]:
    return {
        "status": status,
        "evidence_source": evidence_source,
        "rationale": rationale,
        "missing_semantic_tags": list(missing_tags),
        "blocked_or_deferred_reason": blocked_or_deferred_reason,
    }


def _s07_reason(*, status: str, blocker_reasons: Sequence[str], gap_reasons: Sequence[str]) -> str:
    if status == "passed":
        return "S07 evidence report passed strict coverage"
    if status == "coverage_gap":
        return "S07 evidence report has coverage gaps: " + (_comma_list(gap_reasons) if gap_reasons else "unspecified coverage gap")
    if status == "blocked":
        return "S07 evidence report is blocked: " + (_comma_list(blocker_reasons) if blocker_reasons else "blocked coverage report")
    if status == "failed":
        return "S07 evidence report failed strict coverage"
    return "S07 evidence report status is unsupported"


def _s07_counts(evidence_accounting: Mapping[str, Any]) -> dict[str, int]:
    totals = evidence_accounting.get("totals") if isinstance(evidence_accounting.get("totals"), Mapping) else {}
    return {key: _int(totals.get(key)) for key in ("tp", "tn", "fp", "fn", "blocked", "not_assessed")}


def _safe_str_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        return []
    return [str(item) for item in value if isinstance(item, (str, int, float, bool))]


def _join_reasons(value: Any) -> str | None:
    reasons = _safe_str_list(value)
    return "; ".join(reasons) if reasons else None

def scan_publication_safety(text: str) -> dict[str, Any]:
    scan = scan_report_redactions(text)
    findings = scan.get("findings") if isinstance(scan.get("findings"), list) else []
    return {
        "passed": scan.get("passed") is True,
        "redaction_hits": len(findings),
        "findings": [str(item) for item in findings],
    }


def render_markdown(package: Mapping[str, Any]) -> str:
    lines = [
        "# M002 Validation Package",
        "",
        f"- Final status: `{package.get('final_status', 'unknown')}`",
        f"- Final reason: {package.get('final_reason', 'unknown')}",
        f"- Residual risk: {package.get('residual_risk') or 'none'}",
        f"- No-change/shared-threshold decision: `{package.get('no_change_decision') or 'none'}`",
        "",
        "## Evidence Statuses",
    ]
    evidence = package.get("evidence") if isinstance(package.get("evidence"), Mapping) else {}
    for name, item in evidence.items():
        if not isinstance(item, Mapping):
            continue
        lines.extend(
            [
                f"### {name}",
                f"- Artifact: `{item.get('artifact', 'unknown')}`",
                f"- Status: `{item.get('status', 'unknown')}`",
                f"- Reason: {item.get('reason', 'unknown')}",
                f"- Verifier state: `{item.get('verifier_state', 'not_applicable')}`",
                f"- Decision: `{item.get('decision', 'not_applicable')}`",
                f"- Counts: `{_safe_json(item.get('counts', {}))}`",
                f"- Publication safety: `{_safe_json(item.get('publication_safety', {}))}`",
                "",
            ]
        )
    lines.extend(["## Validation Boundaries"])
    validation_boundaries = package.get("validation_boundaries") if isinstance(package.get("validation_boundaries"), Mapping) else {}
    for name, item in validation_boundaries.items():
        if not isinstance(item, Mapping):
            continue
        lines.extend(
            [
                f"### {name}",
                f"- Status: `{item.get('status', 'unknown')}`",
                f"- Summary: {item.get('summary', 'unknown')}",
            ]
        )
        if item.get("acquisition_contract"):
            lines.append(f"- Acquisition contract: `{item.get('acquisition_contract')}`")
        if item.get("blocked_requirements"):
            lines.append(f"- Blocked requirements: {_comma_list(item.get('blocked_requirements'))}")
        if item.get("verifier_state"):
            lines.append(f"- Verifier state: `{item.get('verifier_state')}`")
        if item.get("missing_inputs"):
            lines.append(f"- Missing inputs: {_comma_list(item.get('missing_inputs'))}")
        if item.get("blocker_reason"):
            lines.append(f"- Blocker reason: {item.get('blocker_reason')}")
        if item.get("live_soak_implication"):
            lines.append(f"- Live-soak implication: {item.get('live_soak_implication')}")
        if item.get("missing_semantic_tags"):
            lines.append(f"- Missing semantic tags: {_comma_list(item.get('missing_semantic_tags'))}")
        if item.get("required_acquisition"):
            lines.append(f"- Required acquisition: {_comma_list(item.get('required_acquisition'))}")
        if item.get("not_a_substitute_for"):
            lines.append(f"- Not a substitute for: {_comma_list(item.get('not_a_substitute_for'))}")
        lines.append("")
    if not validation_boundaries:
        lines.append("- none")
    lines.extend(["## Requirement Coverage"])
    requirement_coverage = package.get("requirement_coverage") if isinstance(package.get("requirement_coverage"), Mapping) else {}
    for requirement_id in sorted(requirement_coverage):
        item = requirement_coverage.get(requirement_id)
        if not isinstance(item, Mapping):
            continue
        missing_tags = item.get("missing_semantic_tags", [])
        lines.extend(
            [
                f"### {requirement_id}",
                f"- Status: `{item.get('status', 'unknown')}`",
                f"- Evidence source: `{item.get('evidence_source', 'unknown')}`",
                f"- Rationale: {item.get('rationale', 'unknown')}",
                f"- Missing semantic tags: {_comma_list(missing_tags)}",
                f"- Blocked/deferred reason: {item.get('blocked_or_deferred_reason') or 'none'}",
                "",
            ]
        )
    if not requirement_coverage:
        lines.append("- none")
    lines.extend(["", "## Requirement Implications"])
    implications = package.get("requirement_implications") if isinstance(package.get("requirement_implications"), list) else []
    lines.extend(f"- {implication}" for implication in implications)
    if not implications:
        lines.append("- none")
    lines.extend(["", "## Publication Safety Scan"])
    publication_safety = package.get("publication_safety") if isinstance(package.get("publication_safety"), Mapping) else {}
    for scope, scan in publication_safety.items():
        lines.append(f"- {scope}: `{_safe_json(scan)}`")
    lines.append("")
    return "\n".join(lines)


def evidence_result(
    *,
    artifact: str,
    status: str,
    reason: str,
    requirement_implications: Sequence[str],
    counts: Any | None = None,
    decision: str | None = None,
    verifier_state: str | None = None,
    publication_safety: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "artifact": artifact,
        "status": status,
        "reason": reason,
        "counts": counts or {},
        "decision": decision,
        "verifier_state": verifier_state,
        "requirement_implications": list(requirement_implications),
        "publication_safety": dict(publication_safety or {"redaction_hits": 0, "passed": True}),
    }


def _redaction_failure(report: Mapping[str, Any], reason: str, artifact_label: str) -> dict[str, Any] | None:
    safety = _redaction_counts(report)
    redaction = report.get("redaction_scan") if isinstance(report.get("redaction_scan"), Mapping) else {}
    if redaction.get("passed") is not True or safety.get("redaction_hits", 0) > 0:
        return evidence_result(
            artifact=artifact_label,
            status="failed",
            reason=reason,
            requirement_implications=["Publication safety failed; generated evidence must not be published"],
            publication_safety=safety,
        )
    return None


def _redaction_counts(report: Mapping[str, Any]) -> dict[str, Any]:
    redaction = report.get("redaction_scan") if isinstance(report.get("redaction_scan"), Mapping) else {}
    secret_hits = _int(redaction.get("secret_occurrences"))
    forbidden_hits = _int(redaction.get("forbidden_pattern_occurrences"))
    findings = redaction.get("findings") if isinstance(redaction.get("findings"), list) else []
    return {
        "passed": redaction.get("passed", secret_hits == 0 and forbidden_hits == 0) is True and secret_hits == 0 and forbidden_hits == 0,
        "redaction_hits": secret_hits + forbidden_hits,
        "secret_occurrences": secret_hits,
        "forbidden_pattern_occurrences": forbidden_hits,
        "redaction_replacements": _int(redaction.get("redaction_replacements")),
        "findings": [str(item) for item in findings],
    }


def _counts(value: Any) -> dict[str, int]:
    source = value if isinstance(value, Mapping) else {}
    return {key: _int(source.get(key)) for key in ("passed", "failed", "blocked", "not_covered")}


def _nested_str(report: Mapping[str, Any], key: str, nested_key: str) -> str:
    nested = report.get(key) if isinstance(report.get(key), Mapping) else {}
    return str(nested.get(nested_key, "unknown"))


def _is_live_preflight(result: Mapping[str, Any]) -> bool:
    return str(result.get("status")) == "preflight_failed"


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _unique(items: Any) -> list[str]:
    result: list[str] = []
    for item in items:
        if item not in result:
            result.append(item)
    return result


def _no_change_decision(tuning: Mapping[str, Any]) -> str | None:
    decision = tuning.get("decision")
    if decision in {"keep_shared_thresholds", "apply_shared_tuning"}:
        return str(decision)
    return None


def _publication_scan_payload(
    evidence: Mapping[str, Mapping[str, Any]],
    implications: Sequence[str],
    requirement_coverage: Mapping[str, Mapping[str, Any]],
    validation_boundaries: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "evidence": evidence,
        "requirement_implications": list(implications),
        "requirement_coverage": requirement_coverage,
        "validation_boundaries": validation_boundaries,
    }



def _comma_list(values: Any) -> str:
    if not values:
        return "None"
    if isinstance(values, str):
        return values
    if not isinstance(values, Sequence):
        return "Unknown"
    return ", ".join(f"`{value}`" for value in values)

def _safe_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


if __name__ == "__main__":
    raise SystemExit(main())
