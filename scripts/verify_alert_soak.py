#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parking_spot_monitor.replay import scan_report_redactions

DATA_DIR = Path("data")
RESULT_PATH = DATA_DIR / "alert-soak-result.json"
EVIDENCE_PATH = DATA_DIR / "alert-soak-evidence.md"
ORGANIC_ALERT_EVENT = "occupancy-open-event"
SUCCESS_STATUS = "success"
COVERAGE_GAP_STATUS = "coverage_gap_no_alert"
PREFLIGHT_STATUS = "preflight_failed"
FAILURE_STATUSES = {"docker_failed", "validation_failed", "readback_gap"}
SUPPORTED_STATUSES = {SUCCESS_STATUS, COVERAGE_GAP_STATUS, PREFLIGHT_STATUS, *FAILURE_STATUSES}


class VerificationError(Exception):
    def __init__(self, public_reason: str) -> None:
        super().__init__(public_reason)
        self.public_reason = public_reason


@dataclass(frozen=True)
class VerificationOutcome:
    accepted: bool
    state: str
    findings: tuple[str, ...]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Strictly verify Docker alert-soak evidence and render a publication-safe report.")
    parser.add_argument("--result", default=str(RESULT_PATH), help="Path to alert-soak result JSON.")
    parser.add_argument("--evidence", default=str(EVIDENCE_PATH), help="Path to write publication-safe Markdown evidence.")
    parser.add_argument(
        "--allow-coverage-gap",
        action="store_true",
        help="Exit successfully for an honest no-alert coverage-gap report; S08 strict live soak validation remains incomplete.",
    )
    parser.add_argument(
        "--allow-preflight-blocker",
        action="store_true",
        help="Exit successfully for an honest names-only preflight blocker report; S08 strict live soak validation remains blocked.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result_path = Path(args.result)
    evidence_path = Path(args.evidence)
    try:
        result = normalize_result_contract(_load_result(result_path))
        outcome = validate_result(
            result,
            allow_coverage_gap=bool(args.allow_coverage_gap),
            allow_preflight_blocker=bool(args.allow_preflight_blocker),
        )
        report = render_evidence_report(result, outcome)
        _assert_report_redacted(report)
    except VerificationError as exc:
        failure_result = {"status": "verification_failed", "phase": "verifier", "failure": exc.public_reason}
        outcome = VerificationOutcome(False, "verifier_error", (exc.public_reason,))
        report = render_evidence_report(failure_result, outcome)
        _assert_report_redacted(report)
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(report, encoding="utf-8")
        print(exc.public_reason, file=sys.stderr)
        return 1
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(report, encoding="utf-8")
    return 0 if outcome.accepted else 1


def _load_result(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise VerificationError("result JSON is missing") from exc
    except json.JSONDecodeError as exc:
        raise VerificationError("result JSON is malformed") from exc
    if not isinstance(raw, dict):
        raise VerificationError("result JSON must be an object")
    return raw


def normalize_result_contract(result: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(result)
    alias_pairs = [
        ("alerts", "alert_summary"),
        ("duplicates", "duplicate_summary"),
        ("artifacts", "artifact_summary"),
        ("matrix_room_readback", "room_readback"),
        ("redaction", "redaction_scan"),
    ]
    for alias, detailed in alias_pairs:
        alias_present = alias in normalized
        detailed_present = detailed in normalized
        alias_value = normalized.get(alias)
        detailed_value = normalized.get(detailed)
        if alias_present and detailed_present and alias_value != detailed_value:
            raise VerificationError(f"result JSON has inconsistent {alias}/{detailed} values")
        value = detailed_value if detailed_present else alias_value
        normalized[detailed] = value
        normalized[alias] = value

    status = normalized.get("status")
    if status == "coverage_gap" and _organic_alert_count(normalized) == 0:
        normalized["status"] = COVERAGE_GAP_STATUS
    if normalized.get("status") not in SUPPORTED_STATUSES:
        raise VerificationError(f"unsupported alert-soak status: {normalized.get('status')}")
    return normalized


def validate_result(result: Mapping[str, Any], *, allow_coverage_gap: bool, allow_preflight_blocker: bool) -> VerificationOutcome:
    status = str(result.get("status"))
    if status == SUCCESS_STATUS:
        return VerificationOutcome(True, "success", tuple(_validate_success(result)))
    if status == COVERAGE_GAP_STATUS:
        findings = tuple(_validate_coverage_gap(result))
        return VerificationOutcome(allow_coverage_gap, COVERAGE_GAP_STATUS, findings)
    if status == PREFLIGHT_STATUS:
        findings = tuple(_validate_preflight(result))
        return VerificationOutcome(allow_preflight_blocker, "preflight_blocked", findings)
    if status in FAILURE_STATUSES:
        findings = tuple(_failure_findings(result))
        return VerificationOutcome(False, status, findings)
    raise VerificationError(f"unsupported alert-soak status: {status}")


def _validate_success(result: Mapping[str, Any]) -> list[str]:
    findings: list[str] = []
    _require_completed_docker(result)
    findings.append("Docker completed normally or reached the controlled soak timeout and stopped cleanly")

    alerts = _observed_alerts(result)
    if not alerts:
        raise VerificationError("strict success requires at least one organic occupancy-open-event alert")
    findings.append(f"Observed organic occupancy-open-event alerts: {len(alerts)}")

    _require_no_duplicates(result)
    findings.append("Duplicate event and Matrix transaction diagnostics are clean")

    _require_valid_artifacts(result, alerts)
    findings.append("Latest JPEG and per-alert occupancy-open-event snapshots are valid")

    _require_health_and_state(result)
    findings.append("health.json is healthy or diagnosable and state.json is parseable")

    _require_verified_readback(result, alerts)
    findings.append("Matrix readback is verified for every observed alert")

    _require_zero_redaction_hits(result)
    findings.append("Redaction scan has zero secret and forbidden-pattern hits")
    findings.append("S08 strict live soak validation complete")
    return findings


def _validate_coverage_gap(result: Mapping[str, Any]) -> list[str]:
    if _organic_alert_count(result) != 0 or _observed_alerts(result):
        raise VerificationError("coverage_gap_no_alert requires zero observed organic alerts")
    _require_completed_docker(result)
    _require_zero_redaction_hits(result)
    return [
        "No organic occupancy-open-event alerts were observed during the bounded soak",
        "This is an honest coverage gap, not a successful live-alert validation",
        "S08 strict live soak validation remains incomplete",
    ]


def _validate_preflight(result: Mapping[str, Any]) -> list[str]:
    missing_inputs = result.get("missing_inputs")
    if not isinstance(missing_inputs, list) or not all(isinstance(item, str) and item for item in missing_inputs):
        raise VerificationError("preflight_failed result must contain missing input names only")
    rendered_missing = "\n".join(_display_missing_input(item) for item in missing_inputs)
    if scan_report_redactions(rendered_missing).get("passed") is False:
        raise VerificationError("preflight missing inputs contain unsafe private content")
    docker = result.get("docker") if isinstance(result.get("docker"), dict) else {}
    if docker.get("attempted") is True or docker.get("exit_code") is not None:
        raise VerificationError("preflight_failed must not overclaim a Docker run")
    _require_zero_redaction_hits(result)
    return [
        "Preflight blocked before Docker execution",
        "Missing input names: " + _display_missing_inputs(missing_inputs),
        "S08 strict live soak validation blocked by preflight",
    ]


def _failure_findings(result: Mapping[str, Any]) -> list[str]:
    status = str(result.get("status"))
    phase = str(result.get("phase") or "unknown")
    findings = [f"Runner reported failure status `{status}` in phase `{phase}`"]
    duplicates = _duplicates(result)
    if duplicates.get("event_ids") or duplicates.get("txn_ids"):
        findings.append("Duplicate-spam diagnostics are non-empty")
    redaction = result.get("redaction_scan") if isinstance(result.get("redaction_scan"), dict) else {}
    if _int_value(redaction.get("secret_occurrences")) or _int_value(redaction.get("forbidden_pattern_occurrences")):
        findings.append("Redaction scan found unsafe content")
    findings.append("S08 strict live soak validation failed")
    return findings


def _require_completed_docker(result: Mapping[str, Any]) -> None:
    docker = result.get("docker") if isinstance(result.get("docker"), dict) else None
    if docker is None:
        raise VerificationError("verification requires Docker execution summary")
    if docker.get("attempted") is not True:
        raise VerificationError("verification requires Docker to have been attempted")
    if docker.get("killed") is True:
        raise VerificationError("Docker process was killed rather than cleanly controlled")
    exit_code = docker.get("exit_code")
    controlled_timeout = docker.get("expected_timeout_completion") is True and docker.get("timed_out") is True and docker.get("terminated") is True
    if exit_code == 0 or controlled_timeout:
        return
    raise VerificationError("verification requires Docker completion or a controlled soak-timeout stop")


def _require_no_duplicates(result: Mapping[str, Any]) -> None:
    duplicates = _duplicates(result)
    event_ids = duplicates.get("event_ids") if isinstance(duplicates.get("event_ids"), dict) else {}
    txn_ids = duplicates.get("txn_ids") if isinstance(duplicates.get("txn_ids"), dict) else {}
    if event_ids:
        raise VerificationError("duplicate organic event IDs detected")
    if txn_ids:
        raise VerificationError("duplicate Matrix transaction IDs detected")


def _require_valid_artifacts(result: Mapping[str, Any], alerts: Sequence[Mapping[str, Any]]) -> None:
    artifacts = result.get("artifact_summary") if isinstance(result.get("artifact_summary"), dict) else {}
    latest = artifacts.get("latest_jpeg") if isinstance(artifacts.get("latest_jpeg"), dict) else {}
    if latest.get("exists") is not True or latest.get("valid_jpeg") is not True:
        raise VerificationError("strict success requires a valid latest JPEG artifact")
    snapshots = artifacts.get("event_snapshot_jpegs") if isinstance(artifacts.get("event_snapshot_jpegs"), dict) else {}
    files = snapshots.get("files") if isinstance(snapshots.get("files"), list) else []
    valid_paths = [str(item.get("path") or "") for item in files if isinstance(item, dict) and item.get("valid_jpeg") is True]
    if _int_value(snapshots.get("valid_count")) < len(alerts):
        raise VerificationError("strict success requires at least one matching valid occupancy-open-event snapshot per observed alert")
    for alert in alerts:
        spot_id = str(alert.get("spot_id") or "")
        sanitized = spot_id.replace("_", "-").lower()
        if not any(ORGANIC_ALERT_EVENT in path and (not sanitized or sanitized in path.lower()) for path in valid_paths):
            raise VerificationError("strict success requires a matching valid occupancy-open-event snapshot for each observed alert")


def _require_health_and_state(result: Mapping[str, Any]) -> None:
    health = result.get("health_summary") if isinstance(result.get("health_summary"), dict) else {}
    if health.get("exists") is not True:
        raise VerificationError("strict success requires health.json to exist")
    if health.get("parse_ok") is not True and not health.get("error_type"):
        raise VerificationError("health.json must be parseable or include diagnosable parse failure metadata")
    state = result.get("state_summary") if isinstance(result.get("state_summary"), dict) else {}
    if state.get("exists") is not True or state.get("parse_ok") is not True:
        raise VerificationError("strict success requires parseable state.json")


def _require_verified_readback(result: Mapping[str, Any], alerts: Sequence[Mapping[str, Any]]) -> None:
    readback = result.get("room_readback") if isinstance(result.get("room_readback"), dict) else {}
    if result.get("room_readback_status") != "verified" or readback.get("status") != "verified":
        raise VerificationError("strict success requires Matrix readback status verified")
    per_alert = readback.get("per_alert") if isinstance(readback.get("per_alert"), list) else []
    if len(per_alert) < len(alerts):
        raise VerificationError("Matrix readback must include every observed alert")
    for item in per_alert[: len(alerts)]:
        if not isinstance(item, dict) or item.get("text_found") is not True or item.get("image_found") is not True:
            raise VerificationError("Matrix readback must verify text and image evidence for every observed alert")


def _require_zero_redaction_hits(result: Mapping[str, Any]) -> None:
    redaction = result.get("redaction_scan") if isinstance(result.get("redaction_scan"), dict) else None
    if redaction is None:
        raise VerificationError("verification requires redaction_scan")
    if _int_value(redaction.get("secret_occurrences")) != 0 or _int_value(redaction.get("forbidden_pattern_occurrences")) != 0:
        raise VerificationError("redaction scan must have zero hits")


def render_evidence_report(result: Mapping[str, Any], validation: VerificationOutcome) -> str:
    status = str(result.get("status", "unknown"))
    lines = [
        "# Alert Soak Evidence",
        "",
        f"- Status: `{status}`",
        f"- Phase: `{result.get('phase', 'unknown')}`",
        f"- Verification state: `{validation.state}`",
        f"- Requirement status: {_requirement_status(validation)}",
        "",
        "## Organic Alert and Readback Summary",
        f"- Organic alerts: `{_organic_alert_count(result)}`",
        f"- Matrix readback: `{result.get('room_readback_status', 'not_attempted')}`",
        f"- Readback alerts checked: `{_readback_alerts_checked(result)}`",
        "",
        "## Artifact Summary",
        *_artifact_lines(result),
        "",
        "## Duplicate-Spam Diagnostics",
        *_duplicate_lines(result),
        "",
        "## Health and State Summary",
        *_health_state_lines(result),
        "",
        "## Redaction Counts",
        *_redaction_lines(result),
        "",
        "## Operator Handoff",
        *_operator_handoff_lines(result, validation),
        "",
        "## Findings",
        *(f"- {finding}" for finding in validation.findings),
        "",
    ]
    return "\n".join(lines)


def _artifact_lines(result: Mapping[str, Any]) -> list[str]:
    artifacts = result.get("artifact_summary") if isinstance(result.get("artifact_summary"), dict) else {}
    latest = artifacts.get("latest_jpeg") if isinstance(artifacts.get("latest_jpeg"), dict) else {}
    snapshots = artifacts.get("event_snapshot_jpegs") if isinstance(artifacts.get("event_snapshot_jpegs"), dict) else {}
    return [
        f"- Latest JPEG: exists=`{latest.get('exists')}` valid=`{latest.get('valid_jpeg')}` bytes=`{latest.get('byte_size', 0)}`",
        f"- Event snapshots: count=`{snapshots.get('count', 0)}` valid_count=`{snapshots.get('valid_count', 0)}` summarized_count=`{snapshots.get('summarized_count', 0)}`",
    ]


def _duplicate_lines(result: Mapping[str, Any]) -> list[str]:
    duplicates = _duplicates(result)
    event_ids = duplicates.get("event_ids") if isinstance(duplicates.get("event_ids"), dict) else {}
    txn_ids = duplicates.get("txn_ids") if isinstance(duplicates.get("txn_ids"), dict) else {}
    return [
        f"- Duplicate event IDs: {_none_or_count(event_ids)}",
        f"- Duplicate Matrix transaction IDs: {_none_or_count(txn_ids)}",
    ]


def _health_state_lines(result: Mapping[str, Any]) -> list[str]:
    health = result.get("health_summary") if isinstance(result.get("health_summary"), dict) else {}
    state = result.get("state_summary") if isinstance(result.get("state_summary"), dict) else {}
    return [
        f"- health.json: exists=`{health.get('exists')}` parse_ok=`{health.get('parse_ok')}` status=`{health.get('status', 'unknown')}` iteration=`{health.get('iteration', 'unknown')}`",
        f"- state.json: exists=`{state.get('exists')}` parse_ok=`{state.get('parse_ok')}` spot_count=`{state.get('spot_count', 'unknown')}`",
    ]


def _redaction_lines(result: Mapping[str, Any]) -> list[str]:
    redaction = result.get("redaction_scan") if isinstance(result.get("redaction_scan"), dict) else {}
    return [
        f"- Secret occurrences: `{redaction.get('secret_occurrences', 0)}`",
        f"- Forbidden pattern occurrences: `{redaction.get('forbidden_pattern_occurrences', 0)}`",
        f"- Redaction replacements: `{redaction.get('redaction_replacements', 0)}`",
    ]


def _operator_handoff_lines(result: Mapping[str, Any], validation: VerificationOutcome) -> list[str]:
    status = str(result.get("status"))
    if status == SUCCESS_STATUS and validation.accepted:
        return ["- Strict gate passed; evidence is suitable for publication-safe S08 strict live soak validation."]
    if status == COVERAGE_GAP_STATUS:
        return [
            "- Outcome: The soak completed without organic alert traffic.",
            "- Next step: Run another bounded soak when a real parking spot opens.",
            "- Validation rule: S08 strict live soak validation remains incomplete until strict success observes and verifies at least one organic alert.",
        ]
    if status == PREFLIGHT_STATUS:
        missing = result.get("missing_inputs") if isinstance(result.get("missing_inputs"), list) else []
        return [
            "- Outcome: Blocked during preflight. No Docker alert soak was attempted.",
            "- Missing input names: " + _display_missing_inputs(missing),
            "- Next step: Provide the named config/environment inputs and rerun the runner plus verifier.",
        ]
    return ["- Outcome: Strict gate failed; inspect the named status, phase, and summarized diagnostics before rerunning."]


def _requirement_status(validation: VerificationOutcome) -> str:
    if validation.accepted and validation.state == SUCCESS_STATUS:
        return "S08 strict live soak validation complete"
    if validation.accepted and validation.state == COVERAGE_GAP_STATUS:
        return "S08 strict live soak validation remains incomplete"
    if validation.accepted and validation.state == "preflight_blocked":
        return "S08 strict live soak validation blocked by preflight"
    return "S08 strict live soak validation failed or incomplete"


def _assert_report_redacted(report: str) -> None:
    scan = scan_report_redactions(report)
    if scan.get("passed") is not True:
        findings = ", ".join(str(item) for item in scan.get("findings", []))
        raise VerificationError(f"evidence report contains unsafe content: {findings}")


def _observed_alerts(result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    log_summary = result.get("log_summary") if isinstance(result.get("log_summary"), dict) else {}
    alerts = log_summary.get("organic_alerts") if isinstance(log_summary.get("organic_alerts"), list) else []
    return [alert for alert in alerts if isinstance(alert, Mapping)]


def _organic_alert_count(result: Mapping[str, Any]) -> int:
    alert_summary = result.get("alert_summary") if isinstance(result.get("alert_summary"), dict) else {}
    if "organic_alert_count" in alert_summary:
        return _int_value(alert_summary.get("organic_alert_count"))
    log_summary = result.get("log_summary") if isinstance(result.get("log_summary"), dict) else {}
    return _int_value(log_summary.get("organic_alert_count"))


def _readback_alerts_checked(result: Mapping[str, Any]) -> int:
    readback = result.get("room_readback") if isinstance(result.get("room_readback"), dict) else {}
    return _int_value(readback.get("alerts_checked"))


def _duplicates(result: Mapping[str, Any]) -> dict[str, Any]:
    duplicates = result.get("duplicate_summary") if isinstance(result.get("duplicate_summary"), dict) else None
    if duplicates is not None:
        return duplicates
    log_summary = result.get("log_summary") if isinstance(result.get("log_summary"), dict) else {}
    nested = log_summary.get("duplicates") if isinstance(log_summary.get("duplicates"), dict) else {}
    return nested


def _none_or_count(value: Mapping[str, Any]) -> str:
    return "none" if not value else f"{len(value)} keys"


def _display_missing_inputs(missing: Sequence[Any]) -> str:
    names = [_display_missing_input(str(item)) for item in missing]
    return ", ".join(names) if names else "none reported"


def _display_missing_input(name: str) -> str:
    if name in {"MATRIX_ACCESS_TOKEN", "MATRIX_TOKEN_ENV"}:
        return "Matrix token env key"
    if name == "matrix.room_id":
        return "Matrix room config key"
    if name == "matrix.access_token_env":
        return "Matrix token config key"
    return name


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
