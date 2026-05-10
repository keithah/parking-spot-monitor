#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CONFIG_PATH = Path("config.yaml")
DATA_DIR = Path("data")
RESULT_PATH = DATA_DIR / "live-proof-result.json"
EVIDENCE_PATH = DATA_DIR / "live-proof-evidence.md"
STDOUT_LOG = DATA_DIR / "live-proof-docker.stdout.log"
STDERR_LOG = DATA_DIR / "live-proof-docker.stderr.log"

SKIPPED_CONFIG_ABSENT = "LIVE_PROOF_SKIPPED_CONFIG_ABSENT"
SKIPPED_RTSP_ENV_ABSENT = "LIVE_PROOF_SKIPPED_RTSP_ENV_ABSENT"
SKIPPED_MATRIX_ENV_ABSENT = "LIVE_PROOF_SKIPPED_MATRIX_ENV_ABSENT"
LIVE_RTSP_CAPTURE_OK = "LIVE_RTSP_CAPTURE_OK"
LIVE_MATRIX_TEXT_OK = "LIVE_MATRIX_TEXT_OK"
LIVE_MATRIX_IMAGE_OK = "LIVE_MATRIX_IMAGE_OK"

FORBIDDEN_REPORT_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [r"rtsp://", r"Authorization", r"Bearer", r"access_token", r"Traceback"]
]
FORBIDDEN_LOG_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [r"rtsp://", r"Authorization", r"Bearer", r"access_token", r"Traceback"]
]


def skip_markers(*, config_path: Path = CONFIG_PATH, environ: Mapping[str, str] | None = None) -> list[str]:
    source_environ = os.environ if environ is None else environ
    markers: list[str] = []
    if not config_path.is_file():
        markers.append(SKIPPED_CONFIG_ABSENT)
        return markers
    if not source_environ.get("RTSP_URL"):
        markers.append(SKIPPED_RTSP_ENV_ABSENT)
    if not source_environ.get("MATRIX_ACCESS_TOKEN"):
        markers.append(SKIPPED_MATRIX_ENV_ABSENT)
    return markers


def run_live_proof_command(*, config_path: Path = CONFIG_PATH, data_dir: Path = DATA_DIR) -> subprocess.CompletedProcess[str]:
    data_dir.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "parking_spot_monitor",
            "--config",
            str(config_path),
            "--data-dir",
            str(data_dir),
            "--live-proof-once",
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify redacted live-proof result JSON and write auditable evidence.")
    parser.add_argument("--result", default=str(RESULT_PATH), help="Path to live-proof result JSON.")
    parser.add_argument("--evidence", default=str(EVIDENCE_PATH), help="Path to write redacted evidence report.")
    parser.add_argument(
        "--allow-preflight-blocker",
        action="store_true",
        help="Exit successfully for an honest preflight blocker report; requirements remain unvalidated.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result_path = Path(args.result)
    evidence_path = Path(args.evidence)
    try:
        result = _load_result(result_path)
        normalized = normalize_result_contract(result)
        validation = validate_result(normalized, allow_preflight_blocker=args.allow_preflight_blocker, artifact_root=result_path.parent)
        report = render_evidence_report(normalized, validation)
        _assert_report_redacted(report)
    except VerificationError as exc:
        failure_result = {"status": "verification_failed", "failure": exc.public_reason}
        report = render_evidence_report(failure_result, VerificationOutcome(False, exc.public_reason, [exc.public_reason]))
        _assert_report_redacted(report)
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(report, encoding="utf-8")
        print(exc.public_reason, file=sys.stderr)
        return 1

    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(report, encoding="utf-8")
    return 0 if validation.accepted else 1


class VerificationError(Exception):
    def __init__(self, public_reason: str) -> None:
        super().__init__(public_reason)
        self.public_reason = public_reason


class VerificationOutcome:
    def __init__(self, accepted: bool, state: str, findings: list[str]) -> None:
        self.accepted = accepted
        self.state = state
        self.findings = findings


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
        ("markers", "marker_checks"),
        ("artifacts", "artifact_checks"),
        ("redaction", "redaction_scan"),
        ("matrix_room_readback", "room_readback"),
    ]
    for alias, detailed in alias_pairs:
        alias_value = normalized.get(alias)
        detailed_value = normalized.get(detailed)
        if alias in normalized and detailed in normalized and alias_value != detailed_value:
            raise VerificationError(f"result JSON has inconsistent {alias}/{detailed} values")
        value = detailed_value if detailed in normalized else alias_value
        normalized[detailed] = value
        normalized[alias] = value
    return normalized


def validate_result(result: Mapping[str, Any], *, allow_preflight_blocker: bool, artifact_root: Path = DATA_DIR) -> VerificationOutcome:
    status = result.get("status")
    if status == "preflight_failed":
        findings = _validate_preflight(result)
        accepted = allow_preflight_blocker
        return VerificationOutcome(accepted, "preflight_failed", findings)
    if status == "success":
        return VerificationOutcome(True, "success", _validate_success(result, artifact_root=artifact_root))
    raise VerificationError(f"unsupported live-proof status: {status}")


def _validate_preflight(result: Mapping[str, Any]) -> list[str]:
    missing_inputs = result.get("missing_inputs")
    if not isinstance(missing_inputs, list) or not all(isinstance(item, str) and item for item in missing_inputs):
        raise VerificationError("preflight result must name missing path/key inputs")
    allowed_missing = {
        "config.yaml",
        "RTSP_URL",
        "MATRIX_ACCESS_TOKEN",
        "MATRIX_TOKEN_ENV",
        "matrix.homeserver",
        "matrix.room_id",
        "matrix.access_token_env",
        "stream.rtsp_url_env",
    }
    unexpected = sorted(set(missing_inputs) - allowed_missing)
    if unexpected:
        raise VerificationError("preflight result contains unexpected missing input names")
    if result.get("docker_exit_code") is not None:
        raise VerificationError("preflight result must not include a Docker exit code")
    redaction = result.get("redaction_scan") or {}
    if not isinstance(redaction, dict) or int(redaction.get("secret_occurrences", 0)) != 0:
        raise VerificationError("preflight result failed redaction scan")
    display_missing = ", ".join(_display_missing_input(item) for item in missing_inputs)
    return [f"missing inputs: {display_missing}", "R003/R015 remain unvalidated"]


def _validate_success(result: Mapping[str, Any], *, artifact_root: Path = DATA_DIR) -> list[str]:
    findings: list[str] = []
    if result.get("docker_exit_code") != 0:
        raise VerificationError("success requires Docker exit code 0")
    markers = result.get("marker_checks")
    if not isinstance(markers, dict):
        raise VerificationError("success requires marker checks")
    if markers.get("required_present") is not True:
        raise VerificationError("success requires all live-proof markers")
    forbidden = markers.get("forbidden_present")
    if forbidden:
        raise VerificationError("success cannot include skipped or failed live-proof markers")
    findings.append("Docker markers: RTSP capture, Matrix text, and Matrix image all present")

    artifacts = result.get("artifact_checks")
    if not isinstance(artifacts, dict):
        raise VerificationError("success requires artifact checks")
    latest = artifacts.get("latest_jpeg")
    snapshots = artifacts.get("snapshot_jpegs")
    if not isinstance(latest, dict) or latest.get("valid_jpeg") is not True:
        raise VerificationError("success requires valid data/latest.jpg")
    if not isinstance(snapshots, dict) or int(snapshots.get("valid_count", 0)) < 1:
        raise VerificationError("success requires at least one valid live-proof snapshot JPEG")
    findings.append("JPEG artifacts: latest frame and live-proof snapshot are valid")

    readback = result.get("room_readback")
    if result.get("room_readback_status") != "verified" or not isinstance(readback, dict):
        raise VerificationError("success requires Matrix room readback verification")
    if readback.get("text_found") is not True or readback.get("image_found") is not True:
        raise VerificationError("success cannot infer Matrix visibility from send responses alone")
    findings.append("Matrix room readback: text and image evidence found in room history")

    redaction = result.get("redaction_scan")
    if not isinstance(redaction, dict) or int(redaction.get("secret_occurrences", 0)) != 0:
        raise VerificationError("success failed redaction scan")
    _assert_logs_redacted(artifact_root)
    findings.append("Redaction scan: no forbidden pattern detected")
    findings.append("R003/R015 validated")
    return findings


def render_evidence_report(result: Mapping[str, Any], validation: VerificationOutcome) -> str:
    status = str(result.get("status", "unknown"))
    lines = [
        "# Live Proof Evidence",
        "",
        f"- Status: `{status}`",
        f"- Verification state: `{validation.state}`",
    ]
    lines.extend(_evidence_summary_lines(result))
    if status == "preflight_failed":
        missing = result.get("missing_inputs") if isinstance(result.get("missing_inputs"), list) else []
        lines.append(f"- Missing inputs: {', '.join(_display_missing_input(str(item)) for item in missing)}")
    lines.append(f"- Requirement status: {_requirement_status(validation)}")
    if status == "preflight_failed":
        lines.extend(_operator_handoff_lines(result))
    lines.extend(["", "## Findings"])
    lines.extend(f"- {finding}" for finding in validation.findings)
    lines.append("")
    return "\n".join(lines)


def _operator_handoff_lines(result: Mapping[str, Any]) -> list[str]:
    missing = result.get("missing_inputs") if isinstance(result.get("missing_inputs"), list) else []
    display_missing = ", ".join(_display_missing_input(str(item)) for item in missing) or "none reported"
    return [
        "",
        "## Operator Handoff",
        "- Outcome: Blocked during preflight. No Docker/live proof was attempted.",
        "- Missing input names: " + display_missing,
        "- Required future inputs: RTSP_URL, matrix.homeserver, matrix.room_id, and the configured Matrix token env key.",
        "- Future strict run command: `python scripts/run_docker_live_proof.py` followed by `python scripts/verify_live_proof.py`.",
        "- Validation rule: keep R003/R015 active until strict success includes Docker exit 0, valid JPEG artifacts, Matrix room readback, and zero redaction hits.",
    ]


def _evidence_summary_lines(result: Mapping[str, Any]) -> list[str]:
    markers = result.get("marker_checks") if isinstance(result.get("marker_checks"), dict) else {}
    artifacts = result.get("artifact_checks") if isinstance(result.get("artifact_checks"), dict) else {}
    latest = artifacts.get("latest_jpeg") if isinstance(artifacts.get("latest_jpeg"), dict) else {}
    snapshots = artifacts.get("snapshot_jpegs") if isinstance(artifacts.get("snapshot_jpegs"), dict) else {}
    readback = result.get("room_readback") if isinstance(result.get("room_readback"), dict) else {}
    redaction = result.get("redaction_scan") if isinstance(result.get("redaction_scan"), dict) else {}

    required_missing = markers.get("missing_required", "not checked")
    forbidden_present = markers.get("forbidden_present", "not checked")
    readback_status = str(result.get("room_readback_status", "not_attempted"))
    return [
        f"- Docker exit code: `{result.get('docker_exit_code')}`",
        f"- Required marker gaps: `{required_missing}`",
        f"- Forbidden markers present: `{forbidden_present}`",
        f"- Latest JPEG: `{latest.get('path', 'data/latest.jpg')}` exists={latest.get('exists')} valid={latest.get('valid_jpeg')}",
        f"- Snapshot JPEG count: `{snapshots.get('count', 0)}` valid_count=`{snapshots.get('valid_count', 0)}`",
        f"- Matrix room readback: {readback_status} text_found={readback.get('text_found')} image_found={readback.get('image_found')}",
        f"- Redaction secret occurrences: `{redaction.get('secret_occurrences', 0)}`",
        f"- Redaction replacements: `{redaction.get('redaction_replacements', 0)}`",
    ]


def _requirement_status(validation: VerificationOutcome) -> str:
    if validation.accepted and validation.state == "success":
        return "R003/R015 validated"
    return "R003/R015 remain unvalidated"


def _display_missing_input(name: str) -> str:
    if name in {"MATRIX_ACCESS_TOKEN", "MATRIX_TOKEN_ENV"}:
        return "Matrix token env key"
    return name


def _assert_logs_redacted(artifact_root: Path = DATA_DIR) -> None:
    for filename in [RESULT_PATH.name, STDOUT_LOG.name, STDERR_LOG.name]:
        path = artifact_root / filename
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if any(pattern.search(text) for pattern in FORBIDDEN_LOG_PATTERNS):
            raise VerificationError("proof artifacts contain a forbidden leakage pattern")


def _assert_report_redacted(report: str) -> None:
    if any(pattern.search(report) for pattern in FORBIDDEN_REPORT_PATTERNS):
        raise VerificationError("evidence report contains a forbidden leakage pattern")


if __name__ == "__main__":
    raise SystemExit(main())
