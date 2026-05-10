from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from scripts import verify_alert_soak as verifier

OBSERVED_AT = "2026-05-18T19:00:00Z"
EVENT_ID = f"occupancy-open-event:left_spot:{OBSERVED_AT}"
SNAPSHOT_PATH = "data/snapshots/occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"


def write_result(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def base_success() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "success",
        "phase": "complete",
        "requested_soak_seconds": 300.0,
        "observed_soak_seconds": 300.2,
        "docker": {
            "attempted": True,
            "exit_code": -15,
            "timed_out": True,
            "terminated": True,
            "killed": False,
            "expected_timeout_completion": True,
        },
        "log_summary": {
            "organic_alert_count": 1,
            "organic_alerts": [
                {
                    "event_id": EVENT_ID,
                    "spot_id": "left_spot",
                    "observed_at": OBSERVED_AT,
                    "snapshot_path": "/data/latest.jpg",
                }
            ],
            "matrix_delivery": {"attempt_count": 1, "succeeded_count": 1, "failed_count": 0},
            "matrix_snapshot_copied_count": 1,
            "matrix_snapshots": [
                {
                    "event_type": "occupancy-open-event",
                    "spot_id": "left_spot",
                    "snapshot_path": SNAPSHOT_PATH,
                    "width": 8,
                    "height": 6,
                }
            ],
            "duplicates": {"event_ids": {}, "txn_ids": {}},
        },
        "alert_summary": {
            "organic_alert_count": 1,
            "delivery_attempt_count": 1,
            "delivery_succeeded_count": 1,
            "delivery_failed_count": 0,
            "snapshot_copied_count": 1,
            "readback_status": "verified",
        },
        "duplicate_summary": {"event_ids": {}, "txn_ids": {}},
        "artifact_summary": {
            "latest_jpeg": {"path": "data/latest.jpg", "exists": True, "byte_size": 123, "valid_jpeg": True, "format": "JPEG"},
            "event_snapshot_jpegs": {
                "count": 1,
                "summarized_count": 1,
                "valid_count": 1,
                "files": [
                    {"path": SNAPSHOT_PATH, "exists": True, "byte_size": 456, "valid_jpeg": True, "format": "JPEG"}
                ],
            },
        },
        "health_summary": {"path": "data/health.json", "exists": True, "parse_ok": True, "status": "ok", "iteration": 10},
        "state_summary": {"path": "data/state.json", "exists": True, "parse_ok": True, "spot_count": 1},
        "room_readback_status": "verified",
        "room_readback": {
            "status": "verified",
            "alerts_checked": 1,
            "inspected_count": 4,
            "per_alert": [{"event_id": EVENT_ID, "spot_id": "left_spot", "text_found": True, "image_found": True}],
        },
        "redaction_scan": {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 2},
    }


def run_main(tmp_path: Path, payload: dict[str, Any], *extra: str) -> tuple[int, str]:
    result_path = tmp_path / "alert-soak-result.json"
    evidence_path = tmp_path / "alert-soak-evidence.md"
    write_result(result_path, payload)
    exit_code = verifier.main(["--result", str(result_path), "--evidence", str(evidence_path), *extra])
    return exit_code, evidence_path.read_text(encoding="utf-8")


def test_successful_organic_alert_report_passes_and_is_publication_safe(tmp_path: Path) -> None:
    exit_code, report = run_main(tmp_path, base_success())

    assert exit_code == 0
    assert "Verification state: `success`" in report
    assert "Organic alerts: `1`" in report
    assert "Matrix readback: `verified`" in report
    assert "Duplicate event IDs: none" in report
    assert "Requirement status: S08 strict live soak validation complete" in report
    assert "event_id" not in report
    assert "rtsp://" not in report.lower()


def test_unsupported_status_fails_closed_and_writes_failure_report(tmp_path: Path) -> None:
    payload = base_success() | {"status": "maybe_ok"}

    exit_code, report = run_main(tmp_path, payload)

    assert exit_code == 1
    assert "unsupported alert-soak status" in report
    assert "Verification state: `verifier_error`" in report


def test_malformed_json_fails_closed_and_writes_evidence(tmp_path: Path) -> None:
    result_path = tmp_path / "alert-soak-result.json"
    evidence_path = tmp_path / "alert-soak-evidence.md"
    result_path.write_text("{not-json", encoding="utf-8")

    exit_code = verifier.main(["--result", str(result_path), "--evidence", str(evidence_path)])

    assert exit_code == 1
    report = evidence_path.read_text(encoding="utf-8")
    assert "result JSON is malformed" in report


def test_preflight_blocker_fails_strict_and_names_missing_inputs_only(tmp_path: Path) -> None:
    payload = {
        "status": "preflight_failed",
        "phase": "preflight",
        "missing_inputs": ["config.yaml", "RTSP_URL", "MATRIX_TOKEN_ENV"],
        "docker": {"attempted": False, "exit_code": None},
        "redaction_scan": {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 0},
    }

    exit_code, report = run_main(tmp_path, payload)

    assert exit_code == 1
    assert "Verification state: `preflight_blocked`" in report
    assert "Missing input names: config.yaml, RTSP_URL, Matrix token env key" in report
    assert "MATRIX_TOKEN_ENV" not in report


def test_preflight_blocker_flag_allows_names_only_handoff_report(tmp_path: Path) -> None:
    payload = {
        "status": "preflight_failed",
        "phase": "preflight",
        "missing_inputs": ["matrix.room_id"],
        "docker": {"attempted": False, "exit_code": None},
        "redaction_scan": {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 0},
    }

    exit_code, report = run_main(tmp_path, payload, "--allow-preflight-blocker")

    assert exit_code == 0
    assert "Requirement status: S08 strict live soak validation blocked by preflight" in report
    assert "No Docker alert soak was attempted" in report


def test_no_alert_strict_failure_and_allowed_coverage_gap_report(tmp_path: Path) -> None:
    payload = base_success()
    payload.update({"status": "coverage_gap", "phase": "alert_detection", "room_readback_status": "not_applicable"})
    payload["log_summary"]["organic_alert_count"] = 0
    payload["log_summary"]["organic_alerts"] = []
    payload["alert_summary"]["organic_alert_count"] = 0
    payload["room_readback"] = {"status": "not_applicable", "alerts_checked": 0, "per_alert": []}

    strict_exit, strict_report = run_main(tmp_path, payload)
    allowed_exit, allowed_report = run_main(tmp_path, payload, "--allow-coverage-gap")

    assert strict_exit == 1
    assert "coverage_gap_no_alert" in strict_report
    assert allowed_exit == 0
    assert "Verification state: `coverage_gap_no_alert`" in allowed_report
    assert "S08 strict live soak validation remains incomplete" in allowed_report


def test_duplicate_txn_failure(tmp_path: Path) -> None:
    payload = base_success()
    payload["duplicate_summary"] = {"event_ids": {}, "txn_ids": {"txn-1": 2}}
    payload["log_summary"]["duplicates"] = payload["duplicate_summary"]

    exit_code, report = run_main(tmp_path, payload)

    assert exit_code == 1
    assert "duplicate Matrix transaction IDs detected" in report


def test_missing_snapshot_failure(tmp_path: Path) -> None:
    payload = base_success()
    payload["artifact_summary"]["event_snapshot_jpegs"] = {"count": 1, "summarized_count": 1, "valid_count": 0, "files": []}

    exit_code, report = run_main(tmp_path, payload)

    assert exit_code == 1
    assert "matching valid occupancy-open-event snapshot" in report


def test_redaction_hit_failure(tmp_path: Path) -> None:
    payload = base_success()
    payload["redaction_scan"] = {"secret_occurrences": 0, "forbidden_pattern_occurrences": 1, "redaction_replacements": 2}

    exit_code, report = run_main(tmp_path, payload)

    assert exit_code == 1
    assert "redaction scan must have zero hits" in report


def test_alias_inconsistency_fails_closed(tmp_path: Path) -> None:
    payload = base_success()
    payload["alerts"] = {"organic_alert_count": 99}

    exit_code, report = run_main(tmp_path, payload)

    assert exit_code == 1
    assert "inconsistent alerts/alert_summary values" in report


def test_bare_verifier_invocation_imports_package_from_repository_root(tmp_path: Path) -> None:
    result_path = tmp_path / "alert-soak-result.json"
    evidence_path = tmp_path / "alert-soak-evidence.md"
    write_result(result_path, base_success())

    completed = subprocess.run(
        [sys.executable, "scripts/verify_alert_soak.py", "--result", str(result_path), "--evidence", str(evidence_path)],
        check=False,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )

    assert completed.returncode == 0
    assert "ModuleNotFoundError" not in completed.stderr
    assert "Verification state: `success`" in evidence_path.read_text(encoding="utf-8")
