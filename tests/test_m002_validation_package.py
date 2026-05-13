from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from scripts import assemble_m002_validation_package as assembler

SECRET_RTSP = "rtsp://user:secret@example.test/live"
SECRET_TOKEN = "syt_secretsecret"
RAW_MATRIX_BODY = "LIVE PROOF / TEST MESSAGE raw private body"


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def replay_report(**overrides: Any) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": "parking-spot-monitor.replay-report.v1",
        "status_counts": {"passed": 2, "failed": 0, "blocked": 0, "not_covered": 0},
        "shared_threshold_sufficiency": {"verdict": "sufficient"},
        "redaction_scan": {"passed": True, "secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "findings": []},
    }
    report.update(overrides)
    return report


def tuning_report(**overrides: Any) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": "parking-spot-monitor.tuning-report.v1",
        "decision": "keep_shared_thresholds",
        "decision_rationale": "proposed shared thresholds do not improve false-positive/false-negative evidence",
        "status_counts": {
            "baseline": {"passed": 1, "failed": 0, "blocked": 0, "not_covered": 0},
            "proposed": {"passed": 1, "failed": 0, "blocked": 0, "not_covered": 0},
        },
        "blocked_reasons": [],
        "not_covered_reasons": [],
        "redaction_scan": {"passed": True, "secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "findings": []},
    }
    report.update(overrides)
    return report



def s07_report(**overrides: Any) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": "parking-spot-monitor.s07-evidence-report.v1",
        "status": "passed",
        "required_tags": ["real_capture", "bottom_driveway", "passing_traffic", "threshold_decision"],
        "tag_coverage": {"real_capture": True, "bottom_driveway": True, "passing_traffic": True, "threshold_decision": True},
        "missing_tags": [],
        "gap_reasons": [],
        "blocker_reasons": [],
        "evidence_accounting": {"totals": {"tp": 1, "tn": 1, "fp": 0, "fn": 0, "blocked": 0, "not_assessed": 0}},
        "tuning_decision": "keep_shared_thresholds",
        "tuning_status": "safe",
        "threshold_verdict": "shared_thresholds_sufficient",
        "redaction_scan": {"passed": True, "secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "findings": []},
        "replay_redaction": {"passed": True, "findings": [], "reason": "no_forbidden_report_content"},
    }
    report.update(overrides)
    return report

def live_proof_success(**overrides: Any) -> dict[str, Any]:
    report: dict[str, Any] = {
        "status": "success",
        "docker_exit_code": 0,
        "marker_checks": {"required_present": True, "forbidden_present": [], "missing_required": []},
        "artifact_checks": {
            "latest_jpeg": {"path": "data/latest.jpg", "exists": True, "valid_jpeg": True},
            "snapshot_jpegs": {"count": 1, "valid_count": 1},
        },
        "room_readback_status": "verified",
        "room_readback": {"status": "verified", "text_found": True, "image_found": True, "body": RAW_MATRIX_BODY},
        "redaction_scan": {"secret_occurrences": 0, "redaction_replacements": 2},
    }
    report.update(overrides)
    return report


def alert_soak_success(**overrides: Any) -> dict[str, Any]:
    report: dict[str, Any] = {
        "status": "success",
        "phase": "complete",
        "docker": {"attempted": True, "exit_code": -15, "timed_out": True, "terminated": True, "killed": False, "expected_timeout_completion": True},
        "log_summary": {
            "organic_alert_count": 1,
            "organic_alerts": [{"event_id": "occupancy-open-event:left:now", "spot_id": "left_spot"}],
            "duplicates": {"event_ids": {}, "txn_ids": {}},
        },
        "alert_summary": {"organic_alert_count": 1},
        "duplicate_summary": {"event_ids": {}, "txn_ids": {}},
        "artifact_summary": {
            "latest_jpeg": {"exists": True, "valid_jpeg": True},
            "event_snapshot_jpegs": {"count": 1, "valid_count": 1, "files": [{"path": "data/snapshots/occupancy-open-event-left-spot.jpg", "valid_jpeg": True}]}, 
        },
        "health_summary": {"exists": True, "parse_ok": True, "status": "ok"},
        "state_summary": {"exists": True, "parse_ok": True, "spot_count": 1},
        "room_readback_status": "verified",
        "room_readback": {"status": "verified", "alerts_checked": 1, "per_alert": [{"text_found": True, "image_found": True, "body": RAW_MATRIX_BODY}]},
        "redaction_scan": {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 2},
    }
    report.update(overrides)
    return report


def write_all_inputs(tmp_path: Path) -> dict[str, Path]:
    return {
        "replay": write_json(tmp_path / "replay-report.json", replay_report()),
        "tuning": write_json(tmp_path / "tuning-report.json", tuning_report()),
        "live": write_json(tmp_path / "live-proof-result.json", live_proof_success()),
        "alert": write_json(tmp_path / "alert-soak-result.json", alert_soak_success()),
        "s07": write_json(tmp_path / "s07-evidence-report.json", s07_report()),
    }


def read_package(output_dir: Path) -> dict[str, Any]:
    return json.loads((output_dir / "m002-validation-package.json").read_text(encoding="utf-8"))


def assert_publication_safe(text: str) -> None:
    for forbidden in [SECRET_RTSP, SECRET_TOKEN, RAW_MATRIX_BODY, "Authorization", "Bearer", "access_token", "Traceback"]:
        assert forbidden.lower() not in text.lower()


def test_cli_uses_default_local_evidence_paths() -> None:
    args = assembler.build_parser().parse_args([])

    assert args.replay_report == "data/s07-replay-evidence/replay/replay-report.json"
    assert args.tuning_report == "data/s07-replay-evidence/tuning/tuning-report.json"
    assert args.live_proof_result == "data/live-proof-result.json"
    assert args.alert_soak_result == "data/alert-soak-result.json"
    assert args.s07_evidence_report == "data/s07-replay-evidence/coverage/s07-evidence-report.json"
    assert args.output_dir == "data/m002-validation"



def test_strict_pass_cli_writes_json_and_markdown_without_raw_evidence(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    output_dir = tmp_path / "validation"

    exit_code = assembler.main(
        [
            "--replay-report",
            str(paths["replay"]),
            "--tuning-report",
            str(paths["tuning"]),
            "--live-proof-result",
            str(paths["live"]),
            "--alert-soak-result",
            str(paths["alert"]),
            "--s07-evidence-report",
            str(paths["s07"]),
            "--output-dir",
            str(output_dir),
        ]
    )

    package = read_package(output_dir)
    markdown = (output_dir / "m002-validation-package.md").read_text(encoding="utf-8")
    assert exit_code == 0
    assert package["final_status"] == "validated"
    assert package["evidence"]["replay"]["status"] == "passed"
    assert package["evidence"]["tuning"]["status"] == "passed"
    assert package["evidence"]["live_proof"]["status"] == "passed"
    assert package["evidence"]["alert_soak"]["status"] == "passed"
    assert package["evidence"]["s07_coverage"]["status"] == "passed"
    assert package["requirement_coverage"]["R018"]["status"] == "passed"
    assert package["requirement_coverage"]["R019"]["status"] == "validated"
    assert package["requirement_coverage"]["R028"]["status"] == "passed"
    assert package["requirement_coverage"]["R020"]["status"] == "deferred"
    assert package["requirement_coverage"]["R021"]["status"] == "out_of_scope"
    assert package["requirement_coverage"]["R022"]["status"] == "out_of_scope"
    assert package["no_change_decision"] == "keep_shared_thresholds"
    assert "R003/R015 validated" in package["requirement_implications"]
    assert "S08 strict live soak validation complete" in package["requirement_implications"]
    assert "Final status: `validated`" in markdown
    assert "## Requirement Coverage" in markdown
    assert "### R019" in markdown
    assert "keep_shared_thresholds" in markdown
    assert_publication_safe(json.dumps(package) + markdown)


def test_missing_path_blocks_with_safe_path_label(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    missing = tmp_path / "missing-replay-report.json"

    package = assembler.assemble_package(
        replay_report_path=missing,
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=paths["s07"],
        allow_residual_risk=None,
    )

    assert package["final_status"] == "blocked"
    assert package["evidence"]["replay"]["status"] == "blocked"
    assert package["evidence"]["replay"]["reason"] == "artifact path is missing"
    assert package["evidence"]["replay"]["artifact"] == str(missing)


def test_malformed_json_blocks_with_parser_category_only(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    paths["replay"].write_text(f"{{not json {SECRET_RTSP} {SECRET_TOKEN}", encoding="utf-8")

    package = assembler.assemble_package(
        replay_report_path=paths["replay"],
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=paths["s07"],
        allow_residual_risk=None,
    )

    assert package["final_status"] == "blocked"
    assert package["evidence"]["replay"]["status"] == "blocked"
    assert package["evidence"]["replay"]["reason"] == "artifact JSON is malformed"
    assert "not json" not in json.dumps(package)
    assert_publication_safe(json.dumps(package))



def test_s07_coverage_gap_keeps_r018_r019_r028_unvalidated(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    write_json(
        paths["s07"],
        s07_report(
            status="coverage_gap",
            tag_coverage={"real_capture": True, "bottom_driveway": False, "passing_traffic": True, "threshold_decision": True},
            missing_tags=["bottom_driveway"],
            gap_reasons=["missing required semantic tags: bottom_driveway"],
        ),
    )

    package = assembler.assemble_package(
        replay_report_path=paths["replay"],
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=paths["s07"],
        allow_residual_risk=None,
    )

    assert package["final_status"] == "coverage_gap"
    assert package["evidence"]["s07_coverage"]["status"] == "coverage_gap"
    assert package["requirement_coverage"]["R018"]["status"] == "coverage_gap"
    assert package["requirement_coverage"]["R019"]["status"] == "coverage_gap"
    assert package["requirement_coverage"]["R028"]["status"] == "coverage_gap"
    assert package["requirement_coverage"]["R028"]["blocked_or_deferred_reason"] == "missing bottom_driveway semantic tag"
    assert package["requirement_coverage"]["R019"]["rationale"] == "Tuning decision is acceptable only at smoke level until strict S07 threshold-decision evidence passes."
    s10_boundary = package["validation_boundaries"]["s10_strict_replay_gap"]
    assert s10_boundary["status"] == "evidence_gap"
    assert s10_boundary["blocked_requirements"] == ["R018", "R019", "R028"]
    assert s10_boundary["acquisition_contract"] == "data/s07-replay-evidence/evidence-notes.md"
    assert "bottom_driveway" in s10_boundary["missing_semantic_tags"]
    s11_boundary = package["validation_boundaries"]["s11_live_proof_boundary"]
    assert s11_boundary["status"] == "separate_responsibility"
    assert "S11 remains responsible" in s11_boundary["summary"]
    markdown = assembler.render_markdown(package)
    assert "S10 strict semantic replay evidence is missing" in markdown
    assert "S11 remains responsible" in markdown
    assert "evidence-notes.md" in markdown
    assert_publication_safe(json.dumps(package) + markdown)


def test_missing_s07_report_blocks_requirement_coverage_safely(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    missing = tmp_path / "missing-s07-evidence-report.json"

    package = assembler.assemble_package(
        replay_report_path=paths["replay"],
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=missing,
        allow_residual_risk=None,
    )

    assert package["final_status"] == "blocked"
    assert package["evidence"]["s07_coverage"]["status"] == "blocked"
    assert package["evidence"]["s07_coverage"]["reason"] == "artifact path is missing"
    assert package["requirement_coverage"]["R018"]["status"] == "blocked"
    assert package["requirement_coverage"]["R019"]["status"] == "coverage_gap"
    assert package["requirement_coverage"]["R028"]["status"] == "blocked"
    assert_publication_safe(json.dumps(package))


def test_malformed_s07_report_blocks_without_leaking_raw_content(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    paths["s07"].write_text(f"{{bad s07 json {SECRET_RTSP} {SECRET_TOKEN}", encoding="utf-8")

    package = assembler.assemble_package(
        replay_report_path=paths["replay"],
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=paths["s07"],
        allow_residual_risk=None,
    )

    rendered = assembler.render_markdown(package)
    assert package["final_status"] == "blocked"
    assert package["evidence"]["s07_coverage"]["reason"] == "artifact JSON is malformed"
    assert "bad s07 json" not in json.dumps(package)
    assert "bad s07 json" not in rendered
    assert_publication_safe(json.dumps(package) + rendered)


def test_requirement_coverage_publication_scan_catches_forbidden_text(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    package = assembler.assemble_package(
        replay_report_path=paths["replay"],
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=paths["s07"],
        allow_residual_risk=None,
    )
    package["requirement_coverage"]["R020"]["blocked_or_deferred_reason"] = SECRET_TOKEN

    safety = assembler.scan_publication_safety(assembler.render_markdown(package))

    assert safety["passed"] is False
    assert safety["redaction_hits"] > 0

def test_redaction_failure_fails_final_package(tmp_path: Path) -> None:
    report = replay_report(redaction_scan={"passed": False, "secret_occurrences": 1, "forbidden_pattern_occurrences": 0, "findings": ["rtsp_url"]})

    evidence = assembler.classify_replay(report, artifact_label="replay-report.json")

    assert evidence["status"] == "failed"
    assert evidence["publication_safety"]["redaction_hits"] == 1
    assert evidence["reason"] == "replay report redaction scan failed"


def test_alert_soak_no_organic_alert_is_coverage_gap(tmp_path: Path) -> None:
    payload = alert_soak_success(status="coverage_gap_no_alert", alert_summary={"organic_alert_count": 0}, log_summary={"organic_alert_count": 0, "organic_alerts": [], "duplicates": {"event_ids": {}, "txn_ids": {}}}, room_readback={"status": "not_applicable", "alerts_checked": 0, "per_alert": []})

    evidence = assembler.classify_alert_soak(payload, artifact_label="alert-soak-result.json")

    assert evidence["status"] == "coverage_gap"
    assert evidence["reason"] == "no organic occupancy-open-event alerts were observed"
    assert "S08 strict live soak validation remains incomplete" in evidence["requirement_implications"]


def test_alert_soak_preflight_blocker_is_not_package_passed(tmp_path: Path) -> None:
    payload = {
        "status": "preflight_failed",
        "phase": "preflight",
        "missing_inputs": ["config.yaml", "RTSP_URL", "MATRIX_TOKEN_ENV"],
        "docker": {"attempted": False, "exit_code": None},
        "redaction_scan": {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 0},
    }

    evidence = assembler.classify_alert_soak(payload, artifact_label="alert-soak-result.json")

    assert evidence["status"] == "blocked"
    assert evidence["verifier_state"] == "preflight_blocked"
    assert evidence["missing_inputs"] == ["config.yaml", "RTSP_URL", "MATRIX_TOKEN_ENV"]
    assert "complete" not in evidence["requirement_implications"]
    assert_publication_safe(json.dumps(evidence))


def test_preflight_blocked_alert_soak_builds_explicit_s11_boundary(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    write_json(
        paths["alert"],
        {
            "status": "preflight_failed",
            "phase": "preflight",
            "missing_inputs": ["RTSP_URL", "MATRIX_TOKEN_ENV"],
            "docker": {"attempted": False, "exit_code": None},
            "redaction_scan": {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 0},
        },
    )

    package = assembler.assemble_package(
        replay_report_path=paths["replay"],
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=paths["s07"],
        allow_residual_risk=None,
    )

    boundary = package["validation_boundaries"]["s11_alert_soak_boundary"]
    assert package["final_status"] == "blocked"
    assert package["evidence"]["alert_soak"]["status"] == "blocked"
    assert boundary["status"] == "preflight_blocked"
    assert boundary["verifier_state"] == "preflight_blocked"
    assert boundary["blocked_requirements"] == ["R003", "R015"]
    assert boundary["missing_inputs"] == ["RTSP_URL", "MATRIX_TOKEN_ENV"]
    assert "cannot validate strict Matrix/RTSP alert-soak success" in boundary["summary"]
    assert "strict live alert-soak evidence is documentation-only" in boundary["live_soak_implication"]
    assert_publication_safe(json.dumps(package) + assembler.render_markdown(package))


def test_strict_passed_alert_soak_builds_success_s11_boundary(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)

    package = assembler.assemble_package(
        replay_report_path=paths["replay"],
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=paths["s07"],
        allow_residual_risk=None,
    )

    boundary = package["validation_boundaries"]["s11_alert_soak_boundary"]
    assert package["final_status"] == "validated"
    assert boundary["status"] == "strict_success"
    assert boundary["verifier_state"] == "success"
    assert boundary["blocked_requirements"] == []
    assert "strict Matrix/RTSP alert-soak success is available" in boundary["summary"]
    assert boundary["missing_inputs"] == []
    assert_publication_safe(json.dumps(package) + assembler.render_markdown(package))


def test_alert_soak_readback_duplicate_and_redaction_gaps_are_not_package_passed(tmp_path: Path) -> None:
    cases = [
        (
            alert_soak_success(room_readback_status="gap", room_readback={"status": "gap", "alerts_checked": 1, "per_alert": [{"text_found": True, "image_found": False, "body": RAW_MATRIX_BODY}]}),
            "failed",
            "verifier_error",
        ),
        (
            alert_soak_success(duplicate_summary={"event_ids": {}, "txn_ids": {"txn-1": 2}}, log_summary={"organic_alert_count": 1, "organic_alerts": [{"event_id": "occupancy-open-event:left:now", "spot_id": "left_spot"}], "duplicates": {"event_ids": {}, "txn_ids": {"txn-1": 2}}}),
            "failed",
            "verifier_error",
        ),
        (
            alert_soak_success(redaction_scan={"secret_occurrences": 0, "forbidden_pattern_occurrences": 1, "redaction_replacements": 2}),
            "failed",
            "verifier_error",
        ),
    ]

    for payload, expected_status, expected_state in cases:
        evidence = assembler.classify_alert_soak(payload, artifact_label="alert-soak-result.json")

        assert evidence["status"] == expected_status
        assert evidence["verifier_state"] == expected_state
        assert evidence["status"] != "passed"
        assert_publication_safe(json.dumps(evidence))


def test_live_proof_verifier_failure_is_failed_with_safe_reason(tmp_path: Path) -> None:
    payload = live_proof_success(status="success", room_readback_status="gap", room_readback={"status": "gap", "text_found": False, "image_found": False, "body": RAW_MATRIX_BODY})

    evidence = assembler.classify_live_proof(payload, artifact_label="live-proof-result.json")

    assert evidence["status"] == "failed"
    assert evidence["reason"] == "success requires Matrix room readback verification"
    assert_publication_safe(json.dumps(evidence))


def test_tuning_needs_per_spot_thresholds_blocks_r019_followup() -> None:
    evidence = assembler.classify_tuning(tuning_report(decision="needs_per_spot_thresholds"), artifact_label="tuning-report.json")

    assert evidence["status"] == "blocked"
    assert evidence["reason"] == "per-spot threshold follow-up required before M002 closure"
    assert "R019 follow-up required" in evidence["requirement_implications"]


def test_replay_blocked_and_not_covered_evidence_is_not_validated() -> None:
    blocked = assembler.classify_replay(replay_report(status_counts={"passed": 0, "failed": 0, "blocked": 1, "not_covered": 0}), artifact_label="replay-report.json")
    not_covered = assembler.classify_replay(replay_report(status_counts={"passed": 0, "failed": 0, "blocked": 0, "not_covered": 1}), artifact_label="replay-report.json")

    assert blocked["status"] == "blocked"
    assert blocked["reason"] == "replay evidence contains blocked cases"
    assert not_covered["status"] == "coverage_gap"
    assert not_covered["reason"] == "replay evidence is not fully covered"


def test_residual_risk_acceptance_produces_non_validated_final_status(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    write_json(paths["alert"], alert_soak_success(status="coverage_gap_no_alert", alert_summary={"organic_alert_count": 0}, log_summary={"organic_alert_count": 0, "organic_alerts": [], "duplicates": {"event_ids": {}, "txn_ids": {}}}, room_readback={"status": "not_applicable", "alerts_checked": 0, "per_alert": []}))

    package = assembler.assemble_package(
        replay_report_path=paths["replay"],
        tuning_report_path=paths["tuning"],
        live_proof_result_path=paths["live"],
        alert_soak_result_path=paths["alert"],
        s07_evidence_report_path=paths["s07"],
        allow_residual_risk="Accept bounded no-alert soak gap; rerun when an organic opening occurs.",
    )

    assert package["final_status"] == "residual_risk_accepted"
    assert package["residual_risk"] == "Accept bounded no-alert soak gap; rerun when an organic opening occurs."
    assert package["evidence"]["alert_soak"]["status"] == "coverage_gap"


def test_bare_script_invocation_imports_from_repository_root(tmp_path: Path) -> None:
    paths = write_all_inputs(tmp_path)
    output_dir = tmp_path / "validation"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/assemble_m002_validation_package.py",
            "--replay-report",
            str(paths["replay"]),
            "--tuning-report",
            str(paths["tuning"]),
            "--live-proof-result",
            str(paths["live"]),
            "--alert-soak-result",
            str(paths["alert"]),
            "--s07-evidence-report",
            str(paths["s07"]),
            "--output-dir",
            str(output_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )

    assert completed.returncode == 0
    assert "ModuleNotFoundError" not in completed.stderr
    assert read_package(output_dir)["final_status"] == "validated"
