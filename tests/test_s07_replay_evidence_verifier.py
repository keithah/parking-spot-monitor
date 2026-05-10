from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts import verify_s07_replay_evidence as verifier

SECRET_RTSP = "rtsp://user:secret@example.test/live"
SECRET_TOKEN = "syt_secretsecret"


def replay_report(**overrides: Any) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": "parking-spot-monitor.replay-report.v1",
        "spot_ids": ["left_spot", "right_spot"],
        "status_counts": {"passed": 1, "failed": 0, "blocked": 0, "not_covered": 0},
        "cases": [
            {
                "case_id": "case-1",
                "tags": ["real_capture", "bottom_driveway"],
                "scenario_tags": {"driveway": ["passing_traffic", "threshold_decision"]},
                "status": "passed",
                "blocked_reasons": [],
                "not_covered_reasons": [],
            }
        ],
        "metrics_by_spot": {
            "left_spot": {"tp": 1, "tn": 1, "fp": 0, "fn": 0, "blocked": 0, "not_assessed": 0},
            "right_spot": {"tp": 0, "tn": 2, "fp": 0, "fn": 0, "blocked": 0, "not_assessed": 0},
        },
        "coverage": {"assessed_frames": 2, "blocked_frames": 0, "not_assessed_frames": 0, "blocked_reasons": [], "not_covered_reasons": []},
        "redaction_scan": {"passed": True, "findings": [], "reason": "no_forbidden_report_content"},
    }
    report.update(overrides)
    return report


def tuning_report(**overrides: Any) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": "parking-spot-monitor.tuning-report.v1",
        "decision": "keep_shared_thresholds",
        "decision_rationale": "synthetic evidence supports shared thresholds",
        "status_counts": {
            "baseline": {"passed": 1, "failed": 0, "blocked": 0, "not_covered": 0},
            "proposed": {"passed": 1, "failed": 0, "blocked": 0, "not_covered": 0},
        },
        "metric_deltas": {"totals": {"tp": 0, "tn": 0, "fp": 0, "fn": 0, "blocked": 0, "not_assessed": 0}, "by_spot": {}},
        "blocked_reasons": [],
        "not_covered_reasons": [],
        "redaction_scan": {"passed": True, "findings": [], "reason": "no_forbidden_report_content"},
    }
    report.update(overrides)
    return report


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def invoke(tmp_path: Path, replay: dict[str, Any] | None = None, tuning: dict[str, Any] | None = None) -> tuple[int, Path, Path]:
    replay_path = tmp_path / "replay-report.json"
    tuning_path = tmp_path / "tuning-report.json"
    if replay is not None:
        write_json(replay_path, replay)
    if tuning is not None:
        write_json(tuning_path, tuning)
    output_dir = tmp_path / "reports"
    exit_code = verifier.main([
        "--replay-report",
        str(replay_path),
        "--tuning-report",
        str(tuning_path),
        "--output-dir",
        str(output_dir),
    ])
    return exit_code, output_dir / "s07-evidence-report.json", output_dir / "s07-evidence-report.md"


def load_report(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def assert_publication_safe(text: str) -> None:
    assert SECRET_RTSP not in text
    assert SECRET_TOKEN not in text
    assert "Traceback" not in text
    assert "data:image/" not in text
    assert "m.room.message" not in text


def test_passes_when_required_tags_spots_redaction_and_shared_tuning_are_safe(tmp_path: Path, capsys) -> None:
    exit_code, json_path, markdown_path = invoke(tmp_path, replay_report(), tuning_report())

    captured = capsys.readouterr()
    assert exit_code == 0
    summary = json.loads(captured.out)
    assert summary["status"] == "passed"
    report = load_report(json_path)
    assert report["status"] == "passed"
    assert report["missing_tags"] == []
    assert report["replay_redaction"]["passed"] is True
    assert report["threshold_verdict"] == "shared_thresholds_sufficient"
    assert report["evidence_accounting"]["totals"]["fp"] == 0
    assert report["evidence_accounting"]["totals"]["fn"] == 0
    assert report["per_spot_coverage"]["left_spot"]["has_assessed_evidence"] is True
    assert report["per_spot_coverage"]["right_spot"]["has_assessed_evidence"] is True
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "# S07 Replay/Tuning Evidence Report" in markdown
    assert "Status: **passed**" in markdown
    assert_publication_safe(json_path.read_text(encoding="utf-8") + markdown + captured.out + captured.err)


def test_missing_required_tag_is_coverage_gap_not_blocked(tmp_path: Path) -> None:
    replay = replay_report(cases=[{"case_id": "case-1", "tags": ["real_capture"], "scenario_tags": {"driveway": ["bottom_driveway", "passing_traffic"]}, "status": "passed"}])

    exit_code, json_path, _ = invoke(tmp_path, replay, tuning_report())

    assert exit_code == 0
    report = load_report(json_path)
    assert report["status"] == "coverage_gap"
    assert report["missing_tags"] == ["threshold_decision"]
    assert report["blocker_reasons"] == []
    assert any("missing required semantic tags" in reason for reason in report["gap_reasons"])


def test_single_spot_only_evidence_is_coverage_gap(tmp_path: Path) -> None:
    replay = replay_report(
        spot_ids=["left_spot", "right_spot"],
        metrics_by_spot={
            "left_spot": {"tp": 1, "tn": 0, "fp": 0, "fn": 0, "blocked": 0, "not_assessed": 0},
            "right_spot": {"tp": 0, "tn": 0, "fp": 0, "fn": 0, "blocked": 0, "not_assessed": 2},
        },
    )

    exit_code, json_path, _ = invoke(tmp_path, replay, tuning_report())

    assert exit_code == 0
    report = load_report(json_path)
    assert report["status"] == "coverage_gap"
    assert report["per_spot_coverage"]["right_spot"]["has_assessed_evidence"] is False
    assert any("right_spot" in reason for reason in report["gap_reasons"])


def test_replay_redaction_findings_block_publication(tmp_path: Path) -> None:
    replay = replay_report(redaction_scan={"passed": False, "findings": ["rtsp_url"], "reason": "forbidden_report_content_detected"})

    exit_code, json_path, markdown_path = invoke(tmp_path, replay, tuning_report())

    assert exit_code == 0
    report = load_report(json_path)
    assert report["status"] == "blocked"
    assert "replay report redaction scan did not pass" in report["blocker_reasons"]
    assert_publication_safe(json_path.read_text(encoding="utf-8") + markdown_path.read_text(encoding="utf-8"))


def test_blocked_replay_case_blocks_even_when_tags_are_present(tmp_path: Path) -> None:
    replay = replay_report(cases=[{"case_id": "case-blocked", "tags": ["real_capture", "bottom_driveway"], "scenario_tags": {"driveway": ["passing_traffic", "threshold_decision"]}, "status": "blocked", "blocked_reasons": ["missing_detector_data"]}])

    exit_code, json_path, _ = invoke(tmp_path, replay, tuning_report())

    assert exit_code == 0
    report = load_report(json_path)
    assert report["status"] == "blocked"
    assert report["blocked_cases"] == [{"case_id": "case-blocked", "reasons": ["missing_detector_data"]}]
    assert "replay report contains blocked cases" in report["blocker_reasons"]


def test_needs_per_spot_thresholds_blocks_with_remediation_guidance(tmp_path: Path) -> None:
    tuning = tuning_report(decision="needs_per_spot_thresholds", decision_rationale="residual errors diverge by spot")

    exit_code, json_path, markdown_path = invoke(tmp_path, replay_report(), tuning)

    assert exit_code == 0
    report = load_report(json_path)
    assert report["status"] == "blocked"
    assert report["threshold_verdict"] == "per_spot_thresholds_required"
    assert any("per-spot thresholds" in reason for reason in report["blocker_reasons"])
    assert "per_spot_thresholds_required" in markdown_path.read_text(encoding="utf-8")


def test_invalid_json_emits_blocked_reports_with_file_name_only(tmp_path: Path) -> None:
    replay_path = tmp_path / "replay-report.json"
    tuning_path = tmp_path / "tuning-report.json"
    replay_path.write_text(f'{{"bad":  # {SECRET_RTSP} {SECRET_TOKEN}', encoding="utf-8")
    write_json(tuning_path, tuning_report())
    output_dir = tmp_path / "reports"

    exit_code = verifier.main([
        "--replay-report",
        str(replay_path),
        "--tuning-report",
        str(tuning_path),
        "--output-dir",
        str(output_dir),
    ])

    assert exit_code == 0
    report_text = (output_dir / "s07-evidence-report.json").read_text(encoding="utf-8")
    markdown = (output_dir / "s07-evidence-report.md").read_text(encoding="utf-8")
    report = json.loads(report_text)
    assert report["status"] == "blocked"
    assert any("REPORT_INVALID_JSON" in reason and "replay-report.json" in reason for reason in report["blocker_reasons"])
    assert str(replay_path.parent) not in report_text
    assert_publication_safe(report_text + markdown)


def test_missing_report_file_still_emits_blocked_json_and_markdown(tmp_path: Path) -> None:
    exit_code, json_path, markdown_path = invoke(tmp_path, replay=None, tuning=tuning_report())

    assert exit_code == 0
    report = load_report(json_path)
    assert report["status"] == "blocked"
    assert any("REPORT_NOT_FOUND" in reason and "replay-report.json" in reason for reason in report["blocker_reasons"])
    assert markdown_path.is_file()


def test_replay_only_workflow_smoke_missing_real_semantics_fails_closed(tmp_path: Path) -> None:
    replay = replay_report(
        cases=[
            {
                "case_id": "workflow-smoke",
                "tags": ["operator_derived", "workflow_smoke", "threshold_decision"],
                "scenario_tags": {"smoke": ["threshold_decision", "insufficient_real_semantic_coverage"]},
                "status": "passed",
                "blocked_reasons": [],
                "not_covered_reasons": [],
            }
        ]
    )
    replay_path = write_json(tmp_path / "replay-report.json", replay)
    output_dir = tmp_path / "reports"

    exit_code = verifier.main([
        "--replay-report",
        str(replay_path),
        "--output-dir",
        str(output_dir),
    ])

    assert exit_code == 0
    report = load_report(output_dir / "s07-evidence-report.json")
    assert report["status"] == "blocked"
    assert report["tag_coverage"]["threshold_decision"] is True
    assert report["tag_coverage"]["real_capture"] is False
    assert report["tag_coverage"]["bottom_driveway"] is False
    assert report["tag_coverage"]["passing_traffic"] is False
    assert report["per_spot_coverage"]["left_spot"]["has_assessed_evidence"] is False
    assert report["per_spot_coverage"]["right_spot"]["has_assessed_evidence"] is False
    assert any("missing real semantic evidence tags" in reason for reason in report["blocker_reasons"])
    assert any("tuning report not provided" in reason for reason in report["blockers"])
    assert any("semantic" in finding for finding in report["findings"])
    assert_publication_safe((output_dir / "s07-evidence-report.md").read_text(encoding="utf-8"))


def test_missing_required_keys_blocks_as_malformed_report(tmp_path: Path) -> None:
    replay = {"schema_version": "parking-spot-monitor.replay-report.v1", "redaction_scan": {"passed": True, "findings": []}}

    exit_code, json_path, _ = invoke(tmp_path, replay, tuning_report())

    assert exit_code == 0
    report = load_report(json_path)
    assert report["status"] == "blocked"
    assert "replay report missing cases list" in report["blocker_reasons"]
    assert "replay report missing metrics_by_spot object" in report["blocker_reasons"]


def test_unsupported_tuning_decision_blocks(tmp_path: Path) -> None:
    exit_code, json_path, _ = invoke(tmp_path, replay_report(), tuning_report(decision="unknown_future_decision"))

    assert exit_code == 0
    report = load_report(json_path)
    assert report["status"] == "blocked"
    assert "unsupported tuning decision: unknown_future_decision" in report["blocker_reasons"]


def test_output_path_that_is_file_is_operational_misuse_and_exits_nonzero(tmp_path: Path, capsys) -> None:
    replay_path = write_json(tmp_path / "replay-report.json", replay_report())
    tuning_path = write_json(tmp_path / "tuning-report.json", tuning_report())
    output_path = tmp_path / "reports"
    output_path.write_text("not a directory", encoding="utf-8")

    exit_code = verifier.main([
        "--replay-report",
        str(replay_path),
        "--tuning-report",
        str(tuning_path),
        "--output-dir",
        str(output_path),
    ])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "OUTPUT_WRITE_FAILED"
