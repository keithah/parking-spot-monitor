from __future__ import annotations

import json
from pathlib import Path

from scripts import compare_calibration_tuning as cli


SECRET_RTSP = "rtsp://user:secret@example.test/live"
SECRET_TOKEN = "syt_secretsecret"


def write_config(path: Path, *, confidence_threshold: float = 0.35) -> None:
    path.write_text(
        f"""
stream:
  rtsp_url_env: RTSP_URL
  frame_width: 10
  frame_height: 10
  reconnect_seconds: 5
spots:
  left_spot:
    name: Left spot
    polygon:
      - [0, 0]
      - [5, 0]
      - [5, 5]
      - [0, 5]
  right_spot:
    name: Right spot
    polygon:
      - [5, 0]
      - [10, 0]
      - [10, 5]
      - [5, 5]
detection:
  model: yolov8n.pt
  confidence_threshold: {confidence_threshold}
  min_bbox_area_px: 1
  min_polygon_overlap_ratio: 0.2
  vehicle_classes: [car, truck]
occupancy:
  iou_threshold: 0.2
  confirm_frames: 1
  release_frames: 1
matrix:
  homeserver: https://matrix.example.org
  room_id: "!parking-room:example.org"
  access_token_env: MATRIX_ACCESS_TOKEN
storage:
  data_dir: ./data
runtime:
  health_file: health.json
""".lstrip(),
        encoding="utf-8",
    )


def write_labels(path: Path, *, snapshot_path: str = "replay://snapshot") -> None:
    path.write_text(
        f"""
schema_version: parking-spot-monitor.replay.v1
cases:
  - case_id: synthetic-case
    scenarios:
      - scenario_id: driveway-filter
        frames:
          - frame_id: low-confidence-occupied-left
            observed_at: "2026-05-10T12:00:00Z"
            snapshot_path: {json.dumps(snapshot_path)}
            expected:
              left_spot: occupied
              right_spot: empty
            detections:
              - class_name: car
                confidence: 0.42
                bbox: [1, 1, 4, 4]
          - frame_id: empty-frame
            observed_at: "2026-05-10T12:00:01Z"
            expected:
              left_spot: empty
              right_spot: empty
            detections: []
""".lstrip(),
        encoding="utf-8",
    )


def invoke(baseline_config: Path, proposed_config: Path, labels_path: Path, output_dir: Path) -> int:
    return cli.main(
        [
            "--baseline-config",
            str(baseline_config),
            "--proposed-config",
            str(proposed_config),
            "--labels",
            str(labels_path),
            "--output-dir",
            str(output_dir),
        ],
        environ={},
    )


def assert_safe(text: str) -> None:
    assert SECRET_RTSP not in text
    assert SECRET_TOKEN not in text
    assert "Traceback" not in text


def write_success_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    baseline_config = tmp_path / "baseline.yaml"
    proposed_config = tmp_path / "proposed.yaml"
    labels_path = tmp_path / "labels.yaml"
    write_config(baseline_config, confidence_threshold=0.55)
    write_config(proposed_config, confidence_threshold=0.35)
    write_labels(labels_path)
    return baseline_config, proposed_config, labels_path


def test_success_writes_tuning_reports_and_json_status(tmp_path: Path, capsys) -> None:
    baseline_config, proposed_config, labels_path = write_success_inputs(tmp_path)
    output_dir = tmp_path / "nested" / "reports"

    exit_code = invoke(baseline_config, proposed_config, labels_path, output_dir)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""
    assert_safe(captured.out)
    summary = json.loads(captured.out)
    assert summary["status"] == "ok"
    assert summary["phase"] == "complete"
    assert summary["outputs"]["json"].endswith("tuning-report.json")
    assert summary["outputs"]["markdown"].endswith("tuning-report.md")
    assert summary["redaction_scan"]["passed"] is True
    assert summary["decision"] == "apply_shared_tuning"
    assert summary["metric_deltas"]["totals"]["fn"] == -1
    assert summary["status_counts"] == {
        "baseline": {"passed": 0, "failed": 1, "blocked": 0, "not_covered": 0},
        "proposed": {"passed": 1, "failed": 0, "blocked": 0, "not_covered": 0},
    }

    report_json = output_dir / "tuning-report.json"
    report_md = output_dir / "tuning-report.md"
    assert report_json.is_file()
    assert report_md.is_file()
    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["schema_version"] == "parking-spot-monitor.tuning-report.v1"
    assert report["decision"] == "apply_shared_tuning"
    assert report["baseline"]["metrics_by_spot"]["left_spot"]["fn"] == 1
    assert "# Tuning Comparison Report" in report_md.read_text(encoding="utf-8")


def test_missing_baseline_config_exits_nonzero_with_safe_diagnostic(tmp_path: Path, capsys) -> None:
    _baseline_config, proposed_config, labels_path = write_success_inputs(tmp_path)

    exit_code = invoke(tmp_path / "missing-baseline.yaml", proposed_config, labels_path, tmp_path / "reports")

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "BASELINE_CONFIG_INVALID"
    assert diagnostic["phase"] == "baseline_read"
    assert diagnostic["path"].endswith("missing-baseline.yaml")


def test_invalid_proposed_config_exits_nonzero_with_key_identifiers_only(tmp_path: Path, capsys) -> None:
    baseline_config, proposed_config, labels_path = write_success_inputs(tmp_path)
    proposed_config.write_text("stream: [not: valid: yaml", encoding="utf-8")

    exit_code = invoke(baseline_config, proposed_config, labels_path, tmp_path / "reports")

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "PROPOSED_CONFIG_INVALID"
    assert diagnostic["phase"] == "proposed_yaml"


def test_missing_labels_exits_nonzero_with_safe_diagnostic(tmp_path: Path, capsys) -> None:
    baseline_config, proposed_config, _labels_path = write_success_inputs(tmp_path)

    exit_code = invoke(baseline_config, proposed_config, tmp_path / "missing-labels.yaml", tmp_path / "reports")

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "LABELS_NOT_FOUND"
    assert diagnostic["phase"] == "labels_read"


def test_malformed_labels_exit_nonzero_without_echoing_secret_content(tmp_path: Path, capsys) -> None:
    baseline_config, proposed_config, labels_path = write_success_inputs(tmp_path)
    labels_path.write_text(f"cases: [not: valid: yaml # {SECRET_RTSP} {SECRET_TOKEN}", encoding="utf-8")

    exit_code = invoke(baseline_config, proposed_config, labels_path, tmp_path / "reports")

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "LABELS_INVALID"
    assert diagnostic["phase"] == "labels_parse"


def test_unsafe_report_content_fails_closed_without_writing_reports(tmp_path: Path, capsys) -> None:
    baseline_config, proposed_config, labels_path = write_success_inputs(tmp_path)
    output_dir = tmp_path / "reports"
    write_labels(labels_path, snapshot_path=SECRET_RTSP)

    exit_code = invoke(baseline_config, proposed_config, labels_path, output_dir)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "REPORT_UNSAFE"
    assert diagnostic["phase"] == "render_report"
    assert diagnostic["path"] == "rtsp_url"
    assert not (output_dir / "tuning-report.json").exists()
    assert not (output_dir / "tuning-report.md").exists()


def test_output_path_that_is_file_fails_safely(tmp_path: Path, capsys) -> None:
    baseline_config, proposed_config, labels_path = write_success_inputs(tmp_path)
    output_path = tmp_path / "reports"
    output_path.write_text("not a directory", encoding="utf-8")

    exit_code = invoke(baseline_config, proposed_config, labels_path, output_path)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "OUTPUT_WRITE_FAILED"
    assert diagnostic["phase"] == "output_write"
