from __future__ import annotations

import json
from pathlib import Path

from scripts import replay_calibration_cases as cli


SECRET_RTSP = "rtsp://user:secret@example.test/live"
SECRET_TOKEN = "syt_secretsecret"


def write_config(path: Path) -> None:
    path.write_text(
        """
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
  confidence_threshold: 0.35
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


def write_labels(path: Path, *, bundle_manifest: str | None = None) -> None:
    bundle_line = f"    bundle_manifest: {bundle_manifest}\n" if bundle_manifest is not None else ""
    path.write_text(
        f"""
schema_version: parking-spot-monitor.replay.v1
cases:
  - case_id: synthetic-case
    tags: [real_capture, bottom_driveway]
{bundle_line}    scenarios:
      - scenario_id: driveway-filter
        tags: [passing_traffic, false_negative_probe, threshold_decision]
        frames:
          - frame_id: occupied-left
            observed_at: "2026-05-10T12:00:00Z"
            expected:
              left_spot: occupied
              right_spot: empty
            detections:
              - class_name: car
                confidence: 0.91
                bbox: [1, 1, 4, 4]
""".lstrip(),
        encoding="utf-8",
    )


def invoke(config_path: Path, labels_path: Path, output_dir: Path) -> int:
    return cli.main(["--config", str(config_path), "--labels", str(labels_path), "--output-dir", str(output_dir)], environ={})


def assert_safe(text: str) -> None:
    assert SECRET_RTSP not in text
    assert SECRET_TOKEN not in text
    assert "Traceback" not in text


def test_success_writes_json_and_markdown_reports_without_live_services(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "config.yaml"
    labels_path = tmp_path / "labels.yaml"
    output_dir = tmp_path / "nested" / "reports"
    write_config(config_path)
    write_labels(labels_path)

    exit_code = invoke(config_path, labels_path, output_dir)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert_safe(captured.out + captured.err)
    summary = json.loads(captured.out)
    assert summary["status"] == "ok"
    assert summary["outputs"]["json"].endswith("replay-report.json")
    assert summary["outputs"]["markdown"].endswith("replay-report.md")
    assert summary["status_counts"] == {"passed": 1, "failed": 0, "blocked": 0, "not_covered": 0}
    assert summary["redaction_scan"]["passed"] is True
    assert summary["shared_threshold_sufficiency"] == "sufficient"

    report_json = output_dir / "replay-report.json"
    report_md = output_dir / "replay-report.md"
    assert report_json.is_file()
    assert report_md.is_file()
    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["schema_version"] == "parking-spot-monitor.replay-report.v1"
    assert report["metrics_by_spot"]["left_spot"]["tp"] == 1
    assert report["cases"][0]["tags"] == ["real_capture", "bottom_driveway"]
    assert report["cases"][0]["scenario_tags"] == {"driveway-filter": ["passing_traffic", "false_negative_probe", "threshold_decision"]}
    markdown = report_md.read_text(encoding="utf-8")
    assert "## Summary Verdict" in markdown
    assert "## Semantic Tags" in markdown
    assert "passing_traffic" in markdown


def test_repeated_runs_overwrite_reports_deterministically(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    labels_path = tmp_path / "labels.yaml"
    output_dir = tmp_path / "reports"
    write_config(config_path)
    write_labels(labels_path)

    assert invoke(config_path, labels_path, output_dir) == 0
    first_json = (output_dir / "replay-report.json").read_text(encoding="utf-8")
    first_markdown = (output_dir / "replay-report.md").read_text(encoding="utf-8")
    assert invoke(config_path, labels_path, output_dir) == 0

    assert (output_dir / "replay-report.json").read_text(encoding="utf-8") == first_json
    assert (output_dir / "replay-report.md").read_text(encoding="utf-8") == first_markdown


def test_missing_labels_exits_nonzero_with_safe_diagnostic(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "config.yaml"
    write_config(config_path)

    exit_code = invoke(config_path, tmp_path / "missing.yaml", tmp_path / "reports")

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "LABELS_NOT_FOUND"
    assert diagnostic["phase"] == "labels_read"


def test_malformed_labels_exit_nonzero_without_echoing_secret_content(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "config.yaml"
    labels_path = tmp_path / "labels.yaml"
    write_config(config_path)
    labels_path.write_text(f"cases: [not: valid: yaml # {SECRET_RTSP} {SECRET_TOKEN}", encoding="utf-8")

    exit_code = invoke(config_path, labels_path, tmp_path / "reports")

    captured = capsys.readouterr()
    assert exit_code == 2
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "LABELS_INVALID"
    assert diagnostic["phase"] == "labels_parse"


def test_schema_invalid_bbox_names_field_without_traceback(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "config.yaml"
    labels_path = tmp_path / "labels.yaml"
    write_config(config_path)
    write_labels(labels_path)
    text = labels_path.read_text(encoding="utf-8").replace("bbox: [1, 1, 4, 4]", "bbox: [4, 1, 1, 4]")
    labels_path.write_text(text, encoding="utf-8")

    exit_code = invoke(config_path, labels_path, tmp_path / "reports")

    captured = capsys.readouterr()
    assert exit_code == 2
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "LABELS_INVALID"
    assert diagnostic["phase"] == "labels_schema"
    assert any("bbox" in field for field in diagnostic["fields"])


def test_missing_bundle_reference_blocks_case_instead_of_counting_pass(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    labels_path = tmp_path / "labels.yaml"
    output_dir = tmp_path / "reports"
    write_config(config_path)
    write_labels(labels_path, bundle_manifest="missing-bundle/manifest.json")

    assert invoke(config_path, labels_path, output_dir) == 0

    report = json.loads((output_dir / "replay-report.json").read_text(encoding="utf-8"))
    assert report["status_counts"] == {"passed": 0, "failed": 0, "blocked": 1, "not_covered": 0}
    assert report["cases"][0]["blocked_reasons"] == ["missing_bundle_manifest"]
    assert report["metrics_by_spot"]["left_spot"]["blocked"] == 1
    assert report["metrics_by_spot"]["left_spot"]["tp"] == 0
    assert report["shared_threshold_sufficiency"]["verdict"] == "inconclusive"


def test_valid_bundle_reference_reads_json_metadata_only_and_allows_replay(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    labels_path = tmp_path / "labels.yaml"
    bundle_manifest = tmp_path / "bundle" / "manifest.json"
    bundle_manifest.parent.mkdir(parents=True)
    bundle_manifest.write_text(json.dumps({"schema_version": 1, "artifacts": {"raw_frame": {"bundle_path": "latest.jpg"}}}), encoding="utf-8")
    write_config(config_path)
    write_labels(labels_path, bundle_manifest="bundle/manifest.json")

    assert invoke(config_path, labels_path, tmp_path / "reports") == 0

    report = json.loads((tmp_path / "reports" / "replay-report.json").read_text(encoding="utf-8"))
    assert report["status_counts"]["passed"] == 1
    assert report["status_counts"]["blocked"] == 0


def test_config_failure_is_safe_and_nonzero(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "config.yaml"
    labels_path = tmp_path / "labels.yaml"
    config_path.write_text("stream: [not: valid: yaml", encoding="utf-8")
    write_labels(labels_path)

    exit_code = invoke(config_path, labels_path, tmp_path / "reports")

    captured = capsys.readouterr()
    assert exit_code == 2
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "CONFIG_INVALID"


def test_output_path_that_is_file_fails_safely(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "config.yaml"
    labels_path = tmp_path / "labels.yaml"
    output_path = tmp_path / "reports"
    write_config(config_path)
    write_labels(labels_path)
    output_path.write_text("not a directory", encoding="utf-8")

    exit_code = invoke(config_path, labels_path, output_path)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert_safe(captured.err)
    diagnostic = json.loads(captured.err)
    assert diagnostic["code"] == "OUTPUT_WRITE_FAILED"
    assert diagnostic["phase"] == "output_write"
