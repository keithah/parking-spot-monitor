from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts import validate_s07_label_contract as contract


def write_labels(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def strict_manifest(**frame_overrides):
    frame = {
        "frame_id": "real-frame-1",
        "observed_at": "2026-05-18T19:00:00Z",
        "source_timestamp": "2026-05-18T19:00:00Z",
        "snapshot_path": "replay://s10/operator-capture/real-frame-1",
        "expected": {"left_spot": "empty", "right_spot": "occupied"},
        "detections": [{"class_name": "car", "confidence": 0.91, "bbox": [10, 20, 110, 160]}],
    }
    frame.update(frame_overrides)
    return {
        "schema_version": "parking-spot-monitor.replay.v1",
        "cases": [
            {
                "case_id": "real-bottom-driveway-passing-traffic",
                "tags": ["real_capture", "bottom_driveway"],
                "assessed": True,
                "scenarios": [
                    {
                        "scenario_id": "passing-traffic-threshold-check",
                        "tags": ["passing_traffic", "threshold_decision"],
                        "frames": [frame],
                    }
                ],
            }
        ],
    }


def blocked_manifest():
    return {
        "schema_version": "parking-spot-monitor.replay.v1",
        "cases": [
            {
                "case_id": "workflow-smoke-gap",
                "tags": ["operator_derived", "workflow_smoke", "insufficient_bbox_detail", "threshold_decision"],
                "assessed": True,
                "scenarios": [
                    {
                        "scenario_id": "shared-threshold-no-change",
                        "tags": ["threshold_decision", "insufficient_real_semantic_coverage"],
                        "frames": [
                            {
                                "frame_id": "smoke-summary",
                                "observed_at": "2026-05-18T19:00:00Z",
                                "snapshot_path": "replay://s10/workflow-smoke/redacted",
                                "expected": {"left_spot": "empty", "right_spot": "empty"},
                                "detections": [],
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_strict_real_manifest_passes_contract(tmp_path: Path) -> None:
    labels = write_labels(tmp_path / "labels.yaml", strict_manifest())

    report = contract.validate_label_contract(labels)

    assert report["status"] == "passed"
    assert report["blocker_reasons"] == []
    assert report["gap_reasons"] == []


def test_current_fail_closed_shape_is_blocked_but_allowed_by_cli(tmp_path: Path, capsys) -> None:
    labels = write_labels(tmp_path / "labels.yaml", blocked_manifest())

    report = contract.validate_label_contract(labels)
    exit_code = contract.main(["--labels", str(labels), "--allow-blocked"])

    captured = capsys.readouterr()
    cli_report = json.loads(captured.out)
    assert report["status"] == "blocked"
    assert exit_code == 0
    assert cli_report["status"] == "blocked"
    assert any("STRICT_EVIDENCE_BLOCKED" in reason for reason in report["blocker_reasons"])
    assert any("real_capture" in reason for reason in report["gap_reasons"])


def test_blocked_manifest_without_allow_blocked_exits_nonzero(tmp_path: Path) -> None:
    labels = write_labels(tmp_path / "labels.yaml", blocked_manifest())

    assert contract.main(["--labels", str(labels)]) == 1


def test_strict_real_frame_requires_per_spot_expected_labels(tmp_path: Path) -> None:
    labels = write_labels(tmp_path / "labels.yaml", strict_manifest(expected={"left_spot": "empty"}))

    report = contract.validate_label_contract(labels)

    assert report["status"] == "blocked"
    assert any("right_spot" in reason for reason in report["gap_reasons"])
    assert any("STRICT_FRAME_CONTRACT_INCOMPLETE" in reason for reason in report["blocker_reasons"])


def test_strict_real_frame_requires_detector_neutral_bbox_evidence(tmp_path: Path) -> None:
    labels = write_labels(tmp_path / "labels.yaml", strict_manifest(detections=[]))

    report = contract.validate_label_contract(labels)

    assert report["status"] == "blocked"
    assert any("detector-neutral bbox evidence" in reason for reason in report["gap_reasons"])


def test_raw_private_artifact_references_are_blocked(tmp_path: Path) -> None:
    labels = tmp_path / "labels.yaml"
    labels.write_text(
        """
schema_version: parking-spot-monitor.replay.v1
cases:
  - case_id: unsafe
    tags: [real_capture, bottom_driveway]
    scenarios:
      - scenario_id: unsafe
        tags: [passing_traffic, threshold_decision]
        frames:
          - frame_id: unsafe
            snapshot_path: /private/capture/latest.jpg
            expected: {left_spot: empty, right_spot: empty}
            detections: [{class_name: car, confidence: 0.9, bbox: [1, 1, 10, 10]}]
""".strip(),
        encoding="utf-8",
    )

    report = contract.validate_label_contract(labels)

    assert report["status"] == "blocked"
    assert any("PUBLICATION_BOUNDARY_VIOLATION" in reason for reason in report["blocker_reasons"])


def test_fabricated_strict_tags_are_blocked(tmp_path: Path) -> None:
    manifest = strict_manifest()
    manifest["cases"][0]["tags"].append("fabricated")
    labels = write_labels(tmp_path / "labels.yaml", manifest)

    report = contract.validate_label_contract(labels)

    assert report["status"] == "blocked"
    assert any("FABRICATED_LABEL_MARKER" in reason for reason in report["blocker_reasons"])
