from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts import capture_passing_traffic_candidates as candidates


def write_bundle(root: Path, name: str = "2026-05-11T00-00-00Z") -> Path:
    bundle = root / name
    bundle.mkdir(parents=True)
    (bundle / "latest.jpg").write_bytes(b"fake-jpeg-bytes")
    manifest = {
        "bundle_dir": str(bundle),
        "started_at": "2026-05-11T00:00:00Z",
        "completed_at": "2026-05-11T00:00:01Z",
        "status": "success",
        "phase": "complete",
        "docker_exit_code": 0,
        "detection_summary": {
            "candidate_summaries": [
                {"spot_id": "left_spot", "class_name": "car", "confidence": 0.91, "bbox": [300, 180, 650, 340]},
                {"spot_id": "right_spot", "class_name": "car", "confidence": 0.88, "bbox": [1025, 190, 1412, 448]},
            ]
        },
        "redaction_scan": {"secret_occurrences": 0},
        "artifacts": {"raw_frame": {"valid_jpeg": True, "bundle_path": str(bundle / "latest.jpg")}},
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return bundle


def test_record_bundle_candidate_writes_publication_safe_pending_record(tmp_path: Path) -> None:
    bundle = write_bundle(tmp_path / "calibration-bundles")
    index_path = tmp_path / "candidates.json"

    record = candidates.record_bundle_candidate(bundle / "manifest.json", index_path=index_path)

    saved = json.loads(index_path.read_text(encoding="utf-8"))
    assert record["candidate_id"] == "2026-05-11T00-00-00Z"
    assert record["status"] == "needs_review"
    assert record["semantic_tags"] == ["real_capture"]
    assert record["snapshot_ref"] == "replay://passing-traffic-candidates/2026-05-11T00-00-00Z"
    assert "latest.jpg" not in json.dumps(saved)
    assert "data/" not in json.dumps(saved)
    assert saved["candidates"][0]["detections"][0]["bbox"] == [300, 180, 650, 340]


def test_promote_accepted_candidate_appends_strict_passing_traffic_label(tmp_path: Path) -> None:
    bundle = write_bundle(tmp_path / "calibration-bundles")
    index_path = tmp_path / "candidates.json"
    labels_path = tmp_path / "real-traffic-labels.yaml"
    candidates.record_bundle_candidate(bundle / "manifest.json", index_path=index_path)
    candidates.mark_candidate_status(index_path, "2026-05-11T00-00-00Z", "accepted", semantic_tags=["real_capture", "bottom_driveway", "passing_traffic", "threshold_decision"])

    candidates.promote_accepted_candidates(index_path=index_path, labels_path=labels_path)

    manifest = yaml.safe_load(labels_path.read_text(encoding="utf-8"))
    case = manifest["cases"][0]
    scenario = case["scenarios"][0]
    frame = scenario["frames"][0]
    assert case["tags"] == ["real_capture", "bottom_driveway"]
    assert scenario["tags"] == ["passing_traffic", "threshold_decision"]
    assert frame["snapshot_path"] == "replay://passing-traffic-candidates/2026-05-11T00-00-00Z"
    assert frame["expected"] == {"left_spot": "empty", "right_spot": "empty"}
    assert frame["detections"] == [
        {"spot_id": "left_spot", "class_name": "car", "confidence": 0.91, "bbox": [300, 180, 650, 340]},
        {"spot_id": "right_spot", "class_name": "car", "confidence": 0.88, "bbox": [1025, 190, 1412, 448]},
    ]


def test_promote_ignores_unaccepted_candidates(tmp_path: Path) -> None:
    bundle = write_bundle(tmp_path / "calibration-bundles")
    index_path = tmp_path / "candidates.json"
    labels_path = tmp_path / "real-traffic-labels.yaml"
    candidates.record_bundle_candidate(bundle / "manifest.json", index_path=index_path)

    promoted = candidates.promote_accepted_candidates(index_path=index_path, labels_path=labels_path)

    assert promoted == 0
    assert not labels_path.exists()



def test_cli_runs_capture_attempts_and_indexes_new_bundles(tmp_path: Path, monkeypatch) -> None:
    bundle_root = tmp_path / "bundles"
    index_path = tmp_path / "candidates.json"
    calls = []

    def fake_capture_main(argv):
        calls.append(list(argv))
        write_bundle(bundle_root, f"2026-05-11T00-00-0{len(calls)}Z")
        return 0

    monkeypatch.setattr(candidates.capture_calibration_bundle, "main", fake_capture_main)

    exit_code = candidates.main([
        "--attempts", "2",
        "--interval-seconds", "0",
        "--bundle-root", str(bundle_root),
        "--candidate-index", str(index_path),
    ])

    saved = json.loads(index_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert len(calls) == 2
    assert [item["candidate_id"] for item in saved["candidates"]] == ["2026-05-11T00-00-01Z", "2026-05-11T00-00-02Z"]
    assert all(item["status"] == "needs_review" for item in saved["candidates"])


def test_cli_accept_latest_promotes_latest_candidate(tmp_path: Path, monkeypatch) -> None:
    bundle_root = tmp_path / "bundles"
    index_path = tmp_path / "candidates.json"
    labels_path = tmp_path / "labels.yaml"

    def fake_capture_main(argv):
        write_bundle(bundle_root, "2026-05-11T00-00-03Z")
        return 0

    monkeypatch.setattr(candidates.capture_calibration_bundle, "main", fake_capture_main)

    candidates.main([
        "--attempts", "1",
        "--bundle-root", str(bundle_root),
        "--candidate-index", str(index_path),
        "--labels", str(labels_path),
        "--accept-latest",
    ])

    saved = json.loads(index_path.read_text(encoding="utf-8"))
    labels = yaml.safe_load(labels_path.read_text(encoding="utf-8"))
    assert saved["candidates"][0]["status"] == "accepted"
    assert labels["cases"][0]["case_id"] == "passing-traffic-2026-05-11T00-00-03Z"



def test_cli_scan_existing_indexes_prior_bundles_without_new_capture(tmp_path: Path, monkeypatch) -> None:
    bundle_root = tmp_path / "bundles"
    index_path = tmp_path / "candidates.json"
    write_bundle(bundle_root, "2026-05-11T00-00-04Z")

    def fail_capture_main(argv):
        raise AssertionError("capture should not run when attempts is zero and scan-existing is used")

    monkeypatch.setattr(candidates.capture_calibration_bundle, "main", fail_capture_main)

    candidates.main([
        "--attempts", "0",
        "--scan-existing",
        "--bundle-root", str(bundle_root),
        "--candidate-index", str(index_path),
    ])

    saved = json.loads(index_path.read_text(encoding="utf-8"))
    assert [item["candidate_id"] for item in saved["candidates"]] == ["2026-05-11T00-00-04Z"]
