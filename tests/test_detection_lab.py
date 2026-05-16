from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from parking_spot_monitor.detection_lab import (
    DetectionLabError,
    DetectionLabManager,
    detection_lab_root,
)


def _write_fixed_inputs(data_dir: Path, *, tuning: bool = False) -> Path:
    lab = detection_lab_root(data_dir)
    lab.mkdir(parents=True, exist_ok=True)
    (lab / "labels.json").write_text('{"cases": []}\n', encoding="utf-8")
    (lab / "replay-config.json").write_text('{"spots": {}}\n', encoding="utf-8")
    if tuning:
        (lab / "baseline-config.json").write_text('{"baseline": true}\n', encoding="utf-8")
        (lab / "proposed-config.json").write_text('{"proposed": true}\n', encoding="utf-8")
    return lab


def _wait_for_terminal(manager: DetectionLabManager, job_id: str) -> dict:
    deadline = time.monotonic() + 3
    last = {}
    while time.monotonic() < deadline:
        last = manager.summarize(job_id)
        if last["status"] in {"succeeded", "failed", "blocked"}:
            return last
        time.sleep(0.01)
    pytest.fail(f"job did not finish: {last}")


def test_replay_job_uses_fixed_inputs_and_persists_bounded_summary(tmp_path: Path) -> None:
    lab = _write_fixed_inputs(tmp_path)
    seen = {}

    def runner(inputs):
        seen.update(inputs)
        report = inputs["job_dir"] / "replay-report.json"
        report.write_text(
            json.dumps(
                {
                    "schema_version": "parking-spot-monitor.replay-report.v1",
                    "status_counts": {"passed": 2, "failed": 1, "blocked": 0},
                    "coverage": {"assessed_frames": 3, "blocked_frames": 0, "not_assessed_frames": 1},
                    "shared_threshold_sufficiency": {"verdict": "sufficient", "rationale": "covered"},
                    "redaction_scan": {"passed": True, "findings": []},
                }
            ),
            encoding="utf-8",
        )
        return report

    manager = DetectionLabManager(tmp_path, replay_runner=runner)
    job = manager.start_replay()
    status = _wait_for_terminal(manager, job.job_id)

    assert status["status"] == "succeeded"
    assert status["job_id"] == job.job_id
    assert status["report_path"] == "replay-report.json"
    assert status["summary"]["status_counts"] == {"blocked": 0, "failed": 1, "passed": 2}
    assert status["summary"]["coverage"]["assessed_frames"] == 3
    assert seen["labels"] == lab / "labels.json"
    assert seen["config"] == lab / "replay-config.json"
    assert seen["job_dir"] == job.job_dir
    assert str(tmp_path) not in json.dumps(status)


def test_tuning_job_summarizes_decision_and_metric_deltas(tmp_path: Path) -> None:
    _write_fixed_inputs(tmp_path, tuning=True)

    def runner(inputs):
        return {
            "schema_version": "parking-spot-monitor.tuning-report.v1",
            "decision": "apply_shared_tuning",
            "decision_rationale": "lower false positives",
            "metric_deltas": {"totals": {"tp": 0, "tn": 1, "fp": -2, "fn": 0}},
            "status_counts": {"baseline": {"failed": 2}, "proposed": {"passed": 2}},
            "redaction_scan": {"passed": True, "findings": []},
        }

    manager = DetectionLabManager(tmp_path, tuning_runner=runner)
    job = manager.start_tuning()
    status = _wait_for_terminal(manager, job.job_id)

    assert status["status"] == "succeeded"
    assert status["summary"]["decision"] == "apply_shared_tuning"
    assert status["summary"]["metric_delta_totals"]["fp"] == -2


def test_missing_fixed_inputs_blocks_without_running_runner(tmp_path: Path) -> None:
    called = False

    def runner(inputs):  # pragma: no cover - assertion below proves it is not reached
        nonlocal called
        called = True
        return {}

    manager = DetectionLabManager(tmp_path, replay_runner=runner)
    job = manager.start_replay()
    status = manager.summarize(job.job_id)

    assert called is False
    assert status["status"] == "blocked"
    assert status["error"]["code"] == "missing_fixed_inputs"
    assert status["summary"]["missing_inputs"] == ["config", "labels"]


def test_malformed_report_is_persisted_as_blocked_status(tmp_path: Path) -> None:
    _write_fixed_inputs(tmp_path)

    def runner(inputs):
        report = inputs["job_dir"] / "replay-report.json"
        report.write_text("not-json", encoding="utf-8")
        return report

    manager = DetectionLabManager(tmp_path, replay_runner=runner)
    job = manager.start_replay()
    status = _wait_for_terminal(manager, job.job_id)

    assert status["status"] == "blocked"
    assert status["error"]["code"] == "malformed_report"


def test_runner_exception_is_redacted_and_bounded(tmp_path: Path) -> None:
    _write_fixed_inputs(tmp_path)

    def runner(inputs):
        raise RuntimeError("rtsp://user:pass@example.local/stream token=super-secret Traceback raw_image_bytes abc")

    manager = DetectionLabManager(tmp_path, replay_runner=runner)
    job = manager.start_replay()
    status = _wait_for_terminal(manager, job.job_id)
    encoded = json.dumps(status)

    assert status["status"] == "failed"
    assert status["error"]["code"] == "runner_exception"
    assert "super-secret" not in encoded
    assert "user:pass" not in encoded
    assert "Traceback" not in encoded
    assert len(job.status_path.read_bytes()) < 24_000


def test_runner_report_path_must_stay_inside_job_dir(tmp_path: Path) -> None:
    _write_fixed_inputs(tmp_path)

    def runner(inputs):
        escaped = tmp_path / "outside-report.json"
        escaped.write_text("{}", encoding="utf-8")
        return escaped

    manager = DetectionLabManager(tmp_path, replay_runner=runner)
    job = manager.start_replay()
    status = _wait_for_terminal(manager, job.job_id)

    assert status["status"] == "blocked"
    assert status["error"]["code"] == "path_outside_lab"


@pytest.mark.parametrize("job_id", ["../x", "lab-20240101T000000Z-deadbeef/../../x", "latest/../x"])
def test_lookup_rejects_path_traversal_job_ids(tmp_path: Path, job_id: str) -> None:
    manager = DetectionLabManager(tmp_path)

    with pytest.raises(DetectionLabError) as excinfo:
        manager.summarize(job_id)

    assert excinfo.value.code == "invalid_job_id"


def test_retention_removes_old_bounded_job_directories(tmp_path: Path) -> None:
    jobs_root = tmp_path / "detection-lab" / "jobs"
    jobs_root.mkdir(parents=True)
    old = jobs_root / "lab-20240101T000000Z-00000001"
    kept = jobs_root / "lab-20240101T000001Z-00000002"
    old.mkdir()
    kept.mkdir()
    (old / "status.json").write_text("{}", encoding="utf-8")
    (kept / "status.json").write_text("{}", encoding="utf-8")
    time.sleep(0.01)
    (kept / "status.json").write_text('{"new": true}', encoding="utf-8")

    removed = DetectionLabManager(tmp_path, max_jobs=1).retain_recent_jobs()

    assert old in removed
    assert not old.exists()
    assert kept.exists()


def test_detection_lab_does_not_import_or_mutate_live_occupancy_state(tmp_path: Path) -> None:
    _write_fixed_inputs(tmp_path)

    def runner(inputs):
        return {"schema_version": "x", "status_counts": {"passed": 1}, "coverage": {}, "redaction_scan": {"passed": True}}

    manager = DetectionLabManager(tmp_path, replay_runner=runner)
    job = manager.start_replay()
    status = _wait_for_terminal(manager, job.job_id)

    assert status["status"] == "succeeded"
    assert not (tmp_path / "occupancy-state.json").exists()
    assert not (tmp_path / "archive-corrections.json").exists()
