from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive


def occupied_event(*, spot_id: str, observed_at: str) -> OccupancyEvent:
    return OccupancyEvent(
        event_type=OccupancyEventType.STATE_CHANGED,
        spot_id=spot_id,
        previous_status=OccupancyStatus.EMPTY,
        new_status=OccupancyStatus.OCCUPIED,
        observed_at=observed_at,
        source_timestamp=None,
        snapshot_path="rtsp://camera.local/stream access_token=supersecret",
        candidate_summary={"score": 0.97, "bbox": [1, 2, 3, 4]},
    )


def open_event(*, spot_id: str, observed_at: str) -> OccupancyEvent:
    return OccupancyEvent(
        event_type=OccupancyEventType.OPEN_EVENT,
        spot_id=spot_id,
        previous_status=OccupancyStatus.OCCUPIED,
        new_status=OccupancyStatus.EMPTY,
        observed_at=observed_at,
        source_timestamp=None,
        snapshot_path="/data/snapshots/end.jpg",
        candidate_summary=None,
    )


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "parking_spot_monitor.vehicle_history_cli", *args],
        text=True,
        capture_output=True,
        check=False,
    )


def seed_closed_session(data_dir: Path, *, spot_id: str = "old", started_at: str = "2026-01-01T08:00:00Z") -> str:
    archive = VehicleHistoryArchive(data_dir)
    record = archive.start_session(occupied_event(spot_id=spot_id, observed_at=started_at))
    archive.close_session(open_event(spot_id=spot_id, observed_at="2026-01-01T09:00:00Z"))
    return record.session_id


def test_export_cli_writes_bundle_and_safe_json_summary(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    session_id = seed_closed_session(data_dir)
    output = tmp_path / "vehicle-history-export.tar.gz"

    completed = run_cli("--data-dir", str(data_dir), "export", "--output", str(output))

    assert completed.returncode == 0
    assert completed.stderr == ""
    summary = json.loads(completed.stdout)
    assert summary["operation"] == "export"
    assert summary["status"] == "ok"
    assert summary["closed_session_count"] == 1
    assert output.exists()
    assert f"vehicle-history/sessions/closed/{session_id}.json" in summary["member_names"]
    rendered = completed.stdout + completed.stderr
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered
    assert "raw_image_bytes" not in rendered


def test_prune_cli_requires_apply_for_deletion_and_preserves_dry_run(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    session_id = seed_closed_session(data_dir)
    closed_path = data_dir / "vehicle-history" / "sessions" / "closed" / f"{session_id}.json"

    dry = run_cli("--data-dir", str(data_dir), "prune", "--older-than", "2026-02-01T00:00:00Z", "--dry-run")

    assert dry.returncode == 0
    dry_summary = json.loads(dry.stdout)
    assert dry_summary["status"] == "dry_run"
    assert dry_summary["candidate_session_count"] == 1
    assert closed_path.exists()

    apply = run_cli("--data-dir", str(data_dir), "prune", "--older-than", "2026-02-01T00:00:00Z", "--apply")

    assert apply.returncode == 0
    apply_summary = json.loads(apply.stdout)
    assert apply_summary["status"] == "ok"
    assert apply_summary["candidate_session_count"] == 1
    assert not closed_path.exists()


def test_prune_cli_rejects_invalid_arguments_without_pruning_or_secret_output(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    session_id = seed_closed_session(data_dir)
    closed_path = data_dir / "vehicle-history" / "sessions" / "closed" / f"{session_id}.json"

    invalid_cutoff = run_cli("--data-dir", str(data_dir), "prune", "--older-than", "not-a-date", "--dry-run")
    missing_mode = run_cli("--data-dir", str(data_dir), "prune", "--older-than", "2026-02-01T00:00:00Z")

    assert invalid_cutoff.returncode == 2
    assert missing_mode.returncode == 2
    assert closed_path.exists()
    rendered = invalid_cutoff.stdout + invalid_cutoff.stderr + missing_mode.stdout + missing_mode.stderr
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered
    assert "Traceback" not in rendered


def test_export_cli_unwritable_output_exits_nonzero_without_raw_traceback(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    seed_closed_session(data_dir)
    output_dir = tmp_path / "already-a-directory"
    output_dir.mkdir()

    completed = run_cli("--data-dir", str(data_dir), "export", "--output", str(output_dir))

    assert completed.returncode == 2
    assert completed.stdout == ""
    error = json.loads(completed.stderr)
    assert error["status"] == "error"
    assert error["error_type"] == "ArchiveWriteError"
    rendered = completed.stdout + completed.stderr
    assert "Traceback" not in rendered
    assert "raw_image_bytes" not in rendered
