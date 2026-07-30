from __future__ import annotations

import json
import math
import os
import stat
import tarfile
from io import StringIO
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from PIL import Image

from parking_spot_monitor import vehicle_history_corrections, vehicle_history_storage
from parking_spot_monitor.logging import setup_logging
from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus
from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleRuntimeCache
from parking_spot_monitor.vehicle_history import (
    ArchiveSchemaError,
    ArchiveWriteError,
    CorrectionReplayState,
    VehicleHistoryArchive,
    cutoff_older_than_days,
    estimate_profile_history,
    estimate_session_history,
)
from parking_spot_monitor.vehicle_history_images import VehicleHistoryImageError, clamp_crop_box
from parking_spot_monitor.vehicle_profiles import MatchResult, MatchStatus


def logger_records(stream: StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def occupied_event(
    *,
    spot_id: str = "left spot/1",
    observed_at: str = "2026-05-18T13:00:00Z",
    snapshot_path: str = "/data/snapshots/start.jpg",
    candidate_summary: dict[str, Any] | None = None,
) -> OccupancyEvent:
    return OccupancyEvent(
        event_type=OccupancyEventType.STATE_CHANGED,
        spot_id=spot_id,
        previous_status=OccupancyStatus.EMPTY,
        new_status=OccupancyStatus.OCCUPIED,
        observed_at=observed_at,
        source_timestamp=None,
        snapshot_path=snapshot_path,
        candidate_summary=candidate_summary if candidate_summary is not None else {"score": 0.97, "bbox": [1, 2, 3, 4]},
    )


def open_event(
    *,
    spot_id: str = "left spot/1",
    observed_at: str = "2026-05-18T13:04:30Z",
    snapshot_path: str = "/data/snapshots/end.jpg",
) -> OccupancyEvent:
    return OccupancyEvent(
        event_type=OccupancyEventType.OPEN_EVENT,
        spot_id=spot_id,
        previous_status=OccupancyStatus.OCCUPIED,
        new_status=OccupancyStatus.EMPTY,
        observed_at=observed_at,
        source_timestamp=None,
        snapshot_path=snapshot_path,
        candidate_summary=None,
    )


def set_session_profile(
    root: Path,
    *,
    archive_state: str,
    session_id: str,
    profile_id: str | None,
    profile_confidence: float | None,
) -> None:
    path = root / "vehicle-history" / "sessions" / archive_state / f"{session_id}.json"
    payload = json.loads(path.read_text())
    payload["profile_id"] = profile_id
    payload["profile_confidence"] = profile_confidence
    path.write_text(json.dumps(payload, allow_nan=False))


def test_start_and_close_session_round_trip_writes_inspectable_json(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)

    active = archive.start_session(occupied_event())

    assert active.session_id == "sess_left-spot-1_2026-05-18t13-00-00z"
    assert active.spot_id == "left spot/1"
    assert active.started_at == "2026-05-18T13:00:00Z"
    assert active.ended_at is None
    assert active.duration_seconds is None
    assert active.source_snapshot_path == "/data/snapshots/start.jpg"
    assert active.occupied_snapshot_path is None
    assert active.occupied_crop_path is None
    assert active.profile_id is None
    assert active.profile_confidence is None

    active_files = list((tmp_path / "vehicle-history" / "sessions" / "active").glob("*.json"))
    assert [path.name for path in active_files] == ["sess_left-spot-1_2026-05-18t13-00-00z.json"]
    raw_active = json.loads(active_files[0].read_text())
    assert raw_active["schema_version"] == 1
    assert raw_active["start_event"]["event_type"] == "occupancy-state-changed"
    assert raw_active["start_event"]["snapshot_path"] == "/data/snapshots/start.jpg"
    assert raw_active["candidate_summary"] == {"score": 0.97, "bbox": [1, 2, 3, 4]}
    assert raw_active["close_event"] is None
    assert stat.S_IMODE(active_files[0].stat().st_mode) == 0o644

    closed = archive.close_session(open_event())

    assert closed is not None
    assert closed.session_id == active.session_id
    assert closed.ended_at == "2026-05-18T13:04:30Z"
    assert closed.duration_seconds == 270
    assert closed.close_event is not None
    assert closed.close_event["event_type"] == "occupancy-open-event"
    assert archive.load_active_sessions() == []
    assert archive.list_closed_sessions() == [closed]
    assert not active_files[0].exists()
    closed_path = tmp_path / "vehicle-history" / "sessions" / "closed" / f"{closed.session_id}.json"
    raw_closed = json.loads(closed_path.read_text())
    assert raw_closed["duration_seconds"] == 270
    assert raw_closed["occupied_snapshot_path"] is None
    assert raw_closed["occupied_crop_path"] is None
    assert raw_closed["profile_id"] is None
    assert raw_closed["profile_confidence"] is None
    rendered = json.dumps(raw_closed)
    assert "NaN" not in rendered
    assert "Infinity" not in rendered


def test_list_closed_sessions_preserves_sorted_path_order_when_creation_order_is_reversed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    created_first = archive.start_session(
        occupied_event(spot_id="z-spot", observed_at="2026-05-18T13:00:00Z")
    )
    archive.close_session(open_event(spot_id="z-spot", observed_at="2026-05-18T13:30:00Z"))
    created_second = archive.start_session(
        occupied_event(spot_id="a-spot", observed_at="2026-05-18T14:00:00Z")
    )
    archive.close_session(open_event(spot_id="a-spot", observed_at="2026-05-18T14:30:00Z"))
    original_glob = Path.glob
    reversed_closed_paths = sorted(original_glob(archive.closed_dir, "*.json"), reverse=True)

    def reverse_closed_paths(path: Path, pattern: str):
        if path == archive.closed_dir and pattern == "*.json":
            return iter(reversed_closed_paths)
        return original_glob(path, pattern)

    monkeypatch.setattr(Path, "glob", reverse_closed_paths)

    closed = archive.list_closed_sessions()

    assert [record.session_id for record in closed] == [created_second.session_id, created_first.session_id]


def test_close_session_revision_invalidates_snapshot_taken_before_active_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event())
    cache = OwnerVehicleRuntimeCache(archive.root / "owner-vehicles.json", logger=setup_logging())
    active_path = archive.active_dir / f"{active.session_id}.json"
    real_unlink = Path.unlink
    overlap_snapshots = []

    def snapshot_before_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path == active_path:
            overlap_snapshots.append(cache.snapshot(archive))
        real_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", snapshot_before_unlink)

    archive.close_session(open_event())
    final_snapshot = cache.snapshot(archive)

    assert [record.session_id for record in overlap_snapshots[0].active_sessions] == [active.session_id]
    assert final_snapshot.active_sessions == ()
    assert final_snapshot is not overlap_snapshots[0]


def test_close_session_unlink_failure_keeps_active_record_and_does_not_claim_final_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event())
    revision_before_close = archive.mutation_revision()
    active_path = archive.active_dir / f"{active.session_id}.json"
    real_unlink = Path.unlink

    def fail_active_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path == active_path:
            raise PermissionError("active unlink denied")
        real_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_active_unlink)

    with pytest.raises(ArchiveWriteError):
        archive.close_session(open_event())

    assert active_path.exists()
    assert (archive.closed_dir / f"{active.session_id}.json").exists()
    assert archive.mutation_revision() == revision_before_close + 1


def test_resolve_wrong_match_subject_prefers_exact_session_then_latest_spot_session(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    first = archive.start_session(occupied_event(spot_id="left_spot", observed_at="2026-05-18T13:00:00Z"))
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-18T13:05:00Z"))
    second = archive.start_session(occupied_event(spot_id="left_spot", observed_at="2026-05-18T14:00:00Z"))
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-18T14:05:00Z"))

    assert archive.resolve_wrong_match_subject(first.session_id) == first.session_id
    assert archive.resolve_wrong_match_subject("left_spot") == second.session_id
    assert archive.resolve_wrong_match_subject("missing_spot") == "missing_spot"
    assert archive.health_snapshot()["vehicle_history_failure_count"] == 0


def test_wrong_match_subject_does_not_sort_archive_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    first = archive.start_session(occupied_event(spot_id="left_spot", observed_at="2026-05-18T10:00:00Z"))
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-18T10:30:00Z"))
    second = archive.start_session(occupied_event(spot_id="left_spot", observed_at="2026-05-19T10:00:00Z"))
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-19T10:30:00Z"))
    monkeypatch.setattr(
        vehicle_history_storage,
        "sorted",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected sort")),
        raising=False,
    )

    assert archive.resolve_wrong_match_subject("left_spot") == second.session_id
    assert first.session_id != second.session_id


def test_wrong_match_latest_uses_parsed_timestamp_when_traversal_is_reversed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    chronologically_older = archive.start_session(
        occupied_event(spot_id="left_spot", observed_at="2026-05-18T00:00:00+02:00")
    )
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-18T01:00:00+02:00"))
    chronologically_newer = archive.start_session(
        occupied_event(spot_id="left_spot", observed_at="2026-05-17T23:15:00Z")
    )
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-17T23:30:00Z"))
    closed_records = archive.list_closed_sessions()

    def reversed_records(directory: Path, *, ordered: bool = True):
        del ordered
        return iter(reversed(closed_records)) if directory == archive.closed_dir else iter(())

    monkeypatch.setattr(archive, "_iter_records", reversed_records)

    assert archive.resolve_wrong_match_subject("left_spot") == chronologically_newer.session_id
    assert chronologically_older.session_id != chronologically_newer.session_id


def test_wrong_match_equivalent_latest_timestamps_use_stable_session_id_tie_break(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    first = archive.start_session(occupied_event(spot_id="left_spot", observed_at="2026-05-17T22:00:00Z"))
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-18T01:00:00Z"))
    second = archive.start_session(occupied_event(spot_id="left_spot", observed_at="2026-05-17T23:00:00Z"))
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-18T01:00:00Z"))
    closed_records = archive.list_closed_sessions()

    def resolve_with(records):
        monkeypatch.setattr(
            archive,
            "_iter_records",
            lambda directory, *, ordered=True: iter(records) if directory == archive.closed_dir else iter(()),
        )
        return archive.resolve_wrong_match_subject("left_spot")

    assert (resolve_with(closed_records), resolve_with(reversed(closed_records))) == (
        second.session_id,
        second.session_id,
    )
    assert first.session_id < second.session_id


def test_duplicate_start_for_same_spot_is_noop_and_logs_safe_warning(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="left spot"))

    second = archive.start_session(
        occupied_event(
            spot_id="left spot",
            observed_at="2026-05-18T13:05:00Z",
            snapshot_path="rtsp://camera.local/stream access_token=supersecret",
        )
    )

    assert second == first
    assert len(archive.load_active_sessions()) == 1
    records = logger_records(stream)
    lifecycle_records = [record for record in records if record["event"].startswith("vehicle-session-start")]
    assert [record["event"] for record in lifecycle_records] == ["vehicle-session-started", "vehicle-session-start-noop"]
    assert lifecycle_records[1]["reason"] == "active-session-exists"
    assert lifecycle_records[1]["spot_id"] == "left spot"
    assert any(record["event"] == "vehicle-archive-loaded" for record in records)
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered


def test_close_with_no_active_session_returns_none_and_logs_noop(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))

    assert archive.close_session(open_event(spot_id="missing")) is None

    assert archive.load_active_sessions() == []
    records = logger_records(stream)
    close_noops = [record for record in records if record["event"] == "vehicle-session-close-noop"]
    assert close_noops == [
        {
            "event": "vehicle-session-close-noop",
            "level": "WARNING",
            "reason": "active-session-missing",
            "spot_id": "missing",
        }
    ]
    assert any(record["event"] == "vehicle-archive-loaded" for record in records)


def test_malformed_and_oversized_session_files_are_quarantined_individually(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    valid = archive.start_session(occupied_event(spot_id="valid", observed_at="2026-05-18T13:00:00Z"))
    active_dir = tmp_path / "vehicle-history" / "sessions" / "active"
    corrupt_path = active_dir / "broken.json"
    corrupt_path.write_text("{not-json rtsp://camera.local access_token=supersecret Traceback raw_image_bytes")
    oversized_path = active_dir / "too-large.json"
    oversized_path.write_text(" " * 1_000_001)

    loaded = archive.load_active_sessions()

    assert loaded == [valid]
    assert not corrupt_path.exists()
    assert not oversized_path.exists()
    quarantined = sorted((tmp_path / "vehicle-history" / "sessions" / "quarantine").glob("*.corrupt-*"))
    assert len(quarantined) == 2
    assert {path.name.split(".corrupt-")[0] for path in quarantined} == {"broken.json", "too-large.json"}
    records = logger_records(stream)
    quarantine_records = [record for record in records if record["event"] == "vehicle-session-quarantined"]
    assert [record["phase"] for record in quarantine_records] == ["json-load", "size-validate"]
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "Traceback" not in rendered
    assert "raw_image_bytes" not in rendered


def test_schema_invalid_session_file_is_quarantined_without_blocking_valid_sessions(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    valid = archive.start_session(occupied_event(spot_id="valid"))
    active_dir = tmp_path / "vehicle-history" / "sessions" / "active"
    invalid_path = active_dir / "invalid.json"
    invalid_path.write_text(json.dumps({"schema_version": 1, "session_id": "missing-required-fields"}))

    loaded = archive.load_active_sessions()

    assert loaded == [valid]
    assert not invalid_path.exists()
    assert len(list((tmp_path / "vehicle-history" / "sessions" / "quarantine").glob("invalid.json.corrupt-*"))) == 1
    records = logger_records(stream)
    quarantine_records = [record for record in records if record["event"] == "vehicle-session-quarantined"]
    assert quarantine_records[-1]["phase"] == "schema-validate"
    assert quarantine_records[-1]["error_type"] == "ArchiveSchemaError"
    assert any(record["event"] == "vehicle-archive-loaded" for record in records)


def test_wrong_event_types_are_rejected_without_writing_files(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)

    with pytest.raises(ArchiveSchemaError):
        archive.start_session(open_event())
    with pytest.raises(ArchiveSchemaError):
        archive.close_session(occupied_event())

    sessions_dir = tmp_path / "vehicle-history" / "sessions"
    assert (not list(sessions_dir.rglob("*.json"))) if sessions_dir.exists() else True


def test_duration_is_none_when_close_timestamp_precedes_start_or_timestamps_do_not_parse(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.start_session(occupied_event(spot_id="backward", observed_at="2026-05-18T13:00:00Z"))
    backward = archive.close_session(open_event(spot_id="backward", observed_at="2026-05-18T12:59:59Z"))
    assert backward is not None
    assert backward.duration_seconds is None

    archive.start_session(occupied_event(spot_id="invalid", observed_at="not a timestamp"))
    invalid = archive.close_session(open_event(spot_id="invalid", observed_at="also not a timestamp"))
    assert invalid is not None
    assert invalid.started_at == "not a timestamp"
    assert invalid.ended_at == "also not a timestamp"
    assert invalid.duration_seconds is None


def test_zero_duration_session_is_allowed(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.start_session(occupied_event(spot_id="same-time", observed_at="2026-05-18T13:00:00Z"))

    closed = archive.close_session(open_event(spot_id="same-time", observed_at="2026-05-18T13:00:00Z"))

    assert closed is not None
    assert closed.duration_seconds == 0


def test_atomic_write_failure_preserves_existing_active_file_and_logs_safe_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="left"))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    existing = active_path.read_text()

    real_replace = os.replace

    def failing_replace(src: str | bytes | os.PathLike[str], dst: str | bytes | os.PathLike[str]) -> None:
        if Path(dst).parent.name == "closed":
            raise PermissionError("cannot write rtsp://camera access_token=supersecret Traceback raw_image_bytes")
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", failing_replace)

    with pytest.raises(ArchiveWriteError):
        archive.close_session(open_event(spot_id="left"))

    assert active_path.read_text() == existing
    assert not list((tmp_path / "vehicle-history" / "sessions" / "closed").glob("*.json"))
    assert not list((tmp_path / "vehicle-history" / "sessions" / "closed").glob("*.tmp"))
    records = logger_records(stream)
    assert records[-1]["event"] == "vehicle-session-write-failed"
    assert records[-1]["error_type"] == "PermissionError"
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "Traceback" not in rendered
    assert "raw_image_bytes" not in rendered


def test_mutation_revision_advances_on_writes_and_is_stable_on_reads(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)

    start = archive.mutation_revision()
    record = archive.start_session(occupied_event(spot_id="left", observed_at="2026-05-18T13:00:00Z"))
    after_start = archive.mutation_revision()
    assert after_start > start

    # Reads must not advance the revision, so the health cache stays warm.
    archive.health_snapshot()
    archive.load_active_sessions()
    assert archive.mutation_revision() == after_start

    # Even a read that quarantines a corrupt file must not bump the revision,
    # or health_snapshot() could invalidate its own cache entry mid-read.
    (tmp_path / "vehicle-history" / "sessions" / "active" / "bad.json").write_text("{bad json")
    quiet = archive.mutation_revision()
    archive.load_active_sessions()
    assert archive.mutation_revision() == quiet

    archive.close_session(open_event(spot_id="left", observed_at="2026-05-18T13:30:00Z"))
    assert archive.mutation_revision() > after_start
    assert record.session_id


def test_health_snapshot_summarizes_archive_counts_image_growth_retention_and_latest_failure(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    older = archive.start_session(occupied_event(spot_id="older", observed_at="2026-05-17T12:00:00Z"))
    archive.close_session(open_event(spot_id="older", observed_at="2026-05-17T12:30:00Z"))
    active = archive.start_session(occupied_event(spot_id="active", observed_at="2026-05-18T13:00:00Z"))
    closed = archive.start_session(occupied_event(spot_id="closed", observed_at="2026-05-18T14:00:00Z"))
    archive.close_session(open_event(spot_id="closed", observed_at="2026-05-18T14:30:00Z"))
    corrections_dir = tmp_path / "vehicle-history" / "corrections"
    corrections_dir.mkdir(parents=True)
    (corrections_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "correction_id": "corr_health_1",
                "action": "profile_summary_requested",
                "created_at": "2026-05-18T14:45:00Z",
                "matrix_event_id": "$event",
                "matrix_sender": "@operator:example",
                "matrix_room_id": "!room:example",
                "profile_id": "prof_known",
            }
        )
        + "\n"
    )
    full_dir = tmp_path / "vehicle-history" / "images" / "occupied-full"
    crop_dir = tmp_path / "vehicle-history" / "images" / "occupied-crops"
    full_dir.mkdir(parents=True)
    crop_dir.mkdir(parents=True)
    (full_dir / f"{closed.session_id}.jpg").write_bytes(b"full-frame")
    (crop_dir / f"{closed.session_id}.jpg").write_bytes(b"crop")
    (tmp_path / "vehicle-history" / "sessions" / "active" / "bad.json").write_text("{bad json")
    (tmp_path / "vehicle-history" / "profiles" / "quarantine").mkdir(parents=True)
    (tmp_path / "vehicle-history" / "profiles" / "quarantine" / "profile.json.corrupt-test").write_text("profile-metadata")
    maintenance_dir = tmp_path / "vehicle-history" / "metadata" / "maintenance"
    maintenance_dir.mkdir(parents=True)
    (maintenance_dir / "last.json").write_text(
        json.dumps(
            {
                "operation": "export",
                "status": "ok",
                "completed_at": "2026-05-18T15:00:00Z",
                "archive_file_count": 99,
                "access_token": "supersecret",
                "notes": "rtsp://camera.local/stream raw_image_bytes should-not-export",
            }
        )
    )

    snapshot = archive.health_snapshot()

    assert snapshot["active_session_count"] == 1
    assert snapshot["closed_session_count"] == 2
    assert snapshot["retention_policy"] == "indefinite"
    assert snapshot["management_capabilities"] == ["export", "prune"]
    assert snapshot["oldest_retained_session_started_at"] == older.started_at
    assert snapshot["archive_file_count"] > snapshot["image_file_count"]
    assert snapshot["archive_bytes"] >= snapshot["image_bytes"]
    assert snapshot["last_maintenance_metadata"] == {
        "operation": "export",
        "status": "ok",
        "completed_at": "2026-05-18T15:00:00Z",
        "archive_file_count": 99,
        "manifest_name": "last.json",
    }
    assert snapshot["occupied_snapshot_count"] == 1
    assert snapshot["occupied_crop_count"] == 1
    assert snapshot["image_file_count"] == 2
    assert snapshot["image_bytes"] == len(b"full-frame") + len(b"crop")
    assert snapshot["missing_occupied_image_reference_count"] == 3
    assert snapshot["correction_count"] == 1
    assert snapshot["profile_quarantine_count"] == 1
    assert snapshot["vehicle_history_failure_count"] == 1
    assert snapshot["last_vehicle_history_error"] is not None
    assert snapshot["last_vehicle_history_error"]["phase"] == "json-load"
    assert snapshot["last_vehicle_history_error"]["path_name"] == "bad.json"
    assert active.occupied_snapshot_path is None
    rendered = json.dumps(snapshot)
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered
    assert "raw_image_bytes" not in rendered
    assert "should-not-export" not in rendered


def test_health_snapshot_streams_closed_sessions_without_list_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    for index in range(250):
        spot_id = f"closed-{index}"
        archive.start_session(
            occupied_event(spot_id=spot_id, observed_at=f"2026-05-18T{index // 60:02d}:{index % 60:02d}:00Z")
        )
        archive.close_session(
            open_event(spot_id=spot_id, observed_at=f"2026-05-19T{index // 60:02d}:{index % 60:02d}:00Z")
        )

    def forbidden_list_closed_sessions() -> None:
        raise AssertionError("health must stream the closed archive")

    def forbidden_load_records(_directory: Path) -> None:
        raise AssertionError("health must not materialize session records")

    monkeypatch.setattr(archive, "list_closed_sessions", forbidden_list_closed_sessions)
    monkeypatch.setattr(archive, "_load_records", forbidden_load_records)

    health = archive.health_snapshot()

    assert health["closed_session_count"] == 250
    assert health["oldest_retained_session_started_at"] is not None


def test_streaming_health_quarantines_corrupt_closed_records_without_counting_them(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    valid = archive.start_session(occupied_event(spot_id="valid"))
    archive.close_session(open_event(spot_id="valid"))
    corrupt_path = tmp_path / "vehicle-history" / "sessions" / "closed" / "broken.json"
    corrupt_path.write_text("{not-json")

    health = archive.health_snapshot()

    assert health["closed_session_count"] == 1
    assert health["vehicle_history_failure_count"] == 1
    assert health["last_vehicle_history_error"] is not None
    assert health["last_vehicle_history_error"]["phase"] == "json-load"
    assert not corrupt_path.exists()
    assert valid.session_id


def test_health_oldest_session_uses_parsed_timestamp_order(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    chronologically_oldest = archive.start_session(
        occupied_event(spot_id="offset", observed_at="2026-05-18T01:00:00+02:00")
    )
    archive.close_session(open_event(spot_id="offset", observed_at="2026-05-18T01:15:00+02:00"))
    archive.start_session(occupied_event(spot_id="utc", observed_at="2026-05-17T23:30:00Z"))

    health = archive.health_snapshot()

    assert health["oldest_retained_session_started_at"] == chronologically_oldest.started_at


def test_health_equivalent_oldest_timestamps_use_legacy_sorted_path_tie_break(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    legacy_winner = archive.start_session(
        occupied_event(spot_id="same_spot", observed_at="2026-05-17T23:00:00Z")
    )
    archive.close_session(open_event(spot_id="same_spot", observed_at="2026-05-17T23:15:00Z"))
    archive.start_session(occupied_event(spot_id="same_spot", observed_at="2026-05-18T01:00:00+02:00"))
    archive.close_session(open_event(spot_id="same_spot", observed_at="2026-05-18T01:15:00+02:00"))
    closed_records = archive.list_closed_sessions()

    def health_with(records) -> dict[str, Any]:
        monkeypatch.setattr(
            archive,
            "_iter_records",
            lambda directory, *, ordered=True: iter(records) if directory == archive.closed_dir else iter(()),
        )
        return archive.health_snapshot()

    reverse_health = health_with(reversed(closed_records))
    forward_health = health_with(closed_records)

    assert reverse_health["oldest_retained_session_started_at"] == "2026-05-17T23:00:00Z"
    assert forward_health["oldest_retained_session_started_at"] == "2026-05-17T23:00:00Z"
    assert legacy_winner.session_id < closed_records[-1].session_id


def test_health_equal_active_and_closed_timestamps_preserve_legacy_active_precedence(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.start_session(occupied_event(spot_id="same_spot", observed_at="2026-05-17T23:00:00Z"))
    archive.close_session(open_event(spot_id="same_spot", observed_at="2026-05-17T23:15:00Z"))
    legacy_winner = archive.start_session(
        occupied_event(spot_id="same_spot", observed_at="2026-05-18T01:00:00+02:00")
    )

    health = archive.health_snapshot()

    assert health["oldest_retained_session_started_at"] == legacy_winner.started_at


def test_empty_archive_health_snapshot_exposes_retention_defaults_without_files(tmp_path: Path) -> None:
    snapshot = VehicleHistoryArchive(tmp_path).health_snapshot()

    assert snapshot["active_session_count"] == 0
    assert snapshot["closed_session_count"] == 0
    assert snapshot["retention_policy"] == "indefinite"
    assert snapshot["management_capabilities"] == ["export", "prune"]
    assert snapshot["oldest_retained_session_started_at"] is None
    assert snapshot["archive_file_count"] == 0
    assert snapshot["archive_bytes"] == 0
    assert snapshot["last_maintenance_metadata"] is None
    assert snapshot["image_file_count"] == 0
    assert snapshot["image_bytes"] == 0


def test_archive_health_scan_errors_are_non_blocking_and_safely_recorded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.start_session(occupied_event(spot_id="scan failure"))

    def fail_archive_stats(directory: Path) -> tuple[int, int]:
        raise OSError("rtsp://camera.local/stream access_token=supersecret raw_image_bytes")

    monkeypatch.setattr("parking_spot_monitor.vehicle_history_maintenance._archive_directory_stats", fail_archive_stats)

    snapshot = archive.health_snapshot()

    assert snapshot["archive_file_count"] == 0
    assert snapshot["archive_bytes"] == 0
    assert snapshot["vehicle_history_failure_count"] == 1
    assert snapshot["last_vehicle_history_error"] is not None
    assert snapshot["last_vehicle_history_error"]["phase"] == "archive-scan"
    rendered = json.dumps(snapshot)
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered
    assert "raw_image_bytes" not in rendered


def test_public_json_rejects_non_finite_candidate_values(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)

    with pytest.raises(ArchiveSchemaError):
        archive.start_session(occupied_event(candidate_summary={"score": math.nan}))

    assert not list((tmp_path / "vehicle-history" / "sessions" / "active").glob("*.json"))


def write_test_jpeg(path: Path, *, size: tuple[int, int] = (8, 6), color: tuple[int, int, int] = (10, 80, 140)) -> Path:
    Image.new("RGB", size, color).save(path, format="JPEG")
    return path


def test_attach_occupied_images_writes_full_frame_and_clamped_crop_then_close_preserves_refs(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="image spot"))
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))

    updated = archive.attach_occupied_images(
        session_id=active.session_id,
        source_frame_path=source,
        bbox=(-2.2, 1.2, 5.1, 20.9),
    )

    assert updated.occupied_snapshot_path is not None
    assert updated.occupied_crop_path is not None
    full_path = Path(updated.occupied_snapshot_path)
    crop_path = Path(updated.occupied_crop_path)
    assert full_path.exists()
    assert crop_path.exists()
    assert full_path != crop_path
    assert full_path.name == f"{active.session_id}.jpg"
    assert crop_path.name == f"{active.session_id}.jpg"
    assert stat.S_IMODE(full_path.stat().st_mode) == 0o644
    assert stat.S_IMODE(crop_path.stat().st_mode) == 0o644
    with Image.open(full_path) as full_frame:
        assert full_frame.size == (8, 6)
        assert full_frame.format == "JPEG"
    with Image.open(crop_path) as crop:
        assert crop.size == (6, 5)
        assert crop.format == "JPEG"

    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    raw_active = json.loads(active_path.read_text())
    assert raw_active["occupied_snapshot_path"] == str(full_path)
    assert raw_active["occupied_crop_path"] == str(crop_path)

    snapshot = archive.health_snapshot()
    assert snapshot["occupied_snapshot_count"] == 1
    assert snapshot["occupied_crop_count"] == 1
    assert snapshot["image_file_count"] == 2
    assert snapshot["image_bytes"] == full_path.stat().st_size + crop_path.stat().st_size
    assert snapshot["missing_occupied_image_reference_count"] == 0

    closed = archive.close_session(open_event(spot_id="image spot"))

    assert closed is not None
    assert closed.occupied_snapshot_path == str(full_path)
    assert closed.occupied_crop_path == str(crop_path)
    raw_closed = json.loads((tmp_path / "vehicle-history" / "sessions" / "closed" / f"{active.session_id}.json").read_text())
    assert raw_closed["occupied_snapshot_path"] == str(full_path)
    assert raw_closed["occupied_crop_path"] == str(crop_path)
    records = logger_records(stream)
    image_records = [record for record in records if record["event"].startswith("vehicle-session-images")]
    assert image_records == [
        {
            "crop_path_name": f"{active.session_id}.jpg",
            "event": "vehicle-session-images-captured",
            "full_path_name": f"{active.session_id}.jpg",
            "level": "INFO",
            "session_id": active.session_id,
            "spot_id": "image spot",
        }
    ]


def test_attach_occupied_images_is_idempotent_and_does_not_overwrite_existing_files(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="duplicate image"))
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6), color=(20, 30, 40))
    first = archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=(1, 1, 6, 5))
    assert first.occupied_snapshot_path is not None
    assert first.occupied_crop_path is not None
    full_path = Path(first.occupied_snapshot_path)
    crop_path = Path(first.occupied_crop_path)
    full_before = full_path.read_bytes()
    crop_before = crop_path.read_bytes()
    source.unlink()

    second = archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=(0, 0, 8, 6))

    assert second == first
    assert full_path.read_bytes() == full_before
    assert crop_path.read_bytes() == crop_before
    records = logger_records(stream)
    noop = [record for record in records if record["event"] == "vehicle-session-images-noop"]
    assert noop == [
        {
            "crop_path_name": crop_path.name,
            "event": "vehicle-session-images-noop",
            "full_path_name": full_path.name,
            "level": "INFO",
            "reason": "already-attached",
            "session_id": active.session_id,
            "spot_id": "duplicate image",
        }
    ]


@pytest.mark.parametrize(
    ("bbox", "message"),
    [
        ((1, 1, 1, 3), "empty"),
        ((math.nan, 1, 3, 4), "finite"),
        ((20, 20, 25, 25), "empty"),
    ],
)
def test_attach_occupied_images_rejects_invalid_bbox_without_mutating_session_json(
    tmp_path: Path, bbox: tuple[float, float, float, float], message: str
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event(spot_id="bad bbox"))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    before = active_path.read_text()
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))

    with pytest.raises(ArchiveWriteError, match=message):
        archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=bbox)

    assert active_path.read_text() == before
    assert not list((tmp_path / "vehicle-history" / "images").rglob("*.jpg"))


def test_attach_occupied_images_rejects_missing_and_non_jpeg_sources_without_mutating_session_json(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    missing_active = archive.start_session(occupied_event(spot_id="missing source"))
    missing_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{missing_active.session_id}.json"
    missing_before = missing_path.read_text()

    with pytest.raises(ArchiveWriteError, match="missing or unreadable"):
        archive.attach_occupied_images(session_id=missing_active.session_id, source_frame_path=tmp_path / "missing.jpg", bbox=(0, 0, 2, 2))

    assert missing_path.read_text() == missing_before

    non_jpeg_active = archive.start_session(occupied_event(spot_id="non jpeg"))
    non_jpeg_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{non_jpeg_active.session_id}.json"
    non_jpeg_before = non_jpeg_path.read_text()
    png = tmp_path / "source.png"
    Image.new("RGB", (4, 4), (1, 2, 3)).save(png, format="PNG")

    with pytest.raises(ArchiveWriteError, match="must be a JPEG"):
        archive.attach_occupied_images(session_id=non_jpeg_active.session_id, source_frame_path=png, bbox=(0, 0, 2, 2))

    assert non_jpeg_path.read_text() == non_jpeg_before
    assert not list((tmp_path / "vehicle-history" / "images").rglob("*.jpg"))


def test_image_atomic_replace_failure_cleans_temp_files_and_keeps_active_json_unmodified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="replace fail"))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    before = active_path.read_text()
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))
    real_replace = os.replace

    def failing_replace(src: str | bytes | os.PathLike[str], dst: str | bytes | os.PathLike[str]) -> None:
        if Path(dst).parent.name == "occupied-full":
            raise PermissionError("cannot write rtsp://camera access_token=supersecret Traceback raw_image_bytes")
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", failing_replace)

    with pytest.raises(ArchiveWriteError):
        archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=(0, 0, 2, 2))

    assert active_path.read_text() == before
    images_dir = tmp_path / "vehicle-history" / "images"
    assert not list(images_dir.rglob("*.jpg")) if images_dir.exists() else True
    assert not list(images_dir.rglob("*.tmp")) if images_dir.exists() else True
    records = logger_records(stream)
    failure = [record for record in records if record["event"] == "vehicle-session-images-failed"][-1]
    assert failure["phase"] == "image-capture"
    assert failure["path_name"] == f"{active.session_id}.json"
    assert failure["session_id"] == active.session_id
    assert failure["error_type"] == "VehicleHistoryImageError"
    snapshot = archive.health_snapshot()
    last_error = snapshot["last_vehicle_history_error"]
    assert last_error is not None
    assert last_error["phase"] == "image-capture"
    assert last_error["path_name"] == f"{active.session_id}.json"
    assert last_error["session_id"] == active.session_id
    assert last_error["error_type"] == "VehicleHistoryImageError"
    assert "cannot write" in last_error["error_message"]
    assert "supersecret" not in last_error["error_message"]
    assert "Traceback" not in last_error["error_message"]
    assert "raw_image_bytes" not in last_error["error_message"]
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "Traceback" not in rendered
    assert "raw_image_bytes" not in rendered


def test_match_or_create_profile_creates_profile_updates_session_and_close_preserves_assignment(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="profile new"))
    source = write_test_jpeg(tmp_path / "profile-source.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=(0, 0, 96, 48))

    assignment = archive.match_or_create_profile(session_id=active.session_id)

    assert assignment.status == "new_profile"
    assert assignment.profile_id is not None
    assert assignment.profile_id.startswith("prof_")
    assert assignment.profile_confidence == pytest.approx(1.0)
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    raw_active = json.loads(active_path.read_text())
    assert raw_active["profile_id"] == assignment.profile_id
    assert raw_active["profile_confidence"] == pytest.approx(1.0)
    profile_path = tmp_path / "vehicle-history" / "profiles" / "active" / f"{assignment.profile_id}.json"
    raw_profile = json.loads(profile_path.read_text())
    assert raw_profile["schema_version"] == 1
    assert raw_profile["profile_id"] == assignment.profile_id
    assert raw_profile["label"] is None
    assert raw_profile["status"] == "active"
    assert raw_profile["sample_count"] == 1
    assert raw_profile["sample_session_ids"] == [active.session_id]
    assert raw_profile["exemplar_crop_path"] == f"{active.session_id}.jpg"
    assert "NaN" not in json.dumps(raw_profile)

    closed = archive.close_session(open_event(spot_id="profile new"))

    assert closed is not None
    assert closed.profile_id == assignment.profile_id
    assert closed.profile_confidence == pytest.approx(1.0)
    records = logger_records(stream)
    assert any(record["event"] == "vehicle-session-profile-created" for record in records)


def test_match_or_create_profile_matches_existing_profile_and_is_idempotent(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="first", observed_at="2026-05-18T13:00:00Z"))
    first_source = write_test_jpeg(tmp_path / "first.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=first.session_id, source_frame_path=first_source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=first.session_id)
    archive.close_session(open_event(spot_id="first", observed_at="2026-05-18T13:02:00Z"))

    second = archive.start_session(occupied_event(spot_id="second", observed_at="2026-05-18T13:03:00Z"))
    second_source = write_test_jpeg(tmp_path / "second.jpg", size=(96, 48), color=(122, 42, 42))
    archive.attach_occupied_images(session_id=second.session_id, source_frame_path=second_source, bbox=(0, 0, 96, 48))

    matched = archive.match_or_create_profile(session_id=second.session_id)
    profile_path = tmp_path / "vehicle-history" / "profiles" / "active" / f"{created.profile_id}.json"
    after_match = json.loads(profile_path.read_text())
    second_assignment = archive.match_or_create_profile(session_id=second.session_id)
    after_noop = json.loads(profile_path.read_text())

    assert matched.status == "matched"
    assert matched.profile_id == created.profile_id
    assert matched.profile_confidence is not None and matched.profile_confidence > 0.9
    assert after_match["sample_count"] == 2
    assert second.session_id in after_match["sample_session_ids"]
    assert second_assignment.profile_id == created.profile_id
    assert after_noop == after_match
    assert len(list((tmp_path / "vehicle-history" / "profiles" / "active").glob("*.json"))) == 1
    records = logger_records(stream)
    assert any(record["event"] == "vehicle-session-profile-matched" for record in records)
    assert any(record["event"] == "vehicle-session-profile-noop" for record in records)


def test_assign_owner_profile_to_active_spot_updates_session_and_profile_sample(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="right_spot", observed_at="2026-05-18T13:00:00Z"))
    first_source = write_test_jpeg(tmp_path / "owner-first.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=first.session_id, source_frame_path=first_source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=first.session_id)
    assert created.profile_id is not None
    archive.close_session(open_event(spot_id="right_spot", observed_at="2026-05-18T13:02:00Z"))
    (tmp_path / "vehicle-history" / "owner-vehicles.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_vehicles": [
                    {
                        "profile_id": created.profile_id,
                        "label": "Keith's black Tesla",
                        "description": "black Tesla, tinted windows, roof rack",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    current = archive.start_session(occupied_event(spot_id="right_spot", observed_at="2026-05-18T13:03:00Z"))
    current_source = write_test_jpeg(tmp_path / "owner-current.jpg", size=(120, 60), color=(121, 41, 41))
    archive.attach_occupied_images(session_id=current.session_id, source_frame_path=current_source, bbox=(0, 0, 120, 60))

    assignment = archive.assign_owner_profile_to_active_spot("right_spot")

    assert assignment.status == "owner_assigned"
    assert assignment.session_id == current.session_id
    assert assignment.profile_id == created.profile_id
    assert assignment.profile_confidence == pytest.approx(1.0)
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{current.session_id}.json"
    active_payload = json.loads(active_path.read_text(encoding="utf-8"))
    assert active_payload["profile_id"] == created.profile_id
    assert active_payload["profile_confidence"] == pytest.approx(1.0)
    profile_path = tmp_path / "vehicle-history" / "profiles" / "active" / f"{created.profile_id}.json"
    profile_payload = json.loads(profile_path.read_text(encoding="utf-8"))
    assert profile_payload["sample_count"] == 2
    assert profile_payload["sample_session_ids"] == [first.session_id, current.session_id]
    records = logger_records(stream)
    assert any(
        record["event"] == "vehicle-session-owner-profile-assigned"
        and record["spot_id"] == "right_spot"
        and record["session_id"] == current.session_id
        and record["profile_id"] == created.profile_id
        for record in records
    )


def test_active_spot_assignments_summarizes_owner_and_unknown_active_sessions(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    owner_session = archive.start_session(occupied_event(spot_id="right_spot", observed_at="2026-05-18T13:00:00Z"))
    owner_source = write_test_jpeg(tmp_path / "owner-current.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=owner_session.session_id, source_frame_path=owner_source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=owner_session.session_id)
    assert created.profile_id is not None
    (tmp_path / "vehicle-history" / "owner-vehicles.json").write_text(
        json.dumps({"schema_version": 1, "owner_vehicles": [{"profile_id": created.profile_id, "label": "Keith's black Tesla"}]}),
        encoding="utf-8",
    )
    unknown = archive.start_session(occupied_event(spot_id="left_spot", observed_at="2026-05-18T13:05:00Z"))
    unknown_source = write_test_jpeg(tmp_path / "unknown.jpg", size=(96, 48), color=(90, 90, 90))
    archive.attach_occupied_images(session_id=unknown.session_id, source_frame_path=unknown_source, bbox=(0, 0, 96, 48))

    assignments = archive.active_spot_assignments()

    assert assignments == [
        {
            "spot_id": "left_spot",
            "session_id": unknown.session_id,
            "profile_id": None,
            "profile_label": None,
            "profile_confidence": None,
            "is_owner": False,
            "owner_label": None,
            "profile_sample_count": None,
            "started_at": unknown.started_at,
        },
        {
            "spot_id": "right_spot",
            "session_id": owner_session.session_id,
            "profile_id": created.profile_id,
            "profile_label": "Keith's black Tesla",
            "profile_confidence": 1.0,
            "is_owner": True,
            "owner_label": "Keith's black Tesla",
            "profile_sample_count": 1,
            "started_at": owner_session.started_at,
        },
    ]


def test_match_or_create_profile_does_not_update_owner_profile_for_low_confidence_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="owner", observed_at="2026-05-18T13:00:00Z"))
    first_source = write_test_jpeg(tmp_path / "owner.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=first.session_id, source_frame_path=first_source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=first.session_id)
    assert created.profile_id is not None
    archive.close_session(open_event(spot_id="owner", observed_at="2026-05-18T13:02:00Z"))
    owner_registry_path = tmp_path / "vehicle-history" / "owner-vehicles.json"
    owner_registry_path.write_text(
        json.dumps({"schema_version": 1, "owner_vehicles": [{"profile_id": created.profile_id, "label": "Keith's black Tesla"}]}),
        encoding="utf-8",
    )

    candidate = archive.start_session(occupied_event(spot_id="left", observed_at="2026-05-18T13:03:00Z"))
    candidate_source = write_test_jpeg(tmp_path / "candidate.jpg", size=(96, 48), color=(122, 42, 42))
    archive.attach_occupied_images(session_id=candidate.session_id, source_frame_path=candidate_source, bbox=(0, 0, 96, 48))
    profile_path = tmp_path / "vehicle-history" / "profiles" / "active" / f"{created.profile_id}.json"
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{candidate.session_id}.json"
    before_profile = profile_path.read_text(encoding="utf-8")
    before_session = active_path.read_text(encoding="utf-8")

    def low_confidence_owner_match(_descriptor: object, _profiles: object) -> MatchResult:
        return MatchResult(
            status=MatchStatus.MATCHED,
            profile_id=created.profile_id,
            confidence=0.90,
            distance=0.10,
            reason="forced-low-confidence-owner-match",
        )

    monkeypatch.setattr("parking_spot_monitor.vehicle_history_profiles._match_vehicle_profile", low_confidence_owner_match)

    assignment = archive.match_or_create_profile(session_id=candidate.session_id)

    assert assignment.status == "unknown"
    assert assignment.profile_id is None
    assert assignment.profile_confidence is None
    assert assignment.reason == "owner-profile-confidence-too-low"
    assert profile_path.read_text(encoding="utf-8") == before_profile
    assert active_path.read_text(encoding="utf-8") == before_session
    records = logger_records(stream)
    assert any(
        record["event"] == "vehicle-session-profile-owner-match-skipped"
        and record["reason"] == "owner-profile-confidence-too-low"
        and record["profile_confidence"] == 0.90
        for record in records
    )


def test_ambiguous_profile_match_leaves_session_and_profiles_unchanged(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    base = archive.start_session(occupied_event(spot_id="base", observed_at="2026-05-18T13:00:00Z"))
    source = write_test_jpeg(tmp_path / "base.jpg", size=(96, 48), color=(90, 90, 90))
    archive.attach_occupied_images(session_id=base.session_id, source_frame_path=source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=base.session_id)
    assert created.profile_id is not None
    active_profiles = tmp_path / "vehicle-history" / "profiles" / "active"
    first_profile_path = active_profiles / f"{created.profile_id}.json"
    second_profile = json.loads(first_profile_path.read_text())
    second_profile["profile_id"] = "prof_duplicate_candidate"
    (active_profiles / "prof_duplicate_candidate.json").write_text(json.dumps(second_profile, allow_nan=False))
    before_first = first_profile_path.read_text()
    before_second = (active_profiles / "prof_duplicate_candidate.json").read_text()

    ambiguous = archive.start_session(occupied_event(spot_id="ambiguous", observed_at="2026-05-18T13:05:00Z"))
    ambiguous_source = write_test_jpeg(tmp_path / "ambiguous.jpg", size=(96, 48), color=(90, 90, 90))
    archive.attach_occupied_images(session_id=ambiguous.session_id, source_frame_path=ambiguous_source, bbox=(0, 0, 96, 48))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{ambiguous.session_id}.json"
    before_session = active_path.read_text()

    assignment = archive.match_or_create_profile(session_id=ambiguous.session_id)

    assert assignment.status == "ambiguous"
    assert assignment.profile_id is None
    assert assignment.profile_confidence is None
    assert active_path.read_text() == before_session
    assert first_profile_path.read_text() == before_first
    assert (active_profiles / "prof_duplicate_candidate.json").read_text() == before_second


def test_malformed_profile_json_is_quarantined_without_blocking_valid_profile_match(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="valid profile", observed_at="2026-05-18T13:00:00Z"))
    source = write_test_jpeg(tmp_path / "valid-profile.jpg", size=(96, 48), color=(120, 40, 40))
    archive.attach_occupied_images(session_id=first.session_id, source_frame_path=source, bbox=(0, 0, 96, 48))
    created = archive.match_or_create_profile(session_id=first.session_id)
    assert created.profile_id is not None
    archive.close_session(open_event(spot_id="valid profile", observed_at="2026-05-18T13:02:00Z"))
    bad_path = tmp_path / "vehicle-history" / "profiles" / "active" / "broken.json"
    bad_path.write_text("{not-json rtsp://camera.local access_token=supersecret raw_image_bytes")

    second = archive.start_session(occupied_event(spot_id="uses valid", observed_at="2026-05-18T13:03:00Z"))
    second_source = write_test_jpeg(tmp_path / "uses-valid.jpg", size=(96, 48), color=(122, 42, 42))
    archive.attach_occupied_images(session_id=second.session_id, source_frame_path=second_source, bbox=(0, 0, 96, 48))
    matched = archive.match_or_create_profile(session_id=second.session_id)
    snapshot = archive.health_snapshot()

    assert matched.profile_id == created.profile_id
    assert not bad_path.exists()
    assert len(list((tmp_path / "vehicle-history" / "profiles" / "quarantine").glob("broken.json.corrupt-*"))) == 1
    assert snapshot["profile_quarantine_count"] == 1
    assert snapshot["profile_count"] == 1
    assert snapshot["profile_sample_count"] == 2
    assert snapshot["last_vehicle_history_error"] is not None
    assert snapshot["last_vehicle_history_error"]["phase"] == "profile-load"
    records = logger_records(stream)
    assert any(record["event"] == "vehicle-profile-quarantined" for record in records)
    assert any(record["event"] == "vehicle-session-profile-failed" for record in records)
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered
    assert "raw_image_bytes" not in rendered


def test_profile_assignment_requires_occupied_crop_and_descriptor_failures_do_not_mutate_session(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    missing_crop = archive.start_session(occupied_event(spot_id="missing crop"))

    with pytest.raises(ArchiveWriteError, match="occupied_crop_path"):
        archive.match_or_create_profile(session_id=missing_crop.session_id)

    bad_crop = archive.start_session(occupied_event(spot_id="bad crop", observed_at="2026-05-18T13:05:00Z"))
    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{bad_crop.session_id}.json"
    raw = json.loads(active_path.read_text())
    raw["occupied_crop_path"] = str(tmp_path / "not-a-jpeg.txt")
    active_path.write_text(json.dumps(raw))
    (tmp_path / "not-a-jpeg.txt").write_text("not image bytes access_token=supersecret")
    before = active_path.read_text()

    with pytest.raises(ArchiveWriteError, match="file is unreadable"):
        archive.match_or_create_profile(session_id=bad_crop.session_id)

    assert active_path.read_text() == before
    snapshot = archive.health_snapshot()
    assert snapshot["last_vehicle_history_error"] is not None
    assert snapshot["last_vehicle_history_error"]["phase"] == "profile-match"
    assert snapshot["profile_unknown_session_count"] == 1


def test_estimate_for_profile_uses_closed_matching_sessions_and_excludes_weak_or_mismatched_history(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    first = archive.start_session(occupied_event(spot_id="estimate-a", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="estimate-a", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=first.session_id, profile_id="prof_repeat", profile_confidence=0.96)
    second = archive.start_session(occupied_event(spot_id="estimate-b", observed_at="2026-05-19T08:10:00Z"))
    archive.close_session(open_event(spot_id="estimate-b", observed_at="2026-05-19T09:15:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=second.session_id, profile_id="prof_repeat", profile_confidence=0.92)
    weak = archive.start_session(occupied_event(spot_id="estimate-weak", observed_at="2026-05-20T08:00:00Z"))
    archive.close_session(open_event(spot_id="estimate-weak", observed_at="2026-05-20T22:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=weak.session_id, profile_id="prof_repeat", profile_confidence=0.40)
    other = archive.start_session(occupied_event(spot_id="estimate-other", observed_at="2026-05-21T01:00:00Z"))
    archive.close_session(open_event(spot_id="estimate-other", observed_at="2026-05-21T23:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=other.session_id, profile_id="prof_other", profile_confidence=0.99)

    result = archive.estimate_for_profile("prof_repeat")

    assert result.status == "estimated"
    assert result.reason is None
    assert result.profile_id == "prof_repeat"
    assert result.sample_count == 2
    assert result.dwell_range is not None
    assert result.dwell_range.lower_seconds <= 3600
    assert result.dwell_range.upper_seconds >= 3900
    assert result.leave_time_window is not None
    assert result.leave_time_window.start_minute <= 9 * 60
    assert result.leave_time_window.end_minute >= 9 * 60 + 15


def test_estimate_for_profile_unknown_or_sparse_profile_returns_insufficient_history(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    closed = archive.start_session(occupied_event(spot_id="sparse", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="sparse", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=closed.session_id, profile_id="prof_repeat", profile_confidence=0.96)

    unknown = archive.estimate_for_profile(None)
    sparse = archive.estimate_for_profile("prof_repeat")

    assert unknown.status == "insufficient_history"
    assert unknown.reason == "unknown-profile"
    assert unknown.profile_id is None
    assert unknown.sample_count == 0
    assert sparse.status == "insufficient_history"
    assert sparse.reason == "insufficient-samples"
    assert sparse.profile_id == "prof_repeat"
    assert sparse.sample_count == 1


def test_estimate_for_session_uses_active_profile_but_never_counts_active_session(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    historical = archive.start_session(occupied_event(spot_id="historical", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="historical", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=historical.session_id, profile_id="prof_repeat", profile_confidence=0.96)
    active = archive.start_session(occupied_event(spot_id="current", observed_at="2026-05-19T08:00:00Z"))
    set_session_profile(tmp_path, archive_state="active", session_id=active.session_id, profile_id="prof_repeat", profile_confidence=1.0)

    result = archive.estimate_for_session(active.session_id)

    assert result.status == "insufficient_history"
    assert result.reason == "insufficient-samples"
    assert result.profile_id == "prof_repeat"
    assert result.sample_count == 1


def test_estimate_for_session_missing_or_unprofiled_active_session_returns_unknown_profile(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event(spot_id="unprofiled"))

    missing = archive.estimate_for_session("sess_missing")
    unprofiled = archive.estimate_for_session(active.session_id)

    assert missing.status == "insufficient_history"
    assert missing.reason == "unknown-profile"
    assert missing.profile_id is None
    assert missing.sample_count == 0
    assert unprofiled.status == "insufficient_history"
    assert unprofiled.reason == "unknown-profile"
    assert unprofiled.profile_id is None
    assert unprofiled.sample_count == 0


def test_estimate_helpers_preserve_closed_session_quarantine_and_module_convenience_api(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    first = archive.start_session(occupied_event(spot_id="valid-a", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="valid-a", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=first.session_id, profile_id="prof_repeat", profile_confidence=0.96)
    second = archive.start_session(occupied_event(spot_id="valid-b", observed_at="2026-05-19T08:00:00Z"))
    archive.close_session(open_event(spot_id="valid-b", observed_at="2026-05-19T09:05:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=second.session_id, profile_id="prof_repeat", profile_confidence=0.96)
    active = archive.start_session(occupied_event(spot_id="current", observed_at="2026-05-20T08:00:00Z"))
    set_session_profile(tmp_path, archive_state="active", session_id=active.session_id, profile_id="prof_repeat", profile_confidence=1.0)
    bad_path = tmp_path / "vehicle-history" / "sessions" / "closed" / "broken.json"
    bad_path.write_text("{not-json rtsp://camera.local access_token=supersecret raw_image_bytes")

    profile_result = archive.estimate_for_profile("prof_repeat")
    session_result = estimate_session_history(tmp_path, session_id=active.session_id)
    module_profile_result = estimate_profile_history(tmp_path, profile_id="prof_repeat")
    snapshot = archive.health_snapshot()

    assert profile_result.status == "estimated"
    assert profile_result.sample_count == 2
    assert session_result.status == "estimated"
    assert session_result.sample_count == 2
    assert module_profile_result.status == "estimated"
    assert module_profile_result.sample_count == 2
    assert not bad_path.exists()
    assert len(list((tmp_path / "vehicle-history" / "sessions" / "quarantine").glob("broken.json.corrupt-*"))) == 1
    assert snapshot["vehicle_history_failure_count"] == 1
    assert snapshot["last_vehicle_history_error"] is not None
    assert snapshot["last_vehicle_history_error"]["phase"] == "json-load"
    rendered = json.dumps(logger_records(stream))
    assert "supersecret" not in rendered
    assert "rtsp://camera.local" not in rendered
    assert "raw_image_bytes" not in rendered


def test_profile_corrections_rename_merge_summary_and_wrong_match_are_derived_only(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    source_a = archive.start_session(occupied_event(spot_id="source-a", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="source-a", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=source_a.session_id, profile_id="prof_source", profile_confidence=0.96)
    source_b = archive.start_session(occupied_event(spot_id="source-b", observed_at="2026-05-19T08:05:00Z"))
    archive.close_session(open_event(spot_id="source-b", observed_at="2026-05-19T09:05:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=source_b.session_id, profile_id="prof_source", profile_confidence=0.96)
    target = archive.start_session(occupied_event(spot_id="target", observed_at="2026-05-20T08:10:00Z"))
    archive.close_session(open_event(spot_id="target", observed_at="2026-05-20T09:15:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=target.session_id, profile_id="prof_target", profile_confidence=0.96)
    active = archive.start_session(occupied_event(spot_id="active-target", observed_at="2026-05-21T08:00:00Z"))
    set_session_profile(tmp_path, archive_state="active", session_id=active.session_id, profile_id="prof_source", profile_confidence=1.0)
    raw_before = (tmp_path / "vehicle-history" / "sessions" / "closed" / f"{source_a.session_id}.json").read_text()

    archive.rename_profile("prof_target", "Blue hatchback", matrix_event_id="$event", matrix_sender="@operator:example", matrix_room_id="!room:example")
    archive.merge_profiles("prof_source", "prof_target")
    archive.mark_wrong_match(source_b.session_id, profile_id="prof_target")

    profile_estimate = archive.estimate_for_profile("prof_target")
    session_estimate = archive.estimate_for_session(active.session_id)
    summary = archive.profile_summary("prof_source")
    raw_after = (tmp_path / "vehicle-history" / "sessions" / "closed" / f"{source_a.session_id}.json").read_text()
    event_lines = (tmp_path / "vehicle-history" / "corrections" / "events.jsonl").read_text().splitlines()

    assert archive.resolve_profile_id("prof_source") == "prof_target"
    assert archive.effective_label("prof_source") == "Blue hatchback"
    assert profile_estimate.status == "estimated"
    assert profile_estimate.profile_id == "prof_target"
    assert profile_estimate.sample_count == 2
    assert session_estimate.status == "estimated"
    assert session_estimate.profile_id == "prof_target"
    assert session_estimate.sample_count == 2
    assert summary == {
        "profile_id": "prof_target",
        "requested_profile_id": "prof_source",
        "label": "Blue hatchback",
        "closed_session_count": 2,
        "active_session_count": 1,
        "wrong_match_excluded_session_count": 1,
        "merged_profile_ids": ["prof_source"],
        "estimate_status": "estimated",
        "estimate_reason": None,
        "estimate_sample_count": 2,
        "estimate_confidence": profile_estimate.confidence,
    }
    assert raw_after == raw_before
    assert len(event_lines) == 4
    rendered_summary = json.dumps(summary)
    assert "snapshot" not in rendered_summary
    assert "crop" not in rendered_summary
    assert "descriptor" not in rendered_summary


def test_profile_summary_scans_closed_sessions_once_and_preserves_estimate_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    first = archive.start_session(occupied_event(spot_id="profile-a", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="profile-a", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=first.session_id, profile_id="prof_a", profile_confidence=0.96)
    second = archive.start_session(occupied_event(spot_id="profile-b", observed_at="2026-05-19T08:10:00Z"))
    archive.close_session(open_event(spot_id="profile-b", observed_at="2026-05-19T09:15:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=second.session_id, profile_id="prof_a", profile_confidence=0.92)
    expected = archive.estimate_for_profile("prof_a")
    original_list_closed_sessions = archive.list_closed_sessions
    closed_session_scans = 0

    def counted_list_closed_sessions() -> list[Any]:
        nonlocal closed_session_scans
        closed_session_scans += 1
        return original_list_closed_sessions()

    monkeypatch.setattr(archive, "list_closed_sessions", counted_list_closed_sessions)

    summary = archive.profile_summary("prof_a")

    assert closed_session_scans == 1
    assert summary["estimate_status"] == expected.status == "estimated"
    assert summary["estimate_reason"] == expected.reason is None
    assert summary["estimate_sample_count"] == expected.sample_count == 2
    assert summary["estimate_confidence"] == expected.confidence


def test_correction_validation_rejects_unknown_ids_oversized_labels_and_merge_cycles_without_appending(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    source = archive.start_session(occupied_event(spot_id="known-source", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="known-source", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=source.session_id, profile_id="prof_source", profile_confidence=0.96)
    target = archive.start_session(occupied_event(spot_id="known-target", observed_at="2026-05-19T08:00:00Z"))
    archive.close_session(open_event(spot_id="known-target", observed_at="2026-05-19T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=target.session_id, profile_id="prof_target", profile_confidence=0.96)

    with pytest.raises(ArchiveSchemaError, match="unknown profile_id"):
        archive.rename_profile("prof_missing", "Missing")
    with pytest.raises(ArchiveSchemaError, match="exceeds maximum length"):
        archive.rename_profile("prof_source", "x" * 161)
    with pytest.raises(ArchiveSchemaError, match="unknown session_id"):
        archive.mark_wrong_match("sess_missing")
    archive.merge_profiles("prof_source", "prof_target")
    with pytest.raises(ArchiveSchemaError, match="profile merge cycle detected"):
        archive.merge_profiles("prof_target", "prof_source")

    event_lines = (tmp_path / "vehicle-history" / "corrections" / "events.jsonl").read_text().splitlines()
    assert len(event_lines) == 1
    assert json.loads(event_lines[0])["action"] == "merge_profiles"


def test_malformed_correction_jsonl_is_quarantined_and_health_reports_metadata(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    closed = archive.start_session(occupied_event(spot_id="health", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="health", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(tmp_path, archive_state="closed", session_id=closed.session_id, profile_id="prof_health", profile_confidence=0.96)
    archive.rename_profile("prof_health", "Silver sedan")
    corrections_path = tmp_path / "vehicle-history" / "corrections" / "events.jsonl"
    with corrections_path.open("a", encoding="utf-8") as handle:
        handle.write("{not-json rtsp://camera.local access_token=supersecret raw_image_bytes\n")
    archive.write_matrix_cursor({"next_batch": "s123"})

    loaded = archive.load_corrections()
    loaded_again = archive.load_corrections()
    snapshot = archive.health_snapshot()

    assert [event.action for event in loaded] == ["rename_profile"]
    assert [event.action for event in loaded_again] == ["rename_profile"]
    assert snapshot["correction_count"] == 1
    assert snapshot["correction_invalid_count"] == 1
    assert snapshot["correction_quarantine_count"] == 1
    assert snapshot["last_correction_action"] == "rename_profile"
    assert snapshot["last_correction_created_at"] is not None
    assert snapshot["matrix_command_cursor_present"] is True
    assert archive.read_matrix_cursor() == {"next_batch": "s123"}
    quarantine_path = tmp_path / "vehicle-history" / "corrections" / "quarantine.jsonl"
    assert quarantine_path.exists()
    rendered_logs = json.dumps(logger_records(stream))
    assert "supersecret" not in rendered_logs
    assert "rtsp://camera.local" not in rendered_logs
    assert "raw_image_bytes" not in rendered_logs


def test_correction_replay_is_cached_until_revision_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    correction_reads = 0
    original_open = Path.open

    def counted(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal correction_reads
        if path == archive.corrections_path and args and args[0] == "rb":
            correction_reads += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted)

    first = archive.correction_replay_state()
    second = archive.correction_replay_state()

    assert second is first
    assert correction_reads == 1
    assert archive.correction_revision() == 0

    archive._bump_correction_revision()
    third = archive.correction_replay_state()

    assert third is not second
    assert correction_reads == 2
    assert archive.correction_revision() == 1


def test_correction_event_seen_reuses_replay_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    closed = archive.start_session(occupied_event(spot_id="cache", observed_at="2026-05-18T08:00:00Z"))
    archive.close_session(open_event(spot_id="cache", observed_at="2026-05-18T09:00:00Z"))
    set_session_profile(
        tmp_path,
        archive_state="closed",
        session_id=closed.session_id,
        profile_id="prof_cache",
        profile_confidence=0.96,
    )
    archive.rename_profile("prof_cache", "Blue car", matrix_event_id="$event")
    correction_reads = 0
    original_open = Path.open

    def counted(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal correction_reads
        if path == archive.corrections_path and args and args[0] == "rb":
            correction_reads += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted)

    assert archive.correction_event_seen("$event") is True
    assert archive.correction_event_seen("$event") is True
    assert archive.correction_event_seen("$missing") is False
    assert correction_reads == 1


def test_successful_correction_append_bumps_explicit_revision(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    closed = archive.start_session(occupied_event(spot_id="revision"))
    archive.close_session(open_event(spot_id="revision"))
    set_session_profile(
        tmp_path,
        archive_state="closed",
        session_id=closed.session_id,
        profile_id="prof_revision",
        profile_confidence=0.96,
    )

    archive.rename_profile("prof_revision", "Revision")

    assert archive.correction_revision() == 1


def test_external_same_process_correction_replace_invalidates_signature_cache(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    original_payload = {
        "schema_version": 1,
        "correction_id": "corr_external",
        "action": "rename_profile",
        "created_at": "2026-05-18T14:45:00Z",
        "matrix_event_id": None,
        "matrix_sender": None,
        "matrix_room_id": None,
        "profile_id": "prof_external",
        "label": "Old",
    }
    archive.corrections_path.write_text(json.dumps(original_payload, sort_keys=True) + "\n", encoding="utf-8")
    first = archive.correction_replay_state()
    assert archive.correction_replay_state() is first
    original_stat = archive.corrections_path.stat()
    replacement_payload = {**original_payload, "label": "New"}
    replacement_path = archive.corrections_dir / "replacement.jsonl"
    replacement_path.write_text(json.dumps(replacement_payload, sort_keys=True) + "\n", encoding="utf-8")
    os.utime(
        replacement_path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns + 1_000_000),
    )
    os.replace(replacement_path, archive.corrections_path)

    assert archive.corrections_path.stat().st_size == original_stat.st_size
    second = archive.correction_replay_state()

    assert second is not first
    assert dict(second.labels) == {"prof_external": "New"}


def test_correction_replay_does_not_cache_state_read_before_external_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    old_payload = {
        "schema_version": 1,
        "correction_id": "corr_overlap",
        "action": "rename_profile",
        "created_at": "2026-05-18T14:45:00Z",
        "matrix_event_id": None,
        "matrix_sender": None,
        "matrix_room_id": None,
        "profile_id": "prof_overlap",
        "label": "Old",
    }
    archive.corrections_path.write_text(json.dumps(old_payload, sort_keys=True) + "\n", encoding="utf-8")
    old_stat = archive.corrections_path.stat()
    replacement_path = archive.corrections_dir / "overlap-replacement.jsonl"
    replacement_path.write_text(
        json.dumps({**old_payload, "label": "New"}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.utime(replacement_path, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000))
    original_open = Path.open
    correction_reads = 0
    replaced = False

    class ReplaceAfterCorrectionRead:
        def __init__(self, handle: Any) -> None:
            self.handle = handle

        def __enter__(self) -> Any:
            return self.handle.__enter__()

        def __exit__(self, *args: Any) -> Any:
            nonlocal replaced
            result = self.handle.__exit__(*args)
            if not replaced:
                os.replace(replacement_path, archive.corrections_path)
                replaced = True
            return result

    def replace_after_read(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal correction_reads
        handle = original_open(path, *args, **kwargs)
        if path == archive.corrections_path and args and args[0] == "rb":
            correction_reads += 1
            return ReplaceAfterCorrectionRead(handle)
        return handle

    monkeypatch.setattr(Path, "open", replace_after_read)

    old = archive.correction_replay_state()
    new = archive.correction_replay_state()
    stable = archive.correction_replay_state()

    assert dict(old.labels) == {"prof_overlap": "Old"}
    assert dict(new.labels) == {"prof_overlap": "New"}
    assert stable is new
    assert correction_reads == 2


def test_correction_replay_recomputes_after_transient_signature_stat_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("", encoding="utf-8")
    first = archive.correction_replay_state()
    correction_reads = 0
    original_open = Path.open
    original_stat = Path.stat
    failed = False

    def counted(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal correction_reads
        if path == archive.corrections_path and args and args[0] == "rb":
            correction_reads += 1
        return original_open(path, *args, **kwargs)

    def fail_signature_stat_once(path: Path, *args: Any, **kwargs: Any) -> os.stat_result:
        nonlocal failed
        if path == archive.corrections_path and not failed:
            failed = True
            raise PermissionError("transient signature failure")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted)
    monkeypatch.setattr(Path, "stat", fail_signature_stat_once)

    rebuilt = archive.correction_replay_state()
    recovered = archive.correction_replay_state()
    stable = archive.correction_replay_state()

    assert rebuilt is not first
    assert recovered is not rebuilt
    assert stable is recovered
    assert correction_reads == 2


def test_correction_replay_does_not_cache_transient_read_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    payload = {
        "schema_version": 1,
        "correction_id": "corr_read_retry",
        "action": "profile_summary_requested",
        "created_at": "2026-05-18T14:45:00Z",
        "matrix_event_id": None,
        "matrix_sender": None,
        "matrix_room_id": None,
        "profile_id": "prof_retry",
    }
    archive.corrections_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    original_open = Path.open
    read_attempts = 0

    def fail_read_once(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal read_attempts
        if path == archive.corrections_path and args and args[0] == "rb":
            read_attempts += 1
            if read_attempts == 1:
                raise PermissionError("transient correction read failure")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_read_once)

    failed = archive.correction_replay_state()
    recovered = archive.correction_replay_state()
    stable = archive.correction_replay_state()

    assert failed.valid_count == 0
    assert recovered.valid_count == 1
    assert stable is recovered
    assert read_attempts == 2


def test_correction_replay_retries_after_combined_stat_and_read_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    payload = {
        "schema_version": 1,
        "correction_id": "corr_combined_retry",
        "action": "profile_summary_requested",
        "created_at": "2026-05-18T14:45:00Z",
        "matrix_event_id": None,
        "matrix_sender": None,
        "matrix_room_id": None,
        "profile_id": "prof_retry",
    }
    archive.corrections_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    original = archive.correction_replay_state()
    original_open = Path.open
    original_stat = Path.stat
    read_attempts = 0
    stat_failed = False

    def fail_stat_once(path: Path, *args: Any, **kwargs: Any) -> os.stat_result:
        nonlocal stat_failed
        if path == archive.corrections_path and not stat_failed:
            stat_failed = True
            raise PermissionError("transient signature failure")
        return original_stat(path, *args, **kwargs)

    def fail_read_once(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal read_attempts
        if path == archive.corrections_path and args and args[0] == "rb":
            read_attempts += 1
            if read_attempts == 1:
                raise PermissionError("transient correction read failure")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", fail_stat_once)
    monkeypatch.setattr(Path, "open", fail_read_once)

    failed = archive.correction_replay_state()
    recovered = archive.correction_replay_state()

    assert failed.valid_count == 0
    assert recovered.valid_count == 1
    assert recovered is not original
    assert read_attempts == 2


def test_malformed_correction_quarantine_signature_is_stable_after_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("{not-json\n", encoding="utf-8")
    correction_reads = 0
    original_open = Path.open

    def counted(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal correction_reads
        if path == archive.corrections_path and args and args[0] == "rb":
            correction_reads += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted)

    first = archive.correction_replay_state()
    second = archive.correction_replay_state()

    assert first.invalid_count == 1
    assert second is first
    assert correction_reads == 1
    assert archive.correction_revision() == 1
    assert archive.corrections_quarantine_path.read_text(encoding="utf-8").count("\n") == 1


def test_successful_new_correction_quarantine_bumps_both_revisions_exactly_once(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("{not-json\n", encoding="utf-8")
    mutation_before = archive.mutation_revision()
    correction_before = archive.correction_revision()

    replay = archive.correction_replay_state()

    assert replay.quarantine_count == 1
    assert archive.mutation_revision() == mutation_before + 1
    assert archive.correction_revision() == correction_before + 1


def test_failed_correction_quarantine_write_bumps_neither_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("{not-json\n", encoding="utf-8")
    original_open = Path.open

    def fail_quarantine_write(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path == archive.corrections_quarantine_path and args and args[0] == "a":
            raise PermissionError("quarantine denied")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_quarantine_write)
    mutation_before = archive.mutation_revision()
    correction_before = archive.correction_revision()

    replay = archive.correction_replay_state()

    assert replay.quarantine_count == 0
    assert archive.mutation_revision() == mutation_before
    assert archive.correction_revision() == correction_before


def test_deduplicated_correction_quarantine_bumps_neither_revision(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("{not-json\n", encoding="utf-8")
    archive.corrections_quarantine_path.write_text(
        json.dumps(
            {
                "line_number": 1,
                "quarantined_at": "2026-05-18T14:45:00Z",
                "reason": "JSONDecodeError",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    mutation_before = archive.mutation_revision()
    correction_before = archive.correction_revision()

    replay = archive.correction_replay_state()

    assert replay.quarantine_count == 1
    assert archive.mutation_revision() == mutation_before
    assert archive.correction_revision() == correction_before


def test_correction_replay_retries_after_quarantine_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("{not-json\n", encoding="utf-8")
    original_open = Path.open
    correction_reads = 0
    write_failed = False

    def fail_quarantine_write_once(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal correction_reads, write_failed
        if path == archive.corrections_path and args and args[0] == "rb":
            correction_reads += 1
        if path == archive.corrections_quarantine_path and args and args[0] == "a" and not write_failed:
            write_failed = True
            raise PermissionError("transient quarantine write failure")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_quarantine_write_once)

    failed = archive.correction_replay_state()
    recovered = archive.correction_replay_state()
    stable = archive.correction_replay_state()

    assert failed.invalid_count == 0
    assert recovered.invalid_count == 1
    assert stable is recovered
    assert correction_reads == 2


def test_correction_replay_retries_after_quarantine_read_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("{not-json\n", encoding="utf-8")
    archive.corrections_quarantine_path.write_text(
        json.dumps({"line_number": 1, "reason": "JSONDecodeError", "quarantined_at": "2026-05-18T14:45:00Z"}) + "\n",
        encoding="utf-8",
    )
    original_open = Path.open
    correction_reads = 0
    read_failed = False

    def fail_quarantine_read_once(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal correction_reads, read_failed
        if path == archive.corrections_path and args and args[0] == "rb":
            correction_reads += 1
        if path == archive.corrections_quarantine_path and args and args[0] == "r" and not read_failed:
            read_failed = True
            raise PermissionError("transient quarantine read failure")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_quarantine_read_once)

    first = archive.correction_replay_state()
    recovered = archive.correction_replay_state()
    stable = archive.correction_replay_state()

    assert first.invalid_count == 2
    assert recovered.invalid_count == 2
    assert stable is recovered
    assert correction_reads == 2


def test_correction_replay_retries_after_quarantine_count_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("", encoding="utf-8")
    archive.corrections_quarantine_path.write_text(
        json.dumps({"line_number": 1, "reason": "JSONDecodeError", "quarantined_at": "2026-05-18T14:45:00Z"}) + "\n",
        encoding="utf-8",
    )
    original_open = Path.open
    correction_reads = 0
    count_failed = False

    def fail_quarantine_count_once(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal correction_reads, count_failed
        if path == archive.corrections_path and args and args[0] == "rb":
            correction_reads += 1
        if path == archive.corrections_quarantine_path and args and args[0] == "r" and not count_failed:
            count_failed = True
            raise PermissionError("transient quarantine count failure")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_quarantine_count_once)

    failed = archive.correction_replay_state()
    recovered = archive.correction_replay_state()
    stable = archive.correction_replay_state()

    assert failed.invalid_count == 0
    assert recovered.invalid_count == 1
    assert stable is recovered
    assert correction_reads == 2


def test_cached_correction_replay_mappings_are_immutable(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    state = archive.correction_replay_state()

    with pytest.raises(TypeError):
        state.labels["prof_source"] = "Changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        state.merges["prof_source"] = "prof_target"  # type: ignore[index]
    with pytest.raises(TypeError):
        state.canonical_profile_ids["prof_source"] = "prof_target"  # type: ignore[index]


def test_canonical_profile_map_compresses_merge_chains_and_detects_cycles() -> None:
    assert vehicle_history_corrections._canonical_profile_map(
        {"prof_a": "prof_b", "prof_b": "prof_c", "prof_c": "prof_d"}
    ) == {
        "prof_a": "prof_d",
        "prof_b": "prof_d",
        "prof_c": "prof_d",
        "prof_d": "prof_d",
    }

    with pytest.raises(ArchiveSchemaError, match="profile merge cycle detected"):
        vehicle_history_corrections._canonical_profile_map({"prof_a": "prof_b", "prof_b": "prof_a"})


def test_effective_sessions_use_precompressed_canonical_profile_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event(spot_id="compressed"))
    set_session_profile(
        tmp_path,
        archive_state="active",
        session_id=active.session_id,
        profile_id="prof_a",
        profile_confidence=0.96,
    )
    record = archive.load_active_sessions()[0]
    state = CorrectionReplayState(
        labels=MappingProxyType({}),
        merges=MappingProxyType({"prof_a": "prof_b", "prof_b": "prof_c"}),
        canonical_profile_ids=MappingProxyType(
            {"prof_a": "prof_c", "prof_b": "prof_c", "prof_c": "prof_c"}
        ),
        wrong_match_session_ids=frozenset(),
        valid_count=2,
        invalid_count=0,
        quarantine_count=0,
        last_action="merge_profiles",
        last_created_at="2026-05-18T14:45:00Z",
    )
    monkeypatch.setattr(
        archive,
        "resolve_profile_id",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected merge-chain walk")),
    )

    effective = archive._effective_sessions([record], state=state)

    assert effective[0].profile_id == "prof_c"


def test_export_archive_writes_tar_bundle_and_safe_maintenance_manifest(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    old = archive.start_session(occupied_event(spot_id="export-old", observed_at="2026-05-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="export-old", observed_at="2026-05-01T09:00:00Z"))
    active = archive.start_session(occupied_event(spot_id="export-active", observed_at="2026-05-02T08:00:00Z"))
    image_dir = tmp_path / "vehicle-history" / "images" / "occupied-full"
    image_dir.mkdir(parents=True)
    image_path = image_dir / f"{old.session_id}.jpg"
    image_path.write_bytes(b"explicit operator bundle may contain image bytes")

    output = tmp_path / "vehicle-history-export.tar.gz"
    result = archive.export_archive(output)

    assert output.exists()
    assert result.status == "ok"
    assert result.retention_policy == "indefinite"
    assert result.active_session_count == 1
    assert result.closed_session_count == 1
    assert result.member_count == len(result.member_names)
    assert "vehicle-history/sessions/closed/" + old.session_id + ".json" in result.member_names
    assert "vehicle-history/sessions/active/" + active.session_id + ".json" in result.member_names
    assert "vehicle-history/images/occupied-full/" + image_path.name in result.member_names
    assert any(name.startswith("vehicle-history/metadata/maintenance/export-") for name in result.member_names)
    with tarfile.open(output, "r:gz") as bundle:
        names = bundle.getnames()
        assert sorted(names) == sorted(result.member_names)
        manifest_name = next(name for name in names if name.startswith("vehicle-history/metadata/maintenance/export-"))
        manifest_file = bundle.extractfile(manifest_name)
        assert manifest_file is not None
        bundle_manifest = json.loads(manifest_file.read().decode("utf-8"))
    disk_manifest = json.loads(Path(result.manifest_path).read_text())
    assert disk_manifest["operation"] == "export"
    assert disk_manifest["member_names"] == list(result.member_names)
    assert bundle_manifest["member_names"] == list(result.member_names)
    rendered = json.dumps(result.to_json_dict()) + json.dumps(disk_manifest)
    assert "explicit operator bundle may contain image bytes" not in rendered
    assert "raw_image_bytes" not in rendered
    assert archive.health_snapshot()["last_maintenance_metadata"]["operation"] == "export"


def test_prune_closed_sessions_dry_run_apply_and_reference_safety(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    old = archive.start_session(occupied_event(spot_id="old", observed_at="2026-01-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="old", observed_at="2026-01-01T09:00:00Z"))
    retained = archive.start_session(occupied_event(spot_id="retained", observed_at="2026-05-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="retained", observed_at="2026-05-01T09:00:00Z"))
    active = archive.start_session(occupied_event(spot_id="active", observed_at="2026-05-02T08:00:00Z"))
    full_dir = tmp_path / "vehicle-history" / "images" / "occupied-full"
    crop_dir = tmp_path / "vehicle-history" / "images" / "occupied-crops"
    full_dir.mkdir(parents=True)
    crop_dir.mkdir(parents=True)
    old_image = full_dir / "old-shared.jpg"
    old_crop = crop_dir / "old-only.jpg"
    retained_image = full_dir / "retained-shared.jpg"
    old_image.write_bytes(b"old shared")
    old_crop.write_bytes(b"old crop")
    retained_image.write_bytes(b"retained shared")

    for archive_state, session_id, full_path, crop_path in [
        ("closed", old.session_id, old_image, old_crop),
        ("closed", retained.session_id, retained_image, retained_image),
        ("active", active.session_id, old_image, retained_image),
    ]:
        path = tmp_path / "vehicle-history" / "sessions" / archive_state / f"{session_id}.json"
        payload = json.loads(path.read_text())
        payload["occupied_snapshot_path"] = str(full_path)
        payload["occupied_crop_path"] = str(crop_path)
        path.write_text(json.dumps(payload, allow_nan=False))

    cutoff = "2026-02-01T00:00:00Z"
    dry = archive.prune_closed_sessions(older_than=cutoff, dry_run=True)

    assert dry.status == "dry_run"
    assert dry.candidate_session_count == 1
    assert dry.pruned_file_count == 2  # old session JSON + unshared crop only
    assert dry.skipped_active_session_count == 1
    assert dry.skipped_retained_image_count == 1
    assert (tmp_path / "vehicle-history" / "sessions" / "closed" / f"{old.session_id}.json").exists()
    assert old_crop.exists()

    applied = archive.prune_closed_sessions(older_than=cutoff, dry_run=False)

    assert applied.status == "ok"
    assert applied.candidate_session_count == 1
    assert not (tmp_path / "vehicle-history" / "sessions" / "closed" / f"{old.session_id}.json").exists()
    assert not old_crop.exists()
    assert old_image.exists()  # still referenced by active session
    assert retained_image.exists()
    assert (tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json").exists()
    assert (tmp_path / "vehicle-history" / "sessions" / "closed" / f"{retained.session_id}.json").exists()
    assert archive.health_snapshot()["last_maintenance_metadata"]["operation"] == "prune"


def test_prune_closed_sessions_deduplicates_image_refs_without_list_membership_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import parking_spot_monitor.vehicle_history_maintenance as maintenance

    class HashOnlyPath:
        def __init__(self, path: Path) -> None:
            self.path = path

        def __fspath__(self) -> str:
            return os.fspath(self.path)

        def __hash__(self) -> int:
            return hash(self.path)

        def __eq__(self, other: object) -> bool:
            raise AssertionError("deduplication should use a hash set, not repeated list membership")

        def stat(self) -> os.stat_result:
            return self.path.stat()

        def is_file(self) -> bool:
            return self.path.is_file()

        def unlink(self, *, missing_ok: bool = False) -> None:
            self.path.unlink(missing_ok=missing_ok)

    archive = VehicleHistoryArchive(tmp_path)
    old = archive.start_session(occupied_event(spot_id="old", observed_at="2026-01-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="old", observed_at="2026-01-01T09:00:00Z"))
    image_path = tmp_path / "vehicle-history" / "images" / "occupied-full" / "old.jpg"
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"old image")
    monkeypatch.setattr(maintenance, "_record_archive_image_paths", lambda root, record: [HashOnlyPath(image_path), HashOnlyPath(image_path)])

    result = archive.prune_closed_sessions(older_than="2026-02-01T00:00:00Z", dry_run=True)

    assert result.pruned_file_count == 2


def test_resolve_profile_id_uses_provided_merge_mapping_without_copying(tmp_path: Path) -> None:
    class LookupOnlyMerges:
        def __contains__(self, key: object) -> bool:
            return key == "prof_source"

        def __getitem__(self, key: str) -> str:
            if key == "prof_source":
                return "prof_target"
            raise KeyError(key)

        def __iter__(self) -> Any:
            raise AssertionError("provided merge mapping should not be copied")

        def __len__(self) -> int:
            return 1

    archive = VehicleHistoryArchive(tmp_path)

    assert archive.resolve_profile_id("prof_source", merges=LookupOnlyMerges()) == "prof_target"


def test_prune_counts_missing_image_refs_and_rejects_invalid_cutoff(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    old = archive.start_session(occupied_event(spot_id="missing", observed_at="2026-01-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="missing", observed_at="2026-01-01T09:00:00Z"))
    path = tmp_path / "vehicle-history" / "sessions" / "closed" / f"{old.session_id}.json"
    payload = json.loads(path.read_text())
    payload["occupied_snapshot_path"] = str(tmp_path / "vehicle-history" / "images" / "occupied-full" / "missing.jpg")
    path.write_text(json.dumps(payload, allow_nan=False))

    result = archive.prune_closed_sessions(older_than="2026-02-01T00:00:00Z", dry_run=True)

    assert result.missing_file_count == 1
    with pytest.raises(ArchiveSchemaError, match="ISO timestamp"):
        archive.prune_closed_sessions(older_than="not a date", dry_run=True)
    with pytest.raises(ArchiveSchemaError, match="non-negative"):
        cutoff_older_than_days(-1)


def test_clamp_crop_box_uses_floor_ceil_and_image_bounds() -> None:
    clamped = clamp_crop_box((-1.9, 2.1, 5.2, 8.8), (6, 7))

    assert clamped.as_pillow_box == (0, 2, 6, 7)
