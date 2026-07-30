from __future__ import annotations

from tests.support._vehicle_history import *  # noqa: F403


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
