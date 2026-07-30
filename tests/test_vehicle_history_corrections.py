from __future__ import annotations

from tests.support._vehicle_history import *  # noqa: F403


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
