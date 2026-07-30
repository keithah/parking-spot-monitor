from __future__ import annotations

import json
from pathlib import Path
import threading
from typing import Any

import pytest

from parking_spot_monitor.vehicle_history import VehicleHistoryArchive
from parking_spot_monitor import vehicle_history_correction_compaction, vehicle_history_correction_io
from parking_spot_monitor.vehicle_history_correction_io import compact_correction_events, load_correction_events
from parking_spot_monitor.vehicle_history_models import (
    MAX_CORRECTION_INVALID_LINES,
    ArchiveSchemaError,
    ProfileCorrectionEvent,
)


def _event_payload(index: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "correction_id": f"correction-{index}",
        "action": "profile_summary_requested",
        "created_at": "2026-07-30T00:00:00Z",
        "matrix_event_id": f"$event-{index}",
        "matrix_sender": "@operator:example.org",
        "matrix_room_id": "!room:example.org",
        "profile_id": "prof_a",
        "label": None,
        "source_profile_id": None,
        "target_profile_id": None,
        "session_id": None,
    }


def test_replay_loads_quarantine_index_once_for_many_invalid_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text("{bad-one\n{bad-two\n{bad-three\n", encoding="utf-8")
    reads = 0
    original_open = Path.open

    def counted(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal reads
        if path == archive.corrections_quarantine_path and args and args[0] == "r":
            reads += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted)

    replay = archive.correction_replay_state()

    assert replay.invalid_count == 3
    assert reads <= 1


def test_replay_stops_at_event_and_invalid_limits(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        "".join(json.dumps(_event_payload(index)) + "\n" for index in range(3))
        + "{bad-one\n{bad-two\n",
        encoding="utf-8",
    )
    quarantined: list[tuple[int, str]] = []

    result = load_correction_events(
        path,
        max_line_bytes=16_000,
        max_file_bytes=1_000_000,
        max_events=2,
        max_invalid_lines=1,
        quarantine_path=tmp_path / "quarantine.jsonl",
        quarantine_line=lambda **item: quarantined.append((item["line_number"], item["reason"])),
        record_failure=lambda **_fields: None,
    )

    assert len(result.events) == 2
    assert result.succeeded is False
    assert quarantined == []


def test_replay_rejects_oversized_ledger_before_opening_it(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_bytes(b"x" * 33)
    opened = False

    def quarantine(**_fields: object) -> None:
        raise AssertionError("oversized ledger must not be parsed")

    result = load_correction_events(
        path,
        max_line_bytes=16,
        max_file_bytes=32,
        max_events=10,
        max_invalid_lines=2,
        quarantine_path=tmp_path / "quarantine.jsonl",
        quarantine_line=quarantine,
        record_failure=lambda **_fields: None,
    )

    assert opened is False
    assert result.events == ()
    assert result.succeeded is False


def test_compaction_rewrites_only_bounded_valid_events(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    events = tuple(ProfileCorrectionEvent.from_json_dict(_event_payload(index)) for index in range(2))
    path.write_text("stale and invalid\n" + "x" * 2_000, encoding="utf-8")

    assert compact_correction_events(
        path,
        events,
        max_file_bytes=10_000,
        record_failure=lambda **_fields: None,
    )

    assert [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()] == [
        event.to_json_dict() for event in events
    ]
    assert path.stat().st_size < 2_000


def test_replay_limit_failure_blocks_state_and_new_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    original = "{invalid\n" * (MAX_CORRECTION_INVALID_LINES + 1)
    archive.corrections_path.write_text(original, encoding="utf-8")
    event = ProfileCorrectionEvent.from_json_dict(_event_payload(99_999))
    monkeypatch.setattr(
        type(archive),
        "_validate_correction_against_archive",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(ArchiveSchemaError, match="correction replay unavailable"):
        archive.append_correction(event)

    assert archive.corrections_path.read_text(encoding="utf-8") == original


def test_legacy_summary_audit_at_event_cap_is_compacted_before_real_correction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    archive.corrections_dir.mkdir(parents=True)
    archive.corrections_path.write_text(
        "".join(json.dumps(_event_payload(index)) + "\n" for index in range(10_000)),
        encoding="utf-8",
    )
    rename_payload = _event_payload(20_000)
    rename_payload.update(
        action="rename_profile",
        label="Blue hatchback",
        matrix_event_id="$real-correction",
    )
    event = ProfileCorrectionEvent.from_json_dict(rename_payload)
    monkeypatch.setattr(
        type(archive),
        "_validate_correction_against_archive",
        lambda *_args, **_kwargs: None,
    )

    assert archive.append_correction(event) == event

    lines = archive.corrections_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["action"] == "rename_profile"
    assert archive.correction_replay_state().labels["prof_a"] == "Blue hatchback"


def test_legacy_compaction_serializes_concurrent_append_without_record_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_archive = VehicleHistoryArchive(tmp_path)
    second_archive = VehicleHistoryArchive(tmp_path)
    first_archive.corrections_dir.mkdir(parents=True)
    first_archive.corrections_path.write_text(json.dumps(_event_payload(1)) + "\n", encoding="utf-8")
    first_payload = _event_payload(2)
    first_payload.update(action="rename_profile", label="First", matrix_event_id="$first")
    second_payload = _event_payload(3)
    second_payload.update(action="rename_profile", label="Second", matrix_event_id="$second")
    first_event = ProfileCorrectionEvent.from_json_dict(first_payload)
    second_event = ProfileCorrectionEvent.from_json_dict(second_payload)
    monkeypatch.setattr(type(first_archive), "_validate_correction_against_archive", lambda *_args, **_kwargs: None)
    real_compact = vehicle_history_correction_compaction.compact_correction_events
    real_flock = vehicle_history_correction_io.fcntl.flock
    compaction_entered = threading.Event()
    release_compaction = threading.Event()
    concurrent_lock_attempted = threading.Event()
    failures: list[BaseException] = []

    def paused_compaction(*args: Any, **kwargs: Any) -> bool:
        compaction_entered.set()
        if not release_compaction.wait(2):
            raise AssertionError("test did not release legacy compaction")
        return real_compact(*args, **kwargs)

    def observed_flock(descriptor: int, operation: int) -> None:
        if threading.current_thread().name == "concurrent-correction" and operation & vehicle_history_correction_io.fcntl.LOCK_EX:
            concurrent_lock_attempted.set()
        real_flock(descriptor, operation)

    def append(archive: VehicleHistoryArchive, event: ProfileCorrectionEvent) -> None:
        try:
            archive.append_correction(event)
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(vehicle_history_correction_compaction, "compact_correction_events", paused_compaction)
    monkeypatch.setattr(vehicle_history_correction_io.fcntl, "flock", observed_flock)
    first = threading.Thread(target=append, args=(first_archive, first_event), name="legacy-compaction")
    second = threading.Thread(target=append, args=(second_archive, second_event), name="concurrent-correction")
    first.start()
    assert compaction_entered.wait(2)
    second.start()
    assert concurrent_lock_attempted.wait(2)
    release_compaction.set()
    first.join(2)
    second.join(2)

    assert not first.is_alive() and not second.is_alive()
    assert failures == []
    payloads = [json.loads(line) for line in first_archive.corrections_path.read_text(encoding="utf-8").splitlines()]
    assert {item["matrix_event_id"] for item in payloads} == {"$first", "$second"}
