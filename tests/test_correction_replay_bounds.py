from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from parking_spot_monitor.vehicle_history import VehicleHistoryArchive
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
