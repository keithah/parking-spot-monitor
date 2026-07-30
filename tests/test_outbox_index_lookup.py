from __future__ import annotations

from pathlib import Path

import parking_monitor.outbox as outbox_module
from parking_monitor.matrix_outbox_snapshots import MatrixOutboxSnapshots
from parking_monitor.outbox import AlertIntent, LocalOutbox


def _intent(event_id: str) -> AlertIntent:
    return AlertIntent(
        event_id=event_id,
        phase="text",
        body="body",
        metadata={"observed_at": "2026-07-30T12:00:00Z"},
    )


def test_indexed_record_lookup_copies_only_the_selected_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    outbox = LocalOutbox(tmp_path / "outbox.json")
    records = [outbox.enqueue(_intent(f"event-{index}")) for index in range(3)]
    real_deepcopy = outbox_module.copy.deepcopy
    copied: list[object] = []

    def tracked(value):
        copied.append(value)
        return real_deepcopy(value)

    monkeypatch.setattr(outbox_module.copy, "deepcopy", tracked)

    selected = outbox.get_record(records[1].id)
    assert selected == records[1]
    assert copied == [outbox._records[1]]
    assert outbox.get_record("missing") is None


def test_indexed_event_lookup_filters_phase_and_returns_detached_value(
    tmp_path: Path,
) -> None:
    outbox = LocalOutbox(tmp_path / "outbox.json")
    record = outbox.enqueue_with_phases(_intent("snapshot-event"), ("text", "upload", "image"))

    selected = outbox.find_event_record("snapshot-event", required_phase="upload")
    assert selected == record
    assert outbox.find_event_record("snapshot-event", required_phase="missing") is None
    assert selected is not outbox._records[0]


def test_matrix_snapshot_paths_use_indexed_single_record_lookups(
    tmp_path: Path,
    monkeypatch,
) -> None:
    outbox = LocalOutbox(tmp_path / "outbox.json")
    record = outbox.enqueue_with_phases(_intent("snapshot-event"), ("text", "upload", "image"))
    snapshots = MatrixOutboxSnapshots(
        room_id="!room:example",
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=outbox,
        logger=None,
        retention_count=10,
    )
    monkeypatch.setattr(
        outbox,
        "list_records",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("full scan")),
    )

    existing = snapshots.enqueue(
        event={"spot_id": "left", "observed_at": "2026-07-30T12:00:00Z"},
        event_id="snapshot-event",
        body="body",
        metadata={},
        source_path=str(tmp_path / "unused.jpg"),
        event_type="occupancy-open-event",
    )
    monkeypatch.setattr(snapshots, "_prepare_upload_locked", lambda value: value)

    assert existing == record
    assert snapshots.prepare_upload(record) == record
