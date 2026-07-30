from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import parking_monitor.outbox as outbox_module
import parking_monitor.outbox_storage as outbox_storage
from parking_monitor.outbox import AlertIntent, LocalOutbox, OutboxPersistenceError


def _set_limits(monkeypatch: pytest.MonkeyPatch, *, document: int, record: int) -> None:
    monkeypatch.setattr(outbox_module, "_MAX_OUTBOX_FILE_BYTES", document)
    monkeypatch.setattr(outbox_module, "_MAX_OUTBOX_RECORD_BYTES", record, raising=False)


@pytest.mark.parametrize(
    "oversized",
    [
        AlertIntent(event_id="oversized-body", phase="text", body="x" * 1_000),
        AlertIntent(event_id="oversized-metadata", phase="text", body="ok", metadata={"detail": "x" * 1_000}),
    ],
    ids=("body", "metadata"),
)
def test_enqueue_rejects_oversized_record_without_changing_durable_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, oversized: AlertIntent
) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path)
    retained = outbox.enqueue(AlertIntent(event_id="retained", phase="text", body="ok"))
    durable_before = path.read_bytes()
    _set_limits(monkeypatch, document=5_000, record=700)

    with pytest.raises(OutboxPersistenceError, match="record exceeds byte limit"):
        outbox.enqueue(oversized)

    assert path.read_bytes() == durable_before
    assert outbox.list_records() == [retained]
    assert LocalOutbox(path).list_records() == [retained]


def test_enqueue_rejects_document_overflow_even_when_each_record_fits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path, max_records=None)
    retained = outbox.enqueue(AlertIntent(event_id="retained", phase="text", body="x" * 200))
    durable_before = path.read_bytes()
    _set_limits(monkeypatch, document=len(durable_before) + 100, record=2_000)

    with pytest.raises(OutboxPersistenceError, match="document exceeds byte limit"):
        outbox.enqueue(AlertIntent(event_id="overflow", phase="text", body="y" * 200))

    assert path.read_bytes() == durable_before
    assert outbox.list_records() == [retained]


def test_phase_update_overflow_preserves_prior_record_and_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path)
    record = outbox.enqueue(AlertIntent(event_id="phase", phase="upload", body="ok"))
    durable_before = path.read_bytes()
    _set_limits(monkeypatch, document=len(durable_before) + 100, record=5_000)

    with pytest.raises(OutboxPersistenceError, match="document exceeds byte limit"):
        outbox.mark_phase_delivered(record.id, "upload", result={"content_uri": "mxc://example/" + "x" * 500})

    assert path.read_bytes() == durable_before
    assert outbox.list_records() == [record]
    assert LocalOutbox(path).list_records() == [record]


def test_phase_update_rejects_per_record_overflow_before_document_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path)
    record = outbox.enqueue(AlertIntent(event_id="phase-record", phase="upload", body="ok"))
    durable_before = path.read_bytes()
    persisted_record = json.loads(durable_before)["items"][0]
    record_size = len(json.dumps(persisted_record, sort_keys=True, separators=(",", ":")).encode())
    _set_limits(monkeypatch, document=10_000, record=record_size + 50)

    with pytest.raises(OutboxPersistenceError, match="record exceeds byte limit"):
        outbox.mark_phase_delivered(record.id, "upload", result={"content_uri": "mxc://example/" + "x" * 300})

    assert path.read_bytes() == durable_before
    assert outbox.list_records() == [record]


def test_loader_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    path = tmp_path / "matrix-outbox.json"
    os.mkfifo(path)

    with pytest.raises(OutboxPersistenceError, match="regular file"):
        LocalOutbox(path)


def test_loader_rejects_symlink_without_reading_target(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text(json.dumps({"schema_version": 1, "items": []}), encoding="utf-8")
    before = target.read_bytes()
    path = tmp_path / "matrix-outbox.json"
    path.symlink_to(target.name)

    with pytest.raises(OutboxPersistenceError, match="non-symlink regular file"):
        LocalOutbox(path)

    assert target.read_bytes() == before


def test_loader_rejects_path_replacement_during_descriptor_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "matrix-outbox.json"
    seeded = LocalOutbox(path)
    seeded.enqueue(AlertIntent(event_id="original", phase="text", body="x" * 100_000))
    replacement = tmp_path / "replacement.json"
    replacement.write_text(json.dumps({"schema_version": 1, "items": []}), encoding="utf-8")
    real_read = outbox_storage.os.read
    replaced = False

    def replace_after_read(descriptor: int, size: int) -> bytes:
        nonlocal replaced
        payload = real_read(descriptor, size)
        if payload and not replaced:
            replaced = True
            os.replace(replacement, path)
        return payload

    monkeypatch.setattr(outbox_storage.os, "read", replace_after_read)

    with pytest.raises(OutboxPersistenceError, match="changed while reading"):
        LocalOutbox(path)

    assert replaced is True
    assert json.loads(path.read_text(encoding="utf-8"))["items"] == []


def test_loader_quarantines_legacy_record_over_record_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "matrix-outbox.json"
    seeded = LocalOutbox(path)
    seeded.enqueue(AlertIntent(event_id="legacy-large", phase="text", body="x" * 2_000))
    monkeypatch.setattr(outbox_module, "_MAX_OUTBOX_RECORD_BYTES", 500)

    reloaded = LocalOutbox(path)

    assert reloaded.list_records() == []
    assert reloaded.status_summary()["recovery"]["reason_counts"] == {"oversized_record": 1}


def test_loader_quarantines_non_finite_legacy_record(tmp_path: Path) -> None:
    path = tmp_path / "matrix-outbox.json"
    seeded = LocalOutbox(path)
    seeded.enqueue(AlertIntent(event_id="legacy-nan", phase="text", body="ok"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["items"][0]["retry_attempt_count"] = float("nan")
    path.write_text(json.dumps(payload, allow_nan=True), encoding="utf-8")

    reloaded = LocalOutbox(path)

    assert reloaded.list_records() == []
    assert reloaded.status_summary()["recovery"]["reason_counts"] == {"malformed_record": 1}
