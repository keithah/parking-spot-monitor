from __future__ import annotations

from tests.support._outbox_persistence import *  # noqa: F403


def test_retention_prunes_terminal_records_before_retryable_records(tmp_path):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json", max_records=3)
    delivered = outbox.enqueue(AlertIntent(event_id="evt-delivered", phase="text", body="ok"))
    outbox.mark_delivered(delivered.id)
    dead = outbox.enqueue(AlertIntent(event_id="evt-dead", phase="text", body="ok"))
    outbox.mark_dead_lettered(dead.id, reason="matrix_forbidden")
    pending = outbox.enqueue(AlertIntent(event_id="evt-pending", phase="text", body="ok"))
    retrying = outbox.enqueue(AlertIntent(event_id="evt-retrying", phase="text", body="ok"))
    outbox.mark_retrying(retrying.id, reason="timeout")

    records = LocalOutbox(tmp_path / "matrix-outbox.json", max_records=3).list_records()

    assert len(records) == 3
    assert {record.id for record in records} == {dead.id, pending.id, retrying.id}
    assert {record.state for record in records} == {"dead_lettered", "pending", "retrying"}


def test_terminal_age_retention_prunes_old_delivered_records_only(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path, max_records=None)
    old_terminal = outbox.enqueue(AlertIntent(event_id="evt-old", phase="text", body="ok"))
    outbox.mark_delivered(old_terminal.id)
    pending = outbox.enqueue(AlertIntent(event_id="evt-pending-age", phase="text", body="ok"))
    payload = json.loads(store_path.read_text(encoding="utf-8"))
    for item in payload["items"]:
        if item["id"] == old_terminal.id:
            item["updated_at"] = "2000-01-01T00:00:00Z"
    store_path.write_text(json.dumps(payload), encoding="utf-8")

    reloaded = LocalOutbox(store_path, max_records=None, max_terminal_age_seconds=0)

    assert reloaded.list_records() == [pending]


def test_duplicate_persisted_ids_use_the_last_rebuilt_index(tmp_path: Path) -> None:
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    first = outbox.enqueue(AlertIntent(event_id="evt-duplicate-first", phase="text", body="first"))
    second = outbox.enqueue(AlertIntent(event_id="evt-duplicate-second", phase="text", body="second"))
    payload = json.loads(store_path.read_text(encoding="utf-8"))
    payload["items"][1]["id"] = first.id
    store_path.write_text(json.dumps(payload), encoding="utf-8")

    reloaded = LocalOutbox(store_path)
    transitioned = reloaded.mark_retrying(first.id, reason="timeout")
    records = reloaded.list_records()

    assert reloaded._index_by_id == {first.id: 1}
    assert records[0] == first
    assert records[1] == transitioned
    assert records[1].intent == second.intent
    assert records[1].state == "retrying"


def test_retention_and_replacement_rebuild_id_positions(tmp_path: Path) -> None:
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path, max_records=2)
    pruned = outbox.enqueue(AlertIntent(event_id="evt-pruned", phase="text", body="old"))
    outbox.mark_delivered(pruned.id)
    retained = outbox.enqueue(AlertIntent(event_id="evt-retained", phase="text", body="middle"))
    newest = outbox.enqueue(AlertIntent(event_id="evt-newest", phase="text", body="new"))

    assert outbox._index_by_id == {retained.id: 0, newest.id: 1}
    retrying = outbox.mark_retrying(retained.id, reason="timeout")
    delivered = outbox.mark_phase_delivered(newest.id, "text")
    assert outbox.list_records() == [retrying, delivered]
    assert outbox._index_by_id == {retained.id: 0, newest.id: 1}

    reloaded = LocalOutbox(store_path, max_records=2)
    assert reloaded.list_records() == [retrying, delivered]
    assert reloaded._index_by_id == {retained.id: 0, newest.id: 1}


def test_invalid_transitions_are_safe_and_do_not_mutate_store(tmp_path):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="evt-invalid", phase="text", body="ok"))

    with pytest.raises(OutboxTransitionError, match="unknown_phase"):
        outbox.mark_phase_delivered(record.id, "thumbnail")
    with pytest.raises(OutboxTransitionError, match="unknown_record"):
        outbox.mark_retrying("missing", reason="timeout")

    assert outbox.list_records() == [record]


def test_upload_derivative_attach_is_atomic_identity_stable_and_detached(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue_with_phases(
        AlertIntent(
            event_id="evt-derivative",
            phase="text",
            body="body",
            metadata={"retained_snapshot_filename": "event.jpg"},
        ),
        ("text", "upload", "image"),
    )
    original_id = record.id
    original_transaction = record.transaction_id
    info = {
        "mimetype": "image/jpeg",
        "size": 100,
        "w": 10,
        "h": 5,
        "sha256": "a" * 64,
    }

    attached = outbox.attach_upload_derivative(
        record.id,
        path="/data/snapshots/.upload-derivatives/event.jpg",
        info=info,
    )

    assert attached.id == original_id
    assert attached.transaction_id == original_transaction
    assert attached.intent.metadata["upload_derivative_info"] == info
    nested = attached.intent.metadata["upload_derivative_info"]
    assert isinstance(nested, dict)
    nested["w"] = 999
    [stored] = outbox.list_records()
    assert stored.intent.metadata["upload_derivative_info"]["w"] == 10
    assert outbox.attach_upload_derivative(record.id, path=str(attached.intent.metadata["upload_derivative_path"]), info=info) == stored


def test_upload_derivative_attach_rejects_terminal_or_conflicting_mutation(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue_with_phases(
        AlertIntent(event_id="evt-terminal-derivative", phase="text", body="body"),
        ("text", "upload", "image"),
    )
    info = {"mimetype": "image/jpeg", "size": 100, "w": 10, "h": 5, "sha256": "b" * 64}
    outbox.attach_upload_derivative(record.id, path="/data/one.jpg", info=info)
    with pytest.raises(OutboxTransitionError, match="already_attached"):
        outbox.attach_upload_derivative(record.id, path="/data/two.jpg", info=info)

    outbox.mark_failed(record.id, reason="failed")
    with pytest.raises(OutboxTransitionError, match="terminal"):
        outbox.attach_upload_derivative(record.id, path="/data/one.jpg", info=info)


def test_upload_derivative_attach_rejects_malformed_transport_evidence(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="evt-invalid-derivative", phase="upload", body="body"))
    malformed = {"mimetype": "image/jpeg", "size": 100, "w": 10, "h": 5}

    with pytest.raises(OutboxTransitionError, match="invalid_upload_derivative"):
        outbox.attach_upload_derivative(record.id, path="/data/event.jpg", info=malformed)

    assert outbox.list_records() == [record]
