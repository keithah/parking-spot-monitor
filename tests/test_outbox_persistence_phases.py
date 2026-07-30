from __future__ import annotations

from tests.support._outbox_persistence import *  # noqa: F403


def test_quarantine_directory_is_bounded_for_many_bad_records(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    bad_items = [{"id": f"bad-{index}"} for index in range(25)]
    store_path.write_text(json.dumps({"schema_version": 1, "items": bad_items}), encoding="utf-8")

    outbox = LocalOutbox(store_path)

    summary = outbox.status_summary()
    assert summary["recovery"]["quarantined_count"] == 25
    assert summary["recovery"]["reason_counts"] == {"malformed_record": 25}
    quarantine_files = list((tmp_path / ".matrix-outbox-quarantine").iterdir())
    assert len(quarantine_files) <= 20


def test_phase_transition_is_idempotent_and_reloadable(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    record = outbox.enqueue(AlertIntent(event_id="evt-phase", phase="text", body="ok"))

    delivered = outbox.mark_phase_delivered(record.id, "text")
    duplicate = outbox.mark_phase_delivered(record.id, "text")

    assert duplicate == delivered
    assert delivered.state == "delivered"
    assert delivered.phase_states == {"text": "delivered"}
    assert delivered.phase_results == {}
    assert LocalOutbox(store_path).list_records() == [delivered]


def test_enqueue_with_phases_declares_all_phases_in_one_persistence(tmp_path: Path, monkeypatch) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    original = outbox._persist_records
    calls = 0

    def counted_persist(records):
        nonlocal calls
        calls += 1
        return original(records)

    monkeypatch.setattr(outbox, "_persist_records", counted_persist)

    record = outbox.enqueue_with_phases(
        AlertIntent(event_id="evt-snapshot", phase="text", body="Parking status"),
        ("text", "upload", "image"),
    )

    assert calls == 1
    assert record.phase_states == {"text": "pending", "upload": "pending", "image": "pending"}
    assert LocalOutbox(outbox.path).list_records() == [record]


def test_phase_result_round_trips_and_is_exposed_in_status_summary(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    record = outbox.enqueue(AlertIntent(event_id="evt-upload", phase="upload", body="ok"))

    delivered = outbox.mark_phase_delivered(
        record.id,
        "upload",
        result={
            "content_uri": "mxc://example.org/media-id",
            "width": 640,
            "height": 480,
            "mime_type": "image/jpeg",
            "empty": "",
            "nested": {"thumbnail": False},
        },
    )

    expected_result = {
        "content_uri": "mxc://example.org/media-id",
        "width": 640,
        "height": 480,
        "mime_type": "image/jpeg",
        "nested": {"thumbnail": False},
    }
    assert delivered.phase_results == {"upload": expected_result}
    persisted = json.loads(store_path.read_text(encoding="utf-8"))
    assert persisted["items"][0]["phases"] == [
        {
            "phase": "upload",
            "state": "delivered",
            "updated_at": delivered.phase_updated_at["upload"],
            "result": expected_result,
        }
    ]

    reloaded = LocalOutbox(store_path)
    assert reloaded.list_records() == [delivered]
    summary_phase = reloaded.status_summary()["items"][0]["phases"][0]
    assert summary_phase["result"] == expected_result


def test_phase_results_are_backward_compatible_when_absent(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    record = outbox.enqueue(AlertIntent(event_id="evt-legacy", phase="text", body="ok"))
    payload = json.loads(store_path.read_text(encoding="utf-8"))
    payload["items"][0]["phases"] = [
        {"phase": "text", "state": "delivered", "updated_at": "2026-05-19T00:00:00Z"}
    ]
    payload["items"].append(
        {
            "id": "legacy-without-phases",
            "matrix_transaction_id": "txn-legacy",
            "intent": {"event_id": "evt-legacy-no-phases", "phase": "image", "body": "ok"},
            "state": "pending",
            "created_at": "2026-05-19T00:00:00Z",
            "updated_at": "2026-05-19T00:00:00Z",
        }
    )
    store_path.write_text(json.dumps(payload), encoding="utf-8")

    records = LocalOutbox(store_path).list_records()

    assert records[0].id == record.id
    assert records[0].phase_results == {}
    assert records[0].phase_states == {"text": "delivered"}
    assert records[1].id == "legacy-without-phases"
    assert records[1].phase_states == {"image": "pending"}
    assert records[1].phase_results == {}


def test_phase_result_rejects_unsafe_keys_values_and_binary_without_mutating_store(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    record = outbox.enqueue(AlertIntent(event_id="evt-unsafe-result", phase="upload", body="ok"))
    durable_before = store_path.read_text(encoding="utf-8")

    unsafe_results = [
        {"access_token": "abc123"},
        {"content_uri": "Authorization: Bearer abc123"},
        {"source": "rtsp://camera.local/stream"},
        {"image_bytes": b"not allowed"},
    ]
    for result in unsafe_results:
        with pytest.raises(SecretBearingIntentError):
            outbox.mark_phase_delivered(record.id, "upload", result=result)

    assert store_path.read_text(encoding="utf-8") == durable_before
    assert LocalOutbox(store_path).list_records() == [record]


def test_persisted_unsafe_phase_result_is_quarantined_without_leaking_secret(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    record = outbox.enqueue(AlertIntent(event_id="evt-bad-result", phase="upload", body="ok"))
    payload = json.loads(store_path.read_text(encoding="utf-8"))
    payload["items"][0]["phases"] = [
        {
            "phase": "upload",
            "state": "delivered",
            "updated_at": record.updated_at,
            "result": {"authorization": "Bearer abc123"},
        }
    ]
    store_path.write_text(json.dumps(payload), encoding="utf-8")

    recovered = LocalOutbox(store_path)

    assert recovered.list_records() == []
    summary = recovered.status_summary()
    assert summary["recovery"]["reason_counts"] == {"unsafe_record_content": 1}
    assert "authorization" not in json.dumps(summary).lower()
    assert "bearer" not in json.dumps(summary).lower()


def test_delivered_phase_result_update_is_idempotent_and_not_overwritten(tmp_path):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="evt-idempotent-result", phase="upload", body="ok"))

    delivered = outbox.mark_phase_delivered(record.id, "upload", result={"content_uri": "mxc://example.org/a"})
    duplicate_without_result = outbox.mark_phase_delivered(record.id, "upload")
    duplicate_with_same_result = outbox.mark_phase_delivered(
        record.id, "upload", result={"content_uri": "mxc://example.org/a"}
    )

    assert duplicate_without_result == delivered
    assert duplicate_with_same_result == delivered
    with pytest.raises(OutboxTransitionError, match="delivered_phase_result_cannot_change"):
        outbox.mark_phase_delivered(record.id, "upload", result={"content_uri": "mxc://example.org/b"})
    assert outbox.list_records() == [delivered]


def test_dead_letter_reason_is_preserved_as_safe_code_and_not_retried(tmp_path):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="evt-dead", phase="text", body="ok"))

    dead = outbox.mark_dead_lettered(record.id, reason="matrix_forbidden")

    assert dead.state == "dead_lettered"
    assert dead.dead_letter_reason == "matrix_forbidden"
    summary = outbox.status_summary()
    assert summary["counts_by_state"] == {"dead_lettered": 1}
    assert summary["dead_letter_reason_counts"] == {"matrix_forbidden": 1}
    with pytest.raises(OutboxTransitionError, match="terminal_record_cannot_transition"):
        outbox.mark_retrying(record.id, reason="timeout")


def test_phase_failure_dead_letters_record_with_reason_code_in_one_persistence(tmp_path, monkeypatch):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="evt-phase-fail", phase="image", body="ok"))
    original = outbox._persist_records
    calls = 0

    def counted_persist(records):
        nonlocal calls
        calls += 1
        return original(records)

    monkeypatch.setattr(outbox, "_persist_records", counted_persist)

    dead = outbox.mark_phase_failed(record.id, "image", reason="matrix_upload_rejected")

    assert calls == 1
    assert dead.state == "dead_lettered"
    assert dead.dead_letter_reason == "matrix_upload_rejected"
    assert dead.phase_states == {"image": "failed"}
    assert LocalOutbox(outbox.path).list_records() == [dead]


def test_status_summary_redacts_secret_shaped_failure_text(tmp_path):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="evt-secret", phase="text", body="ok"))

    outbox.mark_retrying(record.id, reason="Authorization: Bearer abc123")
    summary = outbox.status_summary()

    assert summary["retry_reason_counts"] == {"redacted": 1}
    rendered = json.dumps(summary).lower()
    assert "authorization" not in rendered
    assert "bearer" not in rendered
    assert "abc123" not in rendered
    assert "rtsp://" not in rendered
