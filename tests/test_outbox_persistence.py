import json
from pathlib import Path

import pytest

from parking_monitor.outbox import (
    AlertIntent,
    LocalOutbox,
    OutboxPersistenceError,
    OutboxTransitionError,
    SecretBearingIntentError,
    derive_matrix_transaction_id,
)


def test_enqueue_persists_sanitized_pending_item_and_reloads(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)

    intent = AlertIntent(
        event_id="camera-1:1700000000:occupied",
        phase="text",
        room_id="!room:example.org",
        body="Parking spot occupied",
        metadata={"camera_id": "camera-1", "occupied": True, "empty_optional": ""},
    )

    record = outbox.enqueue(intent)

    assert record.state == "pending"
    assert record.intent.metadata == {"camera_id": "camera-1", "occupied": True}
    assert record.transaction_id == derive_matrix_transaction_id(intent)

    raw = store_path.read_text(encoding="utf-8")
    assert "access_token" not in raw.lower()
    assert "authorization" not in raw.lower()
    assert "bearer" not in raw.lower()
    assert "rtsp://" not in raw.lower()
    assert "image_bytes" not in raw.lower()

    persisted = json.loads(raw)
    assert persisted["items"][0]["id"] == record.id
    assert persisted["items"][0]["matrix_transaction_id"] == record.transaction_id

    restarted = LocalOutbox(store_path)
    assert restarted.list_pending() == [record]


def test_enqueue_is_idempotent_for_duplicate_logical_alerts(tmp_path):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    intent = AlertIntent(
        event_id="camera-1:1700000000:occupied",
        phase="text",
        room_id="!room:example.org",
        body="Parking spot occupied",
        metadata={"camera_id": "camera-1"},
    )

    first = outbox.enqueue(intent)
    second = outbox.enqueue(intent)

    assert second == first
    assert outbox.list_records() == [first]
    assert outbox.status_summary()["counts_by_state"] == {"pending": 1}
    item = outbox.status_summary()["items"][0]
    assert item["id"] == first.id
    assert item["state"] == "pending"
    assert item["phase"] == "text"
    assert item["phases"] == [{"phase": "text", "state": "pending", "updated_at": first.updated_at}]
    assert item["retry_reason"] is None
    assert item["dead_letter_reason"] is None


@pytest.mark.parametrize(
    "bad_intent",
    [
        AlertIntent(
            event_id="camera-1:1700000000:occupied",
            phase="text",
            body="Parking spot occupied",
            metadata={"access_token": "abc123"},
        ),
        AlertIntent(
            event_id="camera-1:1700000000:occupied",
            phase="text",
            body="Authorization: Bearer abc123",
        ),
        AlertIntent(
            event_id="camera-1:1700000000:occupied",
            phase="text",
            body="rtsp://camera.local/stream",
        ),
        AlertIntent(
            event_id="camera-1:1700000000:occupied",
            phase="image",
            body="Parking spot occupied",
            metadata={"image_bytes": b"not allowed"},
        ),
    ],
)
def test_enqueue_rejects_secret_shaped_values(tmp_path, bad_intent):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")

    with pytest.raises(SecretBearingIntentError):
        outbox.enqueue(bad_intent)

    assert outbox.list_records() == []
    assert not (tmp_path / "matrix-outbox.json").exists()


def test_empty_optional_fields_do_not_change_stable_transaction_id(tmp_path):
    base = AlertIntent(
        event_id="camera-1:1700000000:occupied",
        phase="text",
        body="Parking spot occupied",
        metadata={"camera_id": "camera-1"},
    )
    with_empty_optionals = AlertIntent(
        event_id="camera-1:1700000000:occupied",
        phase="text",
        room_id="",
        body="Parking spot occupied",
        metadata={"camera_id": "camera-1", "empty": "", "none": None},
    )

    assert derive_matrix_transaction_id(base) == derive_matrix_transaction_id(with_empty_optionals)

    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(with_empty_optionals)

    assert record.intent.room_id is None
    assert record.intent.metadata == {"camera_id": "camera-1"}


def test_persistence_failure_is_explicit_and_does_not_pretend_success(tmp_path, monkeypatch):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    intent = AlertIntent(event_id="camera-1:1700000000:occupied", phase="text", body="Parking spot occupied")

    def fail_replace(src, dst):
        raise OSError("disk full: token should not leak")

    monkeypatch.setattr("parking_monitor.outbox.os.replace", fail_replace)

    with pytest.raises(OutboxPersistenceError) as excinfo:
        outbox.enqueue(intent)

    assert "disk full" not in str(excinfo.value)
    assert outbox.list_records() == []
    assert not (tmp_path / "matrix-outbox.json").exists()



def test_corrupt_json_is_quarantined_and_startup_continues_without_raw_diagnostics(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    store_path.write_text('{"items": [Bearer token should stay out of diagnostics', encoding="utf-8")

    outbox = LocalOutbox(store_path)

    assert outbox.list_records() == []
    summary = outbox.status_summary()
    assert summary["recovery"]["quarantined_count"] == 1
    assert summary["recovery"]["reason_counts"] == {"invalid_json": 1}
    event = summary["recovery"]["events"][0]
    assert event["reason"] == "invalid_json"
    assert "Bearer token" not in str(summary)
    quarantine_path = tmp_path / ".matrix-outbox-quarantine" / Path(event["quarantine_path"]).name
    assert quarantine_path.exists()
    assert quarantine_path.read_text(encoding="utf-8") == store_path.read_text(encoding="utf-8")


def test_oversized_json_is_quarantined_before_json_load(tmp_path, monkeypatch):
    monkeypatch.setattr("parking_monitor.outbox._MAX_OUTBOX_FILE_BYTES", 16)
    store_path = tmp_path / "matrix-outbox.json"
    store_path.write_text('{"items": []} extra bytes', encoding="utf-8")

    outbox = LocalOutbox(store_path)

    assert outbox.list_records() == []
    summary = outbox.status_summary()
    assert summary["recovery"]["quarantined_count"] == 1
    assert summary["recovery"]["reason_counts"] == {"oversized_file": 1}
    quarantine_path = Path(summary["recovery"]["events"][0]["quarantine_path"])
    assert quarantine_path.exists()
    assert quarantine_path.read_text(encoding="utf-8") == store_path.read_text(encoding="utf-8")


def test_unsupported_schema_version_is_quarantined_with_empty_recovery(tmp_path):
    store_path = tmp_path / "matrix-outbox.json"
    store_path.write_text(json.dumps({"schema_version": 999, "items": []}), encoding="utf-8")

    outbox = LocalOutbox(store_path)

    assert outbox.list_records() == []
    summary = outbox.status_summary()
    assert summary["recovery"]["quarantined_count"] == 1
    assert summary["recovery"]["reason_counts"] == {"unsupported_schema_version": 1}
    assert Path(summary["recovery"]["events"][0]["quarantine_path"]).exists()


@pytest.mark.parametrize(
    ("bad_item", "reason"),
    [
        ("not-a-record-object", "malformed_record"),
        ({"id": "missing required fields"}, "malformed_record"),
        (
            {
                "id": "bad-state",
                "matrix_transaction_id": "txn",
                "intent": {"event_id": "evt", "phase": "text", "body": "ok"},
                "state": "unknown",
                "created_at": "2026-05-19T00:00:00Z",
                "updated_at": "2026-05-19T00:00:00Z",
            },
            "invalid_state",
        ),
    ],
)
def test_malformed_records_are_quarantined_while_valid_records_recover(tmp_path, bad_item, reason):
    store_path = tmp_path / "matrix-outbox.json"
    seeded = LocalOutbox(store_path)
    valid = seeded.enqueue(AlertIntent(event_id="evt-1", phase="text", body="ok"))
    payload = json.loads(store_path.read_text(encoding="utf-8"))
    payload["items"].append(bad_item)
    store_path.write_text(json.dumps(payload), encoding="utf-8")

    outbox = LocalOutbox(store_path)

    assert outbox.list_records() == [valid]
    summary = outbox.status_summary()
    assert summary["recovery"]["recovered_count"] == 1
    assert summary["recovery"]["quarantined_count"] == 1
    assert summary["recovery"]["reason_counts"] == {reason: 1}
    assert Path(summary["recovery"]["events"][0]["quarantine_path"]).exists()


def test_replace_failure_preserves_prior_durable_file(tmp_path, monkeypatch):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    first = outbox.enqueue(AlertIntent(event_id="evt-1", phase="text", body="ok"))
    durable_before = store_path.read_text(encoding="utf-8")

    def fail_replace(src, dst):
        raise OSError("permission denied: bearer token should not leak")

    monkeypatch.setattr("parking_monitor.outbox.os.replace", fail_replace)

    with pytest.raises(OutboxPersistenceError) as excinfo:
        outbox.enqueue(AlertIntent(event_id="evt-2", phase="text", body="ok"))

    assert "permission denied" not in str(excinfo.value)
    assert "bearer" not in str(excinfo.value).lower()
    assert store_path.read_text(encoding="utf-8") == durable_before
    assert LocalOutbox(store_path).list_records() == [first]


def test_write_failure_preserves_prior_durable_file(tmp_path, monkeypatch):
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    first = outbox.enqueue(AlertIntent(event_id="evt-1", phase="text", body="ok"))
    durable_before = store_path.read_text(encoding="utf-8")

    def fail_dump(*args, **kwargs):
        raise OSError("disk full: access_token should not leak")

    monkeypatch.setattr("parking_monitor.outbox.json.dump", fail_dump)

    with pytest.raises(OutboxPersistenceError) as excinfo:
        outbox.enqueue(AlertIntent(event_id="evt-2", phase="text", body="ok"))

    assert "disk full" not in str(excinfo.value)
    assert "access_token" not in str(excinfo.value)
    assert store_path.read_text(encoding="utf-8") == durable_before
    assert LocalOutbox(store_path).list_records() == [first]



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



def test_phase_failure_dead_letters_record_with_reason_code(tmp_path):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="evt-phase-fail", phase="image", body="ok"))

    dead = outbox.mark_phase_failed(record.id, "image", reason="matrix_upload_rejected")

    assert dead.state == "dead_lettered"
    assert dead.dead_letter_reason == "matrix_upload_rejected"
    assert dead.phase_states == {"image": "failed"}


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


def test_invalid_transitions_are_safe_and_do_not_mutate_store(tmp_path):
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="evt-invalid", phase="text", body="ok"))

    with pytest.raises(OutboxTransitionError, match="unknown_phase"):
        outbox.mark_phase_delivered(record.id, "thumbnail")
    with pytest.raises(OutboxTransitionError, match="unknown_record"):
        outbox.mark_retrying("missing", reason="timeout")

    assert outbox.list_records() == [record]
