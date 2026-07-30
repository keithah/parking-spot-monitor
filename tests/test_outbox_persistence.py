from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest
import parking_monitor.outbox as outbox_module

from parking_monitor.outbox import (
    AlertIntent,
    LocalOutbox,
    OutboxPersistenceError,
    OutboxRetryPolicy,
    OutboxTransitionError,
    RetrySchedule,
    SecretBearingIntentError,
    derive_outbox_item_id,
    derive_matrix_transaction_id,
)


def test_legacy_record_without_retry_fields_loads_with_safe_defaults(tmp_path: Path) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path)
    record = outbox.enqueue(AlertIntent(event_id="legacy-retry", phase="text", body="body"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["items"][0].pop("retry_attempt_count", None)
    payload["items"][0].pop("retry_due_at", None)
    path.write_text(json.dumps(payload), encoding="utf-8")

    [loaded] = LocalOutbox(path).list_records()

    assert loaded.id == record.id
    assert loaded.retry_attempt_count == 0
    assert loaded.retry_due_at is None


def test_retry_due_and_exponential_count_survive_restart(tmp_path: Path) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path)
    record = outbox.enqueue(AlertIntent(event_id="retry-restart", phase="text", body="body"))

    outbox.mark_retrying(
        record.id,
        reason="timeout",
        retry_due_at="2026-07-30T12:05:00Z",
        retry_attempt_count=3,
    )

    [loaded] = LocalOutbox(path).list_records()
    assert loaded.retry_attempt_count == 3
    assert loaded.retry_due_at == "2026-07-30T12:05:00Z"


def test_next_due_record_selects_pending_then_earliest_eligible_retry(tmp_path: Path) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path)
    first = outbox.enqueue(AlertIntent(event_id="retry-first", phase="text", body="body"))
    second = outbox.enqueue(AlertIntent(event_id="retry-second", phase="text", body="body"))
    third = outbox.enqueue(AlertIntent(event_id="still-pending", phase="text", body="body"))
    outbox.mark_retrying(
        first.id, reason="timeout", retry_due_at="2026-07-30T12:05:00Z", retry_attempt_count=2
    )
    outbox.mark_retrying(
        second.id, reason="timeout", retry_due_at="2026-07-30T12:02:00Z", retry_attempt_count=1
    )

    assert outbox.next_due_record(datetime(2026, 7, 30, 12, 1, tzinfo=timezone.utc)) == third
    outbox.mark_dead_lettered(third.id, reason="done")
    assert outbox.next_due_record(datetime(2026, 7, 30, 12, 3, tzinfo=timezone.utc)).id == second.id
    assert outbox.next_due_record(datetime(2026, 7, 30, 12, 6, tzinfo=timezone.utc)).id == second.id


def test_due_records_rank_pending_before_legacy_and_scheduled_retries(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    legacy_one = outbox.enqueue(AlertIntent(event_id="legacy-one", phase="text", body="body"))
    legacy_two = outbox.enqueue(AlertIntent(event_id="legacy-two", phase="text", body="body"))
    scheduled = outbox.enqueue(AlertIntent(event_id="scheduled", phase="text", body="body"))
    pending = outbox.enqueue(AlertIntent(event_id="new-pending", phase="text", body="body"))
    outbox.mark_retrying(legacy_one.id, reason="timeout")
    outbox.mark_retrying(legacy_two.id, reason="timeout")
    outbox.mark_retrying(
        scheduled.id,
        reason="timeout",
        retry_due_at="2026-07-30T11:00:00Z",
        retry_attempt_count=1,
    )

    due = outbox.due_records(datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc))

    legacy_ids = [record.id for record in sorted((legacy_one, legacy_two), key=lambda item: (item.created_at, item.id))]
    assert [record.id for record in due] == [pending.id, *legacy_ids, scheduled.id]


def test_retry_policy_caps_exponential_delay_and_validates_randomness() -> None:
    policy = OutboxRetryPolicy(initial_seconds=60, max_seconds=900, jitter_ratio=0.2)

    assert policy.delay_seconds(1, random_unit=0) == 60
    assert policy.delay_seconds(4, random_unit=0.5) == 528
    assert policy.delay_seconds(99, random_unit=1) == 900
    with pytest.raises(ValueError, match="random_unit"):
        policy.delay_seconds(1, random_unit=float("nan"))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial_seconds": True, "max_seconds": 900, "jitter_ratio": 0.2},
        {"initial_seconds": 60, "max_seconds": False, "jitter_ratio": 0.2},
        {"initial_seconds": 60, "max_seconds": 900, "jitter_ratio": True},
        {"initial_seconds": "60", "max_seconds": 900, "jitter_ratio": 0.2},
        {"initial_seconds": 60, "max_seconds": 900j, "jitter_ratio": 0.2},
        {"initial_seconds": 60, "max_seconds": 900, "jitter_ratio": None},
        {"initial_seconds": 60, "max_seconds": 30, "jitter_ratio": 0.2},
    ],
)
def test_retry_policy_rejects_non_real_booleans_and_smaller_maximum(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        OutboxRetryPolicy(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("random_unit", [True, "0.5", 0.5j, None])
def test_retry_policy_delay_rejects_boolean_and_non_real_randomness(random_unit: object) -> None:
    policy = OutboxRetryPolicy(initial_seconds=60, max_seconds=900, jitter_ratio=0.2)
    with pytest.raises(ValueError):
        policy.delay_seconds(1, random_unit=random_unit)  # type: ignore[arg-type]


def test_retry_policy_delay_rejects_boolean_attempt_count() -> None:
    policy = OutboxRetryPolicy(initial_seconds=60, max_seconds=900, jitter_ratio=0.2)
    with pytest.raises(ValueError):
        policy.delay_seconds(True, random_unit=0.5)


def test_apply_phase_result_requires_one_outcome_and_publishes_once(tmp_path: Path, monkeypatch) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="phase-outcome", phase="text", body="body"))
    original = outbox._persist_records
    calls = 0

    def counted(records):
        nonlocal calls
        calls += 1
        return original(records)

    monkeypatch.setattr(outbox, "_persist_records", counted)
    retry = RetrySchedule("2026-07-30T12:05:00Z", 1, "timeout")

    with pytest.raises(OutboxTransitionError, match="exactly_one"):
        outbox.apply_phase_result(record.id, "text")
    with pytest.raises(OutboxTransitionError, match="exactly_one"):
        outbox.apply_phase_result(record.id, "text", retry=retry, terminal_reason="bad")
    updated = outbox.apply_phase_result(record.id, "text", retry=retry)

    assert calls == 1
    assert updated.state == "retrying"
    assert LocalOutbox(outbox.path).list_records() == [updated]


def test_compact_summary_is_cached_per_revision_and_returns_fresh_data(tmp_path: Path, monkeypatch) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="summary-cache", phase="text", body="body"))
    original = outbox._status_summary
    calls = 0

    def counted(*, include_items: bool):
        nonlocal calls
        calls += 1
        return original(include_items=include_items)

    monkeypatch.setattr(outbox, "_status_summary", counted)
    first = outbox.compact_status_summary()
    first["total"] = 999
    second = outbox.compact_status_summary()
    assert calls == 1
    assert second["total"] == 1

    outbox.mark_retrying(record.id, reason="timeout")
    assert outbox.compact_status_summary()["counts_by_state"] == {"retrying": 1}
    assert calls == 2


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("retry_attempt_count", -1, "invalid_retry_attempt_count"),
        ("retry_attempt_count", True, "invalid_retry_attempt_count"),
        ("retry_due_at", "2026-07-30T12:05:00+01:00", "invalid_retry_due_at"),
    ],
)
def test_malformed_retry_fields_are_quarantined(
    tmp_path: Path, field: str, value: object, reason: str
) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path)
    outbox.enqueue(AlertIntent(event_id=f"bad-{field}-{value}", phase="text", body="body"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["items"][0][field] = value
    path.write_text(json.dumps(payload), encoding="utf-8")

    recovered = LocalOutbox(path)

    assert recovered.list_records() == []
    assert recovered.recovery.reason_counts == {reason: 1}


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


def test_concurrent_enqueue_and_transition_preserve_every_record(tmp_path: Path, monkeypatch) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    original = outbox._persist_records

    def delayed_persist(records):
        time.sleep(0.01)
        return original(records)

    monkeypatch.setattr(outbox, "_persist_records", delayed_persist)
    intents = [
        AlertIntent(event_id=f"event-{index}", phase="text", body=f"body-{index}")
        for index in range(4)
    ]

    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(outbox.enqueue, intents))
        transitioned = list(pool.map(lambda record: outbox.mark_retrying(record.id, reason="timeout"), records))

    reloaded = LocalOutbox(outbox.path)
    assert {record.id for record in reloaded.list_records()} == {record.id for record in records}
    assert {record.id for record in transitioned} == {record.id for record in records}
    assert {record.state for record in reloaded.list_records()} == {"retrying"}


def test_concurrent_duplicate_id_enqueues_persist_once(tmp_path: Path, monkeypatch) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    intent = AlertIntent(event_id="event-duplicate", phase="text", body="Parking status")
    original = outbox._persist_records
    calls = 0
    calls_lock = threading.Lock()

    def delayed_counted_persist(records):
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.01)
        return original(records)

    monkeypatch.setattr(outbox, "_persist_records", delayed_counted_persist)

    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(outbox.enqueue, [intent] * 4))

    assert calls == 1
    assert len({record.id for record in records}) == 1
    assert outbox.list_records() == [records[0]]
    assert LocalOutbox(outbox.path).list_records() == [records[0]]


def test_mixed_enqueue_and_transition_share_one_mutation_lock(tmp_path: Path, monkeypatch) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    existing = outbox.enqueue(AlertIntent(event_id="event-existing", phase="text", body="existing"))
    original = outbox._persist_records
    enqueue_persist_entered = threading.Event()
    allow_enqueue_persist = threading.Event()
    transition_started = threading.Event()
    transition_lock_attempted = threading.Event()
    transition_persist_entered = threading.Event()
    persist_calls = 0
    calls_lock = threading.Lock()
    enqueue_thread_id: int | None = None
    real_lock = outbox._lock

    class ObservedRLock:
        def __enter__(self):
            if (
                enqueue_persist_entered.is_set()
                and not allow_enqueue_persist.is_set()
                and threading.get_ident() != enqueue_thread_id
            ):
                transition_lock_attempted.set()
            real_lock.acquire()
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            real_lock.release()

    def controlled_persist(records):
        nonlocal enqueue_thread_id, persist_calls
        with calls_lock:
            persist_calls += 1
            call_number = persist_calls
        if call_number == 1:
            enqueue_thread_id = threading.get_ident()
            enqueue_persist_entered.set()
            assert allow_enqueue_persist.wait(timeout=2)
        else:
            transition_persist_entered.set()
            if not allow_enqueue_persist.is_set():
                raise RuntimeError("transition persistence entered while enqueue held the mutation lock")
        return original(records)

    def transition_existing():
        transition_started.set()
        return outbox.mark_retrying(existing.id, reason="timeout")

    monkeypatch.setattr(outbox, "_lock", ObservedRLock())
    monkeypatch.setattr(outbox, "_persist_records", controlled_persist)
    with ThreadPoolExecutor(max_workers=2) as pool:
        enqueue_future = pool.submit(
            outbox.enqueue,
            AlertIntent(event_id="event-new", phase="text", body="new"),
        )
        assert enqueue_persist_entered.wait(timeout=2)
        transition_future = pool.submit(transition_existing)
        assert transition_started.wait(timeout=2)
        try:
            assert transition_lock_attempted.wait(timeout=2)
            assert not transition_persist_entered.is_set()
        finally:
            allow_enqueue_persist.set()
        enqueued = enqueue_future.result(timeout=2)
        transitioned = transition_future.result(timeout=2)

    assert transition_persist_entered.is_set()
    assert LocalOutbox(outbox.path).list_records() == [transitioned, enqueued]


def test_nested_outbox_lock_paths_complete_within_a_bounded_subprocess(tmp_path: Path) -> None:
    source_root = Path(__file__).resolve().parents[1] / "src"
    script = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from parking_monitor.outbox import AlertIntent, LocalOutbox

outbox = LocalOutbox(Path(sys.argv[2]))
record = outbox.enqueue_with_phases(
    AlertIntent(event_id="event-reentrant", phase="text", body="body"),
    ("text", "upload", "image"),
)
retrying = outbox.mark_retrying(record.id, reason="timeout")
assert outbox.list_records() == [retrying]
"""

    completed = subprocess.run(
        [sys.executable, "-c", script, str(source_root), str(tmp_path / "subprocess-outbox.json")],
        capture_output=True,
        text=True,
        timeout=2,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_persisted_outbox_is_compact_and_reloads_without_value_changes(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(
        AlertIntent(
            event_id="event-compact",
            phase="text",
            body="Parking status",
            metadata={"nested": {"occupied": True}, "count": 2},
        )
    )
    retrying = outbox.mark_retrying(record.id, reason="timeout")

    raw = outbox.path.read_text(encoding="utf-8")

    assert "\n  " not in raw
    assert json.loads(raw)["schema_version"] == 1
    assert LocalOutbox(outbox.path).list_records() == [retrying]


def test_compact_status_summary_omits_record_items(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="event-1", phase="text", body="Parking status"))
    outbox.mark_delivered(record.id)

    detailed = outbox.status_summary()
    compact = outbox.compact_status_summary()

    assert len(detailed["items"]) == 1
    assert "items" not in compact
    assert compact["total"] == 1
    assert compact["counts_by_state"] == {"delivered": 1}


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


def test_failed_persistence_does_not_publish_a_stale_id_index(tmp_path: Path, monkeypatch) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    first = outbox.enqueue(AlertIntent(event_id="evt-first", phase="text", body="ok"))
    second_intent = AlertIntent(event_id="evt-second", phase="text", body="ok")
    original = outbox._persist_records

    def fail_persist(records):
        raise OutboxPersistenceError("failed to persist local outbox record")

    monkeypatch.setattr(outbox, "_persist_records", fail_persist)
    with pytest.raises(OutboxPersistenceError):
        outbox.enqueue(second_intent)

    assert outbox._index_by_id == {first.id: 0}
    assert outbox.list_records() == [first]

    monkeypatch.setattr(outbox, "_persist_records", original)
    second = outbox.enqueue(second_intent)
    retrying = outbox.mark_retrying(second.id, reason="timeout")
    assert outbox._index_by_id == {first.id: 0, second.id: 1}
    assert outbox.list_records() == [first, retrying]


def test_post_rename_sync_failure_reconciles_memory_before_error_and_preserves_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store_path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(store_path)
    first = outbox.enqueue(AlertIntent(event_id="evt-first-committed", phase="text", body="first"))
    second_intent = AlertIntent(event_id="evt-second-committed", phase="text", body="second")
    second_id = derive_outbox_item_id(second_intent)
    original_sync_directory = outbox_module._fsync_directory

    def fail_directory_sync(_path: Path) -> None:
        raise OSError("directory sync failed after replace")

    monkeypatch.setattr(outbox_module, "_fsync_directory", fail_directory_sync)
    with pytest.raises(OutboxPersistenceError, match="failed to persist local outbox record"):
        outbox.enqueue(second_intent)

    disk_records = LocalOutbox(store_path).list_records()
    assert outbox.list_records() == disk_records
    assert [record.id for record in disk_records] == [first.id, second_id]
    assert outbox._index_by_id == {first.id: 0, second_id: 1}

    monkeypatch.setattr(outbox_module, "_fsync_directory", original_sync_directory)
    third = outbox.enqueue(AlertIntent(event_id="evt-third", phase="text", body="third"))

    assert [record.id for record in LocalOutbox(store_path).list_records()] == [first.id, second_id, third.id]



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
