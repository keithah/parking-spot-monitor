from __future__ import annotations

from tests.support._outbox_persistence import *  # noqa: F403


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
