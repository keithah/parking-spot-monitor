from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from threading import Event, Thread, current_thread
from unittest.mock import patch

import parking_spot_monitor.operator_decision_memory as decision_memory
import pytest
from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import (
    DecisionMemoryLoad,
    append_decision_memory_record,
    decision_memory_path,
    load_decision_memory,
    make_decision_memory_record,
)
from parking_spot_monitor.runtime_decision_memory import (
    _append_decision_memory,
    _append_lab_outcome_memory,
)


def _record(kind: str, spot_id: str | None, summary: str):
    return make_decision_memory_record(
        kind,
        observed_at=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
        spot_id=spot_id,
        summary=summary,
    )


def _store(path: Path, *, interval: float = 300, count: int = 50, **kwargs):
    return DecisionMemoryStore(
        path,
        checkpoint_interval_seconds=interval,
        checkpoint_max_pending_records=count,
        **kwargs,
    )


def test_routine_decision_records_batch_until_count_boundary(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    with patch(
        "parking_spot_monitor.operator_decision_memory._write_memory",
        wraps=decision_memory._write_memory,
    ) as write:
        store = _store(path, count=3, monotonic=lambda: 0)
        assert store.extend(
            (_record("miss", "left_spot", "first"), _record("miss", "left_spot", "second")),
            durability="routine",
        )
        assert write.call_count == 0
        assert store.append(_record("miss", "left_spot", "third"), durability="routine")
        assert write.call_count == 1
    assert [record.summary for record in load_decision_memory(path).records] == ["first", "second", "third"]


def test_immediate_decision_flushes_prior_routine_records(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    store = _store(path, monotonic=lambda: 0)
    assert store.append(_record("miss", "left_spot", "routine"), durability="routine")
    assert not path.exists()
    assert store.append(_record("alert", "left_spot", "immediate"), durability="immediate")
    assert [record.summary for record in load_decision_memory(path).records] == ["routine", "immediate"]


def test_immediate_checkpoint_serializes_candidate_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    real_encode = decision_memory._encode_memory_payload
    encoded = 0

    def count_encode(records):
        nonlocal encoded
        encoded += 1
        return real_encode(records)

    monkeypatch.setattr(decision_memory, "_encode_memory_payload", count_encode)
    store = _store(path, monotonic=lambda: 0)

    assert store.append(_record("alert", "left_spot", "once"), durability="immediate")
    assert encoded == 1


def test_decision_store_time_checkpoint_close_and_truncation(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    clock = [10.0]
    store = _store(path, interval=5, max_records=2, monotonic=lambda: clock[0])
    assert store.extend(tuple(_record("miss", "left_spot", str(index)) for index in range(3)), durability="routine")
    assert store.checkpoint_if_due() is False
    clock[0] = 15.0
    assert store.checkpoint_if_due() is True
    assert [record.summary for record in load_decision_memory(path).records] == ["1", "2"]
    assert store.append(_record("miss", "left_spot", "close"), durability="routine")
    assert store.close() is True
    assert [record.summary for record in load_decision_memory(path).records] == ["2", "close"]


def test_failed_immediate_write_retains_dirty_state_for_retry(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    logs = StringIO()
    store = _store(path, monotonic=lambda: 0, logger=StructuredLogger(stream=logs))
    with patch("parking_spot_monitor.operator_decision_memory._write_memory", side_effect=OSError("disk unavailable")):
        assert store.append(_record("alert", "left_spot", "retry me"), durability="immediate") is False
    assert not path.exists()
    assert "operator-decision-memory-append-failed" in logs.getvalue()
    assert "OSError" in logs.getvalue()
    assert "disk unavailable" not in logs.getvalue()
    assert store.flush() is True
    assert [record.summary for record in load_decision_memory(path).records] == ["retry me"]


def test_directory_fsync_failure_retries_without_duplicates_or_lost_external_records(
    tmp_path: Path,
) -> None:
    path = decision_memory_path(tmp_path)
    store = _store(path, monotonic=lambda: 0)
    real_fsync = decision_memory.os.fsync
    fsync_calls = 0

    def fail_first_directory_fsync(file_descriptor: int) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 2:
            raise OSError("directory unavailable")
        real_fsync(file_descriptor)

    with patch.object(
        decision_memory.os,
        "fsync",
        side_effect=fail_first_directory_fsync,
    ):
        assert store.append(
            _record("alert", "left_spot", "local"), durability="immediate"
        ) is False

    assert [record.summary for record in load_decision_memory(path).records] == [
        "local"
    ]
    assert append_decision_memory_record(
        path, _record("alert", "left_spot", "external")
    )
    assert store.flush() is True
    assert [record.summary for record in load_decision_memory(path).records] == [
        "local",
        "external",
    ]


def test_failed_deadline_checkpoint_retries_at_next_iteration_boundary(
    tmp_path: Path,
) -> None:
    path = decision_memory_path(tmp_path)
    clock = [0.0]
    sleeps: list[float] = []
    store = _store(path, interval=5, monotonic=lambda: clock[0])
    assert store.append(_record("miss", "left_spot", "retry deadline"), durability="routine")

    def wait(seconds: float) -> bool:
        sleeps.append(seconds)
        clock[0] += seconds
        return False

    real_write = decision_memory._write_memory
    attempts = 0

    def flaky_write(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("disk unavailable")
        return real_write(*args, **kwargs)

    with patch(
        "parking_spot_monitor.operator_decision_memory._write_memory",
        side_effect=flaky_write,
    ) as write:
        assert store.wait_for_checkpoint(600, wait=wait) is False
        assert not path.exists()
        assert write.call_count == 1
        assert store.wait_for_checkpoint(600, wait=wait) is False

    assert sleeps == [5, 595, 0, 600]
    assert write.call_count == 2
    assert [record.summary for record in load_decision_memory(path).records] == [
        "retry deadline"
    ]


@pytest.mark.parametrize(
    ("checkpoint_seconds", "expected_sleeps", "expected_elapsed"),
    ((10, [5, 585], 600), (700, [5], 705)),
)
def test_checkpoint_duration_counts_toward_requested_wait_cadence(
    tmp_path: Path,
    checkpoint_seconds: float,
    expected_sleeps: list[float],
    expected_elapsed: float,
) -> None:
    path = decision_memory_path(tmp_path)
    clock = [0.0]
    sleeps: list[float] = []
    store = _store(path, interval=5, monotonic=lambda: clock[0])
    assert store.append(_record("miss", "left_spot", "timed checkpoint"), durability="routine")
    real_write = decision_memory._write_memory

    def timed_write(*args, **kwargs):
        clock[0] += checkpoint_seconds
        return real_write(*args, **kwargs)

    def wait(seconds: float) -> bool:
        sleeps.append(seconds)
        clock[0] += seconds
        return False

    with patch(
        "parking_spot_monitor.operator_decision_memory._write_memory",
        side_effect=timed_write,
    ):
        assert store.wait_for_checkpoint(600, wait=wait) is False

    assert sleeps == expected_sleeps
    assert clock[0] == expected_elapsed
    assert [record.summary for record in load_decision_memory(path).records] == [
        "timed checkpoint"
    ]


def test_decision_store_merges_external_replacement_before_flush(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(path, _record("miss", "left_spot", "baseline"))
    store = _store(path, monotonic=lambda: 0)
    assert store.append(_record("miss", "left_spot", "local"), durability="routine")
    assert append_decision_memory_record(path, _record("alert", "left_spot", "external"))
    assert store.flush() is True
    assert [record.summary for record in load_decision_memory(path).records] == ["baseline", "external", "local"]


def test_decision_store_rejects_non_positive_checkpoint_limits(tmp_path: Path) -> None:
    for interval, count in ((0, 50), (float("inf"), 50), (300, 0), (300, True)):
        with pytest.raises(ValueError):
            _store(decision_memory_path(tmp_path), interval=interval, count=count)


def test_decision_store_quarantines_corrupt_startup_artifact(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    path.write_text("not-json", encoding="utf-8")
    store = _store(path)
    assert store.records == ()
    assert not path.exists()
    assert path.with_name(f"{path.name}.quarantine").exists()


def test_decision_store_crash_window_is_below_count_bound_and_immediate_is_acknowledged(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    store = _store(path, monotonic=lambda: 0)
    assert store.append(_record("alert", None, "durable baseline"), durability="immediate")
    routine = tuple(_record("miss", "left_spot", f"routine {index}") for index in range(49))
    assert store.extend(routine, durability="routine")
    crashed_reader = _store(path)
    assert [record.summary for record in crashed_reader.records] == ["durable baseline"]
    assert len(store.records) - len(crashed_reader.records) == 49
    assert store.append(_record("feedback", "left_spot", "immediate feedback"), durability="immediate")
    reloaded = load_decision_memory(path).records
    assert reloaded[-1].summary == "immediate feedback"
    assert len(reloaded) == 51


def test_external_unavailable_load_defers_without_overwriting_or_losing_dirty_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(path, _record("alert", None, "baseline"))
    store = _store(path, monotonic=lambda: 0)
    assert store.append(_record("miss", "left_spot", "local"), durability="routine")
    assert append_decision_memory_record(path, _record("alert", None, "external"))
    before = path.read_bytes()

    with monkeypatch.context() as context:
        context.setattr(
            decision_memory,
            "load_decision_memory",
            lambda *_args, **_kwargs: DecisionMemoryLoad(
                state="unavailable", error_type="PermissionError"
            ),
        )
        assert store.flush() is False

    assert path.read_bytes() == before
    assert [record.summary for record in store.records] == ["baseline", "local"]
    assert store.flush() is True
    assert [record.summary for record in load_decision_memory(path).records] == [
        "baseline",
        "external",
        "local",
    ]


def test_false_missing_load_for_extant_source_defers_without_overwriting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(path, _record("alert", None, "baseline"))
    store = _store(path, monotonic=lambda: 0)
    assert store.append(_record("miss", "left_spot", "local"), durability="routine")
    assert append_decision_memory_record(path, _record("alert", None, "external"))
    before = path.read_bytes()

    with monkeypatch.context() as context:
        context.setattr(
            decision_memory,
            "load_decision_memory",
            lambda *_args, **_kwargs: DecisionMemoryLoad(state="missing"),
        )
        assert store.flush() is False

    assert path.read_bytes() == before
    assert [record.summary for record in store.records] == ["baseline", "local"]
    assert store.flush() is True
    assert [record.summary for record in load_decision_memory(path).records] == [
        "baseline",
        "external",
        "local",
    ]


def test_available_load_for_missing_source_defers_without_creating_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    with monkeypatch.context() as context:
        context.setattr(
            decision_memory,
            "load_decision_memory",
            lambda *_args, **_kwargs: DecisionMemoryLoad(
                state="unavailable", error_type="PermissionError"
            ),
        )
        store = _store(path, monotonic=lambda: 0)
    assert store.append(_record("miss", "left_spot", "local"), durability="routine")

    with monkeypatch.context() as context:
        context.setattr(
            decision_memory,
            "load_decision_memory",
            lambda *_args, **_kwargs: DecisionMemoryLoad(
                state="available", records=(_record("alert", None, "phantom"),)
            ),
        )
        assert store.flush() is False

    assert not path.exists()
    assert [record.summary for record in store.records] == ["local"]
    assert store.flush() is True
    assert [record.summary for record in load_decision_memory(path).records] == ["local"]


def test_quarantined_external_source_requires_a_missing_source_retry(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(path, _record("alert", None, "baseline"))
    store = _store(path, monotonic=lambda: 0)
    assert store.append(_record("miss", "left_spot", "local"), durability="routine")
    path.write_text("not-json", encoding="utf-8")

    assert store.flush() is False
    assert not path.exists()
    assert path.with_name(f"{path.name}.quarantine").exists()
    assert store.flush() is True
    assert [record.summary for record in load_decision_memory(path).records] == [
        "baseline",
        "local",
    ]


def test_constructor_holds_memory_lock_across_load_and_signature_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(path, _record("alert", None, "baseline"))
    load_entered = Event()
    release_load = Event()
    writer_finished = Event()
    real_load = decision_memory.load_decision_memory

    def blocked_load(*args, **kwargs):
        if current_thread().name != "store-constructor":
            return real_load(*args, **kwargs)
        load_entered.set()
        assert release_load.wait(2)
        return real_load(*args, **kwargs)

    monkeypatch.setattr(decision_memory, "load_decision_memory", blocked_load)
    constructed: list[DecisionMemoryStore] = []
    constructor = Thread(
        target=lambda: constructed.append(_store(path)), name="store-constructor"
    )
    constructor.start()
    assert load_entered.wait(1)

    writer = Thread(
        target=lambda: (
            append_decision_memory_record(path, _record("alert", None, "external")),
            writer_finished.set(),
        )
    )
    writer.start()
    writer_was_blocked = writer_finished.wait(0.05) is False
    release_load.set()
    constructor.join(2)
    writer.join(2)
    assert writer_was_blocked
    assert len(constructed) == 1
    assert writer_finished.is_set()

    assert constructed[0].append(
        _record("miss", "left_spot", "local"), durability="immediate"
    )
    assert [record.summary for record in load_decision_memory(path).records] == [
        "baseline",
        "external",
        "local",
    ]


def test_constructor_reconciles_noncooperating_replacement_before_immediate_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(path, _record("alert", None, "baseline"))
    real_load = decision_memory.load_decision_memory

    def racing_load(*args, **kwargs):
        loaded = real_load(*args, **kwargs)
        decision_memory._write_memory(
            path,
            (*loaded.records, _record("alert", None, "external")),
        )
        return loaded

    with monkeypatch.context() as context:
        context.setattr(decision_memory, "load_decision_memory", racing_load)
        store = _store(path, monotonic=lambda: 0)

    assert store.append(
        _record("miss", "left_spot", "local"), durability="immediate"
    )
    assert [record.summary for record in load_decision_memory(path).records] == [
        "baseline",
        "external",
        "local",
    ]


def test_constructor_reconciles_false_missing_when_source_appears_during_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    real_load = decision_memory.load_decision_memory

    def racing_missing_load(*args, **kwargs):
        loaded = real_load(*args, **kwargs)
        decision_memory._write_memory(
            path,
            (_record("alert", None, "external"),),
        )
        return loaded

    with monkeypatch.context() as context:
        context.setattr(decision_memory, "load_decision_memory", racing_missing_load)
        store = _store(path, monotonic=lambda: 0)

    assert store.append(
        _record("miss", "left_spot", "local"), durability="immediate"
    )
    assert [record.summary for record in load_decision_memory(path).records] == [
        "external",
        "local",
    ]


@pytest.mark.parametrize("load_state", ["available", "unavailable", "partial"])
def test_constructor_rejects_unstable_records_when_source_remains_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    load_state: str,
) -> None:
    path = decision_memory_path(tmp_path)
    with monkeypatch.context() as context:
        context.setattr(
            decision_memory,
            "load_decision_memory",
            lambda *_args, **_kwargs: DecisionMemoryLoad(
                state=load_state,  # type: ignore[arg-type]
                records=(_record("alert", None, "unstable ghost"),),
                error_type="TransientLoad" if load_state != "available" else None,
            ),
        )
        store = _store(path, monotonic=lambda: 0)

    assert store.records == ()
    assert store.append(
        _record("miss", "left_spot", "local"), durability="immediate"
    )
    assert [record.summary for record in load_decision_memory(path).records] == [
        "local"
    ]


def test_constructor_false_missing_for_stable_available_source_reloads_external(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(path, _record("alert", None, "external"))
    with monkeypatch.context() as context:
        context.setattr(
            decision_memory,
            "load_decision_memory",
            lambda *_args, **_kwargs: DecisionMemoryLoad(state="missing"),
        )
        store = _store(path, monotonic=lambda: 0)

    assert store.records == ()
    assert store.append(
        _record("miss", "left_spot", "local"), durability="immediate"
    )
    assert [record.summary for record in load_decision_memory(path).records] == [
        "external",
        "local",
    ]


def test_external_signature_change_during_reconciliation_defers_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(path, _record("alert", None, "baseline"))
    store = _store(path, monotonic=lambda: 0)
    assert store.append(_record("miss", "left_spot", "local"), durability="routine")
    assert append_decision_memory_record(path, _record("alert", None, "external-one"))
    real_load = decision_memory.load_decision_memory

    def racing_load(*args, **kwargs):
        loaded = real_load(*args, **kwargs)
        decision_memory._write_memory(
            path,
            (*loaded.records, _record("alert", None, "external-two")),
        )
        return loaded

    with monkeypatch.context() as context:
        context.setattr(decision_memory, "load_decision_memory", racing_load)
        assert store.flush() is False

    assert [record.summary for record in load_decision_memory(path).records] == [
        "baseline",
        "external-one",
        "external-two",
    ]
    assert store.flush() is True
    assert [record.summary for record in load_decision_memory(path).records] == [
        "baseline",
        "external-one",
        "external-two",
        "local",
    ]


def test_failed_lab_outcome_append_propagates_false_without_recorded_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs = StringIO()
    logger = StructuredLogger(stream=logs)
    store = _store(tmp_path / "operator-decision-memory.json", logger=logger)
    monkeypatch.setattr(store, "append", lambda *_args, **_kwargs: False)

    assert _append_decision_memory(
        store,
        "command_outcome",
        spot_id=None,
        observed_at=None,
        summary="not durable",
        details={},
        logger=logger,
    ) is False
    assert _append_lab_outcome_memory(
        store,
        {
            "job_id": "job-1",
            "kind": "replay",
            "status": "failed",
            "phase": "persist",
            "updated_at": "2026-05-18T19:00:00Z",
        },
        data_dir=tmp_path,
        logger=logger,
    ) is False
    assert "detection-lab-outcome-recorded" not in logs.getvalue()
