from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import parking_spot_monitor.operator_decision_memory as decision_memory
import pytest
from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import (
    append_decision_memory_record,
    decision_memory_path,
    load_decision_memory,
    make_decision_memory_record,
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
