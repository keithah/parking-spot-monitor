from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
from parking_spot_monitor.operator_decision_memory import (
    _write_memory,
    load_decision_memory,
    make_decision_memory_record,
)


def _record(summary: str):
    return make_decision_memory_record(
        "alert",
        observed_at=datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc),
        spot_id="left_spot",
        summary=summary,
    )


def _store(path: Path) -> DecisionMemoryStore:
    return DecisionMemoryStore(
        path,
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=50,
        monotonic=lambda: 0,
    )


def test_flush_merges_same_size_in_place_rewrite_with_preserved_mtime(
    tmp_path: Path,
) -> None:
    path = tmp_path / "operator-decision-memory.json"
    _write_memory(path, (_record("baseline"),))
    original_stat = path.stat()
    store = _store(path)
    assert store.append(_record("local___"), durability="routine")

    _write_memory(path, (_record("external"),))
    os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    assert store.flush()
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external",
        "local___",
    ]


def test_flush_never_overwrites_same_size_replacement_during_stable_load(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "operator-decision-memory.json"
    _write_memory(path, (_record("baseline"),))
    store = _store(path)
    assert store.append(_record("local___"), durability="routine")
    _write_memory(path, (_record("outside1"),))
    preserved = path.stat()

    import parking_spot_monitor.operator_decision_memory as memory

    real_load = memory.load_decision_memory
    replaced = False

    def replace_after_load(*args, **kwargs):
        nonlocal replaced
        loaded = real_load(*args, **kwargs)
        if not replaced:
            replaced = True
            _write_memory(path, (_record("outside2"),))
            os.utime(path, ns=(preserved.st_atime_ns, preserved.st_mtime_ns))
        return loaded

    monkeypatch.setattr(memory, "load_decision_memory", replace_after_load)

    assert store.flush() is False
    assert [item.summary for item in real_load(path).records] == ["outside2"]
    monkeypatch.setattr(memory, "load_decision_memory", real_load)
    assert store.flush()
    assert [item.summary for item in real_load(path).records] == [
        "outside2",
        "local___",
    ]
