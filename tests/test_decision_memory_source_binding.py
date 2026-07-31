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


def _store(path: Path, *, max_records: int = 200) -> DecisionMemoryStore:
    return DecisionMemoryStore(
        path,
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=50,
        max_records=max_records,
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


def test_post_publication_replacement_defers_and_retries_with_local_records(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "operator-decision-memory.json"
    _write_memory(path, (_record("baseline"),))
    store = _store(path)
    assert store.append(_record("local___"), durability="routine")
    import parking_spot_monitor.operator_decision_memory as memory

    real_write = memory._write_memory

    def replace_after_publish(target, records, *args, **kwargs):
        result = real_write(target, records, *args, **kwargs)
        real_write(target, (_record("external"),))
        return result

    with monkeypatch.context() as context:
        context.setattr(memory, "_write_memory", replace_after_publish)
        assert store.flush() is False

    assert [item.summary for item in load_decision_memory(path).records] == ["external"]
    assert [item.summary for item in store.records] == ["baseline", "local___"]
    assert store.flush()
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external",
        "local___",
    ]


def test_replacement_immediately_before_exchange_is_restored_and_merged_on_retry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "operator-decision-memory.json"
    _write_memory(path, (_record("baseline"),))
    store = _store(path)
    assert store.append(_record("local___"), durability="routine")
    import parking_spot_monitor.operator_decision_memory as memory

    real_exchange = getattr(memory, "_conditional_exchange", None)
    raced = False

    def replace_before_exchange(source: Path, destination: Path) -> None:
        nonlocal raced
        if not raced:
            raced = True
            _write_memory(path, (_record("external"),))
        if real_exchange is not None:
            real_exchange(source, destination)

    monkeypatch.setattr(
        memory,
        "_conditional_exchange",
        replace_before_exchange,
        raising=False,
    )

    assert store.flush() is False
    assert [item.summary for item in load_decision_memory(path).records] == ["external"]
    monkeypatch.undo()
    assert store.flush()
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external",
        "local___",
    ]


def test_missing_source_creation_race_is_never_overwritten(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "operator-decision-memory.json"
    store = _store(path)
    assert store.append(_record("local___"), durability="routine")
    import parking_spot_monitor.operator_decision_memory as memory

    real_link = getattr(memory, "_conditional_link", None)
    raced = False

    def create_before_link(source: Path, destination: Path) -> None:
        nonlocal raced
        if not raced:
            raced = True
            _write_memory(path, (_record("external"),))
        if real_link is not None:
            real_link(source, destination)

    monkeypatch.setattr(memory, "_conditional_link", create_before_link, raising=False)

    assert store.flush() is False
    assert [item.summary for item in load_decision_memory(path).records] == ["external"]
    monkeypatch.undo()
    assert store.flush()
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external",
        "local___",
    ]


def test_confirmed_local_records_do_not_displace_newer_external_history(
    tmp_path: Path,
) -> None:
    path = tmp_path / "operator-decision-memory.json"
    store = _store(path, max_records=3)
    assert store.append(_record("local-old-1"), durability="immediate")
    assert store.append(_record("local-old-2"), durability="immediate")
    _write_memory(
        path,
        (_record("external-1"), _record("external-2"), _record("external-3")),
    )
    assert store.append(_record("local-new"), durability="routine")

    assert store.flush()
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external-2",
        "external-3",
        "local-new",
    ]


def test_rollback_restores_newer_canonical_writer_without_stranding_temp(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "operator-decision-memory.json"
    _write_memory(path, (_record("baseline"),))
    store = _store(path)
    assert store.append(_record("local___"), durability="routine")
    import parking_spot_monitor.operator_decision_memory as memory

    real_exchange = memory._conditional_exchange
    exchange_count = 0

    def replace_around_exchange(source: Path, destination: Path) -> None:
        nonlocal exchange_count
        exchange_count += 1
        if exchange_count == 1:
            _write_memory(path, (_record("external-before"),))
            real_exchange(source, destination)
            _write_memory(path, (_record("external-newer"),))
            return
        real_exchange(source, destination)

    monkeypatch.setattr(memory, "_conditional_exchange", replace_around_exchange)

    assert store.flush() is False
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external-newer"
    ]
    assert not list(tmp_path.glob(f".{path.name}.*.tmp"))
    monkeypatch.undo()
    assert store.flush()
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external-newer",
        "local___",
    ]


def test_rollback_exhaustion_restores_latest_external_writer_to_canonical(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "operator-decision-memory.json"
    _write_memory(path, (_record("baseline"),))
    store = _store(path)
    assert store.append(_record("local___"), durability="routine")
    import parking_spot_monitor.operator_decision_memory as memory

    real_exchange = memory._conditional_exchange
    exchange_count = 0

    def replace_before_every_exchange(source: Path, destination: Path) -> None:
        nonlocal exchange_count
        exchange_count += 1
        _write_memory(path, (_record(f"external-{exchange_count}"),))
        real_exchange(source, destination)

    monkeypatch.setattr(memory, "_conditional_exchange", replace_before_every_exchange)

    assert store.flush() is False
    assert exchange_count == 9
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external-9"
    ]
    assert not list(tmp_path.glob(f".{path.name}.*.tmp"))
    monkeypatch.undo()
    assert store.flush()
    assert [item.summary for item in load_decision_memory(path).records] == [
        "external-9",
        "local___",
    ]
