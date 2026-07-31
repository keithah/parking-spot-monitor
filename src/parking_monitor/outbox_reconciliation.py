"""Pure optimistic reconciliation for stale local outbox snapshots."""

from __future__ import annotations

from parking_monitor.outbox_models import OutboxPersistenceError, OutboxRecord

RecordKey = tuple[str, int]


def merge_records(
    base: list[OutboxRecord],
    proposed: list[OutboxRecord],
    current: list[OutboxRecord],
) -> list[OutboxRecord]:
    """Merge disjoint changes while rejecting divergent edits to one record."""
    base_by_key = dict(_keyed_records(base))
    current_pairs = _keyed_records(current)
    current_by_key = dict(current_pairs)
    current_index = {key: index for index, (key, _record) in enumerate(current_pairs)}
    current_ids = {record.id for record in current}
    merged = list(current)
    for key, record in _keyed_records(proposed):
        baseline = base_by_key.get(key)
        canonical = current_by_key.get(key)
        if baseline is None:
            if record.id not in current_ids:
                current_ids.add(record.id)
                merged.append(record)
            continue
        if record == baseline:
            continue
        if canonical is None or canonical not in (baseline, record):
            raise OutboxPersistenceError("concurrent outbox record mutation")
        if canonical == baseline:
            merged[current_index[key]] = record
            current_by_key[key] = record
    return merged


def _keyed_records(records: list[OutboxRecord]) -> list[tuple[RecordKey, OutboxRecord]]:
    occurrences: dict[str, int] = {}
    keyed: list[tuple[RecordKey, OutboxRecord]] = []
    for record in records:
        ordinal = occurrences.get(record.id, 0)
        keyed.append(((record.id, ordinal), record))
        occurrences[record.id] = ordinal + 1
    return keyed
