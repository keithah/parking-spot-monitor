"""Indexed, detached single-record lookups for the durable outbox."""

from __future__ import annotations

import copy
from collections.abc import Sequence
from typing import Any, Protocol

from parking_monitor.outbox_models import OutboxRecord


class _LookupOwner(Protocol):
    _lock: Any
    _records: list[OutboxRecord]
    _index_by_id: dict[str, int]
    _indices_by_event_id: dict[str, tuple[int, ...]]


class OutboxLookupMixin:
    def get_record(self: _LookupOwner, record_id: str) -> OutboxRecord | None:
        with self._lock:
            index = self._index_by_id.get(record_id)
            return None if index is None else copy.deepcopy(self._records[index])

    def find_event_record(
        self: _LookupOwner,
        event_id: str,
        *,
        required_phase: str | None = None,
    ) -> OutboxRecord | None:
        with self._lock:
            for index in self._indices_by_event_id.get(event_id, ()):
                record = self._records[index]
                if required_phase is None or required_phase in record.phase_states:
                    return copy.deepcopy(record)
        return None


def build_event_index(records: Sequence[OutboxRecord]) -> dict[str, tuple[int, ...]]:
    mutable: dict[str, list[int]] = {}
    for index, record in enumerate(records):
        mutable.setdefault(record.intent.event_id, []).append(index)
    return {event_id: tuple(indices) for event_id, indices in mutable.items()}
