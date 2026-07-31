"""Stable source snapshots and bounded decision-memory record reconciliation."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from parking_spot_monitor.decision_memory_publication import (
    SourceSignature,
    read_decision_memory_source,
)
from parking_spot_monitor.operator_decision_memory import (
    MAX_MEMORY_FILE_BYTES,
    DecisionMemoryRecord,
    LoadState,
)


@dataclass(frozen=True, slots=True)
class DecisionMemorySourceSnapshot:
    signature: SourceSignature | None
    available: bool


def decision_memory_source_snapshot(path: Path) -> DecisionMemorySourceSnapshot:
    try:
        _raw, signature = read_decision_memory_source(path, MAX_MEMORY_FILE_BYTES)
    except FileNotFoundError:
        return DecisionMemorySourceSnapshot(None, True)
    except (OSError, OverflowError):
        return DecisionMemorySourceSnapshot(None, False)
    return DecisionMemorySourceSnapshot(signature, True)


def decision_memory_load_is_consistent(
    state: LoadState,
    before: DecisionMemorySourceSnapshot,
    after: DecisionMemorySourceSnapshot,
    source_signature: SourceSignature | None = None,
    *,
    has_conflicts: bool = False,
) -> bool:
    if not before.available or not after.available or before.signature != after.signature:
        return False
    expected_state = (
        "missing" if before.signature is None and not has_conflicts else "available"
    )
    if state != expected_state:
        return False
    return source_signature is None or source_signature == before.signature


def deduplicated_decision_memory_tail(
    records: Sequence[DecisionMemoryRecord],
    *,
    max_records: int,
) -> tuple[DecisionMemoryRecord, ...]:
    unique: dict[str, DecisionMemoryRecord] = {}
    for record in records:
        key = json.dumps(
            record.to_json_dict(),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        unique[key] = record
    return tuple(unique.values())[-max_records:]
