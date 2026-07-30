"""Stable source snapshots and bounded decision-memory record reconciliation."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from parking_spot_monitor.operator_decision_memory import DecisionMemoryRecord


@dataclass(frozen=True, slots=True)
class DecisionMemorySourceSnapshot:
    signature: tuple[int, int] | None
    available: bool


def decision_memory_source_snapshot(path: Path) -> DecisionMemorySourceSnapshot:
    try:
        stat_result = path.stat()
    except FileNotFoundError:
        return DecisionMemorySourceSnapshot(None, True)
    except OSError:
        return DecisionMemorySourceSnapshot(None, False)
    return DecisionMemorySourceSnapshot((stat_result.st_mtime_ns, stat_result.st_size), True)


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
