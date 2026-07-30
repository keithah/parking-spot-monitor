"""Service-scoped, checkpointed ownership of operator decision memory."""

from __future__ import annotations

import json
import math
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Literal

import parking_spot_monitor.operator_decision_memory as _memory
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import (
    MAX_MEMORY_FILE_BYTES,
    MAX_RECORDS,
    DecisionMemoryRecord,
)

DecisionMemoryDurability = Literal["routine", "immediate"]


def runtime_decision_memory_store(
    runtime_settings: object,
    path: str | Path,
    *,
    monotonic: Callable[[], float],
    logger: StructuredLogger | None,
) -> DecisionMemoryStore:
    return DecisionMemoryStore(
        path,
        checkpoint_interval_seconds=float(
            getattr(runtime_settings, "decision_memory_checkpoint_interval_seconds")
        ),
        checkpoint_max_pending_records=int(
            getattr(runtime_settings, "decision_memory_checkpoint_max_pending_records")
        ),
        monotonic=monotonic,
        logger=logger,
    )


class DecisionMemoryStore:
    """Keep bounded decision records in memory and publish explicit durability tiers."""

    def __init__(
        self,
        path: str | Path,
        *,
        checkpoint_interval_seconds: float,
        checkpoint_max_pending_records: int,
        max_records: int = MAX_RECORDS,
        monotonic: Callable[[], float] = time.monotonic,
        logger: StructuredLogger | None = None,
    ) -> None:
        self.path = Path(path)
        self.checkpoint_interval_seconds = _positive_finite_float(
            checkpoint_interval_seconds, "checkpoint_interval_seconds"
        )
        self.checkpoint_max_pending_records = _positive_int(
            checkpoint_max_pending_records, "checkpoint_max_pending_records"
        )
        self.max_records = _positive_int(max_records, "max_records")
        self._monotonic = monotonic
        self._logger = logger
        loaded = _memory.load_decision_memory(
            self.path,
            max_records=self.max_records,
            max_file_bytes=MAX_MEMORY_FILE_BYTES,
            logger=logger,
        )
        self._records: deque[DecisionMemoryRecord] = deque(
            loaded.records,
            maxlen=self.max_records,
        )
        self._dirty_records: deque[DecisionMemoryRecord] = deque(maxlen=self.max_records)
        self._dirty = False
        self._pending_count = 0
        now = self._monotonic()
        self._next_checkpoint_at = now + self.checkpoint_interval_seconds
        self._signature = _file_signature(self.path)

    @property
    def records(self) -> tuple[DecisionMemoryRecord, ...]:
        with _memory._MEMORY_WRITE_LOCK:
            return tuple(self._records)

    def append(
        self,
        record: DecisionMemoryRecord | Mapping[str, object],
        *,
        durability: DecisionMemoryDurability,
    ) -> bool:
        return self.extend((record,), durability=durability)

    def extend(
        self,
        records: Sequence[DecisionMemoryRecord | Mapping[str, object]],
        *,
        durability: DecisionMemoryDurability,
    ) -> bool:
        if durability not in {"routine", "immediate"}:
            raise ValueError("durability must be 'routine' or 'immediate'")
        sanitized = tuple(_memory._record_from_any(record) for record in records)
        if not sanitized:
            return True
        with _memory._MEMORY_WRITE_LOCK:
            self._records.extend(sanitized)
            self._dirty_records.extend(sanitized)
            self._dirty = True
            self._pending_count += len(sanitized)
            if durability == "immediate" or self._pending_count >= self.checkpoint_max_pending_records:
                return self._flush_locked()
        return True

    def checkpoint_if_due(self) -> bool:
        with _memory._MEMORY_WRITE_LOCK:
            if not self._dirty or self._monotonic() < self._next_checkpoint_at:
                return False
            return self._flush_locked()

    def flush(self) -> bool:
        with _memory._MEMORY_WRITE_LOCK:
            if not self._dirty:
                return True
            return self._flush_locked()

    def close(self) -> bool:
        return self.flush()

    def _flush_locked(self) -> bool:
        try:
            current_signature = _file_signature(self.path)
            if current_signature != self._signature:
                external = _memory.load_decision_memory(
                    self.path,
                    max_records=self.max_records,
                    max_file_bytes=MAX_MEMORY_FILE_BYTES,
                    logger=self._logger,
                )
                merged = _deduplicated_tail(
                    (*external.records, *self._dirty_records),
                    max_records=self.max_records,
                )
                self._records = deque(merged, maxlen=self.max_records)
            _memory._write_memory(self.path, tuple(self._records))
        except Exception as exc:
            _memory._log(
                self._logger,
                "warning",
                "operator-decision-memory-append-failed",
                path=self.path,
                error_type=type(exc).__name__,
            )
            return False
        self._signature = _file_signature(self.path)
        self._dirty_records.clear()
        self._dirty = False
        self._pending_count = 0
        self._next_checkpoint_at = self._monotonic() + self.checkpoint_interval_seconds
        _memory._log(
            self._logger,
            "debug",
            "operator-decision-memory-checkpointed",
            path=self.path,
            record_count=len(self._records),
        )
        return True


def _deduplicated_tail(
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


def _file_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat_result = path.stat()
    except (FileNotFoundError, OSError):
        return None
    return stat_result.st_mtime_ns, stat_result.st_size


def _positive_finite_float(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be a positive finite number")
    resolved = float(value)
    if not math.isfinite(resolved) or resolved <= 0:
        raise ValueError(f"{field_name} must be a positive finite number")
    return resolved


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value
