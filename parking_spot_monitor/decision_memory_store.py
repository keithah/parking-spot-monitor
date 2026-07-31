"""Service-scoped, checkpointed ownership of operator decision memory."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Literal

import parking_spot_monitor.operator_decision_memory as _memory
from parking_spot_monitor.decision_memory_reconciliation import (
    decision_memory_load_is_consistent,
    decision_memory_source_snapshot,
    deduplicated_decision_memory_tail,
)
from parking_spot_monitor.decision_memory_store_validation import positive_finite_float, positive_int
from parking_spot_monitor.decision_memory_store_publication import (
    load_initial_state,
    publish_bounded_candidate,
    verified_publication_signature,
)
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import (
    MAX_MEMORY_FILE_BYTES,
    MAX_RECORDS,
    DecisionMemoryRecord,
)

DecisionMemoryDurability = Literal["routine", "immediate"]


class DecisionMemoryStore:
    """Keep bounded decision records in memory and publish explicit durability tiers."""

    def __init__(
        self,
        path: str | Path,
        *,
        checkpoint_interval_seconds: float,
        checkpoint_max_pending_records: int,
        max_records: int = MAX_RECORDS,
        max_file_bytes: int = MAX_MEMORY_FILE_BYTES,
        monotonic: Callable[[], float] = time.monotonic,
        logger: StructuredLogger | None = None,
    ) -> None:
        self.path = Path(path)
        self.checkpoint_interval_seconds = positive_finite_float(
            checkpoint_interval_seconds, "checkpoint_interval_seconds"
        )
        self.checkpoint_max_pending_records = positive_int(
            checkpoint_max_pending_records, "checkpoint_max_pending_records"
        )
        self.max_records = positive_int(max_records, "max_records")
        self.max_file_bytes = min(
            positive_int(max_file_bytes, "max_file_bytes"),
            MAX_MEMORY_FILE_BYTES,
        )
        self._monotonic = monotonic
        self._logger = logger
        initial = load_initial_state(
            self.path,
            max_records=self.max_records,
            max_file_bytes=self.max_file_bytes,
            logger=logger,
        )
        self._records: deque[DecisionMemoryRecord] = deque(initial.records, maxlen=self.max_records)
        self._dirty_records: deque[DecisionMemoryRecord] = deque(maxlen=self.max_records)
        self._dirty = False
        self._pending_count = 0
        now = self._monotonic()
        self._next_checkpoint_at = now + self.checkpoint_interval_seconds
        self._signature = initial.signature
        self._reconcile_required = initial.reconcile_required

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

    def wait_for_checkpoint(
        self,
        wait_seconds: float,
        *,
        wait: Callable[[float], bool],
    ) -> bool:
        wait_deadline = self._monotonic() + wait_seconds
        bounded = self._bounded_wait_seconds(wait_seconds)
        if wait(bounded):
            return True
        self.checkpoint_if_due()
        if bounded >= wait_seconds:
            return False
        remaining = max(0.0, wait_deadline - self._monotonic())
        return wait(remaining) if remaining else False

    def flush(self) -> bool:
        with _memory._MEMORY_WRITE_LOCK:
            if not self._dirty:
                return True
            return self._flush_locked()

    def close(self) -> bool:
        return self.flush()

    def _flush_locked(self) -> bool:
        candidate = tuple(self._records)
        conflicts_to_clear = ()
        try:
            before = decision_memory_source_snapshot(self.path, self.max_file_bytes)
            if not before.available:
                return self._defer_reconciliation("source-stat-unavailable")
            if self._reconcile_required or before.signature != self._signature:
                external = _memory.load_decision_memory(
                    self.path,
                    max_records=self.max_records,
                    max_file_bytes=self.max_file_bytes,
                    logger=self._logger,
                )
                after = decision_memory_source_snapshot(self.path, self.max_file_bytes)
                if external.state not in {"available", "missing"}:
                    return self._defer_reconciliation(
                        "source-load-unavailable", source_state=external.state
                    )
                if not after.available or after.signature != before.signature:
                    return self._defer_reconciliation("source-changed-during-load")
                if not decision_memory_load_is_consistent(
                    external.state,
                    before,
                    after,
                    external.source_signature,
                    has_conflicts=bool(external.conflict_signatures),
                ):
                    return self._defer_reconciliation("source-state-signature-mismatch")
                if external.state == "available":
                    candidate = deduplicated_decision_memory_tail(
                        (*external.records, *self._dirty_records),
                        max_records=self.max_records,
                    )
                    conflicts_to_clear = external.conflict_signatures
            candidate, conflicts_to_clear, publication = publish_bounded_candidate(
                self.path,
                candidate,
                conflicts_to_clear,
                expected_signature=before.signature,
                max_records=self.max_records,
                max_file_bytes=self.max_file_bytes,
            )
        except Exception as exc:
            _memory._log(
                self._logger,
                "warning",
                "operator-decision-memory-append-failed",
                path=self.path,
                error_type=type(exc).__name__,
            )
            return False
        signature = verified_publication_signature(
            self.path, publication, max_file_bytes=self.max_file_bytes
        )
        if signature is None:
            return self._defer_reconciliation("written-source-stat-unavailable")
        self._records = deque(candidate, maxlen=self.max_records)
        self._signature = signature
        self._dirty_records.clear()
        self._reconcile_required = not _memory.clear_decision_memory_conflicts(
            conflicts_to_clear,
            max_file_bytes=self.max_file_bytes,
        )
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

    def _bounded_wait_seconds(self, wait_seconds: float) -> float:
        with _memory._MEMORY_WRITE_LOCK:
            if not self._dirty:
                return wait_seconds
            return min(
                wait_seconds,
                max(0.0, self._next_checkpoint_at - self._monotonic()),
            )

    def _defer_reconciliation(
        self,
        reason_code: str,
        *,
        source_state: str | None = None,
    ) -> bool:
        _memory._log(
            self._logger,
            "warning",
            "operator-decision-memory-append-failed",
            path=self.path,
            error_type="DecisionMemorySourceUnavailable",
            reason_code=reason_code,
            source_state=source_state,
        )
        return False
