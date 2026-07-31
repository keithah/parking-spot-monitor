"""Bounded publication and verification for the decision-memory store."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import parking_spot_monitor.operator_decision_memory as _memory
from parking_spot_monitor.decision_memory_publication import (
    ConditionalPublication,
    SourceSignature,
)
from parking_spot_monitor.decision_memory_reconciliation import (
    decision_memory_load_is_consistent,
    decision_memory_source_snapshot,
)
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import DecisionMemoryRecord


@dataclass(frozen=True, slots=True)
class InitialDecisionMemoryState:
    records: tuple[DecisionMemoryRecord, ...]
    signature: SourceSignature | None
    reconcile_required: bool


def load_initial_state(
    path: Path,
    *,
    max_records: int,
    max_file_bytes: int,
    logger: StructuredLogger | None,
) -> InitialDecisionMemoryState:
    with _memory._MEMORY_WRITE_LOCK:
        before = decision_memory_source_snapshot(path, max_file_bytes)
        loaded = _memory.load_decision_memory(
            path,
            max_records=max_records,
            max_file_bytes=max_file_bytes,
            logger=logger,
        )
        after = decision_memory_source_snapshot(path, max_file_bytes)
    consistent = decision_memory_load_is_consistent(
        loaded.state,
        before,
        after,
        loaded.source_signature,
        has_conflicts=bool(loaded.conflict_signatures),
    )
    return InitialDecisionMemoryState(
        records=loaded.records if consistent else (),
        signature=after.signature,
        reconcile_required=not consistent or bool(loaded.conflict_signatures),
    )


def publish_bounded_candidate(
    path: Path,
    candidate: Sequence[DecisionMemoryRecord],
    conflicts: Sequence[tuple[Path, SourceSignature]],
    *,
    expected_signature: SourceSignature | None,
    max_records: int,
    max_file_bytes: int,
) -> tuple[
    tuple[DecisionMemoryRecord, ...],
    tuple[tuple[Path, SourceSignature], ...],
    ConditionalPublication | None,
]:
    bounded, _encoded = _memory._bounded_memory_payload(
        candidate,
        max_records=max_records,
        max_file_bytes=max_file_bytes,
    )
    retained_conflicts = tuple(conflicts)
    if retained_conflicts:
        retained_conflicts = _memory.compact_decision_memory_conflicts(
            path,
            bounded,
            retained_conflicts,
            max_records=max_records,
            max_file_bytes=max_file_bytes,
        )
    publication = _memory._write_memory(
        path,
        bounded,
        expected_signature=expected_signature,
        max_file_bytes=max_file_bytes,
    )
    return bounded, retained_conflicts, publication


def verified_publication_signature(
    path: Path,
    publication: ConditionalPublication | None,
    *,
    max_file_bytes: int,
) -> SourceSignature | None:
    if publication is None or not publication.published or publication.signature is None:
        return None
    written = decision_memory_source_snapshot(path, max_file_bytes)
    return publication.signature if written.signature == publication.signature else None
