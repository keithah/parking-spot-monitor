"""Runtime construction for the service-scoped decision-memory store."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
from parking_spot_monitor.logging import StructuredLogger


class DecisionMemoryRuntimeSettings(Protocol):
    decision_memory_checkpoint_interval_seconds: float
    decision_memory_checkpoint_max_pending_records: int


def runtime_decision_memory_store(
    runtime_settings: DecisionMemoryRuntimeSettings,
    path: str | Path,
    *,
    monotonic: Callable[[], float],
    logger: StructuredLogger | None,
) -> DecisionMemoryStore:
    return DecisionMemoryStore(
        path,
        checkpoint_interval_seconds=float(runtime_settings.decision_memory_checkpoint_interval_seconds),
        checkpoint_max_pending_records=int(runtime_settings.decision_memory_checkpoint_max_pending_records),
        monotonic=monotonic,
        logger=logger,
    )
