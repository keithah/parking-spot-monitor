"""Runtime construction for the service-scoped decision-memory store."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
from parking_spot_monitor.logging import StructuredLogger


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
