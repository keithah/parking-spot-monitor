from __future__ import annotations

from collections.abc import Callable
from typing import Any

from parking_spot_monitor.config import StreamConfig
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.runtime_log_aggregation import RuntimeLogAggregator


def capture_reconnect_delay(
    failure_count: int,
    *,
    initial_seconds: float,
    max_seconds: float,
    jitter_ratio: float,
    random_unit: Callable[[], float],
) -> float:
    if failure_count < 1:
        raise ValueError("failure_count must be positive")
    base = initial_seconds
    for _ in range(failure_count - 1):
        if base >= max_seconds / 2:
            return max_seconds
        base *= 2
    return min(base * (1 + jitter_ratio * random_unit()), max_seconds)


def log_capture_reconnect_failure(
    error: Any,
    *,
    failure_count: int,
    stream: StreamConfig,
    random_unit: Callable[[], float],
    log_aggregator: RuntimeLogAggregator,
    logger: StructuredLogger,
    iteration: int,
) -> float:
    delay = capture_reconnect_delay(
        failure_count,
        initial_seconds=stream.reconnect_seconds,
        max_seconds=stream.reconnect_max_seconds,
        jitter_ratio=stream.reconnect_jitter_ratio,
        random_unit=random_unit,
    )
    diagnostics = error.diagnostics()
    failure_type = str(diagnostics.get("reason", type(error).__name__))
    log = logger.error if log_aggregator.record_failure("capture", failure_type) else logger.debug
    log("capture-loop-failure", iteration=iteration, backoff_seconds=delay, **diagnostics)
    return delay
