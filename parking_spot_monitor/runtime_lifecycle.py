"""Process lifecycle signals and shutdown state for the capture runtime."""

from __future__ import annotations

import signal
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix import MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE, monitor_lifecycle_event
from parking_spot_monitor.matrix_dispatch import dispatch_matrix_event


@dataclass
class ShutdownState:
    """Mutable shutdown flag set from Unix signal handlers (no I/O)."""

    requested: bool = False
    signum: int | None = None

    @property
    def signal_name(self) -> str | None:
        if self.signum is None:
            return None
        return _signal_name(self.signum)


def install_shutdown_signal_handlers(
    state: ShutdownState,
    *,
    logger: StructuredLogger,
) -> dict[int, Any]:
    """Install SIGTERM/SIGINT handlers that only record ``state``."""

    handler = _make_shutdown_signal_handler(state)
    previous: dict[int, Any] = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, handler)
        except (ValueError, OSError):
            logger.warning("lifecycle-signal-handler-unavailable", signal=_signal_name(signum))
    return previous


def restore_shutdown_signal_handlers(previous: Mapping[int, Any]) -> None:
    for signum, handler in previous.items():
        try:
            signal.signal(signum, handler)
        except (ValueError, OSError):
            pass


@contextmanager
def monitor_signal_handlers(
    state: ShutdownState,
    *,
    logger: StructuredLogger,
) -> Iterator[ShutdownState]:
    previous = install_shutdown_signal_handlers(state, logger=logger)
    try:
        yield state
    finally:
        restore_shutdown_signal_handlers(previous)


def _make_shutdown_signal_handler(state: ShutdownState) -> Callable[[int, Any], None]:
    def handle(signum: int, _frame: Any) -> None:
        if not state.requested:
            state.requested = True
            state.signum = signum

    return handle


def _signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except ValueError:
        return f"signal-{signum}"


def return_if_shutdown_requested(
    *,
    shutdown_state: ShutdownState,
    matrix_delivery: Any | None,
    now_fn: Callable[[], datetime],
    logger: StructuredLogger,
    decision_memory_path: Path,
    iteration: int,
) -> int | None:
    """Exit the capture loop after dispatching a shutdown lifecycle notice."""

    if not shutdown_state.requested:
        return None
    logger.info("capture-loop-shutdown-requested", iteration=iteration, signal=shutdown_state.signal_name)
    dispatch_matrix_event(
        matrix_delivery,
        MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
        monitor_lifecycle_event(
            MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
            now_fn(),
            signal=shutdown_state.signal_name,
        ),
        logger=logger,
        decision_memory_path=decision_memory_path,
    )
    return 0
