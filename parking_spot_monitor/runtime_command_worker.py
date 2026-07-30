from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_models import MatrixSyncResult
from parking_spot_monitor.runtime_command_results import (
    CommandResult,
    collect_matrix_commands_once,
    record_matrix_command_failure,
    record_matrix_command_result,
)
from parking_spot_monitor.runtime_matrix_commands import (
    MatrixCommandPollState,
    MatrixCommandSchedule,
    command_poll_due,
    record_command_poll_result,
)


class CommandService(Protocol):
    def poll_once(self) -> CommandResult: ...
    def fetch_once(self) -> MatrixSyncResult: ...
    def apply_sync_result(self, result: MatrixSyncResult) -> CommandResult: ...
    def cancel_pending(self) -> None: ...


class CommandHealth(Protocol):
    def record_command_result(self, error: dict[str, object] | None) -> None: ...


class MatrixCommandPollWorker:
    """Run one fetch at a time and retain at most one completed result."""

    def __init__(
        self,
        fetch_once: Callable[[], MatrixSyncResult],
        *,
        cancel_pending: Callable[[], None] | None = None,
        close_timeout_seconds: float = 2,
    ) -> None:
        self._fetch_once = fetch_once
        self._cancel_pending = cancel_pending
        self._close_timeout_seconds = max(0.0, close_timeout_seconds)
        self._condition = threading.Condition()
        self._requested = False
        self._running = False
        self._completed: MatrixSyncResult | BaseException | None = None
        self._closed = False
        self._thread = threading.Thread(
            target=self._run,
            name="matrix-command-fetch",
            daemon=True,
        )
        self._thread.start()

    def request(self) -> bool:
        with self._condition:
            if self._closed or self._requested or self._running or self._completed is not None:
                return False
            self._requested = True
            self._condition.notify()
            return True

    def take_completed(self) -> MatrixSyncResult | BaseException | None:
        with self._condition:
            completed = self._completed
            self._completed = None
            return completed

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._requested = False
            self._condition.notify_all()
        if self._cancel_pending is not None:
            try:
                self._cancel_pending()
            except Exception:
                pass
        self._thread.join(self._close_timeout_seconds)

    def _run(self) -> None:
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._closed or self._requested)
                if self._closed:
                    return
                self._requested = False
                self._running = True
            try:
                completed: MatrixSyncResult | BaseException = self._fetch_once()
            except BaseException as exc:
                completed = exc
            with self._condition:
                self._running = False
                if not self._closed:
                    self._completed = completed


def build_matrix_command_worker(service: object | None) -> MatrixCommandPollWorker | None:
    fetch = getattr(service, "fetch_once", None)
    apply = getattr(service, "apply_sync_result", None)
    cancel = getattr(service, "cancel_pending", None)
    if not callable(fetch) or not callable(apply):
        return None
    return MatrixCommandPollWorker(
        fetch,
        cancel_pending=cancel if callable(cancel) else None,
    )


def advance_matrix_command_poll(
    service: CommandService,
    worker: MatrixCommandPollWorker | None,
    *,
    settings: MatrixCommandSchedule,
    state: MatrixCommandPollState,
    now_monotonic: float,
    logger: StructuredLogger,
    iteration: int,
    health: CommandHealth,
    decision_memory_path: Path | None,
    completed_at: Callable[[], float],
) -> MatrixCommandPollState:
    if worker is None:
        if not command_poll_due(settings, state, now_monotonic):
            return state
        try:
            outcome = record_matrix_command_result(
                service.poll_once(),
                logger=logger,
                iteration=iteration,
                decision_memory_path=decision_memory_path,
            )
        except Exception as exc:
            outcome = record_matrix_command_failure(
                exc,
                logger=logger,
                iteration=iteration,
                decision_memory_path=decision_memory_path,
            )
        health.record_command_result(outcome.health_error)
        return record_command_poll_result(
            settings,
            state,
            completed_at(),
            failed=outcome.transport_failed,
        )
    completed = worker.take_completed()
    if completed is not None:
        outcome = collect_matrix_commands_once(
            service,
            completed,
            logger=logger,
            iteration=iteration,
            decision_memory_path=decision_memory_path,
        )
        state = record_command_poll_result(
            settings,
            state,
            completed_at(),
            failed=outcome.transport_failed,
        )
        health.record_command_result(outcome.health_error)
    if command_poll_due(settings, state, now_monotonic):
        worker.request()
    return state
