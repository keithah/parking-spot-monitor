from __future__ import annotations

from pathlib import Path
from typing import Protocol

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_models import MatrixSyncResult
from parking_spot_monitor.runtime_command_results import (
    record_matrix_command_failure,
    record_matrix_command_result,
)
from parking_spot_monitor.runtime_matrix_commands import RuntimeMatrixCommandPollOutcome


class RuntimeMatrixCommandPollResult(Protocol):
    processed_count: int
    ignored_count: int
    error_count: int
    bootstrapped: bool


class RuntimeMatrixCommandService(Protocol):
    def poll_once(self) -> RuntimeMatrixCommandPollResult: ...
    def fetch_once(self) -> MatrixSyncResult: ...
    def apply_sync_result(self, result: MatrixSyncResult) -> RuntimeMatrixCommandPollResult: ...
    def cancel_pending(self) -> None: ...


def _poll_matrix_commands_once(
    matrix_command_service: RuntimeMatrixCommandService | None,
    *,
    logger: StructuredLogger,
    iteration: int,
    decision_memory_path: Path | None = None,
) -> RuntimeMatrixCommandPollOutcome:
    if matrix_command_service is None:
        return RuntimeMatrixCommandPollOutcome()
    logger.debug(
        "matrix-command-poll-attempt",
        phase="matrix-command",
        action="matrix-command",
        iteration=iteration,
    )
    try:
        result = matrix_command_service.poll_once()
    except Exception as exc:
        return record_matrix_command_failure(
            exc,
            logger=logger,
            iteration=iteration,
            decision_memory_path=decision_memory_path,
        )
    return record_matrix_command_result(
        result,
        logger=logger,
        iteration=iteration,
        decision_memory_path=decision_memory_path,
    )
