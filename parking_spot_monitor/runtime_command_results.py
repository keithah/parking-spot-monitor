from __future__ import annotations

from pathlib import Path
from typing import Protocol

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_dispatch import append_matrix_event_memory
from parking_spot_monitor.matrix_models import MatrixSyncResult
from parking_spot_monitor.runtime_decision_memory import _append_decision_memory
from parking_spot_monitor.runtime_health import safe_error_context
from parking_spot_monitor.runtime_matrix_commands import RuntimeMatrixCommandPollOutcome


class CommandResult(Protocol):
    processed_count: int
    ignored_count: int
    error_count: int
    bootstrapped: bool


class CommandApplyService(Protocol):
    def apply_sync_result(self, result: MatrixSyncResult) -> CommandResult: ...


def collect_matrix_commands_once(
    service: CommandApplyService,
    completed: MatrixSyncResult | BaseException,
    *,
    logger: StructuredLogger,
    iteration: int,
    decision_memory_path: Path | None = None,
) -> RuntimeMatrixCommandPollOutcome:
    if isinstance(completed, BaseException):
        return record_matrix_command_failure(
            completed, logger=logger, iteration=iteration, decision_memory_path=decision_memory_path
        )
    try:
        result = service.apply_sync_result(completed)
    except Exception as exc:
        return record_matrix_command_failure(
            exc, logger=logger, iteration=iteration, decision_memory_path=decision_memory_path
        )
    return record_matrix_command_result(
        result, logger=logger, iteration=iteration, decision_memory_path=decision_memory_path
    )


def record_matrix_command_failure(
    error: BaseException,
    *,
    logger: StructuredLogger,
    iteration: int,
    decision_memory_path: Path | None,
) -> RuntimeMatrixCommandPollOutcome:
    context = safe_error_context(
        "matrix-command", error, extra={"action": "matrix-command", "iteration": iteration}
    )
    logger.warning("matrix-command-poll-failed", **context)
    append_matrix_event_memory(
        decision_memory_path,
        event_name="matrix-command",
        event={"event_id": f"poll:{iteration}"},
        outcome="failed",
        error_type=context.get("error_type"),
        logger=logger,
    )
    return RuntimeMatrixCommandPollOutcome(transport_failed=True, health_error=context)


def record_matrix_command_result(
    result: CommandResult,
    *,
    logger: StructuredLogger,
    iteration: int,
    decision_memory_path: Path | None,
) -> RuntimeMatrixCommandPollOutcome:
    processed = getattr(result, "processed_count", None)
    ignored = getattr(result, "ignored_count", None)
    errors = getattr(result, "error_count", None)
    bootstrapped = getattr(result, "bootstrapped", None)
    log = logger.info if any(isinstance(value, int) and value > 0 for value in (processed, ignored, errors)) else logger.debug
    log(
        "matrix-command-poll-succeeded",
        phase="matrix-command",
        action="matrix-command",
        iteration=iteration,
        processed_count=processed,
        ignored_count=ignored,
        error_count=errors,
        bootstrapped=bootstrapped,
    )
    if decision_memory_path is not None:
        _append_decision_memory(
            decision_memory_path,
            "command_outcome",
            spot_id=None,
            observed_at=None,
            summary="matrix-command poll polled",
            details={
                "event_type": "matrix-command",
                "event_id": f"poll:{iteration}",
                "outcome": "polled",
                "processed_count": processed,
                "ignored_count": ignored,
                "error_count": errors,
                "bootstrapped": bootstrapped,
            },
            logger=logger,
        )
    if not isinstance(errors, int) or errors <= 0:
        return RuntimeMatrixCommandPollOutcome()
    context: dict[str, object] = {
        "phase": "matrix-command",
        "action": "matrix-command",
        "iteration": iteration,
        "error_type": "poll_result_errors",
        "message": "matrix command poll completed with command errors",
        "error_count": errors,
        "processed_count": processed,
    }
    logger.warning("matrix-command-poll-degraded", **context)
    append_matrix_event_memory(
        decision_memory_path,
        event_name="matrix-command",
        event={"event_id": f"poll:{iteration}"},
        outcome="failed",
        error_type="poll_result_errors",
        logger=logger,
    )
    return RuntimeMatrixCommandPollOutcome(health_error=context)
