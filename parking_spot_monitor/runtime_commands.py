from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_dispatch import append_matrix_event_memory
from parking_spot_monitor.runtime_decision_memory import _append_decision_memory
from parking_spot_monitor.runtime_health import safe_error_context as _safe_error_context
from parking_spot_monitor.runtime_matrix_commands import RuntimeMatrixCommandPollOutcome


class RuntimeMatrixCommandPollResult(Protocol):
    processed_count: int
    ignored_count: int
    error_count: int
    bootstrapped: bool


class RuntimeMatrixCommandService(Protocol):
    def poll_once(self) -> RuntimeMatrixCommandPollResult: ...


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
        context = _safe_error_context(
            "matrix-command", exc, extra={"action": "matrix-command", "iteration": iteration}
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
    processed_count = getattr(result, "processed_count", None)
    ignored_count = getattr(result, "ignored_count", None)
    error_count = getattr(result, "error_count", None)
    bootstrapped = getattr(result, "bootstrapped", None)
    log_success = (
        logger.info
        if bootstrapped is True
        or any(
            isinstance(count, int) and count > 0
            for count in (processed_count, ignored_count, error_count)
        )
        else logger.debug
    )
    log_success(
        "matrix-command-poll-succeeded",
        phase="matrix-command",
        action="matrix-command",
        iteration=iteration,
        processed_count=processed_count,
        ignored_count=ignored_count,
        error_count=error_count,
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
                "processed_count": processed_count,
                "ignored_count": ignored_count,
                "error_count": error_count,
                "bootstrapped": bootstrapped,
            },
            logger=logger,
        )
    if isinstance(error_count, int) and error_count > 0:
        context = {
            "phase": "matrix-command",
            "action": "matrix-command",
            "iteration": iteration,
            "error_type": "poll_result_errors",
            "message": "matrix command poll completed with command errors",
            "error_count": error_count,
            "processed_count": processed_count,
        }
        logger.warning("matrix-command-poll-degraded", **context)
        append_matrix_event_memory(
            decision_memory_path,
            event_name="matrix-command",
            event={"event_id": f"poll:{iteration}"},
            outcome="failed",
            error_type=context["error_type"],
            logger=logger,
        )
        return RuntimeMatrixCommandPollOutcome(health_error=context)
    return RuntimeMatrixCommandPollOutcome()
