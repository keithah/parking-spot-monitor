from __future__ import annotations

from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_dispatch import append_matrix_event_memory
from parking_spot_monitor.runtime_decision_memory import _append_decision_memory
from parking_spot_monitor.runtime_health import safe_error_context as _safe_error_context


def _poll_matrix_commands_once(
    matrix_command_service: Any | None,
    *,
    logger: StructuredLogger,
    iteration: int,
    decision_memory_path: Path | None = None,
) -> dict[str, Any] | None:
    if matrix_command_service is None:
        return None
    logger.info(
        "matrix-command-poll-attempt",
        phase="matrix-command",
        action="matrix-command",
        iteration=iteration,
    )
    try:
        result = matrix_command_service.poll_once()
    except Exception as exc:
        context = _safe_error_context(
            "matrix-command",
            exc,
            extra={
                "action": "matrix-command",
                "iteration": iteration,
            },
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
        return context
    logger.info(
        "matrix-command-poll-succeeded",
        phase="matrix-command",
        action="matrix-command",
        iteration=iteration,
        processed_count=getattr(result, "processed_count", None),
        ignored_count=getattr(result, "ignored_count", None),
        error_count=getattr(result, "error_count", None),
        bootstrapped=getattr(result, "bootstrapped", None),
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
                "processed_count": getattr(result, "processed_count", None),
                "ignored_count": getattr(result, "ignored_count", None),
                "error_count": getattr(result, "error_count", None),
                "bootstrapped": getattr(result, "bootstrapped", None),
            },
            logger=logger,
        )
    return None
