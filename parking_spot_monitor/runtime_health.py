"""Runtime health reporting helpers for the capture loop."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.health import HealthStatus, write_health_status
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text


def safe_error_context(phase: str, error: BaseException, *, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    context = {
        "phase": phase,
        "error_type": type(error).__name__,
        "error_message": redact_diagnostic_text(error),
    }
    if extra:
        context.update(dict(extra))
    return context


def format_health_timestamp(value: Any | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def observed_at(frame_timestamp: Any | None, now: Callable[[], datetime]) -> datetime:
    parsed = parse_frame_timestamp(frame_timestamp)
    value = parsed if parsed is not None else now()
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def parse_frame_timestamp(value: Any | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def matrix_outbox_health_payload(matrix_outbox_file: Path | None) -> dict[str, Any] | None:
    if matrix_outbox_file is None:
        return None
    try:
        summary = LocalOutbox(matrix_outbox_file).status_summary()
    except Exception as exc:
        return {
            "available": False,
            "phase": "matrix-outbox",
            "error": {
                "phase": "matrix-outbox",
                "action": "status-summary",
                "error_type": type(exc).__name__,
                "error_message": "matrix outbox status unavailable",
            },
        }
    payload = dict(summary)
    payload["available"] = True
    return payload


@dataclass
class RuntimeLoopHealthState:
    consecutive_capture_failures: int = 0
    consecutive_detection_failures: int = 0
    last_frame_at: str | None = None
    selected_decode_mode: str | None = None
    last_matrix_error: dict[str, Any] | None = None
    last_error: dict[str, Any] | None = None
    state_save_error: dict[str, Any] | None = None
    last_matrix_command_error: dict[str, Any] | None = None
    matrix_command_failure_count: int = 0
    last_vehicle_history_error: dict[str, Any] | None = None
    vehicle_history_failure_count: int = 0
    retention_failure_count: int = 0

    def record_capture_success(self, *, timestamp: Any, selected_mode: Any) -> None:
        self.consecutive_capture_failures = 0
        self.last_frame_at = format_health_timestamp(timestamp)
        self.selected_decode_mode = str(selected_mode.value if hasattr(selected_mode, "value") else selected_mode)

    def record_capture_failure(self, error: BaseException, *, iteration: int) -> None:
        self.consecutive_capture_failures += 1
        self.last_error = safe_error_context("capture", error, extra={"iteration": iteration})

    def record_detection_failure(self, error: BaseException, *, iteration: int) -> None:
        self.consecutive_detection_failures += 1
        self.last_error = safe_error_context("detection", error, extra={"iteration": iteration})

    def record_detection_success(self) -> None:
        self.consecutive_detection_failures = 0
        self.last_error = None

    def record_matrix_result(self, error: dict[str, Any] | None) -> None:
        if error is not None:
            self.last_matrix_error = error
            self.last_error = error
            return
        self._clear_if_current(self.last_matrix_error)
        self.last_matrix_error = None

    def record_frame_update(
        self,
        *,
        matrix_errors: list[dict[str, Any]],
        history_errors: list[dict[str, Any]] | None,
        state_save_error: dict[str, Any] | None,
    ) -> None:
        self.record_matrix_result(matrix_errors[-1] if matrix_errors else None)
        if history_errors:
            self.vehicle_history_failure_count += len(history_errors)
            self.last_vehicle_history_error = history_errors[-1]
            self.last_error = self.last_vehicle_history_error
        else:
            self._clear_if_current(self.last_vehicle_history_error)
            self.vehicle_history_failure_count = 0
            self.last_vehicle_history_error = None
        previous_state_error = self.state_save_error
        self.state_save_error = state_save_error
        if state_save_error is not None:
            self.last_error = state_save_error
        else:
            self._clear_if_current(previous_state_error)

    def record_command_result(self, error: dict[str, Any] | None) -> None:
        if error is not None:
            error_count = error.get("error_count")
            self.matrix_command_failure_count += (
                error_count if isinstance(error_count, int) and error_count > 0 else 1
            )
            self.last_matrix_command_error = error
            self.last_error = error
            return
        self._clear_if_current(self.last_matrix_command_error)
        self.matrix_command_failure_count = 0
        self.last_matrix_command_error = None

    def status(self) -> str:
        return health_status_for_loop(
            consecutive_capture_failures=self.consecutive_capture_failures,
            consecutive_detection_failures=self.consecutive_detection_failures,
            last_matrix_error=self.last_matrix_error,
            state_save_error=self.state_save_error,
            retention_failure_count=self.retention_failure_count,
            matrix_command_failure_count=self.matrix_command_failure_count,
            last_matrix_command_error=self.last_matrix_command_error,
            vehicle_history_failure_count=self.vehicle_history_failure_count,
            last_vehicle_history_error=self.last_vehicle_history_error,
        )

    def _clear_if_current(self, previous_error: Mapping[str, Any] | None) -> None:
        if previous_error is not None and (self.last_error is previous_error or self.last_error == previous_error):
            self.last_error = None


def health_status_for_loop(
    *,
    consecutive_capture_failures: int,
    consecutive_detection_failures: int,
    last_matrix_error: Mapping[str, Any] | None,
    state_save_error: Mapping[str, Any] | None,
    retention_failure_count: int,
    matrix_command_failure_count: int = 0,
    last_matrix_command_error: Mapping[str, Any] | None = None,
    vehicle_history_failure_count: int = 0,
    last_vehicle_history_error: Mapping[str, Any] | None = None,
) -> str:
    if consecutive_capture_failures:
        return "down"
    if (
        consecutive_detection_failures
        or last_matrix_error is not None
        or state_save_error is not None
        or retention_failure_count
        or matrix_command_failure_count
        or last_matrix_command_error is not None
        or vehicle_history_failure_count
        or last_vehicle_history_error is not None
    ):
        return "degraded"
    return "ok"


def write_loop_health(
    settings: RuntimeSettings,
    *,
    logger: StructuredLogger,
    status: str,
    iteration: int,
    last_frame_at: str | None,
    selected_decode_mode: str | None,
    consecutive_capture_failures: int,
    consecutive_detection_failures: int,
    last_matrix_error: Mapping[str, Any] | None,
    last_error: Mapping[str, Any] | None,
    retention_failure_count: int,
    state_save_error: Mapping[str, Any] | None,
    matrix_command_failure_count: int = 0,
    last_matrix_command_error: Mapping[str, Any] | None = None,
    vehicle_history_failure_count: int = 0,
    last_vehicle_history_error: Mapping[str, Any] | None = None,
    vehicle_history: Mapping[str, Any] | None = None,
    matrix_outbox_file: Path | None = None,
) -> None:
    try:
        write_health_status(
            settings.runtime.health_file,
            HealthStatus(
                status=status,  # type: ignore[arg-type]
                updated_at=datetime.now(timezone.utc).isoformat(),
                iteration=iteration,
                last_frame_at=last_frame_at,
                selected_decode_mode=selected_decode_mode,
                consecutive_capture_failures=consecutive_capture_failures,
                consecutive_detection_failures=consecutive_detection_failures,
                last_matrix_error=last_matrix_error,
                last_error=last_error,
                retention_failure_count=retention_failure_count,
                state_save_error=state_save_error,
                matrix_command_failure_count=matrix_command_failure_count,
                last_matrix_command_error=last_matrix_command_error,
                vehicle_history_failure_count=vehicle_history_failure_count,
                last_vehicle_history_error=last_vehicle_history_error,
                vehicle_history=vehicle_history,
                matrix_outbox=matrix_outbox_health_payload(matrix_outbox_file),
            ),
            logger=logger,
        )
    except Exception as exc:
        logger.error(
            "health-write-failed",
            path=str(settings.runtime.health_file),
            error_type=type(exc).__name__,
            error_message=redact_diagnostic_text(exc),
        )
