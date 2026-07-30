"""Capture-loop health reporting with explicit runtime dependencies."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.paths import RuntimePaths
from parking_spot_monitor.runtime_health import RuntimeLoopHealthState, write_loop_health
from parking_spot_monitor.runtime_health_cache import VehicleHistoryHealthSnapshotCache


@dataclass(frozen=True)
class RuntimeLoopHealthReporter:
    settings: RuntimeSettings
    logger: StructuredLogger
    state: RuntimeLoopHealthState
    vehicle_history: VehicleHistoryHealthSnapshotCache
    paths: RuntimePaths
    outbox_summary: Callable[[], Any] | None

    def write(self, *, status: str, iteration: int) -> None:
        state = self.state
        write_loop_health(
            self.settings,
            logger=self.logger,
            status=status,
            iteration=iteration,
            last_frame_at=state.last_frame_at,
            selected_decode_mode=state.selected_decode_mode,
            capture_last_success_at=state.capture_last_success_at,
            capture_selected_decode_mode=state.capture_selected_decode_mode,
            consecutive_capture_failures=state.consecutive_capture_failures,
            consecutive_detection_failures=state.consecutive_detection_failures,
            last_matrix_error=state.last_matrix_error,
            last_error=state.last_error,
            retention_failure_count=state.retention_failure_count,
            state_save_error=state.state_save_error,
            matrix_command_failure_count=state.matrix_command_failure_count,
            last_matrix_command_error=state.last_matrix_command_error,
            vehicle_history_failure_count=state.vehicle_history_failure_count,
            last_vehicle_history_error=state.last_vehicle_history_error,
            vehicle_history=self.vehicle_history.snapshot(
                force=state.last_vehicle_history_error is not None
                or state.vehicle_history_failure_count > 0
            ),
            matrix_outbox_file=self.paths.matrix_outbox_file,
            matrix_outbox_summary_provider=self.outbox_summary,
        )
