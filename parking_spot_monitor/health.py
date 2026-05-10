from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_value

HealthStatusValue = Literal["starting", "ok", "degraded", "down"]


@dataclass(frozen=True)
class HealthStatus:
    """Compact operator health surface persisted under the effective data directory."""

    status: HealthStatusValue
    updated_at: str
    iteration: int
    last_frame_at: str | None = None
    selected_decode_mode: str | None = None
    consecutive_capture_failures: int = 0
    consecutive_detection_failures: int = 0
    last_matrix_error: Mapping[str, Any] | None = None
    last_error: Mapping[str, Any] | None = None
    retention_failure_count: int = 0
    state_save_error: Mapping[str, Any] | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return redact_diagnostic_value(
            {
                "status": self.status,
                "updated_at": self.updated_at,
                "iteration": self.iteration,
                "last_frame_at": self.last_frame_at,
                "selected_decode_mode": self.selected_decode_mode,
                "consecutive_capture_failures": self.consecutive_capture_failures,
                "consecutive_detection_failures": self.consecutive_detection_failures,
                "last_matrix_error": dict(self.last_matrix_error) if self.last_matrix_error is not None else None,
                "last_error": dict(self.last_error) if self.last_error is not None else None,
                "retention_failure_count": self.retention_failure_count,
                "state_save_error": dict(self.state_save_error) if self.state_save_error is not None else None,
            }
        )


def write_health_status(path: str | os.PathLike[str], status: HealthStatus, logger: StructuredLogger | None = None) -> None:
    """Atomically write compact health JSON without logging per-iteration noise."""

    health_path = Path(path)
    health_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            delete=False,
            dir=health_path.parent,
            prefix=f".{health_path.name}.",
            suffix=".tmp",
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(status.to_json_dict(), handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, health_path)
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise
