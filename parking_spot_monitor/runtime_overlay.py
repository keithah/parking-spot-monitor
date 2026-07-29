from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger


def _write_debug_overlay(
    settings: RuntimeSettings,
    source_path: str | Path,
    output_path: str | Path,
    *,
    logger: StructuredLogger,
) -> Any:
    from parking_spot_monitor.debug_overlay import write_debug_overlay

    return write_debug_overlay(settings, source_path, output_path, logger=logger)


def write_overlay_for_capture(
    settings: RuntimeSettings,
    latest_path: Path,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    overlay: Callable[..., Any],
) -> bool:
    output_path = data_dir / "debug_latest.jpg"
    try:
        overlay(settings, latest_path, output_path, logger=logger)
    except Exception as exc:
        if not _is_expected_debug_overlay_error(exc):
            logger.error(
                "debug-overlay-failed",
                source_path=str(latest_path),
                output_path=str(output_path),
                spot_ids=["left_spot", "right_spot"],
                width=None,
                height=None,
                error_type=type(exc).__name__,
                error_message="debug overlay failed unexpectedly",
            )
        return False
    return True


def _is_expected_debug_overlay_error(exc: Exception) -> bool:
    return type(exc).__name__ == "DebugOverlayError" and hasattr(exc, "diagnostics")
