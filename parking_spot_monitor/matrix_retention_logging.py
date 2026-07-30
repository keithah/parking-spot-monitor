"""Structured telemetry for Matrix snapshot retention."""

from __future__ import annotations

from pathlib import Path

from parking_spot_monitor.logging import StructuredLogger


def log_retention_pruned(
    logger: StructuredLogger | None,
    *,
    event: str,
    root: Path,
    trigger: str,
    pruned_count: int,
    pruned_bytes: int,
    retained_count: int,
) -> None:
    if logger is not None:
        logger.info(
            event,
            root=str(root),
            trigger=trigger,
            pruned_count=pruned_count,
            pruned_bytes=pruned_bytes,
            retained_count=retained_count,
        )


def log_retention_failure(
    logger: StructuredLogger | None,
    *,
    event: str,
    root: Path,
    trigger: str,
    error_type: str,
    message: str,
    failed_count: int = 1,
    pruned_count: int = 0,
    pruned_bytes: int = 0,
) -> None:
    if logger is not None:
        logger.warning(
            event,
            root=str(root),
            trigger=trigger,
            error_type=error_type,
            message=message,
            failed_count=failed_count,
            pruned_count=pruned_count,
            pruned_bytes=pruned_bytes,
        )
