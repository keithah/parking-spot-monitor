from __future__ import annotations

from pathlib import Path

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import decision_memory_path, format_recent_reply, format_why_reply


def format_operator_why_reply(
    *,
    data_dir: str | Path,
    spot_id: str,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, redacted decision-memory explanation for one spot."""

    return format_why_reply(decision_memory_path(data_dir), spot_id, logger=logger)


def format_operator_recent_reply(
    *,
    data_dir: str | Path,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded, redacted recent decision-memory timeline."""

    return format_recent_reply(decision_memory_path(data_dir), logger=logger)
