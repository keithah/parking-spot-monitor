"""Startup protection for snapshots referenced by durable Matrix retries."""

from __future__ import annotations

from pathlib import Path

from parking_monitor.outbox import LocalOutbox, OutboxRecoveryError
from parking_spot_monitor.logging import StructuredLogger


def startup_retryable_retained_snapshots(
    outbox_path: Path,
    *,
    logger: StructuredLogger,
) -> tuple[Path, ...] | None:
    try:
        outbox = LocalOutbox(outbox_path)
        if outbox.recovery.quarantined_count or outbox.recovery.events:
            raise OutboxRecoveryError("local outbox recovery was not clean")
        records = outbox.list_records()
    except Exception as exc:
        logger.warning(
            "startup-outbox-snapshot-protection-failed",
            phase="startup-retention",
            action="load-outbox",
            error_type=type(exc).__name__,
        )
        return None
    protected: list[Path] = []
    for record in records:
        if record.state not in {"pending", "retrying"}:
            continue
        retained = record.intent.metadata.get("retained_snapshot_path")
        if isinstance(retained, str) and retained.strip():
            protected.append(Path(retained))
    return tuple(protected)
