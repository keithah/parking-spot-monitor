"""Sanitized durable text fallback construction for degraded occupied snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from parking_monitor.outbox import AlertIntent
from parking_spot_monitor.logging import redact_diagnostic_text
from parking_spot_monitor.matrix_alerts import OCCUPIED_SPOT_EVENT_TYPE

_RECOGNIZED_REASONS = frozenset(
    {
        "snapshot_invalid_source",
        "snapshot_missing_source",
        "snapshot_copy_failed",
        "snapshot_metadata_failed",
        "snapshot_resize_failed",
    }
)


def build_occupied_snapshot_fallback_intent(
    *,
    error_type: object,
    event: Mapping[str, Any],
    event_id: str,
    room_id: str,
    body: str,
) -> AlertIntent | None:
    """Build a text-only intent only for recognized snapshot preparation failures."""

    if not isinstance(error_type, str) or error_type not in _RECOGNIZED_REASONS:
        return None
    return AlertIntent(
        event_id=event_id,
        phase="text",
        room_id=room_id,
        body=body,
        metadata={
            "event_type": OCCUPIED_SPOT_EVENT_TYPE,
            "spot_id": redact_diagnostic_text(event.get("spot_id", "")),
            "observed_at": _safe_observed_at(event.get("observed_at")),
            "snapshot_degraded_reason": error_type,
        },
    )


def _safe_observed_at(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return value.isoformat()
    return redact_diagnostic_text(value)
