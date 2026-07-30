"""Bounded projection of Matrix sync payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from parking_spot_monitor.matrix_models import MatrixSyncResult, MatrixTextEvent
from parking_spot_monitor.matrix_support import MatrixError


def parse_sync_response(
    payload: Any,
    *,
    room_id: str,
    operation: str,
    status_code: int,
) -> MatrixSyncResult:
    """Project one Matrix sync payload into bounded safe text events."""

    if not isinstance(payload, dict):
        raise _malformed(
            operation,
            status_code,
            "next_batch",
            "Matrix sync response was malformed",
        )
    next_batch = payload.get("next_batch")
    if not isinstance(next_batch, str) or not next_batch:
        raise _malformed(
            operation,
            status_code,
            "next_batch",
            "Matrix sync response was missing a required field",
        )
    events_payload = (
        (((payload.get("rooms") or {}).get("join") or {}).get(room_id) or {})
        .get("timeline", {})
        .get("events", [])
    )
    if not isinstance(events_payload, list):
        raise _malformed(
            operation,
            status_code,
            "rooms.join.timeline.events",
            "Matrix sync response room timeline was malformed",
        )
    events: list[MatrixTextEvent] = []
    for item in events_payload:
        if not isinstance(item, Mapping) or item.get("type") != "m.room.message":
            continue
        content = item.get("content")
        if not isinstance(content, Mapping) or content.get("msgtype") != "m.text":
            continue
        body, event_id, sender = content.get("body"), item.get("event_id"), item.get("sender")
        if isinstance(body, str) and isinstance(event_id, str) and isinstance(sender, str):
            events.append(
                MatrixTextEvent(
                    event_id=event_id,
                    sender=sender,
                    room_id=room_id,
                    body=body[:512],
                )
            )
    return MatrixSyncResult(next_batch=next_batch, events=tuple(events))


def _malformed(operation: str, status_code: int, missing_key: str, message: str) -> MatrixError:
    return MatrixError(
        message,
        error_type="malformed_response",
        operation=operation,
        status_code=status_code,
        missing_key=missing_key,
    )
