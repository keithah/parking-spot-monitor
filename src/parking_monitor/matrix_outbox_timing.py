from __future__ import annotations

from datetime import datetime
from math import isfinite

from parking_monitor.outbox_models import OutboxRecord, parse_utc_timestamp


def delivery_latency_fields(record: OutboxRecord) -> dict[str, float]:
    fields: dict[str, float] = {}
    observed_at = parse_utc_timestamp(record.intent.metadata.get("observed_at", ""))
    created_at = parse_utc_timestamp(record.created_at)
    delivered_at = parse_utc_timestamp(record.updated_at)
    observation_to_enqueue = _elapsed_seconds(observed_at, created_at)
    if observation_to_enqueue is not None:
        fields["observation_to_enqueue_seconds"] = observation_to_enqueue
    enqueue_to_delivery = _elapsed_seconds(created_at, delivered_at)
    if enqueue_to_delivery is not None:
        fields["enqueue_to_delivery_seconds"] = enqueue_to_delivery
    return fields


def _elapsed_seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    elapsed = (end - start).total_seconds()
    if not isfinite(elapsed) or elapsed < 0:
        return None
    return round(elapsed, 6)
