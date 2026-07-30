"""Matrix event dispatch helpers for runtime alert delivery."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Protocol

from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_alerts import (
    LIFECYCLE_EVENT_TYPES,
    OCCUPIED_SPOT_EVENT_TYPE,
    OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
    occupied_spot_event_id,
    open_spot_event_id,
    owner_vehicle_quiet_window_event_id,
)
from parking_spot_monitor.matrix_support import MatrixError
from parking_spot_monitor.occupancy import OccupancyEventType
from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, make_decision_memory_record
from parking_spot_monitor.scheduler import QuietWindowEventType


class RuntimeMatrixDelivery(Protocol):
    """Matrix operations consumed by runtime event dispatch."""

    def send_lifecycle_notice(self, event: Mapping[str, Any]) -> object: ...

    def enqueue_lifecycle_notice(self, event: Mapping[str, Any]) -> object: ...

    def enqueue_text_notice(self, event_name: str, event: Mapping[str, Any]) -> object: ...

    def enqueue_open_spot_alert(self, event: Mapping[str, Any]) -> object: ...

    def enqueue_occupied_spot_alert(self, event: Mapping[str, Any]) -> object: ...

    def outbox_health_summary(self) -> Mapping[str, Any]: ...


def append_matrix_event_memory(
    path: Path | DecisionMemoryStore | None,
    *,
    event_name: str,
    event: Mapping[str, Any],
    outcome: str,
    logger: StructuredLogger,
    error_type: str | None = None,
    reason: str | None = None,
) -> None:
    if path is None:
        return
    spot_id = event.get("spot_id") if isinstance(event.get("spot_id"), str) else None
    record = make_decision_memory_record(
        "alert" if event_name != "matrix-command" else "command_outcome",
        observed_at=event.get("observed_at") if isinstance(event.get("observed_at"), str) else None,
        spot_id=spot_id,
        summary=f"{event_name} {outcome}",
        details={
            "event_type": event_name,
            "event_id": event.get("event_id"),
            "outcome": outcome,
            "reason": reason,
            "error_type": error_type,
            "suppressed_reason": event.get("suppressed_reason"),
            "snapshot_path": event.get("retained_snapshot_path") or event.get("snapshot_path") or event.get("occupied_snapshot_path"),
            "retained_snapshot_path": event.get("retained_snapshot_path"),
        },
    )
    if isinstance(path, DecisionMemoryStore):
        path.append(record, durability="immediate")
    else:
        append_decision_memory_record(path, record, logger=logger)


def dispatch_matrix_event(
    matrix_delivery: RuntimeMatrixDelivery | None,
    event_name: str,
    event: Mapping[str, Any],
    *,
    logger: StructuredLogger,
    decision_memory_path: Path | None = None,
    decision_memory_store: DecisionMemoryStore | None = None,
) -> dict[str, Any] | None:
    memory_target = decision_memory_store if decision_memory_store is not None else decision_memory_path
    if matrix_delivery is None:
        logger.info("matrix-delivery-skipped", event_type=event_name, reason="not-configured")
        append_matrix_event_memory(
            memory_target,
            event_name=event_name,
            event=event,
            outcome="skipped",
            reason="not-configured",
            logger=logger,
        )
        return None

    if event_name in LIFECYCLE_EVENT_TYPES:
        txn_id = str(event.get("event_id", ""))
        logger.info(event_name, **{key: value for key, value in event.items() if key != "event_type"})
        return _attempt_matrix_operation(
            event_name=event_name,
            event=event,
            txn_id=txn_id,
            send=lambda: matrix_delivery.enqueue_lifecycle_notice(event),
            logger=logger,
            decision_memory_path=memory_target,
            attempt_log_fields={"delivery_mode": "outbox_enqueue"},
            success_log_fields={"delivery_mode": "outbox_enqueue"},
            success_memory_outcome="queued",
            success_memory_reason="outbox_enqueue",
        )

    if event_name == OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE:
        txn_id = str(event.get("event_id") or owner_vehicle_quiet_window_event_id(event))
        return _attempt_matrix_operation(
            event_name=event_name,
            event=event,
            txn_id=txn_id,
            send=lambda: matrix_delivery.enqueue_text_notice(event_name, event),
            logger=logger,
            decision_memory_path=memory_target,
            attempt_log_fields={"delivery_mode": "outbox_enqueue"},
            success_log_fields={"delivery_mode": "outbox_enqueue"},
            success_memory_outcome="queued",
            success_memory_reason="outbox_enqueue",
        )

    if event_name in {QuietWindowEventType.UPCOMING.value, QuietWindowEventType.STARTED.value, QuietWindowEventType.ENDED.value}:
        txn_id = str(event.get("event_id", ""))
        return _attempt_matrix_operation(
            event_name=event_name,
            event=event,
            txn_id=txn_id,
            send=lambda: matrix_delivery.enqueue_text_notice(event_name, event),
            logger=logger,
            decision_memory_path=memory_target,
            attempt_log_fields={"delivery_mode": "outbox_enqueue"},
            success_log_fields={"delivery_mode": "outbox_enqueue"},
            success_memory_outcome="queued",
            success_memory_reason="outbox_enqueue",
        )

    if event_name == OCCUPIED_SPOT_EVENT_TYPE:
        txn_id = occupied_spot_event_id(event)
        alert_fields = _occupied_alert_log_fields(event, txn_id=txn_id)
        return _attempt_matrix_operation(
            event_name=event_name,
            event=event,
            txn_id=txn_id,
            send=lambda: matrix_delivery.enqueue_occupied_spot_alert(event),
            logger=logger,
            decision_memory_path=memory_target,
            attempt_log_fields=alert_fields | {"delivery_mode": "outbox_enqueue"},
            success_log_fields=alert_fields | {"delivery_mode": "outbox_enqueue"},
            success_memory_outcome="queued",
            success_memory_reason="outbox_enqueue",
            process_send_result=lambda result: (_event_with_retained_snapshot_path(event, result), None, "error"),
        )

    if event_name == OccupancyEventType.OPEN_EVENT.value:
        txn_id = open_spot_event_id(event)
        alert_fields = _open_alert_log_fields(event, txn_id=txn_id)
        return _attempt_matrix_operation(
            event_name=event_name,
            event=event,
            txn_id=txn_id,
            send=lambda: matrix_delivery.enqueue_open_spot_alert(event),
            logger=logger,
            decision_memory_path=memory_target,
            attempt_log_fields=alert_fields | {"delivery_mode": "outbox_enqueue"},
            success_log_fields=alert_fields | {"delivery_mode": "outbox_enqueue"},
            success_memory_outcome="queued",
            success_memory_reason="outbox_enqueue",
            process_send_result=lambda result: (_event_with_retained_snapshot_path(event, result), None, "error"),
        )

    reason = "suppressed" if event_name == OccupancyEventType.OPEN_SUPPRESSED.value else "unsupported-event-type"
    extra_fields: dict[str, Any] = {}
    if event_name == OccupancyEventType.STATE_CHANGED.value:
        reason = "state-change-not-alert"
        extra_fields = {
            "matrix_dispatch_policy": "open-events-only",
            "next_expected_event": OccupancyEventType.OPEN_EVENT.value,
        }
    logger.info(
        "matrix-delivery-skipped",
        event_type=event_name,
        spot_id=event.get("spot_id"),
        event_id=event.get("event_id"),
        reason=reason,
        **extra_fields,
    )
    append_matrix_event_memory(
        memory_target,
        event_name=event_name,
        event=event,
        outcome="skipped",
        reason=reason,
        logger=logger,
    )
    return None


def _event_mapping_field(source: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = source.get(name)
    return value if isinstance(value, Mapping) else {}


def _occupied_alert_log_fields(event: Mapping[str, Any], *, txn_id: str) -> dict[str, Any]:
    return {
        "spot_id": event.get("spot_id"),
        "event_id": event.get("event_id"),
        "txn_id": txn_id,
        "session_id": event.get("session_id"),
        "profile_id": event.get("profile_id"),
        "estimate_status": _event_mapping_field(event, "vehicle_history_estimate").get("status"),
        "occupied_snapshot_path": event.get("occupied_snapshot_path"),
    }


def _open_alert_log_fields(event: Mapping[str, Any], *, txn_id: str) -> dict[str, Any]:
    return {
        "spot_id": event.get("spot_id"),
        "txn_id": txn_id,
        "snapshot_path": event.get("snapshot_path"),
    }


def _attempt_matrix_operation(
    *,
    event_name: str,
    event: Mapping[str, Any],
    txn_id: str,
    send: Callable[[], Any],
    logger: StructuredLogger,
    decision_memory_path: Path | DecisionMemoryStore | None,
    attempt_log_fields: Mapping[str, Any] | None = None,
    success_log_fields: Mapping[str, Any] | None = None,
    sent_event: Mapping[str, Any] | None = None,
    success_memory_outcome: str = "sent",
    success_memory_reason: str | None = None,
    process_send_result: Callable[[Any], tuple[Mapping[str, Any], dict[str, Any] | None, str]] | None = None,
) -> dict[str, Any] | None:
    attempt_fields: dict[str, Any] = {
        "event_type": event_name,
        "event_id": txn_id,
        "txn_id": txn_id,
        "attempt": 1,
    }
    if attempt_log_fields is not None:
        attempt_fields.update(attempt_log_fields)
    logger.info("matrix-delivery-attempt", **attempt_fields)
    try:
        send_result = send()
    except Exception as exc:
        context = _log_matrix_delivery_failed(logger, event_name=event_name, event=event, txn_id=txn_id, error=exc)
        append_matrix_event_memory(
            decision_memory_path,
            event_name=event_name,
            event=event,
            outcome="failed",
            error_type=context.get("error_type"),
            logger=logger,
        )
        return context

    resolved_sent_event = sent_event if sent_event is not None else event
    if process_send_result is not None:
        resolved_sent_event, error_context, failure_log_level = process_send_result(send_result)
        if error_context is not None:
            if failure_log_level == "warning":
                logger.warning("matrix-delivery-failed", **error_context)
            else:
                logger.error("matrix-delivery-failed", **error_context)
            append_matrix_event_memory(
                decision_memory_path,
                event_name=event_name,
                event=event,
                outcome="failed",
                error_type=error_context.get("error_type"),
                logger=logger,
            )
            return error_context

    success_fields: dict[str, Any] = {
        "event_type": event_name,
        "event_id": txn_id,
        "txn_id": txn_id,
        "attempt": 1,
    }
    if success_log_fields is not None:
        success_fields.update(success_log_fields)
    logger.info("matrix-delivery-succeeded", **success_fields)
    append_matrix_event_memory(
        decision_memory_path,
        event_name=event_name,
        event=resolved_sent_event,
        outcome=success_memory_outcome,
        reason=success_memory_reason,
        logger=logger,
    )
    return None


def _log_matrix_delivery_failed(
    logger: StructuredLogger,
    *,
    event_name: str,
    event: Mapping[str, Any],
    txn_id: str,
    error: BaseException,
) -> dict[str, Any]:
    diagnostics = dict(error.diagnostics) if isinstance(error, MatrixError) else {"error_type": type(error).__name__}
    attempt = diagnostics.pop("attempt", 1)
    context = {
        "phase": "matrix",
        "event_type": event_name,
        "event_id": event.get("event_id"),
        "spot_id": event.get("spot_id"),
        "txn_id": txn_id,
        "snapshot_path": event.get("snapshot_path") or event.get("occupied_snapshot_path"),
        "session_id": event.get("session_id"),
        "profile_id": event.get("profile_id"),
        "attempt": attempt,
        "final": True,
        **diagnostics,
    }
    logger.error("matrix-delivery-failed", **context)
    return context


def _event_with_retained_snapshot_path(event: Mapping[str, Any], snapshot: Any | None) -> Mapping[str, Any]:
    retained_path = _safe_retained_snapshot_memory_path(snapshot)
    if retained_path is None:
        return event
    enriched = dict(event)
    enriched["retained_snapshot_path"] = retained_path
    return enriched


def _safe_retained_snapshot_memory_path(snapshot: Any | None) -> str | None:
    snapshot_path = getattr(snapshot, "path", None)
    if snapshot_path is None:
        intent = getattr(snapshot, "intent", None)
        metadata = getattr(intent, "metadata", None)
        if isinstance(metadata, Mapping):
            snapshot_path = metadata.get("retained_snapshot_path")
    if snapshot_path is None:
        return None
    path = Path(snapshot_path)
    if path.is_absolute():
        parts = path.parts
        for marker in ("snapshots", "matrix-snapshots"):
            if marker in parts:
                index = parts.index(marker)
                relative = Path(*parts[index:])
                return relative.as_posix()
        if not path.name:
            return None
        return path.name
    if ".." in path.parts:
        return None
    return path.as_posix()
