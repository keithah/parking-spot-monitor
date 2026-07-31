"""Durable Matrix snapshot-alert outbox delivery executor.

The executor persists a snapshot alert intent before touching Matrix and then
phase-drains the durable record in Matrix's idempotent transaction order:
text, upload, image. Phase results are stored in the local outbox so process
restarts retry only missing work.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from pathlib import Path
import random
import threading
import time
from typing import Any

from parking_monitor.outbox import (
    AlertIntent,
    LocalOutbox,
    OutboxRecord,
    OutboxRetryPolicy,
    RetrySchedule,
    format_utc_timestamp,
)
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.matrix_alerts import (
    LIFECYCLE_EVENT_TYPES,
    OCCUPIED_SPOT_EVENT_TYPE,
    OPEN_SPOT_EVENT_TYPE,
    OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
    format_occupied_spot_alert,
    format_lifecycle_notice,
    format_open_spot_alert,
    format_owner_vehicle_quiet_window_alert,
    format_quiet_window_notice,
    occupied_spot_event_id,
    open_spot_event_id,
    owner_vehicle_quiet_window_event_id,
)
from parking_spot_monitor.matrix_delivery import MatrixDelivery
from parking_monitor.matrix_outbox_snapshots import (
    MatrixOutboxSnapshots,
    SNAPSHOT_ALERT_PHASES,
)
from parking_monitor.matrix_outbox_occupied_fallback import build_occupied_snapshot_fallback_intent
from parking_spot_monitor.matrix_support import MatrixError

_QUIET_WINDOW_EVENT_TYPES = frozenset({"quiet-window-upcoming", "quiet-window-started", "quiet-window-ended"})
_WORKER_JOIN_TIMEOUT_SECONDS = 1.0


@dataclass(frozen=True)
class MatrixOutboxDrainResult:
    """Safe summary of one outbox drain pass."""

    attempted_count: int
    delivered_count: int
    retrying_count: int


class MatrixOutboxDelivery:
    """Retry-aware Matrix open-alert delivery facade backed by ``LocalOutbox``."""

    def __init__(
        self,
        *,
        client: Any,
        room_id: str,
        data_dir: str | Path,
        snapshots_dir: str | Path | None,
        outbox: LocalOutbox,
        logger: StructuredLogger | None = None,
        snapshot_retention_count: int = 50,
        outbox_retry_max_seconds: float = 900,
        outbox_retry_jitter_ratio: float = 0.2,
        utc_now: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        random_unit: Callable[[], float] = random.random,
    ) -> None:
        self.client = client
        self.room_id = room_id
        self.data_dir = Path(data_dir)
        self.snapshots_dir = Path(snapshots_dir) if snapshots_dir is not None else None
        self.outbox = outbox
        self.logger = logger
        self.snapshot_retention_count = snapshot_retention_count
        self._utc_now = utc_now or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic
        self._random_unit = random_unit
        self._retry_max_seconds = float(outbox_retry_max_seconds)
        self._retry_jitter_ratio = float(outbox_retry_jitter_ratio)
        self._retry_policy = OutboxRetryPolicy(60, self._retry_max_seconds, self._retry_jitter_ratio)
        self._worker_lock = threading.Lock()
        self._drain_lock = threading.Lock()
        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._retry_interval_seconds = 60.0
        self._worker_last_attempt_at: str | None = None
        self._worker_last_error_type: str | None = None
        self._closing = False
        self._client_closed = False
        self._snapshot_artifacts = MatrixOutboxSnapshots(
            room_id=room_id,
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            outbox=outbox,
            logger=logger,
            retention_count=snapshot_retention_count,
        )
        self._immediate_delivery = MatrixDelivery(
            client=client,
            room_id=room_id,
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            logger=logger,
            snapshot_retention_count=snapshot_retention_count,
            protected_snapshots_provider=self._snapshot_artifacts.retryable_paths,
        )

    def close(self) -> None:
        with self._worker_lock:
            if self._closing:
                return
            self._closing = True
            self._stop_event.set()
            self._wake_event.set()
            worker = self._worker
        cancel_pending = getattr(self.client, "cancel_pending", None)
        if callable(cancel_pending):
            try:
                cancel_pending()
            except Exception as exc:
                self._log(
                    "warning",
                    "matrix-outbox-client-cancel-failed",
                    error_type=redact_diagnostic_text(exc.__class__.__name__) or "Exception",
                )
        if worker is not None and worker is not threading.current_thread() and worker.is_alive():
            worker.join(timeout=_WORKER_JOIN_TIMEOUT_SECONDS)
            if worker.is_alive():
                self._log("warning", "matrix-outbox-worker-cancel-timeout")
        with self._worker_lock:
            if self._client_closed:
                return
            self._client_closed = True
        close = getattr(self.client, "close", None)
        if callable(close):
            close()

    @property
    def worker_thread(self) -> threading.Thread | None:
        with self._worker_lock:
            return self._worker

    def start_worker(self, *, retry_interval_seconds: float) -> None:
        retry_interval = float(retry_interval_seconds)
        if not isfinite(retry_interval) or retry_interval <= 0:
            raise ValueError("retry_interval_seconds must be finite and positive")
        with self._worker_lock:
            if self._closing or self._client_closed:
                raise RuntimeError("cannot start Matrix outbox worker after close")
            if self._worker is not None and self._worker.is_alive():
                return
            self._retry_interval_seconds = retry_interval
            self._retry_policy = OutboxRetryPolicy(
                retry_interval,
                self._retry_max_seconds,
                self._retry_jitter_ratio,
            )
            self._stop_event.clear()
            self._worker = threading.Thread(
                target=self._worker_main,
                name="matrix-outbox-delivery",
                daemon=True,
            )
            self._worker.start()

    def _worker_main(self) -> None:
        while not self._stop_event.is_set():
            try:
                now = self._utc_now()
                record = self.outbox.next_due_record(now)
                if record is None:
                    due_at = self.outbox.next_retry_due_at()
                    timeout = None if due_at is None else max(0.0, (due_at - now).total_seconds())
                    self._wake_event.wait(timeout)
                    self._wake_event.clear()
                    continue
                self._record_worker_attempt()
                self.drain_outbox(record_id=record.id, max_records=1)
                self.outbox.compact_status_summary()
            except Exception as exc:
                self._record_worker_error(exc)
                self._log(
                    "warning",
                    "matrix-outbox-worker-pass-failed",
                    error_type=redact_diagnostic_text(exc.__class__.__name__) or "Exception",
                )
                self._pace_unexpected_worker_failure()
                continue

    def _pace_unexpected_worker_failure(self) -> None:
        deadline = self._monotonic() + self._retry_policy.initial_seconds
        while not self._stop_event.is_set():
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return
            if self._stop_event.wait(remaining):
                return

    def _record_worker_attempt(self) -> None:
        attempted_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        with self._worker_lock:
            self._worker_last_attempt_at = attempted_at

    def _record_worker_error(self, exc: BaseException) -> None:
        error_type = redact_diagnostic_text(exc.__class__.__name__) or "Exception"
        with self._worker_lock:
            self._worker_last_error_type = error_type

    def _stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def outbox_health_summary(self) -> Mapping[str, Any]:
        summary = dict(self.outbox.compact_status_summary())
        with self._worker_lock:
            worker = self._worker
            summary.update(
                {
                    "worker_running": worker is not None and worker.is_alive(),
                    "worker_last_attempt_at": self._worker_last_attempt_at,
                    "worker_last_error_type": self._worker_last_error_type,
                }
            )
        return summary

    def send_occupied_spot_alert(self, event: Mapping[str, Any]) -> OutboxRecord:
        """Persist an occupied alert without performing Matrix network I/O."""

        return self.enqueue_occupied_spot_alert(event)

    def send_quiet_window_notice(self, event: Mapping[str, Any]) -> str:
        """Send quiet-window notices through the immediate Matrix path."""

        return self._immediate_delivery.send_quiet_window_notice(event)

    def send_owner_vehicle_quiet_window_alert(self, event: Mapping[str, Any]) -> str:
        """Send owner-vehicle quiet-window alerts through the immediate Matrix path."""

        return self._immediate_delivery.send_owner_vehicle_quiet_window_alert(event)

    def send_lifecycle_notice(self, event: Mapping[str, Any]) -> str:
        """Send monitor lifecycle notices through the immediate Matrix path."""

        return self._immediate_delivery.send_lifecycle_notice(event)

    def send_open_spot_alert(self, event: Mapping[str, Any]) -> MatrixOutboxDrainResult:
        """Persist an open alert before Matrix I/O, then drain retryable work."""

        record = self.enqueue_open_spot_alert(event)
        return self.drain_outbox(record_id=record.id)

    def enqueue_open_spot_alert(self, event: Mapping[str, Any]) -> OutboxRecord:
        """Persist a text-plus-image open-alert record without performing network I/O."""

        event_id = open_spot_event_id(event)
        metadata = _open_alert_metadata(event)
        body = format_open_spot_alert(event)
        return self._enqueue_snapshot_alert(
            event=event,
            event_id=event_id,
            body=body,
            metadata=metadata,
            snapshot_source_path=str(metadata.get("snapshot_path", "")),
            snapshot_event_type=OPEN_SPOT_EVENT_TYPE,
        )

    def enqueue_occupied_spot_alert(self, event: Mapping[str, Any]) -> OutboxRecord:
        """Persist a text-plus-image occupied-alert record without performing network I/O."""

        event_id = occupied_spot_event_id(event)
        metadata = _occupied_alert_metadata(event)
        body = format_occupied_spot_alert(event)
        try:
            return self._enqueue_snapshot_alert(
                event=event,
                event_id=event_id,
                body=body,
                metadata=metadata,
                snapshot_source_path=str(metadata.get("occupied_snapshot_path", "")),
                snapshot_event_type=OCCUPIED_SPOT_EVENT_TYPE,
            )
        except MatrixError as exc:
            fallback_intent = build_occupied_snapshot_fallback_intent(
                error_type=exc.diagnostics.get("error_type"),
                event=event,
                event_id=event_id,
                room_id=self.room_id,
                body=body,
            )
            if fallback_intent is None:
                raise
            existing = self.outbox.find_event_record(event_id)
            if existing is not None:
                self._wake_event.set()
                return existing
            record = self.outbox.enqueue_with_phases(fallback_intent, ("text",))
            reason = fallback_intent.metadata["snapshot_degraded_reason"]
            self._log(
                "warning", "matrix-outbox-occupied-snapshot-degraded", item_id=record.id, event_id=event_id, snapshot_degraded_reason=reason
            )
            self._log("info", "matrix-outbox-enqueued", item_id=record.id, event_id=event_id, phase="text")
            self._wake_event.set()
            return record

    def enqueue_text_notice(self, event_name: str, event: Mapping[str, Any]) -> OutboxRecord:
        """Persist a text-only frame notice without performing Matrix network I/O."""

        if event_name == OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE:
            event_id = str(event.get("event_id") or owner_vehicle_quiet_window_event_id(event))
            body = format_owner_vehicle_quiet_window_alert(event)
        elif event_name in _QUIET_WINDOW_EVENT_TYPES:
            event_id = str(event.get("event_id", ""))
            body = format_quiet_window_notice(event)
        elif event_name in LIFECYCLE_EVENT_TYPES:
            event_id = str(event.get("event_id", ""))
            body = format_lifecycle_notice(event)
        else:
            raise MatrixError(
                "Matrix text-only outbox event type is unsupported",
                error_type="unsupported_outbox_text_event",
                event_type=event_name,
            )
        intent = AlertIntent(
            event_id=event_id,
            phase="text",
            room_id=self.room_id,
            body=body,
            metadata={"event_type": event_name},
        )
        record = self.outbox.enqueue_with_phases(intent, ("text",))
        self._log("info", "matrix-outbox-enqueued", item_id=record.id, event_id=event_id, phase="text")
        self._wake_event.set()
        return record

    def enqueue_lifecycle_notice(self, event: Mapping[str, Any]) -> OutboxRecord:
        """Persist a monitor lifecycle notice before any Matrix network I/O."""

        return self.enqueue_text_notice(str(event.get("event_type", "")), event)

    def _enqueue_snapshot_alert(
        self,
        *,
        event: Mapping[str, Any],
        event_id: str,
        body: str,
        metadata: dict[str, Any],
        snapshot_source_path: str,
        snapshot_event_type: str,
    ) -> OutboxRecord:
        record = self._snapshot_artifacts.enqueue(
            event=event,
            event_id=event_id,
            body=body,
            metadata=metadata,
            source_path=snapshot_source_path,
            event_type=snapshot_event_type,
        )
        self._log("info", "matrix-outbox-enqueued", item_id=record.id, event_id=event_id, phase=SNAPSHOT_ALERT_PHASES[0])
        self._wake_event.set()
        return record

    def drain_outbox(self, *, record_id: str | None = None, max_records: int | None = None) -> MatrixOutboxDrainResult:
        """Drain pending/retrying outbox records, skipping already delivered phases."""

        with self._drain_lock:
            return self._drain_outbox_locked(record_id=record_id, max_records=max_records)

    def _drain_outbox_locked(
        self,
        *,
        record_id: str | None,
        max_records: int | None,
    ) -> MatrixOutboxDrainResult:

        records = self.outbox.due_records(
            self._utc_now(),
            record_id=record_id,
            max_records=max_records,
        )
        self._log(
            "info" if records else "debug",
            "matrix-outbox-drain-started",
            attempted_count=len(records),
            item_id=record_id,
        )
        attempted = 0
        delivered = 0
        retrying = 0
        for record in records:
            if self._stop_requested():
                break
            attempted += 1
            drained = self._drain_record(record)
            if drained.state == "delivered":
                delivered += 1
            elif drained.state in {"pending", "retrying"}:
                retrying += 1
        self._log(
            "info" if any((attempted, delivered, retrying)) else "debug",
            "matrix-outbox-drain-finished",
            attempted_count=attempted,
            delivered_count=delivered,
            retrying_count=retrying,
            item_id=record_id,
        )
        return MatrixOutboxDrainResult(attempted_count=attempted, delivered_count=delivered, retrying_count=retrying)

    def _drain_record(self, record: OutboxRecord) -> OutboxRecord:
        current = record
        for phase in _record_delivery_phases(current):
            if self._stop_requested():
                return current
            if current.phase_states.get(phase) == "delivered":
                self._log("info", "matrix-outbox-phase-skip", item_id=current.id, phase=phase, reason="already_delivered")
                continue
            try:
                self._log(
                    "info",
                    "matrix-outbox-phase-attempt",
                    item_id=current.id,
                    phase=phase,
                    transaction_id=_transaction_id(current, phase),
                )
                if phase == "text":
                    current = self._send_text_phase(current)
                elif phase == "upload":
                    current = self._upload_phase(current)
                elif phase == "image":
                    current = self._send_image_phase(current)
                self._log(
                    "info",
                    "matrix-outbox-phase-succeeded",
                    item_id=current.id,
                    phase=phase,
                    transaction_id=_transaction_id(current, phase),
                )
            except Exception as exc:
                if threading.current_thread() is self.worker_thread:
                    self._record_worker_error(exc)
                classification = _classify_delivery_failure(exc, phase=phase)
                if classification.retryable:
                    current = self.outbox.apply_phase_result(
                        current.id,
                        phase,
                        retry=self._retry_schedule(current, reason=classification.reason),
                    )
                    self._log(
                        "warning",
                        "matrix-outbox-phase-retryable-failure",
                        item_id=current.id,
                        phase=phase,
                        transaction_id=_transaction_id(current, phase),
                        reason=classification.reason,
                        error_type=exc.__class__.__name__,
                    )
                else:
                    current = self.outbox.mark_phase_failed(current.id, phase, reason=classification.reason)
                    self._log(
                        "warning",
                        "matrix-outbox-phase-dead-lettered",
                        item_id=current.id,
                        phase=phase,
                        transaction_id=_transaction_id(current, phase),
                        reason=classification.reason,
                        error_type=exc.__class__.__name__,
                    )
                return current
        if current.state == "delivered":
            self._log("info", "matrix-outbox-record-delivered", item_id=current.id, event_id=current.intent.event_id)
        return current

    def _retry_schedule(self, record: OutboxRecord, *, reason: str) -> RetrySchedule:
        attempt_count = record.retry_attempt_count + 1
        delay = self._retry_policy.delay_seconds(
            attempt_count,
            random_unit=self._random_unit(),
        )
        due_at = format_utc_timestamp(self._utc_now() + timedelta(seconds=delay))
        return RetrySchedule(due_at=due_at, attempt_count=attempt_count, reason=reason)

    def _send_text_phase(self, record: OutboxRecord) -> OutboxRecord:
        event_id = self.client.send_text(
            room_id=record.intent.room_id or self.room_id,
            txn_id=_transaction_id(record, "text"),
            body=record.intent.body,
        )
        return self.outbox.mark_phase_delivered(record.id, "text", result={"matrix_event_id": event_id})

    def _upload_phase(self, record: OutboxRecord) -> OutboxRecord:
        prepared = self._snapshot_artifacts.prepare_upload(record)
        self._log(
            "info",
            "matrix-outbox-snapshot-prepared",
            item_id=record.id,
            phase="upload",
            snapshot_path=prepared.snapshot_path,
            byte_size=prepared.info["size"],
        )
        content_uri = self.client.upload_image(
            filename=prepared.filename,
            data=prepared.data,
            content_type="image/jpeg",
        )
        return self.outbox.mark_phase_delivered(
            record.id,
            "upload",
            result={
                "content_uri": content_uri,
                "filename": prepared.filename,
                "body": prepared.body,
                "info": prepared.info,
            },
        )

    def _send_image_phase(self, record: OutboxRecord) -> OutboxRecord:
        upload_result = record.phase_results.get("upload")
        if not isinstance(upload_result, Mapping):
            raise MatrixError("Matrix upload phase result missing", error_type="upload_result_missing")
        content_uri = upload_result.get("content_uri")
        body = upload_result.get("body")
        info = upload_result.get("info")
        if not isinstance(content_uri, str) or not isinstance(body, str) or not isinstance(info, Mapping):
            raise MatrixError("Matrix upload phase result was malformed", error_type="upload_result_malformed")
        event_id = self.client.send_image(
            room_id=record.intent.room_id or self.room_id,
            txn_id=_transaction_id(record, "image"),
            body=body,
            content_uri=content_uri,
            info=dict(info),
        )
        return self.outbox.mark_phase_delivered(record.id, "image", result={"matrix_event_id": event_id})

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            getattr(self.logger, level)(event, **fields)
        except (OSError, ValueError):
            # Shutdown may close an injected stream while a bounded worker join
            # is still unwinding a network call. Logging must not kill delivery.
            return



@dataclass(frozen=True)
class _FailureClassification:
    retryable: bool
    reason: str


def _classify_delivery_failure(exc: Exception, *, phase: str) -> _FailureClassification:
    reason = _delivery_failure_reason(exc, phase=phase)
    return _FailureClassification(retryable=_is_retryable_delivery_failure(exc), reason=reason)


def _is_retryable_delivery_failure(exc: Exception) -> bool:
    if not isinstance(exc, MatrixError):
        return True
    error_type = exc.diagnostics.get("error_type")
    if error_type in {"timeout", "request_error", "malformed_response"}:
        return True
    if error_type == "http_status":
        status_code = exc.diagnostics.get("status_code")
        return status_code in {429, 500, 502, 503, 504}
    if error_type in {
        "upload_result_missing",
        "upload_result_malformed",
        "snapshot_missing_source",
        "snapshot_invalid_source",
        "snapshot_copy_failed",
        "snapshot_metadata_failed",
        "snapshot_resize_failed",
    }:
        return False
    return True


def _delivery_failure_reason(exc: Exception, *, phase: str) -> str:
    if isinstance(exc, MatrixError):
        error_type = exc.diagnostics.get("error_type")
        if error_type == "http_status":
            status_code = exc.diagnostics.get("status_code")
            if isinstance(status_code, int):
                return f"matrix_{phase}_http_{status_code}"
        if isinstance(error_type, str) and error_type:
            return f"matrix_{phase}_{redact_diagnostic_text(error_type)}"
    return f"matrix_{phase}_{redact_diagnostic_text(exc.__class__.__name__).lower() or 'error'}"


def _transaction_id(record: OutboxRecord, phase: str) -> str:
    if phase == "text" and tuple(record.phase_states) == ("text",):
        return record.intent.event_id
    return f"{record.intent.event_id}:{phase}"


def _record_delivery_phases(record: OutboxRecord) -> tuple[str, ...]:
    return tuple(phase for phase in SNAPSHOT_ALERT_PHASES if phase in record.phase_states)


def _open_alert_metadata(event: Mapping[str, Any]) -> dict[str, str]:
    return {
        "event_type": OPEN_SPOT_EVENT_TYPE,
        "spot_id": redact_diagnostic_text(event.get("spot_id", "")),
        "observed_at": _safe_observed_at(event.get("observed_at")),
        "snapshot_path": redact_diagnostic_text(event.get("snapshot_path", "")),
    }


def _occupied_alert_metadata(event: Mapping[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "event_type": OCCUPIED_SPOT_EVENT_TYPE,
        "spot_id": redact_diagnostic_text(event.get("spot_id", "")),
        "observed_at": _safe_observed_at(event.get("observed_at")),
        "occupied_snapshot_path": redact_diagnostic_text(event.get("occupied_snapshot_path", "")),
    }
    for key in ("session_id", "profile_id", "source_timestamp"):
        value = event.get(key)
        if isinstance(value, (str, int, float, bool)) or value is None:
            metadata[key] = value
    return metadata


def _safe_observed_at(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return value.isoformat()
    return redact_diagnostic_text(value)
