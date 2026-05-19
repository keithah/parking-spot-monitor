"""Durable Matrix open-alert outbox delivery executor.

The executor persists an open-spot alert intent before touching Matrix and then
phase-drains the durable record in Matrix's idempotent transaction order:
text, upload, image. Phase results are stored in the local outbox so process
restarts retry only missing work.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_monitor.outbox import AlertIntent, LocalOutbox, OutboxRecord
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.matrix import (
    JPEG_MIMETYPE,
    OPEN_SPOT_EVENT_TYPE,
    MatrixError,
    MatrixSnapshot,
    _matrix_snapshot_upload,
    format_open_spot_alert,
    open_spot_event_id,
    prepare_event_snapshot,
)

_OPEN_ALERT_PHASES = ("text", "upload", "image")


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
    ) -> None:
        self.client = client
        self.room_id = room_id
        self.data_dir = Path(data_dir)
        self.snapshots_dir = Path(snapshots_dir) if snapshots_dir is not None else None
        self.outbox = outbox
        self.logger = logger
        self.snapshot_retention_count = snapshot_retention_count

    def send_open_spot_alert(self, event: Mapping[str, Any]) -> MatrixOutboxDrainResult:
        """Persist an open alert before Matrix I/O, then drain retryable work."""

        record = self.enqueue_open_spot_alert(event)
        return self.drain_outbox(record_id=record.id)

    def enqueue_open_spot_alert(self, event: Mapping[str, Any]) -> OutboxRecord:
        """Persist a three-phase open-alert record without performing network I/O."""

        event_id = open_spot_event_id(event)
        body = format_open_spot_alert(event)
        metadata = _open_alert_metadata(event)
        snapshot = self._prepare_retained_snapshot(event=event, event_id=event_id, source_path=str(metadata.get("snapshot_path", "")))
        metadata.update(
            {
                "retained_snapshot_path": str(snapshot.path),
                "retained_snapshot_filename": snapshot.filename,
            }
        )
        intent = AlertIntent(
            event_id=event_id,
            phase="text",
            room_id=self.room_id,
            body=body,
            metadata=metadata,
        )
        record = self.outbox.enqueue(intent)
        self._log("info", "matrix-outbox-enqueued", item_id=record.id, event_id=event_id, phase="text")
        # Predeclare all phases before delivery so text success alone cannot make
        # the record terminal-delivered.
        for phase in ("upload", "image"):
            record = self.outbox.ensure_phase_pending(record.id, phase)
        return record

    def drain_outbox(self, *, record_id: str | None = None) -> MatrixOutboxDrainResult:
        """Drain pending/retrying outbox records, skipping already delivered phases."""

        records = [
            record
            for record in self.outbox.list_records()
            if record.state in {"pending", "retrying"} and (record_id is None or record.id == record_id)
        ]
        self._log("info", "matrix-outbox-drain-started", attempted_count=len(records), item_id=record_id)
        attempted = 0
        delivered = 0
        retrying = 0
        for record in records:
            attempted += 1
            drained = self._drain_record(record)
            if drained.state == "delivered":
                delivered += 1
            elif drained.state in {"pending", "retrying"}:
                retrying += 1
        self._log(
            "info",
            "matrix-outbox-drain-finished",
            attempted_count=attempted,
            delivered_count=delivered,
            retrying_count=retrying,
            item_id=record_id,
        )
        return MatrixOutboxDrainResult(attempted_count=attempted, delivered_count=delivered, retrying_count=retrying)

    def _drain_record(self, record: OutboxRecord) -> OutboxRecord:
        current = record
        for phase in _OPEN_ALERT_PHASES:
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
                classification = _classify_delivery_failure(exc, phase=phase)
                if classification.retryable:
                    current = self.outbox.mark_retrying(current.id, reason=classification.reason)
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

    def _send_text_phase(self, record: OutboxRecord) -> OutboxRecord:
        event_id = self.client.send_text(
            room_id=record.intent.room_id or self.room_id,
            txn_id=_transaction_id(record, "text"),
            body=record.intent.body,
        )
        return self.outbox.mark_phase_delivered(record.id, "text", result={"matrix_event_id": event_id})

    def _upload_phase(self, record: OutboxRecord) -> OutboxRecord:
        metadata = record.intent.metadata
        retained_path = str(metadata.get("retained_snapshot_path") or metadata.get("snapshot_path", ""))
        if not retained_path.strip() or not Path(retained_path).is_file():
            raise MatrixError("Matrix retained snapshot evidence is missing", error_type="snapshot_missing_source")
        snapshot = self._prepare_retained_snapshot(event=metadata, event_id=record.intent.event_id, source_path=retained_path)
        self._log("info", "matrix-outbox-snapshot-prepared", item_id=record.id, phase="upload", **snapshot.log_context)
        upload = _matrix_snapshot_upload(snapshot, logger=self.logger)
        content_uri = self.client.upload_image(
            filename=snapshot.filename,
            data=upload["data"],
            content_type=JPEG_MIMETYPE,
        )
        info = dict(upload["info"])
        return self.outbox.mark_phase_delivered(
            record.id,
            "upload",
            result={
                "content_uri": content_uri,
                "filename": snapshot.filename,
                "body": snapshot.body,
                "info": info,
            },
        )

    def _prepare_retained_snapshot(self, *, event: Mapping[str, Any], event_id: str, source_path: str) -> MatrixSnapshot:
        return prepare_event_snapshot(
            source_path=source_path,
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type=OPEN_SPOT_EVENT_TYPE,
            event_id=event_id,
            spot_id=str(event.get("spot_id", "")),
            observed_at=event.get("observed_at"),
            snapshot_retention_count=self.snapshot_retention_count,
            logger=self.logger,
            retention_trigger="matrix-outbox",
            protected_snapshots=self._retryable_retained_snapshots(),
        )

    def _retryable_retained_snapshots(self) -> tuple[Path, ...]:
        paths: list[Path] = []
        for record in self.outbox.list_records():
            if record.state not in {"pending", "retrying"}:
                continue
            retained = record.intent.metadata.get("retained_snapshot_path")
            if isinstance(retained, str) and retained.strip():
                paths.append(Path(retained))
        return tuple(paths)

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
        getattr(self.logger, level)(event, **fields)



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
    return f"{record.intent.event_id}:{phase}"


def _retry_reason(exc: Exception, *, phase: str) -> str:
    return _delivery_failure_reason(exc, phase=phase)


def _open_alert_metadata(event: Mapping[str, Any]) -> dict[str, str]:
    return {
        "event_type": OPEN_SPOT_EVENT_TYPE,
        "spot_id": redact_diagnostic_text(event.get("spot_id", "")),
        "observed_at": _safe_observed_at(event.get("observed_at")),
        "snapshot_path": redact_diagnostic_text(event.get("snapshot_path", "")),
    }


def _safe_observed_at(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return value.isoformat()
    return redact_diagnostic_text(value)
