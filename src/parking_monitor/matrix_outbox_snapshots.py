"""Durable snapshot artifact lifecycle for the Matrix outbox."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

from parking_monitor.outbox import AlertIntent, LocalOutbox, OutboxRecord
from parking_spot_monitor.jpeg_artifacts import JpegDecodeError
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_alerts import OPEN_SPOT_EVENT_TYPE
from parking_spot_monitor.matrix_snapshot_naming import event_snapshot_path
from parking_spot_monitor.matrix_snapshots import MatrixSnapshot, _matrix_snapshot_upload, prepare_event_snapshot
from parking_spot_monitor.matrix_support import MatrixError
from parking_spot_monitor.matrix_upload_derivatives import (
    MatrixUploadDerivative,
    UploadPublicationLocks,
    load_upload_derivative,
    publish_upload_derivative,
    read_upload_derivative_bytes,
    upload_derivative_path,
    validate_retained_snapshot_path,
)

SNAPSHOT_ALERT_PHASES = ("text", "upload", "image")


@dataclass(frozen=True, slots=True)
class PreparedSnapshotUpload:
    filename: str
    body: str
    data: bytes
    info: dict[str, int | str]
    snapshot_path: Path


class MatrixOutboxSnapshots:
    """Own canonical snapshot and upload-derivative state for outbox records."""

    def __init__(
        self,
        *,
        room_id: str,
        data_dir: Path,
        snapshots_dir: Path | None,
        outbox: LocalOutbox,
        logger: StructuredLogger | None,
        retention_count: int,
    ) -> None:
        self.room_id = room_id
        self.data_dir = data_dir
        self.snapshots_dir = snapshots_dir
        self.outbox = outbox
        self.logger = logger
        self.retention_count = retention_count
        self._publication_locks = UploadPublicationLocks()

    def enqueue(
        self,
        *,
        event: Mapping[str, Any],
        event_id: str,
        body: str,
        metadata: dict[str, Any],
        source_path: str,
        event_type: str,
    ) -> OutboxRecord:
        with self._publication_locks.hold(self._publication_key(event_id)):
            existing = next(
                (
                    record
                    for record in self.outbox.list_records()
                    if record.intent.event_id == event_id and "upload" in record.phase_states
                ),
                None,
            )
            if existing is not None:
                return existing
            snapshot = self.prepare_retained_snapshot(
                event=event,
                event_id=event_id,
                source_path=source_path,
                event_type=event_type,
            )
            durable_metadata = dict(metadata)
            durable_metadata.update(
                {
                    "event_type": event_type,
                    "retained_snapshot_path": str(snapshot.path),
                    "retained_snapshot_filename": snapshot.filename,
                    "retained_snapshot_body": snapshot.body,
                }
            )
            intent = AlertIntent(
                event_id=event_id,
                phase=SNAPSHOT_ALERT_PHASES[0],
                room_id=self.room_id,
                body=body,
                metadata=durable_metadata,
            )
            record = self.outbox.enqueue_with_phases(intent, SNAPSHOT_ALERT_PHASES)
            upload = _matrix_snapshot_upload(snapshot, logger=self.logger)
            derivative = publish_upload_derivative(
                self.snapshot_root,
                snapshot.filename,
                data=upload["data"],
                info=upload["info"],
            )
            return self.outbox.attach_upload_derivative(
                record.id,
                path=str(derivative.path),
                info=dict(derivative.info),
            )

    def prepare_upload(self, record: OutboxRecord) -> PreparedSnapshotUpload:
        with self._publication_locks.hold(self._publication_key(record.intent.event_id)):
            refreshed = next((item for item in self.outbox.list_records() if item.id == record.id), None)
            if refreshed is None:
                raise MatrixError("Matrix outbox record is missing", error_type="snapshot_missing_source")
            return self._prepare_upload_locked(refreshed)

    def _prepare_upload_locked(self, record: OutboxRecord) -> PreparedSnapshotUpload:
        metadata = record.intent.metadata
        try:
            retained_path = self._expected_retained_snapshot(record)
            retained_path = validate_retained_snapshot_path(
                self.snapshot_root,
                retained_path.name,
                metadata.get("retained_snapshot_path"),
            )
        except (JpegDecodeError, OSError, MatrixError):
            raise MatrixError("Matrix retained snapshot evidence is missing", error_type="snapshot_missing_source") from None
        derivative, metadata = self._load_or_create_derivative(record, retained_path=retained_path)
        filename = metadata.get("retained_snapshot_filename")
        body = metadata.get("retained_snapshot_body")
        if not isinstance(filename, str) or not filename or not isinstance(body, str) or not body:
            raise MatrixError("Matrix retained snapshot metadata is malformed", error_type="snapshot_metadata_failed")
        try:
            data = read_upload_derivative_bytes(self.snapshot_root, derivative)
        except JpegDecodeError as exc:
            raise MatrixError("Matrix upload derivative is missing", error_type="snapshot_missing_source") from exc
        info = {key: derivative.info[key] for key in ("mimetype", "size", "w", "h")}
        return PreparedSnapshotUpload(filename, body, data, info, retained_path)

    def prepare_retained_snapshot(
        self,
        *,
        event: Mapping[str, Any],
        event_id: str,
        source_path: str,
        event_type: str,
    ) -> MatrixSnapshot:
        return prepare_event_snapshot(
            source_path=source_path,
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type=event_type,
            event_id=event_id,
            spot_id=str(event.get("spot_id", "")),
            observed_at=event.get("observed_at"),
            snapshot_retention_count=self.retention_count,
            logger=self.logger,
            retention_trigger="matrix-outbox",
            protected_snapshots=self.retryable_paths(),
        )

    def retryable_paths(self) -> tuple[Path, ...]:
        paths: list[Path] = []
        for record in self.outbox.list_records():
            if record.state not in {"pending", "retrying"}:
                continue
            try:
                retained = self._expected_retained_snapshot(record)
            except (MatrixError, ValueError):
                continue
            paths.extend((retained, upload_derivative_path(self.snapshot_root, retained.name)))
        return tuple(paths)

    @property
    def snapshot_root(self) -> Path:
        root = self.snapshots_dir if self.snapshots_dir is not None else self.data_dir / "snapshots"
        return Path(os.path.abspath(root))

    def _publication_key(self, event_id: str) -> str:
        return f"{self.snapshot_root}::{event_id}"

    def _load_or_create_derivative(
        self, record: OutboxRecord, *, retained_path: Path
    ) -> tuple[MatrixUploadDerivative, Mapping[str, Any]]:
        metadata = record.intent.metadata
        derivative_path = metadata.get("upload_derivative_path")
        derivative_info = metadata.get("upload_derivative_info")
        if derivative_path is not None or derivative_info is not None:
            if not isinstance(derivative_path, str) or not isinstance(derivative_info, Mapping):
                raise MatrixError("Matrix upload derivative metadata is malformed", error_type="snapshot_resize_failed")
            try:
                derivative = load_upload_derivative(
                    self.snapshot_root,
                    retained_path.name,
                    persisted_path=derivative_path,
                    info=derivative_info,
                )
                return derivative, metadata
            except (JpegDecodeError, OSError):
                raise MatrixError("Matrix upload derivative is invalid", error_type="snapshot_resize_failed") from None

        snapshot = self.prepare_retained_snapshot(
            event=metadata,
            event_id=record.intent.event_id,
            source_path=str(retained_path),
            event_type=str(metadata.get("event_type") or OPEN_SPOT_EVENT_TYPE),
        )
        upload = _matrix_snapshot_upload(snapshot, logger=self.logger)
        derivative = publish_upload_derivative(
            self.snapshot_root,
            snapshot.filename,
            data=upload["data"],
            info=upload["info"],
        )
        updated = self.outbox.attach_upload_derivative(
            record.id,
            path=str(derivative.path),
            info=dict(derivative.info),
        )
        return derivative, updated.intent.metadata

    def _expected_retained_snapshot(self, record: OutboxRecord) -> Path:
        metadata = record.intent.metadata
        event_type = metadata.get("event_type")
        observed_at = metadata.get("observed_at")
        spot_id = metadata.get("spot_id")
        if not isinstance(event_type, str) or not event_type or not isinstance(observed_at, str) or not observed_at:
            raise MatrixError("Matrix retained snapshot metadata is malformed", error_type="snapshot_metadata_failed")
        expected = event_snapshot_path(
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type=event_type,
            event_id=record.intent.event_id,
            spot_id=spot_id if isinstance(spot_id, str) else None,
            observed_at=observed_at,
        )
        if metadata.get("retained_snapshot_filename") != expected.name:
            raise MatrixError("Matrix retained snapshot metadata is malformed", error_type="snapshot_metadata_failed")
        return expected
