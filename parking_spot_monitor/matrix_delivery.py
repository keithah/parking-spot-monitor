from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_client import MatrixClient
from parking_spot_monitor.matrix_alerts import (
    LIFECYCLE_EVENT_TYPES,
    OCCUPIED_SPOT_EVENT_TYPE,
    OPEN_SPOT_EVENT_TYPE,
    OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
    format_lifecycle_notice,
    format_live_proof_image_body,
    format_live_proof_text,
    format_occupied_spot_alert,
    format_open_spot_alert,
    format_owner_vehicle_quiet_window_alert,
    format_quiet_window_notice,
    live_proof_event_id,
    occupied_spot_event_id,
    open_spot_event_id,
    owner_vehicle_quiet_window_event_id,
)
from parking_spot_monitor.matrix_snapshots import (
    JPEG_MIMETYPE,
    MatrixSnapshot,
    prepare_event_snapshot,
    _matrix_snapshot_upload,
)
from parking_spot_monitor.matrix_support import MatrixError, _require_non_empty

class MatrixDelivery:
    """Runtime delivery façade for parking events sent to one Matrix room."""

    def __init__(
        self,
        *,
        client: MatrixClient,
        room_id: str,
        data_dir: str | Path,
        snapshots_dir: str | Path | None,
        logger: StructuredLogger,
        snapshot_retention_count: int = 50,
        protected_snapshots_provider: Callable[[], Sequence[str | Path]] | None = None,
    ) -> None:
        self.client = client
        self.room_id = room_id
        self.data_dir = Path(data_dir)
        self.snapshots_dir = Path(snapshots_dir) if snapshots_dir is not None else None
        self.logger = logger
        self.snapshot_retention_count = snapshot_retention_count
        self._protected_snapshots_provider = protected_snapshots_provider

    def send_quiet_window_notice(self, event: Mapping[str, Any]) -> None:
        event_id = _require_non_empty("event_id", str(event.get("event_id", "")))
        self.client.send_text(room_id=self.room_id, txn_id=event_id, body=format_quiet_window_notice(event))

    def send_open_spot_alert(self, event: Mapping[str, Any]) -> MatrixSnapshot:
        event_id = open_spot_event_id(event)
        body = format_open_spot_alert(event)
        self.client.send_text(
            room_id=self.room_id,
            txn_id=f"{event_id}:text",
            body=body,
        )
        snapshot = prepare_event_snapshot(
            source_path=str(event.get("snapshot_path", "")),
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type=OPEN_SPOT_EVENT_TYPE,
            event_id=event_id,
            spot_id=str(event.get("spot_id", "")),
            observed_at=event.get("observed_at"),
            snapshot_retention_count=self.snapshot_retention_count,
            logger=self.logger,
            retention_trigger="matrix-event",
            protected_snapshots=self._protected_snapshots(),
        )
        self.logger.info("matrix-snapshot-copied", **snapshot.log_context, txn_id=snapshot.txn_id)
        upload = _matrix_snapshot_upload(snapshot, logger=self.logger)
        content_uri = self.client.upload_image(
            filename=snapshot.filename,
            data=upload["data"],
            content_type=JPEG_MIMETYPE,
        )
        self.client.send_image(
            room_id=self.room_id,
            txn_id=f"{event_id}:image",
            body=body,
            content_uri=content_uri,
            info=upload["info"],
        )
        return snapshot

    def send_occupied_spot_alert(self, event: Mapping[str, Any]) -> MatrixSnapshot:
        event_id = occupied_spot_event_id(event)
        spot_id = _require_non_empty("spot_id", str(event.get("spot_id", "")))
        observed_at = event.get("observed_at")
        source_path = str(event.get("occupied_snapshot_path", ""))
        if not source_path.strip():
            raise MatrixError(
                "Matrix occupied snapshot path is required",
                error_type="snapshot_missing_source",
                event_type=OCCUPIED_SPOT_EVENT_TYPE,
                event_id=event_id,
                spot_id=spot_id,
            )

        if self.logger is not None:
            self.logger.info(
                "matrix-send-attempt",
                event_type=OCCUPIED_SPOT_EVENT_TYPE,
                event_id=event_id,
                spot_id=spot_id,
                operation="occupied-alert",
            )
        try:
            body = format_occupied_spot_alert(event)
            self.client.send_text(
                room_id=self.room_id,
                txn_id=f"{event_id}:text",
                body=body,
            )
            snapshot = prepare_event_snapshot(
                source_path=source_path,
                data_dir=self.data_dir,
                snapshots_dir=self.snapshots_dir,
                event_type=OCCUPIED_SPOT_EVENT_TYPE,
                event_id=event_id,
                spot_id=spot_id,
                observed_at=observed_at,
                snapshot_retention_count=self.snapshot_retention_count,
                logger=self.logger,
                retention_trigger="matrix-event",
                protected_snapshots=self._protected_snapshots(),
            )
            if self.logger is not None:
                self.logger.info("matrix-snapshot-copied", **snapshot.log_context, txn_id=snapshot.txn_id)
            upload = _matrix_snapshot_upload(snapshot, logger=self.logger)
            content_uri = self.client.upload_image(
                filename=snapshot.filename,
                data=upload["data"],
                content_type=JPEG_MIMETYPE,
            )
            self.client.send_image(
                room_id=self.room_id,
                txn_id=f"{event_id}:image",
                body=body,
                content_uri=content_uri,
                info=upload["info"],
            )
        except Exception as exc:
            if self.logger is not None:
                self.logger.warning(
                    "matrix-send-failed",
                    event_type=OCCUPIED_SPOT_EVENT_TYPE,
                    event_id=event_id,
                    spot_id=spot_id,
                    operation="occupied-alert",
                    error_type=exc.__class__.__name__,
                )
            raise
        if self.logger is not None:
            self.logger.info(
                "matrix-send-succeeded",
                event_type=OCCUPIED_SPOT_EVENT_TYPE,
                event_id=event_id,
                spot_id=spot_id,
                operation="occupied-alert",
            )
        return snapshot

    def send_owner_vehicle_quiet_window_alert(self, event: Mapping[str, Any]) -> str:
        return self.client.send_text(
            room_id=self.room_id,
            txn_id=owner_vehicle_quiet_window_event_id(event),
            body=format_owner_vehicle_quiet_window_alert(event),
        )

    def send_lifecycle_notice(self, event: Mapping[str, Any]) -> str:
        event_type = str(event.get("event_type", ""))
        if event_type not in LIFECYCLE_EVENT_TYPES:
            raise MatrixError(
                "Matrix lifecycle event type is unsupported",
                error_type="unsupported_lifecycle_event",
                event_type=event_type,
                event_id=str(event.get("event_id", "")),
            )
        event_id = _require_non_empty("event_id", str(event.get("event_id", "")))
        return self.client.send_text(room_id=self.room_id, txn_id=event_id, body=format_lifecycle_notice(event))

    def _protected_snapshots(self) -> Sequence[str | Path]:
        if self._protected_snapshots_provider is None:
            return ()
        return tuple(self._protected_snapshots_provider())

    def send_live_proof(self, *, latest_path: str | Path, observed_at: object, selected_mode: object) -> None:
        self.send_live_proof_text(observed_at=observed_at, selected_mode=selected_mode)
        self.send_live_proof_image(latest_path=latest_path, observed_at=observed_at, selected_mode=selected_mode)

    def send_live_proof_text(self, *, observed_at: object, selected_mode: object) -> str:
        txn_base = live_proof_event_id(observed_at)
        return self.client.send_text(
            room_id=self.room_id,
            txn_id=f"{txn_base}:text",
            body=format_live_proof_text(observed_at=observed_at, selected_mode=selected_mode),
        )

    def send_live_proof_image(self, *, latest_path: str | Path, observed_at: object, selected_mode: object) -> str:
        txn_base = live_proof_event_id(observed_at)
        snapshot = prepare_event_snapshot(
            source_path=latest_path,
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type="live-proof",
            event_id=txn_base,
            spot_id="camera",
            observed_at=observed_at,
            snapshot_retention_count=self.snapshot_retention_count,
            logger=self.logger,
            retention_trigger="live-proof",
        )
        if self.logger is not None:
            self.logger.info("matrix-live-proof-snapshot-copied", **snapshot.log_context, txn_id=snapshot.txn_id)
        upload = _matrix_snapshot_upload(snapshot, logger=self.logger)
        content_uri = self.client.upload_image(
            filename=snapshot.filename,
            data=upload["data"],
            content_type=JPEG_MIMETYPE,
        )
        return self.client.send_image(
            room_id=self.room_id,
            txn_id=f"{txn_base}:image",
            body=format_live_proof_image_body(observed_at=observed_at),
            content_uri=content_uri,
            info=upload["info"],
        )
