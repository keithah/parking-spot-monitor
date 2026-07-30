from __future__ import annotations

import json

import os

import threading

import time

from datetime import datetime, timezone

from io import BytesIO, StringIO

from pathlib import Path

from typing import Any

import pytest

import httpx

from PIL import Image

import parking_spot_monitor.matrix_snapshots as matrix_snapshots

import parking_spot_monitor.matrix_snapshot_storage as matrix_snapshot_storage

import parking_spot_monitor.matrix_upload_derivatives as matrix_upload_derivatives

import parking_monitor.matrix_outbox_snapshots as matrix_outbox_snapshots

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery, MatrixOutboxDrainResult

from parking_monitor.outbox import LocalOutbox

from parking_spot_monitor.image_budget import JpegBudgetResult

from parking_spot_monitor.jpeg_artifacts import JpegDecodeError

from parking_spot_monitor.logging import StructuredLogger

from parking_spot_monitor.matrix import MatrixError

from parking_spot_monitor.matrix_alerts import MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE, monitor_lifecycle_event

from parking_spot_monitor.matrix_client import MatrixClient

ROOM_ID = "!parking-room:example.org"

EVENT_ID = "occupancy-open-event:left_spot:2026-05-18T20:01:02Z"

RETRY_DUE_NOW = datetime(2100, 1, 1, tzinfo=timezone.utc)

def write_jpeg(path: Path, *, size: tuple[int, int] = (8, 6), color: tuple[int, int, int] = (25, 50, 75)) -> bytes:
    image = Image.new("RGB", size, color=color)
    image.save(path, format="JPEG")
    return path.read_bytes()

def jpeg_bytes(*, size: tuple[int, int], color: tuple[int, int, int] = (25, 50, 75)) -> bytes:
    output = BytesIO()
    Image.new("RGB", size, color=color).save(output, format="JPEG")
    return output.getvalue()

def rewrite_first_outbox_metadata(path: Path, transform: Any) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    metadata = payload["items"][0]["intent"].setdefault("metadata", {})
    transform(metadata)
    path.write_text(json.dumps(payload), encoding="utf-8")

def open_event(snapshot_path: Path) -> dict[str, Any]:
    return {
        "event_type": "occupancy-open-event",
        "spot_id": "left_spot",
        "previous_status": "occupied",
        "new_status": "empty",
        "observed_at": datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc),
        "snapshot_path": str(snapshot_path),
    }

def occupied_event(snapshot_path: Path) -> dict[str, Any]:
    return {
        "event_type": "occupancy-occupied-event",
        "spot_id": "left_spot",
        "previous_status": "empty",
        "new_status": "occupied",
        "observed_at": datetime(2026, 5, 20, 21, 22, 54, tzinfo=timezone.utc),
        "source_timestamp": "2026-05-20T21:22:54Z",
        "event_id": "occupancy-state-changed:left_spot:2026-05-20T21:22:54Z",
        "session_id": "sess_left-spot_2026-05-20t21-22-54-187227-00-00",
        "occupied_snapshot_path": str(snapshot_path),
        "likely_vehicle": {"label": "unknown vehicle"},
        "vehicle_history_estimate": {"status": "insufficient_history", "sample_count": 0},
    }

class FakeMatrixClient:
    def __init__(
        self,
        *,
        fail: dict[str, Exception] | None = None,
        on_send_text: Any | None = None,
        on_send_image: Any | None = None,
    ) -> None:
        self.fail = fail or {}
        self.on_send_text = on_send_text
        self.on_send_image = on_send_image
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
        if self.on_send_text is not None:
            self.on_send_text()
        self.calls.append({"kind": "text", "room_id": room_id, "txn_id": txn_id, "body": body})
        if "text" in self.fail:
            raise self.fail["text"]
        return "$text:example.org"

    def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
        self.calls.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
        if "upload" in self.fail:
            raise self.fail["upload"]
        return "mxc://example.org/open"

    def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
        self.calls.append(
            {
                "kind": "image",
                "room_id": room_id,
                "txn_id": txn_id,
                "body": body,
                "content_uri": content_uri,
                "info": dict(info),
            }
        )
        if "image" in self.fail:
            raise self.fail["image"]
        if self.on_send_image is not None:
            self.on_send_image()
        return "$image:example.org"

    def close(self) -> None:
        self.closed = True

    def cancel_pending(self) -> None:
        self.close()

def make_delivery(
    tmp_path: Path,
    client: FakeMatrixClient,
    *,
    stream: StringIO | None = None,
    snapshot_retention_count: int = 50,
) -> MatrixOutboxDelivery:
    return MatrixOutboxDelivery(
        client=client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
        logger=StructuredLogger(stream=stream) if stream is not None else None,
        snapshot_retention_count=snapshot_retention_count,
    )

__all__ = [name for name in globals() if not name.startswith("__")]
