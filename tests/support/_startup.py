from __future__ import annotations

import json

import os

import signal

import subprocess

import sys

import threading

import time

from collections.abc import Callable, Sequence

from datetime import datetime, timedelta, timezone

from pathlib import Path

from typing import Any

import pytest

from PIL import Image

import parking_spot_monitor.matrix_snapshots as matrix_snapshots

import parking_spot_monitor.file_descriptor_binding as file_descriptor_binding

import parking_spot_monitor.owned_file_disposal as owned_file_disposal

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery

from parking_monitor.outbox import AlertIntent, LocalOutbox

from parking_spot_monitor.capture import CaptureError, DecodeMode, FrameCaptureResult, FrameGeometry

from parking_spot_monitor.capture_loop import run_capture_loop

from parking_spot_monitor.config import load_settings

from parking_spot_monitor.logging import StructuredLogger

from parking_spot_monitor.operator_decision_memory import (
    MAX_TEXT_FIELD_CHARS,
    DecisionMemoryRecord,
    append_decision_memory_records,
    load_decision_memory,
)

from parking_spot_monitor.matrix import MatrixDelivery, MatrixSnapshot

from parking_spot_monitor.matrix_models import MatrixSyncResult

from parking_spot_monitor.__main__ import _default_matrix_command_service_factory, _main, main

from parking_spot_monitor.runtime_presence import presence_by_spot

from parking_spot_monitor.runtime_health import matrix_outbox_health_payload as _matrix_outbox_health_payload

from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleRuntimeCache

from parking_spot_monitor.matrix_dispatch import dispatch_matrix_event

from parking_spot_monitor.detection import DetectionError, DetectionFilterResult, RejectedDetection, RejectionReason, SpotDetectionResult, VehicleDetection

from parking_spot_monitor.detector_adapter import SharedLazyDetector, adapt_detector

from parking_spot_monitor.errors import ConfigError

from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState

from parking_spot_monitor.state import RuntimeState, save_runtime_state

SECRET_MARKER = "startup-secret-should-not-leak"

FAKE_RTSP_VALUE = f"camera-value-{SECRET_MARKER}"

FAKE_MATRIX_VALUE = f"matrix-value-{SECRET_MARKER}"

def fake_environ(**overrides: str) -> dict[str, str]:
    environ = {
        "RTSP_URL": FAKE_RTSP_VALUE,
        "RTSP_URL_4K": f"{FAKE_RTSP_VALUE}-4k",
        "RTSP_URL_360P": f"{FAKE_RTSP_VALUE}-360p",
        "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_VALUE,
    }
    environ.update(overrides)
    return environ

@pytest.fixture(autouse=True)
def mounted_example_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """Model the production Compose mount used by config.yaml.example."""
    original_is_file = Path.is_file

    def is_file(path: Path) -> bool:
        if str(path) == "/models/yolov8n.pt":
            return True
        return original_is_file(path)

    monkeypatch.setattr(Path, "is_file", is_file)

@pytest.fixture(autouse=True)
def restore_ultralytics_process_environment() -> Any:
    from parking_spot_monitor import paths as runtime_paths

    was_present = "YOLO_CONFIG_DIR" in os.environ
    original = os.environ.get("YOLO_CONFIG_DIR")
    original_managed = runtime_paths._managed_ultralytics_config_dir
    yield
    if was_present and original is not None:
        os.environ["YOLO_CONFIG_DIR"] = original
    else:
        os.environ.pop("YOLO_CONFIG_DIR", None)
    runtime_paths._managed_ultralytics_config_dir = original_managed

def combined_output(capsys: pytest.CaptureFixture[str]) -> str:
    captured = capsys.readouterr()
    return captured.out + captured.err

def assert_no_secret_leak(output: str) -> None:
    assert FAKE_RTSP_VALUE not in output
    assert FAKE_MATRIX_VALUE not in output
    assert SECRET_MARKER not in output

def json_records(output: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in output.splitlines() if line.startswith("{")]

def event_names(output: str) -> list[str]:
    return [str(record.get("event")) for record in json_records(output)]

def noop_overlay(_settings: object, _source_path: Path, _output_path: Path, *, logger: Any) -> object:
    return object()

def captured_frame(tmp_path: Path, timestamp: str = "2026-05-18T20:30:00Z") -> FrameCaptureResult:
    latest_path = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
    return FrameCaptureResult(
        timestamp=timestamp,
        latest_path=latest_path,
        selected_mode=DecodeMode.SOFTWARE,
        duration_seconds=0.01,
        byte_size=latest_path.stat().st_size,
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

def left_spot_vehicle() -> VehicleDetection:
    return VehicleDetection(class_name="car", confidence=0.9, bbox=(350, 200, 550, 330))

def next_detection(
    detections: list[list[VehicleDetection]],
    *,
    allow_exhausted: bool = False,
) -> list[VehicleDetection]:
    if detections:
        return detections.pop(0)
    assert allow_exhausted, "unexpected extra detector call"
    return []

def runtime_state_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def health_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def state_status(path: Path, spot_id: str) -> str:
    return str(runtime_state_payload(path)["spots"][spot_id]["status"])

class NoopDetector:
    def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
        return []

def noop_detector_factory(_settings: object) -> NoopDetector:
    return NoopDetector()

def independent_decision_memory_store(tmp_path: Path) -> Any:
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore

    return DecisionMemoryStore(
        tmp_path / "operator-decision-memory.json",
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=50,
    )

class FakeMatrixClient:
    def __init__(self) -> None:
        self.texts: list[dict[str, Any]] = []
        self.uploads: list[dict[str, Any]] = []
        self.images: list[dict[str, Any]] = []
        self.text_sent = threading.Event()
        self.upload_sent = threading.Event()
        self.image_sent = threading.Event()
        self.call_condition = threading.Condition()

    def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
        with self.call_condition:
            self.texts.append({"room_id": room_id, "txn_id": txn_id, "body": body})
            self.call_condition.notify_all()
        self.text_sent.set()
        return f"${txn_id}:example.org"

    def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
        with self.call_condition:
            self.uploads.append({"filename": filename, "data": data, "content_type": content_type})
            self.call_condition.notify_all()
        self.upload_sent.set()
        return f"mxc://example.org/{filename}"

    def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
        with self.call_condition:
            self.images.append(
                {"room_id": room_id, "txn_id": txn_id, "body": body, "content_uri": content_uri, "info": dict(info)}
            )
            self.call_condition.notify_all()
        self.image_sent.set()
        return f"${txn_id}:example.org"

    def wait_for_image_transaction(self, prefix: str, *, timeout: float = 2) -> bool:
        with self.call_condition:
            return self.call_condition.wait_for(
                lambda: any(image["txn_id"].startswith(prefix) for image in self.images),
                timeout=timeout,
            )

class FakeMatrixDelivery:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.quiet_notices: list[dict[str, Any]] = []
        self.open_alerts: list[dict[str, Any]] = []
        self.occupied_alerts: list[dict[str, Any]] = []
        self.live_proofs: list[dict[str, Any]] = []
        self.owner_alerts: list[dict[str, Any]] = []
        self.lifecycle_notices: list[dict[str, Any]] = []
        self.enqueue_threads: list[str] = []

    def enqueue_text_notice(self, event_name: str, event: dict[str, Any]) -> object:
        self.enqueue_threads.append(threading.current_thread().name)
        if event_name == "owner-vehicle-quiet-window-alert":
            self.owner_alerts.append(dict(event))
        else:
            self.quiet_notices.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix enqueue failure {SECRET_MARKER}")
        return object()

    def enqueue_open_spot_alert(self, event: dict[str, Any]) -> object:
        self.enqueue_threads.append(threading.current_thread().name)
        self.open_alerts.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix enqueue failure {SECRET_MARKER}")
        return object()

    def enqueue_occupied_spot_alert(self, event: dict[str, Any]) -> object:
        self.enqueue_threads.append(threading.current_thread().name)
        self.occupied_alerts.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix enqueue failure {SECRET_MARKER}")
        return object()

    def send_quiet_window_notice(self, event: dict[str, Any]) -> None:
        self.quiet_notices.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix failure {SECRET_MARKER}")

    def send_open_spot_alert(self, event: dict[str, Any]) -> None:
        self.open_alerts.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix failure {SECRET_MARKER}")

    def send_occupied_spot_alert(self, event: dict[str, Any]) -> None:
        self.occupied_alerts.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix failure {SECRET_MARKER}")

    def send_live_proof(self, *, latest_path: Path, observed_at: object, selected_mode: object) -> None:
        self.live_proofs.append({"latest_path": latest_path, "observed_at": observed_at, "selected_mode": selected_mode})
        if self.fail:
            raise RuntimeError(f"matrix failure {SECRET_MARKER}")

    def send_owner_vehicle_quiet_window_alert(self, event: dict[str, Any]) -> None:
        self.owner_alerts.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix failure {SECRET_MARKER}")

    def send_lifecycle_notice(self, event: dict[str, Any]) -> None:
        self.lifecycle_notices.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix failure {SECRET_MARKER}")

    def enqueue_lifecycle_notice(self, event: dict[str, Any]) -> object:
        self.lifecycle_notices.append(dict(event))
        if self.fail:
            raise RuntimeError(f"matrix enqueue failure {SECRET_MARKER}")
        return object()

class FakeOutboxDrainResult:
    attempted_count = 0
    delivered_count = 0
    retrying_count = 0

class FakeCommandPollResult:
    def __init__(
        self,
        *,
        processed_count: int = 0,
        ignored_count: int = 0,
        error_count: int = 0,
        bootstrapped: bool = False,
    ) -> None:
        self.next_batch = "fake-next"
        self.processed_count = processed_count
        self.ignored_count = ignored_count
        self.error_count = error_count
        self.bootstrapped = bootstrapped

class UploadFailsOnceMatrixClient(FakeMatrixClient):
    def __init__(self) -> None:
        super().__init__()
        self.fail_upload = True
        self.failed_upload = threading.Event()

    def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
        if self.fail_upload and filename.startswith("occupancy-open-event-"):
            self.fail_upload = False
            self.failed_upload.set()
            raise RuntimeError(f"matrix upload failed {SECRET_MARKER}")
        return super().upload_image(filename=filename, data=data, content_type=content_type)

def outbox_delivery(
    client: object,
    data_dir: Path,
    logger: StructuredLogger,
    *,
    utc_now: Callable[[], datetime] | None = None,
) -> MatrixOutboxDelivery:
    delivery = MatrixOutboxDelivery(
        client=client,
        room_id="!room:example.org",
        data_dir=data_dir,
        snapshots_dir=data_dir / "snapshots",
        outbox=LocalOutbox(data_dir / "matrix-outbox.json"),
        logger=logger,
        utc_now=utc_now,
    )
    delivery.start_worker(retry_interval_seconds=60)
    return delivery

__all__ = [name for name in globals() if not name.startswith("__")]
