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


def test_explicit_missing_model_path_fails_before_runtime_loop(tmp_path: Path) -> None:
    from parking_spot_monitor import __main__ as cli

    missing = tmp_path / "models" / "yolov8n.pt"

    with pytest.raises(ConfigError, match="configured model file does not exist"):
        cli.validate_model_path(str(missing))


def test_explicit_relative_missing_model_path_fails_before_runtime_loop() -> None:
    from parking_spot_monitor import __main__ as cli

    with pytest.raises(ConfigError, match="configured model file does not exist"):
        cli.validate_model_path("models/yolov8n.pt")


@pytest.mark.parametrize(
    "model",
    [
        r"C:\models\yolov8n.pt",
        r"C:yolov8n.pt",
        r".\models\yolov8n.pt",
        r"\models\yolov8n.pt",
    ],
)
def test_windows_style_model_paths_fail_as_missing_explicit_paths(model: str) -> None:
    from parking_spot_monitor import __main__ as cli

    with pytest.raises(ConfigError, match="configured model file does not exist"):
        cli.validate_model_path(model)


def test_existing_explicit_posix_model_path_with_spaces_is_allowed(tmp_path: Path) -> None:
    from parking_spot_monitor import __main__ as cli

    model = tmp_path / "trusted model weights" / "yolo nano.pt"
    model.parent.mkdir()
    model.touch()

    cli.validate_model_path(str(model))


def test_legacy_bare_model_name_does_not_require_local_file() -> None:
    from parking_spot_monitor import __main__ as cli

    cli.validate_model_path("yolov8n.pt")


def test_validate_config_rejects_explicit_missing_model_before_ready(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = tmp_path / "models" / "yolov8n.pt"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("/models/yolov8n.pt", str(missing)),
        encoding="utf-8",
    )

    exit_code = main(["--config", str(config_path), "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert "configured model file does not exist" in output
    assert '"phase":"model"' in output
    assert '"event":"startup-ready"' not in output


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


def test_structured_logger_recursively_redacts_secret_bearing_fields(capsys: pytest.CaptureFixture[str]) -> None:
    logger = StructuredLogger()

    logger.info(
        "sentinel-redaction-check",
        message="rtsp://user:pass@camera token=top-secret Traceback noisy",
        nested={"frame_path": "/data/latest.jpg?access_token=frame-secret"},
        items=["matrix_token=list-secret"],
    )

    output = combined_output(capsys)
    assert '"event":"sentinel-redaction-check"' in output
    assert "rtsp://<redacted>" in output
    assert "token=<redacted>" in output
    assert "access_token=<redacted>" in output
    assert "matrix_token=<redacted>" in output
    assert "user:pass" not in output
    assert "top-secret" not in output
    assert "frame-secret" not in output
    assert "list-secret" not in output
    assert "Traceback" not in output


def test_importing_main_does_not_import_operator_stack() -> None:
    blocked = {
        "parking_spot_monitor.matrix_cockpit",
        "parking_spot_monitor.matrix_commands",
        "parking_spot_monitor.operator_cockpit",
        "parking_spot_monitor.operator_feedback",
        "parking_spot_monitor.detection_lab",
    }
    script = (
        "import sys; sys.path.insert(0, 'src'); import parking_spot_monitor.__main__; "
        f"blocked={blocked!r}; "
        "present=sorted(blocked.intersection(sys.modules)); "
        "assert not present, present"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_disabled_matrix_commands_do_not_import_operator_stack() -> None:
    blocked = {
        "parking_spot_monitor.matrix_cockpit",
        "parking_spot_monitor.matrix_commands",
        "parking_spot_monitor.operator_cockpit",
        "parking_spot_monitor.operator_feedback",
        "parking_spot_monitor.detection_lab",
    }
    script = (
        "import sys; sys.path.insert(0, 'src'); "
        "from io import StringIO; from types import SimpleNamespace; "
        "import parking_spot_monitor.__main__ as cli; "
        "from parking_spot_monitor.logging import StructuredLogger; "
        "settings=SimpleNamespace(matrix=SimpleNamespace(command_authorized_senders=[])); "
        "result=cli._default_matrix_command_service_factory("
        "settings, None, StructuredLogger(stream=StringIO()), object(), incident_detector=object()); "
        "assert result is None; "
        f"blocked={blocked!r}; "
        "present=sorted(blocked.intersection(sys.modules)); "
        "assert not present, present"
    )

    completed = subprocess.run(
        [sys.executable, "-c", script], text=True, capture_output=True, check=False
    )

    assert completed.returncode == 0, completed.stderr


def test_logger_reports_normalized_enabled_levels_without_serializing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import parking_spot_monitor.logging as structured_logging

    logger = StructuredLogger(level="INFO")
    monkeypatch.setattr(
        structured_logging,
        "redact_diagnostic_value",
        lambda _value: pytest.fail("level query serialized a log record"),
    )

    assert logger.is_enabled_for("debug") is False
    assert logger.is_enabled_for("info") is True
    assert logger.is_enabled_for("WARNING") is True
    assert logger.is_enabled_for("not-a-level") is True


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


def test_dispatch_matrix_open_alert_feedback_uses_retained_snapshot_not_latest(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    latest_path = tmp_path / "latest.jpg"
    Image.new("RGB", (10, 8), (12, 34, 56)).save(latest_path, format="JPEG")
    retained_path = tmp_path / "snapshots" / "occupancy-open-event-left-spot-retained.jpg"
    retained_path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (17, 11), (78, 90, 12)).save(retained_path, format="JPEG")

    class RetainedSnapshotDelivery:
        def __init__(self) -> None:
            self.open_alerts: list[dict[str, Any]] = []

        def enqueue_open_spot_alert(self, event: dict[str, Any]) -> MatrixSnapshot:
            self.open_alerts.append(dict(event))
            return MatrixSnapshot(
                path=retained_path,
                filename=retained_path.name,
                txn_id="snapshot-retained",
                body="retained open alert snapshot",
                info={"mimetype": "image/jpeg", "size": retained_path.stat().st_size, "w": 17, "h": 11},
                log_context={"snapshot_path": str(retained_path)},
            )

    event = {
        "event_type": "occupancy-open-event",
        "event_id": "occupancy-open-event:left_spot:2026-05-18T20:01:02Z",
        "spot_id": "left_spot",
        "observed_at": "2026-05-18T20:01:02Z",
        "snapshot_path": str(latest_path),
    }

    error = dispatch_matrix_event(
        RetainedSnapshotDelivery(),
        "occupancy-open-event",
        event,
        logger=StructuredLogger(),
        decision_memory_path=tmp_path / "operator-decision-memory.json",
    )

    assert error is None
    result = OperatorFeedbackLabeler(data_dir=tmp_path).record_correction(
        spot_id="left_spot",
        actual_state="occupied",
        matrix_event_id="$correction",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        corrected_at="2026-05-18T20:02:00Z",
    )

    assert result.recorded is True
    assert result.reported_state == "open"
    assert result.evidence.available is True
    assert result.evidence.validated_jpeg is True
    assert result.evidence.path == "snapshots/occupancy-open-event-left-spot-retained.jpg"
    assert result.evidence.path != str(latest_path)
    assert result.evidence.width == 17
    assert result.evidence.height == 11

    loaded = load_feedback_labels(feedback_labels_path(tmp_path))
    assert loaded.state == "available"
    assert len(loaded.labels) == 1
    assert loaded.labels[0].evidence.available is True
    assert loaded.labels[0].evidence.validated_jpeg is True
    assert loaded.labels[0].evidence.path == "snapshots/occupancy-open-event-left-spot-retained.jpg"


def test_dispatch_matrix_open_alert_enqueues_without_immediate_network_when_outbox_supported(tmp_path: Path) -> None:
    class EnqueueOnlyDelivery:
        def __init__(self) -> None:
            self.enqueued: list[dict[str, Any]] = []
            self.sent: list[dict[str, Any]] = []

        def enqueue_open_spot_alert(self, event: dict[str, Any]) -> object:
            self.enqueued.append(dict(event))
            return object()

        def send_open_spot_alert(self, event: dict[str, Any]) -> None:
            self.sent.append(dict(event))
            raise AssertionError("frame dispatch should not perform open-alert network drain")

    event = {
        "event_type": "occupancy-open-event",
        "event_id": "occupancy-open-event:left_spot:2026-05-18T20:01:02Z",
        "spot_id": "left_spot",
        "observed_at": "2026-05-18T20:01:02Z",
        "snapshot_path": str(tmp_path / "latest.jpg"),
    }
    delivery = EnqueueOnlyDelivery()

    error = dispatch_matrix_event(
        delivery,
        "occupancy-open-event",
        event,
        logger=StructuredLogger(),
        decision_memory_path=tmp_path / "operator-decision-memory.json",
    )

    assert error is None
    assert delivery.enqueued == [event]
    assert delivery.sent == []


def test_dispatch_matrix_open_alert_enqueue_records_queued_memory(tmp_path: Path) -> None:
    class EnqueueOnlyDelivery:
        def enqueue_open_spot_alert(self, event: dict[str, Any]) -> object:
            return object()

        def send_open_spot_alert(self, event: dict[str, Any]) -> None:
            raise AssertionError("frame dispatch should not drain open alerts")

    event = {
        "event_type": "occupancy-open-event",
        "event_id": "occupancy-open-event:left_spot:2026-05-18T20:01:02Z",
        "spot_id": "left_spot",
        "observed_at": "2026-05-18T20:01:02Z",
        "snapshot_path": str(tmp_path / "latest.jpg"),
    }
    memory_path = tmp_path / "operator-decision-memory.json"

    error = dispatch_matrix_event(
        EnqueueOnlyDelivery(),
        "occupancy-open-event",
        event,
        logger=StructuredLogger(),
        decision_memory_path=memory_path,
    )

    records = json.loads(memory_path.read_text(encoding="utf-8"))["records"]
    assert error is None
    assert len(records) == 1
    assert records[0]["summary"] == "occupancy-open-event queued"
    assert records[0]["details"]["outcome"] == "queued"
    assert records[0]["details"]["reason"] == "outbox_enqueue"


def test_dispatch_matrix_alert_flushes_service_decision_store_immediately(tmp_path: Path) -> None:
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
    from parking_spot_monitor.operator_decision_memory import make_decision_memory_record

    memory_path = tmp_path / "operator-decision-memory.json"
    store = DecisionMemoryStore(
        memory_path,
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=50,
    )
    store.append(
        make_decision_memory_record("miss", spot_id="left_spot", summary="routine"),
        durability="routine",
    )
    event = {
        "event_type": "occupancy-state-changed",
        "event_id": "state:left_spot:1",
        "spot_id": "left_spot",
    }

    dispatch_matrix_event(
        None,
        event["event_type"],
        event,
        logger=StructuredLogger(),
        decision_memory_store=store,
    )

    assert [record.summary for record in load_decision_memory(memory_path).records] == [
        "routine",
        "occupancy-state-changed skipped",
    ]


def test_dispatch_matrix_occupied_alert_uses_durable_snapshot_enqueue(tmp_path: Path) -> None:
    class EnqueueOnlyDelivery:
        def __init__(self) -> None:
            self.enqueued: list[dict[str, Any]] = []

        def enqueue_occupied_spot_alert(self, event: dict[str, Any]) -> object:
            self.enqueued.append(dict(event))
            return object()

        def send_occupied_spot_alert(self, event: dict[str, Any]) -> None:
            raise AssertionError("frame dispatch must not send an occupied alert immediately")

    source = tmp_path / "occupied.jpg"
    Image.new("RGB", (10, 8), (12, 34, 56)).save(source, format="JPEG")
    event = {
        "event_type": "occupancy-occupied-event",
        "event_id": "occupancy-state-changed:left_spot:2026-05-18T20:01:02Z",
        "spot_id": "left_spot",
        "observed_at": "2026-05-18T20:01:02Z",
        "occupied_snapshot_path": str(source),
    }
    delivery = EnqueueOnlyDelivery()

    error = dispatch_matrix_event(delivery, event["event_type"], event, logger=StructuredLogger())

    assert error is None
    assert delivery.enqueued == [event]


@pytest.mark.parametrize(
    ("event_name", "event"),
    [
        (
            "quiet-window-started",
            {
                "event_type": "quiet-window-started",
                "event_id": "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00",
                "window_id": "street_sweeping:2026-05-18:13:00-15:00",
            },
        ),
        (
            "owner-vehicle-quiet-window-alert",
            {
                "event_type": "owner-vehicle-quiet-window-alert",
                "event_id": "owner-vehicle-quiet-window-alert:left_spot:prof-owner:window-1",
                "spot_id": "left_spot",
                "profile_id": "prof-owner",
                "window_id": "window-1",
                "observed_at": "2026-05-18T20:01:02Z",
                "owner_vehicle": {"label": "owner car"},
            },
        ),
    ],
)
def test_dispatch_matrix_frame_text_notices_use_durable_enqueue(
    event_name: str,
    event: dict[str, Any],
) -> None:
    class EnqueueOnlyDelivery:
        def __init__(self) -> None:
            self.enqueued: list[tuple[str, dict[str, Any]]] = []

        def enqueue_text_notice(self, queued_name: str, queued_event: dict[str, Any]) -> object:
            self.enqueued.append((queued_name, dict(queued_event)))
            return object()

        def send_quiet_window_notice(self, event: dict[str, Any]) -> None:
            raise AssertionError("frame dispatch must not send a quiet-window notice immediately")

        def send_owner_vehicle_quiet_window_alert(self, event: dict[str, Any]) -> None:
            raise AssertionError("frame dispatch must not send an owner notice immediately")

    delivery = EnqueueOnlyDelivery()

    error = dispatch_matrix_event(delivery, event_name, event, logger=StructuredLogger())

    assert error is None
    assert delivery.enqueued == [(event_name, event)]


def test_frame_update_network_delivery_runs_only_on_the_outbox_worker(tmp_path: Path) -> None:
    from parking_spot_monitor.runtime_state_update import _update_runtime_state_for_frame

    sent = threading.Event()

    class ThreadTrackingClient(FakeMatrixClient):
        def __init__(self) -> None:
            super().__init__()
            self.send_threads: list[str] = []

        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            self.send_threads.append(threading.current_thread().name)
            result = super().send_text(room_id=room_id, txn_id=txn_id, body=body)
            sent.set()
            return result

    settings = load_settings("config.yaml.example", environ=fake_environ())
    detection_result = DetectionFilterResult(
        by_spot={
            "left_spot": SpotDetectionResult(spot_id="left_spot", accepted=None, rejected=[]),
            "right_spot": SpotDetectionResult(spot_id="right_spot", accepted=None, rejected=[]),
        },
        rejection_counts={},
    )
    client = ThreadTrackingClient()
    delivery = MatrixOutboxDelivery(
        client=client,
        room_id="!room:example.org",
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
    )
    delivery.start_worker(retry_interval_seconds=60)
    try:
        update = _update_runtime_state_for_frame(
            settings=settings,
            runtime_state=RuntimeState.default(["left_spot", "right_spot"]),
            detection_result=detection_result,
            observed_at=datetime(2026, 5, 18, 20, 30, tzinfo=timezone.utc),
            snapshot_path=str(tmp_path / "latest.jpg"),
            logger=StructuredLogger(),
            matrix_delivery=delivery,
            state_path=tmp_path / "state.json",
            configured_spot_ids=["left_spot", "right_spot"],
            owner_vehicle_snapshot_provider=OwnerVehicleRuntimeCache(
                tmp_path / "owner-vehicles.json",
                logger=StructuredLogger(),
            ),
        )

        assert update.matrix_errors == []
        assert sent.wait(2), "worker did not deliver the durable frame notice"
        assert client.send_threads == ["matrix-outbox-delivery"]
    finally:
        delivery.close()

    [record] = delivery.outbox.list_records()
    assert record.intent.event_id == "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00"
    assert record.phase_states == {"text": "delivered"}


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


@pytest.mark.parametrize("bootstrapped", [False, True])
def test_zero_count_matrix_command_success_uses_debug(
    bootstrapped: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from parking_spot_monitor.runtime_commands import _poll_matrix_commands_once

    class NoopCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult(bootstrapped=bootstrapped)

    _poll_matrix_commands_once(NoopCommandService(), logger=StructuredLogger(level="DEBUG"), iteration=1)

    success = [
        record
        for record in json_records(combined_output(capsys))
        if record.get("event") == "matrix-command-poll-succeeded"
    ]
    assert [record["level"] for record in success] == ["DEBUG"]


def test_nonzero_matrix_command_success_remains_info(capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.runtime_commands import _poll_matrix_commands_once

    class ProcessedCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult(processed_count=1)

    _poll_matrix_commands_once(ProcessedCommandService(), logger=StructuredLogger(), iteration=1)

    success = [
        record
        for record in json_records(combined_output(capsys))
        if record.get("event") == "matrix-command-poll-succeeded"
    ]
    assert [record["level"] for record in success] == ["INFO"]



def test_process_detection_uses_spot_crop_inference_to_recover_full_frame_miss(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example").read_text(encoding="utf-8").replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    class FullMissCropDetector:
        def __init__(self) -> None:
            self.calls: list[tuple[Path, tuple[int, int]]] = []

        def detect(
            self,
            frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            path = Path(frame_path)
            with Image.open(path) as image:
                size = image.size
            self.calls.append((path, size))
            assert confidence_threshold == 0.1
            assert inference_image_size == 1280
            if size == (1458, 806):
                return []
            if size == (531, 296):
                return [VehicleDetection(class_name="car", confidence=0.88, bbox=(98, 93, 483, 233))]
            return []

    from parking_spot_monitor.runtime_detection import _process_detection_for_capture

    settings = load_settings(config_path, environ=fake_environ())
    detector = FullMissCropDetector()

    result = _process_detection_for_capture(
        settings,
        adapt_detector(detector),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    right = result.by_spot["right_spot"].accepted
    assert right is not None
    assert right.bbox == pytest.approx((1010, 215, 1395, 355))
    assert [size for _path, size in detector.calls] == [(1458, 806), (526, 276), (531, 296)]
    assert all(not path.exists() for path, _size in detector.calls[1:])
    output = combined_output(capsys)
    assert '"spot_crop_inference_enabled":true' in output
    assert '"spot_crop_detection_count":1' in output


def test_process_detection_uses_no_temporary_files_for_in_memory_crop_detector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")
    real_image_open = Image.open
    image_open_calls = 0

    def counting_image_open(*args: object, **kwargs: object) -> Image.Image:
        nonlocal image_open_calls
        image_open_calls += 1
        return real_image_open(*args, **kwargs)

    monkeypatch.setattr(Image, "open", counting_image_open)

    class RecordingInMemoryDetector:
        def __init__(self) -> None:
            self.crop_images: list[Image.Image] = []
            self.crop_calls: list[dict[str, object]] = []

        def detect(self, frame_path: str | Path, **kwargs: object) -> list[VehicleDetection]:
            return []

        def detect_image(self, image: Image.Image, **kwargs: object) -> list[VehicleDetection]:
            self.crop_images.append(image)
            self.crop_calls.append({"size": image.size, **kwargs})
            if image.size == (531, 296):
                return [VehicleDetection(class_name="car", confidence=0.88, bbox=(98, 93, 483, 233))]
            return []

    import parking_spot_monitor.runtime_detection as runtime_detection

    def fail_temporary_directory(*args: object, **kwargs: object) -> object:
        pytest.fail("in-memory crop inference allocated a temporary directory")

    monkeypatch.setattr(runtime_detection.tempfile, "TemporaryDirectory", fail_temporary_directory)
    detector = RecordingInMemoryDetector()
    settings = load_settings(config_path, environ=fake_environ())

    result = runtime_detection._process_detection_for_capture(
        settings,
        adapt_detector(detector),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert detector.crop_calls == [
        {"size": (526, 276), "confidence_threshold": 0.1, "inference_image_size": 1280},
        {"size": (531, 296), "confidence_threshold": 0.1, "inference_image_size": 1280},
    ]
    assert result.by_spot["right_spot"].accepted is not None
    assert result.by_spot["right_spot"].accepted.bbox == pytest.approx((1010, 215, 1395, 355))
    assert image_open_calls == 1
    for crop in detector.crop_images:
        with pytest.raises(ValueError, match="closed"):
            crop.getpixel((0, 0))


def test_incompatible_detect_image_uses_temporary_jpeg_fallback(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    class IncidentalDetectImageDetector:
        def __init__(self) -> None:
            self.path_calls: list[tuple[Path, tuple[int, int]]] = []
            self.image_calls = 0

        def detect(
            self,
            frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            path = Path(frame_path)
            with Image.open(path) as image:
                size = image.size
            self.path_calls.append((path, size))
            if size == (531, 296):
                return [VehicleDetection(class_name="car", confidence=0.88, bbox=(98, 93, 483, 233))]
            return []

        def detect_image(self, image: Image.Image) -> list[VehicleDetection]:
            self.image_calls += 1
            return []

    import parking_spot_monitor.runtime_detection as runtime_detection

    detector = IncidentalDetectImageDetector()
    result = runtime_detection._process_detection_for_capture(
        load_settings(config_path, environ=fake_environ()),
        adapt_detector(detector),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert detector.image_calls == 0
    assert [size for _path, size in detector.path_calls] == [(1458, 806), (526, 276), (531, 296)]
    assert all(not path.exists() for path, _size in detector.path_calls[1:])
    assert result.by_spot["right_spot"].accepted is not None


def test_compatible_detect_image_internal_type_error_propagates(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")
    sentinel = TypeError("detector inference failed internally")

    class FailingInMemoryDetector:
        def detect(self, frame_path: str | Path, **kwargs: object) -> list[VehicleDetection]:
            return []

        def detect_image(
            self,
            image: Image.Image,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            raise sentinel

    import parking_spot_monitor.runtime_detection as runtime_detection

    with pytest.raises(TypeError) as exc_info:
        runtime_detection._process_detection_for_capture(
            load_settings(config_path, environ=fake_environ()),
            adapt_detector(FailingInMemoryDetector()),
            frame,
            logger=StructuredLogger(),
            mode="test",
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    assert exc_info.value is sentinel



def test_spot_crop_image_size_failure_closes_open_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )

    class FailingSizeImage:
        def __init__(self) -> None:
            self.closed = False

        @property
        def size(self) -> tuple[int, int]:
            raise OSError("unreadable dimensions")

        def close(self) -> None:
            self.closed = True

    class EmptyDetector:
        def detect(self, frame_path: str | Path, **kwargs: object) -> list[VehicleDetection]:
            return []

    source = FailingSizeImage()
    monkeypatch.setattr(Image, "open", lambda *args, **kwargs: source)

    import parking_spot_monitor.runtime_detection as runtime_detection

    runtime_detection._process_detection_for_capture(
        load_settings(config_path, environ=fake_environ()),
        adapt_detector(EmptyDetector()),
        tmp_path / "latest.jpg",
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert source.closed is True



def test_process_detection_scales_configured_polygons_to_actual_frame_size(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    frame = tmp_path / "low-res-latest.jpg"
    Image.new("RGB", (640, 360), (20, 30, 40)).save(frame, format="JPEG")

    class LowResDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return [VehicleDetection(class_name="car", confidence=0.9, bbox=(142.0, 91.0, 265.0, 151.0))]

    from parking_spot_monitor.runtime_detection import _process_detection_for_capture

    settings = load_settings("config.yaml.example", environ=fake_environ())
    result = _process_detection_for_capture(
        settings,
        adapt_detector(LowResDetector()),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="low_resolution", expected_size=(640, 360)),
    )

    output = combined_output(capsys)
    [record] = json_records(output)
    assert result.by_spot["left_spot"].accepted is not None
    assert result.by_spot["right_spot"].accepted is None
    assert record["frame_size_mismatch"] is True
    assert record["configured_frame_size"] == {"height": 806, "width": 1458}
    assert record["actual_frame_size"] == {"height": 360, "width": 640}
    assert record["accepted_by_spot"] == {"left_spot": True, "right_spot": False}


def test_process_detection_skips_candidate_summaries_when_info_is_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import parking_spot_monitor.runtime_detection as runtime_detection

    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    def forbidden_candidate_summaries(_result: DetectionFilterResult) -> list[dict[str, Any]]:
        pytest.fail("candidate summaries were computed for a suppressed INFO record")

    monkeypatch.setattr(runtime_detection, "_candidate_summaries", forbidden_candidate_summaries)

    result = runtime_detection._process_detection_for_capture(
        load_settings("config.yaml.example", environ=fake_environ()),
        adapt_detector(NoopDetector()),
        frame,
        logger=StructuredLogger(level="WARNING"),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert set(result.by_spot) == {"left_spot", "right_spot"}


def test_runtime_detection_does_not_build_candidate_arrays_for_info_logging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import parking_spot_monitor.runtime_detection as runtime_detection

    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    def forbidden_candidate_summaries(_result: DetectionFilterResult) -> list[dict[str, Any]]:
        pytest.fail("routine runtime INFO must not build candidate arrays")

    monkeypatch.setattr(runtime_detection, "_candidate_summaries", forbidden_candidate_summaries)
    runtime_detection._process_detection_for_capture(
        load_settings("config.yaml.example", environ=fake_environ()),
        adapt_detector(NoopDetector()),
        frame,
        logger=StructuredLogger(),
        mode="runtime-loop",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert '"event":"detection-frame-processed"' not in combined_output(capsys)


def test_process_detection_keeps_candidate_summary_schema_when_info_is_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import parking_spot_monitor.runtime_detection as runtime_detection

    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")
    calls = 0
    original = runtime_detection._candidate_summaries

    def candidate_summary_spy(result: DetectionFilterResult) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        return original(result)

    monkeypatch.setattr(runtime_detection, "_candidate_summaries", candidate_summary_spy)

    runtime_detection._process_detection_for_capture(
        load_settings("config.yaml.example", environ=fake_environ()),
        adapt_detector(NoopDetector()),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    [record] = json_records(combined_output(capsys))
    assert calls == 1
    assert record["candidate_summaries"] == []


def test_runtime_loop_matrix_state_change_skip_log_explains_policy(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    records = json_records(output)
    skipped = [
        record
        for record in records
        if record.get("event") == "matrix-delivery-skipped"
        and record.get("event_type") == "occupancy-state-changed"
        and record.get("spot_id") == "left_spot"
    ]
    assert exit_code == 0
    assert delivery.open_alerts == []
    assert len(skipped) == 1
    assert skipped[0]["reason"] == "state-change-not-alert"
    assert skipped[0]["matrix_dispatch_policy"] == "open-events-only"
    assert skipped[0]["next_expected_event"] == "occupancy-open-event"
    assert_no_secret_leak(output)


def test_runtime_loop_vehicle_history_confirmed_occupied_creates_one_active_session_with_one_occupied_matrix_alert(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=4,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    active_files = list((tmp_path / "vehicle-history" / "sessions" / "active").glob("*.json"))
    closed_files = list((tmp_path / "vehicle-history" / "sessions" / "closed").glob("*.json"))
    assert exit_code == 0
    assert len(active_files) == 1
    assert closed_files == []
    assert delivery.open_alerts == []
    assert len(delivery.occupied_alerts) == 1
    occupied_alert = delivery.occupied_alerts[0]
    assert occupied_alert["event_type"] == "occupancy-occupied-event"
    assert occupied_alert["spot_id"] == "left_spot"
    assert occupied_alert["session_id"]
    assert occupied_alert["occupied_snapshot_path"] is not None
    assert "occupied_crop_path" not in occupied_alert
    assert "candidate_summary" not in occupied_alert
    assert occupied_alert["vehicle_history_estimate"]["status"] == "insufficient_history"
    assert occupied_alert["vehicle_history_estimate"]["sample_count"] == 0
    active_payload = json.loads(active_files[0].read_text(encoding="utf-8"))
    assert active_payload["spot_id"] == "left_spot"
    assert active_payload["ended_at"] is None
    assert active_payload["start_event"]["event_type"] == "occupancy-state-changed"
    assert active_payload["occupied_snapshot_path"] is not None
    assert active_payload["occupied_crop_path"] is not None
    assert active_payload["profile_id"] is not None
    assert active_payload["profile_confidence"] == pytest.approx(1.0)
    occupied_snapshot = Path(active_payload["occupied_snapshot_path"])
    occupied_crop = Path(active_payload["occupied_crop_path"])
    assert occupied_snapshot.exists()
    assert occupied_crop.exists()
    with Image.open(tmp_path / "latest.jpg") as latest_frame:
        latest_size = latest_frame.size
    with Image.open(occupied_snapshot) as full_frame:
        assert full_frame.format == "JPEG"
        assert full_frame.size == latest_size
    with Image.open(occupied_crop) as crop:
        assert crop.format == "JPEG"
        assert crop.size == (200, 130)
        assert crop.size[0] < 1458
        assert crop.size[1] < 806
    health = health_payload(tmp_path / "health.json")
    assert health["vehicle_history"]["occupied_snapshot_count"] == 1
    assert health["vehicle_history"]["occupied_crop_count"] == 1
    assert health["vehicle_history"]["image_file_count"] == 2
    assert health["vehicle_history"]["image_bytes"] > 0
    assert health["vehicle_history"]["missing_occupied_image_reference_count"] == 0
    assert health["vehicle_history"]["profile_count"] == 1
    assert health["vehicle_history"]["profile_sample_count"] == 1
    assert health["vehicle_history"]["profile_unknown_session_count"] == 0
    assert "vehicle_history" not in runtime_state_payload(tmp_path / "state.json")
    assert '"event":"vehicle-session-lifecycle-recorded"' in output
    assert '"event":"vehicle-session-images-attached"' in output
    assert '"event":"vehicle-session-profile-matched"' in output
    assert '"action":"match-profile"' in output
    assert '"match_status":"new_profile"' in output
    assert '"action":"start"' in output
    assert_no_secret_leak(output)




def test_runtime_loop_owner_vehicle_in_quiet_window_sends_deduped_alert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import parking_spot_monitor.capture_loop as runtime_capture_loop
    import parking_spot_monitor.runtime_owner_vehicle_cache as runtime_owner_vehicle_cache
    from parking_spot_monitor.owner_vehicles import load_owner_vehicle_registry
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    data_dir = tmp_path
    owner_profile_id = "prof_tesla"
    archive_root = data_dir / "vehicle-history"
    active_dir = archive_root / "sessions" / "active"
    active_dir.mkdir(parents=True)
    session_payload = {
        "schema_version": 1,
        "session_id": "sess_owner_right",
        "spot_id": "right_spot",
        "started_at": "2026-05-18T19:30:00+00:00",
        "ended_at": None,
        "duration_seconds": None,
        "start_event": {"event_type": "occupancy-state-changed"},
        "close_event": None,
        "source_snapshot_path": str(data_dir / "latest.jpg"),
        "candidate_summary": None,
        "occupied_snapshot_path": str(data_dir / "latest.jpg"),
        "occupied_crop_path": str(data_dir / "crop.jpg"),
        "profile_id": owner_profile_id,
        "profile_confidence": 0.99,
        "created_at": "2026-05-18T19:30:00Z",
        "updated_at": "2026-05-18T19:30:00Z",
    }
    active_dir.joinpath("sess_owner_right.json").write_text(json.dumps(session_payload), encoding="utf-8")
    archive_root.joinpath("owner-vehicles.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_vehicles": [
                    {
                        "profile_id": owner_profile_id,
                        "label": "Keith's black Tesla",
                        "description": "black Tesla, tinted windows, roof rack",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    active_loads = 0
    registry_loads = 0
    frame_providers: list[object] = []
    real_load_active_sessions = VehicleHistoryArchive.load_active_sessions
    real_update_runtime_state = runtime_capture_loop._update_runtime_state_for_frame

    def counted_active_sessions(archive: VehicleHistoryArchive) -> list[object]:
        nonlocal active_loads
        active_loads += 1
        return real_load_active_sessions(archive)

    def counted_registry(path: str | Path, *, strict: bool = False) -> object:
        nonlocal registry_loads
        registry_loads += 1
        return load_owner_vehicle_registry(path, strict=strict)

    def record_frame_provider(**kwargs: Any) -> object:
        frame_providers.append(kwargs["owner_vehicle_snapshot_provider"])
        return real_update_runtime_state(**kwargs)

    monkeypatch.setattr(VehicleHistoryArchive, "load_active_sessions", counted_active_sessions)
    monkeypatch.setattr(runtime_owner_vehicle_cache, "load_owner_vehicle_registry", counted_registry)
    monkeypatch.setattr(runtime_capture_loop, "_update_runtime_state_for_frame", record_frame_provider)

    delivery = FakeMatrixDelivery()

    class EmptyDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return []

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(data_dir)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(data_dir, timestamp="2026-05-18T20:05:06Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: EmptyDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    state_payload = runtime_state_payload(data_dir / "state.json")
    expected_event_id = "owner-vehicle-quiet-window-alert:right_spot:prof_tesla:street_sweeping:2026-05-18:13:00-15:00"

    assert exit_code == 0
    assert [alert["event_id"] for alert in delivery.owner_alerts] == [expected_event_id]
    assert delivery.owner_alerts[0]["owner_vehicle"]["label"] == "Keith's black Tesla"
    assert delivery.owner_alerts[0]["spot_id"] == "right_spot"
    assert state_payload["owner_quiet_window_alert_ids"] == [expected_event_id]
    assert active_loads == 1
    assert registry_loads == 1
    assert len(frame_providers) == 2
    assert frame_providers[0] is frame_providers[1]
    assert output.count("owner-vehicle-quiet-window-alert") >= 1
    assert_no_secret_leak(output)


def test_owner_vehicle_quiet_window_alerts_skip_unreadable_owner_registry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import parking_spot_monitor.runtime_vehicle_events as runtime_vehicle_events
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    class ActiveQuietStatus:
        active = True
        active_window_id = "street_sweeping:2026-05-18:13:00-15:00"

    class FailingSnapshotProvider:
        def snapshot(self, _archive: object) -> object:
            raise PermissionError(f"registry denied token={SECRET_MARKER} raw_image_bytes")

    alerts = runtime_vehicle_events._owner_vehicle_quiet_window_alerts(
        VehicleHistoryArchive(tmp_path, logger=StructuredLogger()),
        quiet_status=ActiveQuietStatus(),
        observed_at=datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
        emitted_alert_ids=set(),
        configured_spot_ids=("left_spot",),
        logger=StructuredLogger(),
        owner_vehicle_snapshot_provider=FailingSnapshotProvider(),  # type: ignore[arg-type]
    )

    output = combined_output(capsys)
    assert alerts == []
    assert '"event":"owner-vehicle-alert-scan-failed"' in output
    assert '"action":"load-owner-registry"' in output
    assert_no_secret_leak(output)



def test_runtime_loop_owner_vehicle_quiet_window_ignores_low_confidence_profile_match(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data_dir = tmp_path
    owner_profile_id = "prof_tesla"
    archive_root = data_dir / "vehicle-history"
    active_dir = archive_root / "sessions" / "active"
    active_dir.mkdir(parents=True)
    active_dir.joinpath("sess_low_confidence_left.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "session_id": "sess_low_confidence_left",
                "spot_id": "left_spot",
                "started_at": "2026-05-18T19:30:00+00:00",
                "ended_at": None,
                "duration_seconds": None,
                "start_event": {"event_type": "occupancy-state-changed"},
                "close_event": None,
                "source_snapshot_path": str(data_dir / "latest.jpg"),
                "candidate_summary": None,
                "occupied_snapshot_path": str(data_dir / "latest.jpg"),
                "occupied_crop_path": str(data_dir / "crop.jpg"),
                "profile_id": owner_profile_id,
                "profile_confidence": 0.90,
                "created_at": "2026-05-18T19:30:00Z",
                "updated_at": "2026-05-18T19:30:00Z",
            }
        ),
        encoding="utf-8",
    )
    archive_root.joinpath("owner-vehicles.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_vehicles": [
                    {
                        "profile_id": owner_profile_id,
                        "label": "Keith's black Tesla",
                        "description": "black Tesla, tinted windows, roof rack",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    delivery = FakeMatrixDelivery()

    class EmptyDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return []

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(data_dir)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(data_dir, timestamp="2026-05-18T20:05:06Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: EmptyDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    state_payload = runtime_state_payload(data_dir / "state.json")

    assert exit_code == 0
    assert delivery.owner_alerts == []
    assert state_payload["owner_quiet_window_alert_ids"] == []
    assert "owner-vehicle-quiet-window-alert" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_quiet_window_start_notice_sent_once_by_event_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T20:30:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 18, 20, 30, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert [notice["event_id"] for notice in delivery.quiet_notices] == [
        "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00"
    ]
    assert delivery.quiet_notices[0]["event_type"] == "quiet-window-started"
    assert delivery.open_alerts == []
    assert '"event":"matrix-delivery-succeeded"' in output
    assert '"event_type":"quiet-window-started"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_quiet_window_upcoming_notice_sent_once_by_event_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert [notice["event_id"] for notice in delivery.quiet_notices] == [
        "quiet-window-upcoming:street_sweeping:2026-05-18:13:00-15:00:60m"
    ]
    assert delivery.quiet_notices[0]["event_type"] == "quiet-window-upcoming"
    assert delivery.quiet_notices[0]["reminder_minutes_before"] == 60
    assert delivery.open_alerts == []
    assert '"event":"matrix-delivery-succeeded"' in output
    assert '"event_type":"quiet-window-upcoming"' in output
    assert_no_secret_leak(output)

def test_runtime_loop_matrix_quiet_window_end_notice_sent_once_by_event_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    window_id = "street_sweeping:2026-05-18:13:00-15:00"
    save_runtime_state(
        tmp_path / "state.json",
        RuntimeState.default(["left_spot", "right_spot"]).__class__(
            state_by_spot=RuntimeState.default(["left_spot", "right_spot"]).state_by_spot,
            active_quiet_window_ids=frozenset({window_id}),
            quiet_window_notice_ids=frozenset({f"quiet-window-started:{window_id}"}),
        ),
    )
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T22:30:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 18, 22, 30, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert [notice["event_id"] for notice in delivery.quiet_notices] == [f"quiet-window-ended:{window_id}"]
    assert delivery.quiet_notices[0]["event_type"] == "quiet-window-ended"
    assert delivery.open_alerts == []
    assert '"event":"matrix-delivery-succeeded"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_open_event_sends_text_and_raw_snapshot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    matrix_client = FakeMatrixClient()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    def matrix_factory(_settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixOutboxDelivery:
        return outbox_delivery(matrix_client, data_dir, logger)

    sleep_calls = 0

    def wait_for_open_delivery(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 6:
            assert matrix_client.wait_for_image_transaction("occupancy-open-event:")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=matrix_factory,
        sleep=wait_for_open_delivery,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    snapshot_files = list((tmp_path / "snapshots").glob("occupancy-open-event-left-spot-*.jpg"))
    assert exit_code == 0
    assert len(snapshot_files) == 1
    assert snapshot_files[0].read_bytes() == (tmp_path / "latest.jpg").read_bytes()
    open_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("occupancy-open-event:")]
    open_uploads = [upload for upload in matrix_client.uploads if upload["filename"].startswith("occupancy-open-event-")]
    open_images = [image for image in matrix_client.images if image["txn_id"].startswith("occupancy-open-event:")]
    assert len(open_texts) == 1
    assert open_texts[0]["txn_id"].endswith(":text")
    assert open_texts[0]["body"] == "Parking spot open: left_spot at 2026-05-18 12:00:00 PM PDT"
    assert len(open_uploads) == 1
    assert open_uploads[0]["content_type"] == "image/jpeg"
    assert open_uploads[0]["data"] == snapshot_files[0].read_bytes()
    closed_files = list((tmp_path / "vehicle-history" / "sessions" / "closed").glob("*.json"))
    active_files = list((tmp_path / "vehicle-history" / "sessions" / "active").glob("*.json"))
    assert len(open_images) == 1
    assert open_images[0]["txn_id"].endswith(":image")
    assert open_images[0]["body"].startswith("Raw full-frame snapshot for left_spot")
    assert open_images[0]["info"]["mimetype"] == "image/jpeg"
    assert active_files == []
    assert len(closed_files) == 1
    closed_payload = json.loads(closed_files[0].read_text(encoding="utf-8"))
    assert closed_payload["spot_id"] == "left_spot"
    assert closed_payload["close_event"]["event_type"] == "occupancy-state-changed"
    assert closed_payload["close_event"]["new_status"] == "empty"
    assert closed_payload["occupied_snapshot_path"] is not None
    assert closed_payload["occupied_crop_path"] is not None
    assert Path(closed_payload["occupied_snapshot_path"]).exists()
    assert Path(closed_payload["occupied_crop_path"]).exists()
    assert '"event":"matrix-outbox-snapshot-prepared"' in output
    assert '"event":"matrix-delivery-succeeded"' in output
    assert '"event":"vehicle-session-lifecycle-recorded"' in output
    assert '"action":"close"' in output
    assert_no_secret_leak(output)

def test_runtime_loop_occupied_alert_sends_text_image_with_seeded_vehicle_estimate(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor.vehicle_profiles import extract_vehicle_descriptor

    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]
    matrix_client = FakeMatrixClient()
    profile_id = "prof_civic"
    active_profiles_dir = tmp_path / "vehicle-history" / "profiles" / "active"
    active_profiles_dir.mkdir(parents=True)
    closed_dir = tmp_path / "vehicle-history" / "sessions" / "closed"
    closed_dir.mkdir(parents=True)
    exemplar = tmp_path / "seed-crop.jpg"
    Image.new("RGB", (200, 130), (20, 30, 40)).save(exemplar, format="JPEG")
    descriptor = extract_vehicle_descriptor(exemplar)
    active_profiles_dir.joinpath(f"{profile_id}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "profile_id": profile_id,
                "label": "Blue Civic",
                "status": "active",
                "descriptor": {
                    "width": descriptor.width,
                    "height": descriptor.height,
                    "aspect_ratio": descriptor.aspect_ratio,
                    "rgb_histogram": list(descriptor.rgb_histogram),
                    "average_hash": descriptor.average_hash,
                    "hash_bits": descriptor.hash_bits,
                },
                "sample_count": 3,
                "sample_session_ids": ["seed-a", "seed-b"],
                "exemplar_crop_path": exemplar.name,
                "created_at": "2026-05-18T18:00:00+00:00",
                "updated_at": "2026-05-18T18:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    for index, (duration, ended_at) in enumerate(
        [(3600, "2026-05-17T20:00:00+00:00"), (4200, "2026-05-16T20:10:00+00:00")],
        start=1,
    ):
        closed_dir.joinpath(f"seed-{index}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "session_id": f"seed-{index}",
                    "spot_id": "left_spot",
                    "started_at": "2026-05-16T19:00:00+00:00",
                    "ended_at": ended_at,
                    "duration_seconds": duration,
                    "start_event": {"event_type": "occupancy-state-changed"},
                    "close_event": {"event_type": "occupancy-state-changed"},
                    "source_snapshot_path": None,
                    "candidate_summary": None,
                    "occupied_snapshot_path": str(tmp_path / f"seed-full-{index}.jpg"),
                    "occupied_crop_path": str(tmp_path / f"seed-crop-{index}.jpg"),
                    "profile_id": profile_id,
                    "profile_confidence": 0.99,
                    "created_at": "2026-05-16T19:00:00+00:00",
                    "updated_at": "2026-05-16T20:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    def matrix_factory(_settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixOutboxDelivery:
        return outbox_delivery(matrix_client, data_dir, logger)

    sleep_calls = 0

    def wait_for_occupied_delivery(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 4:
            assert matrix_client.wait_for_image_transaction("occupancy-occupied-event:")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=matrix_factory,
        sleep=wait_for_occupied_delivery,
        max_iterations=4,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    active_files = list((tmp_path / "vehicle-history" / "sessions" / "active").glob("*.json"))
    active_payload = json.loads(active_files[0].read_text(encoding="utf-8"))
    snapshot_files = list((tmp_path / "snapshots").glob("occupancy-occupied-event-left-spot-*.jpg"))
    assert exit_code == 0
    assert len(matrix_client.texts) == 3
    assert len(matrix_client.uploads) == 1
    assert len(matrix_client.images) == 1
    assert len(snapshot_files) == 1
    assert snapshot_files[0].read_bytes() == Path(active_payload["occupied_snapshot_path"]).read_bytes()
    assert matrix_client.uploads[0]["data"] == Path(active_payload["occupied_snapshot_path"]).read_bytes()
    reminder_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("quiet-window-upcoming:")]
    occupied_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("occupancy-occupied-event:")]
    occupied_images = [image for image in matrix_client.images if image["txn_id"].startswith("occupancy-occupied-event:")]
    lifecycle_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("parking-monitor-started:")]
    assert len(lifecycle_texts) == 1
    assert len(reminder_texts) == 1
    assert reminder_texts[0]["body"] == "Street sweeping starts in 1 hour: street_sweeping:2026-05-18:13:00-15:00"
    assert len(occupied_images) == 1
    assert len(occupied_texts) == 1
    text_body = occupied_texts[0]["body"]
    assert "Likely vehicle: Blue Civic (profile prof_civic)" in text_body
    assert "Estimated dwell: 1 hr–1 hr 10 min (typical 1 hr 5 min)" in text_body
    assert "Usual leave window: 8:00 PM–8:15 PM" in text_body
    assert "History: 2 samples, estimate confidence low" in text_body
    assert active_payload["profile_id"] == profile_id
    assert active_payload["profile_confidence"] == pytest.approx(1.0)
    assert '"event_type":"occupancy-occupied-event"' in output
    assert '"estimate_status":"estimated"' in output
    assert "seed-crop" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_vehicle_history_final_integrated_regression_includes_retention_health_and_matrix_alerts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive
    from parking_spot_monitor.vehicle_profiles import extract_vehicle_descriptor

    source_profile_id = "prof_source"
    target_profile_id = "prof_target"
    history_root = tmp_path / "vehicle-history"
    active_profiles_dir = history_root / "profiles" / "active"
    closed_dir = history_root / "sessions" / "closed"
    active_profiles_dir.mkdir(parents=True)
    closed_dir.mkdir(parents=True)
    source_exemplar = tmp_path / "source-crop.jpg"
    target_exemplar = tmp_path / "target-crop.jpg"
    Image.new("RGB", (200, 130), (20, 30, 40)).save(source_exemplar, format="JPEG")
    Image.new("RGB", (200, 130), (180, 30, 40)).save(target_exemplar, format="JPEG")

    def write_profile(profile_id: str, label: str, exemplar: Path, sample_count: int) -> None:
        descriptor = extract_vehicle_descriptor(exemplar)
        active_profiles_dir.joinpath(f"{profile_id}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "profile_id": profile_id,
                    "label": label,
                    "status": "active",
                    "descriptor": {
                        "width": descriptor.width,
                        "height": descriptor.height,
                        "aspect_ratio": descriptor.aspect_ratio,
                        "rgb_histogram": list(descriptor.rgb_histogram),
                        "average_hash": descriptor.average_hash,
                        "hash_bits": descriptor.hash_bits,
                    },
                    "sample_count": sample_count,
                    "sample_session_ids": [f"{profile_id}-seed"],
                    "exemplar_crop_path": exemplar.name,
                    "created_at": "2026-05-18T18:00:00+00:00",
                    "updated_at": "2026-05-18T18:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )

    write_profile(source_profile_id, "Uncorrected source", source_exemplar, 3)
    write_profile(target_profile_id, "Uncorrected target", target_exemplar, 1)
    for index, duration in enumerate([3600, 4200], start=1):
        closed_dir.joinpath(f"seed-{index}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "session_id": f"seed-{index}",
                    "spot_id": "left_spot",
                    "started_at": f"2026-05-1{index}T19:00:00+00:00",
                    "ended_at": f"2026-05-1{index}T20:0{index}:00+00:00",
                    "duration_seconds": duration,
                    "start_event": {"event_type": "occupancy-state-changed"},
                    "close_event": {"event_type": "occupancy-state-changed"},
                    "source_snapshot_path": None,
                    "candidate_summary": None,
                    "occupied_snapshot_path": str(tmp_path / f"seed-full-{index}.jpg"),
                    "occupied_crop_path": str(tmp_path / f"seed-crop-{index}.jpg"),
                    "profile_id": source_profile_id,
                    "profile_confidence": 0.99,
                    "created_at": "2026-05-16T19:00:00+00:00",
                    "updated_at": "2026-05-16T20:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )

    archive = VehicleHistoryArchive(history_root, logger=StructuredLogger())
    export_result = archive.export_archive(tmp_path / "vehicle-history-export.tar.gz")
    prune_result = archive.prune_closed_sessions(older_than="2026-05-15T00:00:00Z", dry_run=True)

    detections = [
        [left_spot_vehicle()],
        [left_spot_vehicle()],
        [left_spot_vehicle()],
        [left_spot_vehicle()],
        [],
        [],
        [],
    ]
    matrix_client = FakeMatrixClient()

    class MergeRenameCommandService:
        def __init__(self, runtime_archive: Any) -> None:
            self.archive = runtime_archive
            self.applied = False

        def poll_once(self) -> FakeCommandPollResult:
            if not self.applied:
                self.archive.merge_profiles(
                    source_profile_id,
                    target_profile_id,
                    matrix_event_id="$merge",
                    matrix_sender="@op:example",
                    matrix_room_id="!parking-room:example.org",
                )
                self.archive.rename_profile(
                    target_profile_id,
                    "Corrected Fleet",
                    matrix_event_id="$rename",
                    matrix_sender="@op:example",
                    matrix_room_id="!parking-room:example.org",
                )
                self.archive.write_matrix_cursor({"next_batch": "s1"})
                self.applied = True
                return FakeCommandPollResult(processed_count=2)
            return FakeCommandPollResult()

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    def matrix_factory(_settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixOutboxDelivery:
        return outbox_delivery(matrix_client, data_dir, logger)

    sleep_calls = 0

    def wait_for_integrated_delivery(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 7:
            assert matrix_client.wait_for_image_transaction("occupancy-open-event:")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=matrix_factory,
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, runtime_archive: MergeRenameCommandService(runtime_archive),
        sleep=wait_for_integrated_delivery,
        max_iterations=7,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    active_files = list((history_root / "sessions" / "active").glob("*.json"))
    closed_files = sorted((history_root / "sessions" / "closed").glob("*.json"))
    current_closed = [path for path in closed_files if not path.stem.startswith("seed-")]
    health = health_payload(tmp_path / "health.json")
    vehicle_health = health["vehicle_history"]
    open_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("occupancy-open-event:")]
    occupied_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("occupancy-occupied-event:")]
    occupied_uploads = [upload for upload in matrix_client.uploads if upload["filename"].startswith("occupancy-occupied-event-")]
    open_uploads = [upload for upload in matrix_client.uploads if upload["filename"].startswith("occupancy-open-event-")]
    occupied_images = [image for image in matrix_client.images if image["txn_id"].startswith("occupancy-occupied-event:")]
    open_images = [image for image in matrix_client.images if image["txn_id"].startswith("occupancy-open-event:")]

    assert exit_code == 0
    assert active_files == []
    assert len(closed_files) == 3
    assert len(current_closed) == 1
    current_payload = json.loads(current_closed[0].read_text(encoding="utf-8"))
    assert current_payload["spot_id"] == "left_spot"
    assert current_payload["close_event"]["new_status"] == "empty"
    assert current_payload["occupied_snapshot_path"] is not None
    assert current_payload["occupied_crop_path"] is not None
    assert Path(current_payload["occupied_snapshot_path"]).exists()
    assert Path(current_payload["occupied_crop_path"]).exists()
    assert current_payload["profile_id"] == source_profile_id
    assert current_payload["profile_confidence"] == pytest.approx(1.0)

    assert len(open_texts) == 1
    assert len(occupied_uploads) == 1
    assert len(open_uploads) == 1
    assert len(occupied_images) == 1
    assert len(open_images) == 1
    assert len(occupied_texts) == 1
    occupied_body = occupied_texts[0]["body"]
    assert "Likely vehicle: Corrected Fleet (profile prof_source)" in occupied_body
    assert "Estimated dwell: 1 hr–1 hr 10 min (typical 1 hr 5 min)" in occupied_body
    assert "History: 2 samples, estimate confidence low" in occupied_body
    assert open_texts[0]["body"] == "Parking spot open: left_spot at 2026-05-18 12:00:00 PM PDT"
    assert occupied_uploads[0]["data"] == Path(current_payload["occupied_snapshot_path"]).read_bytes()
    assert open_uploads[0]["data"] == (tmp_path / "snapshots" / open_uploads[0]["filename"]).read_bytes()

    assert health["status"] == "ok"
    assert vehicle_health["retention_policy"] == "indefinite"
    assert vehicle_health["management_capabilities"] == ["export", "prune"]
    assert vehicle_health["oldest_retained_session_started_at"] == "2026-05-11T19:00:00+00:00"
    assert vehicle_health["archive_file_count"] > 0
    assert vehicle_health["archive_bytes"] > 0
    assert vehicle_health["last_maintenance_metadata"]["operation"] == "prune"
    assert vehicle_health["last_maintenance_metadata"]["status"] == "dry_run"
    assert vehicle_health["last_maintenance_metadata"]["retention_policy"] == "indefinite"
    assert vehicle_health["closed_session_count"] == 3
    assert vehicle_health["active_session_count"] == 0
    assert vehicle_health["occupied_snapshot_count"] == 1
    assert vehicle_health["occupied_crop_count"] == 1
    assert vehicle_health["image_file_count"] == 2
    assert vehicle_health["missing_occupied_image_reference_count"] == 0
    assert vehicle_health["profile_count"] == 2
    assert vehicle_health["profile_sample_count"] == 5
    assert vehicle_health["profile_unknown_session_count"] == 0
    assert vehicle_health["correction_count"] == 2
    assert vehicle_health["correction_invalid_count"] == 0
    assert vehicle_health["last_correction_action"] == "rename_profile"
    assert vehicle_health["matrix_command_cursor_present"] is True
    assert vehicle_health["vehicle_history_failure_count"] == 0
    assert vehicle_health["last_vehicle_history_error"] is None
    assert export_result.status == "ok"
    assert prune_result.status == "dry_run"
    assert "seed-crop" not in output
    assert "raw_image_bytes" not in json.dumps(health)
    assert "matrix-command-poll-succeeded" in output
    assert_no_secret_leak(output)



def test_runtime_loop_matrix_command_merge_and_rename_affect_later_occupied_alert(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor.vehicle_profiles import extract_vehicle_descriptor

    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]
    delivery = FakeMatrixDelivery()
    source_profile_id = "prof_source"
    target_profile_id = "prof_target"
    active_profiles_dir = tmp_path / "vehicle-history" / "profiles" / "active"
    closed_dir = tmp_path / "vehicle-history" / "sessions" / "closed"
    active_profiles_dir.mkdir(parents=True)
    closed_dir.mkdir(parents=True)
    source_exemplar = tmp_path / "source-crop.jpg"
    target_exemplar = tmp_path / "target-crop.jpg"
    Image.new("RGB", (200, 130), (20, 30, 40)).save(source_exemplar, format="JPEG")
    Image.new("RGB", (200, 130), (180, 30, 40)).save(target_exemplar, format="JPEG")

    def write_profile(profile_id: str, label: str, exemplar: Path, sample_count: int) -> None:
        descriptor = extract_vehicle_descriptor(exemplar)
        active_profiles_dir.joinpath(f"{profile_id}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "profile_id": profile_id,
                    "label": label,
                    "status": "active",
                    "descriptor": {
                        "width": descriptor.width,
                        "height": descriptor.height,
                        "aspect_ratio": descriptor.aspect_ratio,
                        "rgb_histogram": list(descriptor.rgb_histogram),
                        "average_hash": descriptor.average_hash,
                        "hash_bits": descriptor.hash_bits,
                    },
                    "sample_count": sample_count,
                    "sample_session_ids": [f"{profile_id}-seed"],
                    "exemplar_crop_path": exemplar.name,
                    "created_at": "2026-05-18T18:00:00+00:00",
                    "updated_at": "2026-05-18T18:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )

    write_profile(source_profile_id, "Old source", source_exemplar, 3)
    write_profile(target_profile_id, "Old target", target_exemplar, 1)
    for index, duration in enumerate([3600, 4200], start=1):
        closed_dir.joinpath(f"seed-{index}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "session_id": f"seed-{index}",
                    "spot_id": "left_spot",
                    "started_at": "2026-05-16T19:00:00+00:00",
                    "ended_at": f"2026-05-16T20:0{index}:00+00:00",
                    "duration_seconds": duration,
                    "start_event": {"event_type": "occupancy-state-changed"},
                    "close_event": {"event_type": "occupancy-state-changed"},
                    "source_snapshot_path": None,
                    "candidate_summary": None,
                    "occupied_snapshot_path": str(tmp_path / f"seed-full-{index}.jpg"),
                    "occupied_crop_path": str(tmp_path / f"seed-crop-{index}.jpg"),
                    "profile_id": source_profile_id,
                    "profile_confidence": 0.99,
                    "created_at": "2026-05-16T19:00:00+00:00",
                    "updated_at": "2026-05-16T20:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )

    class MergeRenameCommandService:
        def __init__(self, archive: Any) -> None:
            self.archive = archive
            self.applied = False

        def poll_once(self) -> FakeCommandPollResult:
            if not self.applied:
                self.archive.merge_profiles(source_profile_id, target_profile_id, matrix_event_id="$merge", matrix_sender="@op:example", matrix_room_id="!parking-room:example.org")
                self.archive.rename_profile(target_profile_id, "Corrected Fleet", matrix_event_id="$rename", matrix_sender="@op:example", matrix_room_id="!parking-room:example.org")
                self.archive.write_matrix_cursor({"next_batch": "s1"})
                self.applied = True
                return FakeCommandPollResult(processed_count=2)
            return FakeCommandPollResult()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, archive: MergeRenameCommandService(archive),
        sleep=lambda _seconds: None,
        max_iterations=4,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert len(delivery.occupied_alerts) == 1
    alert = delivery.occupied_alerts[0]
    assert alert["profile_id"] == source_profile_id
    assert alert["profile_label"] == "Corrected Fleet"
    assert alert["likely_vehicle"]["label"] == "Corrected Fleet"
    assert alert["vehicle_history_estimate"]["status"] == "estimated"
    assert alert["vehicle_history_estimate"]["profile_id"] == target_profile_id
    assert alert["vehicle_history_estimate"]["sample_count"] == 2
    health = health_payload(tmp_path / "health.json")
    assert health["vehicle_history"]["correction_count"] == 2
    assert health["vehicle_history"]["last_correction_action"] == "rename_profile"
    assert health["vehicle_history"]["matrix_command_cursor_present"] is True
    assert '"event":"matrix-command-poll-succeeded"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_command_failure_is_non_blocking_and_redacted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], []]
    delivery = FakeMatrixDelivery()

    class FailingCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            raise RuntimeError(f"sync failed token={SECRET_MARKER} rtsp://camera.local/raw_image_bytes")

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, archive: FailingCommandService(),
        sleep=lambda _seconds: None,
        max_iterations=4,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert len(delivery.occupied_alerts) == 1
    assert health["status"] == "degraded"
    assert health["vehicle_history_failure_count"] == 0
    assert health["last_vehicle_history_error"] is None
    assert health["matrix_command_failure_count"] == 1
    assert health["last_matrix_command_error"]["phase"] == "matrix-command"
    assert health["last_matrix_command_error"]["action"] == "matrix-command"
    assert '"event":"matrix-command-poll-failed"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_command_result_errors_degrade_health(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    delivery = FakeMatrixDelivery()

    class CommandServiceWithResultErrors:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult(processed_count=1, error_count=2)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, _archive: CommandServiceWithResultErrors(),
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert health["status"] == "degraded"
    assert health["matrix_command_failure_count"] == 2
    assert health["last_matrix_command_error"] == {
        "phase": "matrix-command",
        "action": "matrix-command",
        "iteration": 1,
        "error_type": "poll_result_errors",
        "message": "matrix command poll completed with command errors",
        "error_count": 2,
        "processed_count": 1,
    }
    assert '"event":"matrix-command-poll-degraded"' in output
    assert_no_secret_leak(output)


@pytest.mark.parametrize(
    ("sender", "body"),
    [
        ("@intruder:example.org", "!parking status"),
        ("@operator:example.org", "!parking status extra"),
    ],
    ids=["unauthorized", "malformed"],
)
def test_runtime_loop_command_event_errors_do_not_suppress_next_healthy_poll(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    sender: str,
    body: str,
) -> None:
    from parking_spot_monitor.matrix import (
        MatrixCommandService,
        MatrixSyncResult,
        MatrixTextEvent,
    )

    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "matrix": settings.matrix.model_copy(
                update={"command_poll_interval_seconds": 0}
            ),
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "adaptive_polling_enabled": False,
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )
    sync_calls = 0

    class Client:
        def sync(self, **_kwargs: Any) -> MatrixSyncResult:
            nonlocal sync_calls
            sync_calls += 1
            events = (
                MatrixTextEvent(
                    event_id="$rejected",
                    sender=sender,
                    room_id=settings.matrix.room_id,
                    body=body,
                ),
            ) if sync_calls == 1 else ()
            return MatrixSyncResult(next_batch=f"s{sync_calls + 1}", events=events)

        def send_text(self, **_kwargs: Any) -> str:
            return "$rejection"

    class CursorArchive:
        def __init__(self) -> None:
            self.cursor = {"next_batch": "s1"}

        def read_matrix_cursor(self) -> dict[str, str]:
            return dict(self.cursor)

        def write_matrix_cursor(self, state: dict[str, str]) -> None:
            self.cursor = dict(state)

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=CursorArchive(),  # type: ignore[arg-type]
        room_id=settings.matrix.room_id,
        authorized_senders=["@operator:example.org"],
        who_snapshot_provider=lambda base_reply: base_reply,
        unauthorized_reply_cooldown_seconds=0,
    )

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T18:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        matrix_command_service=service,
        sleep=lambda _seconds: None,
        max_iterations=2,
        monotonic=lambda: 0.0,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert sync_calls == 2
    assert output.count('"event":"matrix-command-poll-degraded"') == 1



def test_runtime_loop_vehicle_history_close_failure_degrades_health_without_blocking_open_alert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli
    from parking_spot_monitor.vehicle_history_models import ProfileAssignment

    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery()

    class FailingCloseHistoryArchive:
        def __init__(self, _root: Path, *, logger: StructuredLogger | None = None) -> None:
            self.logger = logger

        def health_snapshot(self) -> dict[str, Any]:
            return {"archive_status": "test-double"}

        def mutation_revision(self) -> int:
            return 0

        def start_session(self, event: object) -> object:
            return type("SessionRecord", (), {"session_id": "session-left"})()

        def attach_occupied_images(self, **_kwargs: object) -> object:
            return type(
                "SessionRecord",
                (),
                {"session_id": "session-left", "occupied_snapshot_path": "/safe/full.jpg", "occupied_crop_path": "/safe/crop.jpg"},
            )()

        def match_or_create_profile(self, *, session_id: str) -> object:
            return ProfileAssignment(session_id=session_id, status="matched", profile_id="prof-left", profile_confidence=0.98, reason="test-match")

        def close_session(self, event: object) -> object:
            raise PermissionError(f"history close denied token={SECRET_MARKER} raw_image_bytes abc")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    monkeypatch.setattr(cli, "VehicleHistoryArchive", FailingCloseHistoryArchive)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    state = runtime_state_payload(tmp_path / "state.json")
    assert exit_code == 0
    assert len(delivery.open_alerts) == 1
    assert state["spots"]["left_spot"]["status"] == "empty"
    assert health["status"] == "degraded"
    assert health["vehicle_history_failure_count"] == 1
    assert health["last_vehicle_history_error"]["phase"] == "vehicle-history"
    assert health["last_vehicle_history_error"]["action"] == "close"
    assert health["last_vehicle_history_error"]["spot_id"] == "left_spot"
    assert health["last_vehicle_history_error"]["error_type"] == "PermissionError"
    assert '"event":"vehicle-history-record-failed"' in output
    assert '"event":"state-saved"' in output
    assert '"event":"matrix-delivery-succeeded"' in output
    assert "raw_image_bytes abc" not in output
    assert SECRET_MARKER not in json.dumps(health)
    assert_no_secret_leak(output)



def test_runtime_loop_vehicle_history_image_capture_failure_degrades_health_without_blocking_open_alert(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery()
    capture_calls = 0

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        nonlocal capture_calls
        capture_calls += 1
        latest_path = tmp_path / "latest.jpg"
        if capture_calls <= 4:
            latest_path.write_bytes(b"not a jpeg raw_image_bytes token=should-not-leak")
        else:
            Image.new("RGB", (1458, 806), (40, 30, 20)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T19:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    closed_files = list((tmp_path / "vehicle-history" / "sessions" / "closed").glob("*.json"))
    closed_payload = json.loads(closed_files[0].read_text(encoding="utf-8"))
    assert exit_code == 0
    assert len(delivery.open_alerts) == 1
    assert runtime_state_payload(tmp_path / "state.json")["spots"]["left_spot"]["status"] == "empty"
    assert closed_payload["occupied_snapshot_path"] is None
    assert closed_payload["occupied_crop_path"] is None
    assert health["status"] == "ok"
    assert health["vehicle_history_failure_count"] == 0
    assert health["last_vehicle_history_error"] is None
    assert health["vehicle_history"]["vehicle_history_failure_count"] == 1
    assert health["vehicle_history"]["last_vehicle_history_error"]["phase"] == "image-capture"
    assert health["vehicle_history"]["last_vehicle_history_error"]["session_id"] == closed_payload["session_id"]
    assert health["vehicle_history"]["missing_occupied_image_reference_count"] == 1
    assert '"event":"vehicle-session-images-failed"' in output
    assert '"event":"vehicle-history-record-failed"' in output
    assert '"event":"state-saved"' in output
    assert '"event":"matrix-delivery-succeeded"' in output or len(delivery.open_alerts) == 1
    assert "raw_image_bytes" not in json.dumps(health)
    assert "should-not-leak" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_vehicle_history_profile_failure_degrades_health_after_recording_start_and_images(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]
    delivery = FakeMatrixDelivery()

    class FailingProfileHistoryArchive:
        def __init__(self, _root: Path, *, logger: StructuredLogger | None = None) -> None:
            self.logger = logger

        def health_snapshot(self) -> dict[str, Any]:
            return {"archive_status": "test-double"}

        def mutation_revision(self) -> int:
            return 0

        def start_session(self, event: object) -> object:
            return type("SessionRecord", (), {"session_id": "session-left"})()

        def attach_occupied_images(self, **_kwargs: object) -> object:
            return type(
                "SessionRecord",
                (),
                {"session_id": "session-left", "occupied_snapshot_path": "/safe/full.jpg", "occupied_crop_path": "/safe/crop.jpg"},
            )()

        def match_or_create_profile(self, *, session_id: str) -> object:
            assert session_id == "session-left"
            raise RuntimeError(f"profile failed token={SECRET_MARKER} raw_image_bytes abc")

        def close_session(self, event: object) -> None:
            return None

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    monkeypatch.setattr(cli, "VehicleHistoryArchive", FailingProfileHistoryArchive)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert delivery.open_alerts == []
    assert len(delivery.occupied_alerts) == 1
    assert delivery.occupied_alerts[0]["likely_vehicle"]["label"] == "unknown vehicle"
    assert delivery.occupied_alerts[0]["vehicle_history_estimate"]["status"] == "insufficient_history"
    assert runtime_state_payload(tmp_path / "state.json")["spots"]["left_spot"]["status"] == "occupied"
    assert health["status"] == "degraded"
    assert health["vehicle_history_failure_count"] == 1
    assert health["last_vehicle_history_error"]["phase"] == "vehicle-history"
    assert health["last_vehicle_history_error"]["action"] == "match-profile"
    assert health["last_vehicle_history_error"]["profile_phase"] == "profile-match"
    assert health["last_vehicle_history_error"]["spot_id"] == "left_spot"
    assert health["last_vehicle_history_error"]["error_type"] == "RuntimeError"
    assert '"event":"vehicle-session-lifecycle-recorded"' in output
    assert '"event":"vehicle-session-images-attached"' in output
    assert '"event":"vehicle-history-record-failed"' in output
    assert '"action":"match-profile"' in output
    assert '"event":"matrix-delivery-skipped"' in output
    assert "raw_image_bytes abc" not in output
    assert SECRET_MARKER not in json.dumps(health)
    assert_no_secret_leak(output)


def test_runtime_loop_capture_failure_remains_down_with_prior_vehicle_history_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    calls = 0

    class FailingStartHistoryArchive:
        def __init__(self, _root: Path, *, logger: StructuredLogger | None = None) -> None:
            self.logger = logger

        def health_snapshot(self) -> dict[str, Any]:
            return {"archive_status": "test-double"}

        def mutation_revision(self) -> int:
            return 0

        def start_session(self, event: object) -> object:
            raise RuntimeError(f"history start denied token={SECRET_MARKER}")

        def attach_occupied_images(self, **_kwargs: object) -> object:
            raise AssertionError("images are not attached when start_session fails")

        def match_or_create_profile(self, *, session_id: str) -> object:
            raise AssertionError("profiles are not matched when start_session fails")

        def close_session(self, event: object) -> None:
            return None

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return [left_spot_vehicle()]

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        nonlocal calls
        calls += 1
        if calls <= 3:
            return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")
        raise CaptureError(
            reason="ffmpeg-timeout",
            mode=DecodeMode.SOFTWARE,
            output_path=tmp_path / "latest.jpg",
            message=f"capture failed token={SECRET_MARKER}",
        )

    monkeypatch.setattr(cli, "VehicleHistoryArchive", FailingStartHistoryArchive)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        sleep=lambda _seconds: None,
        max_iterations=4,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert health["status"] == "down"
    assert health["consecutive_capture_failures"] == 1
    assert health["vehicle_history_failure_count"] == 1
    assert health["last_vehicle_history_error"]["action"] == "start"
    assert health["last_error"]["phase"] == "capture"
    assert SECRET_MARKER not in json.dumps(health)
    assert_no_secret_leak(output)

def test_verify_live_proof_skip_markers_are_explicit_for_absent_dependencies(tmp_path: Path) -> None:
    from scripts.verify_live_proof import (
        SKIPPED_CONFIG_ABSENT,
        SKIPPED_MATRIX_ENV_ABSENT,
        SKIPPED_RTSP_ENV_ABSENT,
        skip_markers,
    )

    assert skip_markers(config_path=tmp_path / "missing.yaml", environ={}) == [SKIPPED_CONFIG_ABSENT]

    config_path = tmp_path / "config.yaml"
    config_path.write_text("stream: {}\n", encoding="utf-8")
    assert skip_markers(config_path=config_path, environ={}) == [SKIPPED_RTSP_ENV_ABSENT, SKIPPED_MATRIX_ENV_ABSENT]
    assert skip_markers(config_path=config_path, environ={"RTSP_URL": "rtsp://example"}) == [SKIPPED_MATRIX_ENV_ABSENT]
    assert skip_markers(config_path=config_path, environ={"MATRIX_ACCESS_TOKEN": "token"}) == [SKIPPED_RTSP_ENV_ABSENT]



def test_live_proof_once_captures_raw_frame_and_sends_labelled_matrix_evidence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--live-proof-once"],
        environ=fake_environ(),
        capture=fake_capture,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert len(delivery.live_proofs) == 1
    assert delivery.live_proofs[0]["latest_path"] == tmp_path / "latest.jpg"
    assert delivery.live_proofs[0]["observed_at"] == "2026-05-18T19:00:00Z"
    assert (tmp_path / "latest.jpg").exists()
    assert '"event":"live-proof-started"' in output
    assert '"event":"live-proof-capture-ok"' in output
    assert '"event":"live-proof-matrix-text-ok"' in output
    assert '"event":"live-proof-matrix-image-ok"' in output
    assert '"event":"detection-frame-processed"' not in output
    assert '"event":"matrix-delivery-succeeded"' not in output
    assert_no_secret_leak(output)


def test_live_proof_once_capture_failure_returns_safe_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        raise CaptureError(
            reason="ffmpeg-timeout",
            mode=DecodeMode.SOFTWARE,
            output_path=Path(data_dir) / "latest.jpg",
            message=f"timed out rtsp://user:pass@camera token={SECRET_MARKER}",
            stderr_tail=f"stderr token={SECRET_MARKER}",
            timeout_seconds=15.0,
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--live-proof-once"],
        environ=fake_environ(),
        capture=fake_capture,
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"live-proof-capture-failed"' in output
    assert '"marker":"LIVE_RTSP_CAPTURE_FAILED"' in output
    assert '"event":"live-proof-matrix-text-ok"' not in output
    assert "user:pass" not in output
    assert_no_secret_leak(output)


def test_live_proof_once_matrix_text_failure_returns_safe_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    class TextFailDelivery:
        def send_live_proof_text(self, *, observed_at: object, selected_mode: object) -> None:
            raise RuntimeError(f"text failed token={SECRET_MARKER}")

        def send_live_proof_image(self, *, latest_path: Path, observed_at: object, selected_mode: object) -> None:
            raise AssertionError("image must not be sent after text failure")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--live-proof-once"],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: TextFailDelivery(),
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"live-proof-matrix-text-failed"' in output
    assert '"marker":"LIVE_MATRIX_TEXT_FAILED"' in output
    assert '"event":"live-proof-matrix-image-ok"' not in output
    assert_no_secret_leak(output)


def test_live_proof_once_matrix_image_failure_returns_safe_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    class ImageFailDelivery:
        def send_live_proof_text(self, *, observed_at: object, selected_mode: object) -> None:
            return None

        def send_live_proof_image(self, *, latest_path: Path, observed_at: object, selected_mode: object) -> None:
            raise RuntimeError(f"image failed token={SECRET_MARKER}")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--live-proof-once"],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: ImageFailDelivery(),
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"live-proof-matrix-text-ok"' in output
    assert '"event":"live-proof-matrix-image-failed"' in output
    assert '"marker":"LIVE_MATRIX_IMAGE_FAILED"' in output
    assert_no_secret_leak(output)



def test_validate_config_does_not_construct_matrix_delivery(capsys: pytest.CaptureFixture[str]) -> None:
    def fail_matrix_factory(_settings: object, _data_dir: Path, _logger: StructuredLogger) -> object:
        raise AssertionError("validate-config must not construct Matrix delivery")

    exit_code = _main(
        ["--config", "config.yaml.example", "--validate-config"],
        environ=fake_environ(),
        matrix_delivery_factory=fail_matrix_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"mode":"validate-config"' in output
    assert '"event":"matrix-delivery-attempt"' not in output
    assert_no_secret_leak(output)



def test_runtime_loop_matrix_suppressed_open_event_sends_no_open_alert(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T20:30:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 20, 30, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert [notice["event_type"] for notice in delivery.quiet_notices] == ["quiet-window-started"]
    assert delivery.open_alerts == []
    assert '"event":"matrix-delivery-skipped"' in output
    assert '"event_type":"occupancy-open-suppressed"' in output
    assert '"reason":"suppressed"' in output
    assert_no_secret_leak(output)



def test_runtime_loop_matrix_delivery_failure_logs_and_continues(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery(fail=True)

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert len(delivery.open_alerts) == 1
    assert state_status(tmp_path / "state.json", "left_spot") == "empty"
    assert '"event":"matrix-delivery-failed"' in output
    assert '"event_type":"occupancy-open-event"' in output
    assert '"error_type":"RuntimeError"' in output
    assert '"event":"state-saved"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)



def test_validate_config_success_emits_effective_runtime_paths_without_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--config", "config.yaml.example", "--data-dir", "/data", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    records = json_records(output)
    loaded = next(record for record in records if record.get("event") == "startup-config-loaded")

    assert exit_code == 0
    assert loaded["config"]["storage"]["data_dir"] == "/data"
    assert loaded["config"]["storage"]["state_file"] == "/data/state.json"
    assert loaded["config"]["storage"]["latest_frame"] == "/data/latest.jpg"
    assert loaded["config"]["storage"]["snapshots_dir"] == "/data/snapshots"
    assert loaded["config"]["runtime"]["health_file"] == "/data/health.json"
    assert loaded["config"]["runtime"]["frame_interval_seconds"] == 30
    assert_no_secret_leak(output)


def test_runtime_prepares_ultralytics_config_before_detector_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_modules = tmp_path / "fake-modules"
    fake_modules.mkdir()
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    data_dir = tmp_path / "data"
    yolo_config_dir = data_dir / "ultralytics"
    (fake_modules / "ultralytics.py").write_text(
        """\
import os
from pathlib import Path

config_dir = Path(os.environ.get("YOLO_CONFIG_DIR", Path.home() / ".config" / "Ultralytics"))
config_dir.mkdir(parents=True, exist_ok=True)
(config_dir / "settings.json").write_text("{}", encoding="utf-8")
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(fake_modules))
    monkeypatch.delitem(sys.modules, "ultralytics", raising=False)
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("YOLO_CONFIG_DIR", "test-unset-sentinel")
    monkeypatch.delenv("YOLO_CONFIG_DIR", raising=False)

    class ImportingDetector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            return []

    def detector_factory(_settings: object) -> ImportingDetector:
        assert yolo_config_dir.is_dir()
        assert (yolo_config_dir.stat().st_mode & 0o777) == 0o750
        __import__("ultralytics")
        return ImportingDetector()

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(data_dir)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    assert exit_code == 0
    assert os.environ["YOLO_CONFIG_DIR"] == str(yolo_config_dir)
    assert (yolo_config_dir / "settings.json").is_file()
    assert not (fake_home / ".config" / "Ultralytics" / "settings.json").exists()
    assert_no_secret_leak(combined_output(capsys))


def test_repeated_runtime_main_replaces_its_managed_ultralytics_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_modules = tmp_path / "fake-modules"
    fake_modules.mkdir()
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    (fake_modules / "ultralytics.py").write_text(
        """\
import os
from pathlib import Path

config_dir = Path(os.environ.get("YOLO_CONFIG_DIR", Path.home() / ".config" / "Ultralytics"))
config_dir.mkdir(parents=True, exist_ok=True)
(config_dir / "settings.json").write_text("{}", encoding="utf-8")
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(fake_modules))
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("YOLO_CONFIG_DIR", "test-unset-sentinel")
    monkeypatch.delenv("YOLO_CONFIG_DIR")
    for key, value in fake_environ().items():
        monkeypatch.setenv(key, value)

    class ImportingDetector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            return []

    def detector_factory(_settings: object) -> ImportingDetector:
        monkeypatch.delitem(sys.modules, "ultralytics", raising=False)
        __import__("ultralytics")
        return ImportingDetector()

    data_dirs = [tmp_path / "runtime-a", tmp_path / "runtime-b"]
    exit_codes = [
        _main(
            ["--config", "config.yaml.example", "--data-dir", str(data_dir)],
            capture=lambda _settings, actual_data_dir, **_kwargs: captured_frame(
                Path(actual_data_dir)
            ),
            overlay=noop_overlay,
            detector_factory=detector_factory,
            matrix_delivery_factory=lambda *_args: FakeMatrixDelivery(),
            sleep=lambda _seconds: None,
            max_iterations=1,
        )
        for data_dir in data_dirs
    ]

    assert exit_codes == [0, 0]
    assert os.environ["YOLO_CONFIG_DIR"] == str(data_dirs[1] / "ultralytics")
    assert all(
        (data_dir / "ultralytics" / "settings.json").is_file()
        for data_dir in data_dirs
    )
    assert not (fake_home / ".config" / "Ultralytics" / "settings.json").exists()
    assert_no_secret_leak(combined_output(capsys))


def test_explicit_operator_ultralytics_path_must_match_runtime_data_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor.paths import prepare_ultralytics_config_dir

    data_dir = tmp_path / "runtime"
    operator_path = tmp_path / "operator-selected" / "ultralytics"
    monkeypatch.setenv("YOLO_CONFIG_DIR", str(operator_path))

    with pytest.raises(ValueError, match="must be the ultralytics directory"):
        prepare_ultralytics_config_dir(data_dir)

    assert os.environ["YOLO_CONFIG_DIR"] == str(operator_path)
    assert not (data_dir / "ultralytics").exists()


def test_runtime_loop_startup_prunes_existing_event_snapshots_without_touching_runtime_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    old = snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    newest = snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    latest = snapshots / "latest.jpg"
    state_file = tmp_path / "state.json"
    health_file = tmp_path / "health.json"
    for path in [old, newest, latest, state_file, health_file]:
        path.write_bytes(b"runtime-artifact")
    (tmp_path / "matrix-outbox.json").write_text(
        json.dumps({"schema_version": 1, "items": []}),
        encoding="utf-8",
    )

    base = Path("config.yaml.example").read_text(encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(base.replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"), encoding="utf-8")

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert not old.exists()
    assert newest.exists()
    assert latest.exists()
    assert state_file.exists()
    assert health_file.exists()
    assert '"event":"snapshot-retention-pruned"' in output
    assert '"trigger":"startup"' in output
    assert_no_secret_leak(output)


def test_runtime_startup_recovers_manifested_vehicle_image_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    image_dir = tmp_path / "vehicle-history" / "images" / "occupied-full"
    image_dir.mkdir(parents=True)
    target = image_dir / "pending.jpg"
    target.write_bytes(b"pending")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    real_unlink = owned_file_disposal.os.unlink

    def interrupt_disposal(name: object, *args: object, **kwargs: object) -> None:
        if str(name).endswith(".dispose"):
            raise OSError("simulated crash")
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", interrupt_disposal)
    assert file_descriptor_binding.unlink_owned_path(target, identity) is False
    monkeypatch.setattr(owned_file_disposal.os, "unlink", real_unlink)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(Path("config.yaml.example").read_text(encoding="utf-8"), encoding="utf-8")

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir)),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    assert exit_code == 0
    assert target.read_bytes() == b"pending"


@pytest.mark.parametrize(
    "outbox_payload",
    [
        f'{{"items": [token={SECRET_MARKER} raw_image_bytes abc',
        json.dumps(
            {
                "schema_version": 999,
                "items": [],
                "unsafe": f"token={SECRET_MARKER} raw_image_bytes abc",
            }
        ),
    ],
    ids=["invalid-json", "unsupported-schema"],
)
def test_runtime_loop_startup_retention_skips_pruning_after_whole_outbox_quarantine(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    outbox_payload: str,
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    old = snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    newest = snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    old.write_bytes(b"old")
    newest.write_bytes(b"new")
    (tmp_path / "matrix-outbox.json").write_text(outbox_payload, encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"),
        encoding="utf-8",
    )

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T19:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    rendered = output + json.dumps(health)
    assert exit_code == 0
    assert old.exists()
    assert newest.exists()
    assert health["status"] == "degraded"
    assert health["retention_failure_count"] == 1
    assert '"event":"startup-outbox-snapshot-protection-failed"' in output
    assert SECRET_MARKER not in rendered
    assert "raw_image_bytes abc" not in rendered
    assert_no_secret_leak(output)


def test_runtime_loop_startup_retention_skips_pruning_after_partial_record_quarantine(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    old = snapshots / "occupancy-open-event-left-spot-2026-05-18t18-00-00z.jpg"
    pending = snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    newest = snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    for path in (old, pending, newest):
        path.write_bytes(path.name.encode("utf-8"))
    outbox_path = tmp_path / "matrix-outbox.json"
    LocalOutbox(outbox_path).enqueue(
        AlertIntent(
            event_id="pending-open-alert",
            phase="upload",
            body="Parking spot is open.",
            metadata={"retained_snapshot_path": str(pending)},
        )
    )
    payload = json.loads(outbox_path.read_text(encoding="utf-8"))
    payload["items"].append(
        f"invalid record token={SECRET_MARKER} raw_image_bytes abc"
    )
    outbox_path.write_text(json.dumps(payload), encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"),
        encoding="utf-8",
    )

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T19:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    rendered = output + json.dumps(health)
    assert exit_code == 0
    assert old.exists()
    assert pending.exists()
    assert newest.exists()
    assert health["status"] == "degraded"
    assert health["retention_failure_count"] == 1
    assert '"event":"startup-outbox-snapshot-protection-failed"' in output
    assert SECRET_MARKER not in rendered
    assert "raw_image_bytes abc" not in rendered
    assert_no_secret_leak(output)


def test_runtime_loop_startup_retention_protects_pending_outbox_snapshot(
    tmp_path: Path,
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    pending = snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    unprotected = snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    pending.write_bytes(b"pending-snapshot")
    unprotected.write_bytes(b"newer-snapshot")
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    outbox.enqueue(
        AlertIntent(
            event_id="pending-open-alert",
            phase="upload",
            body="Parking spot is open.",
            metadata={"retained_snapshot_path": str(pending)},
        )
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"),
        encoding="utf-8",
    )

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T19:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    assert exit_code == 0
    assert pending.exists()


def test_runtime_loop_startup_retention_failure_logs_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    (snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg").write_bytes(b"old")
    (snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg").write_bytes(b"new")
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(base.replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"), encoding="utf-8")

    def fail_unlink(_root: Path, _directory: str | None, _filename: str) -> int:
        raise PermissionError(f"permission denied token={FAKE_MATRIX_VALUE} raw_image_bytes abc")

    monkeypatch.setattr(matrix_snapshots, "delete_owned_artifact", fail_unlink)

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"event":"snapshot-retention-failed"' in output
    assert '"trigger":"startup"' in output
    assert '"error_type":"PermissionError"' in output
    assert '"event":"capture-loop-frame-written"' not in output
    health = health_payload(tmp_path / "health.json")
    assert health["status"] == "degraded"
    assert health["retention_failure_count"] == 1
    assert "raw_image_bytes abc" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_passes_effective_paths_to_capture_state_and_matrix(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    captured_data_dirs: list[Path] = []
    matrix_paths: list[tuple[Path, Path]] = []

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        captured_data_dirs.append(Path(data_dir))
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class EmptyDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return []

    def matrix_factory(settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixOutboxDelivery:
        matrix_paths.append((data_dir, settings.storage.snapshots_dir))  # type: ignore[attr-defined]
        return outbox_delivery(FakeMatrixClient(), data_dir, logger)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: EmptyDetector(),
        matrix_delivery_factory=matrix_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert captured_data_dirs == [tmp_path]
    assert matrix_paths == [(tmp_path, tmp_path / "snapshots")]
    assert (tmp_path / "state.json").exists()
    assert_no_secret_leak(output)


def test_validate_config_success_emits_sanitized_startup_events(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--config", "config.yaml.example", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"event":"startup-config-load-start"' in output
    assert '"event":"startup-config-loaded"' in output
    assert '"event":"startup-ready"' in output
    assert '"env_var":"RTSP_URL"' in output
    assert '"env_var":"Matrix token env key"' in output
    assert "access_token" not in output.lower()
    assert_no_secret_leak(output)


def test_validate_config_does_not_capture(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor import __main__ as cli

    def fail_capture(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("validate-config must not call capture")

    monkeypatch.setattr(cli, "capture_latest", fail_capture)

    exit_code = main(["--config", "config.yaml.example", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"mode":"validate-config"' in output
    assert "capture" not in output.lower() or '"event":"startup-config-load-start"' in output
    assert_no_secret_leak(output)


def test_validate_config_does_not_construct_detector(capsys: pytest.CaptureFixture[str]) -> None:
    def fail_detector_factory(_settings: object) -> object:
        raise AssertionError("validate-config must not construct detector")

    exit_code = _main(
        ["--config", "config.yaml.example", "--validate-config"],
        environ=fake_environ(),
        detector_factory=fail_detector_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"mode":"validate-config"' in output
    assert '"event":"detection-frame-failed"' not in output
    assert_no_secret_leak(output)


def test_missing_config_exits_nonzero_with_safe_structured_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing_path = tmp_path / "missing.yaml"

    exit_code = main(["--config", str(missing_path), "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert '"event":"startup-config-invalid"' in output
    assert str(missing_path) in output
    assert '"phase":"read"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_missing_env_exits_nonzero_with_env_names_only(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--config", "config.yaml.example", "--validate-config"], environ={"RTSP_URL": ""})

    output = combined_output(capsys)
    assert exit_code == 2
    assert '"event":"startup-config-invalid"' in output
    assert "RTSP_URL" in output
    assert "MATRIX_ACCESS_TOKEN" in output
    assert '"phase":"env"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_invalid_yaml_exits_nonzero_without_traceback_or_secret(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "invalid.yaml"
    config_path.write_text("stream: [unterminated\n", encoding="utf-8")

    exit_code = main(["--config", str(config_path), "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert '"event":"startup-config-invalid"' in output
    assert '"phase":"yaml"' in output
    assert str(config_path) in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_unknown_cli_flag_exits_nonzero_without_secret(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--unknown-flag"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert "unrecognized arguments" in output
    assert '"event":"startup-arguments-invalid"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_data_dir_override_changes_sanitized_startup_summary(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(
        ["--config", "config.yaml.example", "--data-dir", "/tmp/parking-data", "--validate-config"],
        environ=fake_environ(),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"data_dir":"/tmp/parking-data"' in output
    assert '"event":"startup-ready"' in output
    assert_no_secret_leak(output)


def test_config_error_from_loader_is_converted_to_safe_exit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    def raise_config_error(*_args: object, **_kwargs: object) -> object:
        raise ConfigError(
            "synthetic safe config failure",
            path="config.yaml.example",
            phase="schema",
            fields=("stream.frame_width:Input should be greater than 0",),
        )

    monkeypatch.setattr(cli, "load_settings", raise_config_error)

    exit_code = main(["--config", "config.yaml.example", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert '"event":"startup-config-invalid"' in output
    assert "synthetic safe config failure" in output
    assert "stream.frame_width" in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_capture_once_success_writes_debug_overlay_then_spot_filtered_detection(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_path = tmp_path / "latest.jpg"
    debug_path = tmp_path / "debug_latest.jpg"
    calls: list[tuple[str, Path]] = []

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        assert Path(data_dir) == tmp_path
        assert not latest_path.exists()
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        calls.append(("capture", latest_path))
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    def fake_overlay(_settings: object, source_path: Path, output_path: Path, *, logger: Any) -> object:
        assert Path(source_path) == latest_path
        assert Path(output_path) == debug_path
        assert latest_path.exists()
        calls.append(("overlay", Path(source_path)))
        logger.info(
            "debug-overlay-written",
            source_path=str(source_path),
            output_path=str(output_path),
            width=1458,
            height=806,
            spot_ids=["left_spot", "right_spot"],
        )
        return object()

    class FakeDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            assert Path(frame_path) == latest_path
            assert confidence_threshold == 0.1
            calls.append(("detect", Path(frame_path)))
            return [
                VehicleDetection(class_name="car", confidence=0.9, bbox=(350, 200, 550, 330)),
                VehicleDetection(class_name="person", confidence=0.99, bbox=(350, 200, 550, 330)),
            ]

    constructed: list[str] = []

    def fake_detector_factory(settings: object) -> FakeDetector:
        constructed.append(settings.detection.model)  # type: ignore[attr-defined]
        return FakeDetector()

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--capture-once"],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=fake_overlay,
        detector_factory=fake_detector_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert latest_path.exists()
    assert constructed == ["/models/yolov8n.pt"]
    assert calls == [("capture", latest_path), ("overlay", latest_path), ("detect", latest_path)]
    assert '"event":"capture-once-complete"' in output
    assert '"event":"debug-overlay-written"' in output
    assert '"event":"detection-frame-processed"' in output
    assert '"accepted_count":1' in output
    assert '"detection_count":2' in output
    assert '"spot_ids":["left_spot","right_spot"]' in output
    assert '"candidate_summaries":[{"bbox":[350.0,200.0,550.0,330.0]' in output
    assert '"source_frame_path":"' in output
    assert '"source_timestamp":"2025-01-01T00:00:00Z"' in output
    assert '"class_not_allowed":2' in output
    assert '"centroid_outside":1' in output
    assert '"confidence_threshold":0.35' in output
    assert '"mode":"capture-once"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_capture_once_failure_skips_debug_overlay(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    overlay_calls: list[Path] = []

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        raise CaptureError(
            reason="ffmpeg-nonzero-exit",
            mode=DecodeMode.QSV,
            output_path=Path(data_dir) / "latest.jpg",
            message="ffmpeg exited with a nonzero status",
            stderr_tail="redacted stderr tail",
            duration_seconds=0.02,
            timeout_seconds=15.0,
            returncode=1,
            attempted_modes=[DecodeMode.QSV, DecodeMode.VAAPI, DecodeMode.DRM, DecodeMode.SOFTWARE],
        )

    def fake_overlay(_settings: object, source_path: Path, output_path: Path, *, logger: Any) -> object:
        overlay_calls.append(Path(source_path))
        return object()

    def fail_detector_factory(_settings: object) -> object:
        raise AssertionError("capture failure must not construct detector")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--capture-once"],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=fake_overlay,
        detector_factory=fail_detector_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert overlay_calls == []
    assert '"event":"capture-failed"' in output
    assert '"event":"debug-overlay-written"' not in output
    assert '"event":"debug-overlay-failed"' not in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_capture_once_overlay_failure_returns_nonzero_with_safe_diagnostics(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_path = tmp_path / "latest.jpg"

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    def fake_overlay(_settings: object, source_path: Path, output_path: Path, *, logger: Any) -> object:
        logger.error(
            "debug-overlay-failed",
            source_path=str(source_path),
            output_path=str(output_path),
            spot_ids=["left_spot", "right_spot"],
            width=None,
            height=None,
            error_type="UnidentifiedImageError",
            error_message="debug overlay source frame could not be decoded",
        )
        raise RuntimeError(f"overlay failure with {SECRET_MARKER}")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--capture-once"],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=fake_overlay,
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"capture-once-complete"' not in output
    assert '"event":"debug-overlay-failed"' in output
    assert '"error_type":"UnidentifiedImageError"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_capture_once_detection_failure_returns_nonzero_with_safe_diagnostics(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_path = tmp_path / "latest.jpg"

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    class FailingDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            raise DetectionError(
                f"predict failed rtsp://user:pass@camera access_token={SECRET_MARKER} Traceback noisy",
                model_path="yolov8n.pt",
                frame_path=str(frame_path),
                phase="predict",
                error_type="RuntimeError",
            )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--capture-once"],
        environ=fake_environ(),
        capture=fake_capture,
        detector_factory=lambda _settings: FailingDetector(),
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"detection-frame-failed"' in output
    assert '"phase":"predict"' in output
    assert '"frame_path":"' in output
    assert '"event":"capture-once-complete"' not in output
    assert '"event":"detection-frame-processed"' not in output
    assert "user:pass" not in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_overlay_failure_logs_and_continues(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_path = tmp_path / "latest.jpg"
    sleeps: list[float] = []
    overlay_calls: list[Path] = []
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("adaptive_polling_enabled: true", "adaptive_polling_enabled: false"),
        encoding="utf-8",
    )

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    def fake_overlay(_settings: object, source_path: Path, output_path: Path, *, logger: Any) -> object:
        overlay_calls.append(Path(source_path))
        logger.error(
            "debug-overlay-failed",
            source_path=str(source_path),
            output_path=str(output_path),
            spot_ids=["left_spot", "right_spot"],
            width=None,
            height=None,
            error_type="OSError",
            error_message="debug overlay could not be written",
        )
        raise RuntimeError(f"overlay failure with {SECRET_MARKER}")

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=fake_overlay,
        detector_factory=noop_detector_factory,
        sleep=sleeps.append,
        max_iterations=1,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert overlay_calls == [latest_path]
    assert sleeps == [30]
    assert '"event":"capture-loop-frame-written"' not in output
    assert '"event":"debug-overlay-failed"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_success_logs_detection_frame_processed_with_metadata(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_path = tmp_path / "latest.jpg"
    sleeps: list[float] = []
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("adaptive_polling_enabled: true", "adaptive_polling_enabled: false"),
        encoding="utf-8",
    )

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        assert Path(data_dir) == tmp_path
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-02T03:04:05Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    class FakeDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            assert Path(frame_path) == latest_path
            assert confidence_threshold == 0.1
            return [VehicleDetection(class_name="truck", confidence=0.88, bbox=(350, 200, 550, 330))]

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path), "--log-level", "DEBUG"],
        environ=fake_environ(),
        capture=fake_capture,
        detector_factory=lambda _settings: FakeDetector(),
        sleep=sleeps.append,
        max_iterations=1,
        random_unit=lambda: 0,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert sleeps == [30]
    assert '"event":"detection-frame-processed"' in output
    assert '"mode":"runtime-loop"' in output
    assert '"iteration":1' in output
    assert '"accepted_count":1' in output
    assert '"source_frame_path":"' in output
    assert '"source_timestamp":"2025-01-02T03:04:05Z"' in output
    assert '"event":"capture-loop-frame-written"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_capture_once_failure_returns_nonzero_without_traceback_or_secret(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        raise CaptureError(
            reason="ffmpeg-nonzero-exit",
            mode=DecodeMode.QSV,
            output_path=Path(data_dir) / "latest.jpg",
            message="ffmpeg exited with a nonzero status",
            stderr_tail="redacted stderr tail",
            duration_seconds=0.02,
            timeout_seconds=15.0,
            returncode=1,
            attempted_modes=[DecodeMode.QSV, DecodeMode.VAAPI, DecodeMode.DRM, DecodeMode.SOFTWARE],
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--capture-once"],
        environ=fake_environ(),
        capture=fake_capture,
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"capture-failed"' in output
    assert '"reason":"ffmpeg-nonzero-exit"' in output
    assert '"attempted_modes":["qsv","vaapi","drm","software"]' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_default_runtime_loop_logs_failure_and_uses_reconnect_backoff(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    sleeps: list[float] = []

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        raise CaptureError(
            reason="ffmpeg-timeout",
            mode=DecodeMode.SOFTWARE,
            output_path=Path(data_dir) / "latest.jpg",
            message="timeout",
            stderr_tail="",
            timeout_seconds=15.0,
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        sleep=sleeps.append,
        max_iterations=1,
        random_unit=lambda: 0,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert sleeps == [5]
    assert '"event":"capture-loop-iteration"' not in output
    assert '"event":"capture-loop-failure"' in output
    assert '"backoff_seconds":5' in output
    health = health_payload(tmp_path / "health.json")
    assert health["status"] == "down"
    assert health["iteration"] == 1
    assert health["consecutive_capture_failures"] == 1
    assert health["consecutive_detection_failures"] == 0
    assert health["last_error"]["phase"] == "capture"
    assert health["last_error"]["error_type"] == "CaptureError"
    assert SECRET_MARKER not in json.dumps(health)
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_success_writes_health_and_uses_configured_frame_interval(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    sleeps: list[float] = []
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("frame_interval_seconds: 30", "frame_interval_seconds: 2")
        .replace("adaptive_polling_enabled: true", "adaptive_polling_enabled: false"),
        encoding="utf-8",
    )

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z")

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=sleeps.append,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert sleeps == [2, 2]
    assert health["status"] == "ok"
    assert health["iteration"] == 2
    assert health["last_frame_at"] == "2026-05-18T18:00:00Z"
    assert health["selected_decode_mode"] == "software"
    assert health["capture"] == {
        "last_success_at": "2026-05-18T18:00:00Z",
        "selected_decode_mode": "software",
    }
    assert health["consecutive_capture_failures"] == 0
    assert health["consecutive_detection_failures"] == 0
    assert health["last_matrix_error"] is None
    assert health["last_error"] is None
    timeline_frames = sorted((tmp_path / "timeline" / "frames").glob("*.jpg"))
    assert [path.name for path in timeline_frames] == ["20260518T180000Z.jpg"]
    assert timeline_frames[0].read_bytes() == (tmp_path / "latest.jpg").read_bytes()
    assert '"event":"timeline-frame-retained"' not in output
    assert '"event":"capture-loop-paced"' not in output
    assert_no_secret_leak(output)


def test_runtime_loop_equal_active_and_stable_intervals_preserve_fixed_cadence(
    tmp_path: Path,
) -> None:
    sleeps: list[float] = []
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "frame_interval_seconds": 2,
                    "stable_frame_interval_seconds": 2,
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )
    monotonic_values = iter([0.0, 1.0, 2.0, 3.0])

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T18:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        sleep=sleeps.append,
        max_iterations=2,
        monotonic=lambda: next(monotonic_values),
        decision_memory_store=independent_decision_memory_store(tmp_path),
    )

    assert exit_code == 0
    assert sleeps == [2, 2]


def test_runtime_loop_paces_successful_noop_matrix_command_polls_with_monotonic_clock(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    capture_calls = 0
    poll_calls = 0
    sleeps: list[float] = []
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "matrix": settings.matrix.model_copy(
                update={"command_poll_interval_seconds": 60}
            ),
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "adaptive_polling_enabled": False,
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )

    def capture_frame(
        _settings: object,
        data_dir: str | Path,
        **_kwargs: object,
    ) -> FrameCaptureResult:
        nonlocal capture_calls
        capture_calls += 1
        return captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z")

    class NoopCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            nonlocal poll_calls
            poll_calls += 1
            return FakeCommandPollResult()

    monotonic_values = iter(
        [0.0, 0.0, 0.0, 1.0, 30.0, 30.0, 31.0, 60.0, 60.0, 60.0, 61.0]
    )

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(level="DEBUG"),
        capture=capture_frame,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        matrix_command_service=NoopCommandService(),
        sleep=sleeps.append,
        max_iterations=3,
        monotonic=lambda: next(monotonic_values),
        decision_memory_store=independent_decision_memory_store(tmp_path),
    )

    output = combined_output(capsys)
    success_logs = [
        record
        for record in json_records(output)
        if record.get("event") == "matrix-command-poll-succeeded"
    ]
    command_records = [
        record
        for record in load_decision_memory(
            tmp_path / "operator-decision-memory.json"
        ).records
        if record.kind == "command_outcome"
    ]
    assert exit_code == 0
    assert capture_calls == 3
    assert poll_calls == 2
    assert len(sleeps) == 3
    assert len(command_records) == 2
    assert [record["level"] for record in success_logs] == ["DEBUG", "DEBUG"]


def test_runtime_loop_matrix_poll_interval_is_anchored_to_actual_poll_calls(
    tmp_path: Path,
) -> None:
    current_time = 0.0
    processing_durations = iter([8.0, 8.0, 1.0, 1.0])
    poll_times: list[float] = []
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "matrix": settings.matrix.model_copy(
                update={"command_poll_interval_seconds": 10}
            ),
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "frame_interval_seconds": 1,
                    "adaptive_polling_enabled": False,
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )

    def monotonic() -> float:
        return current_time

    def capture_frame(
        _settings: object,
        data_dir: str | Path,
        **_kwargs: object,
    ) -> FrameCaptureResult:
        nonlocal current_time
        current_time += next(processing_durations)
        return captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z")

    class CommandService:
        def poll_once(self) -> FakeCommandPollResult:
            poll_times.append(current_time)
            return FakeCommandPollResult()

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=capture_frame,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        matrix_command_service=CommandService(),
        sleep=lambda seconds: None,
        max_iterations=4,
        monotonic=monotonic,
    )

    assert exit_code == 0
    assert poll_times == [8.0, 18.0]


def test_runtime_loop_matrix_failure_cooldown_starts_after_failed_poll_completes(
    tmp_path: Path,
) -> None:
    current_time = 0.0
    processing_durations = iter([5.0, 2.0, 0.0, 7.0])
    poll_times: list[float] = []
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "matrix": settings.matrix.model_copy(
                update={
                    "command_poll_interval_seconds": 0,
                    "command_failure_cooldown_seconds": 10,
                    "command_failure_max_cooldown_seconds": 10,
                }
            ),
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "frame_interval_seconds": 1,
                    "adaptive_polling_enabled": False,
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )

    def monotonic() -> float:
        return current_time

    def capture_frame(
        _settings: object,
        data_dir: str | Path,
        **_kwargs: object,
    ) -> FrameCaptureResult:
        nonlocal current_time
        current_time += next(processing_durations)
        return captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z")

    class CommandService:
        def poll_once(self) -> FakeCommandPollResult:
            nonlocal current_time
            poll_times.append(current_time)
            if len(poll_times) == 1:
                current_time += 4.0
                raise RuntimeError("Matrix unavailable")
            return FakeCommandPollResult()

    def sleep(seconds: float) -> None:
        nonlocal current_time
        current_time += seconds

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=capture_frame,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        matrix_command_service=CommandService(),
        sleep=sleep,
        max_iterations=4,
        monotonic=monotonic,
    )

    assert exit_code == 0
    assert poll_times[0] == 5.0
    assert len(poll_times) == 2
    assert poll_times[1] >= 19.0


def test_runtime_loop_open_matrix_command_circuit_skips_polls_without_sleeping_or_recounting(
    tmp_path: Path,
) -> None:
    capture_calls = 0
    poll_calls = 0
    sleeps: list[float] = []
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "matrix": settings.matrix.model_copy(
                update={
                    "command_poll_interval_seconds": 60,
                    "command_failure_cooldown_seconds": 60,
                    "command_failure_max_cooldown_seconds": 900,
                }
            ),
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "adaptive_polling_enabled": False,
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )

    def capture_frame(
        _settings: object,
        data_dir: str | Path,
        **_kwargs: object,
    ) -> FrameCaptureResult:
        nonlocal capture_calls
        capture_calls += 1
        return captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z")

    class FailingCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            nonlocal poll_calls
            poll_calls += 1
            raise RuntimeError("Matrix unavailable")

    monotonic_values = iter(
        [
            0.0,
            0.0,
            0.0,
            1.0,
            30.0,
            30.0,
            31.0,
            60.0,
            60.0,
            60.0,
            61.0,
            120.0,
            120.0,
            121.0,
        ]
    )

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=capture_frame,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        matrix_command_service=FailingCommandService(),
        sleep=sleeps.append,
        max_iterations=4,
        monotonic=lambda: next(monotonic_values),
        decision_memory_store=independent_decision_memory_store(tmp_path),
    )

    health = health_payload(tmp_path / "health.json")
    command_records = [
        record
        for record in load_decision_memory(
            tmp_path / "operator-decision-memory.json"
        ).records
        if record.kind == "command_outcome"
    ]
    assert exit_code == 0
    assert capture_calls == 4
    assert poll_calls == 2
    assert len(sleeps) == 4
    assert health["matrix_command_failure_count"] == 2
    assert len(command_records) == 2


def test_runtime_loop_stable_cadence_starts_after_settle_threshold(
    tmp_path: Path,
) -> None:
    sleeps: list[float] = []
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "frame_interval_seconds": 15,
                    "stable_frame_interval_seconds": 60,
                    "stable_settle_frames": 3,
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )
    save_runtime_state(
        tmp_path / "state.json",
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY, miss_streak=3
                ),
                "right_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY, miss_streak=3
                ),
            }
        ),
    )
    monotonic_values = iter([0.0, 0.0, 15.0, 15.0, 30.0, 30.0])

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T18:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        sleep=sleeps.append,
        max_iterations=3,
        monotonic=lambda: next(monotonic_values),
        decision_memory_store=independent_decision_memory_store(tmp_path),
    )

    assert exit_code == 0
    assert sleeps == [15, 15, 60]


@pytest.mark.parametrize(
    ("processing_finished_at", "expected_sleep"),
    [(104.0, 11.0), (120.0, 0.0)],
)
def test_runtime_loop_deadline_pacing_subtracts_processing_time_and_clamps_overrun(
    tmp_path: Path,
    processing_finished_at: float,
    expected_sleep: float,
) -> None:
    sleeps: list[float] = []
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "frame_interval_seconds": 15,
                    "stable_frame_interval_seconds": 60,
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )
    monotonic_values = iter([100.0, processing_finished_at])

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T18:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        sleep=sleeps.append,
        max_iterations=1,
        monotonic=lambda: next(monotonic_values),
        decision_memory_store=independent_decision_memory_store(tmp_path),
    )

    assert exit_code == 0
    assert sleeps == [expected_sleep]


def test_runtime_loop_overlay_cadence_skips_stable_frames_and_writes_on_transition(
    tmp_path: Path,
) -> None:
    overlay_sources: list[Path] = []
    sleeps: list[float] = []
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={"health_file": tmp_path / "health.json"}
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 0}
            ),
        }
    )
    save_runtime_state(
        tmp_path / "state.json",
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY, miss_streak=3
                ),
                "right_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY, miss_streak=3
                ),
            }
        ),
    )
    detections = [[], [], [left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]

    class SequencedDetector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
        ) -> list[VehicleDetection]:
            return next_detection(detections)

    def record_overlay(
        _settings: object,
        source_path: Path,
        _output_path: Path,
        *,
        logger: Any,
    ) -> object:
        overlay_sources.append(Path(source_path))
        return object()

    monotonic_values = iter(
        [0.0, 1.0, 10.0, 11.0, 20.0, 21.0, 30.0, 31.0, 40.0, 41.0]
    )

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T18:00:00Z"
        ),
        overlay=record_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery=None,
        sleep=sleeps.append,
        max_iterations=5,
        monotonic=lambda: next(monotonic_values),
        decision_memory_store=independent_decision_memory_store(tmp_path),
    )

    assert exit_code == 0
    assert overlay_sources == [tmp_path / "latest.jpg", tmp_path / "latest.jpg"]


def test_runtime_loop_reuses_vehicle_history_health_snapshot_within_cache_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    class CountingHistoryArchive:
        latest: "CountingHistoryArchive | None" = None

        def __init__(self, _root: Path, *, logger: StructuredLogger | None = None) -> None:
            self.calls = 0
            CountingHistoryArchive.latest = self

        def health_snapshot(self) -> dict[str, Any]:
            self.calls += 1
            return {"archive_status": "cached", "calls": self.calls}

        def mutation_revision(self) -> int:
            return 0

    monkeypatch.setattr(cli, "VehicleHistoryArchive", CountingHistoryArchive)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert CountingHistoryArchive.latest is not None
    assert CountingHistoryArchive.latest.calls == 1
    assert health_payload(tmp_path / "health.json")["vehicle_history"]["calls"] == 1
    assert_no_secret_leak(combined_output(capsys))


def test_runtime_loop_noop_matrix_commands_keep_vehicle_history_health_snapshot_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    class CountingHistoryArchive:
        latest: "CountingHistoryArchive | None" = None

        def __init__(self, _root: Path, *, logger: StructuredLogger | None = None) -> None:
            self.calls = 0
            CountingHistoryArchive.latest = self

        def health_snapshot(self) -> dict[str, Any]:
            self.calls += 1
            return {"archive_status": "cached", "calls": self.calls}

        def mutation_revision(self) -> int:
            return 0

    class NoopCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult()

    monkeypatch.setattr(cli, "VehicleHistoryArchive", CountingHistoryArchive)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, _archive: NoopCommandService(),
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert CountingHistoryArchive.latest is not None
    assert CountingHistoryArchive.latest.calls == 1
    assert health_payload(tmp_path / "health.json")["vehicle_history"]["calls"] == 1
    assert_no_secret_leak(combined_output(capsys))


def test_runtime_loop_read_only_matrix_commands_keep_vehicle_history_health_snapshot_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    class CountingHistoryArchive:
        latest: "CountingHistoryArchive | None" = None

        def __init__(self, _root: Path, *, logger: StructuredLogger | None = None) -> None:
            self.calls = 0
            CountingHistoryArchive.latest = self

        def health_snapshot(self) -> dict[str, Any]:
            self.calls += 1
            return {"archive_status": "cached", "calls": self.calls}

        def mutation_revision(self) -> int:
            return 0

    class ReadOnlyCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult(processed_count=1)

    monkeypatch.setattr(cli, "VehicleHistoryArchive", CountingHistoryArchive)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, _archive: ReadOnlyCommandService(),
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert CountingHistoryArchive.latest is not None
    assert CountingHistoryArchive.latest.calls == 1
    assert health_payload(tmp_path / "health.json")["vehicle_history"]["calls"] == 1
    assert_no_secret_leak(combined_output(capsys))


def test_startup_drains_are_not_owned_by_the_capture_loop(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    class WorkerOwnedOutboxDelivery(FakeMatrixDelivery):
        def __init__(self) -> None:
            super().__init__()
            self.drain_calls = 0

        def drain_outbox(self, *, max_records: int | None = None) -> FakeOutboxDrainResult:
            self.drain_calls += 1
            raise AssertionError("capture loop must not drain the worker-owned outbox")

    delivery = WorkerOwnedOutboxDelivery()

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
    )

    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert delivery.drain_calls == 0
    assert health["status"] == "ok"
    assert health["last_matrix_error"] is None
    assert health["last_error"] is None
    assert_no_secret_leak(combined_output(capsys))


def test_runtime_loop_sends_matrix_startup_lifecycle_notice(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(Path(data_dir), timestamp="2026-05-18T18:00:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert len(delivery.lifecycle_notices) == 1
    notice = delivery.lifecycle_notices[0]
    assert notice["event_type"] == "parking-monitor-started"
    assert notice["observed_at"] == "2026-05-18T18:00:00Z"
    assert notice["event_id"] == "parking-monitor-started:2026-05-18T18:00:00Z"
    assert '"event":"parking-monitor-started"' in output
    assert_no_secret_leak(output)


def test_shutdown_signal_handler_records_flag_without_matrix_io(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from parking_spot_monitor.runtime_lifecycle import (
        ShutdownState,
        install_shutdown_signal_handlers,
        restore_shutdown_signal_handlers,
    )

    state = ShutdownState()
    previous = install_shutdown_signal_handlers(state, logger=StructuredLogger())
    try:
        handler = signal.getsignal(signal.SIGTERM)
        handler(signal.SIGTERM, None)
        handler(signal.SIGTERM, None)
    finally:
        restore_shutdown_signal_handlers(previous)

    assert state.requested is True
    assert state.signum == signal.SIGTERM
    assert state.signal_name == "SIGTERM"
    assert combined_output(capsys) == ""


def test_shutdown_state_wakes_wait_immediately() -> None:
    from parking_spot_monitor.runtime_lifecycle import ShutdownState

    state = ShutdownState()
    started = threading.Event()
    finished = threading.Event()

    def wait_for_shutdown() -> None:
        started.set()
        assert state.wait(60) is True
        finished.set()

    thread = threading.Thread(target=wait_for_shutdown)
    thread.start()
    assert started.wait(1)
    state.request(signal.SIGTERM)
    assert finished.wait(1)
    thread.join(1)
    assert thread.is_alive() is False


def test_shutdown_state_reentrant_request_preserves_first_signal() -> None:
    from parking_spot_monitor.runtime_lifecycle import ShutdownState

    state = ShutdownState()
    underlying = threading.Event()

    class ReentrantSetEvent:
        reentered = False

        def is_set(self) -> bool:
            return underlying.is_set()

        def set(self) -> None:
            if not self.reentered:
                self.reentered = True
                state.request(signal.SIGINT)
            underlying.set()

        def wait(self, timeout: float | None = None) -> bool:
            return underlying.wait(timeout)

    state._event = ReentrantSetEvent()  # type: ignore[assignment]
    state.request(signal.SIGTERM)

    assert state.signum == signal.SIGTERM
    assert state.requested is True


def test_shutdown_state_concurrent_request_preserves_first_signal() -> None:
    from parking_spot_monitor.runtime_lifecycle import ShutdownState

    state = ShutdownState()
    underlying = threading.Event()
    first_setting = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    second_returned = threading.Event()

    class OrderedSetEvent:
        def is_set(self) -> bool:
            return underlying.is_set()

        def set(self) -> None:
            if threading.current_thread().name == "first-request":
                first_setting.set()
                assert release_first.wait(1)
            underlying.set()

        def wait(self, timeout: float | None = None) -> bool:
            return underlying.wait(timeout)

    state._event = OrderedSetEvent()  # type: ignore[assignment]
    first = threading.Thread(
        target=lambda: state.request(signal.SIGTERM),
        name="first-request",
    )

    def request_second() -> None:
        second_started.set()
        state.request(signal.SIGINT)
        second_returned.set()

    second = threading.Thread(target=request_second, name="second-request")
    first.start()
    assert first_setting.wait(1)
    second.start()
    assert second_started.wait(1)
    second_returned.wait(0.05)
    release_first.set()
    first.join(1)
    second.join(1)

    assert first.is_alive() is False
    assert second.is_alive() is False
    assert state.signum == signal.SIGTERM
    assert state.requested is True


def test_reconnect_wait_wakes_and_exits_immediately_on_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor.runtime_lifecycle import ShutdownState

    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={"stream": settings.stream.model_copy(update={"reconnect_seconds": 60})}
    )
    monkeypatch.chdir(tmp_path)
    state = ShutdownState()
    capture_attempted = threading.Event()
    exits: list[int] = []

    def fail_capture(
        _settings: object,
        data_dir: str | Path,
        **_kwargs: object,
    ) -> FrameCaptureResult:
        capture_attempted.set()
        raise CaptureError(
            reason="ffmpeg_error",
            mode=DecodeMode.SOFTWARE,
            output_path=Path(data_dir) / "latest.jpg",
            message="capture unavailable",
        )

    thread = threading.Thread(
        target=lambda: exits.append(
            run_capture_loop(
                settings,
                tmp_path,
                logger=StructuredLogger(),
                capture=fail_capture,
                overlay=noop_overlay,
                detector_factory=noop_detector_factory,
                matrix_delivery=None,
                sleep=time.sleep,
                wait=state.wait,
                shutdown_state=state,
            )
        )
    )
    thread.start()
    assert capture_attempted.wait(1)
    started = time.monotonic()
    state.request(signal.SIGTERM)
    thread.join(1)

    assert time.monotonic() - started < 1
    assert thread.is_alive() is False
    assert exits == [0]


def test_close_resources_continues_after_first_close_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from parking_spot_monitor.__main__ import _close_resources

    closed: list[str] = []

    class FailingClose:
        def close(self) -> None:
            raise RuntimeError(f"close failed {SECRET_MARKER}")

    class RecordingClose:
        def close(self) -> None:
            closed.append("delivery")

    _close_resources(
        (("commands", FailingClose()), ("delivery", RecordingClose())),
        logger=StructuredLogger(),
    )

    assert closed == ["delivery"]
    output = combined_output(capsys)
    assert '"event":"runtime-resource-close-failed"' in output
    assert '"resource":"commands"' in output
    assert SECRET_MARKER not in output


def test_close_resources_continues_when_cleanup_logging_fails() -> None:
    from parking_spot_monitor.__main__ import _close_resources

    closed: list[str] = []

    class FailingClose:
        def close(self) -> None:
            raise RuntimeError("close failed")

    class RecordingClose:
        def close(self) -> None:
            closed.append("delivery")

    class ClosedLogger:
        def warning(self, _event: str, **_fields: object) -> None:
            raise OSError("logging sink closed")

    _close_resources(
        (("commands", FailingClose()), ("delivery", RecordingClose())),
        logger=ClosedLogger(),  # type: ignore[arg-type]
    )

    assert closed == ["delivery"]


def test_command_factory_failure_closes_already_created_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor import __main__ as cli

    closed: list[str] = []
    original = RuntimeError("command construction failed")

    class CleanupLogger(StructuredLogger):
        def warning(self, _event: str, **_fields: object) -> None:
            raise OSError("cleanup logging failed")

    cleanup_logger = CleanupLogger()
    monkeypatch.setattr(cli, "setup_logging", lambda **_kwargs: cleanup_logger)

    class Delivery(FakeMatrixDelivery):
        def close(self) -> None:
            closed.append("delivery")
            raise RuntimeError("delivery close failed")

    def fail_command_factory(
        _settings: object,
        _data_dir: Path,
        _logger: StructuredLogger,
        _archive: object,
    ) -> object:
        raise original

    with pytest.raises(RuntimeError) as exc_info:
        _main(
            ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
            environ=fake_environ(),
            capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir)),
            overlay=noop_overlay,
            detector_factory=noop_detector_factory,
            matrix_delivery_factory=lambda _settings, _data_dir, _logger: Delivery(),
            matrix_command_service_factory=fail_command_factory,
            sleep=lambda _seconds: None,
            max_iterations=0,
        )

    assert exc_info.value is original
    assert closed == ["delivery"]


def test_dispatch_shutdown_lifecycle_notice_once(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor.matrix_dispatch import dispatch_matrix_event
    from parking_spot_monitor.matrix import MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE, monitor_lifecycle_event
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore

    delivery = FakeMatrixDelivery()
    store = DecisionMemoryStore(
        tmp_path / "operator-decision-memory.json",
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=50,
    )
    observed_at = datetime(2026, 5, 18, 18, 1, tzinfo=timezone.utc)
    dispatch_matrix_event(
        delivery,
        MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
        monitor_lifecycle_event(MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE, observed_at, signal="SIGTERM"),
        logger=StructuredLogger(),
        decision_memory_store=store,
    )

    output = combined_output(capsys)
    assert len(delivery.lifecycle_notices) == 1
    notice = delivery.lifecycle_notices[0]
    assert notice["event_type"] == "parking-monitor-shutdown-requested"
    assert notice["signal"] == "SIGTERM"
    assert notice["event_id"] == "parking-monitor-shutdown-requested:SIGTERM:2026-05-18T18:01:00Z"
    assert load_decision_memory(tmp_path / "operator-decision-memory.json").records[-1].kind == "alert"
    assert '"event":"parking-monitor-shutdown-requested"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_shutdown_during_sleep_sends_one_shutdown_notice(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    delivery = FakeMatrixDelivery()
    slept = False

    def sleep_then_sigterm(_seconds: float) -> None:
        nonlocal slept
        if slept:
            return
        slept = True
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir)),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=sleep_then_sigterm,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    started = [notice for notice in delivery.lifecycle_notices if notice["event_type"] == "parking-monitor-started"]
    shutdown = [notice for notice in delivery.lifecycle_notices if notice["event_type"] == "parking-monitor-shutdown-requested"]
    assert len(started) == 1
    assert len(shutdown) == 1
    assert shutdown[0]["signal"] == "SIGTERM"
    assert '"event":"capture-loop-shutdown-requested"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_failure_updates_health_and_loop_continues(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery(fail=True)

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert len(delivery.open_alerts) == 1
    assert health["status"] == "degraded"
    assert health["last_matrix_error"]["event_type"] == "occupancy-open-event"
    assert health["last_matrix_error"]["error_type"] == "RuntimeError"
    assert detections == []
    assert SECRET_MARKER not in json.dumps(health)
    assert '"event":"capture-loop-frame-written"' not in output
    assert_no_secret_leak(output)


def test_runtime_loop_state_save_failure_updates_health_and_loop_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import parking_spot_monitor.runtime_state_update as runtime_state_update

    def fail_state_save(*_args: object, **_kwargs: object) -> None:
        raise PermissionError(f"state denied token={SECRET_MARKER} Traceback raw_image_bytes abc")

    monkeypatch.setattr(runtime_state_update, "save_runtime_state", fail_state_save)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--log-level", "DEBUG"],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert health["status"] == "degraded"
    assert health["state_save_error"]["phase"] == "state-save"
    assert health["state_save_error"]["error_type"] == "PermissionError"
    assert SECRET_MARKER not in json.dumps(health)
    assert '"event":"capture-loop-frame-written"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_state_save_failure_still_emits_matrix_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import parking_spot_monitor.runtime_state_update as runtime_state_update

    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.OCCUPIED,
                    hit_streak=3,
                    miss_streak=2,
                    last_bbox=(350.0, 200.0, 550.0, 330.0),
                    open_event_emitted=False,
                ),
                "right_spot": SpotOccupancyState(),
            }
        ),
    )

    def fail_state_save(*_args: object, **_kwargs: object) -> None:
        raise PermissionError(f"state denied token={SECRET_MARKER} Traceback raw_image_bytes abc")

    monkeypatch.setattr(runtime_state_update, "save_runtime_state", fail_state_save)
    delivery = FakeMatrixDelivery()

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert len(delivery.open_alerts) == 1
    assert "occupancy-open-event" in event_names(output)
    assert_no_secret_leak(output)


def test_runtime_loop_state_save_failure_continues_from_previous_durable_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import parking_spot_monitor.runtime_state_update as runtime_state_update

    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.OCCUPIED,
                    hit_streak=3,
                    miss_streak=1,
                    last_bbox=(350.0, 200.0, 550.0, 330.0),
                    open_event_emitted=False,
                ),
                "right_spot": SpotOccupancyState(),
            }
        ),
    )
    real_save_runtime_state = runtime_state_update.save_runtime_state
    save_attempts = 0

    def fail_once_then_save(*args: object, **kwargs: object) -> None:
        nonlocal save_attempts
        save_attempts += 1
        if save_attempts == 1:
            raise PermissionError(f"state denied token={SECRET_MARKER} Traceback raw_image_bytes abc")
        real_save_runtime_state(*args, **kwargs)

    monkeypatch.setattr(runtime_state_update, "save_runtime_state", fail_once_then_save)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    payload = runtime_state_payload(state_path)
    assert exit_code == 0
    assert save_attempts == 2
    assert payload["spots"]["left_spot"]["status"] == "empty"
    assert payload["spots"]["left_spot"]["miss_streak"] == 3
    assert payload["spots"]["left_spot"]["open_event_emitted"] is True
    assert "occupancy-open-event" in event_names(output)
    assert_no_secret_leak(output)


def test_runtime_loop_health_write_failure_logs_safely_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import parking_spot_monitor.runtime_health as runtime_health

    def fail_health_write(*_args: object, **_kwargs: object) -> None:
        raise PermissionError(f"health denied token={SECRET_MARKER} Traceback raw_image_bytes abc")

    monkeypatch.setattr(runtime_health, "write_health_status", fail_health_write)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"event":"health-write-failed"' in output
    assert '"error_type":"PermissionError"' in output
    assert "raw_image_bytes abc" not in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)



def test_runtime_loop_confirms_occupied_releases_empty_and_logs_open_event(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--log-level", "DEBUG"],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    records = json_records(output)
    events = [str(record["event"]) for record in records]
    left_state_changes = [
        record for record in records if record["event"] == "occupancy-state-changed" and record.get("spot_id") == "left_spot"
    ]
    assert exit_code == 0
    assert len(left_state_changes) == 2
    assert events.count("occupancy-open-event") == 1
    assert "occupancy-open-suppressed" not in events
    assert state_status(tmp_path / "state.json", "left_spot") == "empty"
    assert runtime_state_payload(tmp_path / "state.json")["spots"]["left_spot"]["open_event_emitted"] is True
    assert events.index("detection-frame-processed") < events.index("occupancy-state-changed")
    assert events.index("state-saved") < events.index("capture-loop-frame-written")
    assert_no_secret_leak(output)


def test_runtime_loop_startup_unknown_empty_frames_emit_no_open_event(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    events = event_names(output)
    assert exit_code == 0
    assert "occupancy-open-event" not in events
    assert "occupancy-open-suppressed" not in events
    assert state_status(tmp_path / "state.json", "left_spot") == "empty"
    assert "occupancy-state-changed" in events
    assert_no_secret_leak(output)


def test_runtime_loop_quiet_window_suppresses_open_event_and_emits_notice(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T20:30:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 20, 30, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    records = json_records(output)
    events = [str(record["event"]) for record in records]
    assert exit_code == 0
    assert events.count("quiet-window-started") == 1
    assert events.count("occupancy-open-suppressed") == 1
    assert "occupancy-open-event" not in events
    suppressed = next(record for record in records if record["event"] == "occupancy-open-suppressed")
    assert suppressed["suppressed_reason"] == "quiet_window:street_sweeping:2026-05-18:13:00-15:00"
    payload = runtime_state_payload(tmp_path / "state.json")
    assert payload["active_quiet_window_ids"] == ["street_sweeping:2026-05-18:13:00-15:00"]
    assert payload["quiet_window_notice_ids"] == ["quiet-window-started:street_sweeping:2026-05-18:13:00-15:00"]
    assert_no_secret_leak(output)


def test_runtime_loop_persists_occupied_state_across_invocations_before_open_event(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    hit_detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class HitDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return hit_detections.pop(0)

    first_exit = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: HitDetector(),
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )
    first_output = combined_output(capsys)
    assert first_exit == 0
    assert state_status(tmp_path / "state.json", "left_spot") == "occupied"
    assert "occupancy-open-event" not in event_names(first_output)

    class EmptyDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return []

    second_exit = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: EmptyDetector(),
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 19, 1, tzinfo=timezone.utc),
    )
    second_output = combined_output(capsys)
    assert second_exit == 0
    assert event_names(second_output).count("state-loaded") == 1
    assert event_names(second_output).count("occupancy-open-event") == 1
    assert state_status(tmp_path / "state.json", "left_spot") == "empty"
    assert_no_secret_leak(first_output + second_output)


def test_runtime_loop_detection_and_capture_failures_do_not_advance_miss_counters(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.OCCUPIED,
                    hit_streak=3,
                    miss_streak=0,
                    last_bbox=(350.0, 200.0, 550.0, 330.0),
                    open_event_emitted=False,
                ),
                "right_spot": SpotOccupancyState(),
            }
        ),
    )

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class FailingDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            raise DetectionError(
                f"predict failed access_token={SECRET_MARKER}",
                model_path="yolov8n.pt",
                frame_path=str(frame_path),
                phase="predict",
                error_type="RuntimeError",
            )

    detector_exit = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: FailingDetector(),
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )
    detector_output = combined_output(capsys)
    assert detector_exit == 0
    assert runtime_state_payload(state_path)["spots"]["left_spot"]["miss_streak"] == 0
    assert "occupancy-open-event" not in event_names(detector_output)

    def failing_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        raise CaptureError(
            reason="ffmpeg-timeout",
            mode=DecodeMode.SOFTWARE,
            output_path=Path(data_dir) / "latest.jpg",
            message=f"timeout rtsp://camera access_token={SECRET_MARKER}",
            stderr_tail=f"Traceback raw_image_bytes {SECRET_MARKER}",
            timeout_seconds=15.0,
        )

    capture_exit = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=failing_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )
    capture_output = combined_output(capsys)
    assert capture_exit == 0
    assert runtime_state_payload(state_path)["spots"]["left_spot"]["miss_streak"] == 0
    assert "occupancy-open-event" not in event_names(capture_output)
    assert_no_secret_leak(detector_output + capture_output)


def test_runtime_loop_corrupt_state_is_quarantined_and_defaults_unknown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text("{not json rtsp://camera access_token=supersecret Traceback raw_image_bytes", encoding="utf-8")

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    events = event_names(output)
    assert "state-corrupt-quarantined" in events
    assert "state-loaded" in events
    assert state_status(state_path, "left_spot") == "unknown"
    assert len(list(tmp_path.glob("state.json.corrupt-*"))) == 1
    assert "supersecret" not in output
    assert "Traceback" not in output
    assert "raw_image_bytes" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_upload_failure_logs_safe_context_and_retains_copied_snapshot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor.matrix import MatrixError

    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]

    class UploadFailingMatrixClient(FakeMatrixClient):
        def __init__(self) -> None:
            super().__init__()
            self.failed_upload = threading.Event()

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            self.uploads.append({"filename": filename, "data": data, "content_type": content_type})
            self.failed_upload.set()
            raise MatrixError(
                f"Matrix upload failed Authorization: Bearer {FAKE_MATRIX_VALUE}",
                error_type="http_status",
                status_code=500,
                errcode=f"M_UNKNOWN token={FAKE_MATRIX_VALUE}",
                attempt=3,
                raw_body=f"raw response body {FAKE_MATRIX_VALUE} Traceback raw_image_bytes abc",
            )

    matrix_client = UploadFailingMatrixClient()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    def matrix_factory(_settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixOutboxDelivery:
        return outbox_delivery(matrix_client, data_dir, logger)

    sleep_calls = 0

    def wait_for_failed_upload(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 6:
            assert matrix_client.failed_upload.wait(2)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=matrix_factory,
        sleep=wait_for_failed_upload,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    records = json_records(output)
    snapshot_files = list((tmp_path / "snapshots").glob("occupancy-open-event-left-spot-*.jpg"))
    failed = next(
        record
        for record in records
        if record["event"] == "matrix-outbox-phase-retryable-failure" and record.get("phase") == "upload"
    )

    assert exit_code == 0
    assert len(snapshot_files) == 1
    assert snapshot_files[0].read_bytes() == (tmp_path / "latest.jpg").read_bytes()
    assert state_status(tmp_path / "state.json", "left_spot") == "empty"
    assert failed["reason"] == "matrix_upload_http_500"
    assert failed["error_type"] == "MatrixError"
    assert '"event":"matrix-outbox-phase-retryable-failure"' in output
    assert '"event":"state-saved"' in output
    assert "Authorization" not in output
    assert "raw response body" not in output
    assert "Traceback" not in output
    assert "raw_image_bytes abc" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_low_confidence_in_spot_vehicle_suppresses_open_alert(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(Path("config.yaml.example").read_text(encoding="utf-8"), encoding="utf-8")
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.OCCUPIED,
                    hit_streak=3,
                    miss_streak=0,
                    last_bbox=(350.0, 200.0, 550.0, 330.0),
                    open_event_emitted=False,
                ),
                "right_spot": SpotOccupancyState(),
            }
        ),
    )

    class LowConfidenceDetector:
        def __init__(self) -> None:
            self.thresholds: list[float | None] = []

        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            self.thresholds.append(confidence_threshold)
            return [VehicleDetection(class_name="car", confidence=0.12, bbox=(350, 200, 550, 330))]

    detector = LowConfidenceDetector()

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: detector,
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    state = runtime_state_payload(state_path)["spots"]["left_spot"]
    assert exit_code == 0
    assert detector.thresholds == [0.1, 0.1, 0.1]
    assert state["status"] == "occupied"
    assert state["miss_streak"] == 0
    assert '"event":"occupancy-open-event"' not in output
    assert '"event":"spot-detection-miss-diagnostic"' not in output


def test_presence_by_spot_treats_small_in_spot_vehicle_as_release_suppression() -> None:
    small_vehicle = VehicleDetection(class_name="car", confidence=0.9, bbox=(10, 10, 20, 20))
    result = DetectionFilterResult(
        by_spot={
            "left_spot": SpotDetectionResult(
                spot_id="left_spot",
                accepted=None,
                rejected=[
                    RejectedDetection(
                        spot_id="left_spot",
                        detection=small_vehicle,
                        reason=RejectionReason.AREA_TOO_SMALL,
                    )
                ],
            ),
            "right_spot": SpotDetectionResult(spot_id="right_spot", accepted=None, rejected=[]),
        },
        rejection_counts={RejectionReason.AREA_TOO_SMALL: 1},
    )

    assert presence_by_spot(result) == {"left_spot": True, "right_spot": False}


def test_presence_by_spot_does_not_count_centroid_outside_vehicle() -> None:
    passing_vehicle = VehicleDetection(class_name="car", confidence=0.9, bbox=(10, 10, 100, 100))
    result = DetectionFilterResult(
        by_spot={
            "left_spot": SpotDetectionResult(
                spot_id="left_spot",
                accepted=None,
                rejected=[
                    RejectedDetection(
                        spot_id="left_spot",
                        detection=passing_vehicle,
                        reason=RejectionReason.CENTROID_OUTSIDE,
                    )
                ],
            )
        },
        rejection_counts={RejectionReason.CENTROID_OUTSIDE: 1},
    )

    assert presence_by_spot(result) == {"left_spot": False}

def test_runtime_loop_appends_sanitized_decision_memory_records(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detections = [[left_spot_vehicle()]]
    delivery = FakeMatrixDelivery()
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore

    batch_calls: list[tuple[str, tuple[DecisionMemoryRecord, ...]]] = []
    original_extend = DecisionMemoryStore.extend

    def track_batch_append(
        store: DecisionMemoryStore,
        records: Sequence[DecisionMemoryRecord],
        *,
        durability: str,
    ) -> bool:
        batch_calls.append((durability, tuple(records)))
        return original_extend(store, records, durability=durability)  # type: ignore[arg-type]

    monkeypatch.setattr(DecisionMemoryStore, "extend", track_batch_append)

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    memory_path = tmp_path / "operator-decision-memory.json"
    loaded = load_decision_memory(memory_path)
    rendered = memory_path.read_text(encoding="utf-8")

    assert exit_code == 0
    assert loaded.state == "available"
    assert any(record.kind == "accepted_evidence" and record.spot_id == "left_spot" for record in loaded.records)
    assert any(record.kind == "miss" and record.spot_id == "right_spot" for record in loaded.records)
    assert any(record.details and record.details.get("hit_streak") == 1 for record in loaded.records)
    routine_batches = [records for durability, records in batch_calls if durability == "routine" and len(records) == 4]
    assert len(routine_batches) == 1
    frame_records = routine_batches[0]
    assert len(frame_records) == 4
    assert [record.spot_id for record in frame_records].count("left_spot") == 2
    assert [record.spot_id for record in frame_records].count("right_spot") == 2
    assert all(len(record.summary.encode("utf-8")) <= MAX_TEXT_FIELD_CHARS for record in frame_records)
    batch_rendered = json.dumps([record.to_json_dict() for record in frame_records])
    assert_no_secret_leak(output + rendered + batch_rendered)


def test_runtime_transition_decision_memory_is_immediately_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("confirm_frames: 3", "confirm_frames: 1")
        .replace("health_file: health.json", f"health_file: {tmp_path / 'health.json'}"),
        encoding="utf-8",
    )
    calls: list[tuple[str, tuple[DecisionMemoryRecord, ...]]] = []
    original_extend = DecisionMemoryStore.extend

    def tracked(
        store: DecisionMemoryStore,
        records: Sequence[DecisionMemoryRecord],
        *,
        durability: str,
    ) -> bool:
        calls.append((durability, tuple(records)))
        return original_extend(store, records, durability=durability)  # type: ignore[arg-type]

    monkeypatch.setattr(DecisionMemoryStore, "extend", tracked)

    assert _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=lambda _settings: type(
            "Detector",
            (),
            {"detect": lambda self, _path, **_kwargs: [left_spot_vehicle()]},
        )(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    ) == 0

    transition_batches = [
        records
        for durability, records in calls
        if durability == "immediate"
        and any(
            record.details
            and record.details.get("previous_status") == "unknown"
            and record.details.get("new_status") == "occupied"
            for record in records
        )
    ]
    assert len(transition_batches) == 1
    persisted = load_decision_memory(tmp_path / "operator-decision-memory.json").records
    assert any(record.details and record.details.get("new_status") == "occupied" for record in persisted)


def test_runtime_checkpoints_decision_memory_once_per_success_and_failed_iteration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore

    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={"health_file": tmp_path / "health.json"}
            )
        }
    )
    store = DecisionMemoryStore(
        tmp_path / "operator-decision-memory.json",
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=50,
    )
    checkpoint_calls = 0
    original_checkpoint = DecisionMemoryStore.checkpoint_if_due

    def tracked_checkpoint(selected: DecisionMemoryStore) -> bool:
        nonlocal checkpoint_calls
        checkpoint_calls += 1
        return original_checkpoint(selected)

    monkeypatch.setattr(DecisionMemoryStore, "checkpoint_if_due", tracked_checkpoint)
    captures = 0

    def capture(_settings: object, data_dir: str | Path, **_kwargs: object) -> FrameCaptureResult:
        nonlocal captures
        captures += 1
        if captures == 1:
            return captured_frame(Path(data_dir))
        raise CaptureError(
            reason="ffmpeg_error",
            mode=DecodeMode.SOFTWARE,
            output_path=Path(data_dir) / "latest.jpg",
            message="capture unavailable",
        )

    assert run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        sleep=lambda _seconds: None,
        max_iterations=2,
        decision_memory_store=store,
    ) == 0
    assert checkpoint_calls == 2


@pytest.mark.parametrize("first_capture_fails", [False, True])
def test_runtime_fallback_decision_store_uses_injected_checkpoint_clock(
    tmp_path: Path,
    first_capture_fails: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import parking_spot_monitor.capture_loop as capture_loop_module
    from parking_spot_monitor.decision_memory_runtime import (
        runtime_decision_memory_store as real_store_factory,
    )
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
    from parking_spot_monitor.operator_decision_memory import make_decision_memory_record

    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "frame_interval_seconds": 10,
                    "stable_frame_interval_seconds": 10,
                    "adaptive_polling_enabled": False,
                    "decision_memory_checkpoint_interval_seconds": 5,
                }
            ),
            "stream": settings.stream.model_copy(
                update={
                    "reconnect_seconds": 10,
                    "reconnect_max_seconds": 10,
                    "reconnect_jitter_ratio": 0,
                }
            ),
        }
    )
    save_runtime_state(
        tmp_path / "state.json",
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY, miss_streak=3
                ),
                "right_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY, miss_streak=3
                ),
            }
        ),
    )
    clock = [0.0]
    stores: list[DecisionMemoryStore] = []
    captures = 0

    def store_factory(*args, **kwargs) -> DecisionMemoryStore:
        store = real_store_factory(*args, **kwargs)
        stores.append(store)
        return store

    monkeypatch.setattr(
        capture_loop_module, "runtime_decision_memory_store", store_factory
    )

    def capture(
        _settings: object, data_dir: str | Path, **_kwargs: object
    ) -> FrameCaptureResult:
        nonlocal captures
        captures += 1
        if captures == 1:
            assert stores[0].append(
                make_decision_memory_record("miss", summary="fallback clock probe"),
                durability="routine",
            )
            if first_capture_fails:
                raise CaptureError(
                    reason="ffmpeg_error",
                    mode=DecodeMode.SOFTWARE,
                    output_path=Path(data_dir) / "latest.jpg",
                    message="capture unavailable",
                )
        else:
            assert any(
                record.summary == "fallback clock probe"
                for record in load_decision_memory(
                    tmp_path / "operator-decision-memory.json"
                ).records
            )
        return captured_frame(Path(data_dir), timestamp="2026-05-19T19:00:00Z")

    def sleep(seconds: float) -> None:
        clock[0] += seconds

    assert run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        sleep=sleep,
        max_iterations=2,
        now=lambda: datetime(2026, 5, 19, 19, 0, tzinfo=timezone.utc),
        monotonic=lambda: clock[0],
        random_unit=lambda: 0.5,
    ) == 0
    assert captures == 2


@pytest.mark.parametrize("capture_fails", [False, True])
def test_runtime_wait_wakes_at_dirty_decision_checkpoint_without_changing_cadence(
    tmp_path: Path,
    capture_fails: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import parking_spot_monitor.operator_decision_memory as decision_memory
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
    from parking_spot_monitor.operator_decision_memory import make_decision_memory_record

    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "frame_interval_seconds": 600,
                    "stable_frame_interval_seconds": 600,
                    "adaptive_polling_enabled": False,
                }
            ),
            "stream": settings.stream.model_copy(
                update={
                    "reconnect_seconds": 600,
                    "reconnect_max_seconds": 600,
                    "reconnect_jitter_ratio": 0,
                }
            ),
        }
    )
    clock = [0.0]
    sleeps: list[float] = []
    memory_path = tmp_path / "operator-decision-memory.json"
    store = DecisionMemoryStore(
        memory_path,
        checkpoint_interval_seconds=5,
        checkpoint_max_pending_records=50,
        monotonic=lambda: clock[0],
    )
    real_write = decision_memory._write_memory
    checkpoint_timed = False

    def timed_write(path: Path, records: Sequence[DecisionMemoryRecord]) -> None:
        nonlocal checkpoint_timed
        if sleeps and not checkpoint_timed:
            checkpoint_timed = True
            clock[0] += 10
        real_write(path, records)

    monkeypatch.setattr(decision_memory, "_write_memory", timed_write)

    def capture(_settings: object, data_dir: str | Path, **_kwargs: object) -> FrameCaptureResult:
        store.append(
            make_decision_memory_record("miss", summary="deadline probe"),
            durability="routine",
        )
        if capture_fails:
            raise CaptureError(
                reason="ffmpeg_error",
                mode=DecodeMode.SOFTWARE,
                output_path=Path(data_dir) / "latest.jpg",
                message="capture unavailable",
            )
        return captured_frame(Path(data_dir))

    def sleep(seconds: float) -> None:
        if sleeps:
            assert any(
                record.summary == "deadline probe"
                for record in load_decision_memory(memory_path).records
            )
        sleeps.append(seconds)
        clock[0] += seconds

    assert run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        sleep=sleep,
        max_iterations=1,
        monotonic=lambda: clock[0],
        random_unit=lambda: 0.5,
        decision_memory_store=store,
    ) == 0
    assert sleeps == [5, 585]
    assert clock[0] == 600


def test_runtime_loop_decision_memory_append_failure_is_non_fatal(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "operator-decision-memory.json").mkdir()
    (tmp_path / "operator-decision-memory.json.quarantine").mkdir()
    ((tmp_path / "operator-decision-memory.json.quarantine") / "existing").write_text("block quarantine", encoding="utf-8")

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)

    assert exit_code == 0
    assert (tmp_path / "state.json").exists()
    assert "operator-decision-memory-append-failed" in output
    assert_no_secret_leak(output)

def test_default_matrix_command_service_wires_detection_lab_to_effective_paths_and_memory(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "matrix": settings.matrix.model_copy(update={"command_authorized_senders": ["@operator:example.org"]})
        }
    )
    logger = StructuredLogger()
    archive = VehicleHistoryArchive(tmp_path / "vehicle-history", logger=logger)

    service = _default_matrix_command_service_factory(
        settings,
        tmp_path,
        logger,
        archive,
        incident_detector=object(),
    )

    assert service is not None
    context = service.cockpit_context
    assert context is not None
    assert context.data_dir == tmp_path
    assert context.detection_lab_manager is not None
    assert context.detection_lab_manager.lab_root == tmp_path / "detection-lab"

    response = context.lab_run_reply("replay", logger=logger)
    loaded = load_decision_memory(tmp_path / "operator-decision-memory.json")
    output = combined_output(capsys)

    assert "Detection lab job started" in response.text
    assert loaded.state == "available"
    lab_records = [record for record in loaded.records if record.kind == "lab_outcome"]
    assert lab_records
    assert lab_records[-1].details is not None
    assert lab_records[-1].details.get("kind") == "replay"
    assert lab_records[-1].details.get("status") == "blocked"
    assert lab_records[-1].details.get("phase") == "validate_inputs"
    assert "detection-lab-outcome-recorded" in output
    assert not (tmp_path / "state.json").exists()
    assert_no_secret_leak(output + (tmp_path / "operator-decision-memory.json").read_text(encoding="utf-8"))


def test_default_matrix_command_service_wires_feedback_who_snapshot_and_incident_replay_detector(tmp_path: Path) -> None:
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={"matrix": settings.matrix.model_copy(update={"command_authorized_senders": ["@op:example"]})}
    )
    logger = StructuredLogger()
    archive = VehicleHistoryArchive(tmp_path / "vehicle-history", logger=logger)

    incident_detector = object()
    service = _default_matrix_command_service_factory(
        settings,
        tmp_path,
        logger,
        archive,
        incident_detector=incident_detector,
    )

    assert service is not None
    assert service.feedback_labeler is not None
    assert service.who_snapshot_provider is not None
    assert service.cockpit_context is not None
    assert service.cockpit_context.incident_detector is incident_detector
    assert not (tmp_path / "latest.jpg").exists()
    assert not (tmp_path / "state.json").exists()


def test_default_matrix_command_service_defers_incident_detector_construction_until_replay(
    tmp_path: Path,
) -> None:
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={"matrix": settings.matrix.model_copy(update={"command_authorized_senders": ["@op:example"]})}
    )
    logger = StructuredLogger()
    archive = VehicleHistoryArchive(tmp_path / "vehicle-history", logger=logger)
    factory_calls: list[Any] = []
    detect_calls: list[Path] = []

    class Detector:
        def detect(self, frame_path: str | Path, **kwargs: Any) -> list[Any]:
            detect_calls.append(Path(frame_path))
            return []

    def detector_factory(loaded_settings: object) -> Detector:
        factory_calls.append(loaded_settings)
        return Detector()

    shared_detector = SharedLazyDetector(lambda: detector_factory(settings))
    service = _default_matrix_command_service_factory(
        settings,
        tmp_path,
        logger,
        archive,
        incident_detector=shared_detector,
    )

    assert service is not None
    assert service.cockpit_context is not None
    assert service.cockpit_context.incident_detector is shared_detector
    assert factory_calls == []

    missing_frame_response = service.cockpit_context.incident_review_reply(
        spot_id="left_spot",
        incident_time="2026-05-18T02:39:00Z",
        logger=logger,
    )
    assert "Nearest retained frame: unavailable" in missing_frame_response.text
    assert factory_calls == []

    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    frame = frames_dir / "20260518T023900Z.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    replay_response = service.cockpit_context.incident_review_reply(
        spot_id="left_spot",
        incident_time="2026-05-18T02:39:00Z",
        logger=logger,
    )

    assert factory_calls == [settings]
    assert detect_calls == [frame]
    assert "Detector replay: no vehicle evidence" in replay_response.text


def test_runtime_and_default_incident_replay_share_one_lazy_detector_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor import __main__ as cli
    from parking_spot_monitor.detector_adapter import SharedLazyDetector

    constructed: list[object] = []
    incident_owners: list[SharedLazyDetector] = []

    class Detector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
        ) -> list[VehicleDetection]:
            return []

    def detector_factory(_settings: object) -> Detector:
        backend = Detector()
        constructed.append(backend)
        return backend

    def command_factory(
        _settings: object,
        _data_dir: Path,
        _logger: StructuredLogger,
        _archive: object,
        *,
        incident_detector: SharedLazyDetector,
        decision_memory_store: object,
    ) -> None:
        incident_owners.append(incident_detector)
        assert decision_memory_store is not None
        return None

    monkeypatch.setattr(cli, "_default_matrix_command_service_factory", command_factory)

    exit_code = cli._main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert len(incident_owners) == 1
    assert incident_owners[0].loaded is True
    incident_owners[0].detect_path(
        tmp_path / "incident.jpg",
        confidence_threshold=0.1,
        inference_image_size=640,
    )
    assert len(constructed) == 1


def test_startup_summary_includes_sanitized_detection_lab_dir(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = _main(["--config", "config.yaml.example", "--data-dir", "/tmp/parking-data", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)

    assert exit_code == 0
    assert '"detection_lab_dir":"/tmp/parking-data/detection-lab"' in output
    assert_no_secret_leak(output)


def test_default_matrix_command_service_uses_short_independent_client_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from parking_spot_monitor import __main__ as cli
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    captured: list[dict[str, Any]] = []

    class Client:
        def __init__(self, **kwargs: Any) -> None:
            captured.append(kwargs)

        def close(self) -> None:
            return None

    monkeypatch.setattr(cli, "MatrixClient", Client)
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "matrix": settings.matrix.model_copy(
                update={"command_authorized_senders": ["@operator:example.org"]}
            )
        }
    )

    delivery = cli._default_matrix_delivery_factory(
        settings, tmp_path, StructuredLogger()
    )

    service = _default_matrix_command_service_factory(
        settings,
        tmp_path,
        StructuredLogger(),
        VehicleHistoryArchive(tmp_path / "vehicle-history"),
        incident_detector=object(),
    )

    assert service is not None
    assert captured[0]["timeout_seconds"] == 10
    assert captured[0]["retry_attempts"] == 3
    assert captured[1]["timeout_seconds"] == 2
    assert captured[1]["retry_attempts"] == 1
    service.close()
    delivery.close()




def test_validate_config_does_not_construct_matrix_outbox_or_touch_network(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def forbidden_matrix_factory(_settings: object, _data_dir: Path, _logger: StructuredLogger) -> object:
        raise AssertionError("validate-config must not construct Matrix delivery")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--validate-config"],
        environ=fake_environ(),
        matrix_delivery_factory=forbidden_matrix_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert not (tmp_path / "matrix-outbox.json").exists()
    assert "matrix-outbox" not in output
    assert_no_secret_leak(output)


def test_matrix_outbox_health_payload_quarantines_corrupt_json_without_raw_secret(tmp_path: Path) -> None:
    outbox_path = tmp_path / "matrix-outbox.json"
    outbox_path.write_text('{"items": [Authorization: Bearer matrix-secret', encoding="utf-8")

    payload = _matrix_outbox_health_payload(outbox_path)

    assert payload is not None
    assert payload["available"] is True
    assert payload["counts_by_state"] == {}
    assert payload["recovery"]["quarantined_count"] == 1
    assert payload["recovery"]["reason_counts"] == {"invalid_json": 1}
    rendered = json.dumps(payload).lower()
    assert "authorization" not in rendered
    assert "bearer" not in rendered
    assert "matrix-secret" not in rendered


def test_matrix_outbox_health_payload_degrades_on_read_error_without_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ExplodingOutbox:
        def __init__(self, _path: Path) -> None:
            raise OSError("permission denied access_token=matrix-secret")

    monkeypatch.setattr("parking_spot_monitor.runtime_health.LocalOutbox", ExplodingOutbox)

    payload = _matrix_outbox_health_payload(tmp_path / "matrix-outbox.json")

    assert payload is not None
    assert payload["available"] is False
    assert payload["phase"] == "matrix-outbox"
    assert payload["error"]["phase"] == "matrix-outbox"
    assert payload["error"]["action"] == "status-summary"
    rendered = json.dumps(payload).lower()
    assert "access_token" not in rendered
    assert "matrix-secret" not in rendered


def test_matrix_outbox_health_payload_strips_record_items_from_live_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ExplodingOutbox:
        def __init__(self, _path: Path) -> None:
            raise AssertionError("live provider must avoid reopening the outbox")

    monkeypatch.setattr("parking_spot_monitor.runtime_health.LocalOutbox", ExplodingOutbox)

    payload = _matrix_outbox_health_payload(
        tmp_path / "matrix-outbox.json",
        summary_provider=lambda: {
            "path": str(tmp_path / "matrix-outbox.json"),
            "total": 1,
            "counts_by_state": {"delivered": 1},
            "items": [{"id": "event-1", "body": "record-level data"}],
        },
    )

    assert payload == {
        "path": str(tmp_path / "matrix-outbox.json"),
        "total": 1,
        "counts_by_state": {"delivered": 1},
        "available": True,
    }
    assert "items" not in payload


def test_matrix_outbox_health_payload_exposes_only_safe_worker_fields(tmp_path: Path) -> None:
    payload = _matrix_outbox_health_payload(
        tmp_path / "matrix-outbox.json",
        summary_provider=lambda: {
            "total": 0,
            "counts_by_state": {},
            "worker_running": True,
            "worker_last_attempt_at": "2026-07-29T17:00:00Z",
            "worker_last_error_type": "RuntimeError",
            "worker_error_message": "Authorization: Bearer matrix-secret",
        },
    )

    assert payload is not None
    assert payload["worker_running"] is True
    assert payload["worker_last_attempt_at"] == "2026-07-29T17:00:00Z"
    assert payload["worker_last_error_type"] == "RuntimeError"
    assert "worker_error_message" not in payload
    rendered = json.dumps(payload)
    assert "Authorization" not in rendered
    assert "Bearer" not in rendered
    assert "matrix-secret" not in rendered


def test_runtime_health_json_includes_resolved_matrix_outbox_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    pending = outbox.enqueue(AlertIntent(event_id="evt-pending", phase="text", body="ok"))
    retrying = outbox.enqueue(AlertIntent(event_id="evt-retrying", phase="upload", body="ok"))
    delivered = outbox.enqueue(AlertIntent(event_id="evt-delivered", phase="image", body="ok"))
    failed = outbox.enqueue(AlertIntent(event_id="evt-failed", phase="text", body="ok"))
    dead = outbox.enqueue(AlertIntent(event_id="evt-dead", phase="text", body="ok"))
    outbox.mark_retrying(retrying.id, reason="timeout")
    outbox.mark_delivered(delivered.id)
    outbox.mark_failed(failed.id, reason="matrix_forbidden")
    outbox.mark_dead_lettered(dead.id, reason="Authorization: Bearer matrix-secret")
    assert pending.state == "pending"

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=0,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    matrix_outbox = health["matrix_outbox"]
    assert exit_code == 0
    assert matrix_outbox["available"] is True
    assert matrix_outbox["path"] == str(tmp_path / "matrix-outbox.json")
    assert matrix_outbox["counts_by_state"] == {
        "pending": 1,
        "retrying": 1,
        "delivered": 1,
        "failed": 1,
        "dead_lettered": 1,
    }
    assert matrix_outbox["retry_reason_counts"] == {"timeout": 1}
    assert matrix_outbox["dead_letter_reason_counts"] == {"matrix_forbidden": 1, "redacted": 1}
    assert "items" not in matrix_outbox
    rendered = json.dumps(health).lower()
    assert "authorization" not in rendered
    assert "bearer" not in rendered
    assert "matrix-secret" not in rendered
    assert_no_secret_leak(output)


def test_runtime_loop_closes_matrix_services_on_exit(tmp_path: Path) -> None:
    closed: list[str] = []

    class CloseableDelivery(FakeMatrixDelivery):
        def close(self) -> None:
            closed.append("delivery")

    class CloseableCommands:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult()

        def close(self) -> None:
            closed.append("commands")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: CloseableDelivery(),
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, _archive: CloseableCommands(),
        sleep=lambda _seconds: None,
        max_iterations=0,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert closed == ["commands", "delivery"]


def test_runtime_loop_preserves_injected_falsey_history_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import parking_spot_monitor.capture_loop as capture_loop_module
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    class FalseyArchive(VehicleHistoryArchive):
        def __bool__(self) -> bool:
            return False

    supplied = FalseyArchive(tmp_path / "supplied-history")

    def forbidden_fallback(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("falsey injected archive was replaced")

    monkeypatch.setattr(capture_loop_module, "VehicleHistoryArchive", forbidden_fallback)
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={"runtime": settings.runtime.model_copy(update={"health_file": tmp_path / "health.json"})}
    )

    assert run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=lambda *_args, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        history_archive=supplied,
        sleep=lambda _seconds: None,
        max_iterations=0,
    ) == 0


def test_runtime_teardown_cancels_command_worker_before_service_close(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    release = threading.Event()

    class Commands:
        def fetch_once(self) -> MatrixSyncResult:
            release.wait(1)
            return MatrixSyncResult(next_batch="s1", events=())

        def apply_sync_result(self, _result: MatrixSyncResult) -> FakeCommandPollResult:
            return FakeCommandPollResult()

        def cancel_pending(self) -> None:
            events.append("worker-cancel")
            release.set()

        def close(self) -> None:
            events.append("service-close")

    commands = Commands()
    assert _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda *_args: FakeMatrixDelivery(),
        matrix_command_service_factory=lambda *_args: commands,
        sleep=lambda _seconds: None,
        max_iterations=1,
    ) == 0

    assert events == ["worker-cancel", "service-close"]


def test_default_matrix_delivery_factory_starts_one_outbox_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor import __main__ as cli

    class FactoryClient(FakeMatrixClient):
        def __init__(self, **_kwargs: Any) -> None:
            super().__init__()

        def close(self) -> None:
            return None

    monkeypatch.setattr(cli, "MatrixClient", FactoryClient)
    settings = load_settings("config.yaml.example", environ=fake_environ())

    delivery = cli._default_matrix_delivery_factory(settings, tmp_path, StructuredLogger())
    worker = delivery.worker_thread
    try:
        assert worker is not None
        assert worker.is_alive() is True
        delivery.start_worker(retry_interval_seconds=settings.matrix.outbox_retry_interval_seconds)
        assert delivery.worker_thread is worker
    finally:
        delivery.close()


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


def test_runtime_open_alert_failure_persists_retryable_matrix_outbox(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], [], []]
    matrix_client = UploadFailsOnceMatrixClient()

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    sleep_calls = 0

    def wait_for_worker_on_last_iteration(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 7:
            assert matrix_client.image_sent.wait(2)
            assert matrix_client.failed_upload.wait(2)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, data_dir, logger: outbox_delivery(matrix_client, data_dir, logger),
        sleep=wait_for_worker_on_last_iteration,
        max_iterations=7,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    summary = outbox.status_summary()
    item = next(item for item in summary["items"] if item["state"] == "retrying")
    phases = {phase["phase"]: phase for phase in item["phases"]}

    assert exit_code == 0
    assert summary["counts_by_state"] == {"delivered": 3, "retrying": 1}
    assert phases["upload"]["state"] == "pending"
    assert phases["image"]["state"] == "pending"
    occupancy_text_kinds = [text["txn_id"].split(":", 1)[0] for text in matrix_client.texts if text["txn_id"].startswith("occupancy-")]
    assert occupancy_text_kinds == ["occupancy-occupied-event", "occupancy-open-event"]
    assert len(matrix_client.uploads) == 1
    assert matrix_client.uploads[0]["filename"].startswith("occupancy-occupied-event-")
    assert len(matrix_client.images) == 1
    assert matrix_client.images[0]["txn_id"].startswith("occupancy-occupied-event:")
    assert '"event":"matrix-outbox-phase-retryable-failure"' in output
    assert_no_secret_leak(output)


def test_runtime_worker_restarts_existing_matrix_outbox_without_new_occupancy_event(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # First invocation leaves a retryable record before Matrix media upload.
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], [], []]
    failing_client = UploadFailsOnceMatrixClient()

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    sleep_calls = 0

    def wait_for_failed_worker_pass(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 7:
            assert failing_client.image_sent.wait(2)
            assert failing_client.failed_upload.wait(2)

    first_exit = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, data_dir, logger: outbox_delivery(failing_client, data_dir, logger),
        sleep=wait_for_failed_worker_pass,
        max_iterations=7,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )
    assert first_exit == 0
    capsys.readouterr()

    successful_client = FakeMatrixClient()

    def forbidden_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        raise AssertionError("startup drain with max_iterations=0 must not capture a new frame")

    def _started_delivery_after_restart(
        client: FakeMatrixClient,
        data_dir: Path,
        logger: StructuredLogger,
    ) -> MatrixOutboxDelivery:
        delivery = outbox_delivery(
            client,
            data_dir,
            logger,
            utc_now=lambda: datetime.now(timezone.utc) + timedelta(seconds=120),
        )
        assert client.image_sent.wait(2), "restarted worker did not finish durable delivery"
        return delivery

    second_exit = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=forbidden_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, data_dir, logger: _started_delivery_after_restart(
            successful_client,
            data_dir,
            logger,
        ),
        sleep=lambda _seconds: None,
        max_iterations=0,
        now=lambda: datetime(2026, 5, 18, 19, 5, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    summary = outbox.status_summary()
    record = next(record for record in outbox.list_records() if record.intent.event_id.startswith("occupancy-open-event:"))
    phases = {phase: {"state": state} for phase, state in record.phase_states.items()}

    assert second_exit == 0
    assert summary["counts_by_state"] == {"delivered": 5}
    assert phases["upload"]["state"] == "delivered"
    assert phases["image"]["state"] == "delivered"
    assert [text for text in successful_client.texts if text["txn_id"].startswith("occupancy-open-event:")] == []
    assert len([text for text in successful_client.texts if text["txn_id"].startswith("parking-monitor-started:")]) == 1
    assert len(successful_client.uploads) == 1
    assert len(successful_client.images) == 1
    assert '"event":"matrix-outbox-record-delivered"' in output
    assert '"attempted_count":1' in output
    assert_no_secret_leak(output)
