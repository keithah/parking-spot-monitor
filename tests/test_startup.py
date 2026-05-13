from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from parking_spot_monitor.capture import CaptureError, DecodeMode, FrameCaptureResult
from parking_spot_monitor.config import load_settings
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix import MatrixDelivery
from parking_spot_monitor.__main__ import _main, _presence_by_spot, main
from parking_spot_monitor.detection import DetectionError, DetectionFilterResult, RejectedDetection, RejectionReason, SpotDetectionResult, VehicleDetection
from parking_spot_monitor.errors import ConfigError
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
from parking_spot_monitor.state import RuntimeState, save_runtime_state


SECRET_MARKER = "startup-secret-should-not-leak"
FAKE_RTSP_VALUE = f"camera-value-{SECRET_MARKER}"
FAKE_MATRIX_VALUE = f"matrix-value-{SECRET_MARKER}"


def fake_environ(**overrides: str) -> dict[str, str]:
    environ = {
        "RTSP_URL": FAKE_RTSP_VALUE,
        "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_VALUE,
    }
    environ.update(overrides)
    return environ


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
    )


def left_spot_vehicle() -> VehicleDetection:
    return VehicleDetection(class_name="car", confidence=0.9, bbox=(350, 200, 550, 330))


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


class NoopDetector:
    def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
        return []


def noop_detector_factory(_settings: object) -> NoopDetector:
    return NoopDetector()


class FakeMatrixClient:
    def __init__(self) -> None:
        self.texts: list[dict[str, Any]] = []
        self.uploads: list[dict[str, Any]] = []
        self.images: list[dict[str, Any]] = []

    def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
        self.texts.append({"room_id": room_id, "txn_id": txn_id, "body": body})
        return f"${txn_id}:example.org"

    def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
        self.uploads.append({"filename": filename, "data": data, "content_type": content_type})
        return f"mxc://example.org/{filename}"

    def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
        self.images.append(
            {"room_id": room_id, "txn_id": txn_id, "body": body, "content_uri": content_uri, "info": dict(info)}
        )
        return f"${txn_id}:example.org"

class FakeMatrixDelivery:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.quiet_notices: list[dict[str, Any]] = []
        self.open_alerts: list[dict[str, Any]] = []
        self.occupied_alerts: list[dict[str, Any]] = []
        self.live_proofs: list[dict[str, Any]] = []

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


class FakeCommandPollResult:
    def __init__(self, *, processed_count: int = 0, ignored_count: int = 0, error_count: int = 0, bootstrapped: bool = False) -> None:
        self.next_batch = "fake-next"
        self.processed_count = processed_count
        self.ignored_count = ignored_count
        self.error_count = error_count
        self.bootstrapped = bootstrapped



def test_process_detection_scales_configured_polygons_to_actual_frame_size(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    frame = tmp_path / "low-res-latest.jpg"
    Image.new("RGB", (640, 360), (20, 30, 40)).save(frame, format="JPEG")

    class LowResDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return [VehicleDetection(class_name="car", confidence=0.9, bbox=(142.0, 91.0, 265.0, 151.0))]

    from parking_spot_monitor.__main__ import _process_detection_for_capture

    settings = load_settings("config.yaml.example", environ=fake_environ())
    result = _process_detection_for_capture(
        settings,
        LowResDetector(),
        frame,
        logger=StructuredLogger(),
        mode="test",
    )

    output = combined_output(capsys)
    assert result.by_spot["left_spot"].accepted is not None
    assert result.by_spot["right_spot"].accepted is None
    assert '"frame_size_mismatch":true' in output
    assert '"configured_frame_size":{"height":806,"width":1458}' in output
    assert '"actual_frame_size":{"height":360,"width":640}' in output
    assert '"accepted_by_spot":{"left_spot":true,"right_spot":false}' in output


def test_runtime_loop_matrix_state_change_skip_log_explains_policy(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

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


def test_runtime_loop_matrix_quiet_window_start_notice_sent_once_by_event_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

    def matrix_factory(_settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixDelivery:
        return MatrixDelivery(
            client=matrix_client,  # type: ignore[arg-type]
            room_id="!room:example.org",
            data_dir=data_dir,
            snapshots_dir=tmp_path / "snapshots",
            logger=logger,
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=matrix_factory,
        sleep=lambda _seconds: None,
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
    assert '"event":"matrix-snapshot-copied"' in output
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

    def matrix_factory(_settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixDelivery:
        return MatrixDelivery(
            client=matrix_client,  # type: ignore[arg-type]
            room_id="!room:example.org",
            data_dir=data_dir,
            snapshots_dir=tmp_path / "snapshots",
            logger=logger,
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=matrix_factory,
        sleep=lambda _seconds: None,
        max_iterations=4,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    active_files = list((tmp_path / "vehicle-history" / "sessions" / "active").glob("*.json"))
    active_payload = json.loads(active_files[0].read_text(encoding="utf-8"))
    snapshot_files = list((tmp_path / "snapshots").glob("occupancy-occupied-event-left-spot-*.jpg"))
    assert exit_code == 0
    assert len(matrix_client.texts) == 2
    assert len(matrix_client.uploads) == 1
    assert len(matrix_client.images) == 1
    assert len(snapshot_files) == 1
    assert snapshot_files[0].read_bytes() == Path(active_payload["occupied_snapshot_path"]).read_bytes()
    assert matrix_client.uploads[0]["data"] == Path(active_payload["occupied_snapshot_path"]).read_bytes()
    reminder_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("quiet-window-upcoming:")]
    occupied_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("occupancy-occupied-event:")]
    assert len(reminder_texts) == 1
    assert reminder_texts[0]["body"] == "Street sweeping starts in 1 hour: street_sweeping:2026-05-18:13:00-15:00"
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
            return detections.pop(0)

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    def matrix_factory(_settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixDelivery:
        return MatrixDelivery(
            client=matrix_client,  # type: ignore[arg-type]
            room_id="!room:example.org",
            data_dir=data_dir,
            snapshots_dir=tmp_path / "snapshots",
            logger=logger,
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=matrix_factory,
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, runtime_archive: MergeRenameCommandService(runtime_archive),
        sleep=lambda _seconds: None,
        max_iterations=7,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    active_files = list((history_root / "sessions" / "active").glob("*.json"))
    closed_files = sorted((history_root / "sessions" / "closed").glob("*.json"))
    current_closed = [path for path in closed_files if not path.stem.startswith("seed-")]
    health = health_payload(tmp_path / "health.json")
    vehicle_health = health["vehicle_history"]
    occupied_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("occupancy-occupied-event:")]
    open_texts = [text for text in matrix_client.texts if text["txn_id"].startswith("occupancy-open-event:")]
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

    assert len(occupied_texts) == 1
    assert len(open_texts) == 1
    assert len(occupied_uploads) == 1
    assert len(open_uploads) == 1
    assert len(occupied_images) == 1
    assert len(open_images) == 1
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

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
    assert health["last_vehicle_history_error"]["phase"] == "matrix-command"
    assert health["last_vehicle_history_error"]["action"] == "vehicle-history-correction"
    assert '"event":"matrix-command-poll-failed"' in output
    assert_no_secret_leak(output)



def test_runtime_loop_vehicle_history_close_failure_degrades_health_without_blocking_open_alert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery()

    class FailingCloseHistoryArchive:
        def __init__(self, _root: Path, *, logger: StructuredLogger | None = None) -> None:
            self.logger = logger

        def health_snapshot(self) -> dict[str, Any]:
            return {"archive_status": "test-double"}

        def start_session(self, event: object) -> object:
            return type("SessionRecord", (), {"session_id": "session-left"})()

        def attach_occupied_images(self, **_kwargs: object) -> object:
            return type(
                "SessionRecord",
                (),
                {"session_id": "session-left", "occupied_snapshot_path": "/safe/full.jpg", "occupied_crop_path": "/safe/crop.jpg"},
            )()

        def match_or_create_profile(self, *, session_id: str) -> object:
            return type(
                "ProfileAssignment",
                (),
                {"session_id": session_id, "status": "matched", "profile_id": "prof-left", "profile_confidence": 0.98},
            )()

        def close_session(self, event: object) -> object:
            raise PermissionError(f"history close denied token={SECRET_MARKER} raw_image_bytes abc")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

    monkeypatch.setattr(cli, "VehicleHistoryArchive", FailingCloseHistoryArchive)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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
        )

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

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
    assert health["status"] == "degraded"
    assert health["vehicle_history_failure_count"] == 1
    assert health["last_vehicle_history_error"]["action"] == "attach-images"
    assert health["last_vehicle_history_error"]["image_phase"] == "image-capture"
    assert health["last_vehicle_history_error"]["spot_id"] == "left_spot"
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
            return detections.pop(0)

    monkeypatch.setattr(cli, "VehicleHistoryArchive", FailingProfileHistoryArchive)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
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
    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
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
        capture=lambda _settings, data_dir: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
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
        capture=lambda _settings, data_dir: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T20:30:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

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

    base = Path("config.yaml.example").read_text(encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(base.replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"), encoding="utf-8")

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
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

    def fail_unlink(self: Path) -> None:
        raise PermissionError(f"permission denied token={FAKE_MATRIX_VALUE} raw_image_bytes abc")

    monkeypatch.setattr(Path, "unlink", fail_unlink)

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
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
    assert '"event":"capture-loop-frame-written"' in output
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

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        captured_data_dirs.append(Path(data_dir))
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class EmptyDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return []

    def matrix_factory(settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixDelivery:
        matrix_paths.append((data_dir, settings.storage.snapshots_dir))  # type: ignore[attr-defined]
        return MatrixDelivery(
            client=FakeMatrixClient(),  # type: ignore[arg-type]
            room_id="!room:example.org",
            data_dir=data_dir,
            snapshots_dir=settings.storage.snapshots_dir,  # type: ignore[attr-defined]
            logger=logger,
        )

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

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
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
            assert confidence_threshold == 0.35
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
    assert constructed == ["yolov8n.pt"]
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

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
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

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
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

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
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
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
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
    assert '"event":"capture-loop-frame-written"' in output
    assert '"event":"debug-overlay-failed"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_detector_failure_logs_and_continues(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_path = tmp_path / "latest.jpg"
    sleeps: list[float] = []

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
        )

    class FailingDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            raise DetectionError(
                f"predict failed matrix_token={SECRET_MARKER}",
                model_path="yolov8n.pt",
                frame_path=str(frame_path),
                phase="predict",
                error_type="RuntimeError",
            )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        detector_factory=lambda _settings: FailingDetector(),
        sleep=sleeps.append,
        max_iterations=1,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert sleeps == [30]
    assert '"event":"detection-frame-failed"' in output
    assert '"iteration":1' in output
    assert '"event":"capture-loop-frame-written"' in output
    assert '"event":"detection-frame-processed"' not in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_success_logs_detection_frame_processed_with_metadata(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_path = tmp_path / "latest.jpg"
    sleeps: list[float] = []

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        assert Path(data_dir) == tmp_path
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-02T03:04:05Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
        )

    class FakeDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            assert Path(frame_path) == latest_path
            assert confidence_threshold == 0.35
            return [VehicleDetection(class_name="truck", confidence=0.88, bbox=(350, 200, 550, 330))]

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        detector_factory=lambda _settings: FakeDetector(),
        sleep=sleeps.append,
        max_iterations=1,
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

    latest_path = tmp_path / "latest.jpg"

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
        assert Path(data_dir) == tmp_path
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2025-01-01T00:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--capture-once"],
        environ=fake_environ(),
        capture=fake_capture,
        detector_factory=noop_detector_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert latest_path.exists()
    assert '"event":"capture-once-complete"' in output
    assert '"selected_mode":"software"' in output
    assert '"mode":"capture-once"' in output
    assert_no_secret_leak(output)


def test_capture_once_failure_returns_nonzero_without_traceback_or_secret(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
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
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert sleeps == [5]
    assert '"event":"capture-loop-iteration"' in output
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
        Path("config.yaml.example").read_text(encoding="utf-8").replace("frame_interval_seconds: 30", "frame_interval_seconds: 2"),
        encoding="utf-8",
    )

    def fake_capture(_settings: object, data_dir: str | Path) -> FrameCaptureResult:
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
    assert '"event":"capture-loop-paced"' in output
    assert '"sleep_seconds":2' in output
    assert_no_secret_leak(output)


def test_runtime_loop_detection_failure_updates_health_without_advancing_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=3, miss_streak=0),
                "right_spot": SpotOccupancyState(),
            }
        ),
    )

    class FailingDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            raise DetectionError(
                f"predict failed token={SECRET_MARKER}",
                model_path="yolov8n.pt",
                frame_path=str(frame_path),
                phase="predict",
                error_type="RuntimeError",
            )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: FailingDetector(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert runtime_state_payload(state_path)["spots"]["left_spot"]["miss_streak"] == 0
    assert health["status"] == "degraded"
    assert health["consecutive_capture_failures"] == 0
    assert health["consecutive_detection_failures"] == 1
    assert health["last_error"]["phase"] == "detection"
    assert SECRET_MARKER not in json.dumps(health)
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_failure_updates_health_and_loop_continues(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery(fail=True)

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
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
    assert SECRET_MARKER not in json.dumps(health)
    assert '"event":"capture-loop-frame-written"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_state_save_failure_updates_health_and_loop_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    def fail_state_save(*_args: object, **_kwargs: object) -> None:
        raise PermissionError(f"state denied token={SECRET_MARKER} Traceback raw_image_bytes abc")

    monkeypatch.setattr(cli, "save_runtime_state", fail_state_save)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
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


def test_runtime_loop_state_save_failure_continues_from_previous_durable_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

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
    real_save_runtime_state = cli.save_runtime_state
    save_attempts = 0

    def fail_once_then_save(*args: object, **kwargs: object) -> None:
        nonlocal save_attempts
        save_attempts += 1
        if save_attempts == 1:
            raise PermissionError(f"state denied token={SECRET_MARKER} Traceback raw_image_bytes abc")
        real_save_runtime_state(*args, **kwargs)

    monkeypatch.setattr(cli, "save_runtime_state", fail_once_then_save)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
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
    assert payload["spots"]["left_spot"]["status"] == "occupied"
    assert payload["spots"]["left_spot"]["miss_streak"] == 2
    assert payload["spots"]["left_spot"]["open_event_emitted"] is False
    assert "occupancy-open-event" not in event_names(output)
    assert_no_secret_leak(output)


def test_runtime_loop_health_write_failure_logs_safely_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    def fail_health_write(*_args: object, **_kwargs: object) -> None:
        raise PermissionError(f"health denied token={SECRET_MARKER} Traceback raw_image_bytes abc")

    monkeypatch.setattr(cli, "write_health_status", fail_health_write, raising=False)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
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
    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T20:30:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
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
        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            self.uploads.append({"filename": filename, "data": data, "content_type": content_type})
            raise MatrixError(
                f"Matrix upload failed Authorization: Bearer {FAKE_MATRIX_VALUE}",
                error_type="http_status",
                status_code=500,
                errcode=f"M_UNKNOWN token={FAKE_MATRIX_VALUE}",
                attempt=3,
                raw_body=f"raw response body {FAKE_MATRIX_VALUE} Traceback raw_image_bytes abc",
            )

    matrix_client = UploadFailingMatrixClient()

    def fake_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return detections.pop(0)

    def matrix_factory(_settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixDelivery:
        return MatrixDelivery(
            client=matrix_client,  # type: ignore[arg-type]
            room_id="!room:example.org",
            data_dir=data_dir,
            snapshots_dir=tmp_path / "snapshots",
            logger=logger,
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=matrix_factory,
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    records = json_records(output)
    snapshot_files = list((tmp_path / "snapshots").glob("occupancy-open-event-left-spot-*.jpg"))
    failed = next(
        record
        for record in records
        if record["event"] == "matrix-delivery-failed" and record.get("event_type") == "occupancy-open-event"
    )

    assert exit_code == 0
    assert len(snapshot_files) == 1
    assert snapshot_files[0].read_bytes() == (tmp_path / "latest.jpg").read_bytes()
    assert state_status(tmp_path / "state.json", "left_spot") == "empty"
    assert failed["event_type"] == "occupancy-open-event"
    assert failed["spot_id"] == "left_spot"
    assert failed["snapshot_path"] == str(tmp_path / "latest.jpg")
    assert failed["attempt"] == 3
    assert failed["status_code"] == 500
    assert failed["final"] is True
    assert '"event":"matrix-snapshot-copied"' in output
    assert '"event":"state-saved"' in output
    assert "Authorization" not in output
    assert "raw response body" not in output
    assert "Traceback" not in output
    assert "raw_image_bytes abc" not in output
    assert_no_secret_leak(output)


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

    assert _presence_by_spot(result) == {"left_spot": True, "right_spot": False}


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

    assert _presence_by_spot(result) == {"left_spot": False}
