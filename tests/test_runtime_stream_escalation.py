from __future__ import annotations

from pathlib import Path

from PIL import Image

from parking_spot_monitor.capture import DecodeMode, FrameCaptureResult, FrameGeometry
from parking_spot_monitor.config import load_settings
from parking_spot_monitor.detection import VehicleDetection
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
from parking_spot_monitor.operator_decision_memory import load_decision_memory
from parking_spot_monitor.runtime_detection import record_detection_memory_records
from parking_spot_monitor.runtime_stream_escalation import detect_with_stream_escalation
from parking_spot_monitor.state import RuntimeState


def _settings() -> object:
    return load_settings(
        "config.yaml.example",
        environ={
            "RTSP_URL": "primary-camera",
            "RTSP_URL_4K": "high-camera",
            "RTSP_URL_360P": "low-camera",
            "MATRIX_ACCESS_TOKEN": "matrix-token",
        },
    )


def _settings_without_escalation(tmp_path: Path) -> object:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace(
            "  escalation_profile: high_resolution\n"
            "  escalation_min_confidence: 0.75\n"
            "  profiles:\n"
            "    high_resolution:\n"
            "      rtsp_url_env: RTSP_URL_4K\n"
            "      frame_width: 3840\n"
            "      frame_height: 2160\n",
            "",
        ),
        encoding="utf-8",
    )
    return load_settings(
        config_path,
        environ={
            "RTSP_URL": "primary-camera",
            "MATRIX_ACCESS_TOKEN": "matrix-token",
        },
    )


def test_missing_escalation_profile_does_not_recapture_primary_stream(tmp_path: Path) -> None:
    settings = _settings_without_escalation(tmp_path)
    latest_path = tmp_path / "latest.jpg"
    captured_profiles: list[str | None] = []

    def capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        captured_profiles.append(stream_profile)
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    class WeakDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return [VehicleDetection(class_name="car", confidence=0.50, bbox=(1010, 215, 1395, 355))]

    primary = capture(settings, tmp_path)

    frame_result = detect_with_stream_escalation(
        settings,
        tmp_path,
        capture=capture,
        detector=WeakDetector(),
        runtime_state=RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED),
            }
        ),
        primary_result=primary,
        logger=StructuredLogger(),
        mode="runtime-loop",
        iteration=1,
    )

    assert captured_profiles == [None]
    assert frame_result.final_capture.frame_geometry.stream_profile == "primary"


def test_escalation_returns_final_frame_for_caller_owned_memory_recording(tmp_path: Path) -> None:
    settings = _settings()
    latest_path = tmp_path / "latest.jpg"
    memory_path = tmp_path / "operator-decision-memory.json"
    captured_profiles: list[str | None] = []

    def capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        captured_profiles.append(stream_profile)
        profile = stream_profile or "primary"
        size = (3840, 2160) if profile == "high_resolution" else (1458, 806)
        Image.new("RGB", size, (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:01Z" if profile == "high_resolution" else "2026-05-18T18:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile=profile, expected_size=size),
        )

    class Detector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            with Image.open(frame_path) as image:
                if image.size == (3840, 2160):
                    return [
                        VehicleDetection(
                            class_name="car",
                            confidence=0.92,
                            bbox=(1010 * 3840 / 1458, 215 * 2160 / 806, 1395 * 3840 / 1458, 355 * 2160 / 806),
                        )
                    ]
                return [VehicleDetection(class_name="car", confidence=0.50, bbox=(1010, 215, 1395, 355))]

    primary = capture(settings, tmp_path)

    frame_result = detect_with_stream_escalation(
        settings,
        tmp_path,
        capture=capture,
        detector=Detector(),
        runtime_state=RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED),
            }
        ),
        primary_result=primary,
        logger=StructuredLogger(),
        mode="runtime-loop",
        iteration=1,
    )
    result = frame_result.final_capture

    assert captured_profiles == [None, "high_resolution"]
    assert result.frame_geometry.stream_profile == "high_resolution"
    assert not memory_path.exists()

    record_detection_memory_records(
        memory_path,
        frame_result.detection,
        observed_at=result.timestamp,
        logger=StructuredLogger(),
        mode="runtime-loop",
        iteration=1,
    )
    loaded = load_decision_memory(memory_path)
    assert loaded.state == "available"
    assert {record.observed_at for record in loaded.records} == {"2026-05-18T18:00:01Z"}
    candidates = [
        record.details["candidate"]
        for record in loaded.records
        if record.details is not None and "candidate" in record.details
    ]
    assert candidates
    assert {candidate["source_timestamp"] for candidate in candidates} == {"2026-05-18T18:00:01Z"}


def test_detection_uses_capture_profile_dimensions_when_image_size_is_unavailable(tmp_path: Path) -> None:
    from parking_spot_monitor.runtime_detection import _process_detection_for_capture

    settings = _settings()
    unreadable_frame = tmp_path / "missing-profile-frame.jpg"

    class LowResolutionDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return [VehicleDetection(class_name="car", confidence=0.9, bbox=(142.0, 91.0, 265.0, 151.0))]

    result = _process_detection_for_capture(
        settings,
        LowResolutionDetector(),
        unreadable_frame,
        frame_timestamp="2026-05-18T18:00:00Z",
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="low_resolution", expected_size=(640, 360)),
    )

    assert result.by_spot["left_spot"].accepted is not None
