from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from parking_spot_monitor.__main__ import _main
from parking_spot_monitor.capture import CaptureError, DecodeMode, FrameCaptureResult, FrameGeometry
from parking_spot_monitor.capture_loop import run_capture_loop
from parking_spot_monitor.config import load_settings
from parking_spot_monitor.detection import DetectionError, VehicleDetection
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
from parking_spot_monitor.runtime_frame import capture_and_detect_runtime_frame
from parking_spot_monitor.runtime_frame_outcome import (
    RuntimeFrameDetected,
    RuntimeFrameDetectionFailed,
    prepare_runtime_frame_loop_result,
)
from parking_spot_monitor.runtime_health import RuntimeLoopHealthState
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


def runtime_config_text() -> str:
    return Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "model: /models/yolov8n.pt",
        "model: yolov8n.pt",
    )


def combined_output(capsys: pytest.CaptureFixture[str]) -> str:
    captured = capsys.readouterr()
    return captured.out + captured.err


def assert_no_secret_leak(output: str) -> None:
    assert FAKE_RTSP_VALUE not in output
    assert FAKE_MATRIX_VALUE not in output
    assert SECRET_MARKER not in output


def health_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def runtime_state_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def noop_overlay(_settings: object, _source_path: Path, _output_path: Path, *, logger: Any) -> object:
    return object()


def test_runtime_loop_escalates_weak_primary_detection_and_uses_primary_artifacts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        runtime_config_text().replace(
            "  reconnect_seconds: 5\n",
            "  reconnect_seconds: 5\n"
            "  profiles:\n"
            "    high_resolution:\n"
            "      rtsp_url_env: RTSP_URL_4K\n"
            "      frame_width: 3840\n"
            "      frame_height: 2160\n",
        ).replace(
            "adaptive_polling_enabled: true", "adaptive_polling_enabled: false"
        ),
        encoding="utf-8",
    )
    sleeps: list[float] = []
    overlay_sources: list[Path] = []
    capture_profiles: list[str | None] = []
    primary_path = tmp_path / "latest-primary.jpg"
    high_path = tmp_path / "latest-high.jpg"
    save_runtime_state(
        tmp_path / "state.json",
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY, open_event_emitted=True),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY),
            }
        ),
    )

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        capture_profiles.append(stream_profile)
        profile = stream_profile or "primary"
        size = (3840, 2160) if profile == "high_resolution" else (1458, 806)
        latest_path = high_path if profile == "high_resolution" else primary_path
        Image.new("RGB", size, (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:01Z" if profile == "high_resolution" else "2026-05-18T18:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile=profile, expected_size=size),
        )

    class ProfileAwareDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            path = Path(frame_path)
            if path == primary_path:
                return [VehicleDetection(class_name="car", confidence=0.50, bbox=(1010, 215, 1395, 355))]
            if path == high_path:
                return [
                    VehicleDetection(
                        class_name="car",
                        confidence=0.92,
                        bbox=(1010 * 3840 / 1458, 215 * 2160 / 806, 1395 * 3840 / 1458, 355 * 2160 / 806),
                    )
                ]
            return []

    def record_overlay(
        _settings: object,
        source_path: Path,
        _output_path: Path,
        *,
        logger: Any,
    ) -> object:
        overlay_sources.append(Path(source_path))
        return object()

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(RTSP_URL_4K=f"high-camera-{SECRET_MARKER}"),
        capture=fake_capture,
        overlay=record_overlay,
        detector_factory=lambda _settings: ProfileAwareDetector(),
        sleep=sleeps.append,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert capture_profiles == [None, "high_resolution"]
    assert health["capture"] == {
        "last_success_at": "2026-05-18T18:00:01Z",
        "selected_decode_mode": "software",
    }
    assert '"event":"stream-profile-escalated"' in output
    assert '"from_profile":"primary"' in output
    assert '"to_profile":"high_resolution"' in output
    assert '"stream_profile":"high_resolution"' in output
    assert runtime_state_payload(tmp_path / "state.json")["spots"]["right_spot"]["last_bbox"][0] == pytest.approx(1010 * 3840 / 1458)
    assert sleeps == [30]
    timeline_frames = sorted((tmp_path / "timeline" / "frames").glob("*.jpg"))
    assert timeline_frames[0].read_bytes() == primary_path.read_bytes()
    assert overlay_sources == [primary_path]
    assert high_path.read_bytes() != timeline_frames[0].read_bytes()
    assert_no_secret_leak(output)


def test_runtime_frame_periodic_outcome_preserves_primary_and_final_capture_identities(tmp_path: Path) -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())
    capture_profiles: list[str | None] = []
    primary_path = tmp_path / "latest-primary.jpg"
    high_path = tmp_path / "latest-high-resolution.jpg"

    def fake_capture(
        _settings: object,
        data_dir: str | Path,
        *,
        stream_profile: str | None = None,
    ) -> FrameCaptureResult:
        capture_profiles.append(stream_profile)
        profile = stream_profile or "primary"
        size = (3840, 2160) if profile == "high_resolution" else (1458, 806)
        latest_path = high_path if profile == "high_resolution" else primary_path
        Image.new("RGB", size, (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:01Z" if profile == "high_resolution" else "2026-05-18T18:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile=profile, expected_size=size),
        )

    class ProfileAwareDetector:
        def detect(
            self,
            frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
        ) -> list[VehicleDetection]:
            path = Path(frame_path)
            confidence = 0.92
            width_scale = 3840 / 1458 if path == high_path else 1
            height_scale = 2160 / 806 if path == high_path else 1
            return [
                VehicleDetection(
                    class_name="car",
                    confidence=confidence,
                    bbox=(
                        1010 * width_scale,
                        215 * height_scale,
                        1395 * width_scale,
                        355 * height_scale,
                    ),
                )
            ]

    frame_attempt = capture_and_detect_runtime_frame(
        settings,
        tmp_path,
        capture=fake_capture,
        detector=None,
        detector_factory=lambda _settings: ProfileAwareDetector(),
        runtime_state=RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=12),
            }
        ),
        logger=StructuredLogger(),
        mode="runtime-loop",
        iteration=1,
        periodic_verification_due=True,
    )

    assert isinstance(frame_attempt, RuntimeFrameDetected)
    assert capture_profiles == [None, "high_resolution"]
    assert frame_attempt.primary_capture.latest_path == primary_path
    assert frame_attempt.capture.latest_path == high_path
    assert frame_attempt.escalated is True

    health_state = RuntimeLoopHealthState()
    frame_result = prepare_runtime_frame_loop_result(
        frame_attempt,
        health_state=health_state,
        logger=StructuredLogger(),
        iteration=1,
    )
    assert frame_result.primary_capture.latest_path == primary_path
    assert frame_result.capture.latest_path == high_path
    assert frame_result.escalated is True


def test_runtime_loop_transition_verification_resets_periodic_deadline(
    tmp_path: Path,
) -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "debug_overlay_interval_seconds": 0,
                }
            ),
            "stream": settings.stream.model_copy(
                update={"escalation_verification_seconds": 100}
            ),
        }
    )
    primary_path = tmp_path / "latest-primary.jpg"
    high_path = tmp_path / "latest-high.jpg"
    capture_profiles: list[str | None] = []
    primary_detection_count = 0
    save_runtime_state(
        tmp_path / "state.json",
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY,
                    hit_streak=2,
                ),
                "right_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY,
                    miss_streak=3,
                ),
            }
        ),
    )

    def fake_capture(
        _settings: object,
        _data_dir: str | Path,
        *,
        stream_profile: str | None = None,
    ) -> FrameCaptureResult:
        capture_profiles.append(stream_profile)
        profile = stream_profile or "primary"
        size = (3840, 2160) if profile == "high_resolution" else (1458, 806)
        path = high_path if profile == "high_resolution" else primary_path
        Image.new("RGB", size, (20, 30, 40)).save(path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:01Z" if stream_profile else "2026-05-18T18:00:00Z",
            latest_path=path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile=profile, expected_size=size),
        )

    class TransitionThenStableDetector:
        def detect(
            self,
            frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
        ) -> list[VehicleDetection]:
            nonlocal primary_detection_count
            path = Path(frame_path)
            if path == primary_path:
                primary_detection_count += 1
                confidence = 0.5 if primary_detection_count == 1 else 0.92
                return [
                    VehicleDetection(
                        class_name="car",
                        confidence=confidence,
                        bbox=(350, 200, 550, 330),
                    )
                ]
            return [
                VehicleDetection(
                    class_name="car",
                    confidence=0.92,
                    bbox=(
                        350 * 3840 / 1458,
                        200 * 2160 / 806,
                        550 * 3840 / 1458,
                        330 * 2160 / 806,
                    ),
                )
            ]

    timestamps = iter(
        value
        for iteration in range(12)
        for value in (float(iteration * 10), float(iteration * 10 + 1))
    )

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: TransitionThenStableDetector(),
        matrix_delivery=None,
        sleep=lambda _seconds: None,
        max_iterations=12,
        monotonic=lambda: next(timestamps),
    )

    assert exit_code == 0
    assert capture_profiles == [
        None,
        "high_resolution",
        *([None] * 10),
        None,
        "high_resolution",
    ]


def test_disabled_adaptive_polling_keeps_fixed_cadence_and_periodic_verification(
    tmp_path: Path,
) -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "adaptive_polling_enabled": False,
                    "debug_overlay_interval_seconds": 0,
                }
            )
        }
    )
    primary_path = tmp_path / "latest-primary.jpg"
    high_path = tmp_path / "latest-high.jpg"
    capture_profiles: list[str | None] = []
    sleeps: list[float] = []
    save_runtime_state(
        tmp_path / "state.json",
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY,
                    miss_streak=3,
                ),
                "right_spot": SpotOccupancyState(
                    status=OccupancyStatus.EMPTY,
                    miss_streak=3,
                ),
            }
        ),
    )

    def fake_capture(
        _settings: object,
        _data_dir: str | Path,
        *,
        stream_profile: str | None = None,
    ) -> FrameCaptureResult:
        capture_profiles.append(stream_profile)
        profile = stream_profile or "primary"
        size = (3840, 2160) if profile == "high_resolution" else (1458, 806)
        path = high_path if profile == "high_resolution" else primary_path
        Image.new("RGB", size, (20, 30, 40)).save(path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:01Z" if stream_profile else "2026-05-18T18:00:00Z",
            latest_path=path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile=profile, expected_size=size),
        )

    monotonic_values = iter([0.0, 1.0, 10.0, 11.0, 20.0, 21.0, 30.0, 31.0])

    class StableEmptyDetector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
        ) -> list[VehicleDetection]:
            return []

    exit_code = run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: StableEmptyDetector(),
        matrix_delivery=None,
        sleep=sleeps.append,
        max_iterations=4,
        monotonic=lambda: next(monotonic_values),
    )

    assert exit_code == 0
    assert sleeps == [30, 30, 30, 30]
    assert capture_profiles == [None, None, None, None, "high_resolution"]


def test_failed_high_resolution_detection_preserves_primary_capture_identity(tmp_path: Path) -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())
    primary_path = tmp_path / "latest-primary.jpg"
    high_path = tmp_path / "latest-high-resolution.jpg"

    def fake_capture(
        _settings: object,
        data_dir: str | Path,
        *,
        stream_profile: str | None = None,
    ) -> FrameCaptureResult:
        profile = stream_profile or "primary"
        size = (3840, 2160) if profile == "high_resolution" else (1458, 806)
        latest_path = high_path if profile == "high_resolution" else primary_path
        Image.new("RGB", size, (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:01Z" if profile == "high_resolution" else "2026-05-18T18:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile=profile, expected_size=size),
        )

    class FailingHighDetector:
        def detect(
            self,
            frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
        ) -> list[VehicleDetection]:
            if Path(frame_path) == high_path:
                raise DetectionError(
                    "high-resolution detection failed",
                    model_path="yolov8n.pt",
                    frame_path=str(frame_path),
                    phase="predict",
                    error_type="RuntimeError",
                )
            return [VehicleDetection(class_name="car", confidence=0.92, bbox=(1010, 215, 1395, 355))]

    frame_attempt = capture_and_detect_runtime_frame(
        settings,
        tmp_path,
        capture=fake_capture,
        detector=FailingHighDetector(),
        detector_factory=lambda _settings: FailingHighDetector(),
        runtime_state=RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=12),
            }
        ),
        logger=StructuredLogger(),
        mode="runtime-loop",
        iteration=1,
        periodic_verification_due=True,
    )

    assert isinstance(frame_attempt, RuntimeFrameDetectionFailed)
    assert frame_attempt.primary_capture.latest_path == primary_path
    assert frame_attempt.capture.latest_path == high_path

    health_state = RuntimeLoopHealthState()
    frame_result = prepare_runtime_frame_loop_result(
        frame_attempt,
        health_state=health_state,
        logger=StructuredLogger(),
        iteration=1,
    )
    assert frame_result.primary_capture.latest_path == primary_path
    assert frame_result.capture.latest_path == high_path
    assert health_state.capture_last_success_at == "2026-05-18T18:00:01Z"


def test_runtime_loop_failed_escalation_records_capture_success_without_processed_frame(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(runtime_config_text(), encoding="utf-8")
    primary_path = tmp_path / "latest-primary.jpg"
    capture_profiles: list[str | None] = []

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        capture_profiles.append(stream_profile)
        if stream_profile == "high_resolution":
            raise CaptureError(
                reason="ffmpeg-timeout",
                mode=DecodeMode.SOFTWARE,
                output_path=Path(data_dir) / "latest.jpg",
                message=f"timeout rtsp://camera access_token={SECRET_MARKER}",
                stderr_tail=f"Traceback raw_image_bytes {SECRET_MARKER}",
                timeout_seconds=15.0,
            )
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(primary_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:00Z",
            latest_path=primary_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=primary_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    class WeakDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return [VehicleDetection(class_name="car", confidence=0.50, bbox=(1010, 215, 1395, 355))]

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(RTSP_URL_4K=f"high-camera-{SECRET_MARKER}", RTSP_URL_360P=f"low-camera-{SECRET_MARKER}"),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: WeakDetector(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert capture_profiles == [None, "high_resolution"]
    assert health["status"] == "down"
    assert health["last_frame_at"] is None
    assert health["capture"] == {
        "last_success_at": "2026-05-18T18:00:00Z",
        "selected_decode_mode": "software",
    }
    assert health["last_error"]["phase"] == "capture"
    assert primary_path.exists()
    assert_no_secret_leak(output + json.dumps(health))


def test_runtime_loop_failed_high_resolution_detection_records_high_resolution_capture(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(runtime_config_text(), encoding="utf-8")
    capture_profiles: list[str | None] = []
    primary_path = tmp_path / "latest-primary.jpg"
    high_path = tmp_path / "latest-high.jpg"

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        capture_profiles.append(stream_profile)
        profile = stream_profile or "primary"
        size = (3840, 2160) if profile == "high_resolution" else (1458, 806)
        latest_path = high_path if profile == "high_resolution" else primary_path
        Image.new("RGB", size, (20, 30, 40)).save(latest_path, format="JPEG")
        return FrameCaptureResult(
            timestamp="2026-05-18T18:00:01Z" if profile == "high_resolution" else "2026-05-18T18:00:00Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.01,
            byte_size=latest_path.stat().st_size,
            frame_geometry=FrameGeometry(stream_profile=profile, expected_size=size),
        )

    class HighResolutionFailingDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            if Path(frame_path) == high_path:
                raise DetectionError(
                    f"predict failed raw token={SECRET_MARKER}",
                    model_path="yolov8n.pt",
                    frame_path=str(frame_path),
                    phase="predict",
                    error_type="RuntimeError",
                )
            return [VehicleDetection(class_name="car", confidence=0.50, bbox=(1010, 215, 1395, 355))]

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(RTSP_URL_4K=f"high-camera-{SECRET_MARKER}", RTSP_URL_360P=f"low-camera-{SECRET_MARKER}"),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: HighResolutionFailingDetector(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert capture_profiles == [None, "high_resolution"]
    assert health["status"] == "degraded"
    assert health["last_frame_at"] is None
    assert health["capture"] == {
        "last_success_at": "2026-05-18T18:00:01Z",
        "selected_decode_mode": "software",
    }
    assert health["last_error"]["phase"] == "detection"
    assert '"event":"detection-frame-failed"' in output
    assert primary_path.exists()
    assert_no_secret_leak(output + json.dumps(health))
