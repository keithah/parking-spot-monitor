from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image

from parking_spot_monitor.__main__ import _main
from parking_spot_monitor.capture_loop import run_capture_loop
from parking_spot_monitor.capture import DecodeMode, FrameCaptureResult, FrameGeometry
from parking_spot_monitor.detection import DetectionError, VehicleDetection
from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
from parking_spot_monitor.state import RuntimeState, save_runtime_state
from parking_spot_monitor.runtime_reconnect import capture_reconnect_delay
from parking_spot_monitor.config import load_settings
from parking_spot_monitor.logging import StructuredLogger

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


def runtime_state_payload(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def health_payload(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def noop_overlay(_settings: object, _source_path: Path, _output_path: Path, *, logger: object) -> object:
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


def test_capture_reconnect_delay_exponentially_caps_with_injected_jitter() -> None:
    delays = [
        capture_reconnect_delay(
            failure_count,
            initial_seconds=5,
            max_seconds=60,
            jitter_ratio=0.2,
            random_unit=lambda: 0.5,
        )
        for failure_count in range(1, 7)
    ]

    assert delays == [5.5, 11.0, 22.0, 44.0, 60.0, 60.0]


def test_capture_reconnect_delay_saturates_before_large_exponentiation() -> None:
    assert capture_reconnect_delay(
        100_000,
        initial_seconds=5,
        max_seconds=60,
        jitter_ratio=0.2,
        random_unit=lambda: 0.5,
    ) == 60


def test_runtime_loop_emits_bounded_summary_instead_of_per_frame_info(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={
                    "health_file": tmp_path / "health.json",
                    "adaptive_polling_enabled": False,
                    "log_summary_interval_seconds": 1,
                }
            )
        }
    )

    class Detector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
        ) -> list[VehicleDetection]:
            return []

    monotonic_values = iter((0.0, 1.0))
    assert run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir)),
        overlay=lambda *_args, **_kwargs: object(),
        detector_factory=lambda _settings: Detector(),
        matrix_delivery=None,
        sleep=lambda _seconds: None,
        max_iterations=1,
        monotonic=lambda: next(monotonic_values),
        decision_memory_store=DecisionMemoryStore(
            tmp_path / "operator-decision-memory.json",
            checkpoint_interval_seconds=300,
            checkpoint_max_pending_records=50,
        ),
    ) == 0

    output = combined_output(capsys)
    assert '"event":"capture-loop-frame-written"' not in output
    assert '"event":"detection-frame-processed"' not in output
    assert '"event":"runtime-loop-summary"' in output
    assert '"processed_frames":1' in output
    assert "candidate_summaries" not in output


def test_successful_capture_resets_exponential_reconnect_streak(tmp_path: Path) -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={"health_file": tmp_path / "health.json", "adaptive_polling_enabled": False}
            )
        }
    )
    outcomes: list[str] = ["fail", "success", "fail"]
    waits: list[float] = []

    def capture(_settings: object, data_dir: str | Path, **_kwargs: object) -> FrameCaptureResult:
        outcome = outcomes.pop(0)
        if outcome == "fail":
            from parking_spot_monitor.capture import CaptureError

            raise CaptureError(
                reason="timeout",
                mode=DecodeMode.SOFTWARE,
                output_path=Path(data_dir) / "latest.jpg",
                message="timeout",
            )
        return captured_frame(Path(data_dir))

    class Detector:
        def detect(self, *_args: object, **_kwargs: object) -> list[VehicleDetection]:
            return []

    assert run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=capture,
        overlay=lambda *_args, **_kwargs: object(),
        detector_factory=lambda _settings: Detector(),
        matrix_delivery=None,
        sleep=waits.append,
        max_iterations=3,
        monotonic=lambda: 0,
        random_unit=lambda: 0,
    ) == 0

    assert waits == [5, 30, 5]


def test_runtime_loop_detector_failure_logs_and_continues(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    latest_path = tmp_path / "latest.jpg"
    sleeps: list[float] = []
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        runtime_config_text().replace(
            "adaptive_polling_enabled: true",
            "adaptive_polling_enabled: false",
        ),
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
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        detector_factory=lambda _settings: FailingDetector(),
        sleep=sleeps.append,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert sleeps == [30]
    assert '"event":"detection-frame-failed"' in output
    assert '"iteration":1' in output
    assert '"event":"capture-loop-frame-written"' not in output
    assert '"event":"detection-frame-processed"' not in output
    assert health["last_frame_at"] is None
    assert health["selected_decode_mode"] is None
    assert health["capture"] == {
        "last_success_at": "2025-01-01T00:00:00Z",
        "selected_decode_mode": "software",
    }
    assert health["consecutive_capture_failures"] == 0
    assert health["consecutive_detection_failures"] == 1
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_detection_failure_updates_health_without_advancing_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(runtime_config_text(), encoding="utf-8")
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
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
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


def test_runtime_loop_detector_factory_failure_updates_detection_health(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(runtime_config_text(), encoding="utf-8")
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

    def fail_detector_factory(_settings: object) -> object:
        raise DetectionError(
            f"model load failed token={SECRET_MARKER}",
            model_path="yolov8n.pt",
            phase="model_load",
            error_type="ModuleNotFoundError",
        )

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:30:00Z"),
        overlay=noop_overlay,
        detector_factory=fail_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    assert exit_code == 0
    assert runtime_state_payload(state_path)["spots"]["left_spot"]["miss_streak"] == 0
    assert health["status"] == "degraded"
    assert health["capture"] == {
        "last_success_at": "2026-05-18T19:30:00Z",
        "selected_decode_mode": "software",
    }
    assert health["consecutive_capture_failures"] == 0
    assert health["consecutive_detection_failures"] == 1
    assert health["last_error"]["phase"] == "detection"
    assert health["last_error"]["error_type"] == "DetectionError"
    assert SECRET_MARKER not in json.dumps(health)
    assert_no_secret_leak(output)
