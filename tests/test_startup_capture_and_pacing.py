from __future__ import annotations

from tests.support._startup import *  # noqa: F403


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
