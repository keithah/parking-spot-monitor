from __future__ import annotations

from tests.support._startup import *  # noqa: F403


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
