from __future__ import annotations

from tests.support._startup import *  # noqa: F403


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
