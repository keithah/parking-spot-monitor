from __future__ import annotations

from tests.support._startup import *  # noqa: F403


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
