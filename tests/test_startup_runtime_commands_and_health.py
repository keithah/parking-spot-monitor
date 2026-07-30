from __future__ import annotations

from tests.support._startup import *  # noqa: F403


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
