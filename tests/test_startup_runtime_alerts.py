from __future__ import annotations

from tests.support._startup import *  # noqa: F403


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
