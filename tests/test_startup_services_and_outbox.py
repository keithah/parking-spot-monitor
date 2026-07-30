from __future__ import annotations

from tests.support._startup import *  # noqa: F403


def test_startup_summary_includes_sanitized_detection_lab_dir(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = _main(["--config", "config.yaml.example", "--data-dir", "/tmp/parking-data", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)

    assert exit_code == 0
    assert '"detection_lab_dir":"/tmp/parking-data/detection-lab"' in output
    assert_no_secret_leak(output)


def test_default_matrix_command_service_uses_short_independent_client_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from parking_spot_monitor import __main__ as cli
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    captured: list[dict[str, Any]] = []

    class Client:
        def __init__(self, **kwargs: Any) -> None:
            captured.append(kwargs)

        def close(self) -> None:
            return None

    monkeypatch.setattr(cli, "MatrixClient", Client)
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={
            "matrix": settings.matrix.model_copy(
                update={"command_authorized_senders": ["@operator:example.org"]}
            )
        }
    )

    delivery = cli._default_matrix_delivery_factory(
        settings, tmp_path, StructuredLogger()
    )

    service = _default_matrix_command_service_factory(
        settings,
        tmp_path,
        StructuredLogger(),
        VehicleHistoryArchive(tmp_path / "vehicle-history"),
        incident_detector=object(),
    )

    assert service is not None
    assert captured[0]["timeout_seconds"] == 10
    assert captured[0]["retry_attempts"] == 3
    assert captured[1]["timeout_seconds"] == 2
    assert captured[1]["retry_attempts"] == 1
    service.close()
    delivery.close()


def test_validate_config_does_not_construct_matrix_outbox_or_touch_network(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def forbidden_matrix_factory(_settings: object, _data_dir: Path, _logger: StructuredLogger) -> object:
        raise AssertionError("validate-config must not construct Matrix delivery")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--validate-config"],
        environ=fake_environ(),
        matrix_delivery_factory=forbidden_matrix_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert not (tmp_path / "matrix-outbox.json").exists()
    assert "matrix-outbox" not in output
    assert_no_secret_leak(output)


def test_matrix_outbox_health_payload_quarantines_corrupt_json_without_raw_secret(tmp_path: Path) -> None:
    outbox_path = tmp_path / "matrix-outbox.json"
    outbox_path.write_text('{"items": [Authorization: Bearer matrix-secret', encoding="utf-8")

    payload = _matrix_outbox_health_payload(outbox_path)

    assert payload is not None
    assert payload["available"] is True
    assert payload["counts_by_state"] == {}
    assert payload["recovery"]["quarantined_count"] == 1
    assert payload["recovery"]["reason_counts"] == {"invalid_json": 1}
    rendered = json.dumps(payload).lower()
    assert "authorization" not in rendered
    assert "bearer" not in rendered
    assert "matrix-secret" not in rendered


def test_matrix_outbox_health_payload_degrades_on_read_error_without_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ExplodingOutbox:
        def __init__(self, _path: Path) -> None:
            raise OSError("permission denied access_token=matrix-secret")

    monkeypatch.setattr("parking_spot_monitor.runtime_health.LocalOutbox", ExplodingOutbox)

    payload = _matrix_outbox_health_payload(tmp_path / "matrix-outbox.json")

    assert payload is not None
    assert payload["available"] is False
    assert payload["phase"] == "matrix-outbox"
    assert payload["error"]["phase"] == "matrix-outbox"
    assert payload["error"]["action"] == "status-summary"
    rendered = json.dumps(payload).lower()
    assert "access_token" not in rendered
    assert "matrix-secret" not in rendered


def test_matrix_outbox_health_payload_strips_record_items_from_live_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ExplodingOutbox:
        def __init__(self, _path: Path) -> None:
            raise AssertionError("live provider must avoid reopening the outbox")

    monkeypatch.setattr("parking_spot_monitor.runtime_health.LocalOutbox", ExplodingOutbox)

    payload = _matrix_outbox_health_payload(
        tmp_path / "matrix-outbox.json",
        summary_provider=lambda: {
            "path": str(tmp_path / "matrix-outbox.json"),
            "total": 1,
            "counts_by_state": {"delivered": 1},
            "items": [{"id": "event-1", "body": "record-level data"}],
        },
    )

    assert payload == {
        "path": str(tmp_path / "matrix-outbox.json"),
        "total": 1,
        "counts_by_state": {"delivered": 1},
        "available": True,
    }
    assert "items" not in payload


def test_matrix_outbox_health_payload_exposes_only_safe_worker_fields(tmp_path: Path) -> None:
    payload = _matrix_outbox_health_payload(
        tmp_path / "matrix-outbox.json",
        summary_provider=lambda: {
            "total": 0,
            "counts_by_state": {},
            "worker_running": True,
            "worker_last_attempt_at": "2026-07-29T17:00:00Z",
            "worker_last_error_type": "RuntimeError",
            "worker_error_message": "Authorization: Bearer matrix-secret",
        },
    )

    assert payload is not None
    assert payload["worker_running"] is True
    assert payload["worker_last_attempt_at"] == "2026-07-29T17:00:00Z"
    assert payload["worker_last_error_type"] == "RuntimeError"
    assert "worker_error_message" not in payload
    rendered = json.dumps(payload)
    assert "Authorization" not in rendered
    assert "Bearer" not in rendered
    assert "matrix-secret" not in rendered


def test_runtime_health_json_includes_resolved_matrix_outbox_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    pending = outbox.enqueue(AlertIntent(event_id="evt-pending", phase="text", body="ok"))
    retrying = outbox.enqueue(AlertIntent(event_id="evt-retrying", phase="upload", body="ok"))
    delivered = outbox.enqueue(AlertIntent(event_id="evt-delivered", phase="image", body="ok"))
    failed = outbox.enqueue(AlertIntent(event_id="evt-failed", phase="text", body="ok"))
    dead = outbox.enqueue(AlertIntent(event_id="evt-dead", phase="text", body="ok"))
    outbox.mark_retrying(retrying.id, reason="timeout")
    outbox.mark_delivered(delivered.id)
    outbox.mark_failed(failed.id, reason="matrix_forbidden")
    outbox.mark_dead_lettered(dead.id, reason="Authorization: Bearer matrix-secret")
    assert pending.state == "pending"

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=0,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    matrix_outbox = health["matrix_outbox"]
    assert exit_code == 0
    assert matrix_outbox["available"] is True
    assert matrix_outbox["path"] == str(tmp_path / "matrix-outbox.json")
    assert matrix_outbox["counts_by_state"] == {
        "pending": 1,
        "retrying": 1,
        "delivered": 1,
        "failed": 1,
        "dead_lettered": 1,
    }
    assert matrix_outbox["retry_reason_counts"] == {"timeout": 1}
    assert matrix_outbox["dead_letter_reason_counts"] == {"matrix_forbidden": 1, "redacted": 1}
    assert "items" not in matrix_outbox
    rendered = json.dumps(health).lower()
    assert "authorization" not in rendered
    assert "bearer" not in rendered
    assert "matrix-secret" not in rendered
    assert_no_secret_leak(output)


def test_runtime_loop_closes_matrix_services_on_exit(tmp_path: Path) -> None:
    closed: list[str] = []

    class CloseableDelivery(FakeMatrixDelivery):
        def close(self) -> None:
            closed.append("delivery")

    class CloseableCommands:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult()

        def close(self) -> None:
            closed.append("commands")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: CloseableDelivery(),
        matrix_command_service_factory=lambda _settings, _data_dir, _logger, _archive: CloseableCommands(),
        sleep=lambda _seconds: None,
        max_iterations=0,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert closed == ["commands", "delivery"]


def test_runtime_loop_preserves_injected_falsey_history_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import parking_spot_monitor.capture_loop as capture_loop_module
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    class FalseyArchive(VehicleHistoryArchive):
        def __bool__(self) -> bool:
            return False

    supplied = FalseyArchive(tmp_path / "supplied-history")

    def forbidden_fallback(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("falsey injected archive was replaced")

    monkeypatch.setattr(capture_loop_module, "VehicleHistoryArchive", forbidden_fallback)
    settings = load_settings("config.yaml.example", environ=fake_environ())
    settings = settings.model_copy(
        update={"runtime": settings.runtime.model_copy(update={"health_file": tmp_path / "health.json"})}
    )

    assert run_capture_loop(
        settings,
        tmp_path,
        logger=StructuredLogger(),
        capture=lambda *_args, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery=None,
        history_archive=supplied,
        sleep=lambda _seconds: None,
        max_iterations=0,
    ) == 0


def test_runtime_teardown_cancels_command_worker_before_service_close(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    release = threading.Event()

    class Commands:
        def fetch_once(self) -> MatrixSyncResult:
            release.wait(1)
            return MatrixSyncResult(next_batch="s1", events=())

        def apply_sync_result(self, _result: MatrixSyncResult) -> FakeCommandPollResult:
            return FakeCommandPollResult()

        def cancel_pending(self) -> None:
            events.append("worker-cancel")
            release.set()

        def close(self) -> None:
            events.append("service-close")

    commands = Commands()
    assert _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda *_args: FakeMatrixDelivery(),
        matrix_command_service_factory=lambda *_args: commands,
        sleep=lambda _seconds: None,
        max_iterations=1,
    ) == 0

    assert events == ["worker-cancel", "service-close"]


def test_default_matrix_delivery_factory_starts_one_outbox_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor import __main__ as cli

    class FactoryClient(FakeMatrixClient):
        def __init__(self, **_kwargs: Any) -> None:
            super().__init__()

        def close(self) -> None:
            return None

    monkeypatch.setattr(cli, "MatrixClient", FactoryClient)
    settings = load_settings("config.yaml.example", environ=fake_environ())

    delivery = cli._default_matrix_delivery_factory(settings, tmp_path, StructuredLogger())
    worker = delivery.worker_thread
    try:
        assert worker is not None
        assert worker.is_alive() is True
        delivery.start_worker(retry_interval_seconds=settings.matrix.outbox_retry_interval_seconds)
        assert delivery.worker_thread is worker
    finally:
        delivery.close()


def test_runtime_open_alert_failure_persists_retryable_matrix_outbox(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], [], []]
    matrix_client = UploadFailsOnceMatrixClient()

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    sleep_calls = 0

    def wait_for_worker_on_last_iteration(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 7:
            assert matrix_client.image_sent.wait(2)
            assert matrix_client.failed_upload.wait(2)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, data_dir, logger: outbox_delivery(matrix_client, data_dir, logger),
        sleep=wait_for_worker_on_last_iteration,
        max_iterations=7,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    summary = outbox.status_summary()
    item = next(item for item in summary["items"] if item["state"] == "retrying")
    phases = {phase["phase"]: phase for phase in item["phases"]}

    assert exit_code == 0
    assert summary["counts_by_state"] == {"delivered": 3, "retrying": 1}
    assert phases["upload"]["state"] == "pending"
    assert phases["image"]["state"] == "pending"
    occupancy_text_kinds = [text["txn_id"].split(":", 1)[0] for text in matrix_client.texts if text["txn_id"].startswith("occupancy-")]
    assert occupancy_text_kinds == ["occupancy-occupied-event", "occupancy-open-event"]
    assert len(matrix_client.uploads) == 1
    assert matrix_client.uploads[0]["filename"].startswith("occupancy-occupied-event-")
    assert len(matrix_client.images) == 1
    assert matrix_client.images[0]["txn_id"].startswith("occupancy-occupied-event:")
    assert '"event":"matrix-outbox-phase-retryable-failure"' in output
    assert_no_secret_leak(output)


def test_runtime_worker_restarts_existing_matrix_outbox_without_new_occupancy_event(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # First invocation leaves a retryable record before Matrix media upload.
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], [], []]
    failing_client = UploadFailsOnceMatrixClient()

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    sleep_calls = 0

    def wait_for_failed_worker_pass(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 7:
            assert failing_client.image_sent.wait(2)
            assert failing_client.failed_upload.wait(2)

    first_exit = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, data_dir, logger: outbox_delivery(failing_client, data_dir, logger),
        sleep=wait_for_failed_worker_pass,
        max_iterations=7,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )
    assert first_exit == 0
    capsys.readouterr()

    successful_client = FakeMatrixClient()

    def forbidden_capture(_settings: object, _data_dir: str | Path) -> FrameCaptureResult:
        raise AssertionError("startup drain with max_iterations=0 must not capture a new frame")

    def _started_delivery_after_restart(
        client: FakeMatrixClient,
        data_dir: Path,
        logger: StructuredLogger,
    ) -> MatrixOutboxDelivery:
        delivery = outbox_delivery(
            client,
            data_dir,
            logger,
            utc_now=lambda: datetime.now(timezone.utc) + timedelta(seconds=120),
        )
        assert client.image_sent.wait(2), "restarted worker did not finish durable delivery"
        return delivery

    second_exit = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=forbidden_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, data_dir, logger: _started_delivery_after_restart(
            successful_client,
            data_dir,
            logger,
        ),
        sleep=lambda _seconds: None,
        max_iterations=0,
        now=lambda: datetime(2026, 5, 18, 19, 5, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    summary = outbox.status_summary()
    record = next(record for record in outbox.list_records() if record.intent.event_id.startswith("occupancy-open-event:"))
    phases = {phase: {"state": state} for phase, state in record.phase_states.items()}

    assert second_exit == 0
    assert summary["counts_by_state"] == {"delivered": 5}
    assert phases["upload"]["state"] == "delivered"
    assert phases["image"]["state"] == "delivered"
    assert [text for text in successful_client.texts if text["txn_id"].startswith("occupancy-open-event:")] == []
    assert len([text for text in successful_client.texts if text["txn_id"].startswith("parking-monitor-started:")]) == 1
    assert len(successful_client.uploads) == 1
    assert len(successful_client.images) == 1
    assert '"event":"matrix-outbox-record-delivered"' in output
    assert '"attempted_count":1' in output
    assert_no_secret_leak(output)
