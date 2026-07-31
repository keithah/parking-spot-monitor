from __future__ import annotations

from tests.support._startup import *  # noqa: F403


def test_runtime_startup_recovers_manifested_vehicle_image_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    image_dir = tmp_path / "vehicle-history" / "images" / "occupied-full"
    image_dir.mkdir(parents=True)
    target = image_dir / "pending.jpg"
    target.write_bytes(b"pending")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    real_unlink = owned_file_disposal.os.unlink

    def interrupt_disposal(name: object, *args: object, **kwargs: object) -> None:
        if str(name).endswith(".dispose"):
            raise OSError("simulated crash")
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", interrupt_disposal)
    assert file_descriptor_binding.unlink_owned_path(target, identity) is False
    monkeypatch.setattr(owned_file_disposal.os, "unlink", real_unlink)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(Path("config.yaml.example").read_text(encoding="utf-8"), encoding="utf-8")

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir)),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    assert exit_code == 0
    assert target.read_bytes() == b"pending"


@pytest.mark.parametrize(
    "outbox_payload",
    [
        f'{{"items": [token={SECRET_MARKER} raw_image_bytes abc',
        json.dumps(
            {
                "schema_version": 999,
                "items": [],
                "unsafe": f"token={SECRET_MARKER} raw_image_bytes abc",
            }
        ),
    ],
    ids=["invalid-json", "unsupported-schema"],
)
def test_runtime_loop_startup_retention_skips_pruning_after_whole_outbox_quarantine(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    outbox_payload: str,
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    old = snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    newest = snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    old.write_bytes(b"old")
    newest.write_bytes(b"new")
    (tmp_path / "matrix-outbox.json").write_text(outbox_payload, encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"),
        encoding="utf-8",
    )

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T19:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    rendered = output + json.dumps(health)
    assert exit_code == 0
    assert old.exists()
    assert newest.exists()
    assert health["status"] == "degraded"
    assert health["retention_failure_count"] == 1
    assert '"event":"startup-outbox-snapshot-protection-failed"' in output
    assert SECRET_MARKER not in rendered
    assert "raw_image_bytes abc" not in rendered
    assert_no_secret_leak(output)


def test_runtime_loop_startup_retention_skips_pruning_after_partial_record_quarantine(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    old = snapshots / "occupancy-open-event-left-spot-2026-05-18t18-00-00z.jpg"
    pending = snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    newest = snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    for path in (old, pending, newest):
        path.write_bytes(path.name.encode("utf-8"))
    outbox_path = tmp_path / "matrix-outbox.json"
    LocalOutbox(outbox_path).enqueue(
        AlertIntent(
            event_id="pending-open-alert",
            phase="upload",
            body="Parking spot is open.",
            metadata={"retained_snapshot_path": str(pending)},
        )
    )
    payload = json.loads(outbox_path.read_text(encoding="utf-8"))
    payload["items"].append(
        f"invalid record token={SECRET_MARKER} raw_image_bytes abc"
    )
    outbox_path.write_text(json.dumps(payload), encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"),
        encoding="utf-8",
    )

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T19:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    health = health_payload(tmp_path / "health.json")
    rendered = output + json.dumps(health)
    assert exit_code == 0
    assert old.exists()
    assert pending.exists()
    assert newest.exists()
    assert health["status"] == "degraded"
    assert health["retention_failure_count"] == 1
    assert '"event":"startup-outbox-snapshot-protection-failed"' in output
    assert SECRET_MARKER not in rendered
    assert "raw_image_bytes abc" not in rendered
    assert_no_secret_leak(output)


def test_runtime_loop_startup_retention_protects_pending_outbox_snapshot(
    tmp_path: Path,
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    pending = snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    unprotected = snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    pending.write_bytes(b"pending-snapshot")
    unprotected.write_bytes(b"newer-snapshot")
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    outbox.enqueue(
        AlertIntent(
            event_id="pending-open-alert",
            phase="upload",
            body="Parking spot is open.",
            metadata={"retained_snapshot_path": str(pending)},
        )
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"),
        encoding="utf-8",
    )

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(
            Path(data_dir), timestamp="2026-05-18T19:00:00Z"
        ),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    assert exit_code == 0
    assert pending.exists()


def test_runtime_loop_startup_retention_failure_logs_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    (snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg").write_bytes(b"old")
    (snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg").write_bytes(b"new")
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(base.replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"), encoding="utf-8")

    def fail_unlink(_root: Path, _directory: str | None, _filename: str) -> int:
        raise PermissionError(f"permission denied token={FAKE_MATRIX_VALUE} raw_image_bytes abc")

    monkeypatch.setattr(matrix_snapshots, "delete_owned_artifact", fail_unlink)

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"event":"snapshot-retention-failed"' in output
    assert '"trigger":"startup"' in output
    assert '"error_type":"PermissionError"' in output
    assert '"event":"capture-loop-frame-written"' not in output
    health = health_payload(tmp_path / "health.json")
    assert health["status"] == "degraded"
    assert health["retention_failure_count"] == 1
    assert "raw_image_bytes abc" not in output
    assert_no_secret_leak(output)


def test_runtime_loop_passes_effective_paths_to_capture_state_and_matrix(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    captured_data_dirs: list[Path] = []
    matrix_paths: list[tuple[Path, Path]] = []

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        captured_data_dirs.append(Path(data_dir))
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class EmptyDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return []

    def matrix_factory(settings: object, data_dir: Path, logger: StructuredLogger) -> MatrixOutboxDelivery:
        matrix_paths.append((data_dir, settings.storage.snapshots_dir))  # type: ignore[attr-defined]
        return outbox_delivery(FakeMatrixClient(), data_dir, logger)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: EmptyDetector(),
        matrix_delivery_factory=matrix_factory,
        sleep=lambda _seconds: None,
        max_iterations=1,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert captured_data_dirs == [tmp_path]
    assert matrix_paths == [(tmp_path, tmp_path / "snapshots")]
    assert (tmp_path / "state.json").exists()
    assert_no_secret_leak(output)


def test_capture_once_default_capture_binds_configured_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor import __main__ as cli

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("capture_timeout_seconds: 15", "capture_timeout_seconds: 4"),
        encoding="utf-8",
    )
    capture_calls: list[dict[str, object]] = []

    def record_capture(
        _settings: object,
        data_dir: str | Path,
        *,
        logger: StructuredLogger,
        timeout_seconds: float | None = None,
        stream_profile: str | None = None,
    ) -> FrameCaptureResult:
        capture_calls.append(
            {
                "stream_profile": stream_profile,
                "timeout_seconds": timeout_seconds,
            }
        )
        return captured_frame(Path(data_dir))

    monkeypatch.setattr(cli, "capture_latest", record_capture)

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path), "--capture-once"],
        environ=fake_environ(),
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
    )

    assert exit_code == 0
    assert capture_calls == [
        {
            "stream_profile": None,
            "timeout_seconds": 4.0,
        }
    ]


def test_validate_config_success_emits_sanitized_startup_events(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--config", "config.yaml.example", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"event":"startup-config-load-start"' in output
    assert '"event":"startup-config-loaded"' in output
    assert '"event":"startup-ready"' in output
    assert '"env_var":"RTSP_URL"' in output
    assert '"env_var":"Matrix token env key"' in output
    assert "access_token" not in output.lower()
    assert_no_secret_leak(output)


def test_validate_config_does_not_capture(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor import __main__ as cli

    def fail_capture(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("validate-config must not call capture")

    monkeypatch.setattr(cli, "capture_latest", fail_capture)

    exit_code = main(["--config", "config.yaml.example", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"mode":"validate-config"' in output
    assert "capture" not in output.lower() or '"event":"startup-config-load-start"' in output
    assert_no_secret_leak(output)


def test_validate_config_does_not_construct_detector(capsys: pytest.CaptureFixture[str]) -> None:
    def fail_detector_factory(_settings: object) -> object:
        raise AssertionError("validate-config must not construct detector")

    exit_code = _main(
        ["--config", "config.yaml.example", "--validate-config"],
        environ=fake_environ(),
        detector_factory=fail_detector_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"mode":"validate-config"' in output
    assert '"event":"detection-frame-failed"' not in output
    assert_no_secret_leak(output)


def test_missing_config_exits_nonzero_with_safe_structured_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing_path = tmp_path / "missing.yaml"

    exit_code = main(["--config", str(missing_path), "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert '"event":"startup-config-invalid"' in output
    assert str(missing_path) in output
    assert '"phase":"read"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_missing_env_exits_nonzero_with_env_names_only(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--config", "config.yaml.example", "--validate-config"], environ={"RTSP_URL": ""})

    output = combined_output(capsys)
    assert exit_code == 2
    assert '"event":"startup-config-invalid"' in output
    assert "RTSP_URL" in output
    assert "MATRIX_ACCESS_TOKEN" in output
    assert '"phase":"env"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_invalid_yaml_exits_nonzero_without_traceback_or_secret(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "invalid.yaml"
    config_path.write_text("stream: [unterminated\n", encoding="utf-8")

    exit_code = main(["--config", str(config_path), "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert '"event":"startup-config-invalid"' in output
    assert '"phase":"yaml"' in output
    assert str(config_path) in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_unknown_cli_flag_exits_nonzero_without_secret(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--unknown-flag"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert "unrecognized arguments" in output
    assert '"event":"startup-arguments-invalid"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_data_dir_override_changes_sanitized_startup_summary(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(
        ["--config", "config.yaml.example", "--data-dir", "/tmp/parking-data", "--validate-config"],
        environ=fake_environ(),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"data_dir":"/tmp/parking-data"' in output
    assert '"event":"startup-ready"' in output
    assert_no_secret_leak(output)


def test_config_error_from_loader_is_converted_to_safe_exit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor import __main__ as cli

    def raise_config_error(*_args: object, **_kwargs: object) -> object:
        raise ConfigError(
            "synthetic safe config failure",
            path="config.yaml.example",
            phase="schema",
            fields=("stream.frame_width:Input should be greater than 0",),
        )

    monkeypatch.setattr(cli, "load_settings", raise_config_error)

    exit_code = main(["--config", "config.yaml.example", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert '"event":"startup-config-invalid"' in output
    assert "synthetic safe config failure" in output
    assert "stream.frame_width" in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)
