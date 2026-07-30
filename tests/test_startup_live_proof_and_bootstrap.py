from __future__ import annotations

from tests.support._startup import *  # noqa: F403


def test_live_proof_once_captures_raw_frame_and_sends_labelled_matrix_evidence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--live-proof-once"],
        environ=fake_environ(),
        capture=fake_capture,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert len(delivery.live_proofs) == 1
    assert delivery.live_proofs[0]["latest_path"] == tmp_path / "latest.jpg"
    assert delivery.live_proofs[0]["observed_at"] == "2026-05-18T19:00:00Z"
    assert (tmp_path / "latest.jpg").exists()
    assert '"event":"live-proof-started"' in output
    assert '"event":"live-proof-capture-ok"' in output
    assert '"event":"live-proof-matrix-text-ok"' in output
    assert '"event":"live-proof-matrix-image-ok"' in output
    assert '"event":"detection-frame-processed"' not in output
    assert '"event":"matrix-delivery-succeeded"' not in output
    assert_no_secret_leak(output)


def test_live_proof_once_capture_failure_returns_safe_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        raise CaptureError(
            reason="ffmpeg-timeout",
            mode=DecodeMode.SOFTWARE,
            output_path=Path(data_dir) / "latest.jpg",
            message=f"timed out rtsp://user:pass@camera token={SECRET_MARKER}",
            stderr_tail=f"stderr token={SECRET_MARKER}",
            timeout_seconds=15.0,
        )

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--live-proof-once"],
        environ=fake_environ(),
        capture=fake_capture,
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"live-proof-capture-failed"' in output
    assert '"marker":"LIVE_RTSP_CAPTURE_FAILED"' in output
    assert '"event":"live-proof-matrix-text-ok"' not in output
    assert "user:pass" not in output
    assert_no_secret_leak(output)


def test_live_proof_once_matrix_text_failure_returns_safe_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    class TextFailDelivery:
        def send_live_proof_text(self, *, observed_at: object, selected_mode: object) -> None:
            raise RuntimeError(f"text failed token={SECRET_MARKER}")

        def send_live_proof_image(self, *, latest_path: Path, observed_at: object, selected_mode: object) -> None:
            raise AssertionError("image must not be sent after text failure")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--live-proof-once"],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: TextFailDelivery(),
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"live-proof-matrix-text-failed"' in output
    assert '"marker":"LIVE_MATRIX_TEXT_FAILED"' in output
    assert '"event":"live-proof-matrix-image-ok"' not in output
    assert_no_secret_leak(output)


def test_live_proof_once_matrix_image_failure_returns_safe_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    class ImageFailDelivery:
        def send_live_proof_text(self, *, observed_at: object, selected_mode: object) -> None:
            return None

        def send_live_proof_image(self, *, latest_path: Path, observed_at: object, selected_mode: object) -> None:
            raise RuntimeError(f"image failed token={SECRET_MARKER}")

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path), "--live-proof-once"],
        environ=fake_environ(),
        capture=lambda _settings, data_dir, **_kwargs: captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z"),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: ImageFailDelivery(),
    )

    output = combined_output(capsys)
    assert exit_code == 1
    assert '"event":"live-proof-matrix-text-ok"' in output
    assert '"event":"live-proof-matrix-image-failed"' in output
    assert '"marker":"LIVE_MATRIX_IMAGE_FAILED"' in output
    assert_no_secret_leak(output)


def test_validate_config_does_not_construct_matrix_delivery(capsys: pytest.CaptureFixture[str]) -> None:
    def fail_matrix_factory(_settings: object, _data_dir: Path, _logger: StructuredLogger) -> object:
        raise AssertionError("validate-config must not construct Matrix delivery")

    exit_code = _main(
        ["--config", "config.yaml.example", "--validate-config"],
        environ=fake_environ(),
        matrix_delivery_factory=fail_matrix_factory,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert '"mode":"validate-config"' in output
    assert '"event":"matrix-delivery-attempt"' not in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_suppressed_open_event_sends_no_open_alert(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery()

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
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 20, 30, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert [notice["event_type"] for notice in delivery.quiet_notices] == ["quiet-window-started"]
    assert delivery.open_alerts == []
    assert '"event":"matrix-delivery-skipped"' in output
    assert '"event_type":"occupancy-open-suppressed"' in output
    assert '"reason":"suppressed"' in output
    assert_no_secret_leak(output)


def test_runtime_loop_matrix_delivery_failure_logs_and_continues(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()], [], [], []]
    delivery = FakeMatrixDelivery(fail=True)

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
        max_iterations=6,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert len(delivery.open_alerts) == 1
    assert state_status(tmp_path / "state.json", "left_spot") == "empty"
    assert '"event":"matrix-delivery-failed"' in output
    assert '"event_type":"occupancy-open-event"' in output
    assert '"error_type":"RuntimeError"' in output
    assert '"event":"state-saved"' in output
    assert "Traceback" not in output
    assert_no_secret_leak(output)


def test_validate_config_success_emits_effective_runtime_paths_without_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--config", "config.yaml.example", "--data-dir", "/data", "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    records = json_records(output)
    loaded = next(record for record in records if record.get("event") == "startup-config-loaded")

    assert exit_code == 0
    assert loaded["config"]["storage"]["data_dir"] == "/data"
    assert loaded["config"]["storage"]["state_file"] == "/data/state.json"
    assert loaded["config"]["storage"]["latest_frame"] == "/data/latest.jpg"
    assert loaded["config"]["storage"]["snapshots_dir"] == "/data/snapshots"
    assert loaded["config"]["runtime"]["health_file"] == "/data/health.json"
    assert loaded["config"]["runtime"]["frame_interval_seconds"] == 30
    assert_no_secret_leak(output)


def test_runtime_prepares_ultralytics_config_before_detector_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_modules = tmp_path / "fake-modules"
    fake_modules.mkdir()
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    data_dir = tmp_path / "data"
    yolo_config_dir = data_dir / "ultralytics"
    (fake_modules / "ultralytics.py").write_text(
        """\
import os
from pathlib import Path

config_dir = Path(os.environ.get("YOLO_CONFIG_DIR", Path.home() / ".config" / "Ultralytics"))
config_dir.mkdir(parents=True, exist_ok=True)
(config_dir / "settings.json").write_text("{}", encoding="utf-8")
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(fake_modules))
    monkeypatch.delitem(sys.modules, "ultralytics", raising=False)
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("YOLO_CONFIG_DIR", "test-unset-sentinel")
    monkeypatch.delenv("YOLO_CONFIG_DIR", raising=False)

    class ImportingDetector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            return []

    def detector_factory(_settings: object) -> ImportingDetector:
        assert yolo_config_dir.is_dir()
        assert (yolo_config_dir.stat().st_mode & 0o777) == 0o750
        __import__("ultralytics")
        return ImportingDetector()

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(data_dir)],
        environ=fake_environ(),
        capture=lambda _settings, _data_dir, **_kwargs: captured_frame(tmp_path),
        overlay=noop_overlay,
        detector_factory=detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    assert exit_code == 0
    assert os.environ["YOLO_CONFIG_DIR"] == str(yolo_config_dir)
    assert (yolo_config_dir / "settings.json").is_file()
    assert not (fake_home / ".config" / "Ultralytics" / "settings.json").exists()
    assert_no_secret_leak(combined_output(capsys))


def test_repeated_runtime_main_replaces_its_managed_ultralytics_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_modules = tmp_path / "fake-modules"
    fake_modules.mkdir()
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    (fake_modules / "ultralytics.py").write_text(
        """\
import os
from pathlib import Path

config_dir = Path(os.environ.get("YOLO_CONFIG_DIR", Path.home() / ".config" / "Ultralytics"))
config_dir.mkdir(parents=True, exist_ok=True)
(config_dir / "settings.json").write_text("{}", encoding="utf-8")
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(fake_modules))
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("YOLO_CONFIG_DIR", "test-unset-sentinel")
    monkeypatch.delenv("YOLO_CONFIG_DIR")
    for key, value in fake_environ().items():
        monkeypatch.setenv(key, value)

    class ImportingDetector:
        def detect(
            self,
            _frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            return []

    def detector_factory(_settings: object) -> ImportingDetector:
        monkeypatch.delitem(sys.modules, "ultralytics", raising=False)
        __import__("ultralytics")
        return ImportingDetector()

    data_dirs = [tmp_path / "runtime-a", tmp_path / "runtime-b"]
    exit_codes = [
        _main(
            ["--config", "config.yaml.example", "--data-dir", str(data_dir)],
            capture=lambda _settings, actual_data_dir, **_kwargs: captured_frame(
                Path(actual_data_dir)
            ),
            overlay=noop_overlay,
            detector_factory=detector_factory,
            matrix_delivery_factory=lambda *_args: FakeMatrixDelivery(),
            sleep=lambda _seconds: None,
            max_iterations=1,
        )
        for data_dir in data_dirs
    ]

    assert exit_codes == [0, 0]
    assert os.environ["YOLO_CONFIG_DIR"] == str(data_dirs[1] / "ultralytics")
    assert all(
        (data_dir / "ultralytics" / "settings.json").is_file()
        for data_dir in data_dirs
    )
    assert not (fake_home / ".config" / "Ultralytics" / "settings.json").exists()
    assert_no_secret_leak(combined_output(capsys))


def test_explicit_operator_ultralytics_path_must_match_runtime_data_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor.paths import prepare_ultralytics_config_dir

    data_dir = tmp_path / "runtime"
    operator_path = tmp_path / "operator-selected" / "ultralytics"
    monkeypatch.setenv("YOLO_CONFIG_DIR", str(operator_path))

    with pytest.raises(ValueError, match="must be the ultralytics directory"):
        prepare_ultralytics_config_dir(data_dir)

    assert os.environ["YOLO_CONFIG_DIR"] == str(operator_path)
    assert not (data_dir / "ultralytics").exists()


def test_runtime_loop_startup_prunes_existing_event_snapshots_without_touching_runtime_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    old = snapshots / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    newest = snapshots / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    latest = snapshots / "latest.jpg"
    state_file = tmp_path / "state.json"
    health_file = tmp_path / "health.json"
    for path in [old, newest, latest, state_file, health_file]:
        path.write_bytes(b"runtime-artifact")
    (tmp_path / "matrix-outbox.json").write_text(
        json.dumps({"schema_version": 1, "items": []}),
        encoding="utf-8",
    )

    base = Path("config.yaml.example").read_text(encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(base.replace("snapshot_retention_count: 50", "snapshot_retention_count: 1"), encoding="utf-8")

    def fake_capture(_settings: object, data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(Path(data_dir), timestamp="2026-05-18T19:00:00Z")

    exit_code = _main(
        ["--config", str(config_path), "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=noop_detector_factory,
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: FakeMatrixDelivery(),
        sleep=lambda _seconds: None,
        max_iterations=1,
    )

    output = combined_output(capsys)
    assert exit_code == 0
    assert not old.exists()
    assert newest.exists()
    assert latest.exists()
    assert state_file.exists()
    assert health_file.exists()
    assert '"event":"snapshot-retention-pruned"' in output
    assert '"trigger":"startup"' in output
    assert_no_secret_leak(output)
