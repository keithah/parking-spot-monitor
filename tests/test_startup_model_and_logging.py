from __future__ import annotations

from tests.support._startup import *  # noqa: F403


def test_explicit_missing_model_path_fails_before_runtime_loop(tmp_path: Path) -> None:
    from parking_spot_monitor import __main__ as cli

    missing = tmp_path / "models" / "yolov8n.pt"

    with pytest.raises(ConfigError, match="configured model file does not exist"):
        cli.validate_model_path(str(missing))


def test_explicit_relative_missing_model_path_fails_before_runtime_loop() -> None:
    from parking_spot_monitor import __main__ as cli

    with pytest.raises(ConfigError, match="configured model file does not exist"):
        cli.validate_model_path("models/yolov8n.pt")


@pytest.mark.parametrize(
    "model",
    [
        r"C:\models\yolov8n.pt",
        r"C:yolov8n.pt",
        r".\models\yolov8n.pt",
        r"\models\yolov8n.pt",
    ],
)
def test_windows_style_model_paths_fail_as_missing_explicit_paths(model: str) -> None:
    from parking_spot_monitor import __main__ as cli

    with pytest.raises(ConfigError, match="configured model file does not exist"):
        cli.validate_model_path(model)


def test_existing_explicit_posix_model_path_with_spaces_is_allowed(tmp_path: Path) -> None:
    from parking_spot_monitor import __main__ as cli

    model = tmp_path / "trusted model weights" / "yolo nano.pt"
    model.parent.mkdir()
    model.touch()

    cli.validate_model_path(str(model))


def test_legacy_bare_model_name_does_not_require_local_file() -> None:
    from parking_spot_monitor import __main__ as cli

    cli.validate_model_path("yolov8n.pt")


def test_validate_config_rejects_explicit_missing_model_before_ready(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = tmp_path / "models" / "yolov8n.pt"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("/models/yolov8n.pt", str(missing)),
        encoding="utf-8",
    )

    exit_code = main(["--config", str(config_path), "--validate-config"], environ=fake_environ())

    output = combined_output(capsys)
    assert exit_code == 2
    assert "configured model file does not exist" in output
    assert '"phase":"model"' in output
    assert '"event":"startup-ready"' not in output


def test_structured_logger_recursively_redacts_secret_bearing_fields(capsys: pytest.CaptureFixture[str]) -> None:
    logger = StructuredLogger()

    logger.info(
        "sentinel-redaction-check",
        message="rtsp://user:pass@camera token=top-secret Traceback noisy",
        nested={"frame_path": "/data/latest.jpg?access_token=frame-secret"},
        items=["matrix_token=list-secret"],
    )

    output = combined_output(capsys)
    assert '"event":"sentinel-redaction-check"' in output
    assert "rtsp://<redacted>" in output
    assert "token=<redacted>" in output
    assert "access_token=<redacted>" in output
    assert "matrix_token=<redacted>" in output
    assert "user:pass" not in output
    assert "top-secret" not in output
    assert "frame-secret" not in output
    assert "list-secret" not in output
    assert "Traceback" not in output


def test_importing_main_does_not_import_operator_stack() -> None:
    blocked = {
        "parking_spot_monitor.matrix_cockpit",
        "parking_spot_monitor.matrix_commands",
        "parking_spot_monitor.operator_cockpit",
        "parking_spot_monitor.operator_feedback",
        "parking_spot_monitor.detection_lab",
    }
    script = (
        "import sys; sys.path.insert(0, 'src'); import parking_spot_monitor.__main__; "
        f"blocked={blocked!r}; "
        "present=sorted(blocked.intersection(sys.modules)); "
        "assert not present, present"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_disabled_matrix_commands_do_not_import_operator_stack() -> None:
    blocked = {
        "parking_spot_monitor.matrix_cockpit",
        "parking_spot_monitor.matrix_commands",
        "parking_spot_monitor.operator_cockpit",
        "parking_spot_monitor.operator_feedback",
        "parking_spot_monitor.detection_lab",
    }
    script = (
        "import sys; sys.path.insert(0, 'src'); "
        "from io import StringIO; from types import SimpleNamespace; "
        "import parking_spot_monitor.__main__ as cli; "
        "from parking_spot_monitor.logging import StructuredLogger; "
        "settings=SimpleNamespace(matrix=SimpleNamespace(command_authorized_senders=[])); "
        "result=cli._default_matrix_command_service_factory("
        "settings, None, StructuredLogger(stream=StringIO()), object(), incident_detector=object()); "
        "assert result is None; "
        f"blocked={blocked!r}; "
        "present=sorted(blocked.intersection(sys.modules)); "
        "assert not present, present"
    )

    completed = subprocess.run(
        [sys.executable, "-c", script], text=True, capture_output=True, check=False
    )

    assert completed.returncode == 0, completed.stderr


def test_logger_reports_normalized_enabled_levels_without_serializing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import parking_spot_monitor.logging as structured_logging

    logger = StructuredLogger(level="INFO")
    monkeypatch.setattr(
        structured_logging,
        "redact_diagnostic_value",
        lambda _value: pytest.fail("level query serialized a log record"),
    )

    assert logger.is_enabled_for("debug") is False
    assert logger.is_enabled_for("info") is True
    assert logger.is_enabled_for("WARNING") is True
    assert logger.is_enabled_for("not-a-level") is True
