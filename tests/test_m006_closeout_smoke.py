from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "verify_m006_incident_intelligence_closeout.py"


def _load_closeout_script_module():
    spec = importlib.util.spec_from_file_location("verify_m006_incident_intelligence_closeout", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_m006_closeout_smoke_contract_is_bounded_redacted_and_no_shell() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    module = _load_closeout_script_module()

    assert SCRIPT_PATH.exists()
    assert "subprocess.run(" in source
    assert "shell=True" not in source
    assert "timeout=" in source
    assert "stdout=subprocess.PIPE" in source
    assert "stderr=subprocess.PIPE" in source
    assert "M006_CLOSEOUT_START" in source
    assert "M006_CLOSEOUT_PASS" in source
    assert "M006_CLOSEOUT_FAIL" in source
    assert "M006_CLOSEOUT_DATA" in source
    assert "M006_CLOSEOUT_RESULT" in source
    assert "--capture-once" not in source
    assert "--live-proof-once" not in source
    assert "run_docker_live_proof" not in source
    assert "shell snippets" not in source.lower()
    assert '\".gsd\"' not in source and "'.gsd'" not in source

    commands = {command.label: command.argv for command in module._build_commands(Path("/tmp/m006-smoke-data"))}
    assert commands["pytest-m006-contracts"] == (
        module.sys.executable,
        "-m",
        "pytest",
        "tests/test_operator_docs.py",
        "tests/test_matrix_operator_cockpit.py",
        "tests/test_operator_feedback.py",
        "tests/test_matrix.py",
        "tests/test_timeline_buffer.py",
        "-q",
    )
    assert commands["pytest-runtime-config-state"] == (
        module.sys.executable,
        "-m",
        "pytest",
        "tests/test_startup.py",
        "tests/test_docker_contract.py",
        "tests/test_config.py",
        "tests/test_state.py",
        "tests/test_health.py",
        "-q",
    )
    assert commands["validate-config-entrypoint"] == (
        module.sys.executable,
        "-m",
        "parking_spot_monitor",
        "--config",
        "config.yaml.example",
        "--validate-config",
    )
    assert commands["docker-build"] == ("docker", "build", "-t", module.DOCKER_IMAGE_TAG, ".")
    assert commands["docker-compose-config"] == ("docker", "compose", "config", "--quiet")
    docker_run = commands["docker-run-validate-config"]
    assert docker_run[:6] == ("docker", "run", "--rm", "-e", "RTSP_URL", "-e")
    assert "MATRIX_ACCESS_TOKEN" in docker_run
    assert "--config" in docker_run
    assert "/config/config.yaml" in docker_run
    assert "--data-dir" in docker_run
    assert "/data" in docker_run
    assert "--validate-config" in docker_run

    env = module._smoke_env({})
    assert env["RTSP_URL"] == module.PLACEHOLDER_RTSP_URL
    assert env["MATRIX_ACCESS_TOKEN"] == module.PLACEHOLDER_MATRIX_TOKEN

    redacted = module._safe_output(
        "rtsp://camera.local/stream MATRIX_ACCESS_TOKEN=matrix-secret Authorization: bearer-secret",
        f"{module.PLACEHOLDER_RTSP_URL} {module.PLACEHOLDER_MATRIX_TOKEN} Traceback (most recent call last)",
    )
    assert module.PLACEHOLDER_RTSP_URL not in redacted
    assert module.PLACEHOLDER_MATRIX_TOKEN not in redacted
    assert "rtsp://camera.local" not in redacted
    assert "matrix-secret" not in redacted
    assert "bearer-secret" not in redacted
    assert "Traceback (most recent call last)" not in redacted

    oversized = "x" * (module.OUTPUT_LIMIT + 25)
    bounded = module._bounded(oversized)
    assert len(bounded) < len(oversized)
    assert "chars omitted" in bounded


def test_m006_closeout_data_inspection_reports_safe_empty_without_failure(tmp_path, capsys) -> None:
    module = _load_closeout_script_module()

    assert module._inspect_local_data(tmp_path / "missing-data") == 0
    missing_output = capsys.readouterr().out
    assert "M006_CLOSEOUT_DATA data_dir status=safe-empty reason=missing" in missing_output

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    assert module._inspect_local_data(data_dir) == 0
    empty_output = capsys.readouterr().out
    assert "M006_CLOSEOUT_DATA timeline_frames status=safe-empty count=0" in empty_output
    assert "M006_CLOSEOUT_DATA health status=missing" in empty_output
    assert "M006_CLOSEOUT_DATA state status=missing" in empty_output
    assert "M006_CLOSEOUT_DATA operator_feedback_labels status=missing" in empty_output


def test_m006_closeout_data_inspection_bounds_present_and_malformed_artifacts(tmp_path, capsys) -> None:
    module = _load_closeout_script_module()
    data_dir = tmp_path / "data"
    frames_dir = data_dir / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    for index in range(3):
        (frames_dir / f"frame-{index}.jpg").write_bytes(b"not-real-image-bytes")
    (data_dir / "health.json").write_text('{"status": "ok", "secret": "rtsp://camera.local/stream"}', encoding="utf-8")
    (data_dir / "state.json").write_text("{malformed", encoding="utf-8")
    (data_dir / "operator-feedback-labels.json").write_text("[]", encoding="utf-8")

    assert module._inspect_local_data(data_dir) == 0
    output = capsys.readouterr().out
    assert "M006_CLOSEOUT_DATA timeline_frames status=present count=3" in output
    assert "M006_CLOSEOUT_DATA health status=present" in output
    assert "M006_CLOSEOUT_DATA state status=malformed" in output
    assert "M006_CLOSEOUT_DATA operator_feedback_labels status=present" in output
    assert "rtsp://camera.local" not in output
    assert "not-real-image-bytes" not in output


def test_m006_closeout_usage_rejects_unexpected_args(capsys) -> None:
    module = _load_closeout_script_module()

    assert module.main(["--unexpected"]) == 2
    captured = capsys.readouterr()
    assert "usage: verify_m006_incident_intelligence_closeout.py" in captured.err
