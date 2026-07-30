from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "verify_m007_matrix_outbox_closeout.py"


def _load_closeout_script_module():
    spec = importlib.util.spec_from_file_location("verify_m007_matrix_outbox_closeout", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_m007_closeout_contract_is_bounded_redacted_and_no_shell() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    module = _load_closeout_script_module()

    assert SCRIPT_PATH.exists()
    assert "subprocess.run(" in source
    assert "shell=True" not in source
    assert "timeout=" in source
    assert "stdout=subprocess.PIPE" in source
    assert "stderr=subprocess.PIPE" in source
    assert "--capture-once" not in source
    assert "--live-proof-once" not in source
    assert '".gsd"' not in source and "'.gsd'" not in source

    assert module.M007_CLOSEOUT_START == "M007_CLOSEOUT_START"
    assert module.M007_CLOSEOUT_PASS == "M007_CLOSEOUT_PASS"
    assert module.M007_CLOSEOUT_FAIL == "M007_CLOSEOUT_FAIL"
    assert module.M007_CLOSEOUT_RESULT == "M007_CLOSEOUT_RESULT"
    assert module.M007_OUTBOX_FAILURE_OK == "M007_OUTBOX_FAILURE_OK"
    assert module.M007_OUTBOX_RECOVERY_OK == "M007_OUTBOX_RECOVERY_OK"
    assert module.M007_OUTBOX_HEALTH_OK == "M007_OUTBOX_HEALTH_OK"
    assert module.M007_OUTBOX_DEAD_LETTER_OK == "M007_OUTBOX_DEAD_LETTER_OK"
    assert module.M007_OUTBOX_QUARANTINE_OK == "M007_OUTBOX_QUARANTINE_OK"
    assert module.M007_OUTBOX_RETENTION_OK == "M007_OUTBOX_RETENTION_OK"

    env = module.smoke_env(
        rtsp_placeholder=module.PLACEHOLDER_RTSP_URL,
        matrix_token_placeholder=module.PLACEHOLDER_MATRIX_TOKEN,
        base={},
        pythonpath_prefix=str(module.ROOT / "src"),
    )
    assert env["RTSP_URL"] == module.PLACEHOLDER_RTSP_URL
    assert env["MATRIX_ACCESS_TOKEN"] == module.PLACEHOLDER_MATRIX_TOKEN
    assert env["PYTHONPATH"] == str(module.ROOT / "src")

    env_with_existing_path = module.smoke_env(
        rtsp_placeholder=module.PLACEHOLDER_RTSP_URL,
        matrix_token_placeholder=module.PLACEHOLDER_MATRIX_TOKEN,
        base={"PYTHONPATH": "/already-there"},
        pythonpath_prefix=str(module.ROOT / "src"),
    )
    assert env_with_existing_path["PYTHONPATH"] == f"{module.ROOT / 'src'}{os.pathsep}/already-there"

    redacted = module.safe_output(
        "rtsp://camera.local/stream MATRIX_ACCESS_TOKEN=matrix-secret Authorization: bearer-secret",
        f"{module.PLACEHOLDER_RTSP_URL} {module.PLACEHOLDER_MATRIX_TOKEN} Traceback (most recent call last)",
        patterns=module.SENSITIVE_PATTERNS,
        limit=module.OUTPUT_LIMIT,
    )
    assert module.PLACEHOLDER_RTSP_URL not in redacted
    assert module.PLACEHOLDER_MATRIX_TOKEN not in redacted
    assert "rtsp://camera.local" not in redacted
    assert "matrix-secret" not in redacted
    assert "bearer-secret" not in redacted
    assert "Traceback (most recent call last)" not in redacted

    oversized = "x" * (module.OUTPUT_LIMIT + 25)
    bounded = module.bounded_text(oversized, limit=module.OUTPUT_LIMIT)
    assert len(bounded) < len(oversized)
    assert "chars omitted" in bounded


def test_m007_command_shape_uses_built_image_compose_and_two_restart_like_container_runs() -> None:
    module = _load_closeout_script_module()

    commands = {command.label: command for command in module._build_commands(Path("/tmp/m007-smoke-data"))}

    assert commands["pytest-matrix-outbox-health"].argv == (
        module.sys.executable,
        "-m",
        "pytest",
        "tests/test_matrix_outbox_delivery.py",
        "tests/test_health.py",
        "tests/test_startup.py::test_runtime_open_alert_failure_persists_retryable_matrix_outbox",
        "tests/test_startup.py::test_runtime_startup_drains_existing_matrix_outbox_without_new_occupancy_event",
        "-q",
    )
    assert commands["validate-config-entrypoint"].argv == (
        module.sys.executable,
        "-m",
        "parking_spot_monitor",
        "--config",
        "config.yaml.example",
        "--validate-config",
    )
    assert commands["docker-build"].argv == ("docker", "build", "-t", module.DOCKER_IMAGE_TAG, ".")
    assert commands["docker-compose-config"].argv == ("docker", "compose", "config", "--quiet")

    failure = commands["docker-matrix-outbox-failure"].argv
    recovery = commands["docker-matrix-outbox-recovery"].argv
    quarantine = commands["docker-matrix-outbox-quarantine"].argv
    dead_letter = commands["docker-matrix-outbox-dead-letter"].argv
    retention = commands["docker-matrix-outbox-retention"].argv
    expected_user = f"{os.getuid()}:{os.getgid()}"
    for argv in (failure, recovery, quarantine, dead_letter, retention):
        assert argv[:8] == ("docker", "run", "--rm", "--user", expected_user, "-e", "RTSP_URL", "-e")
        assert "MATRIX_ACCESS_TOKEN" in argv
        assert module.DOCKER_IMAGE_TAG in argv
        assert "python" in argv
        assert "-c" in argv
        assert "-v" in argv
        assert "/tmp/m007-smoke-data:/data" in argv
        assert any(value.endswith(":/config/config.yaml:ro") for value in argv)

    assert failure[-2:] == ("-c", module._failure_smoke_snippet())
    assert recovery[-2:] == ("-c", module._recovery_smoke_snippet())
    assert quarantine[-2:] == ("-c", module._quarantine_smoke_snippet())
    assert dead_letter[-2:] == ("-c", module._dead_letter_smoke_snippet())
    assert retention[-2:] == ("-c", module._retention_smoke_snippet())
    assert failure.index("-v") < failure.index(module.DOCKER_IMAGE_TAG)
    assert commands["docker-build"].timeout_seconds == module.DOCKER_TIMEOUT_SECONDS
    assert commands["docker-matrix-outbox-quarantine"].timeout_seconds == 60
    assert commands["docker-matrix-outbox-dead-letter"].timeout_seconds == 60
    assert commands["docker-matrix-outbox-retention"].timeout_seconds == 60


def test_m007_in_container_smoke_snippets_assert_failure_health_and_recovery_contracts() -> None:
    module = _load_closeout_script_module()
    failure = module._failure_smoke_snippet()
    recovery = module._recovery_smoke_snippet()
    quarantine = module._quarantine_smoke_snippet()
    dead_letter = module._dead_letter_smoke_snippet()
    retention = module._retention_smoke_snippet()

    assert "MatrixOutboxDelivery" in failure
    assert "LocalOutbox(DATA / 'matrix-outbox.json')" in failure
    assert "MatrixError" in failure
    assert "send_open_spot_alert" in failure
    assert "assert record.state == 'retrying'" in failure
    assert "matrix_upload_timeout" in failure
    assert "_matrix_outbox_health_payload" in failure
    assert "write_health_status" in failure
    assert "M007_OUTBOX_FAILURE_OK" in failure
    assert "M007_OUTBOX_HEALTH_OK" in failure
    assert "should-not-leak" not in module.safe_output(
        failure,
        "",
        patterns=module.SENSITIVE_PATTERNS,
        limit=module.OUTPUT_LIMIT,
    )

    assert "drain_outbox" in recovery
    assert "assert [call['kind'] for call in client.calls] == ['upload', 'image']" in recovery
    assert "assert record.state == 'delivered'" in recovery
    assert "counts_by_state'] == {'delivered': 1}" in recovery
    assert "M007_OUTBOX_RECOVERY_OK" in recovery
    assert "send_text" in recovery

    assert "invalid_json" in quarantine
    assert ".matrix-outbox-quarantine" in quarantine
    assert "quarantined_count'] == 1" in quarantine
    assert "M007_OUTBOX_QUARANTINE_OK" in quarantine
    quarantine_output = module.safe_output(
        quarantine,
        "",
        patterns=module.SENSITIVE_PATTERNS,
        limit=module.OUTPUT_LIMIT,
    )
    assert "quarantine-secret" not in quarantine_output
    assert "BEGIN RAW IMAGE BYTES" not in quarantine_output

    assert "MatrixError('Matrix upload rejected'" in dead_letter
    assert "status_code=403" in dead_letter
    assert "assert record.state == 'dead_lettered'" in dead_letter
    assert "matrix_upload_http_403" in dead_letter
    assert "assert later.attempted_count == 0" in dead_letter
    assert "dead_letter_reason_counts" in dead_letter
    assert "M007_OUTBOX_DEAD_LETTER_OK" in dead_letter

    assert "snapshot_retention_count=1" in retention
    assert "unlink(missing_ok=True)" in retention
    assert "shutil.rmtree(DATA / 'snapshots', ignore_errors=True)" in retention
    assert "retained_snapshot_path" in retention
    assert "original_retained_bytes" in retention
    assert "assert recovery.upload_bytes == original_retained_bytes" in retention
    assert "assert not stale.exists()" in retention
    assert "M007_OUTBOX_RETENTION_OK" in retention


def test_m007_run_command_binds_canonical_output_policy(monkeypatch, capsys) -> None:
    module = _load_closeout_script_module()
    calls: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="raw stdout",
            stderr="raw stderr",
        ),
    )

    def fake_safe_output(stdout, stderr, *, patterns, limit):
        calls.append(("safe_output", stdout, stderr, patterns, limit))
        return "safe rendered output"

    def fake_assert_no_forbidden_markers(rendered, forbidden_markers):
        calls.append(("assert_no_forbidden_markers", rendered, forbidden_markers))

    monkeypatch.setattr(module, "safe_output", fake_safe_output)
    monkeypatch.setattr(
        module,
        "assert_no_forbidden_markers",
        fake_assert_no_forbidden_markers,
    )

    result = module._run_command(
        module.SmokeCommand(label="delegated", argv=("command",)),
        env={},
    )

    assert result == 0
    assert calls == [
        (
            "safe_output",
            "raw stdout",
            "raw stderr",
            module.SENSITIVE_PATTERNS,
            module.OUTPUT_LIMIT,
        ),
        (
            "assert_no_forbidden_markers",
            "safe rendered output",
            module.FORBIDDEN_OUTPUT_MARKERS,
        ),
    ]
    assert "safe rendered output" in capsys.readouterr().out


def test_m007_main_binds_canonical_smoke_environment(monkeypatch) -> None:
    module = _load_closeout_script_module()
    calls: list[dict[str, object]] = []

    def fake_smoke_env(**kwargs):
        calls.append(kwargs)
        return {"SAFE": "environment"}

    monkeypatch.setattr(module, "smoke_env", fake_smoke_env)
    monkeypatch.setattr(module, "_build_commands", lambda _data_dir: [])

    result = module.main([])

    assert result == 0
    assert calls == [
        {
            "rtsp_placeholder": module.PLACEHOLDER_RTSP_URL,
            "matrix_token_placeholder": module.PLACEHOLDER_MATRIX_TOKEN,
            "base": None,
            "pythonpath_prefix": str(module.ROOT / "src"),
        }
    ]


def test_m007_run_command_reports_redacted_failure_without_raising(monkeypatch, capsys) -> None:
    module = _load_closeout_script_module()

    def fake_run(*args, **kwargs):
        return SimpleNamespace(
            returncode=7,
            stdout="MATRIX_ACCESS_TOKEN=matrix-secret",
            stderr=f"{module.PLACEHOLDER_RTSP_URL} Traceback (most recent call last)",
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    result = module._run_command(module.SmokeCommand(label="fake-fail", argv=("missing",)), env={})

    assert result == 7
    output = capsys.readouterr().out
    assert "M007_CLOSEOUT_START fake-fail" in output
    assert "M007_CLOSEOUT_FAIL fake-fail exit_code=7" in output
    assert "matrix-secret" not in output
    assert module.PLACEHOLDER_RTSP_URL not in output
    assert "Traceback (most recent call last)" not in output


def test_m007_run_command_reports_missing_executable_and_timeout(monkeypatch, capsys) -> None:
    module = _load_closeout_script_module()

    def missing_run(*args, **kwargs):
        raise FileNotFoundError("docker MATRIX_ACCESS_TOKEN=matrix-secret")

    monkeypatch.setattr(module.subprocess, "run", missing_run)
    missing = module._run_command(module.SmokeCommand(label="missing", argv=("docker",)), env={})
    assert missing == 127
    missing_output = capsys.readouterr().out
    assert "M007_CLOSEOUT_FAIL missing exit_code=127" in missing_output
    assert "matrix-secret" not in missing_output

    def timeout_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=["docker"], timeout=1, output=b"rtsp://camera.local/stream", stderr=b"access_token=secret")

    monkeypatch.setattr(module.subprocess, "run", timeout_run)
    timeout = module._run_command(module.SmokeCommand(label="timeout", argv=("docker",), timeout_seconds=1), env={})
    assert timeout == 124
    timeout_output = capsys.readouterr().out
    assert "M007_CLOSEOUT_FAIL timeout timeout_seconds=1" in timeout_output
    assert "rtsp://camera.local" not in timeout_output
    assert "secret" not in timeout_output


def test_m007_usage_rejects_unexpected_args(capsys) -> None:
    module = _load_closeout_script_module()

    assert module.main(["--unexpected"]) == 2
    captured = capsys.readouterr()
    assert "usage: verify_m007_matrix_outbox_closeout.py" in captured.err
