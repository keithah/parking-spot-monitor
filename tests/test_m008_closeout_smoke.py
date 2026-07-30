from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "verify_m008_operator_intelligence_closeout.py"


def _load_closeout_script_module():
    spec = importlib.util.spec_from_file_location("verify_m008_operator_intelligence_closeout", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_m008_closeout_smoke_contract_is_bounded_redacted_and_no_shell() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    module = _load_closeout_script_module()

    assert SCRIPT_PATH.exists()
    assert "subprocess.run(" in source
    assert "shell=True" not in source
    assert "timeout=" in source
    assert "stdout=subprocess.PIPE" in source
    assert "stderr=subprocess.PIPE" in source
    assert "M008_CLOSEOUT_START" in source
    assert "M008_CLOSEOUT_PASS" in source
    assert "M008_CLOSEOUT_FAIL" in source
    assert "M008_CLOSEOUT_RESULT" in source
    assert "m008-closeout-status.json" in source
    assert "live_matrix_proof" in source
    assert "&&" not in source
    assert "||" not in source
    assert "; pytest" not in source
    assert "--capture-once" not in source
    assert "--live-proof-once" not in source
    assert '".gsd"' not in source and "'.gsd'" not in source

    commands = {command.label: command for command in module._build_commands()}
    assert list(commands) == [
        "pytest-matrix-commands",
        "pytest-decision-memory-feedback",
        "pytest-occupancy-analytics-history",
        "pytest-full-regression",
    ]
    assert commands["pytest-matrix-commands"].argv == (
        module.sys.executable,
        "-m",
        "pytest",
        *module.MATRIX_TEST_MODULES,
        *module.MATRIX_COCKPIT_TEST_MODULES,
        "tests/test_operator_cockpit.py",
        "tests/test_operator_docs.py",
        "-q",
    )
    assert commands["pytest-decision-memory-feedback"].argv == (
        module.sys.executable,
        "-m",
        "pytest",
        "tests/test_operator_decision_memory.py",
        *module.OPERATOR_FEEDBACK_TEST_MODULES,
        "-q",
    )
    assert commands["pytest-occupancy-analytics-history"].argv == (
        module.sys.executable,
        "-m",
        "pytest",
        "tests/test_occupancy_analytics.py",
        *module.VEHICLE_HISTORY_TEST_MODULES,
        "tests/test_vehicle_history_cli.py",
        "tests/test_owner_vehicles.py",
        "tests/test_vehicle_estimates.py",
        "tests/test_vehicle_profiles.py",
        "-q",
    )
    assert commands["pytest-full-regression"].argv == (module.sys.executable, "-m", "pytest", "-q")
    assert commands["pytest-full-regression"].timeout_seconds == module.FULL_REGRESSION_TIMEOUT_SECONDS

    env = module._smoke_env({})
    assert env["RTSP_URL"] == module.PLACEHOLDER_RTSP_URL
    assert env["MATRIX_ACCESS_TOKEN"] == module.PLACEHOLDER_MATRIX_TOKEN
    assert str(ROOT / "src") in env["PYTHONPATH"]

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


def test_m008_closeout_marker_behavior_for_pass_and_fail(monkeypatch, capsys) -> None:
    module = _load_closeout_script_module()
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, stdout="ok", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    command = module.SmokeCommand(label="unit-pass", argv=("python", "-m", "pytest", "-q"), timeout_seconds=5)

    assert module._run_command(command, env={}) == 0
    output = capsys.readouterr().out
    assert "M008_CLOSEOUT_START unit-pass" in output
    assert "M008_CLOSEOUT_PASS unit-pass" in output
    assert "stdout:\nok" in output
    assert calls == [["python", "-m", "pytest", "-q"]]

    def fake_fail(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 3, stdout="", stderr="failed")

    monkeypatch.setattr(module.subprocess, "run", fake_fail)
    assert module._run_command(module.SmokeCommand(label="unit-fail", argv=("pytest",), timeout_seconds=5), env={}) == 3
    output = capsys.readouterr().out
    assert "M008_CLOSEOUT_START unit-fail" in output
    assert "M008_CLOSEOUT_FAIL unit-fail exit_code=3" in output
    assert "stderr:\nfailed" in output


def test_m008_closeout_marker_behavior_for_timeout_and_main_result(monkeypatch, capsys, tmp_path) -> None:
    module = _load_closeout_script_module()
    monkeypatch.setattr(module, "STATUS_PATH", tmp_path / "m008-closeout-status.json")

    def fake_timeout(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, timeout=1, output="partial", stderr="late")

    monkeypatch.setattr(module.subprocess, "run", fake_timeout)
    command = module.SmokeCommand(label="unit-timeout", argv=("pytest",), timeout_seconds=1)

    assert module._run_command(command, env={}) == 124
    output = capsys.readouterr().out
    assert "M008_CLOSEOUT_START unit-timeout" in output
    assert "M008_CLOSEOUT_FAIL unit-timeout timeout_seconds=1" in output
    assert "stdout:\npartial" in output
    assert "stderr:\nlate" in output

    commands = (
        module.SmokeCommand(label="first", argv=("pytest", "first")),
        module.SmokeCommand(label="second", argv=("pytest", "second")),
    )
    monkeypatch.setattr(module, "_build_commands", lambda: commands)
    monkeypatch.setattr(module, "_smoke_env", lambda: {})
    monkeypatch.setattr(module, "_run_command", lambda command, *, env: 0 if command.label == "first" else 7)

    assert module.main([]) == 7
    output = capsys.readouterr().out
    assert "M008_CLOSEOUT_RESULT failed label=second exit_code=7" in output
    status = json.loads(module.STATUS_PATH.read_text(encoding="utf-8"))
    assert status["milestone"] == "M008"
    assert status["result"] == "failed"
    assert status["failed_label"] == "second"
    assert status["commands"][-1]["exit_code"] == 7
    assert "deferred" in status["live_matrix_proof"]

    monkeypatch.setattr(module, "_run_command", lambda command, *, env: 0)
    assert module.main([]) == 0
    output = capsys.readouterr().out
    assert "M008_CLOSEOUT_RESULT passed" in output
    status = json.loads(module.STATUS_PATH.read_text(encoding="utf-8"))
    assert status["result"] == "passed"
    assert status["exit_code"] == 0
    assert [command["label"] for command in status["commands"]] == ["first", "second"]


def test_m008_closeout_usage_rejects_unexpected_args(capsys) -> None:
    module = _load_closeout_script_module()

    assert module.main(["--unexpected"]) == 2
    captured = capsys.readouterr()
    assert "usage: verify_m008_operator_intelligence_closeout.py" in captured.err
