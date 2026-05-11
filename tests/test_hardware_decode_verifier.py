from __future__ import annotations

import subprocess
from typing import Sequence

from scripts.verify_hardware_decode import DockerCommandRunner, evaluate_results, run_diagnostics


def completed(argv: Sequence[str], returncode: int, stderr: str = "", stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(list(argv), returncode, stdout=stdout, stderr=stderr)


def test_hardware_decode_diagnostics_accept_vaapi_when_qsv_is_unavailable() -> None:
    calls: list[list[str]] = []

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        command_text = " ".join(argv)
        if "vainfo" in command_text:
            return completed(argv, 0, stdout="Driver version: Intel iHD driver\n")
        if "vaapi=va" in command_text:
            return completed(argv, 0, stderr="VAAPI ok\n")
        if "qsv=qs" in command_text:
            return completed(argv, 171, stderr="Error creating a MFX session: -9\n")
        return completed(argv, 0, stdout="ok\n")

    result = run_diagnostics(service="parking-spot-monitor", device="/dev/dri/renderD128", runner=runner, timeout_seconds=3)
    verdict = evaluate_results(result, require_qsv=False)

    assert verdict["accepted"] is True
    assert verdict["status"] == "vaapi_supported_qsv_unavailable"
    assert result["checks"]["vaapi_ffmpeg_init"]["passed"] is True
    assert result["checks"]["qsv_ffmpeg_init"]["passed"] is False
    assert "MFX session: -9" in result["checks"]["qsv_ffmpeg_init"]["stderr_tail"]
    assert [check["name"] for check in result["checks"].values()] == [
        "dri_listing",
        "ffmpeg_build_flags",
        "vainfo",
        "vaapi_ffmpeg_init",
        "qsv_ffmpeg_init",
    ]


def test_hardware_decode_diagnostics_fail_when_vaapi_is_unavailable() -> None:
    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        command_text = " ".join(argv)
        if "vainfo" in command_text or "vaapi=va" in command_text:
            return completed(argv, 1, stderr="Failed to initialise VAAPI connection\n")
        return completed(argv, 0, stdout="ok\n")

    result = run_diagnostics(service="parking-spot-monitor", device="/dev/dri/renderD128", runner=runner, timeout_seconds=3)
    verdict = evaluate_results(result, require_qsv=False)

    assert verdict["accepted"] is False
    assert verdict["status"] == "vaapi_unavailable"


def test_hardware_decode_diagnostics_can_require_qsv() -> None:
    result = {
        "checks": {
            "vaapi_ffmpeg_init": {"passed": True},
            "qsv_ffmpeg_init": {"passed": False},
        }
    }

    verdict = evaluate_results(result, require_qsv=True)

    assert verdict["accepted"] is False
    assert verdict["status"] == "qsv_required_but_unavailable"


def test_docker_command_runner_uses_exec_without_shell() -> None:
    runner = DockerCommandRunner(service="parking-spot-monitor")

    argv = runner.argv_for(["echo", "hello"])

    assert argv == ["docker", "compose", "exec", "-T", "parking-spot-monitor", "echo", "hello"]
