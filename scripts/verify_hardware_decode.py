#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

DEFAULT_SERVICE = "parking-spot-monitor"
DEFAULT_DEVICE = "/dev/dri/renderD128"
DEFAULT_TIMEOUT_SECONDS = 30.0
STDERR_TAIL_CHARS = 1200
STDOUT_TAIL_CHARS = 1200

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class DockerCommandRunner:
    service: str = DEFAULT_SERVICE

    def argv_for(self, command: Sequence[str]) -> list[str]:
        return ["docker", "compose", "exec", "-T", self.service, *command]

    def __call__(self, command: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        return subprocess.run(self.argv_for(command), capture_output=True, text=True, check=False, timeout=timeout)


def _tail(text: str | None, limit: int) -> str:
    value = text or ""
    return value[-limit:]


def _run_check(
    name: str,
    command: Sequence[str],
    *,
    runner: Callable[[Sequence[str]], subprocess.CompletedProcess[str]],
    timeout_seconds: float,
) -> dict[str, Any]:
    try:
        completed = runner(command, timeout=timeout_seconds)  # type: ignore[misc]
    except subprocess.TimeoutExpired as exc:
        return {
            "name": name,
            "passed": False,
            "returncode": None,
            "timed_out": True,
            "stdout_tail": _tail(getattr(exc, "stdout", ""), STDOUT_TAIL_CHARS),
            "stderr_tail": _tail(getattr(exc, "stderr", ""), STDERR_TAIL_CHARS),
            "command": list(command),
        }
    return {
        "name": name,
        "passed": completed.returncode == 0,
        "returncode": completed.returncode,
        "timed_out": False,
        "stdout_tail": _tail(completed.stdout, STDOUT_TAIL_CHARS),
        "stderr_tail": _tail(completed.stderr, STDERR_TAIL_CHARS),
        "command": list(command),
    }


def run_diagnostics(
    *,
    service: str = DEFAULT_SERVICE,
    device: str = DEFAULT_DEVICE,
    runner: Callable[[Sequence[str]], subprocess.CompletedProcess[str]] | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    command_runner = DockerCommandRunner(service=service) if runner is None else runner

    checks = {
        "dri_listing": _run_check("dri_listing", ["sh", "-lc", "ls -l /dev/dri"], runner=command_runner, timeout_seconds=timeout_seconds),
        "ffmpeg_build_flags": _run_check(
            "ffmpeg_build_flags",
            ["sh", "-lc", "ffmpeg -hide_banner -buildconf 2>&1 | grep -E 'enable-(libvpl|libmfx)|disable-(libvpl|libmfx)' || true"],
            runner=command_runner,
            timeout_seconds=timeout_seconds,
        ),
        "vainfo": _run_check(
            "vainfo",
            ["vainfo", "--display", "drm", "--device", device],
            runner=command_runner,
            timeout_seconds=timeout_seconds,
        ),
        "vaapi_ffmpeg_init": _run_check(
            "vaapi_ffmpeg_init",
            ["ffmpeg", "-hide_banner", "-init_hw_device", f"vaapi=va:{device}", "-f", "lavfi", "-i", "nullsrc=s=16x16", "-frames:v", "1", "-f", "null", "-"],
            runner=command_runner,
            timeout_seconds=timeout_seconds,
        ),
        "qsv_ffmpeg_init": _run_check(
            "qsv_ffmpeg_init",
            ["ffmpeg", "-hide_banner", "-init_hw_device", f"qsv=qs:{device}", "-f", "lavfi", "-i", "nullsrc=s=16x16", "-frames:v", "1", "-f", "null", "-"],
            runner=command_runner,
            timeout_seconds=timeout_seconds,
        ),
    }
    return {"service": service, "device": device, "checks": checks}


def evaluate_results(result: dict[str, Any], *, require_qsv: bool = False) -> dict[str, Any]:
    checks = result.get("checks", {})
    vaapi_ok = bool(checks.get("vaapi_ffmpeg_init", {}).get("passed")) and bool(checks.get("vainfo", {}).get("passed", True))
    qsv_ok = bool(checks.get("qsv_ffmpeg_init", {}).get("passed"))
    if not vaapi_ok:
        return {"accepted": False, "status": "vaapi_unavailable"}
    if require_qsv and not qsv_ok:
        return {"accepted": False, "status": "qsv_required_but_unavailable"}
    if qsv_ok:
        return {"accepted": True, "status": "vaapi_and_qsv_available"}
    return {"accepted": True, "status": "vaapi_supported_qsv_unavailable"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify container hardware decode initialization for VAAPI and QSV.")
    parser.add_argument("--service", default=DEFAULT_SERVICE, help="Docker Compose service/container to exec into.")
    parser.add_argument("--device", default=DEFAULT_DEVICE, help="DRM render node to test.")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Per-check timeout in seconds.")
    parser.add_argument("--require-qsv", action="store_true", help="Fail unless QSV also initializes. By default VAAPI success is accepted.")
    parser.add_argument("--json", action="store_true", help="Print full JSON diagnostics instead of a concise summary.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_diagnostics(service=args.service, device=args.device, timeout_seconds=args.timeout)
    verdict = evaluate_results(result, require_qsv=args.require_qsv)
    result["verdict"] = verdict
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"hardware_decode_status={verdict['status']}")
        for name, check in result["checks"].items():
            print(f"{name}: passed={check['passed']} returncode={check['returncode']}")
    return 0 if verdict["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
