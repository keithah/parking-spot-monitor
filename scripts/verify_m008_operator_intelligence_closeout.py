#!/usr/bin/env python3
"""Finite M008 operator-intelligence closeout smoke runner.

The runner is intentionally local, bounded, and secret-free. It verifies the
operator-intelligence command/documentation surfaces in focused pytest buckets,
then runs the full regression suite. Commands are executed with argv lists rather
than shell snippets so future agents can inspect and extend the contract safely.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
STATUS_PATH = ROOT / "data" / "m008-closeout-status.json"
TIMEOUT_SECONDS = 180
FULL_REGRESSION_TIMEOUT_SECONDS = 300
OUTPUT_LIMIT = 4_000
PLACEHOLDER_RTSP_URL = "placeholder-rtsp-url-for-m008-closeout"
PLACEHOLDER_MATRIX_TOKEN = "placeholder-matrix-token-for-m008-closeout"

M008_CLOSEOUT_START = "M008_CLOSEOUT_START"
M008_CLOSEOUT_PASS = "M008_CLOSEOUT_PASS"
M008_CLOSEOUT_FAIL = "M008_CLOSEOUT_FAIL"
M008_CLOSEOUT_RESULT = "M008_CLOSEOUT_RESULT"

SENSITIVE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"rtsp://[^\s'\"]+", re.IGNORECASE),
    re.compile(r"(?i)bearer\s+[^\s'\"]+"),
    re.compile(r"(?i)(matrix[_-]?(?:access[_-]?)?token|access_token|authorization)([=:]\s*)([^\s'\"]+)"),
    re.compile(re.escape(PLACEHOLDER_RTSP_URL)),
    re.compile(re.escape(PLACEHOLDER_MATRIX_TOKEN)),
    re.compile(r"Traceback \(most recent call last\)", re.IGNORECASE),
    re.compile(r"BEGIN RAW IMAGE BYTES|END RAW IMAGE BYTES", re.IGNORECASE),
)

FORBIDDEN_OUTPUT_MARKERS = (
    PLACEHOLDER_RTSP_URL,
    PLACEHOLDER_MATRIX_TOKEN,
    "Traceback (most recent call last)",
    "BEGIN RAW IMAGE BYTES",
    "END RAW IMAGE BYTES",
)


@dataclass(frozen=True)
class SmokeCommand:
    label: str
    argv: tuple[str, ...]
    timeout_seconds: int = TIMEOUT_SECONDS


def _build_commands() -> tuple[SmokeCommand, ...]:
    return (
        SmokeCommand(
            label="pytest-matrix-commands",
            argv=(
                sys.executable,
                "-m",
                "pytest",
                "tests/test_matrix.py",
                "tests/test_matrix_operator_cockpit.py",
                "tests/test_operator_cockpit.py",
                "tests/test_operator_docs.py",
                "-q",
            ),
        ),
        SmokeCommand(
            label="pytest-decision-memory-feedback",
            argv=(
                sys.executable,
                "-m",
                "pytest",
                "tests/test_operator_decision_memory.py",
                "tests/test_operator_feedback.py",
                "-q",
            ),
        ),
        SmokeCommand(
            label="pytest-occupancy-analytics-history",
            argv=(
                sys.executable,
                "-m",
                "pytest",
                "tests/test_occupancy_analytics.py",
                "tests/test_vehicle_history.py",
                "tests/test_vehicle_history_cli.py",
                "tests/test_owner_vehicles.py",
                "tests/test_vehicle_estimates.py",
                "tests/test_vehicle_profiles.py",
                "-q",
            ),
        ),
        SmokeCommand(
            label="pytest-full-regression",
            argv=(sys.executable, "-m", "pytest", "-q"),
            timeout_seconds=FULL_REGRESSION_TIMEOUT_SECONDS,
        ),
    )


def _smoke_env(base: Mapping[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ if base is None else base)
    env["RTSP_URL"] = PLACEHOLDER_RTSP_URL
    env["MATRIX_ACCESS_TOKEN"] = PLACEHOLDER_MATRIX_TOKEN
    project_src = str(ROOT / "src")
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = project_src if not existing_pythonpath else f"{project_src}{os.pathsep}{existing_pythonpath}"
    return env


def _redact(text: str) -> str:
    redacted = text
    for pattern in SENSITIVE_PATTERNS:
        if pattern.groups >= 3:
            redacted = pattern.sub(lambda match: f"{match.group(1)}{match.group(2)}<redacted>", redacted)
        else:
            redacted = pattern.sub("<redacted>", redacted)
    return redacted


def _bounded(text: str, *, limit: int = OUTPUT_LIMIT) -> str:
    if len(text) <= limit:
        return text
    marker = f"... <{len(text) - limit} chars omitted> ...\n"
    tail_limit = max(0, limit - len(marker))
    return f"{marker}{text[-tail_limit:]}"


def _safe_output(stdout: str | bytes | None, stderr: str | bytes | None) -> str:
    def decode(value: str | bytes | None) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value

    rendered = ""
    out = decode(stdout)
    err = decode(stderr)
    if out:
        rendered += f"stdout:\n{out}"
    if err:
        rendered += f"\nstderr:\n{err}"
    return _bounded(_redact(rendered.strip()))


def _assert_no_forbidden_markers(rendered: str) -> None:
    for marker in FORBIDDEN_OUTPUT_MARKERS:
        if marker in rendered:
            raise RuntimeError(f"redaction failure for marker: {marker}")


def _run_command(command: SmokeCommand, *, env: Mapping[str, str]) -> int:
    print(f"{M008_CLOSEOUT_START} {command.label}", flush=True)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            list(command.argv),
            cwd=ROOT,
            env=dict(env),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=command.timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        rendered = _safe_output(exc.stdout, exc.stderr)
        _assert_no_forbidden_markers(rendered)
        print(f"{M008_CLOSEOUT_FAIL} {command.label} timeout_seconds={command.timeout_seconds} duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 124
    except FileNotFoundError as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        rendered = _safe_output("", str(exc))
        _assert_no_forbidden_markers(rendered)
        print(f"{M008_CLOSEOUT_FAIL} {command.label} exit_code=127 duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 127

    elapsed_ms = int((time.monotonic() - started) * 1000)
    rendered = _safe_output(completed.stdout, completed.stderr)
    _assert_no_forbidden_markers(rendered)
    if completed.returncode == 0:
        print(f"{M008_CLOSEOUT_PASS} {command.label} duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 0

    print(f"{M008_CLOSEOUT_FAIL} {command.label} exit_code={completed.returncode} duration_ms={elapsed_ms}", flush=True)
    if rendered:
        print(rendered, flush=True)
    return completed.returncode


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _write_status(payload: Mapping[str, Any]) -> None:
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(f"{json.dumps(payload, indent=2, sort_keys=True)}\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    if argv:
        print("usage: verify_m008_operator_intelligence_closeout.py", file=sys.stderr)
        return 2

    env = _smoke_env()
    command_results: list[dict[str, Any]] = []
    for command in _build_commands():
        started = time.monotonic()
        exit_code = _run_command(command, env=env)
        duration_ms = int((time.monotonic() - started) * 1000)
        command_results.append(
            {
                "label": command.label,
                "exit_code": exit_code,
                "duration_ms": duration_ms,
                "passed": exit_code == 0,
            }
        )
        if exit_code != 0:
            _write_status(
                {
                    "milestone": "M008",
                    "result": "failed",
                    "failed_label": command.label,
                    "exit_code": exit_code,
                    "commands": command_results,
                    "live_matrix_proof": "deferred; closeout used local contract checks with placeholder environment values",
                    "updated_at": _utc_now(),
                }
            )
            print(f"{M008_CLOSEOUT_RESULT} failed label={command.label} exit_code={exit_code}", flush=True)
            return exit_code
    _write_status(
        {
            "milestone": "M008",
            "result": "passed",
            "exit_code": 0,
            "commands": command_results,
            "live_matrix_proof": "deferred; closeout used local contract checks with placeholder environment values",
            "updated_at": _utc_now(),
        }
    )
    print(f"{M008_CLOSEOUT_RESULT} passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
