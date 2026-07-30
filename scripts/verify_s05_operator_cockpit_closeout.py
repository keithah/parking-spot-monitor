#!/usr/bin/env python3
"""Finite S05 closeout smoke for operator cockpit runtime/Docker contracts.

This script intentionally exercises only bounded local verification surfaces:
pytest regressions, validate-config through the real package entrypoint, and
Docker Compose config rendering. It must not touch a live camera, construct
Matrix delivery, load detector models, mutate occupancy state, or dump raw
subprocess output without redaction.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from scripts.closeout_helpers import assert_no_forbidden_markers, bounded_text, redact_text, safe_output, smoke_env
from scripts.test_suite_paths import CONFIG_TEST_MODULES, MATRIX_COCKPIT_TEST_MODULES, MATRIX_TEST_MODULES, STARTUP_TEST_MODULES

ROOT = Path(__file__).resolve().parents[1]
TIMEOUT_SECONDS = 120
OUTPUT_LIMIT = 4_000
PLACEHOLDER_RTSP_URL = "placeholder-rtsp-url-for-s05-closeout"
PLACEHOLDER_MATRIX_TOKEN = "placeholder-matrix-token-for-s05-closeout"

SENSITIVE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"rtsp://[^\s'\"]+", re.IGNORECASE),
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


COMMANDS: tuple[SmokeCommand, ...] = (
    SmokeCommand(
        label="pytest-docs-matrix",
        argv=(sys.executable, "-m", "pytest", "tests/test_operator_docs.py", *MATRIX_TEST_MODULES, "-q"),
    ),
    SmokeCommand(
        label="pytest-cockpit-lab-memory",
        argv=(
            sys.executable,
            "-m",
            "pytest",
            *MATRIX_TEST_MODULES,
            *MATRIX_COCKPIT_TEST_MODULES,
            "tests/test_detection_lab.py",
            "tests/test_operator_decision_memory.py",
            "-q",
        ),
    ),
    SmokeCommand(
        label="pytest-runtime-docker-config-state",
        argv=(
            sys.executable,
            "-m",
            "pytest",
            *STARTUP_TEST_MODULES,
            "tests/test_docker_contract.py",
            *CONFIG_TEST_MODULES,
            "tests/test_health.py",
            "tests/test_state.py",
            "-q",
        ),
    ),
    SmokeCommand(
        label="validate-config-entrypoint",
        argv=(
            sys.executable,
            "-m",
            "parking_spot_monitor",
            "--config",
            "config.yaml.example",
            "--validate-config",
        ),
        timeout_seconds=30,
    ),
    SmokeCommand(
        label="docker-compose-config",
        argv=("docker", "compose", "config", "--quiet"),
        timeout_seconds=30,
    ),
)


def _smoke_env(base: Mapping[str, str] | None = None) -> dict[str, str]:
    return smoke_env(rtsp_placeholder=PLACEHOLDER_RTSP_URL, matrix_token_placeholder=PLACEHOLDER_MATRIX_TOKEN, base=base)


def _redact(text: str) -> str:
    return redact_text(text, SENSITIVE_PATTERNS)


def _bounded(text: str, *, limit: int = OUTPUT_LIMIT) -> str:
    return bounded_text(text, limit=limit)


def _safe_output(stdout: str, stderr: str) -> str:
    return safe_output(stdout, stderr, patterns=SENSITIVE_PATTERNS, limit=OUTPUT_LIMIT)


def _assert_no_forbidden_markers(rendered: str) -> None:
    assert_no_forbidden_markers(rendered, FORBIDDEN_OUTPUT_MARKERS)


def _run_command(command: SmokeCommand, *, env: Mapping[str, str]) -> int:
    print(f"S05_CLOSEOUT_START {command.label}", flush=True)
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
        rendered = _safe_output(exc.stdout or "", exc.stderr or "")
        _assert_no_forbidden_markers(rendered)
        print(f"S05_CLOSEOUT_FAIL {command.label} timeout_seconds={command.timeout_seconds} duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 124
    except FileNotFoundError as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        rendered = _safe_output("", str(exc))
        _assert_no_forbidden_markers(rendered)
        print(f"S05_CLOSEOUT_FAIL {command.label} exit_code=127 duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 127

    elapsed_ms = int((time.monotonic() - started) * 1000)
    rendered = _safe_output(completed.stdout, completed.stderr)
    _assert_no_forbidden_markers(rendered)
    if completed.returncode == 0:
        print(f"S05_CLOSEOUT_PASS {command.label} duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 0

    print(f"S05_CLOSEOUT_FAIL {command.label} exit_code={completed.returncode} duration_ms={elapsed_ms}", flush=True)
    if rendered:
        print(rendered, flush=True)
    return completed.returncode


def main(argv: Sequence[str] | None = None) -> int:
    if argv:
        print("usage: verify_s05_operator_cockpit_closeout.py", file=sys.stderr)
        return 2

    env = _smoke_env()
    for command in COMMANDS:
        exit_code = _run_command(command, env=env)
        if exit_code != 0:
            print(f"S05_CLOSEOUT_RESULT failed label={command.label} exit_code={exit_code}", flush=True)
            return exit_code
    print("S05_CLOSEOUT_RESULT passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
