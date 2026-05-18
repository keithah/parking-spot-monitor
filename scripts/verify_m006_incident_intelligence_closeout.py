#!/usr/bin/env python3
"""Finite M006 incident-intelligence closeout smoke runner."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
TIMEOUT_SECONDS = 180
DOCKER_TIMEOUT_SECONDS = 300
OUTPUT_LIMIT = 4_000
JSON_PREVIEW_LIMIT = 600
DOCKER_IMAGE_TAG = "parking-spot-monitor:m006-closeout-smoke"
PLACEHOLDER_RTSP_URL = "placeholder-rtsp-url-for-m006-closeout"
PLACEHOLDER_MATRIX_TOKEN = "placeholder-matrix-token-for-m006-closeout"

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


def _build_commands(temp_data_dir: Path) -> tuple[SmokeCommand, ...]:
    config_path = ROOT / "config.yaml.example"
    return (
        SmokeCommand(
            label="pytest-m006-contracts",
            argv=(
                sys.executable,
                "-m",
                "pytest",
                "tests/test_operator_docs.py",
                "tests/test_matrix_operator_cockpit.py",
                "tests/test_operator_feedback.py",
                "tests/test_matrix.py",
                "tests/test_timeline_buffer.py",
                "-q",
            ),
        ),
        SmokeCommand(
            label="pytest-runtime-config-state",
            argv=(
                sys.executable,
                "-m",
                "pytest",
                "tests/test_startup.py",
                "tests/test_docker_contract.py",
                "tests/test_config.py",
                "tests/test_state.py",
                "tests/test_health.py",
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
            label="docker-build",
            argv=("docker", "build", "-t", DOCKER_IMAGE_TAG, "."),
            timeout_seconds=DOCKER_TIMEOUT_SECONDS,
        ),
        SmokeCommand(
            label="docker-run-validate-config",
            argv=(
                "docker",
                "run",
                "--rm",
                "-e",
                "RTSP_URL",
                "-e",
                "MATRIX_ACCESS_TOKEN",
                "-v",
                f"{config_path}:/config/config.yaml:ro",
                "-v",
                f"{temp_data_dir}:/data",
                DOCKER_IMAGE_TAG,
                "python",
                "-m",
                "parking_spot_monitor",
                "--config",
                "/config/config.yaml",
                "--data-dir",
                "/data",
                "--validate-config",
            ),
            timeout_seconds=60,
        ),
        SmokeCommand(
            label="docker-compose-config",
            argv=("docker", "compose", "config", "--quiet"),
            timeout_seconds=30,
        ),
    )


def _smoke_env(base: Mapping[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ if base is None else base)
    env["RTSP_URL"] = PLACEHOLDER_RTSP_URL
    env["MATRIX_ACCESS_TOKEN"] = PLACEHOLDER_MATRIX_TOKEN
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
    print(f"M006_CLOSEOUT_START {command.label}", flush=True)
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
        print(f"M006_CLOSEOUT_FAIL {command.label} timeout_seconds={command.timeout_seconds} duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 124
    except FileNotFoundError as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        rendered = _safe_output("", str(exc))
        _assert_no_forbidden_markers(rendered)
        print(f"M006_CLOSEOUT_FAIL {command.label} exit_code=127 duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 127

    elapsed_ms = int((time.monotonic() - started) * 1000)
    rendered = _safe_output(completed.stdout, completed.stderr)
    _assert_no_forbidden_markers(rendered)
    if completed.returncode == 0:
        print(f"M006_CLOSEOUT_PASS {command.label} duration_ms={elapsed_ms}", flush=True)
        if rendered:
            print(rendered, flush=True)
        return 0

    print(f"M006_CLOSEOUT_FAIL {command.label} exit_code={completed.returncode} duration_ms={elapsed_ms}", flush=True)
    if rendered:
        print(rendered, flush=True)
    return completed.returncode


def _json_status(path: Path, label: str) -> None:
    if not path.exists():
        print(f"M006_CLOSEOUT_DATA {label} status=missing", flush=True)
        return
    if not path.is_file():
        print(f"M006_CLOSEOUT_DATA {label} status=unavailable reason=not-file", flush=True)
        return
    try:
        raw = path.read_text(encoding="utf-8")
        parsed = json.loads(raw)
    except Exception as exc:
        print(f"M006_CLOSEOUT_DATA {label} status=malformed error_type={type(exc).__name__}", flush=True)
        return
    preview = _bounded(_redact(json.dumps(parsed, sort_keys=True, default=str)), limit=JSON_PREVIEW_LIMIT)
    _assert_no_forbidden_markers(preview)
    print(f"M006_CLOSEOUT_DATA {label} status=present bytes={path.stat().st_size} preview={preview}", flush=True)


def _inspect_local_data(data_dir: Path = ROOT / "data") -> int:
    if not data_dir.exists():
        print("M006_CLOSEOUT_DATA data_dir status=safe-empty reason=missing", flush=True)
        return 0
    if not data_dir.is_dir():
        print("M006_CLOSEOUT_DATA data_dir status=unavailable reason=not-directory", flush=True)
        return 0

    frames_dir = data_dir / "timeline" / "frames"
    if not frames_dir.exists():
        print("M006_CLOSEOUT_DATA timeline_frames status=safe-empty count=0 reason=missing", flush=True)
    elif not frames_dir.is_dir():
        print("M006_CLOSEOUT_DATA timeline_frames status=unavailable reason=not-directory", flush=True)
    else:
        count = sum(1 for path in frames_dir.iterdir() if path.is_file())
        status = "present" if count else "safe-empty"
        print(f"M006_CLOSEOUT_DATA timeline_frames status={status} count={count}", flush=True)

    _json_status(data_dir / "health.json", "health")
    _json_status(data_dir / "state.json", "state")
    _json_status(data_dir / "operator-feedback-labels.json", "operator_feedback_labels")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    if argv:
        print("usage: verify_m006_incident_intelligence_closeout.py", file=sys.stderr)
        return 2

    env = _smoke_env()
    _inspect_local_data(ROOT / "data")
    with tempfile.TemporaryDirectory(prefix="m006-closeout-data-") as temp_dir:
        for command in _build_commands(Path(temp_dir)):
            exit_code = _run_command(command, env=env)
            if exit_code != 0:
                print(f"M006_CLOSEOUT_RESULT failed label={command.label} exit_code={exit_code}", flush=True)
                return exit_code
    print("M006_CLOSEOUT_RESULT passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
