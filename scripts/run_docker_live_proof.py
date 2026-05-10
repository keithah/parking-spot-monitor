#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml
from PIL import Image, UnidentifiedImageError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parking_spot_monitor.logging import redact_diagnostic_text

CONFIG_PATH = Path("config.yaml")
DATA_DIR = Path("data")
RESULT_PATH = "live-proof-result.json"
INPUT_PREFLIGHT_PATH = "live-proof-input-preflight.json"
STDOUT_LOG = "live-proof-docker.stdout.log"
STDERR_LOG = "live-proof-docker.stderr.log"

DOCKER_LIVE_PROOF_COMMAND = [
    "docker",
    "compose",
    "run",
    "--rm",
    "parking-spot-monitor",
    "python",
    "-m",
    "parking_spot_monitor",
    "--config",
    "/config/config.yaml",
    "--data-dir",
    "/data",
    "--live-proof-once",
]

REQUIRED_MARKERS = ["LIVE_RTSP_CAPTURE_OK", "LIVE_MATRIX_TEXT_OK", "LIVE_MATRIX_IMAGE_OK"]
FORBIDDEN_MARKERS = [
    "LIVE_PROOF_SKIPPED_CONFIG_ABSENT",
    "LIVE_PROOF_SKIPPED_RTSP_ENV_ABSENT",
    "LIVE_PROOF_SKIPPED_MATRIX_ENV_ABSENT",
    "LIVE_RTSP_CAPTURE_FAILED",
    "LIVE_MATRIX_TEXT_FAILED",
    "LIVE_MATRIX_IMAGE_FAILED",
]
LIVE_PROOF_TEXT_LABEL = "LIVE PROOF / TEST MESSAGE"
LIVE_PROOF_IMAGE_LABEL = "LIVE PROOF / TEST IMAGE"
DEFAULT_MATRIX_TOKEN_ENV = "MATRIX_ACCESS_TOKEN"
MATRIX_TOKEN_MISSING_INPUT = "MATRIX_TOKEN_ENV"

RunCallable = Callable[..., subprocess.CompletedProcess[str]]
ReadbackCallable = Callable[..., dict[str, Any]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Dockerized live RTSP + Matrix proof and write redacted evidence.")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Host config.yaml path mounted into Docker Compose.")
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="Host data directory for proof artifacts.")
    parser.add_argument("--docker-timeout-seconds", type=float, default=180, help="Maximum Docker proof runtime.")
    parser.add_argument("--readback-timeout-seconds", type=float, default=10, help="Maximum Matrix readback request time.")
    parser.add_argument("--readback-limit", type=int, default=20, help="Recent Matrix message count to inspect.")
    parser.add_argument("--skip-readback", action="store_true", help="Skip Matrix readback and record an explicit gap.")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    run: RunCallable = subprocess.run,
    readback: ReadbackCallable | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    source_environ = os.environ if environ is None else environ
    config_path = Path(args.config)
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    result_path = data_dir / RESULT_PATH

    preflight = _preflight_inputs(config_path=config_path, environ=source_environ)
    _write_input_preflight(data_dir / INPUT_PREFLIGHT_PATH, preflight)
    missing_inputs = preflight["missing_inputs"]
    matrix_token_env = str(preflight.get("matrix_token_env") or DEFAULT_MATRIX_TOKEN_ENV)
    if missing_inputs:
        _write_result(
            result_path,
            {
                "status": "preflight_failed",
                "phase": "preflight",
                "missing_inputs": missing_inputs,
                "docker_exit_code": None,
                "room_readback_status": "not_attempted",
                "redaction_scan": {"secret_occurrences": 0, "redaction_replacements": 0},
            },
        )
        return 2

    secrets = _known_secret_values(source_environ, matrix_token_env=matrix_token_env)
    started_at = _now_iso()
    try:
        completed = run(
            _docker_live_proof_command(matrix_token_env),
            check=False,
            capture_output=True,
            text=True,
            timeout=args.docker_timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = _coerce_text(exc.stdout)
        stderr = _coerce_text(exc.stderr)
        stdout_redacted, stdout_count = _redact_proof_text(stdout, secrets=secrets)
        stderr_redacted, stderr_count = _redact_proof_text(stderr, secrets=secrets)
        (data_dir / STDOUT_LOG).write_text(stdout_redacted, encoding="utf-8")
        (data_dir / STDERR_LOG).write_text(stderr_redacted, encoding="utf-8")
        result = _base_result(started_at=started_at, completed_at=_now_iso(), docker_exit_code=None, docker_command=_docker_live_proof_command(matrix_token_env))
        result.update(
            {
                "status": "docker_timeout",
                "phase": "docker",
                "timeout_seconds": args.docker_timeout_seconds,
                "redaction_scan": _redaction_scan(result_path.parent, secrets=secrets, replacement_count=stdout_count + stderr_count),
            }
        )
        _write_result(result_path, result)
        return 124

    docker_exit_code = int(completed.returncode)
    stdout_redacted, stdout_count = _redact_proof_text(completed.stdout or "", secrets=secrets)
    stderr_redacted, stderr_count = _redact_proof_text(completed.stderr or "", secrets=secrets)
    (data_dir / STDOUT_LOG).write_text(stdout_redacted, encoding="utf-8")
    (data_dir / STDERR_LOG).write_text(stderr_redacted, encoding="utf-8")

    result = _base_result(started_at=started_at, completed_at=_now_iso(), docker_exit_code=docker_exit_code, docker_command=_docker_live_proof_command(matrix_token_env))
    if docker_exit_code != 0:
        result.update(
            {
                "status": "docker_failed",
                "phase": "docker",
                "redaction_scan": _redaction_scan(data_dir, secrets=secrets, replacement_count=stdout_count + stderr_count),
            }
        )
        _write_result(result_path, result)
        return docker_exit_code

    combined_output = "\n".join([stdout_redacted, stderr_redacted])
    marker_checks = _check_markers(combined_output)
    artifact_checks = _check_artifacts(data_dir)
    config_summary = _read_matrix_config(config_path)
    room_readback = _check_room_readback(
        config_summary=config_summary,
        token=source_environ.get(matrix_token_env, ""),
        timeout_seconds=args.readback_timeout_seconds,
        limit=args.readback_limit,
        skip=args.skip_readback,
        readback=fetch_matrix_room_messages if readback is None else readback,
    )
    result.update(
        {
            "marker_checks": marker_checks,
            "artifact_checks": artifact_checks,
            "room_readback_status": room_readback["status"],
            "room_readback": room_readback,
        }
    )

    validation_ok = bool(marker_checks["required_present"]) and not marker_checks["forbidden_present"] and bool(
        artifact_checks["latest_jpeg"]["valid_jpeg"]
    ) and artifact_checks["snapshot_jpegs"]["valid_count"] >= 1
    if not validation_ok:
        result.update(
            {
                "status": "validation_failed",
                "phase": "validation",
                "redaction_scan": _redaction_scan(data_dir, secrets=secrets, replacement_count=stdout_count + stderr_count),
            }
        )
        _write_result(result_path, result)
        return 1

    if room_readback["status"] != "verified":
        result.update(
            {
                "status": "readback_gap",
                "phase": "matrix_readback",
                "redaction_scan": _redaction_scan(data_dir, secrets=secrets, replacement_count=stdout_count + stderr_count),
            }
        )
        _write_result(result_path, result)
        return 1

    result.update(
        {
            "status": "success",
            "phase": "complete",
            "redaction_scan": _redaction_scan(data_dir, secrets=secrets, replacement_count=stdout_count + stderr_count),
        }
    )
    _write_result(result_path, result)
    return 0


def _preflight_inputs(*, config_path: Path, environ: Mapping[str, str]) -> dict[str, Any]:
    if not config_path.is_file():
        return {
            "config_exists": False,
            "config_parse_ok": False,
            "config_path": str(config_path),
            "rtsp_env": "RTSP_URL",
            "rtsp_env_present": bool(environ.get("RTSP_URL")),
            "matrix_homeserver_present": False,
            "matrix_room_id_present": False,
            "matrix_token_env": DEFAULT_MATRIX_TOKEN_ENV,
            "matrix_token_env_present": bool(environ.get(DEFAULT_MATRIX_TOKEN_ENV)),
            "missing_inputs": [config_path.name, "RTSP_URL", MATRIX_TOKEN_MISSING_INPUT],
        }

    config = _load_config_for_preflight(config_path)
    missing: list[str] = []
    if config is None:
        return {
            "config_exists": True,
            "config_parse_ok": False,
            "config_path": str(config_path),
            "rtsp_env": "RTSP_URL",
            "rtsp_env_present": bool(environ.get("RTSP_URL")),
            "matrix_homeserver_present": False,
            "matrix_room_id_present": False,
            "matrix_token_env": DEFAULT_MATRIX_TOKEN_ENV,
            "matrix_token_env_present": bool(environ.get(DEFAULT_MATRIX_TOKEN_ENV)),
            "missing_inputs": [config_path.name, "RTSP_URL", MATRIX_TOKEN_MISSING_INPUT],
        }

    stream = config.get("stream") if isinstance(config, dict) else None
    matrix = config.get("matrix") if isinstance(config, dict) else None
    rtsp_env = _non_empty_mapping_string(stream, "rtsp_url_env") or "RTSP_URL"
    matrix_token_env = _non_empty_mapping_string(matrix, "access_token_env") or DEFAULT_MATRIX_TOKEN_ENV
    rtsp_env_configured = bool(_non_empty_mapping_string(stream, "rtsp_url_env"))
    matrix_token_env_configured = bool(_non_empty_mapping_string(matrix, "access_token_env"))
    rtsp_env_present = bool(environ.get(rtsp_env))
    matrix_token_env_present = bool(environ.get(matrix_token_env))
    matrix_homeserver_present = bool(_non_empty_mapping_string(matrix, "homeserver"))
    matrix_room_id_present = bool(_non_empty_mapping_string(matrix, "room_id"))

    if not rtsp_env_configured:
        missing.append("stream.rtsp_url_env")
    if not rtsp_env_present:
        missing.append(rtsp_env)
    if not matrix_homeserver_present:
        missing.append("matrix.homeserver")
    if not matrix_room_id_present:
        missing.append("matrix.room_id")
    if not matrix_token_env_configured:
        missing.append("matrix.access_token_env")
    elif not matrix_token_env_present:
        missing.append(MATRIX_TOKEN_MISSING_INPUT if matrix_token_env == DEFAULT_MATRIX_TOKEN_ENV else matrix_token_env)

    return {
        "config_exists": True,
        "config_parse_ok": True,
        "config_path": str(config_path),
        "rtsp_env": rtsp_env,
        "rtsp_env_configured": rtsp_env_configured,
        "rtsp_env_present": rtsp_env_present,
        "matrix_homeserver_present": matrix_homeserver_present,
        "matrix_room_id_present": matrix_room_id_present,
        "matrix_token_env": matrix_token_env,
        "matrix_token_env_configured": matrix_token_env_configured,
        "matrix_token_env_present": matrix_token_env_present,
        "missing_inputs": missing,
    }


def _write_input_preflight(path: Path, preflight: Mapping[str, Any]) -> None:
    missing_inputs = [str(item) for item in preflight.get("missing_inputs", []) if isinstance(item, str) and item]
    status = "ready" if not missing_inputs else "preflight_blocked"
    artifact = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "status": status,
        "config": {
            "path": str(preflight.get("config_path") or CONFIG_PATH),
            "exists": bool(preflight.get("config_exists")),
            "parse_ok": bool(preflight.get("config_parse_ok")),
        },
        "routing": {
            "matrix_homeserver_present": bool(preflight.get("matrix_homeserver_present")),
            "matrix_room_id_present": bool(preflight.get("matrix_room_id_present")),
        },
        "environment": {
            "rtsp_env_name": str(preflight.get("rtsp_env") or "RTSP_URL"),
            "rtsp_env_present": bool(preflight.get("rtsp_env_present")),
            "matrix_token_env_name": "Matrix token env key",
            "matrix_token_env_present": bool(preflight.get("matrix_token_env_present")),
        },
        "missing_inputs": missing_inputs,
        "notes": [
            "Names-only preflight summary; raw RTSP URLs, Matrix token values, auth headers, Matrix responses, and image bytes are intentionally omitted."
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_config_for_preflight(config_path: Path) -> dict[str, Any] | None:
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    return raw if isinstance(raw, dict) else None


def _non_empty_mapping_string(value: object, key: str) -> str | None:
    if not isinstance(value, dict):
        return None
    item = value.get(key)
    return item if isinstance(item, str) and item.strip() else None


def _docker_live_proof_command(matrix_token_env: str) -> list[str]:
    if matrix_token_env == DEFAULT_MATRIX_TOKEN_ENV:
        return list(DOCKER_LIVE_PROOF_COMMAND)
    return [
        "docker",
        "compose",
        "run",
        "--rm",
        "-e",
        matrix_token_env,
        "parking-spot-monitor",
        "python",
        "-m",
        "parking_spot_monitor",
        "--config",
        "/config/config.yaml",
        "--data-dir",
        "/data",
        "--live-proof-once",
    ]


def _known_secret_values(environ: Mapping[str, str], *, matrix_token_env: str = DEFAULT_MATRIX_TOKEN_ENV) -> list[str]:
    secret_keys = {"RTSP_URL", DEFAULT_MATRIX_TOKEN_ENV, matrix_token_env}
    return [value for key, value in environ.items() if key in secret_keys and value]


def _base_result(*, started_at: str, completed_at: str, docker_exit_code: int | None, docker_command: Sequence[str] | None = None) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "started_at": started_at,
        "completed_at": completed_at,
        "docker_command": list(docker_command or DOCKER_LIVE_PROOF_COMMAND),
        "docker_exit_code": docker_exit_code,
        "marker_checks": None,
        "artifact_checks": None,
        "room_readback_status": "not_attempted",
    }


def _check_markers(output: str) -> dict[str, Any]:
    missing_required = [marker for marker in REQUIRED_MARKERS if marker not in output]
    forbidden_present = [marker for marker in FORBIDDEN_MARKERS if marker in output]
    return {
        "required": list(REQUIRED_MARKERS),
        "missing_required": missing_required,
        "required_present": not missing_required,
        "forbidden": list(FORBIDDEN_MARKERS),
        "forbidden_present": forbidden_present,
    }


def _check_artifacts(data_dir: Path) -> dict[str, Any]:
    latest_path = data_dir / "latest.jpg"
    snapshots = sorted((data_dir / "snapshots").glob("live-proof-*.jpg"))
    snapshot_results = [_jpeg_check(path) for path in snapshots]
    return {
        "latest_jpeg": _jpeg_check(latest_path),
        "snapshot_jpegs": {
            "count": len(snapshot_results),
            "valid_count": sum(1 for item in snapshot_results if item["valid_jpeg"]),
            "files": snapshot_results,
        },
    }


def _jpeg_check(path: Path) -> dict[str, Any]:
    exists = path.is_file()
    byte_size = path.stat().st_size if exists else 0
    check: dict[str, Any] = {"path": str(path), "exists": exists, "byte_size": byte_size, "valid_jpeg": False}
    if not exists:
        check["error_type"] = "missing"
        return check
    try:
        with Image.open(path) as image:
            check["format"] = image.format
            check["width"], check["height"] = image.size
            image.verify()
        check["valid_jpeg"] = check.get("format") == "JPEG"
    except (OSError, UnidentifiedImageError) as exc:
        check["error_type"] = type(exc).__name__
    return check


def _read_matrix_config(config_path: Path) -> dict[str, str | float | None]:
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {"homeserver": None, "room_id": None, "timeout_seconds": None}
    matrix = raw.get("matrix") if isinstance(raw, dict) else None
    if not isinstance(matrix, dict):
        return {"homeserver": None, "room_id": None, "timeout_seconds": None}
    homeserver = matrix.get("homeserver")
    room_id = matrix.get("room_id")
    timeout = matrix.get("timeout_seconds")
    return {
        "homeserver": homeserver if isinstance(homeserver, str) and homeserver else None,
        "room_id": room_id if isinstance(room_id, str) and room_id else None,
        "timeout_seconds": float(timeout) if isinstance(timeout, (int, float)) else None,
    }


def _check_room_readback(
    *,
    config_summary: Mapping[str, str | float | None],
    token: str,
    timeout_seconds: float,
    limit: int,
    skip: bool,
    readback: ReadbackCallable,
) -> dict[str, Any]:
    if skip:
        return {"status": "skipped", "reason": "operator_skip", "text_found": False, "image_found": False}
    homeserver = config_summary.get("homeserver")
    room_id = config_summary.get("room_id")
    if not isinstance(homeserver, str) or not isinstance(room_id, str) or not token:
        return {"status": "gap", "reason": "missing_matrix_readback_inputs", "text_found": False, "image_found": False}
    effective_timeout = float(config_summary.get("timeout_seconds") or timeout_seconds)
    try:
        payload = readback(homeserver=homeserver, room_id=room_id, access_token=token, timeout_seconds=effective_timeout, limit=limit)
    except Exception as exc:
        return {"status": "gap", "reason": type(exc).__name__, "text_found": False, "image_found": False}
    if not isinstance(payload, dict) or not isinstance(payload.get("chunk"), list):
        return {"status": "gap", "reason": "malformed_response", "text_found": False, "image_found": False}
    text_found = False
    image_found = False
    inspected_count = 0
    for event in payload["chunk"]:
        if not isinstance(event, dict):
            continue
        content = event.get("content")
        if not isinstance(content, dict):
            continue
        body = content.get("body")
        msgtype = content.get("msgtype")
        if isinstance(body, str) and isinstance(msgtype, str):
            inspected_count += 1
            text_found = text_found or (msgtype == "m.text" and LIVE_PROOF_TEXT_LABEL in body)
            image_found = image_found or (msgtype == "m.image" and LIVE_PROOF_IMAGE_LABEL in body)
    status = "verified" if text_found and image_found else "gap"
    reason = None if status == "verified" else "live_proof_messages_not_found"
    return {"status": status, "reason": reason, "text_found": text_found, "image_found": image_found, "inspected_count": inspected_count}


def fetch_matrix_room_messages(*, homeserver: str, room_id: str, access_token: str, timeout_seconds: float, limit: int = 20) -> dict[str, Any]:
    room_segment = urllib.parse.quote(room_id, safe="")
    query = urllib.parse.urlencode({"dir": "b", "limit": max(1, int(limit))})
    url = f"{homeserver.rstrip('/')}/_matrix/client/v3/rooms/{room_segment}/messages?{query}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - operator-provided homeserver
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"matrix_readback_http_{exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("matrix_readback_unavailable") from exc
    except json.JSONDecodeError as exc:
        raise ValueError("matrix_readback_malformed_json") from exc


def _redact_proof_text(text: str, *, secrets: Sequence[str]) -> tuple[str, int]:
    replacements = 0
    safe_lines: list[str] = []
    for line in text.splitlines(keepends=True):
        if "authorization" in line.lower():
            safe_lines.append("[redacted-header]\n" if line.endswith("\n") else "[redacted-header]")
            replacements += 1
            continue
        if "traceback" in line.lower():
            safe_lines.append("[redacted-trace]\n" if line.endswith("\n") else "[redacted-trace]")
            replacements += 1
            continue
        before = line
        redacted = redact_diagnostic_text(line)
        if redacted != before.strip():
            replacements += 1
            redacted = redacted + ("\n" if line.endswith("\n") else "")
        for secret in secrets:
            if secret and secret in redacted:
                redacted = redacted.replace(secret, "<redacted>")
                replacements += 1
        safe_lines.append(redacted)
    return "".join(safe_lines), replacements


def _redaction_scan(data_dir: Path, *, secrets: Sequence[str], replacement_count: int) -> dict[str, int]:
    paths = [data_dir / RESULT_PATH, data_dir / STDOUT_LOG, data_dir / STDERR_LOG]
    secret_occurrences = 0
    for path in paths:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for secret in secrets:
            if secret:
                secret_occurrences += text.count(secret)
        secret_occurrences += text.lower().count("authorization: bearer")
    return {"secret_occurrences": secret_occurrences, "redaction_replacements": replacement_count}


def _write_result(path: Path, result: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_result_contract(result)
    path.write_text(json.dumps(normalized, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _normalize_result_contract(result: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(result)
    normalized.setdefault("marker_checks", None)
    normalized.setdefault("artifact_checks", None)
    normalized.setdefault("room_readback", None)
    normalized.setdefault("redaction_scan", {"secret_occurrences": 0, "redaction_replacements": 0})
    normalized["markers"] = normalized["marker_checks"]
    normalized["artifacts"] = normalized["artifact_checks"]
    normalized["matrix_room_readback"] = normalized["room_readback"]
    normalized["redaction"] = normalized["redaction_scan"]
    return normalized


def _coerce_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
