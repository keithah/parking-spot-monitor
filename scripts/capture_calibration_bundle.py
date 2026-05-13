#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from PIL import Image

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parking_spot_monitor.logging import redact_diagnostic_text

CONFIG_PATH = Path("config.yaml")
DATA_DIR = Path("data")
BUNDLE_ROOT = DATA_DIR / "calibration-bundles"
PREFLIGHT_PATH = "calibration-input-preflight.json"
STDOUT_LOG = "docker.stdout.log"
STDERR_LOG = "docker.stderr.log"
DEFAULT_RTSP_ENV = "RTSP_URL"
DEFAULT_MATRIX_TOKEN_ENV = "MATRIX_ACCESS_TOKEN"
MATRIX_TOKEN_MISSING_INPUT = "MATRIX_TOKEN_ENV"
SCHEMA_VERSION = 1
KNOWN_EVENTS = {
    "capture-frame-written",
    "debug-overlay-written",
    "detection-frame-processed",
    "detection-frame-failed",
    "capture-failed",
    "debug-overlay-failed",
    "capture-once-complete",
}
TEXT_SUFFIXES = {".json", ".md", ".log", ".txt"}

RunCallable = Callable[..., subprocess.CompletedProcess[str]]
NowCallable = Callable[[], str]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture a redacted Dockerized calibration evidence bundle.")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Host config.yaml path mounted into Docker Compose.")
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="Host runtime data directory containing latest capture artifacts.")
    parser.add_argument(
        "--bundle-root",
        default=None,
        help="Directory where timestamped calibration bundles are written (default: <data-dir>/calibration-bundles).",
    )
    parser.add_argument("--docker-timeout-seconds", type=float, default=180, help="Maximum Docker capture runtime.")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    run: RunCallable = subprocess.run,
    now: NowCallable | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    source_environ = os.environ if environ is None else environ
    now_fn = _now_iso if now is None else now
    config_path = Path(args.config)
    data_dir = Path(args.data_dir)
    bundle_root = Path(args.bundle_root) if args.bundle_root else data_dir / "calibration-bundles"
    data_dir.mkdir(parents=True, exist_ok=True)
    bundle_root.mkdir(parents=True, exist_ok=True)

    preflight = _preflight_inputs(config_path=config_path, environ=source_environ)
    _write_preflight(data_dir / PREFLIGHT_PATH, preflight, generated_at=now_fn())
    if preflight["missing_inputs"]:
        return 2

    rtsp_env = str(preflight["environment"]["rtsp_env_name"])
    matrix_token_env = str(preflight["matrix_token_env"] or DEFAULT_MATRIX_TOKEN_ENV)
    secrets = _known_secret_values(source_environ, rtsp_env=rtsp_env, matrix_token_env=matrix_token_env)
    started_at = now_fn()
    bundle_dir = _unique_bundle_dir(bundle_root, _timestamp_for_path(started_at))
    bundle_dir.mkdir(parents=True, exist_ok=False)
    context_dir = bundle_dir / "context"
    context_dir.mkdir(parents=True, exist_ok=True)

    command = _docker_capture_command(rtsp_env=rtsp_env, matrix_token_env=matrix_token_env)
    stdout_text = ""
    stderr_text = ""
    docker_exit_code: int | None = None
    timeout_seconds = args.docker_timeout_seconds
    status = "success"
    phase = "complete"
    exit_code = 0

    try:
        completed = run(command, check=False, capture_output=True, text=True, timeout=timeout_seconds)
        docker_exit_code = int(completed.returncode)
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        if docker_exit_code != 0:
            status = "docker_failed"
            phase = "docker"
            exit_code = docker_exit_code
    except subprocess.TimeoutExpired as exc:
        stdout_text = _coerce_text(getattr(exc, "stdout", None) or getattr(exc, "output", None))
        stderr_text = _coerce_text(getattr(exc, "stderr", None))
        status = "docker_timeout"
        phase = "docker"
        exit_code = 124

    stdout_redacted, stdout_replacements = _redact_output(stdout_text, secrets=secrets)
    stderr_redacted, stderr_replacements = _redact_output(stderr_text, secrets=secrets)
    (bundle_dir / STDOUT_LOG).write_text(stdout_redacted, encoding="utf-8")
    (bundle_dir / STDERR_LOG).write_text(stderr_redacted, encoding="utf-8")

    events = _parse_events("\n".join([stdout_redacted, stderr_redacted]))
    artifact_checks = {
        "raw_frame": _copy_and_validate_jpeg(data_dir / "latest.jpg", bundle_dir / "latest.jpg"),
        "debug_overlay": _copy_and_validate_jpeg(data_dir / "debug_latest.jpg", bundle_dir / "debug_latest.jpg"),
    }
    context = _copy_context_files(data_dir=data_dir, context_dir=context_dir)
    completed_at = now_fn()

    detection_summary = _last_event(events, "detection-frame-processed")
    detection_failure = _last_event(events, "detection-frame-failed")
    capture_event = _last_event(events, "capture-frame-written")
    capture_complete = _last_event(events, "capture-once-complete")
    capture_failure = _last_event(events, "capture-failed")
    overlay_event = _last_event(events, "debug-overlay-written")
    overlay_failure = _last_event(events, "debug-overlay-failed")
    capture_summary = _capture_summary(capture_event=capture_event, capture_complete=capture_complete)

    validation_errors = _validation_errors(
        docker_exit_code=docker_exit_code,
        status=status,
        artifacts=artifact_checks,
        detection_summary=detection_summary,
        detection_failure=detection_failure,
    )
    if status == "success" and detection_failure is not None and detection_summary is None:
        status = "partial_bundle"
        phase = "detection"
        exit_code = 1
    elif status == "success" and validation_errors:
        status = "validation_failed"
        phase = "validation"
        exit_code = 1

    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "bundle_dir": str(bundle_dir),
        "started_at": started_at,
        "completed_at": completed_at,
        "status": status,
        "phase": phase,
        "docker_exit_code": docker_exit_code,
        "timeout_seconds": timeout_seconds,
        "docker_command": _safe_docker_command(command, matrix_token_env=matrix_token_env),
        "preflight": preflight,
        "events": _event_summary(events),
        "capture": capture_summary,
        "capture_failure": capture_failure,
        "debug_overlay": overlay_event,
        "debug_overlay_failure": overlay_failure,
        "detection_summary": detection_summary,
        "detection_failure": detection_failure,
        "artifacts": artifact_checks,
        "context": context,
        "validation_errors": validation_errors,
        "redaction_scan": {"secret_occurrences": 0, "redaction_replacements": stdout_replacements + stderr_replacements},
    }
    _write_report(bundle_dir / "calibration-report.md", manifest)
    _write_manifest(bundle_dir / "manifest.json", manifest)

    redaction_scan = _redaction_scan(bundle_dir, secrets=secrets, replacement_count=stdout_replacements + stderr_replacements)
    manifest["redaction_scan"] = redaction_scan
    if redaction_scan["secret_occurrences"]:
        manifest["status"] = "validation_failed"
        manifest["phase"] = "redaction"
        if "redaction scan found secret occurrences" not in manifest["validation_errors"]:
            manifest["validation_errors"].append("redaction scan found secret occurrences")
        exit_code = 1
    _write_report(bundle_dir / "calibration-report.md", manifest)
    _write_manifest(bundle_dir / "manifest.json", manifest)
    return exit_code


def _preflight_inputs(*, config_path: Path, environ: Mapping[str, str]) -> dict[str, Any]:
    config_exists = config_path.is_file()
    config_parse_ok = False
    config: dict[str, Any] | None = None
    if config_exists:
        config = _load_yaml_mapping(config_path)
        config_parse_ok = config is not None

    stream = config.get("stream") if isinstance(config, dict) else None
    matrix = config.get("matrix") if isinstance(config, dict) else None
    rtsp_env = _mapping_string(stream, "rtsp_url_env") or DEFAULT_RTSP_ENV
    matrix_token_env = _mapping_string(matrix, "access_token_env") or DEFAULT_MATRIX_TOKEN_ENV
    rtsp_present = bool(environ.get(rtsp_env))
    matrix_token_present = bool(environ.get(matrix_token_env))
    missing: list[str] = []
    if not config_exists or not config_parse_ok:
        missing.append(config_path.name)
    if not rtsp_present:
        missing.append(rtsp_env)
    if not matrix_token_present:
        missing.append(MATRIX_TOKEN_MISSING_INPUT if matrix_token_env == DEFAULT_MATRIX_TOKEN_ENV else matrix_token_env)

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ready" if not missing else "preflight_blocked",
        "config": {"path": str(config_path), "exists": config_exists, "parse_ok": config_parse_ok},
        "environment": {
            "rtsp_env_name": rtsp_env,
            "rtsp_env_present": rtsp_present,
            "matrix_token_env_name": "Matrix token env key",
            "matrix_token_env_present": matrix_token_present,
        },
        "matrix_token_env": matrix_token_env,
        "missing_inputs": missing,
        "notes": [
            "Names-only preflight summary; raw RTSP URLs, Matrix tokens, auth headers, and private image bytes are intentionally omitted."
        ],
    }


def _write_preflight(path: Path, preflight: Mapping[str, Any], *, generated_at: str) -> None:
    artifact = dict(preflight)
    artifact["generated_at"] = generated_at
    artifact.pop("matrix_token_env", None)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_yaml_mapping(config_path: Path) -> dict[str, Any] | None:
    try:
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    return loaded if isinstance(loaded, dict) else None


def _mapping_string(value: object, key: str) -> str | None:
    if not isinstance(value, dict):
        return None
    item = value.get(key)
    return item.strip() if isinstance(item, str) and item.strip() else None


def _docker_capture_command(*, rtsp_env: str, matrix_token_env: str) -> list[str]:
    command = ["docker", "compose", "run", "--rm", "-e", rtsp_env]
    if matrix_token_env:
        command.extend(["-e", matrix_token_env])
    command.extend(
        [
            "parking-spot-monitor",
            "python",
            "-m",
            "parking_spot_monitor",
            "--config",
            "/config/config.yaml",
            "--data-dir",
            "/data",
            "--capture-once",
        ]
    )
    return command


def _safe_docker_command(command: Sequence[str], *, matrix_token_env: str) -> list[str]:
    safe: list[str] = []
    for item in command:
        if item == matrix_token_env or "TOKEN" in item.upper() or "SECRET" in item.upper() or "ACCESS" in item.upper():
            safe.append("Matrix token env key")
        else:
            safe.append(str(item))
    return safe


def _known_secret_values(environ: Mapping[str, str], *, rtsp_env: str, matrix_token_env: str) -> list[str]:
    values: list[str] = []
    for key, value in environ.items():
        if not value:
            continue
        upper = key.upper()
        if key in {rtsp_env, matrix_token_env} or any(marker in upper for marker in ("SECRET", "TOKEN", "PASSWORD", "AUTH")):
            values.append(value)
    return sorted(set(values), key=len, reverse=True)


def _redact_output(text: object, *, secrets: Sequence[str]) -> tuple[str, int]:
    raw = _coerce_text(text)
    replacements = 0
    for secret in secrets:
        if secret:
            count = raw.count(secret)
            replacements += count
            raw = raw.replace(secret, "<redacted-secret>")
    return "\n".join(_redact_line(line) for line in raw.splitlines()), replacements


def _redact_line(line: str) -> str:
    sanitized = re.sub(r"(?i)authorization\s*:\s*bearer\s+\S+", "Authorization: <redacted>", line)
    sanitized = re.sub(r"(?i)\bbearer\s+\S+", "Bearer <redacted>", sanitized)
    sanitized = re.sub(r"(?i)\btraceback\b", "<redacted-traceback>", sanitized)
    sanitized = sanitized.replace("raw_secret_line", "<redacted-secret-line>")
    stripped = sanitized.strip()
    if stripped.startswith("{"):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return redact_diagnostic_text(sanitized)
        return json.dumps(_redact_json_value(parsed), sort_keys=True, separators=(",", ":"))
    return redact_diagnostic_text(sanitized)


def _redact_json_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_diagnostic_text(value)
    if isinstance(value, list):
        return [_redact_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _redact_json_value(item) for key, item in value.items()}
    return value


def _parse_events(text: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or not stripped.startswith("{"):
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and parsed.get("event") in KNOWN_EVENTS:
            events.append(parsed)
    return events


def _last_event(events: Sequence[Mapping[str, Any]], name: str) -> dict[str, Any] | None:
    for event in reversed(events):
        if event.get("event") == name:
            return dict(event)
    return None


def _event_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for event in events:
        name = str(event.get("event") or "unknown")
        counts[name] = counts.get(name, 0) + 1
    return {"recognized_count": len(events), "counts": counts}


def _capture_summary(*, capture_event: Mapping[str, Any] | None, capture_complete: Mapping[str, Any] | None) -> dict[str, Any]:
    source = dict(capture_complete or capture_event or {})
    return {
        "event": source.get("event"),
        "frame_timestamp": source.get("timestamp"),
        "decode_mode": source.get("decode_mode") or source.get("selected_decode_mode"),
        "path": source.get("path"),
    }


def _copy_and_validate_jpeg(source: Path, destination: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "source_path": str(source),
        "bundle_path": str(destination),
        "exists": source.is_file(),
        "copied": False,
        "byte_size": None,
        "format": None,
        "width": None,
        "height": None,
        "valid_jpeg": False,
        "error_type": None,
        "error": None,
    }
    if not source.is_file():
        result["error_type"] = "FileNotFoundError"
        result["error"] = "artifact missing"
        return result
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        result["copied"] = True
        result["byte_size"] = destination.stat().st_size
        with Image.open(destination) as image:
            image.verify()
        with Image.open(destination) as image:
            result["format"] = image.format
            result["width"], result["height"] = image.size
        result["valid_jpeg"] = result["format"] == "JPEG"
        if not result["valid_jpeg"]:
            result["error_type"] = "InvalidFormat"
            result["error"] = f"expected JPEG, got {result['format']}"
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        result["error"] = str(exc)
    return result


def _copy_context_files(*, data_dir: Path, context_dir: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"health_json_present": False, "state_json_present": False, "gaps": []}
    for filename, key in (("health.json", "health_json"), ("state.json", "state_json")):
        source = data_dir / filename
        present_key = f"{key}_present"
        status_key = f"{key}_status"
        if not source.is_file():
            result[present_key] = False
            result[status_key] = "missing"
            result["gaps"].append(f"{filename} missing")
            continue
        result[present_key] = True
        destination = context_dir / filename
        try:
            shutil.copy2(source, destination)
            try:
                json.loads(destination.read_text(encoding="utf-8"))
                result[status_key] = "copied"
            except json.JSONDecodeError:
                result[status_key] = "malformed"
                result["gaps"].append(f"{filename} malformed")
            stat = destination.stat()
            result[f"{key}_bundle_path"] = str(destination)
            result[f"{key}_byte_size"] = stat.st_size
        except OSError as exc:
            result[status_key] = "unreadable"
            result["gaps"].append(f"{filename} unreadable: {type(exc).__name__}")
    return result


def _validation_errors(
    *,
    docker_exit_code: int | None,
    status: str,
    artifacts: Mapping[str, Mapping[str, Any]],
    detection_summary: Mapping[str, Any] | None,
    detection_failure: Mapping[str, Any] | None,
) -> list[str]:
    errors: list[str] = []
    if status != "success":
        return errors
    for name, check in artifacts.items():
        if not check.get("exists"):
            errors.append(f"{name} missing")
        elif not check.get("valid_jpeg"):
            errors.append(f"{name} invalid JPEG: {check.get('error_type')}")
    if docker_exit_code == 0 and detection_summary is None and detection_failure is None:
        errors.append("detection-frame-processed")
    return errors


def _redaction_scan(bundle_dir: Path, *, secrets: Sequence[str], replacement_count: int) -> dict[str, Any]:
    secret_occurrences = 0
    scanned_files: list[str] = []
    forbidden_patterns = [r"Authorization:\s*Bearer", r"\bTraceback\b", r"raw_secret_line"]
    for path in sorted(bundle_dir.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        scanned_files.append(str(path))
        text = path.read_text(encoding="utf-8", errors="replace")
        lowered = text.lower()
        for secret in secrets:
            if secret:
                secret_occurrences += lowered.count(secret.lower())
        for pattern in forbidden_patterns:
            secret_occurrences += len(re.findall(pattern, text, flags=re.IGNORECASE))
    return {
        "secret_occurrences": secret_occurrences,
        "redaction_replacements": replacement_count,
        "scanned_files": scanned_files,
    }


def _write_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_report(path: Path, manifest: Mapping[str, Any]) -> None:
    lines = [
        "# Calibration Bundle Report",
        "",
        f"- Status: `{manifest.get('status')}`",
        f"- Phase: `{manifest.get('phase')}`",
        f"- Docker exit code: `{manifest.get('docker_exit_code')}`",
        f"- Started: `{manifest.get('started_at')}`",
        f"- Completed: `{manifest.get('completed_at')}`",
        f"- Raw frame valid: `{manifest.get('artifacts', {}).get('raw_frame', {}).get('valid_jpeg')}`",
        f"- Debug overlay valid: `{manifest.get('artifacts', {}).get('debug_overlay', {}).get('valid_jpeg')}`",
        f"- Detection event: `{(manifest.get('detection_summary') or {}).get('event')}`",
        f"- Detection failure: `{(manifest.get('detection_failure') or {}).get('event')}`",
        f"- Health context present: `{manifest.get('context', {}).get('health_json_present')}`",
        f"- State context present: `{manifest.get('context', {}).get('state_json_present')}`",
        f"- Redaction occurrences: `{manifest.get('redaction_scan', {}).get('secret_occurrences')}`",
        "",
        "Private JPEG artifacts are stored only inside this ignored local bundle. Text artifacts contain metadata and redacted logs only.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _timestamp_for_path(timestamp: str) -> str:
    return timestamp.replace(":", "-").replace("+00-00", "Z")


def _unique_bundle_dir(root: Path, name: str) -> Path:
    candidate = root / name
    if not candidate.exists():
        return candidate
    index = 2
    while True:
        candidate = root / f"{name}-{index}"
        if not candidate.exists():
            return candidate
        index += 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
