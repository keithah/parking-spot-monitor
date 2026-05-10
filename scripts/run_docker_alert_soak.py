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
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

import yaml
from PIL import Image, UnidentifiedImageError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parking_spot_monitor.logging import redact_diagnostic_text

CONFIG_PATH = Path("config.yaml")
DATA_DIR = Path("data")
RESULT_NAME = "alert-soak-result.json"
EVIDENCE_NAME = "alert-soak-evidence.md"
INPUT_PREFLIGHT_NAME = "alert-soak-input-preflight.json"
STDOUT_LOG_NAME = "alert-soak-docker.stdout.log"
STDERR_LOG_NAME = "alert-soak-docker.stderr.log"
DEFAULT_MATRIX_TOKEN_ENV = "MATRIX_ACCESS_TOKEN"
MATRIX_TOKEN_MISSING_INPUT = "MATRIX_TOKEN_ENV"
ORGANIC_ALERT_EVENT = "occupancy-open-event"
LIVE_PROOF_MARKERS = ("LIVE_PROOF", "live-proof")
FORBIDDEN_TEXT_MARKERS = ("rtsp://", "authorization", "bearer ", "access_token", "traceback", "raw image bytes")

DOCKER_ALERT_SOAK_COMMAND = [
    "docker",
    "compose",
    "run",
    "--rm",
    "-e",
    "RTSP_URL",
    "-e",
    DEFAULT_MATRIX_TOKEN_ENV,
    "parking-spot-monitor",
]


class PopenLike(Protocol):
    returncode: int | None

    def communicate(self, timeout: float | None = None) -> tuple[str | bytes | None, str | bytes | None]: ...
    def terminate(self) -> None: ...
    def kill(self) -> None: ...


PopenFactory = Callable[..., PopenLike]
ReadbackCallable = Callable[..., dict[str, Any]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a bounded Docker alert soak and write redacted alert/readback evidence.")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Host config.yaml path mounted into Docker Compose.")
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="Host data directory for soak artifacts.")
    parser.add_argument("--soak-seconds", type=float, default=300.0, help="Bounded soak duration; timeout at this duration is normal completion.")
    parser.add_argument("--readback-timeout-seconds", type=float, default=10.0, help="Maximum Matrix readback request time.")
    parser.add_argument("--readback-limit", type=int, default=50, help="Recent Matrix message count to inspect.")
    parser.add_argument("--skip-readback", action="store_true", help="Skip Matrix room readback and record an explicit coverage gap.")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    popen_factory: PopenFactory = subprocess.Popen,
    readback: ReadbackCallable | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    source_environ = os.environ if environ is None else environ
    config_path = Path(args.config)
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    result_path = data_dir / RESULT_NAME
    started_at = _now_iso()

    preflight = _preflight_inputs(config_path=config_path, environ=source_environ)
    _write_json(data_dir / INPUT_PREFLIGHT_NAME, _preflight_artifact(preflight))
    missing_inputs = list(preflight.get("missing_inputs", []))
    matrix_token_env = str(preflight.get("matrix_token_env") or DEFAULT_MATRIX_TOKEN_ENV)
    secrets = _known_secret_values(source_environ, matrix_token_env=matrix_token_env)
    docker_command = _docker_alert_soak_command(matrix_token_env)

    if missing_inputs:
        result = _normalize_result(
            {
                "status": "preflight_failed",
                "phase": "preflight",
                "started_at": started_at,
                "completed_at": _now_iso(),
                "requested_soak_seconds": float(args.soak_seconds),
                "observed_soak_seconds": 0.0,
                "missing_inputs": missing_inputs,
                "safe_docker_argv": _safe_docker_command(docker_command),
                "docker": {"attempted": False, "exit_code": None, "timed_out": False, "terminated": False},
                "room_readback_status": "not_attempted",
                "redaction_scan": {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 0},
            }
        )
        _write_json(result_path, result)
        _write_evidence_report(data_dir / EVIDENCE_NAME, result)
        return 2

    docker_started = time.monotonic()
    stdout_raw: object = ""
    stderr_raw: object = ""
    docker_exit_code: int | None = None
    timed_out = False
    terminated = False
    killed = False
    try:
        proc = popen_factory(docker_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            stdout_raw, stderr_raw = proc.communicate(timeout=max(0.0, float(args.soak_seconds)))
            docker_exit_code = proc.returncode
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            terminated = True
            stdout_raw = exc.stdout
            stderr_raw = exc.stderr
            try:
                proc.terminate()
                more_stdout, more_stderr = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                killed = True
                proc.kill()
                more_stdout, more_stderr = proc.communicate(timeout=10)
            stdout_raw = _coerce_text(stdout_raw) + _coerce_text(more_stdout)
            stderr_raw = _coerce_text(stderr_raw) + _coerce_text(more_stderr)
            docker_exit_code = proc.returncode
    except OSError as exc:
        result = _normalize_result(
            {
                "status": "docker_failed",
                "phase": "docker",
                "started_at": started_at,
                "completed_at": _now_iso(),
                "requested_soak_seconds": float(args.soak_seconds),
                "observed_soak_seconds": round(time.monotonic() - docker_started, 3),
                "safe_docker_argv": _safe_docker_command(docker_command),
                "docker": {"attempted": True, "exit_code": None, "timed_out": False, "terminated": False, "error_type": type(exc).__name__},
                "room_readback_status": "not_attempted",
                "redaction_scan": {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 0},
            }
        )
        _write_json(result_path, result)
        _write_evidence_report(data_dir / EVIDENCE_NAME, result)
        return 1

    stdout_text, stdout_redactions = _redact_soak_text(_coerce_text(stdout_raw), secrets=secrets)
    stderr_text, stderr_redactions = _redact_soak_text(_coerce_text(stderr_raw), secrets=secrets)
    (data_dir / STDOUT_LOG_NAME).write_text(stdout_text, encoding="utf-8")
    (data_dir / STDERR_LOG_NAME).write_text(stderr_text, encoding="utf-8")

    docker_summary = {
        "attempted": True,
        "exit_code": docker_exit_code,
        "timed_out": timed_out,
        "terminated": terminated,
        "killed": killed,
        "expected_timeout_completion": timed_out,
    }
    base = {
        "schema_version": 1,
        "started_at": started_at,
        "completed_at": _now_iso(),
        "requested_soak_seconds": float(args.soak_seconds),
        "observed_soak_seconds": round(time.monotonic() - docker_started, 3),
        "safe_docker_argv": _safe_docker_command(docker_command),
        "docker": docker_summary,
    }

    if not timed_out and docker_exit_code != 0:
        result = _normalize_result(
            base
            | {
                "status": "docker_failed",
                "phase": "docker",
                "log_summary": _parse_alert_logs(stdout_text + "\n" + stderr_text),
                "room_readback_status": "not_attempted",
                "redaction_scan": _redaction_scan(data_dir, secrets=secrets, replacement_count=stdout_redactions + stderr_redactions),
            }
        )
        _write_json(result_path, result)
        _write_evidence_report(data_dir / EVIDENCE_NAME, result)
        return int(docker_exit_code or 1)

    log_summary = _parse_alert_logs(stdout_text + "\n" + stderr_text)
    artifact_summary = _artifact_summary(data_dir)
    config_summary = _read_matrix_config(config_path)
    room_readback = _check_alert_readback(
        alerts=log_summary["organic_alerts"],
        config_summary=config_summary,
        token=source_environ.get(matrix_token_env, ""),
        timeout_seconds=float(args.readback_timeout_seconds),
        limit=int(args.readback_limit),
        skip=bool(args.skip_readback),
        readback=fetch_matrix_room_messages if readback is None else readback,
    )
    result_body = base | {
        "log_summary": log_summary,
        "alert_summary": _alert_summary(log_summary, room_readback),
        "duplicate_summary": log_summary["duplicates"],
        "artifact_summary": artifact_summary,
        "health_summary": _json_artifact_summary(data_dir / "health.json"),
        "state_summary": _json_artifact_summary(data_dir / "state.json"),
        "room_readback_status": room_readback["status"],
        "room_readback": room_readback,
        "redaction_scan": _redaction_scan(data_dir, secrets=secrets, replacement_count=stdout_redactions + stderr_redactions),
    }
    status, phase, exit_code = _classify_result(result_body)
    result_body.update({"status": status, "phase": phase})
    result = _normalize_result(result_body)
    _write_json(result_path, result)
    _write_evidence_report(data_dir / EVIDENCE_NAME, result)
    return exit_code


def _preflight_inputs(*, config_path: Path, environ: Mapping[str, str]) -> dict[str, Any]:
    if not config_path.is_file():
        return {"config_exists": False, "config_parse_ok": False, "config_path": str(config_path), "rtsp_env": "RTSP_URL", "rtsp_env_present": bool(environ.get("RTSP_URL")), "matrix_homeserver_present": False, "matrix_room_id_present": False, "matrix_token_env": DEFAULT_MATRIX_TOKEN_ENV, "matrix_token_env_present": bool(environ.get(DEFAULT_MATRIX_TOKEN_ENV)), "missing_inputs": [config_path.name, "RTSP_URL", MATRIX_TOKEN_MISSING_INPUT]}
    config = _load_yaml_mapping(config_path)
    if config is None:
        return {"config_exists": True, "config_parse_ok": False, "config_path": str(config_path), "rtsp_env": "RTSP_URL", "rtsp_env_present": bool(environ.get("RTSP_URL")), "matrix_homeserver_present": False, "matrix_room_id_present": False, "matrix_token_env": DEFAULT_MATRIX_TOKEN_ENV, "matrix_token_env_present": bool(environ.get(DEFAULT_MATRIX_TOKEN_ENV)), "missing_inputs": [config_path.name, "RTSP_URL", MATRIX_TOKEN_MISSING_INPUT]}
    stream = config.get("stream") if isinstance(config, dict) else None
    matrix = config.get("matrix") if isinstance(config, dict) else None
    rtsp_env = _non_empty_mapping_string(stream, "rtsp_url_env") or "RTSP_URL"
    matrix_token_env = _non_empty_mapping_string(matrix, "access_token_env") or DEFAULT_MATRIX_TOKEN_ENV
    missing: list[str] = []
    if not _non_empty_mapping_string(stream, "rtsp_url_env"):
        missing.append("stream.rtsp_url_env")
    if not environ.get(rtsp_env):
        missing.append(rtsp_env)
    if not _non_empty_mapping_string(matrix, "homeserver"):
        missing.append("matrix.homeserver")
    if not _non_empty_mapping_string(matrix, "room_id"):
        missing.append("matrix.room_id")
    if not _non_empty_mapping_string(matrix, "access_token_env"):
        missing.append("matrix.access_token_env")
    elif not environ.get(matrix_token_env):
        missing.append(MATRIX_TOKEN_MISSING_INPUT if matrix_token_env == DEFAULT_MATRIX_TOKEN_ENV else matrix_token_env)
    return {"config_exists": True, "config_parse_ok": True, "config_path": str(config_path), "rtsp_env": rtsp_env, "rtsp_env_present": bool(environ.get(rtsp_env)), "matrix_homeserver_present": bool(_non_empty_mapping_string(matrix, "homeserver")), "matrix_room_id_present": bool(_non_empty_mapping_string(matrix, "room_id")), "matrix_token_env": matrix_token_env, "matrix_token_env_present": bool(environ.get(matrix_token_env)), "missing_inputs": missing}


def _preflight_artifact(preflight: Mapping[str, Any]) -> dict[str, Any]:
    missing_inputs = [str(item) for item in preflight.get("missing_inputs", []) if isinstance(item, str) and item]
    return {"schema_version": 1, "generated_at": _now_iso(), "status": "ready" if not missing_inputs else "preflight_blocked", "config": {"path": str(preflight.get("config_path") or CONFIG_PATH), "exists": bool(preflight.get("config_exists")), "parse_ok": bool(preflight.get("config_parse_ok"))}, "routing": {"matrix_homeserver_present": bool(preflight.get("matrix_homeserver_present")), "matrix_room_id_present": bool(preflight.get("matrix_room_id_present"))}, "environment": {"rtsp_env_name": str(preflight.get("rtsp_env") or "RTSP_URL"), "rtsp_env_present": bool(preflight.get("rtsp_env_present")), "matrix_token_env_name": "Matrix token env key", "matrix_token_env_present": bool(preflight.get("matrix_token_env_present"))}, "missing_inputs": missing_inputs, "notes": ["Names-only preflight; raw RTSP URLs, Matrix tokens, auth headers, Matrix responses, and image bytes are omitted."]}


def _docker_alert_soak_command(matrix_token_env: str) -> list[str]:
    command = list(DOCKER_ALERT_SOAK_COMMAND)
    if matrix_token_env != DEFAULT_MATRIX_TOKEN_ENV:
        command[7] = matrix_token_env
    return command


def _safe_docker_command(command: Sequence[str]) -> list[str]:
    return ["Matrix token env key" if "TOKEN" in str(item).upper() else str(item) for item in command]


def _parse_alert_logs(text: str) -> dict[str, Any]:
    marker_counts: Counter[str] = Counter()
    organic_alerts: list[dict[str, Any]] = []
    delivery_attempts: list[dict[str, Any]] = []
    delivery_succeeded: list[dict[str, Any]] = []
    delivery_failed: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    quiet_window_events: list[dict[str, Any]] = []
    live_proof_ignored_count = 0
    event_ids: list[str] = []
    txns: list[str] = []
    interesting = {ORGANIC_ALERT_EVENT, "matrix-delivery-attempt", "matrix-delivery-succeeded", "matrix-delivery-failed", "matrix-snapshot-copied", "occupancy-open-suppressed", "quiet-window-started", "quiet-window-ended"}
    for line in text.splitlines():
        if any(marker in line for marker in LIVE_PROOF_MARKERS):
            live_proof_ignored_count += 1
        record = _json_line(line)
        event_name = record.get("event") if isinstance(record, dict) else None
        if not isinstance(event_name, str) or event_name not in interesting:
            continue
        marker_counts[event_name] += 1
        safe_record = _bounded_event_record(record)
        if event_name == ORGANIC_ALERT_EVENT:
            if _contains_live_proof_marker(safe_record):
                live_proof_ignored_count += 1
                continue
            alert = _organic_alert_summary(safe_record)
            organic_alerts.append(alert)
            if alert.get("event_id"):
                event_ids.append(str(alert["event_id"]))
        elif event_name == "matrix-delivery-attempt":
            delivery_attempts.append(safe_record)
            if safe_record.get("txn_id"):
                txns.append(str(safe_record["txn_id"]))
        elif event_name == "matrix-delivery-succeeded":
            delivery_succeeded.append(safe_record)
        elif event_name == "matrix-delivery-failed":
            delivery_failed.append(safe_record)
        elif event_name == "matrix-snapshot-copied":
            snapshots.append(safe_record)
        elif event_name == "occupancy-open-suppressed":
            suppressed.append(safe_record)
        else:
            quiet_window_events.append(safe_record)
    return {"marker_counts": dict(marker_counts), "organic_alert_count": len(organic_alerts), "organic_alerts": organic_alerts[:50], "matrix_delivery": {"attempt_count": len(delivery_attempts), "succeeded_count": len(delivery_succeeded), "failed_count": len(delivery_failed), "attempts": delivery_attempts[:50], "succeeded": delivery_succeeded[:50], "failed": delivery_failed[:20]}, "matrix_snapshot_copied_count": len(snapshots), "matrix_snapshots": snapshots[:50], "suppressed_count": len(suppressed), "suppressed_events": suppressed[:50], "quiet_window_event_count": len(quiet_window_events), "quiet_window_events": quiet_window_events[:50], "live_proof_ignored_count": live_proof_ignored_count, "duplicates": {"event_ids": _duplicate_counts(event_ids), "txn_ids": _duplicate_counts(txns)}}


def _organic_alert_summary(record: Mapping[str, Any]) -> dict[str, Any]:
    event_id = record.get("event_id") or _fallback_event_id(record)
    return {"event_id": event_id, "spot_id": record.get("spot_id"), "observed_at": record.get("observed_at"), "snapshot_path": record.get("snapshot_path"), "previous_status": record.get("previous_status"), "new_status": record.get("new_status")}


def _fallback_event_id(record: Mapping[str, Any]) -> str | None:
    spot_id = record.get("spot_id")
    observed_at = record.get("observed_at")
    if spot_id and observed_at:
        return f"{ORGANIC_ALERT_EVENT}:{spot_id}:{observed_at}"
    return None


def _bounded_event_record(record: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {"event", "event_type", "event_id", "spot_id", "observed_at", "source_timestamp", "snapshot_path", "txn_id", "attempt", "final", "reason", "suppressed_reason", "error_type", "status_code", "previous_status", "new_status", "byte_size", "mimetype", "width", "height"}
    return {key: _safe_json_value(record[key]) for key in allowed if key in record}


def _safe_json_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_diagnostic_text(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_safe_json_value(item) for item in value[:10]]
    if isinstance(value, dict):
        return {str(key): _safe_json_value(item) for key, item in list(value.items())[:20]}
    return redact_diagnostic_text(value)


def _contains_live_proof_marker(value: Any) -> bool:
    rendered = json.dumps(value, default=str)
    return any(marker in rendered for marker in LIVE_PROOF_MARKERS)


def _duplicate_counts(values: Sequence[str]) -> dict[str, int]:
    counts = Counter(value for value in values if value)
    return {value: count for value, count in sorted(counts.items()) if count > 1}


def _artifact_summary(data_dir: Path) -> dict[str, Any]:
    snapshots = sorted((data_dir / "snapshots").glob("occupancy-open-event-*.jpg"))
    snapshot_checks = [_jpeg_check(path) for path in snapshots[:100]]
    return {"latest_jpeg": _jpeg_check(data_dir / "latest.jpg"), "event_snapshot_jpegs": {"count": len(snapshots), "summarized_count": len(snapshot_checks), "valid_count": sum(1 for item in snapshot_checks if item.get("valid_jpeg") is True), "files": snapshot_checks}}


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


def _json_artifact_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "parse_ok": False}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"path": str(path), "exists": True, "parse_ok": False, "error_type": type(exc).__name__}
    if not isinstance(payload, dict):
        return {"path": str(path), "exists": True, "parse_ok": False, "error_type": "not_object"}
    return {"path": str(path), "exists": True, "parse_ok": True, "top_level_keys": sorted(str(key) for key in payload.keys())[:50], "status": payload.get("status"), "updated_at": payload.get("updated_at"), "iteration": payload.get("iteration"), "spot_count": len(payload.get("spots", {})) if isinstance(payload.get("spots"), dict) else None}


def _check_alert_readback(*, alerts: Sequence[Mapping[str, Any]], config_summary: Mapping[str, str | float | None], token: str, timeout_seconds: float, limit: int, skip: bool, readback: ReadbackCallable) -> dict[str, Any]:
    if skip:
        return {"status": "skipped", "reason": "operator_skip", "alerts_checked": 0, "per_alert": []}
    if not alerts:
        return {"status": "not_applicable", "reason": "no_organic_alerts", "alerts_checked": 0, "per_alert": []}
    homeserver = config_summary.get("homeserver")
    room_id = config_summary.get("room_id")
    if not isinstance(homeserver, str) or not isinstance(room_id, str) or not token:
        return {"status": "gap", "reason": "missing_matrix_readback_inputs", "alerts_checked": 0, "per_alert": []}
    try:
        payload = readback(homeserver=homeserver, room_id=room_id, access_token=token, timeout_seconds=float(config_summary.get("timeout_seconds") or timeout_seconds), limit=limit)
    except Exception as exc:
        return {"status": "gap", "reason": type(exc).__name__, "alerts_checked": 0, "per_alert": []}
    chunk = payload.get("chunk") if isinstance(payload, dict) else None
    if not isinstance(chunk, list):
        return {"status": "gap", "reason": "malformed_response", "alerts_checked": 0, "per_alert": []}
    messages = _safe_matrix_message_bodies(chunk)
    per_alert: list[dict[str, Any]] = []
    for alert in alerts[: max(1, min(limit, 50))]:
        spot_id = str(alert.get("spot_id") or "")
        text_found = any(message["msgtype"] == "m.text" and f"Parking spot open: {spot_id}" in message["body"] for message in messages)
        image_found = any(message["msgtype"] == "m.image" and f"Raw full-frame snapshot for {spot_id}" in message["body"] for message in messages)
        per_alert.append({"event_id": alert.get("event_id"), "spot_id": spot_id, "text_found": text_found, "image_found": image_found})
    verified = all(item["text_found"] and item["image_found"] for item in per_alert)
    return {"status": "verified" if verified else "gap", "reason": None if verified else "alert_messages_not_found", "alerts_checked": len(per_alert), "inspected_count": len(messages), "per_alert": per_alert}


def _safe_matrix_message_bodies(chunk: Sequence[Any]) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    for event in chunk:
        content = event.get("content") if isinstance(event, dict) else None
        if not isinstance(content, dict):
            continue
        msgtype = content.get("msgtype")
        body = content.get("body")
        if isinstance(msgtype, str) and isinstance(body, str):
            messages.append({"msgtype": msgtype, "body": redact_diagnostic_text(body)})
    return messages


def fetch_matrix_room_messages(*, homeserver: str, room_id: str, access_token: str, timeout_seconds: float, limit: int = 50) -> dict[str, Any]:
    room_segment = urllib.parse.quote(room_id, safe="")
    query = urllib.parse.urlencode({"dir": "b", "limit": max(1, int(limit))})
    url = f"{homeserver.rstrip('/')}/_matrix/client/v3/rooms/{room_segment}/messages?{query}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"matrix_readback_http_{exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("matrix_readback_unavailable") from exc
    except json.JSONDecodeError as exc:
        raise ValueError("matrix_readback_malformed_json") from exc


def _alert_summary(log_summary: Mapping[str, Any], room_readback: Mapping[str, Any]) -> dict[str, Any]:
    matrix_delivery = log_summary.get("matrix_delivery") if isinstance(log_summary.get("matrix_delivery"), dict) else {}
    return {"organic_alert_count": int(log_summary.get("organic_alert_count", 0)), "delivery_attempt_count": int(matrix_delivery.get("attempt_count", 0)), "delivery_succeeded_count": int(matrix_delivery.get("succeeded_count", 0)), "delivery_failed_count": int(matrix_delivery.get("failed_count", 0)), "snapshot_copied_count": int(log_summary.get("matrix_snapshot_copied_count", 0)), "suppressed_count": int(log_summary.get("suppressed_count", 0)), "readback_status": room_readback.get("status")}


def _classify_result(result: Mapping[str, Any]) -> tuple[str, str, int]:
    redaction = result.get("redaction_scan") if isinstance(result.get("redaction_scan"), dict) else {}
    if int(redaction.get("secret_occurrences", 0)) or int(redaction.get("forbidden_pattern_occurrences", 0)):
        return "validation_failed", "redaction", 1
    artifacts = result.get("artifact_summary") if isinstance(result.get("artifact_summary"), dict) else {}
    latest = artifacts.get("latest_jpeg") if isinstance(artifacts.get("latest_jpeg"), dict) else {}
    snapshots = artifacts.get("event_snapshot_jpegs") if isinstance(artifacts.get("event_snapshot_jpegs"), dict) else {}
    if latest.get("exists") and latest.get("valid_jpeg") is not True:
        return "validation_failed", "artifact_validation", 1
    if int(snapshots.get("count", 0)) > 0 and int(snapshots.get("valid_count", 0)) != int(snapshots.get("summarized_count", 0)):
        return "validation_failed", "artifact_validation", 1
    log_summary = result.get("log_summary") if isinstance(result.get("log_summary"), dict) else {}
    duplicates = log_summary.get("duplicates") if isinstance(log_summary.get("duplicates"), dict) else {}
    if duplicates.get("event_ids") or duplicates.get("txn_ids"):
        return "validation_failed", "duplicate_diagnostics", 1
    if int(log_summary.get("organic_alert_count", 0)) == 0:
        return "coverage_gap", "alert_detection", 1
    matrix_delivery = log_summary.get("matrix_delivery") if isinstance(log_summary.get("matrix_delivery"), dict) else {}
    if int(matrix_delivery.get("failed_count", 0)) > 0:
        return "validation_failed", "matrix_delivery", 1
    if result.get("room_readback_status") != "verified":
        return "readback_gap", "matrix_readback", 1
    return "success", "complete", 0


def _read_matrix_config(config_path: Path) -> dict[str, str | float | None]:
    raw = _load_yaml_mapping(config_path)
    matrix = raw.get("matrix") if isinstance(raw, dict) else None
    if not isinstance(matrix, dict):
        return {"homeserver": None, "room_id": None, "timeout_seconds": None}
    timeout = matrix.get("timeout_seconds")
    return {"homeserver": matrix.get("homeserver") if isinstance(matrix.get("homeserver"), str) and matrix.get("homeserver") else None, "room_id": matrix.get("room_id") if isinstance(matrix.get("room_id"), str) and matrix.get("room_id") else None, "timeout_seconds": float(timeout) if isinstance(timeout, (int, float)) else None}


def _load_yaml_mapping(config_path: Path) -> dict[str, Any] | None:
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


def _redact_soak_text(text: str, *, secrets: Sequence[str]) -> tuple[str, int]:
    replacements = 0
    safe_lines: list[str] = []
    for line in text.splitlines(keepends=True):
        lower = line.lower()
        if "authorization" in lower:
            safe_lines.append("[redacted-header]\n" if line.endswith("\n") else "[redacted-header]")
            replacements += 1
            continue
        if "traceback" in lower:
            safe_lines.append("[redacted-trace]\n" if line.endswith("\n") else "[redacted-trace]")
            replacements += 1
            continue
        if "raw_image_bytes" in lower or "raw image bytes" in lower:
            safe_lines.append("[redacted-image-bytes]\n" if line.endswith("\n") else "[redacted-image-bytes]")
            replacements += 1
            continue
        before = line
        redacted = redact_diagnostic_text(line)
        if redacted != before.strip():
            replacements += 1
        if line.endswith("\n") and not redacted.endswith("\n"):
            redacted = redacted + "\n"
        redacted = redacted.replace("rtsp://<redacted>", "[redacted-rtsp]")
        for secret in secrets:
            if secret and secret in redacted:
                redacted = redacted.replace(secret, "<redacted>")
                replacements += 1
        safe_lines.append(redacted)
    return "".join(safe_lines), replacements


def _redaction_scan(data_dir: Path, *, secrets: Sequence[str], replacement_count: int) -> dict[str, int]:
    paths = [data_dir / RESULT_NAME, data_dir / STDOUT_LOG_NAME, data_dir / STDERR_LOG_NAME, data_dir / EVIDENCE_NAME]
    secret_occurrences = 0
    forbidden_pattern_occurrences = 0
    for path in paths:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        lower = text.lower()
        for secret in secrets:
            if secret:
                secret_occurrences += text.count(secret)
        for marker in FORBIDDEN_TEXT_MARKERS:
            forbidden_pattern_occurrences += lower.count(marker)
    return {"secret_occurrences": secret_occurrences, "forbidden_pattern_occurrences": forbidden_pattern_occurrences, "redaction_replacements": replacement_count}


def _known_secret_values(environ: Mapping[str, str], *, matrix_token_env: str) -> list[str]:
    secret_keys = {"RTSP_URL", DEFAULT_MATRIX_TOKEN_ENV, matrix_token_env}
    return [value for key, value in environ.items() if key in secret_keys and value]


def _json_line(line: str) -> dict[str, Any]:
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalize_result(result: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(result)
    normalized.setdefault("schema_version", 1)
    normalized.setdefault("log_summary", None)
    normalized.setdefault("alert_summary", None)
    normalized.setdefault("duplicate_summary", None)
    normalized.setdefault("artifact_summary", None)
    normalized.setdefault("room_readback", None)
    normalized.setdefault("health_summary", None)
    normalized.setdefault("state_summary", None)
    normalized.setdefault("redaction_scan", {"secret_occurrences": 0, "forbidden_pattern_occurrences": 0, "redaction_replacements": 0})
    normalized["alerts"] = normalized["alert_summary"]
    normalized["duplicates"] = normalized["duplicate_summary"]
    normalized["artifacts"] = normalized["artifact_summary"]
    normalized["matrix_room_readback"] = normalized["room_readback"]
    normalized["redaction"] = normalized["redaction_scan"]
    return normalized


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_evidence_report(path: Path, result: Mapping[str, Any]) -> None:
    alert_summary = result.get("alert_summary") if isinstance(result.get("alert_summary"), dict) else {}
    redaction = result.get("redaction_scan") if isinstance(result.get("redaction_scan"), dict) else {}
    docker = result.get("docker") if isinstance(result.get("docker"), dict) else {}
    artifacts = result.get("artifact_summary") if isinstance(result.get("artifact_summary"), dict) else {}
    latest = artifacts.get("latest_jpeg") if isinstance(artifacts.get("latest_jpeg"), dict) else {}
    snapshots = artifacts.get("event_snapshot_jpegs") if isinstance(artifacts.get("event_snapshot_jpegs"), dict) else {}
    report = "\n".join([
        "# Alert Soak Evidence", "",
        f"- Status: `{result.get('status')}`",
        f"- Phase: `{result.get('phase')}`",
        f"- Requested soak seconds: `{result.get('requested_soak_seconds')}`",
        f"- Observed soak seconds: `{result.get('observed_soak_seconds')}`",
        f"- Docker exit code: `{docker.get('exit_code')}` timed_out=`{docker.get('timed_out')}` terminated=`{docker.get('terminated')}`",
        f"- Organic alerts: `{alert_summary.get('organic_alert_count', 0)}`",
        f"- Matrix delivery: attempts=`{alert_summary.get('delivery_attempt_count', 0)}` succeeded=`{alert_summary.get('delivery_succeeded_count', 0)}` failed=`{alert_summary.get('delivery_failed_count', 0)}`",
        f"- Matrix readback: `{result.get('room_readback_status')}`",
        f"- Latest JPEG: exists=`{latest.get('exists')}` valid=`{latest.get('valid_jpeg')}`",
        f"- Event snapshots: count=`{snapshots.get('count', 0)}` valid_count=`{snapshots.get('valid_count', 0)}`",
        f"- Redaction secret occurrences: `{redaction.get('secret_occurrences', 0)}`",
        f"- Redaction forbidden pattern occurrences: `{redaction.get('forbidden_pattern_occurrences', 0)}`",
        f"- Redaction replacements: `{redaction.get('redaction_replacements', 0)}`", "",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")


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
