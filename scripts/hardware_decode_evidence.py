from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from typing import Any

RunCallable = Callable[..., subprocess.CompletedProcess[str]]


def collect_hardware_decode_summary(*, run: RunCallable = subprocess.run, timeout_seconds: float = 45.0) -> dict[str, Any]:
    command = [sys.executable, "scripts/verify_hardware_decode.py", "--json"]
    try:
        completed = run(command, check=False, capture_output=True, text=True, timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        return {"attempted": True, "exit_code": None, "status": "verifier_timeout", "accepted": False, "checks": {}}
    except OSError as exc:
        return {
            "attempted": True,
            "exit_code": None,
            "status": "verifier_unavailable",
            "accepted": False,
            "error_type": type(exc).__name__,
            "checks": {},
        }

    parsed = _parse_json_object(completed.stdout)
    verdict = parsed.get("verdict") if isinstance(parsed.get("verdict"), dict) else {}
    checks = parsed.get("checks") if isinstance(parsed.get("checks"), dict) else {}
    return {
        "attempted": True,
        "exit_code": int(completed.returncode),
        "status": str(verdict.get("status") or "verifier_output_unavailable"),
        "accepted": bool(verdict.get("accepted")),
        "checks": _compact_checks(checks),
    }


def _parse_json_object(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _compact_checks(checks: dict[str, Any]) -> dict[str, dict[str, Any]]:
    compact: dict[str, dict[str, Any]] = {}
    for name in sorted(checks):
        check = checks.get(name)
        if not isinstance(check, dict):
            continue
        compact[str(name)] = {"passed": bool(check.get("passed")), "returncode": check.get("returncode")}
    return compact
