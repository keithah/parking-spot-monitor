"""Matrix error types and shared HTTP/diagnostic helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import httpx

from parking_spot_monitor.logging import redact_diagnostic_text

class MatrixError(RuntimeError):
    """Safe Matrix delivery error with structured, redacted diagnostics."""

    def __init__(self, message: str, **diagnostics: Any) -> None:
        self.message = redact_diagnostic_text(message)
        self.diagnostics = _sanitize_diagnostics(diagnostics)
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

def _path_token(value: object) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "-", redact_diagnostic_text(value).strip().lower()).strip("-")
    return token or "unknown"

def _require_non_empty(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty")
    return value


def _require_response_key(response: httpx.Response, key: str, *, operation: str, attempt: int | None = None) -> str:
    try:
        payload = response.json()
    except ValueError as exc:
        raise MatrixError(
            "Matrix response was not valid JSON",
            error_type="malformed_response",
            operation=operation,
            status_code=response.status_code,
            missing_key=key,
            attempt=attempt,
        ) from exc
    value = payload.get(key) if isinstance(payload, dict) else None
    if not isinstance(value, str) or not value:
        raise MatrixError(
            "Matrix response was missing a required field",
            error_type="malformed_response",
            operation=operation,
            status_code=response.status_code,
            missing_key=key,
            attempt=attempt,
        )
    return value


def _http_status_error(response: httpx.Response, *, method: str, path: str, attempt: int) -> MatrixError:
    errcode = None
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("errcode"), str):
        errcode = payload["errcode"]
    return MatrixError(
        "Matrix request returned an error status",
        error_type="http_status",
        operation=method,
        path=path,
        attempt=attempt,
        status_code=response.status_code,
        errcode=errcode,
    )


_UNSAFE_DIAGNOSTIC_KEYS = {"raw_body", "response_body", "body", "headers", "authorization"}


def _sanitize_diagnostics(diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in diagnostics.items():
        if value is None or str(key).lower() in _UNSAFE_DIAGNOSTIC_KEYS:
            continue
        if isinstance(value, str):
            sanitized[key] = redact_diagnostic_text(value)
        elif isinstance(value, Mapping):
            sanitized[key] = _sanitize_diagnostics(value)
        else:
            sanitized[key] = value
    return sanitized
