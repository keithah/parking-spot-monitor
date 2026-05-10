from __future__ import annotations

import json
import re
import sys
from collections.abc import Mapping, Sequence
from typing import Any, TextIO


_LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
_SECRET_PATTERNS = (
    re.compile(r"\b(rtsp://)\S+", re.IGNORECASE),
    re.compile(r"(?i)(access[_-]?token|matrix[_-]?token|token)(\s*[:=]\s*)\S+"),
    re.compile(r"(?i)(password|passwd|pwd|secret|authorization)(\s*[:=]\s*)\S+"),
    re.compile(r"(?i)(bearer\s+)[a-z0-9._~+/=-]+"),
)


class StructuredLogger:
    """Small JSON-lines logger for startup decisions.

    The logger recursively redacts secret-bearing diagnostic strings at the
    structured-log boundary so callers can pass domain fields without leaking
    RTSP credentials, Matrix tokens, raw tracebacks, or token query values.
    """

    def __init__(self, *, level: str = "INFO", stream: TextIO | None = None) -> None:
        self.level = _normalize_level(level)
        self.stream = sys.stderr if stream is None else stream

    def debug(self, event: str, **fields: Any) -> None:
        self.log("DEBUG", event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self.log("INFO", event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self.log("WARNING", event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self.log("ERROR", event, **fields)

    def log(self, level: str, event: str, **fields: Any) -> None:
        normalized_level = _normalize_level(level)
        if _LOG_LEVELS[normalized_level] < _LOG_LEVELS[self.level]:
            return
        record = redact_diagnostic_value({"event": event, "level": normalized_level, **fields})
        self.stream.write(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str) + "\n")
        self.stream.flush()


def redact_diagnostic_text(text: object) -> str:
    """Return a log-safe string with common credential carriers redacted."""

    redacted = _coerce_text(text)
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub(_redact_match, redacted)
    redacted = re.sub(r"(?i)\btraceback\b", "", redacted)
    redacted = re.sub(r"(?i)raw[_ -]?image[_ -]?bytes.*", "raw_image_bytes<redacted>", redacted)
    redacted = re.sub(r"\s+", " ", redacted)
    return redacted.strip()


def redact_diagnostic_value(value: Any) -> Any:
    """Recursively redact diagnostic values before JSON-line serialization."""

    if isinstance(value, str):
        return redact_diagnostic_text(value)
    if isinstance(value, Mapping):
        return {key: redact_diagnostic_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(redact_diagnostic_value(item) for item in value)
    if isinstance(value, list):
        return [redact_diagnostic_value(item) for item in value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact_diagnostic_value(item) for item in value]
    return value


def setup_logging(*, level: str = "INFO", stream: TextIO | None = None) -> StructuredLogger:
    return StructuredLogger(level=level, stream=stream)


def _normalize_level(level: str) -> str:
    normalized = level.upper()
    if normalized not in _LOG_LEVELS:
        return "INFO"
    return normalized


def _coerce_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _redact_match(match: re.Match[str]) -> str:
    groups = match.groups()
    if groups and groups[0].lower().startswith("rtsp://"):
        return "rtsp://<redacted>"
    if len(groups) == 2:
        return f"{groups[0]}{groups[1]}<redacted>"
    if len(groups) == 1:
        return f"{groups[0]}<redacted>"
    return "<redacted>"
