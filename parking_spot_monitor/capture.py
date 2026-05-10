from __future__ import annotations

import re
import subprocess
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger

DEFAULT_CAPTURE_TIMEOUT_SECONDS = 15.0
STDERR_TAIL_CHARS = 2000
_SECRET_PATTERNS = (
    re.compile(r"(rtsp://)[^\s/@]+:[^\s/@]+@", re.IGNORECASE),
    re.compile(r"(?i)(password|passwd|pwd|token|secret|access_token|authorization)(\s*[:=]\s*)([^\s,;]+)"),
    re.compile(r"(?i)(bearer\s+)([a-z0-9._~+/=-]+)"),
)


class DecodeMode(str, Enum):
    QSV = "qsv"
    VAAPI = "vaapi"
    SOFTWARE = "software"


@dataclass(frozen=True)
class FrameCaptureResult:
    timestamp: Any
    latest_path: Path
    selected_mode: DecodeMode
    duration_seconds: float
    byte_size: int

    def diagnostics(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "output_path": str(self.latest_path),
            "selected_mode": self.selected_mode.value,
            "duration_seconds": round(self.duration_seconds, 6),
            "byte_size": self.byte_size,
        }


@dataclass
class CaptureError(Exception):
    reason: str
    mode: DecodeMode
    output_path: Path
    message: str
    stderr_tail: str = ""
    duration_seconds: float = 0.0
    timeout_seconds: float | None = None
    returncode: int | None = None
    attempted_modes: list[DecodeMode] = field(default_factory=list)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def diagnostics(self) -> dict[str, Any]:
        details: dict[str, Any] = {
            "reason": self.reason,
            "mode": self.mode.value,
            "output_path": str(self.output_path),
            "duration_seconds": round(self.duration_seconds, 6),
            "stderr_tail": self.stderr_tail,
            "attempted_modes": [mode.value for mode in self.attempted_modes],
        }
        if self.timeout_seconds is not None:
            details["timeout_seconds"] = self.timeout_seconds
        if self.returncode is not None:
            details["returncode"] = self.returncode
        return details


class SubprocessRunner(Protocol):
    def __call__(self, argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]: ...


def redact_diagnostic_text(text: object, *, secrets: Iterable[str] = ()) -> str:
    redacted = _coerce_text(text)
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub(_redact_match, redacted)
    return redacted


def build_ffmpeg_argv(rtsp_url: str, output_path: str | Path, mode: DecodeMode) -> list[str]:
    argv = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-rtsp_transport",
        "tcp",
    ]
    if mode is DecodeMode.QSV:
        argv.extend(["-hwaccel", "qsv"])
    elif mode is DecodeMode.VAAPI:
        argv.extend(["-hwaccel", "vaapi"])
    argv.extend([
        "-i",
        rtsp_url,
        "-frames:v",
        "1",
        "-q:v",
        "2",
        "-f",
        "image2",
        str(output_path),
    ])
    return argv


def capture_latest(
    settings: RuntimeSettings,
    data_dir: str | Path,
    *,
    logger: StructuredLogger | None = None,
    runner: SubprocessRunner | None = None,
    modes: Sequence[DecodeMode] | None = None,
    timeout_seconds: float = DEFAULT_CAPTURE_TIMEOUT_SECONDS,
    now: Callable[[], Any] | None = None,
) -> FrameCaptureResult:
    output_dir = Path(data_dir)
    output_path = output_dir / "latest.jpg"
    rtsp_url = settings.stream.rtsp_url.value
    secrets = [rtsp_url, settings.matrix.access_token.value]
    selected_modes = list(modes if modes is not None else DecodeMode)
    if not selected_modes:
        raise ValueError("at least one decode mode is required")

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise _failure(
            "output-directory-unavailable",
            selected_modes[0],
            output_path,
            f"capture output directory is unavailable: {exc}",
            secrets=secrets,
            timeout_seconds=timeout_seconds,
        ) from exc

    run = _run_ffmpeg if runner is None else runner
    failures: list[CaptureError] = []
    attempted_modes: list[DecodeMode] = []

    for mode in selected_modes:
        attempted_modes.append(mode)
        argv = build_ffmpeg_argv(rtsp_url, output_path, mode)
        start = time.perf_counter()
        _log(logger, "info", "capture-decode-attempt", mode=mode.value, output_path=str(output_path), timeout_seconds=timeout_seconds)
        try:
            completed = run(argv, timeout=timeout_seconds)
            duration = time.perf_counter() - start
            if completed.returncode != 0:
                raise _failure(
                    "ffmpeg-nonzero-exit",
                    mode,
                    output_path,
                    "ffmpeg exited with a nonzero status",
                    secrets=secrets,
                    stderr=completed.stderr,
                    duration_seconds=duration,
                    timeout_seconds=timeout_seconds,
                    returncode=completed.returncode,
                )
            byte_size = _validate_jpeg_output(output_path, mode=mode, secrets=secrets, duration_seconds=duration)
        except CaptureError as exc:
            exc.attempted_modes = list(attempted_modes)
            failures.append(exc)
            if mode is not selected_modes[-1]:
                _log(logger, "warning", "capture-decode-fallback", **exc.diagnostics(), next_mode=selected_modes[len(attempted_modes)].value)
                continue
            _log(logger, "error", "capture-all-modes-failed", **exc.diagnostics())
            raise exc
        except subprocess.TimeoutExpired as exc:
            failure = _failure(
                "ffmpeg-timeout",
                mode,
                output_path,
                f"ffmpeg timed out after {timeout_seconds} seconds",
                secrets=secrets,
                stderr=getattr(exc, "stderr", ""),
                duration_seconds=time.perf_counter() - start,
                timeout_seconds=timeout_seconds,
            )
            failure.attempted_modes = list(attempted_modes)
            failures.append(failure)
            if mode is not selected_modes[-1]:
                _log(logger, "warning", "capture-decode-fallback", **failure.diagnostics(), next_mode=selected_modes[len(attempted_modes)].value)
                continue
            _log(logger, "error", "capture-all-modes-failed", **failure.diagnostics())
            raise failure from exc
        except FileNotFoundError as exc:
            failure = _failure(
                "ffmpeg-missing",
                mode,
                output_path,
                str(exc),
                secrets=secrets,
                duration_seconds=time.perf_counter() - start,
                timeout_seconds=timeout_seconds,
            )
            failure.attempted_modes = list(attempted_modes)
            failures.append(failure)
            if mode is not selected_modes[-1]:
                _log(logger, "warning", "capture-decode-fallback", **failure.diagnostics(), next_mode=selected_modes[len(attempted_modes)].value)
                continue
            _log(logger, "error", "capture-all-modes-failed", **failure.diagnostics())
            raise failure from exc

        timestamp = now() if now is not None else datetime.now(UTC).isoformat()
        result = FrameCaptureResult(
            timestamp=timestamp,
            latest_path=output_path,
            selected_mode=mode,
            duration_seconds=duration,
            byte_size=byte_size,
        )
        _log(logger, "info", "capture-frame-written", **result.diagnostics())
        return result

    # Defensive: the empty-mode case is rejected above, and loop returns or raises.
    if failures:
        raise failures[-1]
    raise RuntimeError("capture loop exited without result or failure")  # pragma: no cover


def _run_ffmpeg(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(argv), capture_output=True, text=True, timeout=timeout, check=False)


def _validate_jpeg_output(
    output_path: Path,
    *,
    mode: DecodeMode,
    secrets: Iterable[str],
    duration_seconds: float,
) -> int:
    try:
        payload = output_path.read_bytes()
    except OSError as exc:
        raise _failure(
            "output-missing",
            mode,
            output_path,
            f"ffmpeg did not produce readable output: {exc}",
            secrets=secrets,
            duration_seconds=duration_seconds,
        ) from exc
    if not payload:
        raise _failure(
            "output-empty",
            mode,
            output_path,
            "ffmpeg produced an empty output file",
            secrets=secrets,
            duration_seconds=duration_seconds,
        )
    if not (payload.startswith(b"\xff\xd8") and payload.endswith(b"\xff\xd9")):
        raise _failure(
            "output-invalid-jpeg",
            mode,
            output_path,
            "ffmpeg output is not a valid JPEG frame",
            secrets=secrets,
            duration_seconds=duration_seconds,
        )
    return len(payload)


def _failure(
    reason: str,
    mode: DecodeMode,
    output_path: Path,
    message: str,
    *,
    secrets: Iterable[str],
    stderr: object = "",
    duration_seconds: float = 0.0,
    timeout_seconds: float | None = None,
    returncode: int | None = None,
) -> CaptureError:
    safe_message = redact_diagnostic_text(message, secrets=secrets)
    safe_stderr = redact_diagnostic_text(_tail(stderr), secrets=secrets)
    return CaptureError(
        reason=reason,
        mode=mode,
        output_path=output_path,
        message=safe_message,
        stderr_tail=safe_stderr,
        duration_seconds=duration_seconds,
        timeout_seconds=timeout_seconds,
        returncode=returncode,
    )


def _log(logger: StructuredLogger | None, level: str, event: str, **fields: Any) -> None:
    if logger is None:
        return
    getattr(logger, level)(event, **fields)


def _tail(value: object) -> str:
    text = _coerce_text(value)
    return text[-STDERR_TAIL_CHARS:]


def _coerce_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _redact_match(match: re.Match[str]) -> str:
    if len(match.groups()) == 3:
        return f"{match.group(1)}{match.group(2)}[REDACTED]"
    if len(match.groups()) == 2:
        return f"{match.group(1)}[REDACTED]"
    return "[REDACTED]"
