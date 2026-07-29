from __future__ import annotations

import os
import re
import subprocess
import tempfile
import time
import warnings
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from PIL import Image

from parking_spot_monitor.config import RuntimeSettings, sanitize_stream_profile_name
from parking_spot_monitor.logging import StructuredLogger

DEFAULT_CAPTURE_TIMEOUT_SECONDS = 15.0
MAX_CAPTURE_JPEG_BYTES = 32 * 1024 * 1024
STDERR_TAIL_CHARS = 2000
_SECRET_PATTERNS = (
    re.compile(r"(rtsp://)[^\s/@]+:[^\s/@]+@", re.IGNORECASE),
    re.compile(r"(?i)(password|passwd|pwd|token|secret|access_token|authorization)(\s*[:=]\s*)([^\s,;]+)"),
    re.compile(r"(?i)(bearer\s+)([a-z0-9._~+/=-]+)"),
)


class DecodeMode(str, Enum):
    QSV = "qsv"
    VAAPI = "vaapi"
    DRM = "drm"
    SOFTWARE = "software"


DEFAULT_DECODE_MODES = (DecodeMode.VAAPI, DecodeMode.DRM, DecodeMode.SOFTWARE)


@dataclass(frozen=True)
class FrameGeometry:
    stream_profile: str = "primary"
    expected_size: tuple[int, int] | None = None

    def expected_size_diagnostics(self) -> dict[str, int] | None:
        return _frame_size_diagnostics(self.expected_size)


@dataclass(frozen=True)
class FrameCaptureResult:
    timestamp: Any
    latest_path: Path
    selected_mode: DecodeMode
    duration_seconds: float
    byte_size: int
    frame_geometry: FrameGeometry

    def diagnostics(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "output_path": str(self.latest_path),
            "selected_mode": self.selected_mode.value,
            "duration_seconds": round(self.duration_seconds, 6),
            "byte_size": self.byte_size,
            "stream_profile": self.frame_geometry.stream_profile,
            "expected_frame_size": self.frame_geometry.expected_size_diagnostics(),
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


class StreamProfileCapture(Protocol):
    def __call__(
        self,
        settings: RuntimeSettings,
        data_dir: str | Path,
        *,
        stream_profile: str | None = None,
    ) -> FrameCaptureResult: ...


def _frame_size_diagnostics(size: tuple[int, int] | None) -> dict[str, int] | None:
    if size is None:
        return None
    return {"width": int(size[0]), "height": int(size[1])}


def redact_diagnostic_text(text: object, *, secrets: Iterable[str] = ()) -> str:
    redacted = _coerce_text(text)
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub(_redact_match, redacted)
    return redacted


def build_ffmpeg_argv(
    rtsp_url: str,
    output_path: str | Path,
    mode: DecodeMode,
    *,
    network_timeout_seconds: float = DEFAULT_CAPTURE_TIMEOUT_SECONDS,
) -> list[str]:
    network_timeout_us = max(1, int(max(0.001, network_timeout_seconds) * 1_000_000))
    argv = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-rtsp_transport",
        "tcp",
        "-timeout",
        str(network_timeout_us),
    ]
    if mode is DecodeMode.QSV:
        argv.extend(["-hwaccel", "qsv", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "qsv"])
    elif mode is DecodeMode.VAAPI:
        argv.extend(["-hwaccel", "vaapi", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "vaapi"])
    elif mode is DecodeMode.DRM:
        argv.extend(["-hwaccel", "drm", "-hwaccel_device", "/dev/dri/renderD128"])
    argv.extend([
        "-i",
        rtsp_url,
    ])
    if mode is DecodeMode.VAAPI:
        argv.extend(["-vf", "format=nv12|vaapi,hwdownload,format=nv12"])
    argv.extend([
        "-frames:v",
        "1",
        "-q:v",
        "2",
        "-f",
        "image2",
        str(output_path),
    ])
    return argv


def _capture_output_path(output_dir: Path, profile_name: str) -> Path:
    if profile_name == "primary":
        return output_dir / "latest.jpg"
    sanitized_name = sanitize_stream_profile_name(profile_name)
    if not sanitized_name:
        raise ValueError("stream profile name must contain a filename-safe character")
    return output_dir / f"latest-{sanitized_name}.jpg"


def _capture_temp_path(output_path: Path) -> Path:
    with tempfile.NamedTemporaryFile(
        mode="wb",
        delete=False,
        dir=output_path.parent,
        prefix=f".{output_path.stem}.",
        suffix=".jpg",
    ) as handle:
        return Path(handle.name)


def capture_latest(
    settings: RuntimeSettings,
    data_dir: str | Path,
    *,
    logger: StructuredLogger | None = None,
    runner: SubprocessRunner | None = None,
    modes: Sequence[DecodeMode] | None = None,
    timeout_seconds: float = DEFAULT_CAPTURE_TIMEOUT_SECONDS,
    now: Callable[[], Any] | None = None,
    stream_profile: str | None = None,
) -> FrameCaptureResult:
    output_dir = Path(data_dir)
    selected_profile = settings.stream.profile(stream_profile)
    profile_name = selected_profile.name
    output_path = _capture_output_path(output_dir, profile_name)
    rtsp_url = selected_profile.rtsp_url.value
    secrets = [rtsp_url, settings.matrix.access_token.value]
    selected_modes = list(modes if modes is not None else DEFAULT_DECODE_MODES)
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
        start = time.perf_counter()
        temp_path: Path | None = None
        _log(
            logger,
            "info",
            "capture-decode-attempt",
            mode=mode.value,
            output_path=str(output_path),
            timeout_seconds=timeout_seconds,
            stream_profile=profile_name,
        )
        try:
            try:
                temp_path = _capture_temp_path(output_path)
            except OSError as exc:
                raise _failure(
                    "output-temporary-unavailable",
                    mode,
                    output_path,
                    f"capture temporary output is unavailable: {exc.strerror or type(exc).__name__}",
                    secrets=secrets,
                    duration_seconds=time.perf_counter() - start,
                    timeout_seconds=timeout_seconds,
                ) from exc
            argv = build_ffmpeg_argv(rtsp_url, temp_path, mode, network_timeout_seconds=timeout_seconds)
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
            byte_size = _validate_jpeg_output(
                temp_path,
                failure_output_path=output_path,
                mode=mode,
                secrets=secrets,
                duration_seconds=duration,
                expected_size=(selected_profile.frame_width, selected_profile.frame_height),
            )
            try:
                temp_path.chmod(0o644)
                os.replace(temp_path, output_path)
            except OSError as exc:
                raise _failure(
                    "output-publish-failed",
                    mode,
                    output_path,
                    f"capture output could not be published: {exc.strerror or type(exc).__name__}",
                    secrets=secrets,
                    duration_seconds=duration,
                    timeout_seconds=timeout_seconds,
                ) from exc
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
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass

        timestamp = now() if now is not None else datetime.now(UTC).isoformat()
        result = FrameCaptureResult(
            timestamp=timestamp,
            latest_path=output_path,
            selected_mode=mode,
            duration_seconds=duration,
            byte_size=byte_size,
            frame_geometry=FrameGeometry(
                stream_profile=profile_name,
                expected_size=(selected_profile.frame_width, selected_profile.frame_height),
            ),
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
    failure_output_path: Path | None = None,
    mode: DecodeMode,
    secrets: Iterable[str],
    duration_seconds: float,
    expected_size: tuple[int, int],
) -> int:
    reported_output_path = output_path if failure_output_path is None else failure_output_path
    try:
        byte_size = output_path.stat().st_size
    except OSError as exc:
        raise _failure(
            "output-missing",
            mode,
            reported_output_path,
            f"ffmpeg did not produce readable output: {exc.strerror or type(exc).__name__}",
            secrets=secrets,
            duration_seconds=duration_seconds,
        ) from exc
    if byte_size <= 0:
        raise _failure(
            "output-empty",
            mode,
            reported_output_path,
            "ffmpeg produced an empty output file",
            secrets=secrets,
            duration_seconds=duration_seconds,
        )
    if byte_size > MAX_CAPTURE_JPEG_BYTES:
        raise _failure(
            "output-too-large",
            mode,
            reported_output_path,
            "ffmpeg output exceeds the encoded JPEG resource ceiling",
            secrets=secrets,
            duration_seconds=duration_seconds,
        )
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(output_path) as image:
                if image.format != "JPEG":
                    raise _failure(
                        "output-invalid-jpeg",
                        mode,
                        reported_output_path,
                        "ffmpeg output is not a valid JPEG frame",
                        secrets=secrets,
                        duration_seconds=duration_seconds,
                    )
                if image.size != expected_size:
                    raise _failure(
                        "output-dimensions-mismatch",
                        mode,
                        reported_output_path,
                        "ffmpeg output dimensions do not match the selected stream profile",
                        secrets=secrets,
                        duration_seconds=duration_seconds,
                    )
                image.verify()
            with Image.open(output_path) as decoded_image:
                decoded_image.load()
    except CaptureError:
        raise
    except (OSError, SyntaxError, Image.DecompressionBombWarning, Image.DecompressionBombError) as exc:
        raise _failure(
            "output-invalid-jpeg",
            mode,
            reported_output_path,
            "ffmpeg output is not a valid JPEG frame",
            secrets=secrets,
            duration_seconds=duration_seconds,
        ) from exc
    return byte_size


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
