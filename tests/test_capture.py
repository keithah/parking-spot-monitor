from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Sequence

import pytest

from parking_spot_monitor.capture import (
    CaptureError,
    DecodeMode,
    FrameCaptureResult,
    build_ffmpeg_argv,
    capture_latest,
    redact_diagnostic_text,
)
from parking_spot_monitor.config import load_settings
from parking_spot_monitor.logging import StructuredLogger

SECRET_MARKER = "capture-secret-should-not-leak"
FAKE_RTSP_VALUE = f"rtsp://camera-user:{SECRET_MARKER}@10.0.0.7:7447/secret-stream"
FAKE_MATRIX_VALUE = f"matrix-value-{SECRET_MARKER}"


def fake_settings():
    return load_settings(
        "config.yaml.example",
        environ={"RTSP_URL": FAKE_RTSP_VALUE, "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_VALUE},
    )


def jpeg_bytes() -> bytes:
    return b"\xff\xd8fake-jpeg-frame\xff\xd9"


def combined_failure_text(exc: CaptureError) -> str:
    return str(exc) + repr(exc.diagnostics())


def test_decode_modes_are_attempted_in_hardware_then_software_order() -> None:
    assert [mode.value for mode in DecodeMode] == ["qsv", "vaapi", "software"]


def test_ffmpeg_command_builder_returns_argv_lists_not_shell_strings(tmp_path: Path) -> None:
    output_path = tmp_path / "latest.jpg"

    argv = build_ffmpeg_argv(FAKE_RTSP_VALUE, output_path, DecodeMode.QSV)

    assert isinstance(argv, list)
    assert all(isinstance(part, str) for part in argv)
    assert argv[0] == "ffmpeg"
    assert "shell" not in argv
    assert FAKE_RTSP_VALUE in argv
    assert str(output_path) == argv[-1]
    assert "-hwaccel" in argv
    assert "qsv" in argv


def test_redaction_removes_resolved_rtsp_and_credential_like_substrings() -> None:
    diagnostic = (
        f"ffmpeg failed opening {FAKE_RTSP_VALUE}; "
        "Authorization: Bearer abc.def.ghi; "
        "password=super-secret; token=tok_123456789"
    )

    redacted = redact_diagnostic_text(diagnostic, secrets=[FAKE_RTSP_VALUE, FAKE_MATRIX_VALUE])

    assert FAKE_RTSP_VALUE not in redacted
    assert SECRET_MARKER not in redacted
    assert "super-secret" not in redacted
    assert "tok_123456789" not in redacted
    assert "[REDACTED]" in redacted


def test_capture_latest_returns_result_shape_after_valid_jpeg_write(tmp_path: Path) -> None:
    settings = fake_settings()
    calls: list[Sequence[str]] = []

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        Path(argv[-1]).write_bytes(jpeg_bytes())
        return subprocess.CompletedProcess(argv, 0, stderr="captured frame")

    result = capture_latest(settings, tmp_path, runner=runner, now=lambda: "2025-01-01T00:00:00Z")

    assert isinstance(result, FrameCaptureResult)
    assert result.timestamp == "2025-01-01T00:00:00Z"
    assert result.latest_path == tmp_path / "latest.jpg"
    assert result.selected_mode is DecodeMode.QSV
    assert result.duration_seconds >= 0
    assert result.byte_size == len(jpeg_bytes())
    assert calls and isinstance(calls[0], list)


def test_capture_latest_falls_back_from_hardware_failures_to_software_success(tmp_path: Path) -> None:
    settings = fake_settings()
    events: list[str] = []
    attempted: list[DecodeMode] = []

    class RecordingLogger(StructuredLogger):
        def log(self, level: str, event: str, **fields: object) -> None:
            events.append(event)
            super().log(level, event, **fields)

    logger = RecordingLogger()

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        mode = DecodeMode.SOFTWARE
        if "qsv" in argv:
            mode = DecodeMode.QSV
        elif "vaapi" in argv:
            mode = DecodeMode.VAAPI
        attempted.append(mode)
        if mode is not DecodeMode.SOFTWARE:
            return subprocess.CompletedProcess(argv, 1, stderr=f"failed {FAKE_RTSP_VALUE}")
        Path(argv[-1]).write_bytes(jpeg_bytes())
        return subprocess.CompletedProcess(argv, 0, stderr="software ok")

    result = capture_latest(settings, tmp_path, logger=logger, runner=runner)

    assert result.selected_mode is DecodeMode.SOFTWARE
    assert attempted == [DecodeMode.QSV, DecodeMode.VAAPI, DecodeMode.SOFTWARE]
    assert events.count("capture-decode-attempt") == 3
    assert "capture-decode-fallback" in events
    assert "capture-frame-written" in events


def test_capture_nonzero_failure_redacts_stderr_and_does_not_expose_raw_argv(tmp_path: Path) -> None:
    settings = fake_settings()

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 1, stderr=f"boom while opening {FAKE_RTSP_VALUE}")

    with pytest.raises(CaptureError) as raised:
        capture_latest(settings, tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    text = combined_failure_text(raised.value)
    assert raised.value.reason == "ffmpeg-nonzero-exit"
    assert raised.value.mode is DecodeMode.SOFTWARE
    assert FAKE_RTSP_VALUE not in text
    assert SECRET_MARKER not in text
    assert "Traceback" not in text
    assert "argv" not in raised.value.diagnostics()


def test_capture_timeout_failure_is_typed_and_redacted(tmp_path: Path) -> None:
    settings = fake_settings()

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(argv, timeout=timeout, stderr=f"timeout {FAKE_RTSP_VALUE}")

    with pytest.raises(CaptureError) as raised:
        capture_latest(settings, tmp_path, modes=[DecodeMode.SOFTWARE], timeout_seconds=1.25, runner=runner)

    diagnostics = raised.value.diagnostics()
    text = combined_failure_text(raised.value)
    assert raised.value.reason == "ffmpeg-timeout"
    assert diagnostics["timeout_seconds"] == 1.25
    assert FAKE_RTSP_VALUE not in text
    assert SECRET_MARKER not in text


@pytest.mark.parametrize("payload,reason", [(b"", "output-empty"), (b"not-a-jpeg", "output-invalid-jpeg")])
def test_capture_rejects_empty_or_non_jpeg_output(tmp_path: Path, payload: bytes, reason: str) -> None:
    settings = fake_settings()

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        Path(argv[-1]).write_bytes(payload)
        return subprocess.CompletedProcess(argv, 0, stderr="ok")

    with pytest.raises(CaptureError) as raised:
        capture_latest(settings, tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == reason
    assert raised.value.output_path == tmp_path / "latest.jpg"


def test_capture_missing_executable_is_capture_failure(tmp_path: Path) -> None:
    settings = fake_settings()

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        raise FileNotFoundError("ffmpeg missing")

    with pytest.raises(CaptureError) as raised:
        capture_latest(settings, tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == "ffmpeg-missing"
    assert "ffmpeg missing" in str(raised.value)
