from __future__ import annotations

import subprocess
import warnings
from io import BytesIO
from pathlib import Path
from typing import Sequence

import pytest
from PIL import Image

from parking_spot_monitor.capture import (
    DEFAULT_DECODE_MODES,
    CaptureError,
    DecodeMode,
    FrameCaptureResult,
    FrameGeometry,
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
        environ={
            "RTSP_URL": FAKE_RTSP_VALUE,
            "RTSP_URL_4K": f"{FAKE_RTSP_VALUE}/4k",
            "RTSP_URL_360P": f"{FAKE_RTSP_VALUE}/360p",
            "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_VALUE,
        },
    )


def jpeg_bytes(size: tuple[int, int] = (1458, 806)) -> bytes:
    buffer = BytesIO()
    Image.new("RGB", size, (20, 30, 40)).save(buffer, "JPEG")
    return buffer.getvalue()


def combined_failure_text(exc: CaptureError) -> str:
    return str(exc) + repr(exc.diagnostics())


def test_default_decode_modes_use_vaapi_not_qsv_for_intel_hardware_path() -> None:
    assert [mode.value for mode in DEFAULT_DECODE_MODES] == ["vaapi", "drm", "software"]
    assert DecodeMode.QSV not in DEFAULT_DECODE_MODES


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


def test_ffmpeg_command_builder_sets_rtsp_network_timeouts_before_input(tmp_path: Path) -> None:
    output_path = tmp_path / "latest.jpg"

    argv = build_ffmpeg_argv(FAKE_RTSP_VALUE, output_path, DecodeMode.SOFTWARE, network_timeout_seconds=3.25)

    assert argv[argv.index("-timeout") + 1] == "3250000"
    assert argv.index("-timeout") < argv.index("-i")
    assert "-rw_timeout" not in argv



def test_vaapi_capture_downloads_hardware_frames_before_jpeg_encoding(tmp_path: Path) -> None:
    output_path = tmp_path / "latest.jpg"

    argv = build_ffmpeg_argv(FAKE_RTSP_VALUE, output_path, DecodeMode.VAAPI)

    assert "format=nv12|vaapi,hwdownload,format=nv12" in argv
    assert argv.index("-vf") < argv.index("-frames:v")


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
    assert result.selected_mode is DecodeMode.VAAPI
    assert result.duration_seconds >= 0
    assert result.byte_size == len(jpeg_bytes())
    assert calls and isinstance(calls[0], list)
    assert Path(calls[0][-1]).parent == tmp_path
    assert Path(calls[0][-1]) != result.latest_path
    assert not Path(calls[0][-1]).exists()


def test_named_profile_publishes_separate_latest_path(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  reconnect_seconds: 5\n",
        "  reconnect_seconds: 5\n  profiles:\n    high_resolution:\n      rtsp_url_env: RTSP_URL_4K\n      frame_width: 3840\n      frame_height: 2160\n",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config, encoding="utf-8")
    high_url = f"rtsps://high-camera/{SECRET_MARKER}"
    settings = load_settings(
        config_path,
        environ={
            "RTSP_URL": FAKE_RTSP_VALUE,
            "RTSP_URL_4K": high_url,
            "RTSP_URL_360P": f"{FAKE_RTSP_VALUE}/360p",
            "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_VALUE,
        },
    )
    calls: list[Sequence[str]] = []

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        Path(argv[-1]).write_bytes(jpeg_bytes(size=(3840, 2160)))
        return subprocess.CompletedProcess(argv, 0, stderr="captured high-res frame")

    result = capture_latest(settings, tmp_path, stream_profile="high_resolution", modes=[DecodeMode.SOFTWARE], runner=runner)

    assert result.latest_path == tmp_path / "latest-high_resolution.jpg"
    assert result.latest_path.read_bytes() == jpeg_bytes(size=(3840, 2160))
    assert not (tmp_path / "latest.jpg").exists()
    assert result.frame_geometry == FrameGeometry(stream_profile="high_resolution", expected_size=(3840, 2160))
    assert high_url in calls[0]
    assert FAKE_RTSP_VALUE not in calls[0]
    assert result.diagnostics()["stream_profile"] == "high_resolution"


def test_named_profile_sanitizes_published_filename(tmp_path: Path) -> None:
    profile_name = "High Resolution / 4K"
    config = (
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("  escalation_profile: high_resolution\n", f'  escalation_profile: "{profile_name}"\n')
        .replace("    high_resolution:\n", f'    "{profile_name}":\n')
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config, encoding="utf-8")
    settings = load_settings(
        config_path,
        environ={
            "RTSP_URL": FAKE_RTSP_VALUE,
            "RTSP_URL_4K": f"{FAKE_RTSP_VALUE}/4k",
            "RTSP_URL_360P": f"{FAKE_RTSP_VALUE}/360p",
            "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_VALUE,
        },
    )

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        Path(argv[-1]).write_bytes(jpeg_bytes(size=(3840, 2160)))
        return subprocess.CompletedProcess(argv, 0, stderr="captured sanitized profile")

    result = capture_latest(settings, tmp_path, stream_profile=profile_name, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert result.latest_path == tmp_path / "latest-High-Resolution-4K.jpg"
    assert result.latest_path.read_bytes() == jpeg_bytes(size=(3840, 2160))
    assert not (tmp_path / "latest.jpg").exists()


def test_invalid_capture_preserves_previous_published_frame(tmp_path: Path) -> None:
    settings = fake_settings()
    published = tmp_path / "latest.jpg"
    published.write_bytes(jpeg_bytes())

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        Path(argv[-1]).write_bytes(b"not-a-jpeg")
        return subprocess.CompletedProcess(argv, 0, stderr="invalid frame")

    with pytest.raises(CaptureError):
        capture_latest(settings, tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert published.read_bytes() == jpeg_bytes()
    assert not list(tmp_path.glob(".latest.*.jpg"))


def test_capture_rejects_wrong_dimensions_and_preserves_previous_frame(tmp_path: Path) -> None:
    settings = fake_settings()
    published = tmp_path / "latest.jpg"
    previous_frame = jpeg_bytes()
    published.write_bytes(previous_frame)

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        Path(argv[-1]).write_bytes(jpeg_bytes(size=(32, 32)))
        return subprocess.CompletedProcess(argv, 0, stderr="ok")

    with pytest.raises(CaptureError) as raised:
        capture_latest(settings, tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == "output-dimensions-mismatch"
    assert published.read_bytes() == previous_frame


def test_capture_validates_encoded_file_at_exactly_literal_32_mib_before_rejecting_invalid_jpeg(
    tmp_path: Path,
) -> None:
    published = tmp_path / "latest.jpg"
    previous_frame = jpeg_bytes()
    published.write_bytes(previous_frame)

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        with Path(argv[-1]).open("wb") as sparse_output:
            sparse_output.truncate(32 * 1024 * 1024)
        return subprocess.CompletedProcess(argv, 0, stderr="ok")

    with pytest.raises(CaptureError) as raised:
        capture_latest(fake_settings(), tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == "output-invalid-jpeg"
    assert published.read_bytes() == previous_frame
    assert not list(tmp_path.glob(".latest.*.jpg"))


def test_capture_rejects_encoded_file_one_byte_over_literal_32_mib(
    tmp_path: Path,
) -> None:
    published = tmp_path / "latest.jpg"
    previous_frame = jpeg_bytes()
    published.write_bytes(previous_frame)

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        with Path(argv[-1]).open("wb") as sparse_output:
            sparse_output.truncate(32 * 1024 * 1024 + 1)
        return subprocess.CompletedProcess(argv, 0, stderr="ok")

    with pytest.raises(CaptureError) as raised:
        capture_latest(fake_settings(), tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == "output-too-large"
    assert published.read_bytes() == previous_frame
    assert not list(tmp_path.glob(".latest.*.jpg"))


def test_capture_rejects_marker_wrapped_non_image_and_preserves_previous_frame(tmp_path: Path) -> None:
    payload = b"\xff\xd8" + (b"x" * 1024) + b"\xff\xd9"
    published = tmp_path / "latest.jpg"
    previous_frame = jpeg_bytes()
    published.write_bytes(previous_frame)

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        Path(argv[-1]).write_bytes(payload)
        return subprocess.CompletedProcess(argv, 0, stderr="captured frame")

    with pytest.raises(CaptureError) as raised:
        capture_latest(fake_settings(), tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == "output-invalid-jpeg"
    assert published.read_bytes() == previous_frame


def test_capture_rejects_truncated_jpeg_and_preserves_previous_frame(tmp_path: Path) -> None:
    published = tmp_path / "latest.jpg"
    previous_frame = jpeg_bytes()
    published.write_bytes(previous_frame)

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        Path(argv[-1]).write_bytes(jpeg_bytes()[:-2])
        return subprocess.CompletedProcess(argv, 0, stderr="captured truncated frame")

    with pytest.raises(CaptureError) as raised:
        capture_latest(fake_settings(), tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == "output-invalid-jpeg"
    assert published.read_bytes() == previous_frame
    assert not list(tmp_path.glob(".latest.*.jpg"))


def test_capture_rejects_decompression_bomb_warning_and_preserves_previous_frame(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1_000_000)
    published = tmp_path / "latest.jpg"
    previous_frame = jpeg_bytes()
    published.write_bytes(previous_frame)

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        Path(argv[-1]).write_bytes(jpeg_bytes())
        return subprocess.CompletedProcess(argv, 0, stderr="captured frame")

    with pytest.raises(CaptureError) as raised:
        capture_latest(fake_settings(), tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == "output-invalid-jpeg"
    assert published.read_bytes() == previous_frame


def test_capture_decompression_bomb_handling_does_not_change_global_warning_filters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1_000_000)
    filters_before = list(warnings.filters)

    def runner(argv: Sequence[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        Path(argv[-1]).write_bytes(jpeg_bytes())
        return subprocess.CompletedProcess(argv, 0, stderr="captured frame")

    with pytest.raises(CaptureError):
        capture_latest(fake_settings(), tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert warnings.filters == filters_before


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
        elif "drm" in argv:
            mode = DecodeMode.DRM
        attempted.append(mode)
        if mode is not DecodeMode.SOFTWARE:
            return subprocess.CompletedProcess(argv, 1, stderr=f"failed {FAKE_RTSP_VALUE}")
        Path(argv[-1]).write_bytes(jpeg_bytes())
        return subprocess.CompletedProcess(argv, 0, stderr="software ok")

    result = capture_latest(settings, tmp_path, logger=logger, runner=runner)

    assert result.selected_mode is DecodeMode.SOFTWARE
    assert attempted == [DecodeMode.VAAPI, DecodeMode.DRM, DecodeMode.SOFTWARE]
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
