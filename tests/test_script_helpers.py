from __future__ import annotations

import os
from pathlib import Path

import pytest
from PIL import Image

from scripts.closeout_helpers import bounded_text, smoke_env
from scripts.matrix_readback import fetch_matrix_room_messages
from scripts.verification_helpers import jpeg_check, load_result_json


class VerificationError(RuntimeError):
    pass


def test_jpeg_check_reports_valid_missing_and_corrupt(tmp_path: Path) -> None:
    valid = tmp_path / "valid.jpg"
    Image.new("RGB", (8, 6)).save(valid, "JPEG")
    corrupt = tmp_path / "corrupt.jpg"
    corrupt.write_bytes(b"bad")

    assert jpeg_check(valid) == {
        "path": str(valid),
        "exists": True,
        "byte_size": valid.stat().st_size,
        "valid_jpeg": True,
        "format": "JPEG",
        "width": 8,
        "height": 6,
    }
    assert jpeg_check(tmp_path / "missing.jpg") == {
        "path": str(tmp_path / "missing.jpg"),
        "exists": False,
        "byte_size": 0,
        "valid_jpeg": False,
        "error_type": "missing",
    }
    corrupt_result = jpeg_check(corrupt)
    assert corrupt_result["path"] == str(corrupt)
    assert corrupt_result["exists"] is True
    assert corrupt_result["byte_size"] == 3
    assert corrupt_result["valid_jpeg"] is False
    assert corrupt_result["error_type"] == "UnidentifiedImageError"


def test_smoke_env_uses_safe_default_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", "/bin")
    monkeypatch.setenv("HOME", "/home/operator")
    monkeypatch.setenv("MATRIX_SECRET", "secret-token")
    monkeypatch.setenv("PYTHONPATH", "/unsafe")

    env = smoke_env(rtsp_placeholder="rtsp://placeholder", matrix_token_placeholder="matrix-placeholder")

    assert env["PATH"] == "/bin"
    assert env["HOME"] == "/home/operator"
    assert env["RTSP_URL"] == "rtsp://placeholder"
    assert env["MATRIX_ACCESS_TOKEN"] == "matrix-placeholder"
    assert "MATRIX_SECRET" not in env
    assert "PYTHONPATH" not in env


def test_smoke_env_keeps_only_explicit_passthrough_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    passthrough_keys = (
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "DOCKER_CONFIG",
        "XDG_RUNTIME_DIR",
        "BUILDKIT_HOST",
        "DOCKER_BUILDKIT",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    )
    monkeypatch.setenv("PATH", "/bin")
    monkeypatch.setenv("HOME", "/home/operator")
    for key in passthrough_keys:
        monkeypatch.setenv(key, f"value-for-{key}")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "should-not-pass")
    monkeypatch.setenv("MATRIX_SECRET", "should-not-pass")
    monkeypatch.setenv("RTSP_URL", "rtsp://real-camera")
    monkeypatch.setenv("MATRIX_ACCESS_TOKEN", "real-matrix-token")
    monkeypatch.setenv("PYTHONPATH", "/unsafe")

    env = smoke_env(
        rtsp_placeholder="rtsp://placeholder",
        matrix_token_placeholder="matrix-placeholder",
        pythonpath_prefix="/repo",
        passthrough_keys=passthrough_keys,
    )

    assert env["PATH"] == "/bin"
    assert env["HOME"] == "/home/operator"
    assert {key: env[key] for key in passthrough_keys} == {
        key: f"value-for-{key}" for key in passthrough_keys
    }
    assert "AWS_SECRET_ACCESS_KEY" not in env
    assert "MATRIX_SECRET" not in env
    assert env["RTSP_URL"] == "rtsp://placeholder"
    assert env["RTSP_URL_4K"] == "rtsp://placeholder-4k"
    assert env["RTSP_URL_360P"] == "rtsp://placeholder-360p"
    assert env["MATRIX_ACCESS_TOKEN"] == "matrix-placeholder"
    assert env["PYTHONPATH"] == "/repo"


def test_smoke_env_keeps_explicit_base_pythonpath() -> None:
    env = smoke_env(
        rtsp_placeholder="rtsp://placeholder",
        matrix_token_placeholder="matrix-placeholder",
        base={"PYTHONPATH": "/already-there", "CUSTOM": "ok"},
        pythonpath_prefix="/repo",
    )

    assert env["CUSTOM"] == "ok"
    assert env["PYTHONPATH"] == f"/repo{os.pathsep}/already-there"


def test_bounded_text_never_exceeds_limit_when_marker_is_longer_than_limit() -> None:
    assert len(bounded_text("x" * 200, limit=8)) <= 8
    assert bounded_text("x" * 200, limit=0) == ""


@pytest.mark.parametrize("homeserver", ["", "file:///tmp/matrix", "ftp://example.test", "example.test"])
def test_fetch_matrix_room_messages_rejects_non_http_homeservers(homeserver: str) -> None:
    with pytest.raises(ValueError, match="matrix_readback_invalid_homeserver"):
        fetch_matrix_room_messages(
            homeserver=homeserver,
            room_id="!room:example",
            access_token="token",
            timeout_seconds=1,
            limit=1,
        )


def test_load_result_json_wraps_other_filesystem_errors(tmp_path: Path) -> None:
    with pytest.raises(VerificationError, match="result JSON cannot be read"):
        load_result_json(tmp_path, VerificationError)
