from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts.closeout_helpers import bounded_text, smoke_env
from scripts.matrix_readback import fetch_matrix_room_messages
from scripts.verification_helpers import load_result_json


class VerificationError(RuntimeError):
    pass


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
