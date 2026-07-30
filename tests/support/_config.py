from __future__ import annotations

from pathlib import Path

import pytest

from parking_spot_monitor.config import load_settings

from parking_spot_monitor.errors import ConfigError

from parking_spot_monitor.paths import resolve_runtime_paths

SECRET_MARKER = "should-not-leak"

FAKE_RTSP_URL = f"camera-secret-{SECRET_MARKER}"

FAKE_MATRIX_TOKEN = f"matrix-secret-{SECRET_MARKER}"

def fake_environ(**overrides: str) -> dict[str, str]:
    environ = {
        "RTSP_URL": FAKE_RTSP_URL,
        "RTSP_URL_4K": f"{FAKE_RTSP_URL}-4k",
        "RTSP_URL_360P": f"{FAKE_RTSP_URL}-360p",
        "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN,
    }
    environ.update(overrides)
    return environ

def write_config(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(content, encoding="utf-8")
    return path

__all__ = [name for name in globals() if not name.startswith("__")]
