from __future__ import annotations

import json

import os

import warnings

from datetime import datetime, timezone

from io import StringIO

from pathlib import Path

from types import SimpleNamespace

from typing import Any

import pytest

from PIL import Image

import parking_spot_monitor.operator_cockpit_snapshots as operator_cockpit_snapshots

from parking_spot_monitor.config import load_settings

from parking_spot_monitor.detector_adapter import adapt_detector

from parking_spot_monitor.health import HealthStatus, write_health_status

from parking_spot_monitor.image_budget import ImageBudgetError, JpegBudgetResult

from parking_spot_monitor.logging import StructuredLogger

from parking_spot_monitor.state import RuntimeState, save_runtime_state

FAKE_RTSP_URL = "rtsp://operator:super-secret@camera.example.local/live"

FAKE_MATRIX_TOKEN = "matrix-token-secret-value"

RAW_IMAGE_MARKER = "RAW-JPEG-BYTES-should-never-appear"

NESTED_SECRET_MARKER = "nested-secret-marker-should-never-appear"

def _settings(tmp_path: Path) -> Any:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
stream:
  rtsp_url_env: RTSP_URL
  frame_width: 1458
  frame_height: 806
  reconnect_seconds: 7
spots:
  left_spot:
    name: Left curb spot
    polygon: [[10, 20], [300, 20], [300, 350], [10, 350]]
  right_spot:
    name: Right curb spot
    polygon: [[350, 20], [700, 20], [700, 350], [350, 350]]
detection:
  model: models/yolo11n.pt
  confidence_threshold: 0.42
  inference_image_size: 960
  spot_crop_inference: true
  spot_crop_margin_px: 32
  open_suppression_min_confidence: 0.18
  vehicle_classes: [car, truck]
  min_bbox_area_px: 1200
  min_polygon_overlap_ratio: 0.27
occupancy:
  iou_threshold: 0.31
  confirm_frames: 4
  release_frames: 5
matrix:
  homeserver: https://matrix.example.invalid
  room_id: "!room:example.invalid"
  access_token_env: MATRIX_ACCESS_TOKEN
  user_id: "@bot:example.invalid"
  command_prefix: "!parking"
  command_authorized_senders: ["@operator:example.invalid"]
  timeout_seconds: 3
  retry_attempts: 2
  retry_backoff_seconds: 0.5
quiet_windows:
  - name: street_sweeping
    timezone: America/Los_Angeles
    recurrence: monthly_weekday
    weekdays: [monday]
    ordinals: [1, 3]
    start: "13:00"
    end: "15:00"
    reminder_minutes_before: 60
storage:
  data_dir: data
  snapshots_dir: snapshots
  snapshot_retention_count: 12
runtime:
  health_file: health.json
  log_level: INFO
  startup_timeout_seconds: 45
  frame_interval_seconds: 12.5
""".lstrip(),
        encoding="utf-8",
    )
    return load_settings(
        config_path,
        environ={"RTSP_URL": FAKE_RTSP_URL, "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN},
    )

def _write_runtime_files(tmp_path: Path) -> tuple[Path, Path]:
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    write_health_status(
        health_path,
        HealthStatus(
            status="degraded",
            updated_at="2026-05-18T19:00:00Z",
            iteration=42,
            last_frame_at="2026-05-18T18:59:50Z",
            selected_decode_mode="software",
            consecutive_capture_failures=1,
            consecutive_detection_failures=2,
            last_matrix_error={
                "error_type": "timeout",
                "diagnostic": FAKE_MATRIX_TOKEN,
                "nested": {"leak": NESTED_SECRET_MARKER},
            },
            last_error={"message": "Traceback: " + FAKE_RTSP_URL},
        ),
    )
    save_runtime_state(state_path, RuntimeState.default(["left_spot", "right_spot"]))
    return health_path, state_path

def _assert_no_sensitive_text(rendered: str) -> None:
    assert FAKE_RTSP_URL not in rendered
    assert FAKE_MATRIX_TOKEN not in rendered
    assert RAW_IMAGE_MARKER not in rendered
    assert NESTED_SECRET_MARKER not in rendered
    assert "Traceback" not in rendered
    assert "super-secret" not in rendered

def _write_test_jpeg(path: Path, *, size: tuple[int, int] = (16, 9)) -> bytes:
    image = Image.new("RGB", size, color=(12, 34, 56))
    image.save(path, format="JPEG")
    return path.read_bytes()

def _write_incident_runtime_state(path: Path, *, corrupt: bool = False) -> str:
    if corrupt:
        payload = "not json " + FAKE_MATRIX_TOKEN + " " + RAW_IMAGE_MARKER
        path.write_text(payload, encoding="utf-8")
        return payload
    payload = {
        "schema_version": 1,
        "spots": {
            "left_spot": {
                "status": "occupied",
                "hit_streak": 4,
                "miss_streak": 0,
                "open_event_emitted": False,
                "last_bbox": [30, 40, 260, 320],
            },
            "right_spot": {
                "status": "empty",
                "hit_streak": 0,
                "miss_streak": 5,
                "open_event_emitted": True,
            },
        },
    }
    rendered = json.dumps(payload, sort_keys=True)
    path.write_text(rendered, encoding="utf-8")
    return rendered

def _write_incident_timeline_frame(tmp_path: Path, name: str = "20260518T023900Z.jpg", *, corrupt: bool = False) -> Path:
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    frame = frames_dir / name
    if corrupt:
        frame.write_bytes(b"not a jpeg " + RAW_IMAGE_MARKER.encode("utf-8"))
    else:
        _write_test_jpeg(frame, size=(1458, 806))
    return frame

class _IncidentReplayDetector:
    def __init__(self, detections: list[Any] | Exception) -> None:
        self.detections = detections
        self.calls: list[dict[str, Any]] = []

    def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None, inference_image_size: int | None = None) -> list[Any]:
        self.calls.append({"frame_path": Path(frame_path), "confidence_threshold": confidence_threshold, "inference_image_size": inference_image_size})
        if isinstance(self.detections, Exception):
            raise self.detections
        return self.detections

ANALYTICS_CONTRACT_XFAIL = pytest.mark.xfail(
    reason="S03 T03 specifies the future operator analytics cockpit formatter contract.",
    strict=True,
)

def _write_vehicle_history_session(
    data_dir: Path,
    *,
    state: str,
    session_id: str,
    spot_id: str,
    started_at: str,
    ended_at: str | None,
    duration_seconds: int | None,
    secret_marker: str | None = None,
) -> Path:
    session_dir = data_dir / "vehicle-history" / "sessions" / state
    session_dir.mkdir(parents=True, exist_ok=True)
    path = session_dir / f"{session_id}.json"
    payload = {
        "schema_version": 1,
        "session_id": session_id,
        "spot_id": spot_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "start_event": {
            "spot_id": spot_id,
            "event_type": "occupied",
            "observed_at": started_at,
            "snapshot_path": f"snapshots/{session_id}.jpg",
            "diagnostic": secret_marker,
        },
        "close_event": None
        if ended_at is None
        else {
            "spot_id": spot_id,
            "event_type": "open",
            "observed_at": ended_at,
            "snapshot_path": f"snapshots/{session_id}-closed.jpg",
            "diagnostic": secret_marker,
        },
        "source_snapshot_path": f"snapshots/{session_id}.jpg",
        "candidate_summary": {"raw_image": RAW_IMAGE_MARKER, "token": secret_marker},
        "occupied_snapshot_path": None,
        "occupied_crop_path": None,
        "profile_id": None,
        "profile_confidence": None,
        "created_at": started_at,
        "updated_at": ended_at or started_at,
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path

__all__ = [name for name in globals() if not name.startswith("__")]
