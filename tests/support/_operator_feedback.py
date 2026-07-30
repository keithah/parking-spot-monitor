from __future__ import annotations

import json

from collections.abc import Iterable, Mapping, Sequence

from datetime import datetime, timezone

from io import StringIO

from pathlib import Path

from typing import Any

import pytest

from parking_spot_monitor.detector_adapter import adapt_detector

from parking_spot_monitor.logging import StructuredLogger

FAKE_RTSP_URL = "rtsp://user:pass@example.local/live"

FAKE_MATRIX_TOKEN = "syt_secret_matrix_token"

RAW_IMAGE_MARKER = "\xff\xd8\xff\xe0 raw image bytes"

TRACEBACK_TEXT = "Traceback (most recent call last): secret stack"

class CountingSequence(Sequence[object]):
    def __init__(self, values: Iterable[object]) -> None:
        self._values = tuple(values)
        self.consumed = 0

    def __getitem__(self, index):
        return self._values[index]

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self):
        for item in self._values:
            self.consumed += 1
            yield item

class CountingMapping(Mapping[str, object]):
    def __init__(self, values: Iterable[tuple[str, object]]) -> None:
        self._values = dict(values)
        self.consumed = 0

    def __getitem__(self, key: str) -> object:
        return self._values[key]

    def __iter__(self):
        for key in self._values:
            self.consumed += 1
            yield key

    def __len__(self) -> int:
        return len(self._values)

def _assert_no_sensitive_text(rendered: str) -> None:
    assert "user:pass" not in rendered
    assert FAKE_MATRIX_TOKEN not in rendered
    assert RAW_IMAGE_MARKER not in rendered
    assert "\xff\xd8" not in rendered
    assert "\xff" not in rendered
    assert "\\u00ff" not in rendered
    assert "\\u00d8" not in rendered.lower()
    assert "Traceback" not in rendered

def _sample_feedback_label(**overrides):
    from parking_spot_monitor.operator_feedback import FeedbackEvidence, FeedbackLabel

    values = {
        "label_id": "feedback-20260516T174239Z-left_spot-abc12345",
        "spot_id": "left_spot",
        "reported_state": "occupied",
        "actual_state": "open",
        "source": "matrix_command",
        "operator_sender_hash": "sha256:operator",
        "corrected_at": "2026-05-16T17:42:39Z",
        "reported_at": "2026-05-15T21:42:39Z",
        "alert_event_type": "occupancy-occupied-event",
        "alert_event_id": "$alert",
        "evidence": FeedbackEvidence(
            kind="alert_snapshot",
            path="snapshots/occupied.jpg",
            available=True,
            validated_jpeg=True,
            width=11,
            height=7,
            byte_size=633,
            error_type=None,
        ),
        "notes": "",
        "matrix_event_id": "$correct",
    }
    values.update(overrides)
    return FeedbackLabel(**values)

def _write_jpeg(path: Path, *, size: tuple[int, int] = (11, 7)) -> int:
    from PIL import Image

    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", size, color=(128, 64, 32))
    image.save(path, format="JPEG")
    return path.stat().st_size

def _learn_settings(tmp_path: Path) -> Any:
    from parking_spot_monitor.config import load_settings

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
stream:
  rtsp_url_env: RTSP_URL
  frame_width: 1458
  frame_height: 806
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
storage:
  data_dir: data
runtime:
  health_file: health.json
""".lstrip(),
        encoding="utf-8",
    )
    return load_settings(config_path, environ={"RTSP_URL": FAKE_RTSP_URL, "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN})

def _write_learn_timeline_frame(tmp_path: Path, name: str = "20260518T023900Z.jpg", *, corrupt: bool = False) -> Path:
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    frame = frames_dir / name
    if corrupt:
        frame.write_bytes(b"not a jpeg " + RAW_IMAGE_MARKER.encode("utf-8"))
    else:
        _write_jpeg(frame, size=(1458, 806))
    return frame

def _write_learn_state(path: Path, *, corrupt: bool = False) -> str:
    if corrupt:
        payload = "not json " + FAKE_MATRIX_TOKEN + " " + RAW_IMAGE_MARKER
        path.write_text(payload, encoding="utf-8")
        return payload
    payload = {
        "schema_version": 1,
        "spots": {
            "left_spot": {"status": "occupied", "hit_streak": 4, "miss_streak": 0, "open_event_emitted": False},
            "right_spot": {"status": "empty", "hit_streak": 0, "miss_streak": 5, "open_event_emitted": True},
        },
    }
    rendered = json.dumps(payload, sort_keys=True)
    path.write_text(rendered, encoding="utf-8")
    return rendered

class _LearnReplayDetector:
    def __init__(self, detections: list[Any] | Exception) -> None:
        self.detections = detections
        self.calls: list[dict[str, Any]] = []

    def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None, inference_image_size: int | None = None) -> list[Any]:
        self.calls.append({"frame_path": Path(frame_path), "confidence_threshold": confidence_threshold, "inference_image_size": inference_image_size})
        if isinstance(self.detections, Exception):
            raise self.detections
        return self.detections

__all__ = [name for name in globals() if not name.startswith("__")]
