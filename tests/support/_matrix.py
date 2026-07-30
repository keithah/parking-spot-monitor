from __future__ import annotations

import json

import os

from datetime import datetime, timezone

from io import BytesIO, StringIO

from pathlib import Path

from typing import Any

import httpx

import pytest

from PIL import Image

import parking_spot_monitor.matrix_snapshots as matrix_snapshots

import parking_spot_monitor.matrix_snapshot_storage as matrix_snapshot_storage

import parking_spot_monitor.matrix_retained_publication as matrix_retained_publication

import parking_spot_monitor.file_descriptor_binding as file_descriptor_binding

import parking_spot_monitor.jpeg_artifacts as jpeg_artifacts

import parking_spot_monitor.owned_file_disposal as owned_file_disposal

import parking_spot_monitor.owned_directory_durability as owned_directory_durability

from parking_spot_monitor.detector_adapter import adapt_detector

from parking_spot_monitor.image_budget import ImageBudgetError, JpegBudgetResult

from parking_spot_monitor.jpeg_artifacts import JpegDecodeError

from parking_spot_monitor.logging import StructuredLogger

from parking_spot_monitor.matrix_models import MatrixTextEvent

from parking_spot_monitor.matrix import (
    MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
    MONITOR_STARTED_EVENT_TYPE,
    OCCUPIED_SPOT_EVENT_TYPE,
    OPEN_SPOT_EVENT_TYPE,
    MatrixClient,
    MatrixDelivery,
    MatrixError,
    format_lifecycle_notice,
    format_live_proof_text,
    format_occupied_spot_alert,
    format_open_spot_alert,
    format_quiet_window_notice,
    format_owner_vehicle_quiet_window_alert,
    monitor_lifecycle_event,
    monitor_lifecycle_event_id,
    owner_vehicle_quiet_window_event_id,
    occupied_spot_event_id,
    prepare_event_snapshot,
    prune_event_snapshots,
)

ACCESS_TOKEN = "secret-token-value"

def stream_env(rtsp_url: str = "rtsp://operator:secret@camera/live") -> dict[str, str]:
    return {
        "RTSP_URL": rtsp_url,
        "RTSP_URL_4K": f"{rtsp_url}/4k",
        "RTSP_URL_360P": f"{rtsp_url}/360p",
        "MATRIX_ACCESS_TOKEN": ACCESS_TOKEN,
    }

HOMESERVER = "https://matrix.example.org/"

ROOM_ID = "!parking-room:example.org"

TXN_ID = "txn/with space?"

def make_client(handler: httpx.MockTransport) -> MatrixClient:
    http_client = httpx.Client(transport=handler)
    return MatrixClient(homeserver=HOMESERVER, access_token=ACCESS_TOKEN, timeout_seconds=2, http_client=http_client)

def request_json(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content.decode("utf-8"))

def write_jpeg(
    path: Path,
    *,
    size: tuple[int, int] = (4, 3),
    color: tuple[int, int, int] = (25, 50, 75),
) -> bytes:
    image = Image.new("RGB", size, color=color)
    image.save(path, format="JPEG")
    return path.read_bytes()

def open_event(snapshot_path: Path | str = "unused.jpg") -> dict[str, Any]:
    return {
        "event_type": OPEN_SPOT_EVENT_TYPE,
        "spot_id": "left_spot",
        "previous_status": "occupied",
        "new_status": "empty",
        "observed_at": datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc),
        "snapshot_path": str(snapshot_path),
    }

def occupied_event(snapshot_path: Path | str = "unused.jpg") -> dict[str, Any]:
    return {
        "event_type": OCCUPIED_SPOT_EVENT_TYPE,
        "spot_id": "left_spot",
        "observed_at": datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc),
        "occupied_snapshot_path": str(snapshot_path),
        "likely_vehicle": {
            "label": "silver hatchback",
            "profile_id": "prof_repeat",
            "match_status": "matched",
            "confidence": 0.92,
        },
        "vehicle_history_estimate": {
            "status": "estimated",
            "profile_id": "prof_repeat",
            "sample_count": 4,
            "confidence": "medium",
            "dwell_range": {"lower_seconds": 3600, "upper_seconds": 5400, "typical_seconds": 4500},
            "leave_time_window": {
                "start_minute": 23 * 60 + 45,
                "end_minute": 15,
                "typical_minute": 0,
                "crosses_midnight": True,
            },
        },
    }

class FakeCorrection:
    def __init__(self, correction_id: str = "corr_1", matrix_event_id: str | None = None) -> None:
        self.correction_id = correction_id
        self.matrix_event_id = matrix_event_id

class FakeSession:
    def __init__(self, session_id: str, spot_id: str = "left_spot") -> None:
        self.session_id = session_id
        self.spot_id = spot_id
        self.started_at = session_id
        self.ended_at = session_id

class FakeCommandArchive:
    def __init__(self, cursor: dict[str, str] | None = None) -> None:
        self.cursor = cursor
        self.cursor_writes: list[dict[str, str]] = []
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.corrections: list[FakeCorrection] = []
        self.sessions = [FakeSession("sess_current", "left_spot")]

    def read_matrix_cursor(self) -> dict[str, str] | None:
        return self.cursor

    def write_matrix_cursor(self, state: dict[str, str]) -> None:
        self.cursor_writes.append(state)
        self.cursor = state

    def correction_event_seen(self, event_id: str) -> bool:
        return any(correction.matrix_event_id == event_id for correction in self.corrections)

    def rename_profile(self, *args: Any, **kwargs: Any) -> FakeCorrection:
        self.calls.append(("rename_profile", args, kwargs))
        correction = FakeCorrection("rename_1", kwargs.get("matrix_event_id"))
        self.corrections.append(correction)
        return correction

    def merge_profiles(self, *args: Any, **kwargs: Any) -> FakeCorrection:
        self.calls.append(("merge_profiles", args, kwargs))
        correction = FakeCorrection("merge_1", kwargs.get("matrix_event_id"))
        self.corrections.append(correction)
        return correction

    def mark_wrong_match(self, *args: Any, **kwargs: Any) -> FakeCorrection:
        self.calls.append(("mark_wrong_match", args, kwargs))
        correction = FakeCorrection("wrong_1", kwargs.get("matrix_event_id"))
        self.corrections.append(correction)
        return correction

    def profile_summary(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("profile_summary", args, kwargs))
        correction = FakeCorrection("summary_1", kwargs.get("matrix_event_id"))
        self.corrections.append(correction)
        return {"profile_id": args[0], "label": "Blue hatchback", "closed_session_count": 2, "active_session_count": 1, "wrong_match_excluded_session_count": 0, "estimate_status": "estimated", "estimate_sample_count": 3}

    def assign_owner_profile_to_active_spot(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("assign_owner_profile_to_active_spot", args, kwargs))
        return type("Assignment", (), {"session_id": "sess_current", "profile_id": "prof_owner", "profile_confidence": 1.0})()

    def active_spot_assignments(self) -> list[dict[str, Any]]:
        self.calls.append(("active_spot_assignments", (), {}))
        return [
            {
                "spot_id": "left_spot",
                "session_id": "sess_left",
                "profile_id": None,
                "profile_label": None,
                "profile_confidence": None,
                "is_owner": False,
                "owner_label": None,
                "profile_sample_count": None,
            },
            {
                "spot_id": "right_spot",
                "session_id": "sess_current",
                "profile_id": "prof_owner",
                "profile_label": "Keith's black Tesla",
                "profile_confidence": 1.0,
                "is_owner": True,
                "owner_label": "Keith's black Tesla",
                "profile_sample_count": 7,
            },
        ]

    def load_active_sessions(self) -> list[FakeSession]:
        return self.sessions

    def list_closed_sessions(self) -> list[FakeSession]:
        return []

    def resolve_wrong_match_subject(self, subject_id: str) -> str:
        for record in self.sessions:
            if record.session_id == subject_id:
                return subject_id
        matches = [record for record in self.sessions if record.spot_id == subject_id]
        return subject_id if not matches else matches[-1].session_id

__all__ = [name for name in globals() if not name.startswith("__")]
