from __future__ import annotations

import json

import errno

import math

import os

import stat

import tarfile

import threading

from io import BytesIO, StringIO

from pathlib import Path

from types import MappingProxyType

from typing import Any

import pytest

from PIL import Image

from parking_spot_monitor import (
    file_descriptor_binding,
    jpeg_artifacts,
    owned_file_cleanup,
    owned_file_disposal,
    owned_disposal_manifest,
    owned_file_recovery,
    vehicle_history_corrections,
    vehicle_history_images,
    vehicle_history_storage,
)

from parking_spot_monitor.logging import setup_logging

from parking_spot_monitor.jpeg_artifacts import JpegDecodeError, publish_canonical_jpeg

from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus

from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleRuntimeCache

from parking_spot_monitor.vehicle_history import (
    ArchiveSchemaError,
    ArchiveWriteError,
    CorrectionReplayState,
    VehicleHistoryArchive,
    cutoff_older_than_days,
    estimate_profile_history,
    estimate_session_history,
)

from parking_spot_monitor.vehicle_history_images import (
    ClampedCropBox,
    VehicleHistoryImageError,
    capture_occupied_images,
    clamp_crop_box,
)

from parking_spot_monitor.vehicle_profiles import MatchResult, MatchStatus

def logger_records(stream: StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]

def occupied_event(
    *,
    spot_id: str = "left spot/1",
    observed_at: str = "2026-05-18T13:00:00Z",
    snapshot_path: str = "/data/snapshots/start.jpg",
    candidate_summary: dict[str, Any] | None = None,
) -> OccupancyEvent:
    return OccupancyEvent(
        event_type=OccupancyEventType.STATE_CHANGED,
        spot_id=spot_id,
        previous_status=OccupancyStatus.EMPTY,
        new_status=OccupancyStatus.OCCUPIED,
        observed_at=observed_at,
        source_timestamp=None,
        snapshot_path=snapshot_path,
        candidate_summary=candidate_summary if candidate_summary is not None else {"score": 0.97, "bbox": [1, 2, 3, 4]},
    )

def open_event(
    *,
    spot_id: str = "left spot/1",
    observed_at: str = "2026-05-18T13:04:30Z",
    snapshot_path: str = "/data/snapshots/end.jpg",
) -> OccupancyEvent:
    return OccupancyEvent(
        event_type=OccupancyEventType.OPEN_EVENT,
        spot_id=spot_id,
        previous_status=OccupancyStatus.OCCUPIED,
        new_status=OccupancyStatus.EMPTY,
        observed_at=observed_at,
        source_timestamp=None,
        snapshot_path=snapshot_path,
        candidate_summary=None,
    )

def set_session_profile(
    root: Path,
    *,
    archive_state: str,
    session_id: str,
    profile_id: str | None,
    profile_confidence: float | None,
) -> None:
    path = root / "vehicle-history" / "sessions" / archive_state / f"{session_id}.json"
    payload = json.loads(path.read_text())
    payload["profile_id"] = profile_id
    payload["profile_confidence"] = profile_confidence
    path.write_text(json.dumps(payload, allow_nan=False))

def write_test_jpeg(path: Path, *, size: tuple[int, int] = (8, 6), color: tuple[int, int, int] = (10, 80, 140)) -> Path:
    Image.new("RGB", size, color).save(path, format="JPEG")
    return path

def write_owned_temporary(owner: Any, name: str, source_fd: int, mode: int) -> None:
    descriptor = owner.open_file(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        payload = os.pread(source_fd, os.fstat(source_fd).st_size, 0)
        assert os.write(descriptor, payload) == len(payload)
        os.fchmod(descriptor, mode)
    finally:
        os.close(descriptor)

__all__ = [name for name in globals() if not name.startswith("__")]
