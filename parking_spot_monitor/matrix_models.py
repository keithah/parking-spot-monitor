from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass(frozen=True)
class MatrixTextEvent:
    """Safe inbound Matrix text event projected from /sync."""

    event_id: str
    sender: str
    room_id: str
    body: str

@dataclass(frozen=True)
class MatrixSyncResult:
    """Safe bounded result from one Matrix /sync poll."""

    next_batch: str
    events: tuple[MatrixTextEvent, ...]

@dataclass(frozen=True)
class MatrixCommand:
    """Parsed operator command with validated, non-secret arguments."""

    action: str
    profile_id: str | None = None
    label: str | None = None
    source_profile_id: str | None = None
    target_profile_id: str | None = None
    subject_id: str | None = None
    spot_id: str | None = None
    actual_state: str | None = None
    lab_kind: str | None = None
    lab_job_id: str | None = None

@dataclass(frozen=True)
class MatrixCommandResponse:
    """Matrix command reply with optional local JPEG media evidence."""

    text: str
    image_path: Path | None = None
    image_info: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class MatrixCommandPollResult:
    """Metadata-only summary of one command poll."""

    next_batch: str
    processed_count: int
    ignored_count: int
    error_count: int
    bootstrapped: bool = False


class MatrixCommandParseError(ValueError):
    """Safe parse error for an inbound Matrix command."""
