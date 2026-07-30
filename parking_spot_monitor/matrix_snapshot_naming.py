"""Deterministic names and descriptions for Matrix snapshot evidence."""

from __future__ import annotations

import os
from pathlib import Path
import re

from parking_spot_monitor.logging import redact_diagnostic_text
from parking_spot_monitor.matrix_support import _require_non_empty
from parking_spot_monitor.matrix_time import format_observed_at


def event_snapshot_path(
    *,
    data_dir: str | Path,
    snapshots_dir: str | Path | None,
    event_type: str,
    event_id: str,
    spot_id: str | None,
    observed_at: object,
) -> Path:
    root = Path(snapshots_dir) if snapshots_dir is not None else Path(data_dir) / "snapshots"
    filename = _snapshot_filename(
        event_type=_require_non_empty("event_type", event_type),
        stable_id=spot_id or _require_non_empty("event_id", event_id),
        observed_at=format_observed_at(observed_at),
    )
    return Path(os.path.abspath(root)) / filename


def snapshot_body(*, spot_id: str | None, observed_at: str) -> str:
    subject = redact_diagnostic_text(spot_id) if spot_id else "parking spot"
    return f"Raw full-frame snapshot for {subject} at {observed_at.replace('Z', '+00:00')}"


def _snapshot_filename(*, event_type: str, stable_id: str, observed_at: str) -> str:
    return f"{_path_token(event_type)}-{_path_token(stable_id)}-{_path_token(observed_at)}.jpg"


def _path_token(value: object) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "-", redact_diagnostic_text(value).strip().lower()).strip("-")
    return token or "unknown"
