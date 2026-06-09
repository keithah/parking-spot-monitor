from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_TIMELINE_INTERVAL_SECONDS = 60
DEFAULT_TIMELINE_RETENTION_HOURS = 12
TIMELINE_DIRNAME = "timeline"
TIMELINE_FRAMES_DIRNAME = "frames"


@dataclass(frozen=True)
class TimelineFrameResult:
    """Safe metadata for one rolling timeline frame retention attempt."""

    saved: bool
    path: Path | None
    reason: str | None = None
    byte_size: int = 0
    pruned_count: int = 0
    pruned_bytes: int = 0

    def diagnostics(self) -> dict[str, Any]:
        return {
            "saved": self.saved,
            "path": str(self.path) if self.path is not None else None,
            "reason": self.reason,
            "byte_size": self.byte_size,
            "pruned_count": self.pruned_count,
            "pruned_bytes": self.pruned_bytes,
        }


def record_timeline_frame(
    source_path: str | Path,
    *,
    data_dir: str | Path,
    observed_at: datetime | str | None,
    retention_hours: int = DEFAULT_TIMELINE_RETENTION_HOURS,
) -> TimelineFrameResult:
    """Copy a raw frame into a bounded one-frame-per-minute timeline buffer.

    The buffer is intentionally small and local: one JPEG per UTC minute under
    ``<data_dir>/timeline/frames`` and pruning by the timestamp encoded in the
    filename. Existing minute samples are left untouched so a faster runtime loop
    cannot overwrite the evidence for that minute.
    """

    source = Path(source_path)
    if not source.exists():
        return TimelineFrameResult(saved=False, path=None, reason="source-missing")

    observed = _coerce_utc(observed_at)
    frames_dir = Path(data_dir) / TIMELINE_DIRNAME / TIMELINE_FRAMES_DIRNAME
    frame_name = _minute_frame_name(observed)
    target = frames_dir / frame_name

    pruned_count = 0
    pruned_bytes = 0
    if target.exists():
        return TimelineFrameResult(
            saved=False,
            path=target,
            reason="already-sampled",
            byte_size=target.stat().st_size,
            pruned_count=pruned_count,
            pruned_bytes=pruned_bytes,
        )

    try:
        frames_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        byte_size = target.stat().st_size
        pruned_count, pruned_bytes = _prune_old_frames(frames_dir, observed=observed, retention_hours=retention_hours)
    except OSError:
        return TimelineFrameResult(saved=False, path=target, reason="timeline-unavailable")
    return TimelineFrameResult(saved=True, path=target, byte_size=byte_size, pruned_count=pruned_count, pruned_bytes=pruned_bytes)


def _minute_frame_name(value: datetime) -> str:
    minute = value.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return minute.strftime("%Y%m%dT%H%M%SZ.jpg")


def _coerce_utc(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        selected = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return selected.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        normalized = value.strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        selected = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        return selected.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _prune_old_frames(frames_dir: Path, *, observed: datetime, retention_hours: int) -> tuple[int, int]:
    cutoff = observed.astimezone(timezone.utc) - timedelta(hours=max(1, retention_hours))
    pruned_count = 0
    pruned_bytes = 0
    for path in frames_dir.glob("*.jpg"):
        timestamp = _timestamp_from_frame_name(path.name)
        if timestamp is None or timestamp >= cutoff:
            continue
        try:
            size = path.stat().st_size
            path.unlink()
        except OSError:
            continue
        pruned_count += 1
        pruned_bytes += size
    return pruned_count, pruned_bytes


def _timestamp_from_frame_name(name: str) -> datetime | None:
    if not name.endswith("Z.jpg"):
        return None
    try:
        return datetime.strptime(name, "%Y%m%dT%H%M%SZ.jpg").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
