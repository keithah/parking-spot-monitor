from __future__ import annotations

import math
import os
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import redact_diagnostic_value
from parking_spot_monitor.vehicle_history_models import ArchiveSchemaError, SessionRecord, _parse_timestamp


def _profile_quarantine_count(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(1 for path in directory.glob("*.corrupt-*") if path.is_file())


def _archive_files_for_export(root: Path, output: Path) -> list[Path]:
    if not root.exists():
        return []
    resolved_output = _safe_resolve(output)
    files: list[Path] = []
    stack = [root]
    while stack:
        current = stack.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                path = Path(entry.path)
                if entry.is_dir(follow_symlinks=False):
                    stack.append(path)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                if _safe_resolve(path) == resolved_output:
                    continue
                files.append(path)
    return sorted(files, key=lambda path: _archive_member_name(root, path))


def _archive_member_name(root: Path, path: Path) -> str:
    return f"vehicle-history/{path.relative_to(root).as_posix()}"


def _safe_file_size(path: Path) -> int:
    try:
        return path.stat().st_size if path.is_file() else 0
    except OSError:
        return 0


def _maintenance_stamp(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "-", value).strip("-").lower() or "unknown"


def _coerce_cutoff_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = _parse_timestamp(value)
        if parsed is None:
            raise ArchiveSchemaError("cutoff must be an ISO timestamp")
    else:
        raise ArchiveSchemaError("cutoff must be an ISO timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def cutoff_older_than_days(days: int, *, now: datetime | None = None) -> datetime:
    if isinstance(days, bool) or not isinstance(days, int) or days < 0:
        raise ArchiveSchemaError("older-than-days must be a non-negative integer")
    reference = now if now is not None else datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return reference.astimezone(timezone.utc) - timedelta(days=days)


def _record_closed_before(record: SessionRecord, cutoff: datetime) -> bool:
    if record.ended_at is None:
        return False
    ended_at = _parse_timestamp(record.ended_at)
    if ended_at is None:
        return False
    return ended_at.astimezone(timezone.utc) < cutoff


def _referenced_archive_paths(root: Path, records: Sequence[SessionRecord]) -> set[Path]:
    paths: set[Path] = set()
    for record in records:
        paths.update(_record_archive_image_paths(root, record))
    return paths


def _record_archive_image_paths(root: Path, record: SessionRecord) -> set[Path]:
    paths: set[Path] = set()
    for value in (record.occupied_snapshot_path, record.occupied_crop_path):
        path = _archive_local_path(root, value)
        if path is not None:
            paths.add(path)
    return paths


def _archive_local_path(root: Path, value: str | None) -> Path | None:
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    resolved_root = _safe_resolve(root)
    resolved_path = _safe_resolve(path)
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError:
        return None
    return resolved_path


def _safe_resolve(path: Path) -> Path:
    try:
        return path.resolve(strict=False)
    except OSError:
        return path.absolute()


def _maintenance_log_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    blocked = {"member_names", "output_path", "manifest_path"}
    safe: dict[str, Any] = {}
    for key, value in payload.items():
        if key in blocked:
            continue
        safe[key] = _json_scalar_or_collection(redact_diagnostic_value(value))
    return safe


def _archive_directory_stats(directory: Path) -> tuple[int, int]:
    if not directory.exists():
        return (0, 0)
    count = 0
    total_bytes = 0
    stack = [directory]
    while stack:
        current = stack.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                    continue
                if entry.is_file(follow_symlinks=False):
                    stat_result = entry.stat(follow_symlinks=False)
                    count += 1
                    total_bytes += stat_result.st_size
    return (count, total_bytes)


@dataclass
class SessionHealthAccumulator:
    count: int = 0
    oldest_started_at: str | None = None
    missing_refs: int = 0
    profile_unknown_sessions: int = 0
    _oldest_timestamp: datetime | None = field(default=None, init=False, repr=False)
    _oldest_order_key: tuple[int, str] | None = field(default=None, init=False, repr=False)

    def add(self, record: SessionRecord, *, archive_order: int = 0) -> None:
        self.count += 1
        self.missing_refs += int(record.occupied_snapshot_path is None or record.occupied_crop_path is None)
        self.profile_unknown_sessions += int(record.occupied_crop_path is not None and record.profile_id is None)
        parsed = _parse_timestamp(record.started_at)
        order_key = (archive_order, record.session_id)
        if parsed is not None and (
            self._oldest_timestamp is None
            or parsed < self._oldest_timestamp
            or (parsed == self._oldest_timestamp and (self._oldest_order_key is None or order_key < self._oldest_order_key))
        ):
            self._oldest_timestamp = parsed
            self._oldest_order_key = order_key
            self.oldest_started_at = record.started_at

    def to_json(self) -> dict[str, int | str | None]:
        return {
            "count": self.count,
            "missing_refs": self.missing_refs,
            "profile_unknown_sessions": self.profile_unknown_sessions,
            "oldest_started_at": self.oldest_started_at,
        }


def _session_health_stats(records: Iterable[SessionRecord]) -> dict[str, int | str | None]:
    accumulator = SessionHealthAccumulator()
    for record in records:
        accumulator.add(record)
    stats = accumulator.to_json()
    del stats["count"]
    return stats


def _safe_maintenance_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    allowed_keys = {
        "operation",
        "action",
        "status",
        "result",
        "started_at",
        "completed_at",
        "created_at",
        "updated_at",
        "retention_policy",
        "archive_file_count",
        "archive_bytes",
        "file_count",
        "bytes",
        "pruned_file_count",
        "export_file_count",
        "dry_run",
    }
    safe: dict[str, Any] = {}
    for key in allowed_keys:
        if key in payload:
            safe[key] = _json_scalar_or_collection(redact_diagnostic_value(payload[key]))
    return safe


def _json_scalar_or_collection(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value if not isinstance(value, float) or math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _json_scalar_or_collection(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_scalar_or_collection(item) for item in value]
    return str(value)


def _image_directory_stats(directory: Path) -> tuple[int, int]:
    count = 0
    total_bytes = 0
    for path in directory.glob("*.jpg"):
        try:
            stat_result = path.stat()
        except OSError:
            continue
        if path.is_file():
            count += 1
            total_bytes += stat_result.st_size
    return (count, total_bytes)
