"""Atomic persistence, recovery, quarantine, and retention for the outbox."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, BinaryIO

from parking_monitor.outbox_models import (
    JsonValue,
    OutboxPersistenceError,
    OutboxPostCommitPersistenceError,
    OutboxRecord,
    OutboxRetentionPolicy,
    RecordValidationError,
    SCHEMA_VERSION,
    TERMINAL_STATES,
    parse_utc_timestamp,
    require_mapping,
)

MAX_QUARANTINE_FILES = 20
MAX_OUTBOX_FILE_BYTES = 5_000_000


@dataclass(frozen=True)
class RecoveryEvent:
    reason: str
    count: int = 1
    quarantine_path: str | None = None

    def to_json(self) -> dict[str, JsonValue]:
        payload: dict[str, JsonValue] = {"reason": self.reason, "count": self.count}
        if self.quarantine_path is not None:
            payload["quarantine_path"] = self.quarantine_path
        return payload


@dataclass(frozen=True)
class RecoveryResult:
    recovered_count: int = 0
    quarantined_count: int = 0
    events: tuple[RecoveryEvent, ...] = ()

    @property
    def reason_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for event in self.events:
            counts[event.reason] = counts.get(event.reason, 0) + event.count
        return counts

    def with_event(self, event: RecoveryEvent) -> "RecoveryResult":
        return RecoveryResult(
            recovered_count=self.recovered_count,
            quarantined_count=self.quarantined_count + event.count,
            events=(*self.events, event),
        )

    def with_recovered_count(self, count: int) -> "RecoveryResult":
        return RecoveryResult(count, self.quarantined_count, self.events)

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "recovered_count": self.recovered_count,
            "quarantined_count": self.quarantined_count,
            "reason_counts": self.reason_counts,
            "events": [event.to_json() for event in self.events],
        }


def load_records(
    path: Path,
    *,
    max_bytes: int = MAX_OUTBOX_FILE_BYTES,
    fsync_directory: Callable[[Path], None],
) -> tuple[list[OutboxRecord], RecoveryResult]:
    if not path.exists():
        return [], RecoveryResult()
    try:
        if path.stat().st_size > max_bytes:
            event = _quarantine_file(path, reason="oversized_file", suffix="json", fsync_directory=fsync_directory)
            return [], RecoveryResult().with_event(event)
    except OSError as exc:
        raise OutboxPersistenceError("failed to inspect local outbox payload") from exc
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        event = _quarantine_bytes(path, raw, reason="invalid_json", suffix="json", fsync_directory=fsync_directory)
        return [], RecoveryResult().with_event(event)
    invalid_reason = _top_level_error(payload)
    if invalid_reason is not None:
        event = _quarantine_json(path, payload, reason=invalid_reason, fsync_directory=fsync_directory)
        return [], RecoveryResult().with_event(event)
    records: list[OutboxRecord] = []
    recovery = RecoveryResult()
    for item in payload["items"]:
        try:
            records.append(OutboxRecord.from_json(require_mapping(item, "record")))
        except RecordValidationError as exc:
            event = _quarantine_json(path, item, reason=str(exc) or "malformed_record", fsync_directory=fsync_directory)
            recovery = recovery.with_event(event)
        except (TypeError, ValueError):
            event = _quarantine_json(path, item, reason="malformed_record", fsync_directory=fsync_directory)
            recovery = recovery.with_event(event)
    return records, recovery.with_recovered_count(len(records))


def persist_records(
    path: Path,
    records: list[OutboxRecord],
    *,
    fsync_directory: Callable[[Path], None],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": SCHEMA_VERSION, "items": [record.to_json() for record in records]}
    tmp_path: str | None = None
    replaced = False
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp_path = handle.name
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        replaced = True
        fsync_directory(path.parent)
    except OSError as exc:
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        error = OutboxPostCommitPersistenceError if replaced else OutboxPersistenceError
        raise error("failed to persist local outbox record") from exc


def apply_retention(
    records: list[OutboxRecord],
    retention: OutboxRetentionPolicy,
    *,
    now: datetime | None = None,
) -> list[OutboxRecord]:
    retained = list(records)
    if retention.max_terminal_age_seconds is not None:
        cutoff = (now or datetime.now(timezone.utc)) - timedelta(seconds=retention.max_terminal_age_seconds)
        retained = [
            record
            for record in retained
            if record.state not in TERMINAL_STATES
            or (parsed := parse_utc_timestamp(record.updated_at)) is None
            or parsed >= cutoff
        ]
    if retention.max_records is not None and len(retained) > retention.max_records:
        indexed = list(enumerate(retained))
        indexed.sort(key=lambda item: (_prune_rank(item[1]), item[1].updated_at, item[0]))
        remove = {index for index, _record in indexed[: len(retained) - retention.max_records]}
        retained = [record for index, record in enumerate(retained) if index not in remove]
    return retained


def fsync_directory(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _top_level_error(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return "invalid_top_level_schema"
    if payload.get("schema_version") != SCHEMA_VERSION:
        return "unsupported_schema_version"
    if not isinstance(payload.get("items"), list):
        return "invalid_items_schema"
    return None


def _quarantine_json(
    outbox_path: Path,
    payload: Any,
    *,
    reason: str,
    fsync_directory: Callable[[Path], None],
) -> RecoveryEvent:
    serialized = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8") + b"\n"
    return _quarantine_bytes(
        outbox_path,
        serialized,
        reason=reason,
        suffix="json",
        fsync_directory=fsync_directory,
    )


def _quarantine_bytes(
    outbox_path: Path,
    payload: bytes,
    *,
    reason: str,
    suffix: str,
    fsync_directory: Callable[[Path], None],
) -> RecoveryEvent:
    digest = hashlib.sha256(payload).hexdigest()[:16]
    target = _quarantine_dir(outbox_path) / f"{reason}-{digest}.{suffix}.bad"
    _atomic_quarantine_write(target, lambda handle: handle.write(payload), fsync_directory=fsync_directory)
    return RecoveryEvent(reason=reason, quarantine_path=str(target))


def _quarantine_file(
    source: Path,
    *,
    reason: str,
    suffix: str,
    fsync_directory: Callable[[Path], None],
) -> RecoveryEvent:
    stat = source.stat()
    digest = hashlib.sha256(f"{source.name}:{stat.st_size}:{stat.st_mtime_ns}".encode()).hexdigest()[:16]
    target = _quarantine_dir(source) / f"{reason}-{digest}.{suffix}.bad"

    def copy_source(handle: BinaryIO) -> None:
        with source.open("rb") as reader:
            shutil.copyfileobj(reader, handle, length=1024 * 1024)

    _atomic_quarantine_write(target, copy_source, fsync_directory=fsync_directory)
    return RecoveryEvent(reason=reason, quarantine_path=str(target))


def _quarantine_dir(outbox_path: Path) -> Path:
    return outbox_path.parent / f".{outbox_path.stem}-quarantine"


def _atomic_quarantine_write(
    path: Path,
    writer: Callable[[BinaryIO], None],
    *,
    fsync_directory: Callable[[Path], None],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False
        ) as handle:
            tmp_path = handle.name
            writer(handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        fsync_directory(path.parent)
        _prune_quarantine(path.parent)
    except OSError as exc:
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        raise OutboxPersistenceError("failed to quarantine local outbox payload") from exc


def _prune_quarantine(path: Path) -> None:
    files = sorted(
        (item for item in path.iterdir() if item.is_file()),
        key=lambda item: (item.stat().st_mtime_ns, item.name),
        reverse=True,
    )
    for stale in files[MAX_QUARANTINE_FILES:]:
        try:
            stale.unlink()
        except OSError:
            pass


def _prune_rank(record: OutboxRecord) -> int:
    return 0 if record.state in TERMINAL_STATES else 1
