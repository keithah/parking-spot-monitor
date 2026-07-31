"""Atomic persistence, recovery, quarantine, and retention for the outbox."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from typing import Any, BinaryIO

from parking_monitor.outbox_locking import outbox_transaction
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
from parking_monitor.outbox_reconciliation import merge_records

MAX_QUARANTINE_FILES = 20
MAX_OUTBOX_FILE_BYTES = 5_000_000
MAX_OUTBOX_RECORD_BYTES = 1_000_000


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
    max_record_bytes: int = MAX_OUTBOX_RECORD_BYTES,
    fsync_directory: Callable[[Path], None],
    repair_records: bool = True,
) -> tuple[list[OutboxRecord], RecoveryResult]:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        )
    except FileNotFoundError:
        return [], RecoveryResult()
    except OSError as exc:
        raise OutboxPersistenceError("local outbox must be a readable non-symlink regular file") from exc
    try:
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise OutboxPersistenceError("local outbox must be a regular file")
            if before.st_size > max_bytes:
                event = _quarantine_file(
                    path,
                    descriptor,
                    before,
                    reason="oversized_file",
                    suffix="json",
                    fsync_directory=fsync_directory,
                )
                return [], RecoveryResult().with_event(event)
            raw = _read_bound_descriptor(descriptor, before, max_bytes=max_bytes)
            _require_stable_binding(path, descriptor, before)
        except OutboxPersistenceError:
            raise
        except OSError as exc:
            raise OutboxPersistenceError("failed to read local outbox payload") from exc
    finally:
        os.close(descriptor)
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
            item_size = len(_json_bytes(item))
        except (TypeError, ValueError):
            event = _quarantine_json(path, item, reason="malformed_record", fsync_directory=fsync_directory)
            recovery = recovery.with_event(event)
            continue
        if item_size > max_record_bytes:
            event = _quarantine_json(path, item, reason="oversized_record", fsync_directory=fsync_directory)
            recovery = recovery.with_event(event)
            continue
        try:
            records.append(OutboxRecord.from_json(require_mapping(item, "record")))
        except RecordValidationError as exc:
            event = _quarantine_json(path, item, reason=str(exc) or "malformed_record", fsync_directory=fsync_directory)
            recovery = recovery.with_event(event)
        except (TypeError, ValueError):
            event = _quarantine_json(path, item, reason="malformed_record", fsync_directory=fsync_directory)
            recovery = recovery.with_event(event)
    recovery = recovery.with_recovered_count(len(records))
    if recovery.quarantined_count and repair_records:
        _repair_records(
            path,
            records,
            source_stat=before,
            max_bytes=max_bytes,
            max_record_bytes=max_record_bytes,
            fsync_directory=fsync_directory,
        )
    return records, recovery


def persist_records(
    path: Path,
    records: list[OutboxRecord],
    *,
    max_bytes: int = MAX_OUTBOX_FILE_BYTES,
    max_record_bytes: int = MAX_OUTBOX_RECORD_BYTES,
    fsync_directory: Callable[[Path], None],
) -> None:
    serialized = _serialized_document(
        records,
        max_bytes=max_bytes,
        max_record_bytes=max_record_bytes,
    )
    with outbox_transaction(path):
        _persist_serialized(path, serialized, fsync_directory=fsync_directory)


def reconcile_records(
    path: Path,
    base_records: list[OutboxRecord],
    proposed_records: list[OutboxRecord],
    *,
    retention: OutboxRetentionPolicy,
    max_bytes: int = MAX_OUTBOX_FILE_BYTES,
    max_record_bytes: int = MAX_OUTBOX_RECORD_BYTES,
    fsync_directory: Callable[[Path], None],
) -> list[OutboxRecord]:
    """Merge one instance's mutation into the latest canonical snapshot."""
    with outbox_transaction(path):
        current, _recovery = load_records(
            path,
            max_bytes=max_bytes,
            max_record_bytes=max_record_bytes,
            fsync_directory=fsync_directory,
            repair_records=False,
        )
        merged = apply_retention(merge_records(base_records, proposed_records, current), retention)
        serialized = _serialized_document(merged, max_bytes=max_bytes, max_record_bytes=max_record_bytes)
        _persist_serialized(path, serialized, fsync_directory=fsync_directory)
        return merged


def _persist_serialized(
    path: Path,
    serialized: bytes,
    *,
    fsync_directory: Callable[[Path], None],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: str | None = None
    replaced = False
    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp_path = handle.name
            handle.write(serialized)
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


def _repair_records(
    path: Path,
    records: list[OutboxRecord],
    *,
    source_stat: os.stat_result,
    max_bytes: int,
    max_record_bytes: int,
    fsync_directory: Callable[[Path], None],
) -> None:
    serialized = _serialized_document(
        records,
        max_bytes=max_bytes,
        max_record_bytes=max_record_bytes,
    )
    with outbox_transaction(path):
        try:
            current = os.stat(path, follow_symlinks=False)
        except OSError as exc:
            raise OutboxPersistenceError("local outbox changed before recovery repair") from exc
        if _stat_signature(current) != _stat_signature(source_stat):
            raise OutboxPersistenceError("local outbox changed before recovery repair")
        _persist_serialized(path, serialized, fsync_directory=fsync_directory)


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
    descriptor: int,
    source_stat: os.stat_result,
    *,
    reason: str,
    suffix: str,
    fsync_directory: Callable[[Path], None],
) -> RecoveryEvent:
    digest = hashlib.sha256(
        f"{source.name}:{source_stat.st_dev}:{source_stat.st_ino}:{source_stat.st_size}:{source_stat.st_mtime_ns}".encode()
    ).hexdigest()[:16]
    target = _quarantine_dir(source) / f"{reason}-{digest}.{suffix}.bad"

    def copy_source(handle: BinaryIO) -> None:
        offset = 0
        remaining = min(source_stat.st_size, MAX_OUTBOX_FILE_BYTES)
        while remaining:
            chunk = os.pread(descriptor, min(1024 * 1024, remaining), offset)
            if not chunk:
                break
            handle.write(chunk)
            offset += len(chunk)
            remaining -= len(chunk)

    _atomic_quarantine_write(target, copy_source, fsync_directory=fsync_directory)
    _require_stable_binding(source, descriptor, source_stat)
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


def _serialized_document(
    records: list[OutboxRecord],
    *,
    max_bytes: int,
    max_record_bytes: int,
) -> bytes:
    items: list[bytes] = []
    document_size = len(b'{"items":[],"schema_version":}') + len(str(SCHEMA_VERSION))
    for record in records:
        item = _json_bytes(record.to_json())
        if len(item) > max_record_bytes:
            raise OutboxPersistenceError("local outbox record exceeds byte limit")
        document_size += len(item) + (1 if items else 0)
        if document_size > max_bytes:
            raise OutboxPersistenceError("local outbox document exceeds byte limit")
        items.append(item)
    document = b'{"items":[' + b",".join(items) + f'],"schema_version":{SCHEMA_VERSION}}}'.encode("ascii")
    return document


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _read_bound_descriptor(descriptor: int, before: os.stat_result, *, max_bytes: int) -> bytes:
    payload = bytearray()
    while len(payload) <= max_bytes:
        chunk = os.read(descriptor, min(65_536, max_bytes + 1 - len(payload)))
        if not chunk:
            break
        payload.extend(chunk)
    if len(payload) > max_bytes:
        raise OutboxPersistenceError("local outbox document exceeds byte limit")
    after = os.fstat(descriptor)
    if _stat_signature(after) != _stat_signature(before) or len(payload) != before.st_size:
        raise OutboxPersistenceError("local outbox changed while reading")
    return bytes(payload)


def _require_stable_binding(path: Path, descriptor: int, before: os.stat_result) -> None:
    try:
        leaf = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise OutboxPersistenceError("local outbox changed while reading") from exc
    if _stat_signature(os.fstat(descriptor)) != _stat_signature(before) or _stat_signature(leaf) != _stat_signature(before):
        raise OutboxPersistenceError("local outbox changed while reading")


def _stat_signature(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
