"""Bounded durable index and in-process transactions for owned disposal."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from functools import wraps
import json
import os
from pathlib import Path
import secrets
import stat
import threading

_MANIFEST_NAME = ".owned-disposals.json"
_MAX_ENTRIES = 256
_MAX_BYTES = 262_144
_LOCK_GUARD = threading.Lock()
_DIRECTORY_LOCKS: dict[tuple[int, int], _ManifestLock] = {}


@dataclass(slots=True)
class _ManifestLock:
    lock: threading.RLock
    references: int = 0


@dataclass(frozen=True, slots=True)
class DisposalManifestEntry:
    disposal: str
    recovery: str
    dev: int
    ino: int


@contextmanager
def disposal_manifest_transaction(directory_fd: int) -> Iterator[None]:
    value = os.fstat(directory_fd)
    key = (value.st_dev, value.st_ino)
    with _LOCK_GUARD:
        entry = _DIRECTORY_LOCKS.setdefault(key, _ManifestLock(threading.RLock()))
        entry.references += 1
    try:
        with entry.lock:
            yield
    finally:
        with _LOCK_GUARD:
            entry.references -= 1
            if entry.references == 0:
                _DIRECTORY_LOCKS.pop(key, None)


def manifest_transaction(operation):
    @wraps(operation)
    def serialized(directory_fd: int, *args: object, **kwargs: object):
        with disposal_manifest_transaction(directory_fd):
            return operation(directory_fd, *args, **kwargs)
    return serialized


def manifest_entries_at(directory_fd: int, *, limit: int = _MAX_ENTRIES) -> list[DisposalManifestEntry]:
    with disposal_manifest_transaction(directory_fd):
        return _read_manifest(directory_fd)[: max(0, min(limit, _MAX_ENTRIES))]


def record_disposal_at(directory_fd: int, entry: DisposalManifestEntry) -> bool:
    _validate_entry(entry)
    with disposal_manifest_transaction(directory_fd):
        try:
            entries = _read_manifest(directory_fd)
            entries = [item for item in entries if item.disposal != entry.disposal]
            if len(entries) >= _MAX_ENTRIES:
                return False
            entries.append(entry)
            _write_manifest(directory_fd, entries)
            return True
        except OSError:
            return False


def forget_disposal_at(directory_fd: int, disposal: str) -> bool:
    safe_disposal = _safe_basename(disposal)
    with disposal_manifest_transaction(directory_fd):
        try:
            entries = _read_manifest(directory_fd)
            retained = [entry for entry in entries if entry.disposal != safe_disposal]
            if len(retained) == len(entries):
                return True
            _write_manifest(directory_fd, retained)
            return True
        except OSError:
            return False


def _read_manifest(directory_fd: int) -> list[DisposalManifestEntry]:
    descriptor = -1
    try:
        descriptor = os.open(
            _MANIFEST_NAME,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        value = os.fstat(descriptor)
        if not stat.S_ISREG(value.st_mode) or value.st_size > _MAX_BYTES:
            raise OSError("owned disposal manifest is invalid")
        payload = bytearray()
        while len(payload) <= _MAX_BYTES:
            chunk = os.read(descriptor, min(65_536, _MAX_BYTES + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        decoded = json.loads(bytes(payload) or b"{}")
        if not isinstance(decoded, dict) or decoded.get("version") != 1 or not isinstance(decoded.get("entries"), list):
            raise OSError("owned disposal manifest schema is invalid")
        raw_entries = decoded["entries"]
        if len(raw_entries) > _MAX_ENTRIES:
            raise OSError("owned disposal manifest is too large")
        entries = [DisposalManifestEntry(**item) for item in raw_entries]
        for entry in entries:
            _validate_entry(entry)
        return entries
    except FileNotFoundError:
        return []
    except (json.JSONDecodeError, TypeError, ValueError, KeyError) as exc:
        raise OSError("owned disposal manifest is invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _write_manifest(directory_fd: int, entries: list[DisposalManifestEntry]) -> None:
    payload = json.dumps(
        {"version": 1, "entries": [asdict(entry) for entry in entries]},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(payload) > _MAX_BYTES:
        raise OSError("owned disposal manifest is too large")
    temporary = f".{_MANIFEST_NAME}.{secrets.token_hex(8)}.tmp"
    descriptor = -1
    replaced = False
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_DSYNC", os.O_SYNC),
            0o600,
            dir_fd=directory_fd,
        )
        _write_all(descriptor, payload)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, _MANIFEST_NAME, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        replaced = True
        os.fsync(directory_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not replaced:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except OSError:
                pass


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("owned disposal manifest write made no progress")
        offset += written


def _validate_entry(entry: DisposalManifestEntry) -> None:
    if not isinstance(entry.dev, int) or not isinstance(entry.ino, int) or entry.dev < 0 or entry.ino <= 0:
        raise OSError("owned disposal manifest identity is invalid")
    _safe_basename(entry.disposal)
    _safe_basename(entry.recovery)


def _safe_basename(value: str) -> str:
    if not isinstance(value, str) or not value or value in {".", ".."} or Path(value).name != value:
        raise OSError("artifact name must be a basename")
    return value
