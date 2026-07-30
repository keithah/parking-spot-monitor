"""Bounded, identity-bearing publication of retained Matrix snapshots."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import secrets
import stat

from parking_spot_monitor.file_descriptor_binding import (
    OwnedFile,
    RootedDirectoryOwner,
    descriptor_identity,
    safe_basename,
)
from parking_spot_monitor.jpeg_artifacts import MAX_CANONICAL_JPEG_BYTES

_COPY_CHUNK_BYTES = 1024 * 1024
MAX_RETAINED_JPEG_BYTES = MAX_CANONICAL_JPEG_BYTES


def publish_retained_snapshot(source: Path, snapshot_root: Path, filename: str) -> OwnedFile:
    source_path = Path(os.path.abspath(source))
    if not isinstance(filename, str):
        raise OSError("artifact filename must be a basename")
    safe_name = safe_basename(filename)
    with RootedDirectoryOwner(source_path.parent, create=False) as source_owner:
        source_fd = source_owner.open_file(source_path.name, os.O_RDONLY)
        try:
            return _publish_descriptor(source_fd, Path(os.path.abspath(snapshot_root)), safe_name)
        finally:
            os.close(source_fd)


def _publish_descriptor(source_fd: int, snapshot_root: Path, safe_name: str) -> OwnedFile:
    source_before = _descriptor_signature(source_fd)
    if not stat.S_ISREG(os.fstat(source_fd).st_mode) or not 0 < source_before[2] <= MAX_RETAINED_JPEG_BYTES:
        raise OSError("snapshot source is not a bounded regular file")
    with RootedDirectoryOwner(snapshot_root, create=True) as owner:
        if not owner.is_still_bound():
            raise OSError("snapshot root changed")
        return _publish_into_owner(source_fd, source_before, owner, safe_name)


def _publish_into_owner(
    source_fd: int,
    source_before: tuple[int, int, int, int, int],
    owner: RootedDirectoryOwner,
    safe_name: str,
) -> OwnedFile:
    temporary_name = f".{safe_name}.{secrets.token_hex(8)}.tmp"
    temporary_fd = -1
    replaced = False
    published_identity = None
    try:
        temporary_fd = owner.open_file(temporary_name, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o644)
        digest = _copy_bounded(source_fd, temporary_fd, source_before)
        os.fchmod(temporary_fd, 0o644)
        os.fsync(temporary_fd)
        if (
            _descriptor_signature(source_fd) != source_before
            or _digest_descriptor(source_fd) != digest
            or _digest_descriptor(temporary_fd) != digest
        ):
            raise OSError("snapshot source changed while copying")
        published_identity = descriptor_identity(temporary_fd)
        owner.replace(temporary_name, safe_name)
        replaced = True
        if not owner.matches(safe_name, published_identity):
            raise OSError("retained snapshot changed during publication")
        owner.fsync()
        if not owner.matches(safe_name, published_identity) or not owner.is_still_bound():
            raise OSError("retained snapshot binding changed during publication")
        return OwnedFile(owner.path / safe_name, published_identity)
    except Exception:
        if replaced and published_identity is not None:
            owner.unlink_if_matches(safe_name, published_identity)
        raise
    finally:
        if not replaced and temporary_fd >= 0:
            owner.unlink_if_matches(temporary_name, descriptor_identity(temporary_fd))
        if temporary_fd >= 0:
            os.close(temporary_fd)


def _copy_bounded(
    source_fd: int, temporary_fd: int, source_before: tuple[int, int, int, int, int]
) -> bytes:
    remaining = source_before[2]
    digest = hashlib.sha256()
    while remaining:
        chunk = os.read(source_fd, min(_COPY_CHUNK_BYTES, remaining))
        if not chunk:
            raise OSError("snapshot source ended while copying")
        _write_all(temporary_fd, chunk)
        digest.update(chunk)
        remaining -= len(chunk)
    if os.read(source_fd, 1) or _descriptor_signature(source_fd) != source_before:
        raise OSError("snapshot source changed while copying")
    return digest.digest()


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    try:
        offset = 0
        while offset < len(view):
            written = os.write(descriptor, view[offset:])
            if written <= 0:
                raise OSError("snapshot publication write made no progress")
            offset += written
    finally:
        view.release()


def _digest_descriptor(descriptor: int) -> bytes:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while chunk := os.read(descriptor, _COPY_CHUNK_BYTES):
        digest.update(chunk)
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.digest()


def _descriptor_signature(descriptor: int) -> tuple[int, int, int, int, int]:
    value = os.fstat(descriptor)
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
