"""Bounded, identity-bearing publication of retained Matrix snapshots."""

from __future__ import annotations

import os
from pathlib import Path
import secrets
import stat

from parking_spot_monitor.bounded_descriptor_io import descriptor_signature, read_descriptor_exact
from parking_spot_monitor.file_descriptor_binding import (
    OwnedFile,
    RootedDirectoryOwner,
    descriptor_identity,
    safe_basename,
)
from parking_spot_monitor.jpeg_artifacts import MAX_CANONICAL_JPEG_BYTES

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
        temporary_signature = _descriptor_signature(temporary_fd)
        if temporary_signature[2] != source_before[2]:
            raise OSError("snapshot source changed while copying")
        if read_descriptor_exact(source_fd, source_before).digest != digest:
            raise OSError("snapshot source changed while copying")
        if read_descriptor_exact(temporary_fd, temporary_signature).digest != digest:
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
            owner.unlink_if_matches(temporary_name, descriptor_identity(temporary_fd), recover_legacy=False)
        if temporary_fd >= 0:
            os.close(temporary_fd)


def _copy_bounded(
    source_fd: int, temporary_fd: int, source_before: tuple[int, int, int, int, int]
) -> bytes:
    try:
        return read_descriptor_exact(source_fd, source_before, destination_fd=temporary_fd).digest
    except OSError as exc:
        raise OSError("snapshot source changed while copying") from exc


def _descriptor_signature(descriptor: int) -> tuple[int, int, int, int, int]:
    return descriptor_signature(descriptor)
