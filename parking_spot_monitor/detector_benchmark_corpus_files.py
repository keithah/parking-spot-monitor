from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FileIdentity:
    path: Path
    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int
    sha256: str


def read_identity(
    path: Path,
    label: str,
    limit: int,
) -> tuple[FileIdentity, bytes]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError(
            f"{label} must be a readable non-symlink regular file"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"{label} must be a regular file")
        if before.st_size <= 0 or before.st_size > limit:
            raise ValueError(f"{label} size is outside the supported bound")
        chunks: list[bytes] = []
        total = 0
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, min(1024 * 1024, limit + 1 - total)):
            total += len(chunk)
            if total > limit:
                raise ValueError(f"{label} size is outside the supported bound")
            digest.update(chunk)
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        leaf = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise ValueError(f"{label} changed during corpus preflight") from exc
    if (
        _stable_fields(before) != _stable_fields(after)
        or _stable_fields(after) != _stable_fields(leaf)
    ):
        raise ValueError(f"{label} changed during corpus preflight")
    return (
        FileIdentity(
            path=path,
            device=after.st_dev,
            inode=after.st_ino,
            size_bytes=after.st_size,
            modified_ns=after.st_mtime_ns,
            changed_ns=after.st_ctime_ns,
            sha256=digest.hexdigest(),
        ),
        b"".join(chunks),
    )


def create_snapshot(
    path: Path,
    payload: bytes,
    label: str,
    limit: int,
) -> FileIdentity:
    descriptor = os.open(
        path,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
        0o400,
    )
    try:
        _write_all(descriptor, payload)
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return read_identity(path, label, limit)[0]


def require_matching_snapshot(
    original: FileIdentity,
    snapshot: FileIdentity,
) -> None:
    if (
        original.size_bytes != snapshot.size_bytes
        or original.sha256 != snapshot.sha256
    ):
        raise ValueError("corpus snapshot differs from its original")


def _stable_fields(item: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        item.st_dev,
        item.st_ino,
        item.st_size,
        item.st_mtime_ns,
        item.st_ctime_ns,
    )


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        offset += os.write(descriptor, payload[offset:])
