"""Conditional, descriptor-verified publication for decision-memory JSON."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import os
import stat
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

SourceSignature = tuple[int, int, int, int, int, str]
Exchange = Callable[[Path, Path], None]
ExclusiveLink = Callable[[Path, Path], None]

_AT_FDCWD = -100
_RENAME_EXCHANGE = 2
_LIBC = ctypes.CDLL(None, use_errno=True)
_LIBC.renameat2.argtypes = [
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_uint,
]
_LIBC.renameat2.restype = ctypes.c_int


@dataclass(frozen=True, slots=True)
class ConditionalPublication:
    published: bool
    signature: SourceSignature | None = None


def publish_decision_memory_bytes(
    path: Path,
    payload: bytes,
    *,
    expected_signature: SourceSignature | None,
    max_file_bytes: int,
    exchange: Exchange,
    exclusive_link: ExclusiveLink,
) -> ConditionalPublication:
    """Publish only if the destination still identifies the expected source."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _write_temporary(path, payload)
    cleanup_temporary = True
    try:
        candidate = read_source_signature(temporary, max_file_bytes)
        if expected_signature is None:
            try:
                exclusive_link(temporary, path)
            except FileExistsError:
                return ConditionalPublication(False)
            temporary.unlink()
            cleanup_temporary = False
        else:
            exchange(temporary, path)
            try:
                displaced = read_source_signature(temporary, max_file_bytes)
            except (FileNotFoundError, OSError, OverflowError):
                cleanup_temporary = False
                raise
            if not _same_content_identity(displaced, expected_signature):
                try:
                    restored = _restore_latest_canonical(
                        temporary,
                        path,
                        desired=displaced,
                        current=candidate,
                        max_file_bytes=max_file_bytes,
                        exchange=exchange,
                    )
                except (FileNotFoundError, OSError, OverflowError):
                    cleanup_temporary = False
                    raise
                cleanup_temporary = restored
                return ConditionalPublication(False)
        _fsync_directory(path.parent)
        published = read_source_signature(path, max_file_bytes)
        if not _same_content_identity(published, candidate):
            return ConditionalPublication(False)
        return ConditionalPublication(True, published)
    finally:
        if cleanup_temporary:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def read_source_signature(path: Path, max_file_bytes: int) -> SourceSignature:
    return read_decision_memory_source(path, max_file_bytes)[1]


def read_decision_memory_source(
    path: Path, max_file_bytes: int
) -> tuple[bytes, SourceSignature]:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise OSError("decision memory source is not a regular file")
        if before.st_size > max_file_bytes:
            raise OverflowError("decision memory source exceeds byte limit")
        raw = bytearray()
        while chunk := os.read(descriptor, min(65_536, max_file_bytes + 1 - len(raw))):
            raw.extend(chunk)
            if len(raw) > max_file_bytes:
                raise OverflowError("decision memory source exceeds byte limit")
        after = os.fstat(descriptor)
        leaf = os.stat(path, follow_symlinks=False)
    finally:
        os.close(descriptor)
    fields = _source_stat_fields(after)
    if _source_stat_fields(before) != fields or fields != _source_stat_fields(leaf) or len(raw) != after.st_size:
        raise OSError("decision memory source changed during read")
    return bytes(raw), (*fields, hashlib.sha256(raw).hexdigest())


def rename_exchange(source: Path, destination: Path) -> None:
    result = _LIBC.renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(destination),
        _RENAME_EXCHANGE,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), destination)


def link_exclusive(source: Path, destination: Path) -> None:
    try:
        os.link(source, destination, follow_symlinks=False)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            raise FileExistsError(exc.errno, exc.strerror, destination) from exc
        raise


def _write_temporary(path: Path, payload: bytes) -> Path:
    with tempfile.NamedTemporaryFile(
        "wb", delete=False, dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    ) as handle:
        temporary = Path(handle.name)
        handle.write(payload)
        handle.flush()
        os.fchmod(handle.fileno(), 0o644)
        os.fsync(handle.fileno())
    return temporary


def _same_content_identity(left: SourceSignature, right: SourceSignature) -> bool:
    return left[:3] == right[:3] and left[5] == right[5]


def _restore_latest_canonical(
    temporary: Path,
    path: Path,
    *,
    desired: SourceSignature,
    current: SourceSignature,
    max_file_bytes: int,
    exchange: Exchange,
) -> bool:
    canonical = read_source_signature(path, max_file_bytes)
    if not _same_content_identity(canonical, current):
        return True
    while True:
        exchange(temporary, path)
        _fsync_directory(path.parent)
        swapped_out = read_source_signature(temporary, max_file_bytes)
        if _same_content_identity(swapped_out, current):
            return True
        current, desired = desired, swapped_out


def _source_stat_fields(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _fsync_directory(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
