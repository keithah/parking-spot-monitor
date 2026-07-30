"""Exact-size descriptor reads with bounded hashing, capture, and copying."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os

_CHUNK_BYTES = 1024 * 1024
DescriptorSignature = tuple[int, int, int, int, int]


@dataclass(frozen=True, slots=True)
class BoundedDescriptorRead:
    data: bytes | None
    digest: bytes


def read_descriptor_exact(
    descriptor: int,
    expected_signature: DescriptorSignature,
    *,
    capture_bytes: bool = False,
    destination_fd: int | None = None,
) -> BoundedDescriptorRead:
    """Consume exactly the captured size, probe growth once, and reset offset."""

    remaining = expected_signature[2]
    digest = hashlib.sha256()
    captured = bytearray() if capture_bytes else None
    os.lseek(descriptor, 0, os.SEEK_SET)
    try:
        while remaining:
            chunk = os.read(descriptor, min(_CHUNK_BYTES, remaining))
            if not chunk:
                raise OSError("descriptor ended before captured size")
            digest.update(chunk)
            if captured is not None:
                captured.extend(chunk)
            if destination_fd is not None:
                _write_all(destination_fd, chunk)
            remaining -= len(chunk)
        growth = os.read(descriptor, 1)
        after = _descriptor_signature(descriptor)
        if growth or after != expected_signature:
            raise OSError("descriptor changed from captured evidence")
        return BoundedDescriptorRead(
            data=bytes(captured) if captured is not None else None,
            digest=digest.digest(),
        )
    finally:
        os.lseek(descriptor, 0, os.SEEK_SET)


def descriptor_signature(descriptor: int) -> DescriptorSignature:
    return _descriptor_signature(descriptor)


def _descriptor_signature(descriptor: int) -> DescriptorSignature:
    value = os.fstat(descriptor)
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    try:
        offset = 0
        while offset < len(view):
            written = os.write(descriptor, view[offset:])
            if written <= 0:
                raise OSError("descriptor write made no progress")
            offset += written
    finally:
        view.release()
