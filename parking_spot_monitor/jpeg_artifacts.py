"""Durable JPEG publication and shared decode lifecycles."""

from __future__ import annotations

from dataclasses import dataclass
import errno
import fcntl
from io import BytesIO
import os
from pathlib import Path
import secrets
import stat
from typing import Literal
import warnings

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.bounded_descriptor_io import (
    BoundedDescriptorRead,
    descriptor_signature,
    read_descriptor_exact,
)
from parking_spot_monitor.file_descriptor_binding import FileIdentity, RootedDirectoryOwner, descriptor_identity
from parking_spot_monitor.jpeg_decoding import (
    DecodedRgbJpeg,
    JpegDecodeError,
    jpeg_bytes_dimensions,
    open_decoded_rgb_jpeg,
    open_decoded_rgb_jpeg_bytes,
    open_decoded_rgb_jpeg_stream,
)

_FICLONE = 0x40049409
MAX_CANONICAL_JPEG_BYTES = 32 * 1024 * 1024
_FALLBACK_ERRNOS = frozenset(
    error
    for error in (
        errno.EXDEV,
        errno.EOPNOTSUPP,
        getattr(errno, "ENOTSUP", errno.EOPNOTSUPP),
        errno.ENOSYS,
        errno.EPERM,
        errno.EINVAL,
        errno.ENOTTY,
    )
)


@dataclass(frozen=True, slots=True)
class JpegPublication:
    path: Path
    strategy: Literal["reflink", "copy"]
    identity: FileIdentity


@dataclass(frozen=True, slots=True)
class _SourceEvidence:
    signature: tuple[int, int, int, int, int]
    digest: bytes
    mode: int


def publish_canonical_jpeg(source: str | Path, destination: str | Path) -> JpegPublication:
    destination_path = Path(destination)
    source_fd = _open_source_descriptor(Path(source))
    try:
        evidence = _validated_source_evidence(source_fd)
        with RootedDirectoryOwner(destination_path.parent, create=True) as owner:
            return _publish_validated_to_owner(source_fd, evidence, destination_path, owner)
    finally:
        os.close(source_fd)


def publish_canonical_jpeg_to_owner(
    source: str | Path, destination: Path, owner: RootedDirectoryOwner
) -> JpegPublication:
    source_fd = _open_source_descriptor(Path(source))
    try:
        evidence = _validated_source_evidence(source_fd)
        return _publish_validated_to_owner(source_fd, evidence, destination, owner)
    finally:
        os.close(source_fd)


def _publish_validated_to_owner(
    source_fd: int, evidence: _SourceEvidence, destination: Path, owner: RootedDirectoryOwner
) -> JpegPublication:
    temporary_name = f".{destination.name}.{secrets.token_hex(8)}.tmp"
    replaced = False
    temporary_fd = committed_identity_fd = committed_fd = -1
    try:
        try:
            _reflink(source_fd, owner, temporary_name, evidence.mode)
            strategy: Literal["reflink", "copy"] = "reflink"
        except OSError as reflink_exc:
            if reflink_exc.errno not in _FALLBACK_ERRNOS:
                raise
            _copy_file(source_fd, owner, temporary_name, evidence.mode, evidence.signature)
            strategy = "copy"
        temporary_fd = _open_artifact(owner, temporary_name)
        temporary_identity = descriptor_identity(temporary_fd)
        _validate_artifact_descriptor(temporary_fd, source_fd, evidence)
        os.fsync(temporary_fd)
        _validate_artifact_descriptor(temporary_fd, source_fd, evidence)
        owner.replace(temporary_name, destination.name)
        replaced = True
        try:
            committed_identity_fd = owner.open_identity(destination.name)
            committed_identity = descriptor_identity(committed_identity_fd)
            if committed_identity != temporary_identity:
                raise JpegDecodeError("read_failed") from None
            committed_fd = _open_artifact(owner, destination.name)
            _validate_artifact_descriptor(committed_fd, source_fd, evidence)
            if descriptor_identity(committed_fd) != temporary_identity or not owner.matches(destination.name, committed_identity):
                raise JpegDecodeError("read_failed")
        except (JpegDecodeError, OSError):
            cleanup_identity = descriptor_identity(committed_identity_fd) if committed_identity_fd >= 0 else temporary_identity
            owner.unlink_if_matches(destination.name, cleanup_identity)
            raise JpegDecodeError("read_failed") from None
        owner.fsync()
        try:
            _validate_artifact_descriptor(committed_fd, source_fd, evidence)
            if not owner.matches(destination.name, committed_identity) or not owner.is_still_bound():
                raise JpegDecodeError("read_failed")
        except JpegDecodeError:
            owner.unlink_if_matches(destination.name, committed_identity)
            raise
        return JpegPublication(path=destination, strategy=strategy, identity=committed_identity)
    finally:
        if not replaced and temporary_fd >= 0:
            owner.unlink_if_matches(temporary_name, descriptor_identity(temporary_fd))
        for descriptor in (committed_fd, committed_identity_fd, temporary_fd):
            if descriptor >= 0:
                os.close(descriptor)


def _validate_jpeg_bytes(payload: bytes) -> None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(payload)) as image:
                width, height = image.size
                if image.format != "JPEG":
                    raise JpegDecodeError("unidentified")
                if width <= 0 or height <= 0:
                    raise JpegDecodeError("invalid_dimensions")
                image.verify()
    except JpegDecodeError:
        raise
    except UnidentifiedImageError as exc:
        raise JpegDecodeError("unidentified") from exc
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
        raise JpegDecodeError("decompression_bomb") from exc
    except (OSError, ValueError) as exc:
        raise JpegDecodeError("read_failed") from exc


def _reflink(source_fd: int, owner: RootedDirectoryOwner, temporary_name: str, source_mode: int) -> None:
    destination_fd = -1
    try:
        destination_fd = owner.open_file(temporary_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, source_mode)
        fcntl.ioctl(destination_fd, _FICLONE, source_fd)
        os.fchmod(destination_fd, source_mode)
    except Exception:
        if destination_fd >= 0:
            identity = descriptor_identity(destination_fd)
            os.close(destination_fd)
            destination_fd = -1
            owner.unlink_if_matches(temporary_name, identity)
        raise
    finally:
        if destination_fd >= 0:
            os.close(destination_fd)


def _copy_file(
    source_fd: int,
    owner: RootedDirectoryOwner,
    temporary_name: str,
    source_mode: int,
    source_signature: tuple[int, int, int, int, int],
) -> None:
    destination_fd = owner.open_file(temporary_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, source_mode)
    try:
        try:
            read_descriptor_exact(source_fd, source_signature, destination_fd=destination_fd)
        except OSError as exc:
            raise JpegDecodeError("read_failed") from exc
        os.fchmod(destination_fd, source_mode)
    except Exception:
        identity = descriptor_identity(destination_fd)
        os.close(destination_fd)
        destination_fd = -1
        owner.unlink_if_matches(temporary_name, identity)
        raise
    finally:
        if destination_fd >= 0:
            os.close(destination_fd)


def _open_source_descriptor(path: Path) -> int:
    try:
        return os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc


def _validated_source_evidence(source_fd: int) -> _SourceEvidence:
    value = os.fstat(source_fd)
    if not stat.S_ISREG(value.st_mode) or not 0 < value.st_size <= MAX_CANONICAL_JPEG_BYTES:
        raise JpegDecodeError("read_failed")
    before = _stat_signature(value)
    captured = _bounded_capture(source_fd, before)
    if captured.data is None:
        raise JpegDecodeError("read_failed")
    _validate_jpeg_bytes(captured.data)
    digest_after = _bounded_digest(source_fd, before)
    if _descriptor_signature(source_fd) != before or digest_after != captured.digest:
        raise JpegDecodeError("read_failed")
    return _SourceEvidence(before, captured.digest, stat.S_IMODE(value.st_mode))


def _open_artifact(owner: RootedDirectoryOwner, name: str) -> int:
    try:
        return owner.open_file(name, os.O_RDONLY)
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc


def _validate_artifact_descriptor(descriptor: int, source_fd: int, evidence: _SourceEvidence) -> None:
    signature = _descriptor_signature(descriptor)
    if signature[2] != evidence.signature[2] or not stat.S_ISREG(os.fstat(descriptor).st_mode):
        raise JpegDecodeError("read_failed")
    captured = _bounded_capture(descriptor, signature)
    if captured.digest != evidence.digest or captured.data is None:
        raise JpegDecodeError("read_failed")
    _validate_jpeg_bytes(captured.data)
    if (
        _descriptor_signature(descriptor) != signature
        or _bounded_digest(descriptor, signature) != evidence.digest
        or _descriptor_signature(source_fd) != evidence.signature
    ):
        raise JpegDecodeError("read_failed")


def _bounded_digest(descriptor: int, signature: tuple[int, int, int, int, int]) -> bytes:
    try:
        return read_descriptor_exact(descriptor, signature).digest
    except OSError as exc:
        raise JpegDecodeError("read_failed") from exc


def _bounded_capture(
    descriptor: int, signature: tuple[int, int, int, int, int]
) -> BoundedDescriptorRead:
    try:
        return read_descriptor_exact(descriptor, signature, capture_bytes=True)
    except OSError as exc:
        raise JpegDecodeError("read_failed") from exc


def _descriptor_signature(descriptor: int) -> tuple[int, int, int, int, int]:
    return descriptor_signature(descriptor)


def _stat_signature(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
