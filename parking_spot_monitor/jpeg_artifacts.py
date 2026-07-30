"""Durable JPEG publication and shared decode lifecycles."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import errno
import fcntl
import hashlib
from io import BytesIO
import os
from pathlib import Path
import secrets
import stat
from typing import Literal
import warnings

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.file_descriptor_binding import (
    FileIdentity,
    RootedDirectoryOwner,
    descriptor_identity,
)

_FICLONE = 0x40049409
_COPY_CHUNK_BYTES = 1024 * 1024
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
class DecodedRgbJpeg:
    image: Image.Image
    source_width: int
    source_height: int


@dataclass(frozen=True, slots=True)
class _SourceEvidence:
    signature: tuple[int, int, int, int, int]
    digest: bytes
    mode: int


class JpegDecodeError(RuntimeError):
    def __init__(
        self,
        code: Literal["unidentified", "decompression_bomb", "invalid_dimensions", "read_failed"],
        *,
        source_error_type: str | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.source_error_type = source_error_type


def publish_canonical_jpeg(source: str | Path, destination: str | Path) -> JpegPublication:
    source_path = Path(source)
    destination_path = Path(destination)
    source_fd = _open_source_descriptor(source_path)
    try:
        evidence = _validated_source_evidence(source_fd)
        with RootedDirectoryOwner(destination_path.parent, create=True) as owner:
            temporary_name = f".{destination_path.name}.{secrets.token_hex(8)}.tmp"
            replaced = False
            temporary_fd = committed_identity_fd = committed_fd = -1
            try:
                try:
                    _reflink(source_fd, owner, temporary_name, evidence.mode)
                    strategy: Literal["reflink", "copy"] = "reflink"
                except OSError as reflink_exc:
                    if reflink_exc.errno not in _FALLBACK_ERRNOS:
                        raise
                    _copy_file(source_fd, owner, temporary_name, evidence.mode)
                    strategy = "copy"
                temporary_fd = _open_artifact(owner, temporary_name)
                temporary_identity = descriptor_identity(temporary_fd)
                _validate_artifact_descriptor(temporary_fd, source_fd, evidence)
                os.fsync(temporary_fd)
                _validate_artifact_descriptor(temporary_fd, source_fd, evidence)
                owner.replace(temporary_name, destination_path.name)
                replaced = True
                try:
                    committed_identity_fd = owner.open_identity(destination_path.name)
                    committed_identity = descriptor_identity(committed_identity_fd)
                    if committed_identity != temporary_identity:
                        raise JpegDecodeError("read_failed")
                    committed_fd = _open_artifact(owner, destination_path.name)
                    _validate_artifact_descriptor(committed_fd, source_fd, evidence)
                    if descriptor_identity(committed_fd) != temporary_identity:
                        raise JpegDecodeError("read_failed")
                    if not owner.matches(destination_path.name, committed_identity):
                        raise JpegDecodeError("read_failed")
                except (JpegDecodeError, OSError):
                    cleanup_identity = (
                        descriptor_identity(committed_identity_fd)
                        if committed_identity_fd >= 0
                        else temporary_identity
                    )
                    owner.unlink_if_matches(destination_path.name, cleanup_identity)
                    raise JpegDecodeError("read_failed") from None
                owner.fsync()
                try:
                    _validate_artifact_descriptor(committed_fd, source_fd, evidence)
                    if not owner.matches(destination_path.name, committed_identity) or not owner.is_still_bound():
                        raise JpegDecodeError("read_failed")
                except JpegDecodeError:
                    owner.unlink_if_matches(destination_path.name, committed_identity)
                    raise
                return JpegPublication(path=destination_path, strategy=strategy, identity=committed_identity)
            finally:
                if not replaced and temporary_fd >= 0:
                    owner.unlink_if_matches(temporary_name, descriptor_identity(temporary_fd))
                for descriptor in (committed_fd, committed_identity_fd, temporary_fd):
                    if descriptor >= 0:
                        os.close(descriptor)
    finally:
        os.close(source_fd)


def open_decoded_rgb_jpeg(path: Path, *, initial_max_dimension: int) -> AbstractContextManager[DecodedRgbJpeg]:
    return _decoded_rgb_jpeg(Path(path), initial_max_dimension=initial_max_dimension)


def open_decoded_rgb_jpeg_bytes(
    payload: bytes, *, initial_max_dimension: int
) -> AbstractContextManager[DecodedRgbJpeg]:
    return _decoded_rgb_jpeg_bytes(bytes(payload), initial_max_dimension=initial_max_dimension)


def jpeg_bytes_dimensions(payload: bytes) -> tuple[int, int]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(payload)) as image:
                if image.format != "JPEG":
                    raise JpegDecodeError("unidentified")
                if image.width <= 0 or image.height <= 0:
                    raise JpegDecodeError("invalid_dimensions")
                dimensions = image.size
                image.verify()
                return dimensions
    except JpegDecodeError:
        raise
    except UnidentifiedImageError as exc:
        raise JpegDecodeError("unidentified", source_error_type=exc.__class__.__name__) from exc
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
        raise JpegDecodeError("decompression_bomb", source_error_type=exc.__class__.__name__) from exc
    except (OSError, ValueError) as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc


@contextmanager
def _decoded_rgb_jpeg_bytes(payload: bytes, *, initial_max_dimension: int) -> Iterator[DecodedRgbJpeg]:
    with BytesIO(payload) as source:
        with _decoded_rgb_jpeg(source, initial_max_dimension=initial_max_dimension) as decoded:
            yield decoded


@contextmanager
def _decoded_rgb_jpeg(path: Path | BytesIO, *, initial_max_dimension: int) -> Iterator[DecodedRgbJpeg]:
    opened: Image.Image | None = None
    working: Image.Image | None = None
    try:
        try:
            if initial_max_dimension <= 0:
                raise JpegDecodeError("invalid_dimensions")
            with warnings.catch_warnings():
                warnings.simplefilter("error", Image.DecompressionBombWarning)
                opened = Image.open(path)
                if opened.format != "JPEG":
                    raise JpegDecodeError("unidentified")
                width, height = opened.size
                if width <= 0 or height <= 0:
                    raise JpegDecodeError("invalid_dimensions")
                bounded_dimension = min(max(width, height), initial_max_dimension)
                bounded_size = (
                    (bounded_dimension, max(1, height * bounded_dimension // width))
                    if width >= height
                    else (max(1, width * bounded_dimension // height), bounded_dimension)
                )
                opened.draft("RGB", bounded_size)
                opened.load()
                working = opened if opened.mode == "RGB" else opened.convert("RGB")
        except JpegDecodeError:
            raise
        except UnidentifiedImageError as exc:
            raise JpegDecodeError("unidentified", source_error_type=exc.__class__.__name__) from exc
        except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
            raise JpegDecodeError("decompression_bomb", source_error_type=exc.__class__.__name__) from exc
        except (OSError, ValueError) as exc:
            raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc
        yield DecodedRgbJpeg(working, width, height)
    finally:
        if working is not None and working is not opened:
            working.close()
        if opened is not None:
            opened.close()


def _validate_jpeg_descriptor(source_fd: int) -> None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            os.lseek(source_fd, 0, os.SEEK_SET)
            with os.fdopen(os.dup(source_fd), "rb") as source_handle:
                with Image.open(source_handle) as image:
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


def _copy_file(source_fd: int, owner: RootedDirectoryOwner, temporary_name: str, source_mode: int) -> None:
    destination_fd = owner.open_file(temporary_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, source_mode)
    try:
        os.lseek(source_fd, 0, os.SEEK_SET)
        while chunk := os.read(source_fd, _COPY_CHUNK_BYTES):
            _write_all(destination_fd, chunk)
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
    before = _descriptor_signature(source_fd)
    value = os.fstat(source_fd)
    if not stat.S_ISREG(value.st_mode):
        raise JpegDecodeError("read_failed")
    digest_before = _digest_descriptor(source_fd)
    _validate_jpeg_descriptor(source_fd)
    digest_after = _digest_descriptor(source_fd)
    if _descriptor_signature(source_fd) != before or digest_after != digest_before:
        raise JpegDecodeError("read_failed")
    return _SourceEvidence(before, digest_after, stat.S_IMODE(value.st_mode))


def _open_artifact(owner: RootedDirectoryOwner, name: str) -> int:
    try:
        return owner.open_file(name, os.O_RDONLY)
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc


def _validate_artifact_descriptor(descriptor: int, source_fd: int, evidence: _SourceEvidence) -> None:
    signature = _descriptor_signature(descriptor)
    if not stat.S_ISREG(os.fstat(descriptor).st_mode) or _digest_descriptor(descriptor) != evidence.digest:
        raise JpegDecodeError("read_failed")
    _validate_jpeg_descriptor(descriptor)
    if (
        _descriptor_signature(descriptor) != signature
        or _digest_descriptor(descriptor) != evidence.digest
        or _descriptor_signature(source_fd) != evidence.signature
    ):
        raise JpegDecodeError("read_failed")


def _digest_descriptor(descriptor: int) -> bytes:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while chunk := os.read(descriptor, _COPY_CHUNK_BYTES):
        digest.update(chunk)
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.digest()


def _descriptor_signature(descriptor: int) -> tuple[int, int, int, int, int]:
    return _stat_signature(os.fstat(descriptor))


def _stat_signature(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    try:
        offset = 0
        while offset < len(view):
            written = os.write(descriptor, view[offset:])
            if written <= 0:
                raise OSError("JPEG publication write made no progress")
            offset += written
    finally:
        view.release()
