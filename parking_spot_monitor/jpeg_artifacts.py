"""Durable JPEG publication and shared decode lifecycles."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import errno
import fcntl
import hashlib
import os
from pathlib import Path
import secrets
import stat
from typing import Literal
import warnings

from PIL import Image, UnidentifiedImageError

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
    strategy: Literal["hardlink", "reflink", "copy"]


@dataclass(frozen=True, slots=True)
class DecodedRgbJpeg:
    image: Image.Image
    source_width: int
    source_height: int


@dataclass(frozen=True, slots=True)
class _SourceEvidence:
    signature: tuple[int, int, int, int]
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
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination_path.parent / f".{destination_path.name}.{secrets.token_hex(8)}.tmp"
        replaced = False
        try:
            try:
                _bind_source_path(source_path, source_fd, evidence.signature)
                os.link(source_path, temporary)
                strategy: Literal["hardlink", "reflink", "copy"] = "hardlink"
            except OSError as exc:
                if exc.errno not in _FALLBACK_ERRNOS:
                    raise
                try:
                    _reflink(source_fd, temporary, evidence.mode)
                    strategy = "reflink"
                except OSError as reflink_exc:
                    if reflink_exc.errno not in _FALLBACK_ERRNOS:
                        raise
                    _copy_file(source_fd, temporary, evidence.mode)
                    strategy = "copy"
            _validate_temporary(temporary, source_fd, evidence, strategy=strategy)
            _fsync_file(temporary)
            os.replace(temporary, destination_path)
            replaced = True
            _fsync_directory(destination_path.parent)
            return JpegPublication(path=destination_path, strategy=strategy)
        finally:
            if not replaced:
                _unlink_best_effort(temporary)
    finally:
        os.close(source_fd)


def open_decoded_rgb_jpeg(path: Path, *, initial_max_dimension: int) -> AbstractContextManager[DecodedRgbJpeg]:
    return _decoded_rgb_jpeg(Path(path), initial_max_dimension=initial_max_dimension)


@contextmanager
def _decoded_rgb_jpeg(path: Path, *, initial_max_dimension: int) -> Iterator[DecodedRgbJpeg]:
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


def _reflink(source_fd: int, temporary: Path, source_mode: int) -> None:
    destination_fd = -1
    try:
        destination_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, source_mode)
        fcntl.ioctl(destination_fd, _FICLONE, source_fd)
        os.fchmod(destination_fd, source_mode)
    except Exception:
        if destination_fd >= 0:
            os.close(destination_fd)
            destination_fd = -1
        _unlink_best_effort(temporary)
        raise
    finally:
        if destination_fd >= 0:
            os.close(destination_fd)


def _copy_file(source_fd: int, temporary: Path, source_mode: int) -> None:
    destination_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, source_mode)
    try:
        os.lseek(source_fd, 0, os.SEEK_SET)
        while chunk := os.read(source_fd, _COPY_CHUNK_BYTES):
            _write_all(destination_fd, chunk)
        os.fchmod(destination_fd, source_mode)
    finally:
        os.close(destination_fd)


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


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


def _bind_source_path(path: Path, source_fd: int, expected: tuple[int, int, int, int]) -> None:
    try:
        path_value = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc
    if _stat_signature(path_value) != expected or _descriptor_signature(source_fd) != expected:
        raise JpegDecodeError("read_failed")


def _validate_temporary(
    temporary: Path,
    source_fd: int,
    evidence: _SourceEvidence,
    *,
    strategy: Literal["hardlink", "reflink", "copy"],
) -> None:
    if _descriptor_signature(source_fd) != evidence.signature:
        raise JpegDecodeError("read_failed")
    try:
        temporary_fd = os.open(temporary, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc
    try:
        temporary_signature = _descriptor_signature(temporary_fd)
        if strategy == "hardlink" and temporary_signature[:2] != evidence.signature[:2]:
            raise JpegDecodeError("read_failed")
        if _digest_descriptor(temporary_fd) != evidence.digest:
            raise JpegDecodeError("read_failed")
        if _descriptor_signature(temporary_fd) != temporary_signature:
            raise JpegDecodeError("read_failed")
        if _descriptor_signature(source_fd) != evidence.signature:
            raise JpegDecodeError("read_failed")
    finally:
        os.close(temporary_fd)


def _digest_descriptor(descriptor: int) -> bytes:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while chunk := os.read(descriptor, _COPY_CHUNK_BYTES):
        digest.update(chunk)
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.digest()


def _descriptor_signature(descriptor: int) -> tuple[int, int, int, int]:
    return _stat_signature(os.fstat(descriptor))


def _stat_signature(value: os.stat_result) -> tuple[int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


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


def _unlink_best_effort(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _fsync_directory(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
