"""Durable JPEG publication and shared decode lifecycles."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import errno
import fcntl
import os
from pathlib import Path
import secrets
import shutil
import stat
from types import MappingProxyType
from typing import Any, Literal
import warnings

from PIL import Image, UnidentifiedImageError

_FICLONE = 0x40049409
_COPY_CHUNK_BYTES = 1024 * 1024
_MAX_UPLOAD_DERIVATIVE_BYTES = 300_000
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
    initial_signature = _file_signature(source_path)
    _validate_jpeg(source_path)
    validated_signature = _file_signature(source_path)
    if validated_signature != initial_signature:
        raise JpegDecodeError("read_failed")
    source_mode = stat.S_IMODE(source_path.stat().st_mode)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination_path.parent / f".{destination_path.name}.{secrets.token_hex(8)}.tmp"
    replaced = False
    try:
        try:
            os.link(source_path, temporary)
            strategy: Literal["hardlink", "reflink", "copy"] = "hardlink"
        except OSError as exc:
            if exc.errno not in _FALLBACK_ERRNOS:
                raise
            try:
                _reflink(source_path, temporary, source_mode)
                strategy = "reflink"
            except OSError as reflink_exc:
                if reflink_exc.errno not in _FALLBACK_ERRNOS:
                    raise
                _copy_file(source_path, temporary, source_mode)
                strategy = "copy"
        if _file_signature(source_path) != validated_signature:
            raise JpegDecodeError("read_failed")
        if strategy == "hardlink":
            temporary_stat = temporary.stat()
            if (temporary_stat.st_dev, temporary_stat.st_ino) != validated_signature[:2]:
                raise JpegDecodeError("read_failed")
        _fsync_file(temporary)
        os.replace(temporary, destination_path)
        replaced = True
        _fsync_directory(destination_path.parent)
        return JpegPublication(path=destination_path, strategy=strategy)
    finally:
        if not replaced:
            _unlink_best_effort(temporary)


def open_decoded_rgb_jpeg(path: Path, *, initial_max_dimension: int) -> AbstractContextManager[DecodedRgbJpeg]:
    return _decoded_rgb_jpeg(Path(path), initial_max_dimension=initial_max_dimension)


@dataclass(frozen=True, slots=True)
class MatrixUploadDerivative:
    path: Path
    info: Mapping[str, int | str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "info", MappingProxyType(dict(self.info)))


def prepare_upload_derivative(snapshot: Any, *, destination: Path, logger: Any | None = None) -> MatrixUploadDerivative:
    from parking_spot_monitor.matrix_snapshots import _matrix_snapshot_upload

    upload = _matrix_snapshot_upload(snapshot, logger=logger)
    data = bytes(upload["data"])
    info = _validate_derivative_info(upload["info"], expected_size=len(data))
    _publish_bytes(destination, data)
    return MatrixUploadDerivative(path=destination, info=info)


def load_upload_derivative(path: Path, info: Mapping[str, object]) -> MatrixUploadDerivative:
    validated = _validate_derivative_info(info)
    if validated["size"] > _MAX_UPLOAD_DERIVATIVE_BYTES:
        raise JpegDecodeError("read_failed")
    return MatrixUploadDerivative(path=path, info=validated)


def read_upload_derivative_bytes(derivative: MatrixUploadDerivative) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(derivative.path, flags)
        file_stat = os.fstat(descriptor)
        expected_size = derivative.info["size"]
        if (
            not isinstance(expected_size, int)
            or not stat.S_ISREG(file_stat.st_mode)
            or file_stat.st_size != expected_size
            or file_stat.st_size > _MAX_UPLOAD_DERIVATIVE_BYTES
        ):
            raise OSError("upload derivative metadata mismatch")
        payload = bytearray()
        while len(payload) < file_stat.st_size:
            chunk = os.read(descriptor, file_stat.st_size - len(payload))
            if not chunk:
                raise OSError("upload derivative read was incomplete")
            payload.extend(chunk)
        return bytes(payload)
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def upload_derivative_path(snapshot_path: Path) -> Path:
    return snapshot_path.parent / ".upload-derivatives" / snapshot_path.name


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


def _validate_jpeg(path: Path) -> None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(path) as image:
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


def _reflink(source: Path, temporary: Path, source_mode: int) -> None:
    source_fd = os.open(source, os.O_RDONLY)
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
        os.close(source_fd)


def _copy_file(source: Path, temporary: Path, source_mode: int) -> None:
    destination_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, source_mode)
    try:
        with source.open("rb") as reader, os.fdopen(destination_fd, "wb", closefd=False) as writer:
            shutil.copyfileobj(reader, writer, length=_COPY_CHUNK_BYTES)
            writer.flush()
        os.fchmod(destination_fd, source_mode)
    finally:
        os.close(destination_fd)


def _publish_bytes(destination: Path, data: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.{secrets.token_hex(8)}.tmp"
    replaced = False
    descriptor = -1
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(data)
            handle.flush()
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, destination)
        replaced = True
        _fsync_directory(destination.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not replaced:
            _unlink_best_effort(temporary)


def _validate_derivative_info(info: Mapping[str, object], *, expected_size: int | None = None) -> dict[str, int | str]:
    if not isinstance(info, Mapping):
        raise JpegDecodeError("read_failed")
    mimetype = info.get("mimetype")
    size, width, height = info.get("size"), info.get("w"), info.get("h")
    if mimetype != "image/jpeg" or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in (size, width, height)):
        raise JpegDecodeError("read_failed")
    if expected_size is not None and size != expected_size:
        raise JpegDecodeError("read_failed")
    return {"mimetype": "image/jpeg", "size": size, "w": width, "h": height}


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _file_signature(path: Path) -> tuple[int, int, int, int]:
    try:
        value = path.stat()
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


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
