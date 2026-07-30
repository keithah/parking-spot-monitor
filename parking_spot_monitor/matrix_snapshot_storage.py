"""Symlink-safe rooted storage for Matrix snapshot artifacts."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import secrets
import stat
from types import MappingProxyType

from parking_spot_monitor.jpeg_artifacts import jpeg_bytes_dimensions

_COPY_CHUNK_BYTES = 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
MAX_RETAINED_JPEG_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class RootedJpegEvidence:
    data: bytes
    info: Mapping[str, int | str]
    sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", bytes(self.data))
        object.__setattr__(self, "info", MappingProxyType(dict(self.info)))


def absolute_snapshot_root(path: Path) -> Path:
    return Path(os.path.abspath(path))


def safe_artifact_name(value: str) -> str:
    if not isinstance(value, str) or not value or Path(value).name != value or Path(value).is_absolute():
        raise OSError("artifact filename must be a basename")
    return value


def artifact_path(snapshot_root: Path, directory: str | None, filename: str) -> Path:
    root = absolute_snapshot_root(snapshot_root)
    safe_name = safe_artifact_name(filename)
    return root / safe_name if directory is None else root / safe_artifact_name(directory) / safe_name


def ensure_owned_directory(path: Path) -> Path:
    with _directory(path, create=True):
        return absolute_snapshot_root(path)


def publish_owned_bytes(
    snapshot_root: Path,
    directory: str | None,
    filename: str,
    payload: bytes,
    *,
    mode: int,
) -> Path:
    safe_name = safe_artifact_name(filename)
    with _artifact_directory(snapshot_root, directory, create=True) as (directory_fd, parent):
        temporary_name = f".{safe_name}.{secrets.token_hex(8)}.tmp"
        temporary_fd = -1
        replaced = False
        try:
            temporary_fd = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                mode,
                dir_fd=directory_fd,
            )
            _write_all(temporary_fd, payload)
            os.fchmod(temporary_fd, mode)
            os.fsync(temporary_fd)
            os.close(temporary_fd)
            temporary_fd = -1
            os.replace(temporary_name, safe_name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
            replaced = True
            os.fsync(directory_fd)
        finally:
            if temporary_fd >= 0:
                os.close(temporary_fd)
            if not replaced:
                _unlink_best_effort(directory_fd, temporary_name)
        return parent / safe_name


def read_owned_bytes(
    snapshot_root: Path,
    directory: str | None,
    filename: str,
    *,
    max_bytes: int,
) -> bytes:
    with _artifact_directory(snapshot_root, directory, create=False) as (directory_fd, _parent):
        file_fd = os.open(
            safe_artifact_name(filename),
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        try:
            before = _descriptor_signature(file_fd)
            if not stat.S_ISREG(os.fstat(file_fd).st_mode) or not 0 < before[2] <= max_bytes:
                raise OSError("artifact is not a bounded regular file")
            payload = _read_exact(file_fd, before[2])
            if _descriptor_signature(file_fd) != before:
                raise OSError("artifact changed while reading")
            return payload
        finally:
            os.close(file_fd)


def read_owned_jpeg_evidence(
    snapshot_root: Path,
    filename: str,
    *,
    max_bytes: int = MAX_RETAINED_JPEG_BYTES,
) -> RootedJpegEvidence:
    """Capture validated JPEG bytes and metadata from one rooted descriptor."""

    with _artifact_directory(snapshot_root, None, create=False) as (root_fd, _root):
        file_fd = os.open(
            safe_artifact_name(filename),
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_fd,
        )
        try:
            before = _descriptor_signature(file_fd)
            if not stat.S_ISREG(os.fstat(file_fd).st_mode) or not 0 < before[2] <= max_bytes:
                raise OSError("snapshot is not a bounded regular file")
            payload = _read_exact(file_fd, before[2])
            payload_digest = hashlib.sha256(payload).digest()
            width, height = jpeg_bytes_dimensions(payload)
            if _descriptor_signature(file_fd) != before:
                raise OSError("snapshot changed while reading")
            if _digest_descriptor(file_fd) != payload_digest or _descriptor_signature(file_fd) != before:
                raise OSError("snapshot changed while reading")
            return RootedJpegEvidence(
                data=payload,
                info={"mimetype": "image/jpeg", "size": len(payload), "w": width, "h": height},
                sha256=payload_digest.hex(),
            )
        finally:
            os.close(file_fd)


def validate_owned_file(snapshot_root: Path, directory: str | None, filename: str) -> Path:
    with _artifact_directory(snapshot_root, directory, create=False) as (directory_fd, parent):
        safe_name = safe_artifact_name(filename)
        file_fd = os.open(safe_name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory_fd)
        try:
            if not stat.S_ISREG(os.fstat(file_fd).st_mode):
                raise OSError("artifact is not a regular file")
        finally:
            os.close(file_fd)
        return parent / safe_name


def delete_owned_artifact(snapshot_root: Path, directory: str | None, filename: str) -> int:
    try:
        context = _artifact_directory(snapshot_root, directory, create=False)
        with context as (directory_fd, _parent):
            safe_name = safe_artifact_name(filename)
            try:
                value = os.stat(safe_name, dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                return 0
            if directory is None and not stat.S_ISREG(value.st_mode):
                raise OSError("retained snapshot is not a regular file")
            os.unlink(safe_name, dir_fd=directory_fd)
            os.fsync(directory_fd)
            return value.st_size
    except FileNotFoundError:
        return 0


def publish_retained_snapshot(source: Path, snapshot_root: Path, filename: str) -> Path:
    source_fd = _open_source_file(source)
    safe_name = safe_artifact_name(filename)
    try:
        source_before = _descriptor_signature(source_fd)
        if not stat.S_ISREG(os.fstat(source_fd).st_mode):
            raise OSError("snapshot source is not a regular file")
        with _artifact_directory(snapshot_root, None, create=True) as (root_fd, root):
            temporary_name = f".{safe_name}.{secrets.token_hex(8)}.tmp"
            temporary_fd = -1
            replaced = False
            try:
                temporary_fd = os.open(
                    temporary_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o644,
                    dir_fd=root_fd,
                )
                while chunk := os.read(source_fd, _COPY_CHUNK_BYTES):
                    _write_all(temporary_fd, chunk)
                if _descriptor_signature(source_fd) != source_before:
                    raise OSError("snapshot source changed while copying")
                os.fchmod(temporary_fd, 0o644)
                os.fsync(temporary_fd)
                os.close(temporary_fd)
                temporary_fd = -1
                os.replace(temporary_name, safe_name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
                replaced = True
                os.fsync(root_fd)
                return root / safe_name
            finally:
                if temporary_fd >= 0:
                    os.close(temporary_fd)
                if not replaced:
                    _unlink_best_effort(root_fd, temporary_name)
    finally:
        os.close(source_fd)


def secure_snapshot_candidates(snapshot_root: Path) -> list[Path]:
    with _directory(snapshot_root, create=False) as (root_fd, root):
        candidates: list[Path] = []
        for name in os.listdir(root_fd):
            try:
                value = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            except OSError:
                continue
            if stat.S_ISREG(value.st_mode):
                candidates.append(root / name)
        return candidates


@contextmanager
def _artifact_directory(
    snapshot_root: Path, directory: str | None, *, create: bool
) -> Iterator[tuple[int, Path]]:
    with _directory(snapshot_root, create=create) as (root_fd, root):
        if directory is None:
            yield root_fd, root
            return
        safe_directory = safe_artifact_name(directory)
        if create:
            try:
                os.mkdir(safe_directory, 0o700, dir_fd=root_fd)
            except FileExistsError:
                pass
        child_fd = os.open(safe_directory, _DIRECTORY_FLAGS, dir_fd=root_fd)
        try:
            yield child_fd, root / safe_directory
        finally:
            os.close(child_fd)


@contextmanager
def _directory(path: Path, *, create: bool) -> Iterator[tuple[int, Path]]:
    absolute = absolute_snapshot_root(path)
    descriptor = os.open(os.sep, _DIRECTORY_FLAGS)
    try:
        for part in absolute.parts[1:]:
            if create:
                try:
                    os.mkdir(part, 0o755, dir_fd=descriptor)
                except FileExistsError:
                    pass
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor, absolute
    finally:
        os.close(descriptor)


def _open_source_file(path: Path) -> int:
    absolute = absolute_snapshot_root(path)
    with _directory(absolute.parent, create=False) as (parent_fd, _parent):
        return os.open(absolute.name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=parent_fd)


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    try:
        offset = 0
        while offset < len(view):
            written = os.write(descriptor, view[offset:])
            if written <= 0:
                raise OSError("artifact write made no progress")
            offset += written
    finally:
        view.release()


def _read_exact(descriptor: int, size: int) -> bytes:
    payload = bytearray()
    while len(payload) < size:
        chunk = os.read(descriptor, size - len(payload))
        if not chunk:
            raise OSError("artifact read was incomplete")
        payload.extend(chunk)
    return bytes(payload)


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


def _unlink_best_effort(directory_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=directory_fd)
    except OSError:
        pass
