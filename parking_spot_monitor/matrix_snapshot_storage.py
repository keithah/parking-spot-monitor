"""Symlink-safe rooted storage for Matrix snapshot artifacts."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
import secrets
import stat
from types import MappingProxyType
from typing import Literal

from parking_spot_monitor.bounded_descriptor_io import descriptor_signature, read_descriptor_exact
from parking_spot_monitor.file_descriptor_binding import (
    FileIdentity,
    RootedDirectoryOwner,
    descriptor_identity,
    unlink_owned_at,
    unlink_owned_at_result,
)
from parking_spot_monitor.owned_file_recovery import RecoveryResult, recover_owned_directory_at
from parking_spot_monitor.owned_directory_durability import ensure_child_directory_durable
from parking_spot_monitor.jpeg_artifacts import jpeg_bytes_dimensions
from parking_spot_monitor.matrix_retained_publication import MAX_RETAINED_JPEG_BYTES, publish_retained_snapshot

_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_DEFAULT_UNLINK_OWNED_AT = unlink_owned_at


@dataclass(frozen=True, slots=True)
class RootedJpegEvidence:
    data: bytes
    info: Mapping[str, int | str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", bytes(self.data))
        object.__setattr__(self, "info", MappingProxyType(dict(self.info)))


@dataclass(frozen=True, slots=True)
class OwnedArtifactDeleteResult:
    status: Literal["deleted", "missing", "failed"]
    bytes_deleted: int = 0
    durable: bool = True


def absolute_snapshot_root(path: Path) -> Path:
    return Path(os.path.abspath(path))


def safe_artifact_name(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or Path(value).name != value
        or Path(value).is_absolute()
    ):
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
            try:
                payload = read_descriptor_exact(file_fd, before, capture_bytes=True).data
            except OSError as exc:
                raise OSError("artifact changed while reading") from exc
            if payload is None:
                raise OSError("artifact bytes were not captured")
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
    expected_identity: FileIdentity | None = None,
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
            if expected_identity is not None and descriptor_identity(file_fd) != expected_identity:
                raise OSError("snapshot identity changed")
            if not stat.S_ISREG(os.fstat(file_fd).st_mode) or not 0 < before[2] <= max_bytes:
                raise OSError("snapshot is not a bounded regular file")
            try:
                captured = read_descriptor_exact(file_fd, before, capture_bytes=True)
                payload = captured.data
            except OSError as exc:
                raise OSError("snapshot changed while reading") from exc
            if payload is None:
                raise OSError("snapshot bytes were not captured")
            width, height = jpeg_bytes_dimensions(payload)
            try:
                stable_digest = read_descriptor_exact(file_fd, before).digest
            except OSError as exc:
                raise OSError("snapshot changed while reading") from exc
            if stable_digest != captured.digest or _descriptor_signature(file_fd) != before:
                raise OSError("snapshot changed while reading")
            return RootedJpegEvidence(
                data=payload,
                info={"mimetype": "image/jpeg", "size": len(payload), "w": width, "h": height},
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


def delete_owned_artifact(
    snapshot_root: Path,
    directory: str | None,
    filename: str,
    *,
    expected_identity: FileIdentity | None = None,
) -> OwnedArtifactDeleteResult:
    safe_name = safe_artifact_name(filename)
    safe_directory = safe_artifact_name(directory) if directory is not None else None
    try:
        context = _artifact_directory(snapshot_root, safe_directory, create=False)
        with context as (directory_fd, _parent):
            try:
                value = os.stat(safe_name, dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                return OwnedArtifactDeleteResult("missing")
            if directory is None and not stat.S_ISREG(value.st_mode):
                return OwnedArtifactDeleteResult("failed")
            intended_identity = expected_identity or FileIdentity.from_stat(value)
            deletion = (
                unlink_owned_at_result(directory_fd, safe_name, intended_identity)
                if unlink_owned_at is _DEFAULT_UNLINK_OWNED_AT
                else None
            )
            if deletion is None:
                if unlink_owned_at(directory_fd, safe_name, intended_identity):
                    return OwnedArtifactDeleteResult("deleted", value.st_size)
                return OwnedArtifactDeleteResult("failed")
            if deletion.deleted:
                return OwnedArtifactDeleteResult("deleted", value.st_size, deletion.durable)
            return OwnedArtifactDeleteResult("failed")
    except FileNotFoundError:
        return OwnedArtifactDeleteResult("missing")
    except OSError:
        return OwnedArtifactDeleteResult("failed")


def recover_owned_artifacts(snapshot_root: Path, directory: str | None) -> RecoveryResult:
    try:
        with _artifact_directory(snapshot_root, directory, create=False) as (directory_fd, _parent):
            return recover_owned_directory_at(directory_fd)
    except FileNotFoundError:
        return RecoveryResult()
    except OSError:
        return RecoveryResult(pending=True)


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
            ensure_child_directory_durable(root_fd, safe_directory)
        child_fd = os.open(safe_directory, _DIRECTORY_FLAGS, dir_fd=root_fd)
        try:
            yield child_fd, root / safe_directory
        finally:
            os.close(child_fd)


@contextmanager
def _directory(path: Path, *, create: bool) -> Iterator[tuple[int, Path]]:
    absolute = absolute_snapshot_root(path)
    with RootedDirectoryOwner(absolute, create=create) as owner:
        if not owner.is_still_bound():
            raise OSError("snapshot root changed")
        yield owner.fd, absolute
        if not owner.is_still_bound():
            raise OSError("snapshot root changed")


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


def _descriptor_signature(descriptor: int) -> tuple[int, int, int, int, int]:
    return descriptor_signature(descriptor)


def _unlink_best_effort(directory_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=directory_fd)
    except OSError:
        pass
