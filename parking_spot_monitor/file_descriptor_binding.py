"""Rooted directory ownership and identity-safe artifact operations."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from io import BufferedReader
import os
from pathlib import Path
import secrets
import stat

_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


@dataclass(frozen=True, slots=True)
class FileIdentity:
    dev: int
    ino: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> FileIdentity:
        return cls(value.st_dev, value.st_ino)


@dataclass(frozen=True, slots=True)
class OwnedFile:
    path: Path
    identity: FileIdentity


class RootedDirectoryOwner:
    """Retain a no-symlink directory walk for descriptor-relative file work."""

    def __init__(self, path: Path, *, create: bool) -> None:
        self.path = Path(path)
        self.fd = _open_directory(self.path, create=create)
        self.identity = FileIdentity.from_stat(os.fstat(self.fd))

    def __enter__(self) -> RootedDirectoryOwner:
        return self

    def __exit__(self, *args: object) -> None:
        os.close(self.fd)

    def open_file(self, name: str, flags: int, mode: int = 0o666) -> int:
        return os.open(safe_basename(name), flags | getattr(os, "O_NOFOLLOW", 0), mode, dir_fd=self.fd)

    def open_identity(self, name: str) -> int:
        return os.open(
            safe_basename(name),
            getattr(os, "O_PATH", os.O_RDONLY) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=self.fd,
        )

    def replace(self, source_name: str, destination_name: str) -> None:
        os.replace(
            safe_basename(source_name),
            safe_basename(destination_name),
            src_dir_fd=self.fd,
            dst_dir_fd=self.fd,
        )

    def matches(self, name: str, identity: FileIdentity, *, regular: bool = True) -> bool:
        try:
            value = os.stat(safe_basename(name), dir_fd=self.fd, follow_symlinks=False)
        except OSError:
            return False
        return (not regular or stat.S_ISREG(value.st_mode)) and FileIdentity.from_stat(value) == identity

    def unlink_if_matches(self, name: str, identity: FileIdentity) -> bool:
        return unlink_owned_at(self.fd, name, identity)

    def fsync(self) -> None:
        os.fsync(self.fd)

    def is_still_bound(self) -> bool:
        try:
            rebound_fd = _open_directory(self.path, create=False)
        except OSError:
            return False
        try:
            return FileIdentity.from_stat(os.fstat(rebound_fd)) == self.identity
        finally:
            os.close(rebound_fd)


def descriptor_identity(descriptor: int) -> FileIdentity:
    return FileIdentity.from_stat(os.fstat(descriptor))


def unlink_owned_at(directory_fd: int, name: str, identity: FileIdentity) -> bool:
    safe_name = safe_basename(name)
    quarantine = f".{safe_name}.{secrets.token_hex(8)}.quarantine"
    try:
        os.rename(safe_name, quarantine, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
    except FileNotFoundError:
        return False
    try:
        quarantined = os.stat(quarantine, dir_fd=directory_fd, follow_symlinks=False)
        if FileIdentity.from_stat(quarantined) != identity:
            _restore_quarantined(directory_fd, quarantine, safe_name)
            return False
        os.unlink(quarantine, dir_fd=directory_fd)
        os.fsync(directory_fd)
        return True
    except OSError:
        _restore_quarantined(directory_fd, quarantine, safe_name)
        return False


def unlink_owned_path(path: Path, identity: FileIdentity) -> bool:
    try:
        with RootedDirectoryOwner(path.parent, create=False) as owner:
            return owner.unlink_if_matches(path.name, identity)
    except OSError:
        return False


@contextmanager
def open_owned_path(path: Path, identity: FileIdentity) -> Iterator[BufferedReader]:
    with RootedDirectoryOwner(path.parent, create=False) as owner:
        descriptor = owner.open_file(path.name, os.O_RDONLY)
        try:
            if descriptor_identity(descriptor) != identity or not owner.matches(path.name, identity):
                raise OSError("artifact identity changed")
            if not owner.is_still_bound():
                raise OSError("artifact parent changed")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                yield handle
                if not owner.matches(path.name, identity) or not owner.is_still_bound():
                    raise OSError("artifact binding changed")
        finally:
            if descriptor >= 0:
                os.close(descriptor)


def safe_basename(value: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value or Path(value).is_absolute():
        raise OSError("artifact name must be a basename")
    return value


def _restore_quarantined(directory_fd: int, quarantine: str, name: str) -> bool:
    try:
        os.link(quarantine, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd, follow_symlinks=False)
        os.unlink(quarantine, dir_fd=directory_fd)
        os.fsync(directory_fd)
        return True
    except OSError:
        return False


def _open_directory(path: Path, *, create: bool) -> int:
    selected = Path(path)
    descriptor = os.open(os.sep if selected.is_absolute() else os.curdir, _DIRECTORY_FLAGS)
    try:
        parts = selected.parts[1:] if selected.is_absolute() else selected.parts
        for part in parts:
            if part in {"", "."}:
                continue
            if part == "..":
                raise OSError("parent traversal is not allowed")
            if create:
                try:
                    os.mkdir(part, 0o755, dir_fd=descriptor)
                    os.fsync(descriptor)
                except FileExistsError:
                    pass
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except Exception:
        os.close(descriptor)
        raise
