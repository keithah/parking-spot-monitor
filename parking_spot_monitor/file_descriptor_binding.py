"""Rooted directory ownership and identity-safe artifact operations."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from io import BufferedReader
import os
from pathlib import Path
import stat

from parking_spot_monitor.owned_file_cleanup import FileIdentity, recover_quarantined_at, unlink_owned_at

_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


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

    def open_child(self, name: str, *, create: bool) -> RootedDirectoryOwner:
        safe_name = safe_basename(name)
        if create:
            try:
                os.mkdir(safe_name, 0o755, dir_fd=self.fd)
                os.fsync(self.fd)
            except FileExistsError:
                pass
        child = object.__new__(RootedDirectoryOwner)
        child.path = self.path / safe_name
        child.fd = os.open(safe_name, _DIRECTORY_FLAGS, dir_fd=self.fd)
        child.identity = FileIdentity.from_stat(os.fstat(child.fd))
        return child

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


def unlink_owned_path(path: Path, identity: FileIdentity) -> bool:
    try:
        with RootedDirectoryOwner(path.parent, create=False) as owner:
            return owner.unlink_if_matches(path.name, identity)
    except OSError:
        return False


def recover_quarantined_path(path: Path) -> int:
    """Restore a deferred cleanup quarantine once its original name is free."""
    try:
        with RootedDirectoryOwner(path.parent, create=False) as owner:
            return recover_quarantined_at(owner.fd, path.name)
    except OSError:
        return 0


@contextmanager
def open_owned_path(path: Path, identity: FileIdentity) -> Iterator[BufferedReader]:
    with RootedDirectoryOwner(path.parent, create=False) as owner:
        with open_owned_at(owner, path.name, identity) as handle:
            yield handle


@contextmanager
def open_owned_at(owner: RootedDirectoryOwner, name: str, identity: FileIdentity) -> Iterator[BufferedReader]:
    descriptor = owner.open_file(name, os.O_RDONLY)
    try:
        if descriptor_identity(descriptor) != identity or not owner.matches(name, identity):
            raise OSError("artifact identity changed")
        if not owner.is_still_bound():
            raise OSError("artifact parent changed")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            yield handle
            if not owner.matches(name, identity) or not owner.is_still_bound():
                raise OSError("artifact binding changed")
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def safe_basename(value: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value or Path(value).is_absolute():
        raise OSError("artifact name must be a basename")
    return value


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
