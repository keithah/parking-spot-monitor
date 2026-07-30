"""Identity-bound disposal inside a cooperatively owned directory.

Linux has no pathname unlink operation conditional on an already-open inode.
We therefore move candidates to unguessable names, bind and recheck their
identity, and keep the final stat/unlink window minimal.  This contract assumes
noncooperating writers cannot modify the held application-owned directory.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import secrets
import stat

_IDENTITY_OPEN_FLAGS = getattr(os, "O_PATH", os.O_RDONLY) | getattr(os, "O_NOFOLLOW", 0)
_DISPOSAL_PREFIX = ".dispose."


@dataclass(frozen=True, slots=True)
class FileIdentity:
    dev: int
    ino: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> FileIdentity:
        return cls(value.st_dev, value.st_ino)


def dispose_owned_name_at(directory_fd: int, name: str, identity: FileIdentity) -> bool:
    """Randomize a pathname before deleting only its bound regular inode."""

    if not name or name in {".", ".."} or Path(name).name != name or Path(name).is_absolute():
        return False
    try:
        disposal = _fresh_disposal_name(directory_fd)
        os.rename(name, disposal, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
    except OSError:
        return False
    descriptor = -1
    try:
        descriptor = os.open(disposal, _IDENTITY_OPEN_FLAGS, dir_fd=directory_fd)
        value = os.fstat(descriptor)
        bound = FileIdentity.from_stat(value)
        if not stat.S_ISREG(value.st_mode) or bound != identity:
            _restore_disposal(directory_fd, disposal, name, bound)
            return False
        if not _same_regular_identity(directory_fd, disposal, identity):
            _restore_current_disposal(directory_fd, disposal, name)
            return False
        try:
            os.unlink(disposal, dir_fd=directory_fd)
            os.fsync(directory_fd)
            return True
        except OSError:
            _restore_disposal(directory_fd, disposal, name, identity)
            return False
    except OSError:
        _restore_current_disposal(directory_fd, disposal, name)
        return False
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def same_regular_identity_at(directory_fd: int, name: str, identity: FileIdentity) -> bool:
    return _same_regular_identity(directory_fd, name, identity)


def _fresh_disposal_name(directory_fd: int) -> str:
    for _ in range(8):
        candidate = f"{_DISPOSAL_PREFIX}{secrets.token_hex(16)}"
        try:
            os.stat(candidate, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return candidate
    raise OSError("could not allocate a fresh disposal name")


def _restore_current_disposal(directory_fd: int, disposal: str, name: str) -> bool:
    try:
        value = os.stat(disposal, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    if not stat.S_ISREG(value.st_mode):
        return False
    return _restore_disposal(directory_fd, disposal, name, FileIdentity.from_stat(value))


def _restore_disposal(
    directory_fd: int, disposal: str, name: str, identity: FileIdentity
) -> bool:
    try:
        os.link(disposal, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    if not _same_regular_identity(directory_fd, name, identity):
        return False
    if not _same_regular_identity(directory_fd, disposal, identity):
        return False
    try:
        os.unlink(disposal, dir_fd=directory_fd)
        os.fsync(directory_fd)
        return True
    except OSError:
        return False


def _same_regular_identity(directory_fd: int, name: str, identity: FileIdentity) -> bool:
    try:
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    return stat.S_ISREG(current.st_mode) and FileIdentity.from_stat(current) == identity
