"""Crash-recoverable identity disposal in a cooperatively owned directory.

Linux has no pathname unlink operation conditional on an already-open inode.
Candidates therefore move to exact, unguessable disposal names before identity
binding.  The minimal final stat/unlink window still assumes noncooperating
writers cannot modify the held application-owned directory.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import secrets
import stat
from typing import Literal

from parking_spot_monitor.owned_disposal_manifest import (
    DisposalManifestEntry,
    forget_disposal_at,
    record_disposal_at,
)

_IDENTITY_OPEN_FLAGS = getattr(os, "O_PATH", os.O_RDONLY) | getattr(os, "O_NOFOLLOW", 0)


@dataclass(frozen=True, slots=True)
class FileIdentity:
    dev: int
    ino: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> FileIdentity:
        return cls(value.st_dev, value.st_ino)


@dataclass(frozen=True, slots=True)
class DisposalResult:
    status: Literal["deleted", "pending", "restored"]
    durable: bool = True


def disposal_pattern(name: str) -> re.Pattern[str]:
    safe_name = _safe_basename(name)
    return re.compile(rf"^\.{re.escape(safe_name)}\.[0-9a-f]{{16}}\.dispose$")


def dispose_owned_name_at(
    directory_fd: int,
    name: str,
    identity: FileIdentity,
    *,
    recovery_name: str,
) -> DisposalResult:
    """Move a source to recoverable random disposal before deleting it."""

    safe_source = _safe_basename(name)
    safe_recovery = _safe_basename(recovery_name)
    disposal = ""
    try:
        disposal = _fresh_disposal_name(directory_fd, safe_recovery)
        if not record_disposal_at(
            directory_fd,
            DisposalManifestEntry(disposal, safe_recovery, identity.dev, identity.ino),
        ):
            return DisposalResult("restored")
        os.rename(safe_source, disposal, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
    except OSError:
        if disposal:
            forget_disposal_at(directory_fd, disposal)
        return DisposalResult("restored")
    bound = _bound_regular_identity(directory_fd, disposal)
    if bound != identity:
        _restore_disposal(directory_fd, disposal, safe_source, bound)
        forget_disposal_at(directory_fd, disposal)
        return DisposalResult("restored")
    if not _same_regular_identity(directory_fd, disposal, identity):
        _restore_current_disposal(directory_fd, disposal, safe_source)
        forget_disposal_at(directory_fd, disposal)
        return DisposalResult("restored")
    result = _unlink_disposal(directory_fd, disposal, identity)
    if result.status == "deleted" and result.durable:
        forget_disposal_at(directory_fd, disposal)
    return result


def recover_disposal_at(
    directory_fd: int, disposal: str, recovery_name: str, *, expected_identity: FileIdentity | None = None
) -> DisposalResult:
    """Restore one interrupted disposal and finish its randomized-name unlink."""

    safe_disposal = _safe_basename(disposal)
    safe_recovery = _safe_basename(recovery_name)
    if not disposal_pattern(safe_recovery).fullmatch(safe_disposal):
        return DisposalResult("pending")
    identity = _bound_regular_identity(directory_fd, safe_disposal)
    if identity is None or (expected_identity is not None and identity != expected_identity):
        return DisposalResult("pending")
    try:
        current = os.stat(safe_recovery, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        try:
            os.link(
                safe_disposal,
                safe_recovery,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError:
            return DisposalResult("pending")
    except OSError:
        return DisposalResult("pending")
    if not _same_regular_identity(directory_fd, safe_recovery, identity):
        return DisposalResult("pending")
    if not _same_regular_identity(directory_fd, safe_disposal, identity):
        return DisposalResult("pending")
    result = _unlink_disposal(directory_fd, safe_disposal, identity)
    if result.status == "deleted" and result.durable:
        forget_disposal_at(directory_fd, safe_disposal)
    return result


def same_regular_identity_at(directory_fd: int, name: str, identity: FileIdentity) -> bool:
    return _same_regular_identity(directory_fd, name, identity)


def _fresh_disposal_name(directory_fd: int, recovery_name: str) -> str:
    for _ in range(8):
        candidate = f".{recovery_name}.{secrets.token_hex(8)}.dispose"
        try:
            os.stat(candidate, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return candidate
    raise OSError("could not allocate a fresh disposal name")


def _bound_regular_identity(directory_fd: int, name: str) -> FileIdentity | None:
    descriptor = -1
    try:
        descriptor = os.open(name, _IDENTITY_OPEN_FLAGS, dir_fd=directory_fd)
        value = os.fstat(descriptor)
        return FileIdentity.from_stat(value) if stat.S_ISREG(value.st_mode) else None
    except OSError:
        return None
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _unlink_disposal(directory_fd: int, disposal: str, identity: FileIdentity) -> DisposalResult:
    if not _same_regular_identity(directory_fd, disposal, identity):
        return DisposalResult("pending")
    try:
        os.unlink(disposal, dir_fd=directory_fd)
    except OSError:
        return DisposalResult("pending")
    try:
        os.fsync(directory_fd)
    except OSError:
        return DisposalResult("deleted", durable=False)
    return DisposalResult("deleted")


def _restore_current_disposal(directory_fd: int, disposal: str, name: str) -> bool:
    identity = _bound_regular_identity(directory_fd, disposal)
    return identity is not None and _restore_disposal(directory_fd, disposal, name, identity)


def _restore_disposal(
    directory_fd: int,
    disposal: str,
    name: str,
    identity: FileIdentity | None,
) -> bool:
    if identity is None:
        return False
    try:
        os.link(disposal, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    if not _same_regular_identity(directory_fd, name, identity):
        return False
    return _unlink_disposal(directory_fd, disposal, identity).status == "deleted"


def _same_regular_identity(directory_fd: int, name: str, identity: FileIdentity) -> bool:
    try:
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    return stat.S_ISREG(current.st_mode) and FileIdentity.from_stat(current) == identity


def _safe_basename(value: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value or Path(value).is_absolute():
        raise OSError("artifact name must be a basename")
    return value
