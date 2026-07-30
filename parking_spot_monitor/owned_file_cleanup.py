"""Race-safe identity cleanup with recoverable same-directory quarantine."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import secrets
import stat

from parking_spot_monitor.owned_disposal_manifest import (
    DisposalManifestEntry,
    forget_disposal_at,
    record_disposal_at,
)
from parking_spot_monitor.owned_file_disposal import (
    FileIdentity,
    dispose_owned_name_at,
)
from parking_spot_monitor.owned_file_recovery import (
    MAX_RECOVERY_SCAN_ENTRIES,
    recover_owned_at,
    recover_owned_directory_at,
    restore_quarantined_at,
)

_MAX_RECOVERY_SCAN_ENTRIES = MAX_RECOVERY_SCAN_ENTRIES


@dataclass(frozen=True, slots=True)
class OwnedUnlinkResult:
    deleted: bool
    durable: bool = True


def unlink_owned_at(directory_fd: int, name: str, identity: FileIdentity) -> bool:
    return unlink_owned_at_result(directory_fd, name, identity).deleted


def unlink_owned_at_result(directory_fd: int, name: str, identity: FileIdentity) -> OwnedUnlinkResult:
    safe_name = _safe_basename(name)
    try:
        recovery = recover_owned_at(directory_fd, safe_name)
    except OSError:
        return OwnedUnlinkResult(False)
    if recovery.blocking:
        return OwnedUnlinkResult(False)
    try:
        current = os.stat(safe_name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return OwnedUnlinkResult(False)
    if not stat.S_ISREG(current.st_mode) or FileIdentity.from_stat(current) != identity:
        return OwnedUnlinkResult(False)
    quarantine = f".{safe_name}.{secrets.token_hex(8)}.quarantine"
    if not record_disposal_at(
        directory_fd,
        DisposalManifestEntry(quarantine, safe_name, identity.dev, identity.ino),
    ):
        return OwnedUnlinkResult(False)
    try:
        os.rename(safe_name, quarantine, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
    except FileNotFoundError:
        forget_disposal_at(directory_fd, quarantine)
        return OwnedUnlinkResult(False)
    except OSError:
        forget_disposal_at(directory_fd, quarantine)
        return OwnedUnlinkResult(False)
    try:
        quarantined_identity = FileIdentity.from_stat(
            os.stat(quarantine, dir_fd=directory_fd, follow_symlinks=False)
        )
    except OSError:
        return OwnedUnlinkResult(False)
    if quarantined_identity != identity:
        if not record_disposal_at(
            directory_fd,
            DisposalManifestEntry(
                quarantine,
                safe_name,
                quarantined_identity.dev,
                quarantined_identity.ino,
            ),
        ):
            return OwnedUnlinkResult(False)
        if restore_quarantined_at(directory_fd, quarantine, safe_name):
            forget_disposal_at(directory_fd, quarantine)
        return OwnedUnlinkResult(False)
    try:
        disposal = dispose_owned_name_at(
            directory_fd,
            quarantine,
            identity,
            recovery_name=safe_name,
        )
        if disposal.status == "deleted":
            forget_disposal_at(directory_fd, quarantine)
            return OwnedUnlinkResult(True, disposal.durable)
        if disposal.status == "pending":
            forget_disposal_at(directory_fd, quarantine)
            return OwnedUnlinkResult(False)
        if restore_quarantined_at(directory_fd, quarantine, safe_name):
            forget_disposal_at(directory_fd, quarantine)
        return OwnedUnlinkResult(False)
    except OSError:
        if restore_quarantined_at(directory_fd, quarantine, safe_name):
            forget_disposal_at(directory_fd, quarantine)
        return OwnedUnlinkResult(False)


def recover_quarantined_at(
    directory_fd: int, name: str, *, max_entries: int = _MAX_RECOVERY_SCAN_ENTRIES
) -> int:
    return recover_owned_at(directory_fd, name, max_entries=max_entries).recovered


def _safe_basename(value: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value or Path(value).is_absolute():
        raise OSError("artifact name must be a basename")
    return value
