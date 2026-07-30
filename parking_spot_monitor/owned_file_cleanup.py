"""Race-safe identity cleanup with recoverable same-directory quarantine."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import secrets
import stat

_MAX_RECOVERY_SCAN_ENTRIES = 256


@dataclass(frozen=True, slots=True)
class FileIdentity:
    dev: int
    ino: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> FileIdentity:
        return cls(value.st_dev, value.st_ino)


def unlink_owned_at(directory_fd: int, name: str, identity: FileIdentity) -> bool:
    safe_name = _safe_basename(name)
    try:
        recover_quarantined_at(directory_fd, safe_name)
    except OSError:
        pass
    try:
        current = os.stat(safe_name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    if not stat.S_ISREG(current.st_mode) or FileIdentity.from_stat(current) != identity:
        return False
    quarantine = f".{safe_name}.{secrets.token_hex(8)}.quarantine"
    try:
        os.rename(safe_name, quarantine, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
    except FileNotFoundError:
        return False
    try:
        quarantined = os.stat(quarantine, dir_fd=directory_fd, follow_symlinks=False)
        if not stat.S_ISREG(quarantined.st_mode) or FileIdentity.from_stat(quarantined) != identity:
            _restore_quarantined(directory_fd, quarantine, safe_name)
            return False
        os.unlink(quarantine, dir_fd=directory_fd)
        os.fsync(directory_fd)
        return True
    except OSError:
        _restore_quarantined(directory_fd, quarantine, safe_name)
        return False


def recover_quarantined_at(
    directory_fd: int, name: str, *, max_entries: int = _MAX_RECOVERY_SCAN_ENTRIES
) -> int:
    safe_name = _safe_basename(name)
    prefix, suffix = f".{safe_name}.", ".quarantine"
    recovered = 0
    with os.scandir(directory_fd) as entries:
        for index, entry in enumerate(entries):
            if index >= max_entries:
                break
            candidate = entry.name
            if candidate.startswith(prefix) and candidate.endswith(suffix):
                if _restore_quarantined(directory_fd, candidate, safe_name):
                    recovered += 1
                if _name_exists(directory_fd, safe_name):
                    break
    return recovered


def _restore_quarantined(directory_fd: int, quarantine: str, name: str) -> bool:
    try:
        os.link(quarantine, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd, follow_symlinks=False)
        os.unlink(quarantine, dir_fd=directory_fd)
        os.fsync(directory_fd)
        return True
    except OSError:
        try:
            os.fsync(directory_fd)
        except OSError:
            pass
        return False


def _name_exists(directory_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        return True
    except OSError:
        return False


def _safe_basename(value: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value or Path(value).is_absolute():
        raise OSError("artifact name must be a basename")
    return value
