"""Race-safe identity cleanup with recoverable same-directory quarantine."""

from __future__ import annotations

from itertools import islice
import os
from pathlib import Path
import re
import secrets
import stat

from parking_spot_monitor.owned_file_disposal import (
    FileIdentity,
    dispose_owned_name_at,
    same_regular_identity_at,
)

_MAX_RECOVERY_SCAN_ENTRIES = 256


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
        if dispose_owned_name_at(directory_fd, quarantine, identity):
            return True
        _restore_quarantined(directory_fd, quarantine, safe_name)
        return False
    except OSError:
        _restore_quarantined(directory_fd, quarantine, safe_name)
        return False


def recover_quarantined_at(
    directory_fd: int, name: str, *, max_entries: int = _MAX_RECOVERY_SCAN_ENTRIES
) -> int:
    safe_name = _safe_basename(name)
    quarantine_pattern = re.compile(
        rf"^\.{re.escape(safe_name)}\.[0-9a-f]{{16}}\.quarantine$"
    )
    recovered = 0
    candidates: list[str] = []
    with os.scandir(directory_fd) as entries:
        for entry in islice(entries, max(0, max_entries)):
            if quarantine_pattern.fullmatch(entry.name):
                candidates.append(entry.name)
    for candidate in sorted(candidates):
        if _restore_quarantined(directory_fd, candidate, safe_name):
            recovered += 1
        if _name_exists(directory_fd, safe_name):
            break
    return recovered


def _restore_quarantined(directory_fd: int, quarantine: str, name: str) -> bool:
    try:
        quarantined = os.stat(quarantine, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    if not stat.S_ISREG(quarantined.st_mode):
        return False
    quarantined_identity = FileIdentity.from_stat(quarantined)
    if same_regular_identity_at(directory_fd, name, quarantined_identity):
        return dispose_owned_name_at(directory_fd, quarantine, quarantined_identity)
    try:
        os.link(quarantine, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd, follow_symlinks=False)
    except FileExistsError:
        if same_regular_identity_at(directory_fd, name, quarantined_identity):
            return dispose_owned_name_at(directory_fd, quarantine, quarantined_identity)
        return False
    except OSError:
        return False
    if not same_regular_identity_at(directory_fd, name, quarantined_identity):
        return False
    return dispose_owned_name_at(directory_fd, quarantine, quarantined_identity)


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
