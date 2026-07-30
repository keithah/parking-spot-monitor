"""Bounded recovery for interrupted owned-file cleanup transitions."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
import os
from pathlib import Path
import re
import stat

from parking_spot_monitor.owned_disposal_manifest import (
    DisposalManifestEntry,
    forget_disposal_at,
    manifest_entries_at,
)
from parking_spot_monitor.owned_file_disposal import (
    FileIdentity,
    disposal_pattern,
    dispose_owned_name_at,
    recover_disposal_at,
    same_regular_identity_at,
)

MAX_RECOVERY_SCAN_ENTRIES = 256
_TRANSITION_PATTERN = re.compile(
    r"^\.(?P<recovery>.+)\.[0-9a-f]{16}\.(?P<kind>dispose|quarantine)$"
)


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    recovered: int = 0
    pending: bool = False


def recover_owned_at(
    directory_fd: int, name: str, *, max_entries: int = MAX_RECOVERY_SCAN_ENTRIES
) -> RecoveryResult:
    safe_name = _safe_basename(name)
    return _recover(directory_fd, safe_name, max_entries=max_entries)


def recover_owned_directory_at(
    directory_fd: int, *, max_entries: int = MAX_RECOVERY_SCAN_ENTRIES
) -> RecoveryResult:
    return _recover(directory_fd, None, max_entries=max_entries)


def _recover(directory_fd: int, selected: str | None, *, max_entries: int) -> RecoveryResult:
    budget = max(0, min(max_entries, MAX_RECOVERY_SCAN_ENTRIES))
    recovered = 0
    pending = False
    entries = [
        entry
        for entry in manifest_entries_at(directory_fd, limit=budget)
        if selected is None or entry.recovery == selected
    ]
    indexed_disposals = {entry.disposal for entry in entries}
    for entry in entries[:budget]:
        result = _recover_manifest_entry(directory_fd, entry)
        recovered += result.recovered
        pending = pending or result.pending
    remaining = budget - min(len(entries), budget)
    if remaining <= 0:
        return RecoveryResult(recovered, pending)

    candidates: list[tuple[str, str, str]] = []
    with os.scandir(directory_fd) as directory_entries:
        for item in islice(directory_entries, remaining):
            if item.name in indexed_disposals:
                continue
            match = _TRANSITION_PATTERN.fullmatch(item.name)
            if match is None or (selected is not None and match["recovery"] != selected):
                continue
            candidates.append((item.name, match["recovery"], match["kind"]))
    for candidate, recovery, kind in sorted(candidates):
        if kind == "dispose":
            result = recover_disposal_at(directory_fd, candidate, recovery)
            recovered += result.status == "deleted"
            pending = pending or result.status != "deleted" or not result.durable
        elif _restore_quarantined(directory_fd, candidate, recovery):
            recovered += 1
        else:
            pending = True
    return RecoveryResult(recovered, pending)


def restore_quarantined_at(directory_fd: int, quarantine: str, name: str) -> bool:
    return _restore_quarantined(directory_fd, quarantine, name)


def _recover_manifest_entry(directory_fd: int, entry: DisposalManifestEntry) -> RecoveryResult:
    if not disposal_pattern(entry.recovery).fullmatch(entry.disposal):
        return RecoveryResult(pending=True)
    if not _name_exists(directory_fd, entry.disposal):
        return RecoveryResult(pending=not forget_disposal_at(directory_fd, entry.disposal))
    result = recover_disposal_at(
        directory_fd,
        entry.disposal,
        entry.recovery,
        expected_identity=FileIdentity(entry.dev, entry.ino),
    )
    if result.status != "deleted":
        return RecoveryResult(pending=True)
    return RecoveryResult(recovered=1, pending=not result.durable)


def _restore_quarantined(directory_fd: int, quarantine: str, name: str) -> bool:
    try:
        quarantined = os.stat(quarantine, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    if not stat.S_ISREG(quarantined.st_mode):
        return False
    identity = FileIdentity.from_stat(quarantined)
    if same_regular_identity_at(directory_fd, name, identity):
        return dispose_owned_name_at(directory_fd, quarantine, identity, recovery_name=name).status == "deleted"
    try:
        os.link(quarantine, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd, follow_symlinks=False)
    except FileExistsError:
        return False
    except OSError:
        return False
    if not same_regular_identity_at(directory_fd, name, identity):
        return False
    return dispose_owned_name_at(directory_fd, quarantine, identity, recovery_name=name).status == "deleted"


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
