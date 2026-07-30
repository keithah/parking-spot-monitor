"""Bounded cursor state for legacy unindexed owned-file transitions."""

from __future__ import annotations

from collections import OrderedDict
import os
import re
import threading

_MAX_ACTIVE_SCANS = 64
_SCAN_LOCK = threading.Lock()
_ACTIVE_SCANS: OrderedDict[tuple[int, int, str | None], object] = OrderedDict()


def scan_transition_batch(
    directory_fd: int,
    *,
    selected: str | None,
    indexed: set[str],
    max_entries: int,
    transition_pattern: re.Pattern[str],
) -> list[tuple[str, str, str]]:
    if max_entries <= 0:
        return []
    value = os.fstat(directory_fd)
    key = (value.st_dev, value.st_ino, selected)
    candidates: list[tuple[str, str, str]] = []
    with _SCAN_LOCK:
        iterator = _ACTIVE_SCANS.get(key)
        if iterator is None:
            iterator = os.scandir(directory_fd)
            _ACTIVE_SCANS[key] = iterator
            while len(_ACTIVE_SCANS) > _MAX_ACTIVE_SCANS:
                _old_key, old = _ACTIVE_SCANS.popitem(last=False)
                old.close()
        else:
            _ACTIVE_SCANS.move_to_end(key)
        for _ in range(max_entries):
            try:
                item = next(iterator)
            except StopIteration:
                iterator.close()
                _ACTIVE_SCANS.pop(key, None)
                break
            if item.name in indexed:
                continue
            match = transition_pattern.fullmatch(item.name)
            if match is None or (selected is not None and match["recovery"] != selected):
                continue
            candidates.append((item.name, match["recovery"], match["kind"]))
    return candidates
