"""Bounded process-local proof that an owned child-directory entry was synced."""

from __future__ import annotations

import os
import threading

MAX_IDENTITIES = 256
IDENTITY_LOCK = threading.Lock()
DURABLE_IDENTITIES: set[tuple[int, int, str, int, int]] = set()


def ensure_child_directory_durable(root_fd: int, child_name: str) -> None:
    root = os.fstat(root_fd)
    child = os.stat(child_name, dir_fd=root_fd, follow_symlinks=False)
    identity = (root.st_dev, root.st_ino, child_name, child.st_dev, child.st_ino)
    with IDENTITY_LOCK:
        if identity in DURABLE_IDENTITIES:
            return
        os.fsync(root_fd)
        if len(DURABLE_IDENTITIES) >= MAX_IDENTITIES:
            DURABLE_IDENTITIES.clear()
        DURABLE_IDENTITIES.add(identity)
