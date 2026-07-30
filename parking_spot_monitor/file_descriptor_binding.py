"""Identity-safe pathname checks for held regular-file descriptors."""

from __future__ import annotations

import os
from pathlib import Path
import stat


def descriptor_identity(descriptor: int) -> tuple[int, int]:
    value = os.fstat(descriptor)
    return value.st_dev, value.st_ino


def regular_path_matches_descriptor(path: Path, descriptor: int) -> bool:
    try:
        value = os.stat(path, follow_symlinks=False)
    except OSError:
        return False
    return stat.S_ISREG(value.st_mode) and (value.st_dev, value.st_ino) == descriptor_identity(descriptor)


def open_nofollow_regular_descriptors(path: Path) -> tuple[int, int]:
    identity_fd = os.open(path, getattr(os, "O_PATH", os.O_RDONLY) | getattr(os, "O_NOFOLLOW", 0))
    if not stat.S_ISREG(os.fstat(identity_fd).st_mode):
        return identity_fd, -1
    try:
        read_fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError:
        return identity_fd, -1
    if descriptor_identity(read_fd) != descriptor_identity(identity_fd):
        os.close(read_fd)
        return identity_fd, -1
    return identity_fd, read_fd


def unlink_if_descriptor_matches(path: Path, descriptor: int) -> None:
    if descriptor < 0:
        return
    try:
        value = os.stat(path, follow_symlinks=False)
        if (value.st_dev, value.st_ino) == descriptor_identity(descriptor):
            path.unlink()
    except OSError:
        pass
