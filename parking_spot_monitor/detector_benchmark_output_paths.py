from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProtectedInput:
    resolved_path: Path
    device: int
    inode: int


def protected_input(path: Path) -> ProtectedInput:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        file_stat = os.fstat(descriptor)
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError("benchmark input changed during output preflight") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if not stat.S_ISREG(file_stat.st_mode):
        raise ValueError("benchmark protected input must be a regular file")
    return ProtectedInput(
        resolved_path=resolved,
        device=file_stat.st_dev,
        inode=file_stat.st_ino,
    )


def open_directory_walk(path: Path) -> int:
    if not path.is_absolute():
        raise ValueError("benchmark output parent must be absolute after normalization")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open("/", flags)
        for component in path.parts[1:]:
            try:
                next_descriptor = os.open(component, flags, dir_fd=descriptor)
            except BaseException:
                os.close(descriptor)
                raise
            os.close(descriptor)
            descriptor = next_descriptor
    except OSError as exc:
        raise ValueError(
            "benchmark output parent must be an existing safe directory"
        ) from exc
    if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ValueError("benchmark output parent must be a directory")
    return descriptor


def output_identity(parent_descriptor: int, name: str) -> tuple[int, int] | None:
    try:
        file_stat = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(file_stat.st_mode):
        raise ValueError("benchmark output must not be a symlink")
    if not stat.S_ISREG(file_stat.st_mode):
        raise ValueError("benchmark output must be a regular file when it exists")
    return (file_stat.st_dev, file_stat.st_ino)
