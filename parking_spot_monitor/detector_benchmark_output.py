from __future__ import annotations

import json
import os
import stat
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class _ProtectedInput:
    resolved_path: Path
    device: int
    inode: int


@dataclass(frozen=True)
class OutputGuard:
    path: Path
    parent: Path
    parent_device: int
    parent_inode: int
    initial_output_identity: tuple[int, int] | None
    protected_inputs: tuple[_ProtectedInput, ...]


def validate_benchmark_output(
    output: Path,
    *,
    protected_paths: list[Path],
) -> OutputGuard:
    protected = tuple(_protected_input(path) for path in protected_paths)
    path = Path(os.path.abspath(output))
    try:
        parent_resolved = path.parent.resolve(strict=True)
    except OSError as exc:
        raise ValueError("benchmark output parent must be an existing safe directory") from exc
    if parent_resolved != path.parent:
        raise ValueError("benchmark output parent must not use symlinks")
    parent_descriptor = _open_parent(path.parent)
    try:
        parent_stat = os.fstat(parent_descriptor)
    finally:
        os.close(parent_descriptor)
    resolved_output = path.resolve(strict=False)
    if any(item.resolved_path == resolved_output for item in protected):
        raise ValueError("benchmark output must not resolve to an input")
    identity = _output_identity(path)
    if identity is not None and any(
        (item.device, item.inode) == identity for item in protected
    ):
        raise ValueError("benchmark output must not hardlink an input")
    return OutputGuard(
        path=path,
        parent=path.parent,
        parent_device=parent_stat.st_dev,
        parent_inode=parent_stat.st_ino,
        initial_output_identity=identity,
        protected_inputs=protected,
    )


def write_guarded_report(
    guard: OutputGuard,
    report: dict[str, Any],
    *,
    before_publish: Callable[[], None],
) -> None:
    encoded = (
        json.dumps(report, allow_nan=False, indent=2, sort_keys=True) + "\n"
    ).encode()
    parent_descriptor = _open_parent(guard.parent)
    temporary_name: str | None = None
    try:
        parent_stat = os.fstat(parent_descriptor)
        if (parent_stat.st_dev, parent_stat.st_ino) != (
            guard.parent_device,
            guard.parent_inode,
        ):
            raise ValueError("benchmark output parent changed after preflight")
        _recheck_output(guard)
        temporary_descriptor, temporary_path = tempfile.mkstemp(
            prefix=f".{guard.path.name}.tmp-",
            dir=guard.parent,
        )
        temporary_name = Path(temporary_path).name
        try:
            with os.fdopen(temporary_descriptor, "wb", closefd=True) as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            before_publish()
            _recheck_output(guard)
            os.replace(
                temporary_name,
                guard.path.name,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
            )
            temporary_name = None
            os.fsync(parent_descriptor)
        finally:
            if temporary_name is not None:
                try:
                    os.unlink(temporary_name, dir_fd=parent_descriptor)
                except FileNotFoundError:
                    pass
    finally:
        os.close(parent_descriptor)


def _protected_input(path: Path) -> _ProtectedInput:
    try:
        file_stat = path.stat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError("benchmark input changed during output preflight") from exc
    if not stat.S_ISREG(file_stat.st_mode):
        raise ValueError("benchmark protected input must be a regular file")
    return _ProtectedInput(
        resolved_path=resolved,
        device=file_stat.st_dev,
        inode=file_stat.st_ino,
    )


def _open_parent(parent: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(parent, flags)
    except OSError as exc:
        raise ValueError("benchmark output parent must be an existing safe directory") from exc
    if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ValueError("benchmark output parent must be a directory")
    return descriptor


def _output_identity(path: Path) -> tuple[int, int] | None:
    try:
        file_stat = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(file_stat.st_mode):
        raise ValueError("benchmark output must not be a symlink")
    if not stat.S_ISREG(file_stat.st_mode):
        raise ValueError("benchmark output must be a regular file when it exists")
    return (file_stat.st_dev, file_stat.st_ino)


def _recheck_output(guard: OutputGuard) -> None:
    identity = _output_identity(guard.path)
    if identity != guard.initial_output_identity:
        raise ValueError("benchmark output changed after preflight")
    if identity is not None and any(
        (item.device, item.inode) == identity for item in guard.protected_inputs
    ):
        raise ValueError("benchmark output became an input alias")
    if guard.path.resolve(strict=False) in {
        item.resolved_path for item in guard.protected_inputs
    }:
        raise ValueError("benchmark output resolves to an input")
