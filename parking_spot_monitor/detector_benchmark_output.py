from __future__ import annotations

import json
import os
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parking_spot_monitor.detector_benchmark_output_paths import (
    ProtectedInput,
    open_directory_walk,
    output_identity,
    protected_input,
)


@dataclass
class OutputGuard:
    path: Path
    parent: Path
    parent_descriptor: int
    parent_device: int
    parent_inode: int
    initial_output_identity: tuple[int, int] | None
    protected_inputs: tuple[ProtectedInput, ...]

    def close(self) -> None:
        if self.parent_descriptor >= 0:
            os.close(self.parent_descriptor)
            self.parent_descriptor = -1


def validate_benchmark_output(
    output: Path,
    *,
    protected_paths: list[Path],
) -> OutputGuard:
    protected = tuple(protected_input(path) for path in protected_paths)
    path = Path(os.path.abspath(output))
    parent_descriptor = open_directory_walk(path.parent)
    try:
        parent_stat = os.fstat(parent_descriptor)
        if path in {item.resolved_path for item in protected}:
            raise ValueError("benchmark output must not resolve to an input")
        identity = output_identity(parent_descriptor, path.name)
        if identity is not None and any(
            (item.device, item.inode) == identity for item in protected
        ):
            raise ValueError("benchmark output must not hardlink an input")
        return OutputGuard(
            path=path,
            parent=path.parent,
            parent_descriptor=parent_descriptor,
            parent_device=parent_stat.st_dev,
            parent_inode=parent_stat.st_ino,
            initial_output_identity=identity,
            protected_inputs=protected,
        )
    except BaseException:
        os.close(parent_descriptor)
        raise


def write_guarded_report(
    guard: OutputGuard,
    report: dict[str, Any],
    *,
    before_publish: Callable[[], None],
) -> None:
    encoded = (
        json.dumps(report, allow_nan=False, indent=2, sort_keys=True) + "\n"
    ).encode()
    temporary_name: str | None = None
    owned_identity: tuple[int, int] | None = None
    committed = False
    try:
        _verify_requested_parent(guard)
        _recheck_output(guard)
        temporary_name, descriptor = _create_temporary(guard)
        try:
            _write_all(descriptor, encoded)
            os.fsync(descriptor)
            item = os.fstat(descriptor)
            owned_identity = (item.st_dev, item.st_ino)
        finally:
            os.close(descriptor)
        before_publish()
        _verify_requested_parent(guard)
        _recheck_output(guard)
        os.replace(
            temporary_name,
            guard.path.name,
            src_dir_fd=guard.parent_descriptor,
            dst_dir_fd=guard.parent_descriptor,
        )
        temporary_name = None
        committed = True
        os.fsync(guard.parent_descriptor)
        _verify_requested_parent(guard)
        _verify_committed_target(guard, owned_identity)
        _verify_requested_parent(guard)
    except BaseException:
        if temporary_name is not None and owned_identity is not None:
            _unlink_if_owned(guard, temporary_name, owned_identity)
        if committed and owned_identity is not None:
            _unlink_if_owned(guard, guard.path.name, owned_identity)
        raise


def _verify_requested_parent(guard: OutputGuard) -> None:
    descriptor = open_directory_walk(guard.parent)
    try:
        item = os.fstat(descriptor)
        if (item.st_dev, item.st_ino) != (
            guard.parent_device,
            guard.parent_inode,
        ):
            raise ValueError("benchmark output parent changed after preflight")
    finally:
        os.close(descriptor)


def _create_temporary(guard: OutputGuard) -> tuple[str, int]:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    for _attempt in range(128):
        name = f".{guard.path.name}.tmp-{secrets.token_hex(16)}"
        try:
            return name, os.open(
                name,
                flags,
                0o600,
                dir_fd=guard.parent_descriptor,
            )
        except FileExistsError:
            continue
    raise ValueError("unable to reserve a unique benchmark output temporary")


def _recheck_output(guard: OutputGuard) -> None:
    identity = output_identity(guard.parent_descriptor, guard.path.name)
    if identity != guard.initial_output_identity:
        raise ValueError("benchmark output changed after preflight")
    if identity is not None and any(
        (item.device, item.inode) == identity for item in guard.protected_inputs
    ):
        raise ValueError("benchmark output became an input alias")


def _verify_committed_target(
    guard: OutputGuard,
    owned_identity: tuple[int, int] | None,
) -> None:
    if owned_identity is None or output_identity(
        guard.parent_descriptor, guard.path.name
    ) != owned_identity:
        raise ValueError("benchmark output changed during atomic publication")


def _unlink_if_owned(
    guard: OutputGuard,
    name: str,
    owned_identity: tuple[int, int],
) -> None:
    try:
        current = output_identity(guard.parent_descriptor, name)
    except ValueError:
        return
    if current != owned_identity:
        return
    try:
        os.unlink(name, dir_fd=guard.parent_descriptor)
    except FileNotFoundError:
        pass


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        offset += os.write(descriptor, payload[offset:])
