from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path


MAX_MODEL_BYTES = 2 * 1024 * 1024 * 1024
MODEL_SUFFIXES = {"pt": ".pt", "onnx": ".onnx", "torchscript": ".torchscript"}


@dataclass(frozen=True)
class ModelIdentity:
    path: Path
    resolved_path: Path
    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int
    sha256: str


def validated_model_identities(models: dict[str, Path]) -> dict[str, ModelIdentity]:
    identities = {
        backend: read_model_identity(backend, path)
        for backend, path in models.items()
    }
    validate_distinct_model_identities(identities)
    return identities


def validate_distinct_model_identities(
    identities: dict[str, ModelIdentity],
) -> None:
    if len({identity.resolved_path for identity in identities.values()}) != len(identities):
        raise ValueError("backend models must resolve to distinct paths")
    if len({(identity.device, identity.inode) for identity in identities.values()}) != len(identities):
        raise ValueError("backend models must use distinct files")
    if len({identity.sha256 for identity in identities.values()}) != len(identities):
        raise ValueError("backend models must have distinct content")


def require_unchanged_model(backend: str, expected: ModelIdentity) -> None:
    if read_model_identity(backend, expected.path) != expected:
        raise ValueError(f"{backend} model changed after preflight validation")


def require_unchanged_models(identities: dict[str, ModelIdentity]) -> None:
    for backend, identity in identities.items():
        require_unchanged_model(backend, identity)


def read_model_identity(backend: str, path: Path) -> ModelIdentity:
    return _read_model(backend, path)


def model_identity_matches(expected: ModelIdentity) -> bool:
    try:
        current = os.stat(expected.path, follow_symlinks=False)
    except OSError:
        return False
    return stat.S_ISREG(current.st_mode) and _stable_fields(current) == (
        expected.device,
        expected.inode,
        expected.size_bytes,
        expected.modified_ns,
        expected.changed_ns,
    )


def copy_model_to_descriptor(
    backend: str,
    path: Path,
    destination_descriptor: int,
) -> ModelIdentity:
    return _read_model(backend, path, destination_descriptor=destination_descriptor)


def _read_model(
    backend: str,
    path: Path,
    *,
    destination_descriptor: int | None = None,
) -> ModelIdentity:
    if path.suffix != MODEL_SUFFIXES[backend]:
        raise ValueError(
            f"{backend} model must use the documented {MODEL_SUFFIXES[backend]} suffix"
        )
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError(f"{backend} model must be a readable non-symlink file") from exc
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"{backend} model must be a regular file")
        if before.st_size <= 0 or before.st_size > MAX_MODEL_BYTES:
            raise ValueError(f"{backend} model size is outside the supported bound")
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            if total > MAX_MODEL_BYTES:
                raise ValueError(f"{backend} model size is outside the supported bound")
            digest.update(chunk)
            if destination_descriptor is not None:
                _write_all(destination_descriptor, chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        leaf = os.stat(path, follow_symlinks=False)
        resolved_path = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"{backend} model changed during validation") from exc
    if (
        _stable_fields(before) != _stable_fields(after)
        or _stable_fields(after) != _stable_fields(leaf)
        or total != after.st_size
    ):
        raise ValueError(f"{backend} model changed during validation")
    return ModelIdentity(
        path=path,
        resolved_path=resolved_path,
        device=after.st_dev,
        inode=after.st_ino,
        size_bytes=after.st_size,
        modified_ns=after.st_mtime_ns,
        changed_ns=after.st_ctime_ns,
        sha256=digest.hexdigest(),
    )


def _stable_fields(item: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        item.st_dev,
        item.st_ino,
        item.st_size,
        item.st_mtime_ns,
        item.st_ctime_ns,
    )


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        offset += os.write(descriptor, payload[offset:])
