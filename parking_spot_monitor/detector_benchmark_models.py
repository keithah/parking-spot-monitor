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
    sha256: str


def validated_model_identities(models: dict[str, Path]) -> dict[str, ModelIdentity]:
    identities = {
        backend: _model_identity(backend, path) for backend, path in models.items()
    }
    if len({identity.resolved_path for identity in identities.values()}) != len(identities):
        raise ValueError("backend models must resolve to distinct paths")
    if len({(identity.device, identity.inode) for identity in identities.values()}) != len(identities):
        raise ValueError("backend models must use distinct files")
    if len({identity.sha256 for identity in identities.values()}) != len(identities):
        raise ValueError("backend models must have distinct content")
    return identities


def require_unchanged_model(backend: str, expected: ModelIdentity) -> None:
    if _model_identity(backend, expected.path) != expected:
        raise ValueError(f"{backend} model changed after preflight validation")


def _model_identity(backend: str, path: Path) -> ModelIdentity:
    if path.suffix != MODEL_SUFFIXES[backend]:
        raise ValueError(
            f"{backend} model must use the documented {MODEL_SUFFIXES[backend]} suffix"
        )
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
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
        sha256=digest.hexdigest(),
    )


def _stable_fields(item: os.stat_result) -> tuple[int, int, int, int]:
    return (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
