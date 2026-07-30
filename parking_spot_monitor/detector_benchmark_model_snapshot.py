from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from parking_spot_monitor.detector_benchmark_models import (
    MODEL_SUFFIXES,
    ModelIdentity,
    copy_model_to_descriptor,
    read_model_identity,
    require_unchanged_models,
    validate_distinct_model_identities,
)


@dataclass
class ModelSnapshots:
    originals: dict[str, ModelIdentity]
    snapshots: dict[str, ModelIdentity]
    paths: dict[str, Path]
    _temporary: tempfile.TemporaryDirectory[str]

    def require_unchanged(self) -> None:
        require_unchanged_models(self.originals)
        for backend, expected in self.snapshots.items():
            if read_model_identity(backend, expected.path) != expected:
                raise ValueError(
                    f"{backend} model snapshot changed after preflight validation"
                )

    def close(self) -> None:
        self._temporary.cleanup()


def prepare_model_snapshots(models: dict[str, Path]) -> ModelSnapshots:
    temporary = tempfile.TemporaryDirectory(prefix="detector-benchmark-models-")
    root = Path(temporary.name)
    os.chmod(root, 0o700)
    paths: dict[str, Path] = {}
    originals: dict[str, ModelIdentity] = {}
    snapshots: dict[str, ModelIdentity] = {}
    try:
        for backend, model_path in models.items():
            snapshot = root / f"{backend}{MODEL_SUFFIXES[backend]}"
            descriptor = os.open(
                snapshot,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                0o400,
            )
            try:
                original = copy_model_to_descriptor(
                    backend, model_path, descriptor
                )
                os.fchmod(descriptor, 0o400)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            snapshot_identity = read_model_identity(backend, snapshot)
            if (
                snapshot_identity.size_bytes != original.size_bytes
                or snapshot_identity.sha256 != original.sha256
            ):
                raise ValueError(f"{backend} model snapshot differs from its original")
            paths[backend] = snapshot
            originals[backend] = original
            snapshots[backend] = snapshot_identity
        validate_distinct_model_identities(originals)
        os.chmod(root, 0o500)
        return ModelSnapshots(
            originals=originals,
            snapshots=snapshots,
            paths=paths,
            _temporary=temporary,
        )
    except BaseException:
        temporary.cleanup()
        raise
