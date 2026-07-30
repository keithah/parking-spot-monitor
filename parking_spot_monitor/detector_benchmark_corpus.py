from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parking_spot_monitor.detector_benchmark_corpus_files import (
    FileIdentity,
    create_snapshot,
    file_identity_matches,
    read_identity,
    require_matching_snapshot,
)


MAX_MANIFEST_BYTES = 1024 * 1024
MAX_MANIFEST_FRAMES = 256
MAX_FRAME_BYTES = 32 * 1024 * 1024
MAX_CORPUS_BYTES = 512 * 1024 * 1024
MAX_WORKLOAD_BYTES = 64 * 1024 * 1024 * 1024


@dataclass
class CorpusSnapshot:
    manifest: FileIdentity
    frames: tuple[FileIdentity, ...]
    manifest_snapshot: FileIdentity
    frame_snapshots: tuple[FileIdentity, ...]
    corpus_size_bytes: int
    workload_bytes: int
    _temporary: tempfile.TemporaryDirectory[str]

    @property
    def protected_paths(self) -> list[Path]:
        return [self.manifest.path, *(item.path for item in self.frames)]

    @property
    def snapshot_paths(self) -> tuple[Path, ...]:
        return tuple(item.path for item in self.frame_snapshots)

    @property
    def evidence(self) -> dict[str, Any]:
        ordered = [item.sha256 for item in self.frames]
        corpus_digest = hashlib.sha256(
            json.dumps(
                {"manifest": self.manifest.sha256, "frames": ordered},
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()
        return {
            "manifest_sha256": self.manifest.sha256,
            "manifest_snapshot_sha256": self.manifest_snapshot.sha256,
            "ordered_frame_sha256": ordered,
            "ordered_frame_snapshot_sha256": [
                item.sha256 for item in self.frame_snapshots
            ],
            "corpus_sha256": corpus_digest,
            "frame_count": len(self.frames),
            "corpus_size_bytes": self.corpus_size_bytes,
            "workload_bytes": self.workload_bytes,
        }

    def require_unchanged(self, *, comprehensive: bool = False) -> None:
        if not _identity_is_current(
            self.manifest, "manifest", MAX_MANIFEST_BYTES, comprehensive=comprehensive
        ):
            raise ValueError("manifest changed after corpus preflight")
        for expected in self.frames:
            if not _identity_is_current(expected, "frame", MAX_FRAME_BYTES, comprehensive=comprehensive):
                raise ValueError("frame changed after corpus preflight")
        if not _identity_is_current(
            self.manifest_snapshot,
            "manifest snapshot",
            MAX_MANIFEST_BYTES,
            comprehensive=comprehensive,
        ):
            raise ValueError(
                "manifest snapshot changed after preflight validation"
            )
        for expected in self.frame_snapshots:
            if not _identity_is_current(
                expected, "frame snapshot", MAX_FRAME_BYTES, comprehensive=comprehensive
            ):
                raise ValueError("frame snapshot changed after preflight validation")

    def close(self) -> None:
        self._temporary.cleanup()


def prepare_corpus(
    manifest_path: Path,
    *,
    warmup: int,
    iterations: int,
) -> CorpusSnapshot:
    manifest_path = Path(os.path.abspath(manifest_path))
    manifest_identity, manifest_bytes = read_identity(
        manifest_path, "manifest", MAX_MANIFEST_BYTES
    )
    frame_paths = _parse_manifest(manifest_path, manifest_bytes)
    temporary = tempfile.TemporaryDirectory(prefix="detector-benchmark-")
    snapshot_root = Path(temporary.name)
    os.chmod(snapshot_root, 0o700)
    identities: list[FileIdentity] = []
    frame_snapshots: list[FileIdentity] = []
    corpus_size = 0
    try:
        manifest_snapshot = create_snapshot(
            snapshot_root / "manifest.json",
            manifest_bytes,
            "manifest snapshot",
            MAX_MANIFEST_BYTES,
        )
        require_matching_snapshot(manifest_identity, manifest_snapshot)
        for index, frame_path in enumerate(frame_paths):
            identity, frame_bytes = read_identity(
                frame_path, "frame", MAX_FRAME_BYTES
            )
            corpus_size += identity.size_bytes
            if corpus_size > MAX_CORPUS_BYTES:
                raise ValueError("frame corpus exceeds the supported total bound")
            frame_dir = snapshot_root / f"{index:04d}"
            frame_dir.mkdir(mode=0o700)
            snapshot = frame_dir / frame_path.name
            snapshot_identity = create_snapshot(
                snapshot,
                frame_bytes,
                "frame snapshot",
                MAX_FRAME_BYTES,
            )
            require_matching_snapshot(identity, snapshot_identity)
            os.chmod(frame_dir, 0o500)
            identities.append(identity)
            frame_snapshots.append(snapshot_identity)
        workload = corpus_size * (warmup + iterations) + identities[0].size_bytes
        if workload > MAX_WORKLOAD_BYTES:
            raise ValueError("benchmark corpus workload exceeds the supported bound")
        os.chmod(snapshot_root, 0o500)
        return CorpusSnapshot(
            manifest=manifest_identity,
            frames=tuple(identities),
            manifest_snapshot=manifest_snapshot,
            frame_snapshots=tuple(frame_snapshots),
            corpus_size_bytes=corpus_size,
            workload_bytes=workload,
            _temporary=temporary,
        )
    except BaseException:
        temporary.cleanup()
        raise


def _parse_manifest(path: Path, raw: bytes) -> list[Path]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("manifest is missing or is not valid JSON") from exc
    frames = payload.get("frames") if isinstance(payload, dict) else None
    if not isinstance(frames, list) or not frames:
        raise ValueError("manifest must contain a non-empty frames array")
    if len(frames) > MAX_MANIFEST_FRAMES:
        raise ValueError("manifest exceeds the supported frame bound")
    if not all(isinstance(frame, str) and frame for frame in frames):
        raise ValueError("every manifest frame must be a non-empty path string")
    return [
        Path(
            os.path.abspath(
                frame if Path(frame).is_absolute() else path.parent / frame
            )
        )
        for frame in frames
    ]


def _identity_is_current(
    expected: FileIdentity,
    label: str,
    limit: int,
    *,
    comprehensive: bool,
) -> bool:
    if not comprehensive and file_identity_matches(expected):
        return True
    return read_identity(expected.path, label, limit)[0] == expected
