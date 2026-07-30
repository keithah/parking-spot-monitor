from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MAX_MANIFEST_BYTES = 1024 * 1024
MAX_MANIFEST_FRAMES = 256
MAX_FRAME_BYTES = 32 * 1024 * 1024
MAX_CORPUS_BYTES = 512 * 1024 * 1024
MAX_WORKLOAD_BYTES = 64 * 1024 * 1024 * 1024


@dataclass(frozen=True)
class FileIdentity:
    path: Path
    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int
    sha256: str


@dataclass
class CorpusSnapshot:
    manifest: FileIdentity
    frames: tuple[FileIdentity, ...]
    snapshot_paths: tuple[Path, ...]
    corpus_size_bytes: int
    workload_bytes: int
    _temporary: tempfile.TemporaryDirectory[str]

    @property
    def protected_paths(self) -> list[Path]:
        return [self.manifest.path, *(item.path for item in self.frames)]

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
            "ordered_frame_sha256": ordered,
            "corpus_sha256": corpus_digest,
            "frame_count": len(self.frames),
            "corpus_size_bytes": self.corpus_size_bytes,
            "workload_bytes": self.workload_bytes,
        }

    def require_unchanged(self) -> None:
        if _read_identity(
            self.manifest.path, "manifest", MAX_MANIFEST_BYTES
        )[0] != self.manifest:
            raise ValueError("manifest changed after corpus preflight")
        for expected in self.frames:
            if _read_identity(expected.path, "frame", MAX_FRAME_BYTES)[0] != expected:
                raise ValueError("frame changed after corpus preflight")

    def close(self) -> None:
        self._temporary.cleanup()


def prepare_corpus(
    manifest_path: Path,
    *,
    warmup: int,
    iterations: int,
) -> CorpusSnapshot:
    manifest_path = Path(os.path.abspath(manifest_path))
    manifest_identity, manifest_bytes = _read_identity(
        manifest_path, "manifest", MAX_MANIFEST_BYTES
    )
    frame_paths = _parse_manifest(manifest_path, manifest_bytes)
    temporary = tempfile.TemporaryDirectory(prefix="detector-benchmark-")
    snapshot_root = Path(temporary.name)
    os.chmod(snapshot_root, 0o700)
    identities: list[FileIdentity] = []
    snapshot_paths: list[Path] = []
    corpus_size = 0
    try:
        for index, frame_path in enumerate(frame_paths):
            identity, frame_bytes = _read_identity(
                frame_path, "frame", MAX_FRAME_BYTES
            )
            corpus_size += identity.size_bytes
            if corpus_size > MAX_CORPUS_BYTES:
                raise ValueError("frame corpus exceeds the supported total bound")
            frame_dir = snapshot_root / f"{index:04d}"
            frame_dir.mkdir(mode=0o700)
            snapshot = frame_dir / frame_path.name
            descriptor = os.open(
                snapshot,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o400,
            )
            try:
                _write_all(descriptor, frame_bytes)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.chmod(frame_dir, 0o500)
            identities.append(identity)
            snapshot_paths.append(snapshot)
        workload = corpus_size * (warmup + iterations) + identities[0].size_bytes
        if workload > MAX_WORKLOAD_BYTES:
            raise ValueError("benchmark corpus workload exceeds the supported bound")
        os.chmod(snapshot_root, 0o500)
        return CorpusSnapshot(
            manifest=manifest_identity,
            frames=tuple(identities),
            snapshot_paths=tuple(snapshot_paths),
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


def _read_identity(path: Path, label: str, limit: int) -> tuple[FileIdentity, bytes]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError(f"{label} must be a readable non-symlink regular file") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"{label} must be a regular file")
        if before.st_size <= 0 or before.st_size > limit:
            raise ValueError(f"{label} size is outside the supported bound")
        chunks: list[bytes] = []
        total = 0
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, min(1024 * 1024, limit + 1 - total)):
            total += len(chunk)
            if total > limit:
                raise ValueError(f"{label} size is outside the supported bound")
            digest.update(chunk)
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        leaf = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise ValueError(f"{label} changed during corpus preflight") from exc
    if (
        _stable_fields(before) != _stable_fields(after)
        or _stable_fields(after) != _stable_fields(leaf)
    ):
        raise ValueError(f"{label} changed during corpus preflight")
    return (
        FileIdentity(
            path=path,
            device=after.st_dev,
            inode=after.st_ino,
            size_bytes=after.st_size,
            modified_ns=after.st_mtime_ns,
            changed_ns=after.st_ctime_ns,
            sha256=digest.hexdigest(),
        ),
        b"".join(chunks),
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
