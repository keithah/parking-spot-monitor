"""Secure, durable filesystem ownership for Matrix snapshot upload artifacts."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
from pathlib import Path
import threading
from types import MappingProxyType

from parking_spot_monitor.jpeg_artifacts import JpegDecodeError, jpeg_bytes_dimensions
from parking_spot_monitor.matrix_snapshot_storage import (
    OwnedArtifactDeleteResult,
    artifact_path,
    delete_owned_artifact,
    publish_owned_bytes,
    read_owned_bytes,
    safe_artifact_name,
    validate_owned_file,
)

MAX_UPLOAD_DERIVATIVE_BYTES = 300_000
DERIVATIVE_DIRECTORY = ".upload-derivatives"


@dataclass(frozen=True, slots=True)
class MatrixUploadDerivative:
    path: Path
    info: Mapping[str, int | str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "info", MappingProxyType(dict(self.info)))


@dataclass(slots=True)
class _LockEntry:
    lock: threading.Lock
    references: int = 0


class UploadPublicationLocks:
    """Short-lived keyed locks; unrelated events publish concurrently."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._entries: dict[str, _LockEntry] = {}

    @contextmanager
    def hold(self, key: str) -> Iterator[None]:
        with self._guard:
            entry = self._entries.setdefault(key, _LockEntry(threading.Lock()))
            entry.references += 1
        try:
            with entry.lock:
                yield
        finally:
            with self._guard:
                entry.references -= 1
                if entry.references == 0:
                    self._entries.pop(key, None)


def upload_derivative_path(snapshot_root: Path, filename: str) -> Path:
    return artifact_path(snapshot_root, DERIVATIVE_DIRECTORY, filename)


def publish_upload_derivative(
    snapshot_root: Path,
    filename: str,
    *,
    data: bytes,
    info: Mapping[str, object],
) -> MatrixUploadDerivative:
    payload = bytes(data)
    if not 0 < len(payload) <= MAX_UPLOAD_DERIVATIVE_BYTES:
        raise JpegDecodeError("read_failed")
    width, height = jpeg_bytes_dimensions(payload)
    validated = _validated_info(info, expected_size=len(payload), expected_dimensions=(width, height))
    validated["sha256"] = hashlib.sha256(payload).hexdigest()
    try:
        path = publish_owned_bytes(snapshot_root, DERIVATIVE_DIRECTORY, filename, payload, mode=0o600)
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc
    return MatrixUploadDerivative(path=path, info=validated)


def load_upload_derivative(
    snapshot_root: Path,
    filename: str,
    *,
    persisted_path: object,
    info: Mapping[str, object],
) -> MatrixUploadDerivative:
    expected = upload_derivative_path(snapshot_root, filename)
    if not isinstance(persisted_path, str) or persisted_path != str(expected):
        raise JpegDecodeError("read_failed")
    return MatrixUploadDerivative(path=expected, info=_validated_info(info, require_digest=True))


def read_upload_derivative_bytes(snapshot_root: Path, derivative: MatrixUploadDerivative) -> bytes:
    try:
        payload = read_owned_bytes(
            snapshot_root,
            DERIVATIVE_DIRECTORY,
            safe_artifact_name(derivative.path.name),
            max_bytes=MAX_UPLOAD_DERIVATIVE_BYTES,
        )
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc
    width, height = jpeg_bytes_dimensions(payload)
    expected = _validated_info(
        derivative.info,
        expected_size=len(payload),
        expected_dimensions=(width, height),
        require_digest=True,
    )
    if hashlib.sha256(payload).hexdigest() != expected["sha256"]:
        raise JpegDecodeError("read_failed")
    return payload


def validate_retained_snapshot_path(snapshot_root: Path, filename: str, persisted_path: object) -> Path:
    try:
        expected = artifact_path(snapshot_root, None, filename)
        if not isinstance(persisted_path, str) or persisted_path != str(expected):
            raise OSError("retained snapshot path mismatch")
        return validate_owned_file(snapshot_root, None, filename)
    except OSError as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc


def delete_upload_derivative(snapshot_root: Path, filename: str) -> OwnedArtifactDeleteResult:
    return delete_owned_artifact(snapshot_root, DERIVATIVE_DIRECTORY, filename)


def _validated_info(
    info: Mapping[str, object],
    *,
    expected_size: int | None = None,
    expected_dimensions: tuple[int, int] | None = None,
    require_digest: bool = False,
) -> dict[str, int | str]:
    if not isinstance(info, Mapping):
        raise JpegDecodeError("read_failed")
    size, width, height = info.get("size"), info.get("w"), info.get("h")
    digest = info.get("sha256")
    if info.get("mimetype") != "image/jpeg" or any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in (size, width, height)
    ):
        raise JpegDecodeError("read_failed")
    if expected_size is not None and size != expected_size:
        raise JpegDecodeError("read_failed")
    if expected_dimensions is not None and (width, height) != expected_dimensions:
        raise JpegDecodeError("read_failed")
    if require_digest and digest is None:
        raise JpegDecodeError("read_failed")
    if digest is not None and (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(ch not in "0123456789abcdef" for ch in digest)
    ):
        raise JpegDecodeError("read_failed")
    result: dict[str, int | str] = {"mimetype": "image/jpeg", "size": size, "w": width, "h": height}
    if isinstance(digest, str):
        result["sha256"] = digest
    return result
