"""Matrix event snapshot retention and upload preparation."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.image_budget import ImageBudgetError, JpegBudgetResult, encode_jpeg_under_budget
from parking_spot_monitor.jpeg_artifacts import JpegDecodeError, open_decoded_rgb_jpeg_bytes
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_snapshot_storage import (
    delete_owned_artifact,
    ensure_owned_directory,
    RootedJpegEvidence,
    read_owned_jpeg_evidence,
    secure_snapshot_candidates,
)
from parking_spot_monitor.matrix_retained_publication import publish_retained_snapshot
from parking_spot_monitor.matrix_snapshot_naming import event_snapshot_path, snapshot_body
from parking_spot_monitor.matrix_upload_derivatives import delete_upload_derivative
from parking_spot_monitor.matrix_support import MatrixError, _require_non_empty, _sanitize_diagnostics
from parking_spot_monitor.matrix_time import format_observed_at

JPEG_MIMETYPE = "image/jpeg"
MAX_MATRIX_UPLOAD_IMAGE_BYTES = 300_000
MATRIX_UPLOAD_INITIAL_MAX_DIMENSION = 960
MATRIX_UPLOAD_MIN_DIMENSION = 320
MATRIX_UPLOAD_JPEG_QUALITIES = (85, 75, 65, 55, 45, 35)

@dataclass(frozen=True)
class MatrixSnapshot:
    """Event-specific raw snapshot prepared for Matrix media upload."""

    path: Path
    filename: str
    txn_id: str
    body: str
    info: dict[str, int | str]
    log_context: dict[str, Any]
    evidence: RootedJpegEvidence | None = None


@dataclass(frozen=True)
class SnapshotRetentionResult:
    """Safe summary of an event snapshot retention pruning attempt."""

    pruned_count: int = 0
    pruned_bytes: int = 0
    retained_count: int = 0
    failed_count: int = 0


@dataclass(frozen=True, slots=True)
class _MatrixSnapshotResize:
    result: JpegBudgetResult
    info: dict[str, int | str]


def _matrix_snapshot_upload(
    snapshot: MatrixSnapshot,
    *,
    logger: StructuredLogger | None,
) -> dict[str, Any]:
    try:
        evidence = snapshot.evidence or read_owned_jpeg_evidence(snapshot.path.parent, snapshot.filename)
    except (JpegDecodeError, OSError):
        raise MatrixError(
            "Matrix snapshot could not be read for upload", error_type="snapshot_resize_failed", snapshot_path=str(snapshot.path)
        ) from None
    source_size = len(evidence.data)
    if source_size <= MAX_MATRIX_UPLOAD_IMAGE_BYTES:
        return {"data": evidence.data, "info": dict(evidence.info)}
    resized = _resize_jpeg_bytes_for_matrix_upload_result(evidence.data, snapshot_path=snapshot.path)
    if logger is not None:
        logger.info(
            "matrix-snapshot-upload-resized",
            snapshot_path=str(snapshot.path),
            source_size=source_size,
            upload_size=resized.info["size"],
            width=resized.info["w"],
            height=resized.info["h"],
            quality=resized.result.quality,
            attempts=resized.result.attempts,
        )
    return {"data": resized.result.data, "info": resized.info}


def _resize_jpeg_for_matrix_upload(path: Path) -> tuple[bytes, dict[str, int | str]]:
    try:
        evidence = read_owned_jpeg_evidence(path.parent, path.name)
    except (JpegDecodeError, OSError):
        raise MatrixError(
            "Matrix snapshot could not be resized under upload budget", error_type="snapshot_resize_failed", snapshot_path=str(path)
        ) from None
    resized = _resize_jpeg_bytes_for_matrix_upload_result(evidence.data, snapshot_path=path)
    return resized.result.data, resized.info


def _resize_jpeg_bytes_for_matrix_upload_result(payload: bytes, *, snapshot_path: Path) -> _MatrixSnapshotResize:
    try:
        with open_decoded_rgb_jpeg_bytes(payload, initial_max_dimension=MATRIX_UPLOAD_INITIAL_MAX_DIMENSION) as decoded:
            result = encode_jpeg_under_budget(
                decoded.image,
                max_bytes=MAX_MATRIX_UPLOAD_IMAGE_BYTES,
                initial_max_dimension=MATRIX_UPLOAD_INITIAL_MAX_DIMENSION,
                min_dimension=MATRIX_UPLOAD_MIN_DIMENSION,
                dimension_scale=0.85,
                qualities=MATRIX_UPLOAD_JPEG_QUALITIES,
                resampling=getattr(getattr(Image, "Resampling", Image), "LANCZOS"),
            )
        return _MatrixSnapshotResize(
            result=result,
            info={"mimetype": JPEG_MIMETYPE, "size": len(result.data), "w": result.width, "h": result.height},
        )
    except (ImageBudgetError, JpegDecodeError) as exc:
        message = (
            "Matrix snapshot dimensions are invalid"
            if isinstance(exc, JpegDecodeError) and exc.code == "invalid_dimensions"
            else "Matrix snapshot could not be resized under upload budget"
        )
        raise MatrixError(
            message, error_type="snapshot_resize_failed", snapshot_path=str(snapshot_path)
        ) from None


def prepare_event_snapshot(
    *,
    source_path: str | Path,
    data_dir: str | Path,
    snapshots_dir: str | Path | None,
    event_type: str,
    event_id: str,
    spot_id: str | None,
    observed_at: object,
    snapshot_retention_count: int | None = None,
    logger: StructuredLogger | None = None,
    retention_trigger: str = "matrix-event",
    protected_snapshots: Sequence[str | Path] | None = None,
) -> MatrixSnapshot:
    """Copy a raw full-frame snapshot into a stable event-specific Matrix evidence file.

    The helper intentionally rejects local debug overlays and validates JPEG
    metadata before callers can upload bytes as an ``m.image`` message.
    """

    source = Path(source_path)
    event_type_text = _require_non_empty("event_type", event_type)
    event_id_text = _require_non_empty("event_id", event_id)
    observed_text = format_observed_at(observed_at)
    destination = event_snapshot_path(
        data_dir=data_dir,
        snapshots_dir=snapshots_dir,
        event_type=event_type_text,
        event_id=event_id_text,
        spot_id=spot_id,
        observed_at=observed_text,
    )
    snapshot_root = destination.parent
    filename = destination.name
    try:
        snapshot_root = ensure_owned_directory(snapshot_root)
    except OSError as exc:
        raise MatrixError(
            "Matrix snapshot directory is unsafe",
            error_type="snapshot_copy_failed",
            snapshot_path=str(snapshot_root),
            exception_type=exc.__class__.__name__,
        ) from exc
    if source.name == "debug_latest.jpg":
        raise MatrixError(
            "Matrix snapshot source cannot be the local debug overlay",
            error_type="snapshot_invalid_source",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
        )
    published_identity = None
    try:
        copied_snapshot = not _same_path(source, destination)
        if copied_snapshot:
            publication = publish_retained_snapshot(source, snapshot_root, filename)
            destination, published_identity = publication.path, publication.identity
    except OSError as exc:
        raise MatrixError(
            "Matrix snapshot copy failed",
            error_type="snapshot_copy_failed",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
            exception_type=exc.__class__.__name__,
        ) from exc
    byte_size = 0
    try:
        evidence = read_owned_jpeg_evidence(snapshot_root, filename, expected_identity=published_identity)
        byte_size = int(evidence.info["size"])
        width, height = int(evidence.info["w"]), int(evidence.info["h"])
    except (
        JpegDecodeError,
        OSError,
        UnidentifiedImageError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ) as exc:
        if copied_snapshot and published_identity is not None:
            try:
                delete_owned_artifact(snapshot_root, None, filename, expected_identity=published_identity)
            except OSError:
                pass
        raise MatrixError(
            "Matrix snapshot metadata could not be read as JPEG",
            error_type="snapshot_metadata_failed",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
            byte_size=byte_size,
            exception_type=exc.__class__.__name__,
        ) from exc

    info: dict[str, int | str] = {"mimetype": JPEG_MIMETYPE, "size": byte_size, "w": width, "h": height}
    log_context = _sanitize_diagnostics(
        {
            "event_type": event_type_text,
            "event_id": event_id_text,
            "spot_id": spot_id,
            "source_path": str(source),
            "snapshot_path": str(destination),
            "byte_size": byte_size,
            "mimetype": JPEG_MIMETYPE,
            "width": width,
            "height": height,
        }
    )
    if snapshot_retention_count is not None:
        prune_event_snapshots(
            snapshot_root,
            retention_count=snapshot_retention_count,
            logger=logger,
            current_snapshot=destination,
            trigger=retention_trigger,
            protected_snapshots=protected_snapshots,
        )
    return MatrixSnapshot(
        path=destination,
        filename=filename,
        txn_id=f"snapshot-{Path(filename).stem}",
        body=snapshot_body(spot_id=spot_id, observed_at=observed_text),
        info=info,
        log_context=log_context,
        evidence=evidence,
    )


def prune_event_snapshots(
    snapshot_root: str | Path,
    retention_count: int,
    logger: StructuredLogger | None,
    *,
    current_snapshot: str | Path | None = None,
    trigger: str = "manual",
    protected_snapshots: Sequence[str | Path] | None = None,
) -> SnapshotRetentionResult:
    """Prune oldest Matrix event snapshot files while preserving unrelated runtime files.

    Only JPEG names following the event snapshot contract generated by
    ``prepare_event_snapshot`` are considered. Missing directories are empty,
    malformed filenames are ignored, and deletion failures are logged without
    raising so Matrix delivery and monitor startup can continue.
    """

    root = Path(snapshot_root)
    if retention_count < 1:
        _log_retention_failure(
            logger,
            root=root,
            trigger=trigger,
            error_type="ValueError",
            message="snapshot retention count must be positive",
        )
        return SnapshotRetentionResult(failed_count=1)
    if not root.exists():
        return SnapshotRetentionResult()
    try:
        candidates = [path for path in secure_snapshot_candidates(root) if _is_event_snapshot_file(path)]
    except OSError as exc:
        _log_retention_failure(logger, root=root, trigger=trigger, error_type=type(exc).__name__, message=str(exc))
        return SnapshotRetentionResult(failed_count=1)

    candidates.sort(key=lambda path: (_safe_mtime_ns(path), path.name))
    retained_count = min(len(candidates), retention_count)
    if len(candidates) <= retention_count:
        return SnapshotRetentionResult(retained_count=len(candidates))

    current = Path(current_snapshot).resolve() if current_snapshot is not None else None
    protected = _resolved_paths(protected_snapshots or ())
    if current is not None:
        protected.add(current)
    to_delete = candidates[: len(candidates) - retention_count]
    pruned_count = 0
    pruned_bytes = 0
    failed_count = 0
    for path in to_delete:
        if _path_in_resolved_set(path, protected):
            continue
        try:
            derivative_result = delete_upload_derivative(root, path.name)
            if derivative_result.status == "failed":
                raise OSError("upload derivative cleanup failed")
            raw_result = delete_owned_artifact(root, None, path.name)
            if raw_result.status == "failed":
                raise OSError("retained snapshot cleanup failed")
        except OSError as exc:
            failed_count += 1
            _log_retention_failure(logger, root=root, trigger=trigger, error_type=type(exc).__name__, message=str(exc))
            continue
        pruned_count += 1
        pruned_bytes += raw_result.bytes_deleted + derivative_result.bytes_deleted

    if pruned_count:
        _log_retention_pruned(
            logger,
            root=root,
            trigger=trigger,
            pruned_count=pruned_count,
            pruned_bytes=pruned_bytes,
            retained_count=len(candidates) - pruned_count,
        )
    return SnapshotRetentionResult(
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
        retained_count=len(candidates) - pruned_count,
        failed_count=failed_count,
    )


_EVENT_SNAPSHOT_PATTERN = re.compile(
    r"^[a-z0-9]+(?:-[a-z0-9]+)*-.+-\d{4}-\d{2}-\d{2}t\d{2}-\d{2}-\d{2}z\.jpg$"
)


def _is_event_snapshot_file(path: Path) -> bool:
    return bool(_EVENT_SNAPSHOT_PATTERN.match(path.name))


def _safe_mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return 0


def _same_path(path: Path, current: Path) -> bool:
    try:
        return path.resolve() == current.resolve()
    except OSError:
        return False


def _resolved_paths(paths: Sequence[str | Path]) -> set[Path]:
    resolved: set[Path] = set()
    for path in paths:
        try:
            resolved.add(Path(path).resolve())
        except OSError:
            continue
    return resolved


def _path_in_resolved_set(path: Path, resolved: set[Path]) -> bool:
    try:
        return path.resolve() in resolved
    except OSError:
        return False


def _log_retention_pruned(
    logger: StructuredLogger | None,
    *,
    root: Path,
    trigger: str,
    pruned_count: int,
    pruned_bytes: int,
    retained_count: int,
) -> None:
    if logger is None:
        return
    logger.info(
        "snapshot-retention-pruned",
        root=str(root),
        trigger=trigger,
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
        retained_count=retained_count,
    )


def _log_retention_failure(
    logger: StructuredLogger | None,
    *,
    root: Path,
    trigger: str,
    error_type: str,
    message: str,
    failed_count: int = 1,
    pruned_count: int = 0,
    pruned_bytes: int = 0,
) -> None:
    if logger is None:
        return
    logger.warning(
        "snapshot-retention-failed",
        root=str(root),
        trigger=trigger,
        error_type=error_type,
        message=message,
        failed_count=failed_count,
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
    )
