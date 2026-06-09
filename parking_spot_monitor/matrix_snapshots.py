"""Matrix event snapshot retention and upload preparation."""

from __future__ import annotations

import re
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
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


@dataclass(frozen=True)
class SnapshotRetentionResult:
    """Safe summary of an event snapshot retention pruning attempt."""

    pruned_count: int = 0
    pruned_bytes: int = 0
    retained_count: int = 0
    failed_count: int = 0

def _matrix_snapshot_upload(
    snapshot: MatrixSnapshot,
    *,
    logger: StructuredLogger | None,
) -> dict[str, Any]:
    source_size = snapshot.path.stat().st_size
    if source_size <= MAX_MATRIX_UPLOAD_IMAGE_BYTES:
        raw = snapshot.path.read_bytes()
        return {"data": raw, "info": dict(snapshot.info)}

    data, info = _resize_jpeg_for_matrix_upload(snapshot.path)
    if logger is not None:
        logger.info(
            "matrix-snapshot-upload-resized",
            snapshot_path=str(snapshot.path),
            source_size=source_size,
            upload_size=info["size"],
            width=info["w"],
            height=info["h"],
        )
    return {"data": data, "info": info}


def _resize_jpeg_for_matrix_upload(path: Path) -> tuple[bytes, dict[str, int | str]]:
    with Image.open(path) as image:
        width, height = image.size
        if width <= 0 or height <= 0:
            raise MatrixError("Matrix snapshot dimensions are invalid", error_type="snapshot_resize_failed", snapshot_path=str(path))

        max_dimension = min(max(width, height), MATRIX_UPLOAD_INITIAL_MAX_DIMENSION)
        resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
        first_attempt = True
        while first_attempt or max_dimension >= MATRIX_UPLOAD_MIN_DIMENSION:
            first_attempt = False
            resized = image.copy()
            resized.thumbnail((max_dimension, max_dimension), resampling)
            if resized.mode != "RGB":
                resized = resized.convert("RGB")
            for quality in MATRIX_UPLOAD_JPEG_QUALITIES:
                buffer = BytesIO()
                resized.save(buffer, format="JPEG", quality=quality, optimize=True)
                data = buffer.getvalue()
                if len(data) <= MAX_MATRIX_UPLOAD_IMAGE_BYTES:
                    output_width, output_height = resized.size
                    return data, {"mimetype": JPEG_MIMETYPE, "size": len(data), "w": output_width, "h": output_height}
            max_dimension = int(max_dimension * 0.85)
    raise MatrixError("Matrix snapshot could not be resized under upload budget", error_type="snapshot_resize_failed", snapshot_path=str(path))


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
    snapshot_root = Path(snapshots_dir) if snapshots_dir is not None else Path(data_dir) / "snapshots"
    filename = _snapshot_filename(
        event_type=event_type_text,
        stable_id=spot_id or event_id_text,
        observed_at=observed_text,
    )
    destination = snapshot_root / filename

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

    try:
        snapshot_root.mkdir(parents=True, exist_ok=True)
        copied_snapshot = not _same_path(source, destination)
        if copied_snapshot:
            shutil.copyfile(source, destination)
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

    byte_size = destination.stat().st_size
    try:
        width, height = _jpeg_dimensions(destination)
    except (OSError, UnidentifiedImageError) as exc:
        if copied_snapshot:
            try:
                destination.unlink()
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
        body=_snapshot_body(spot_id=spot_id, observed_at=observed_text),
        info=info,
        log_context=log_context,
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
        candidates = [path for path in root.iterdir() if path.is_file() and _is_event_snapshot_file(path)]
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
            byte_size = path.stat().st_size
            path.unlink()
        except OSError as exc:
            failed_count += 1
            _log_retention_failure(logger, root=root, trigger=trigger, error_type=type(exc).__name__, message=str(exc))
            continue
        pruned_count += 1
        pruned_bytes += byte_size

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

def _jpeg_dimensions(path: Path) -> tuple[int, int]:
    with Image.open(path) as image:
        if image.format != "JPEG":
            raise OSError("snapshot is not a JPEG image")
        width, height = image.size
        image.verify()
    return width, height


def _snapshot_filename(*, event_type: str, stable_id: str, observed_at: str) -> str:
    return f"{_path_token(event_type)}-{_path_token(stable_id)}-{_path_token(observed_at)}.jpg"


def _path_token(value: object) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "-", redact_diagnostic_text(value).strip().lower()).strip("-")
    return token or "unknown"


def _snapshot_body(*, spot_id: str | None, observed_at: str) -> str:
    subject = redact_diagnostic_text(spot_id) if spot_id else "parking spot"
    return f"Raw full-frame snapshot for {subject} at {observed_at.replace('Z', '+00:00')}"
