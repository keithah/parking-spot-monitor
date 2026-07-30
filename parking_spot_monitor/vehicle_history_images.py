from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
import secrets
from typing import Sequence

from PIL import Image

from parking_spot_monitor.file_descriptor_binding import OwnedFile, RootedDirectoryOwner, descriptor_identity, open_owned_at
from parking_spot_monitor.jpeg_artifacts import JpegDecodeError, JpegPublication, open_decoded_rgb_jpeg_stream, publish_canonical_jpeg_to_owner
from parking_spot_monitor.owned_file_recovery import RecoveryResult

BBoxInput = Sequence[float]


class VehicleHistoryImageError(RuntimeError):
    """Raised when occupied session images cannot be safely captured."""


@dataclass(frozen=True)
class OccupiedImageCaptureResult:
    """Archive-owned occupied image artifact paths for a vehicle session."""

    full_frame_path: Path
    crop_path: Path


@dataclass(frozen=True)
class ClampedCropBox:
    """Integer crop box after floor/ceil rounding and image-bound clamping."""

    left: int
    top: int
    right: int
    bottom: int

    @property
    def as_pillow_box(self) -> tuple[int, int, int, int]:
        return (self.left, self.top, self.right, self.bottom)


def capture_occupied_images(
    *,
    archive_root: str | os.PathLike[str],
    session_id: str,
    source_frame_path: str | os.PathLike[str],
    bbox: BBoxInput,
) -> OccupiedImageCaptureResult:
    """Publish a canonical full-frame JPEG and crop the accepted bbox.

    The archive owns an independent reflink or bounded copy of the validated
    source bytes, avoiding a full-frame decode/encode cycle.
    Bboxes use detector-style ``(x_min, y_min, x_max, y_max)`` coordinates with
    floor/ceil rounding, clamping to the source image, and empty-box rejection.
    """

    root = Path(archive_root)
    full_frame_path = root / "images" / "occupied-full" / f"{session_id}.jpg"
    crop_path = root / "images" / "occupied-crops" / f"{session_id}.jpg"

    with RootedDirectoryOwner(root, create=True) as archive_owner:
        with archive_owner.open_child("images", create=True) as images_owner:
            with images_owner.open_child("occupied-full", create=True) as full_owner:
                with images_owner.open_child("occupied-crops", create=True) as crop_owner:
                    try:
                        full_recovery = full_owner.recover_owned()
                        crop_recovery = crop_owner.recover_owned()
                    except OSError as exc:
                        raise VehicleHistoryImageError("vehicle image recovery failed") from exc
                    if full_recovery.pending or crop_recovery.pending:
                        raise VehicleHistoryImageError("vehicle image recovery remains pending")
                    _capture_owned_images(
                        source_frame_path=source_frame_path,
                        bbox=bbox,
                        full_path=full_frame_path,
                        crop_path=crop_path,
                        owners=(archive_owner, images_owner, full_owner, crop_owner),
                    )

    return OccupiedImageCaptureResult(full_frame_path=full_frame_path, crop_path=crop_path)


def recover_vehicle_image_artifacts(archive_root: str | os.PathLike[str]) -> RecoveryResult:
    """Recover bounded pending cleanup in both archive-owned image directories."""

    recovered = 0
    pending = False
    try:
        with RootedDirectoryOwner(Path(archive_root), create=False) as archive_owner:
            with archive_owner.open_child("images", create=False) as images_owner:
                for child_name in ("occupied-full", "occupied-crops"):
                    try:
                        with images_owner.open_child(child_name, create=False) as owner:
                            result = owner.recover_owned()
                    except FileNotFoundError:
                        continue
                    recovered += result.recovered
                    pending = pending or result.pending
    except FileNotFoundError:
        return RecoveryResult()
    except OSError:
        return RecoveryResult(pending=True)
    return RecoveryResult(recovered, pending)


def _capture_owned_images(
    *,
    source_frame_path: str | os.PathLike[str],
    bbox: BBoxInput,
    full_path: Path,
    crop_path: Path,
    owners: tuple[RootedDirectoryOwner, RootedDirectoryOwner, RootedDirectoryOwner, RootedDirectoryOwner],
) -> None:
    archive_owner, images_owner, full_owner, crop_owner = owners
    full_publication = crop_publication = None
    try:
        full_publication = publish_canonical_jpeg_to_owner(source_frame_path, full_path, full_owner)
        with open_owned_at(full_owner, full_path.name, full_publication.identity) as source:
            with open_decoded_rgb_jpeg_stream(source, initial_max_dimension=2**31 - 1) as decoded:
                crop_box = clamp_crop_box(bbox, decoded.image.size)
                with decoded.image.crop(crop_box.as_pillow_box) as crop:
                    crop_publication = _write_jpeg_atomic(crop_path, crop, owner=crop_owner)
        if not all(owner.is_still_bound() for owner in owners):
            raise OSError("vehicle image archive binding changed")
        if not full_owner.matches(full_path.name, full_publication.identity):
            raise OSError("vehicle full-frame binding changed")
        if not crop_owner.matches(crop_path.name, crop_publication.identity):
            raise OSError("vehicle crop binding changed")
    except JpegDecodeError as exc:
        _cleanup_owned_images(full_owner, full_path, full_publication, crop_owner, crop_path, crop_publication)
        message = "source occupied frame must be a JPEG" if exc.code == "unidentified" else "source occupied frame is missing or unreadable"
        raise VehicleHistoryImageError(message) from exc
    except (VehicleHistoryImageError, OSError, ValueError) as exc:
        _cleanup_owned_images(full_owner, full_path, full_publication, crop_owner, crop_path, crop_publication)
        raise VehicleHistoryImageError(str(exc) or exc.__class__.__name__) from exc


def _cleanup_owned_images(
    full_owner: RootedDirectoryOwner,
    full_path: Path,
    full_publication: JpegPublication | None,
    crop_owner: RootedDirectoryOwner,
    crop_path: Path,
    crop_publication: OwnedFile | None,
) -> None:
    if crop_publication is not None:
        crop_owner.unlink_if_matches(crop_path.name, crop_publication.identity)
    if full_publication is not None:
        full_owner.unlink_if_matches(full_path.name, full_publication.identity)


def clamp_crop_box(bbox: BBoxInput, image_size: tuple[int, int]) -> ClampedCropBox:
    """Round detector bbox outward, clamp to image bounds, and reject empties."""
    if len(bbox) != 4:
        raise VehicleHistoryImageError("bbox must contain exactly four coordinates")
    width, height = image_size
    if width <= 0 or height <= 0:
        raise VehicleHistoryImageError("source occupied frame has invalid dimensions")

    try:
        x_min, y_min, x_max, y_max = (float(value) for value in bbox)
    except (TypeError, ValueError) as exc:
        raise VehicleHistoryImageError("bbox coordinates must be finite numbers") from exc
    if not all(math.isfinite(value) for value in (x_min, y_min, x_max, y_max)):
        raise VehicleHistoryImageError("bbox coordinates must be finite numbers")

    left = max(0, min(width, math.floor(x_min)))
    top = max(0, min(height, math.floor(y_min)))
    right = max(0, min(width, math.ceil(x_max)))
    bottom = max(0, min(height, math.ceil(y_max)))

    if right <= left or bottom <= top:
        raise VehicleHistoryImageError("bbox is empty after clamping to source image bounds")
    return ClampedCropBox(left=left, top=top, right=right, bottom=bottom)


def _write_jpeg_atomic(path: Path, image: Image.Image, *, owner: RootedDirectoryOwner) -> OwnedFile:
    temporary_name = f".{path.name}.{secrets.token_hex(8)}.tmp"
    temporary_fd = -1
    identity = None
    replaced = False
    try:
        temporary_fd = owner.open_file(temporary_name, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(os.dup(temporary_fd), "wb") as handle:
            image.save(handle, format="JPEG")
            handle.flush()
            os.fsync(handle.fileno())
        os.fchmod(temporary_fd, 0o644)
        os.fsync(temporary_fd)
        identity = descriptor_identity(temporary_fd)
        owner.replace(temporary_name, path.name)
        replaced = True
        if not owner.matches(path.name, identity):
            raise OSError("vehicle crop changed during publication")
        owner.fsync()
        if not owner.matches(path.name, identity) or not owner.is_still_bound():
            raise OSError("vehicle crop binding changed")
        return OwnedFile(path, identity)
    except Exception:
        if replaced and identity is not None:
            owner.unlink_if_matches(path.name, identity)
        raise
    finally:
        if not replaced and temporary_fd >= 0:
            owner.unlink_if_matches(temporary_name, descriptor_identity(temporary_fd))
        if temporary_fd >= 0:
            os.close(temporary_fd)
