from __future__ import annotations

import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from PIL import Image

from parking_spot_monitor.file_descriptor_binding import unlink_owned_path
from parking_spot_monitor.jpeg_artifacts import JpegDecodeError, JpegPublication, open_decoded_rgb_jpeg, publish_canonical_jpeg

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

    publication: JpegPublication | None = None
    try:
        publication = publish_canonical_jpeg(source_frame_path, full_frame_path)
        with open_decoded_rgb_jpeg(full_frame_path, initial_max_dimension=2**31 - 1) as decoded:
            crop_box = clamp_crop_box(bbox, decoded.image.size)
            with decoded.image.crop(crop_box.as_pillow_box) as crop:
                _write_jpeg_atomic(crop_path, crop)
    except JpegDecodeError as exc:
        if publication is not None:
            unlink_owned_path(full_frame_path, publication.identity)
        message = "source occupied frame must be a JPEG" if exc.code == "unidentified" else "source occupied frame is missing or unreadable"
        raise VehicleHistoryImageError(message) from exc
    except (VehicleHistoryImageError, OSError, ValueError) as exc:
        if publication is not None:
            unlink_owned_path(full_frame_path, publication.identity)
        raise VehicleHistoryImageError(str(exc) or exc.__class__.__name__) from exc

    return OccupiedImageCaptureResult(full_frame_path=full_frame_path, crop_path=crop_path)


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


def _write_jpeg_atomic(path: Path, image: Image.Image) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        ) as handle:
            temp_path = Path(handle.name)
            image.save(handle, format="JPEG")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_path, 0o644)
        os.replace(temp_path, path)
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise
