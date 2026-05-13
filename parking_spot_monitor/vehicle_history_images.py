from __future__ import annotations

import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from PIL import Image, UnidentifiedImageError

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
    """Copy full-frame JPEG and crop accepted bbox into archive-owned paths.

    The source frame is opened with Pillow and re-saved as RGB JPEG output so the
    archive owns durable artifacts independent of any mutable runtime snapshot.
    Bboxes use detector-style ``(x_min, y_min, x_max, y_max)`` coordinates with
    floor/ceil rounding, clamping to the source image, and empty-box rejection.
    """

    root = Path(archive_root)
    full_frame_path = root / "images" / "occupied-full" / f"{session_id}.jpg"
    crop_path = root / "images" / "occupied-crops" / f"{session_id}.jpg"

    try:
        with Image.open(source_frame_path) as opened:
            if opened.format != "JPEG":
                raise VehicleHistoryImageError("source occupied frame must be a JPEG")
            image = opened.convert("RGB")
    except VehicleHistoryImageError:
        raise
    except (FileNotFoundError, UnidentifiedImageError, OSError) as exc:
        raise VehicleHistoryImageError("source occupied frame is missing or unreadable") from exc

    crop_box = clamp_crop_box(bbox, image.size)
    crop = image.crop(crop_box.as_pillow_box)

    try:
        _write_jpeg_atomic(full_frame_path, image)
        _write_jpeg_atomic(crop_path, crop)
    except Exception as exc:
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
