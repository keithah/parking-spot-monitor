"""Bounded JPEG metadata and RGB decode lifecycles."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Literal
import warnings

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.file_descriptor_binding import FileIdentity, open_owned_path


@dataclass(frozen=True, slots=True)
class DecodedRgbJpeg:
    image: Image.Image
    source_width: int
    source_height: int


class JpegDecodeError(RuntimeError):
    def __init__(
        self,
        code: Literal["unidentified", "decompression_bomb", "invalid_dimensions", "read_failed"],
        *,
        source_error_type: str | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.source_error_type = source_error_type


def open_decoded_rgb_jpeg(
    path: Path, *, initial_max_dimension: int, expected_identity: FileIdentity | None = None
) -> AbstractContextManager[DecodedRgbJpeg]:
    if expected_identity is None:
        return _decoded_rgb_jpeg(Path(path), initial_max_dimension=initial_max_dimension)
    return _decoded_owned_rgb_jpeg(Path(path), expected_identity, initial_max_dimension=initial_max_dimension)


def open_decoded_rgb_jpeg_bytes(
    payload: bytes, *, initial_max_dimension: int
) -> AbstractContextManager[DecodedRgbJpeg]:
    return _decoded_rgb_jpeg_bytes(bytes(payload), initial_max_dimension=initial_max_dimension)


def open_decoded_rgb_jpeg_stream(
    source: object, *, initial_max_dimension: int
) -> AbstractContextManager[DecodedRgbJpeg]:
    return _decoded_rgb_jpeg(source, initial_max_dimension=initial_max_dimension)


@contextmanager
def _decoded_owned_rgb_jpeg(
    path: Path, identity: FileIdentity, *, initial_max_dimension: int
) -> Iterator[DecodedRgbJpeg]:
    with open_owned_path(path, identity) as source:
        with _decoded_rgb_jpeg(source, initial_max_dimension=initial_max_dimension) as decoded:
            yield decoded


def jpeg_bytes_dimensions(payload: bytes) -> tuple[int, int]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(payload)) as image:
                if image.format != "JPEG":
                    raise JpegDecodeError("unidentified")
                if image.width <= 0 or image.height <= 0:
                    raise JpegDecodeError("invalid_dimensions")
                dimensions = image.size
                image.verify()
                return dimensions
    except JpegDecodeError:
        raise
    except UnidentifiedImageError as exc:
        raise JpegDecodeError("unidentified", source_error_type=exc.__class__.__name__) from exc
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
        raise JpegDecodeError("decompression_bomb", source_error_type=exc.__class__.__name__) from exc
    except (OSError, ValueError) as exc:
        raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc


@contextmanager
def _decoded_rgb_jpeg_bytes(payload: bytes, *, initial_max_dimension: int) -> Iterator[DecodedRgbJpeg]:
    with BytesIO(payload) as source:
        with _decoded_rgb_jpeg(source, initial_max_dimension=initial_max_dimension) as decoded:
            yield decoded


@contextmanager
def _decoded_rgb_jpeg(path: Any, *, initial_max_dimension: int) -> Iterator[DecodedRgbJpeg]:
    opened: Image.Image | None = None
    working: Image.Image | None = None
    try:
        try:
            if initial_max_dimension <= 0:
                raise JpegDecodeError("invalid_dimensions")
            with warnings.catch_warnings():
                warnings.simplefilter("error", Image.DecompressionBombWarning)
                opened = Image.open(path)
                if opened.format != "JPEG":
                    raise JpegDecodeError("unidentified")
                width, height = opened.size
                if width <= 0 or height <= 0:
                    raise JpegDecodeError("invalid_dimensions")
                bounded_dimension = min(max(width, height), initial_max_dimension)
                bounded_size = (
                    (bounded_dimension, max(1, height * bounded_dimension // width))
                    if width >= height
                    else (max(1, width * bounded_dimension // height), bounded_dimension)
                )
                opened.draft("RGB", bounded_size)
                opened.load()
                working = opened if opened.mode == "RGB" else opened.convert("RGB")
        except JpegDecodeError:
            raise
        except UnidentifiedImageError as exc:
            raise JpegDecodeError("unidentified", source_error_type=exc.__class__.__name__) from exc
        except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
            raise JpegDecodeError("decompression_bomb", source_error_type=exc.__class__.__name__) from exc
        except (OSError, ValueError) as exc:
            raise JpegDecodeError("read_failed", source_error_type=exc.__class__.__name__) from exc
        yield DecodedRgbJpeg(working, width, height)
    finally:
        if working is not None and working is not opened:
            working.close()
        if opened is not None:
            opened.close()
