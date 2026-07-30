from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from parking_spot_monitor.config import load_settings
from parking_spot_monitor.debug_overlay import (
    DebugOverlayError,
    _configured_spots,
    _draw_spot_overlay,
    write_debug_overlay,
)
from parking_spot_monitor.logging import StructuredLogger
from tests.test_config import SECRET_MARKER, fake_environ


LEFT_EDGE_PIXEL = (300, 180)
RIGHT_EDGE_PIXEL = (1010, 215)


def synthetic_camera_fixture(path: Path) -> Path:
    settings = load_example_settings()
    Image.new("RGB", (settings.stream.frame_width, settings.stream.frame_height), (20, 30, 40)).save(path, format="PNG")
    return path


def load_example_settings():
    return load_settings("config.yaml.example", environ=fake_environ())


def records_from(stream: StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_spot_overlay_does_not_allocate_or_convert_full_frame_rgba(monkeypatch: pytest.MonkeyPatch) -> None:
    image = Image.new("RGB", (1458, 806), (20, 30, 40))
    original_new = Image.new
    original_convert = Image.Image.convert

    def guarded_new(mode: str, size: tuple[int, int], *args: object, **kwargs: object) -> Image.Image:
        if mode == "RGBA" and size == image.size:
            raise AssertionError("full-frame overlay allocation")
        return original_new(mode, size, *args, **kwargs)

    def guarded_convert(
        source: Image.Image, mode: str | None = None, *args: object, **kwargs: object
    ) -> Image.Image:
        if source.size == image.size and mode == "RGBA":
            raise AssertionError("full-frame RGBA conversion")
        return original_convert(source, mode, *args, **kwargs)

    monkeypatch.setattr(Image, "new", guarded_new)
    monkeypatch.setattr(Image.Image, "convert", guarded_convert)

    _draw_spot_overlay(image, _configured_spots(load_example_settings()))


def test_spot_overlay_does_not_alpha_composite_full_frames(monkeypatch: pytest.MonkeyPatch) -> None:
    image = Image.new("RGB", (1458, 806), (20, 30, 40))

    def reject_alpha_composite(*_args: object, **_kwargs: object) -> Image.Image:
        raise AssertionError("full-frame alpha composite")

    monkeypatch.setattr(Image, "alpha_composite", reject_alpha_composite)

    _draw_spot_overlay(image, _configured_spots(load_example_settings()))


def test_spot_overlay_preserves_representative_pre_jpeg_pixels(monkeypatch: pytest.MonkeyPatch) -> None:
    image = Image.new("RGB", (1458, 806), (20, 30, 40))
    original_text = ImageDraw.ImageDraw.text
    text_calls: list[tuple[tuple[int, int], str, tuple[int, int, int, int] | None]] = []

    def recording_text(
        draw: ImageDraw.ImageDraw,
        position: tuple[int, int],
        label: str,
        *args: object,
        **kwargs: object,
    ) -> None:
        text_calls.append((position, label, kwargs.get("fill")))
        original_text(draw, position, label, *args, **kwargs)

    monkeypatch.setattr(ImageDraw.ImageDraw, "text", recording_text)

    _draw_spot_overlay(image, _configured_spots(load_example_settings()))

    assert image.getpixel((350, 200)) == pytest.approx((30, 75, 99), abs=1)
    assert image.getpixel(LEFT_EDGE_PIXEL) == (14, 165, 233)
    assert text_calls == [
        ((306, 186), "left_spot", (14, 165, 233, 255)),
        ((1016, 221), "right_spot", (22, 163, 74, 255)),
    ]
    left_label_pixels = list(image.crop((306, 191, 380, 197)).getdata())
    near_opaque_label = [
        pixel
        for pixel in left_label_pixels
        if all(abs(pixel[channel] - (14, 165, 233)[channel]) <= 12 for channel in range(3))
    ]
    assert len(near_opaque_label) >= 8


def test_write_debug_overlay_creates_exactly_one_rgb_working_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = load_example_settings()
    source_path = tmp_path / "rgba.png"
    output_path = tmp_path / "debug_latest.jpg"
    Image.new("RGBA", (settings.stream.frame_width, settings.stream.frame_height), (10, 20, 30, 128)).save(source_path)
    original_convert = Image.Image.convert
    rgb_conversions = 0

    def counted_convert(
        source: Image.Image, mode: str | None = None, *args: object, **kwargs: object
    ) -> Image.Image:
        nonlocal rgb_conversions
        if mode == "RGB":
            rgb_conversions += 1
        return original_convert(source, mode, *args, **kwargs)

    monkeypatch.setattr(Image.Image, "convert", counted_convert)

    write_debug_overlay(settings, source_path, output_path)

    assert rgb_conversions == 1


def test_write_debug_overlay_renders_configured_spot_polygons_to_jpeg(tmp_path: Path) -> None:
    settings = load_example_settings()
    source_path = synthetic_camera_fixture(tmp_path / "camera.png")
    output_path = tmp_path / "nested" / "debug_latest.jpg"
    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)
    raw_source_bytes = source_path.read_bytes()

    result = write_debug_overlay(settings, source_path, output_path, logger=logger)

    assert output_path.exists()
    assert source_path.read_bytes() == raw_source_bytes
    assert result.source_path == str(source_path)
    assert result.output_path == str(output_path)
    assert result.width == 1458
    assert result.height == 806
    assert result.spot_ids == ("left_spot", "right_spot")

    with Image.open(output_path) as image:
        assert image.format == "JPEG"
        assert image.mode == "RGB"
        assert image.size == (1458, 806)
        rendered_left = image.getpixel(LEFT_EDGE_PIXEL)
        rendered_right = image.getpixel(RIGHT_EDGE_PIXEL)

    with Image.open(source_path) as source:
        source_rgb = source.convert("RGB")
        assert rendered_left != source_rgb.getpixel(LEFT_EDGE_PIXEL)
        assert rendered_right != source_rgb.getpixel(RIGHT_EDGE_PIXEL)

    records = records_from(log_stream)
    assert records == [
        {
            "event": "debug-overlay-written",
            "level": "INFO",
            "source_path": str(source_path),
            "output_path": str(output_path),
            "width": 1458,
            "height": 806,
            "spot_ids": ["left_spot", "right_spot"],
        }
    ]


def test_write_debug_overlay_converts_rgba_source_to_rgb_jpeg(tmp_path: Path) -> None:
    settings = load_example_settings()
    source_path = tmp_path / "rgba.png"
    output_path = tmp_path / "debug_latest.jpg"
    Image.new("RGBA", (settings.stream.frame_width, settings.stream.frame_height), (10, 20, 30, 128)).save(source_path)

    result = write_debug_overlay(settings, source_path, output_path)

    assert result.width == settings.stream.frame_width
    assert result.height == settings.stream.frame_height
    with Image.open(output_path) as image:
        assert image.format == "JPEG"
        assert image.mode == "RGB"
        assert image.size == (settings.stream.frame_width, settings.stream.frame_height)


@pytest.mark.parametrize(
    "source_name,content,expected_error_type",
    [
        ("missing.jpg", None, "FileNotFoundError"),
        ("corrupt.jpg", b"not an image with secret should-not-leak", "UnidentifiedImageError"),
    ],
)
def test_write_debug_overlay_reports_missing_and_corrupt_sources_safely(
    tmp_path: Path,
    source_name: str,
    content: bytes | None,
    expected_error_type: str,
) -> None:
    settings = load_example_settings()
    source_path = tmp_path / source_name
    output_path = tmp_path / "debug_latest.jpg"
    if content is not None:
        source_path.write_bytes(content)
    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)

    with pytest.raises(DebugOverlayError) as exc_info:
        write_debug_overlay(settings, source_path, output_path, logger=logger)

    assert not output_path.exists()
    error = exc_info.value
    assert error.diagnostics["source_path"] == str(source_path)
    assert error.diagnostics["output_path"] == str(output_path)
    assert error.diagnostics["spot_ids"] == ["left_spot", "right_spot"]
    assert error.diagnostics["error_type"] == expected_error_type
    assert "traceback" not in str(error).lower()
    assert SECRET_MARKER not in str(error)

    records = records_from(log_stream)
    assert len(records) == 1
    record = records[0]
    assert record["event"] == "debug-overlay-failed"
    assert record["level"] == "ERROR"
    assert record["error_type"] == expected_error_type
    assert record["source_path"] == str(source_path)
    assert record["output_path"] == str(output_path)
    assert record["spot_ids"] == ["left_spot", "right_spot"]
    rendered_record = json.dumps(record)
    assert "traceback" not in rendered_record.lower()
    assert SECRET_MARKER not in rendered_record


def test_write_debug_overlay_reports_save_failures_safely(tmp_path: Path) -> None:
    settings = load_example_settings()
    source_path = synthetic_camera_fixture(tmp_path / "camera.png")
    output_path = tmp_path / "not-a-file"
    output_path.mkdir()
    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)

    with pytest.raises(DebugOverlayError) as exc_info:
        write_debug_overlay(settings, source_path, output_path, logger=logger)

    assert exc_info.value.diagnostics["error_type"] in {"IsADirectoryError", "PermissionError", "OSError"}
    assert SECRET_MARKER not in str(exc_info.value)
    records = records_from(log_stream)
    assert records[0]["event"] == "debug-overlay-failed"
    assert records[0]["error_type"] in {"IsADirectoryError", "PermissionError", "OSError"}
    assert SECRET_MARKER not in json.dumps(records[0])
