from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest
from PIL import Image

from parking_spot_monitor.config import load_settings
from parking_spot_monitor.debug_overlay import DebugOverlayError, write_debug_overlay
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
