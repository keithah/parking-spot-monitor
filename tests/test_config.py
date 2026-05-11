from __future__ import annotations

from pathlib import Path

import pytest

from parking_spot_monitor.config import load_settings
from parking_spot_monitor.errors import ConfigError
from parking_spot_monitor.paths import resolve_runtime_paths


SECRET_MARKER = "should-not-leak"
FAKE_RTSP_URL = f"camera-secret-{SECRET_MARKER}"
FAKE_MATRIX_TOKEN = f"matrix-secret-{SECRET_MARKER}"


def fake_environ(**overrides: str) -> dict[str, str]:
    environ = {
        "RTSP_URL": FAKE_RTSP_URL,
        "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN,
    }
    environ.update(overrides)
    return environ


def write_config(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def test_runtime_paths_resolve_relative_values_under_effective_data_dir() -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())

    paths = resolve_runtime_paths(settings, Path("/data"))

    assert paths.data_dir == Path("/data")
    assert paths.state_file == Path("/data/state.json")
    assert paths.latest_frame == Path("/data/latest.jpg")
    assert paths.snapshots_dir == Path("/data/snapshots")
    assert paths.health_file == Path("/data/health.json")


def test_runtime_paths_preserve_absolute_operator_overrides(tmp_path: Path) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    absolute_snapshots = tmp_path / "operator-snapshots"
    absolute_health = tmp_path / "operator-health.json"
    config = base.replace("snapshots_dir: snapshots", f"snapshots_dir: {absolute_snapshots}").replace(
        "health_file: health.json", f"health_file: {absolute_health}"
    )
    path = write_config(tmp_path, config)
    settings = load_settings(path, environ=fake_environ())

    paths = resolve_runtime_paths(settings, Path("/data"))

    assert paths.snapshots_dir == absolute_snapshots
    assert paths.health_file == absolute_health
    assert paths.state_file == Path("/data/state.json")
    assert paths.latest_frame == Path("/data/latest.jpg")


def test_runtime_paths_default_omitted_snapshots_to_effective_data_dir(tmp_path: Path) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    config = base.replace("  snapshots_dir: snapshots\n", "")
    path = write_config(tmp_path, config)
    settings = load_settings(path, environ=fake_environ())

    paths = resolve_runtime_paths(settings, Path("/data"))

    assert settings.storage.snapshots_dir is None
    assert paths.snapshots_dir == Path("/data/snapshots")


def test_example_config_loads_with_fake_env_values() -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())

    assert settings.stream.rtsp_url.value == FAKE_RTSP_URL
    assert settings.matrix.access_token.value == FAKE_MATRIX_TOKEN
    assert settings.spots.left_spot.name == "Left spot"
    assert settings.spots.right_spot.name == "Right spot"


@pytest.mark.parametrize("model_value", ["yolov8n.pt", "models/custom-detector.pt"])
def test_detection_model_accepts_local_model_names_and_relative_paths(tmp_path: Path, model_value: str) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("model: yolov8n.pt", f"model: {model_value}")
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.detection.model == model_value


@pytest.mark.parametrize(
    "model_value",
    [
        "https://example.org/yolov8n.pt",
        "http://example.org/yolov8n.pt",
        "s3://bucket/yolov8n.pt",
        "/models/yolov8n.pt",
        "../models/yolov8n.pt",
        "/models/../secret.pt",
        "models/../../secret.pt",
    ],
)
def test_detection_model_rejects_urls_and_path_traversal(tmp_path: Path, model_value: str) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("model: yolov8n.pt", f"model: {model_value}")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    message = str(exc_info.value)
    assert "detection.model" in message
    assert SECRET_MARKER not in message


def test_sanitized_summary_never_contains_secret_values() -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())

    summary = settings.sanitized_summary()
    rendered = repr(summary) + str(summary) + repr(settings) + settings.model_dump_json()

    assert "RTSP_URL" in rendered
    assert "MATRIX_ACCESS_TOKEN" in rendered
    assert FAKE_RTSP_URL not in rendered
    assert FAKE_MATRIX_TOKEN not in rendered
    assert SECRET_MARKER not in rendered
    summary_rendered = repr(summary) + str(summary)
    assert "access_token" not in summary_rendered.lower()
    assert summary["matrix"]["matrix_token"] == {
        "env_var": "Matrix token env key",
        "present": True,
        "value": "**********",
    }
    assert summary["detection"]["min_bbox_area_px"] == settings.detection.min_bbox_area_px
    assert summary["detection"]["min_polygon_overlap_ratio"] == settings.detection.min_polygon_overlap_ratio
    assert summary["quiet_windows"] == [
        {
            "name": "street_sweeping",
            "timezone": "America/Los_Angeles",
            "recurrence": "monthly_weekday",
            "weekdays": ["monday"],
            "ordinals": [1, 3],
            "start": "13:00",
            "end": "15:00",
        }
    ]
    assert summary["storage"]["snapshot_retention_count"] == 50


def test_storage_snapshot_retention_count_must_be_positive(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("snapshot_retention_count: 50", "snapshot_retention_count: 0")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "snapshot_retention_count" in str(exc_info.value)


def test_storage_snapshot_retention_count_is_configurable(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("snapshot_retention_count: 50", "snapshot_retention_count: 3")
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.storage.snapshot_retention_count == 3


def test_runtime_frame_interval_seconds_is_configurable_and_summarized(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("frame_interval_seconds: 30", "frame_interval_seconds: 7")
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.runtime.frame_interval_seconds == 7
    assert settings.sanitized_summary()["runtime"]["frame_interval_seconds"] == 7


@pytest.mark.parametrize("bad_value", ["0", "-1"])
def test_runtime_frame_interval_seconds_must_be_positive(tmp_path: Path, bad_value: str) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("frame_interval_seconds: 30", f"frame_interval_seconds: {bad_value}")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "frame_interval_seconds" in str(exc_info.value)


def test_missing_env_vars_report_names_only() -> None:
    with pytest.raises(ConfigError) as exc_info:
        load_settings("config.yaml.example", environ={"RTSP_URL": ""})

    message = str(exc_info.value)
    assert "RTSP_URL" in message
    assert "MATRIX_ACCESS_TOKEN" in message
    assert FAKE_RTSP_URL not in message
    assert FAKE_MATRIX_TOKEN not in message


def test_unknown_fields_are_rejected(tmp_path: Path) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    path = write_config(tmp_path, base + "\nunexpected: true\n")

    with pytest.raises(ConfigError, match="unexpected"):
        load_settings(path, environ=fake_environ())


@pytest.mark.parametrize("section", ["stream", "spots", "detection", "occupancy", "matrix", "storage", "runtime"])
def test_missing_top_level_sections_are_rejected(tmp_path: Path, section: str) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    lines = base.splitlines()
    start = lines.index(f"{section}:")
    end = next(
        (index for index in range(start + 1, len(lines)) if lines[index] and not lines[index].startswith(" ")),
        len(lines),
    )
    path = write_config(tmp_path, "\n".join(lines[:start] + lines[end:]) + "\n")

    with pytest.raises(ConfigError, match=section):
        load_settings(path, environ=fake_environ())


@pytest.mark.parametrize(
    "section,field,bad_value",
    [
        ("detection", "confidence_threshold", "1.1"),
        ("detection", "min_bbox_area_px", "0"),
        ("detection", "min_polygon_overlap_ratio", "1.1"),
        ("occupancy", "iou_threshold", "-0.1"),
        ("occupancy", "confirm_frames", "0"),
    ],
)
def test_invalid_thresholds_and_counters_are_rejected(
    tmp_path: Path, section: str, field: str, bad_value: str
) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    path = write_config(tmp_path, base.replace(f"{field}: 0.35", f"{field}: {bad_value}").replace(f"{field}: 1200", f"{field}: {bad_value}").replace(f"{field}: 0.2", f"{field}: {bad_value}").replace(f"{field}: 3", f"{field}: {bad_value}"))

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    message = str(exc_info.value)
    assert section in message
    assert field in message


def test_polygons_must_have_three_or_more_points(tmp_path: Path) -> None:
    config = """
stream:
  rtsp_url_env: RTSP_URL
  frame_width: 1920
  frame_height: 1080
spots:
  left_spot:
    name: Left spot
    polygon:
      - [0, 0]
      - [10, 10]
  right_spot:
    name: Right spot
    polygon:
      - [20, 20]
      - [30, 20]
      - [30, 30]
detection:
  model: yolov8n.pt
  confidence_threshold: 0.35
  min_bbox_area_px: 1200
  min_polygon_overlap_ratio: 0.2
occupancy:
  iou_threshold: 0.2
  confirm_frames: 3
matrix:
  homeserver: https://matrix.example.org
  room_id: "!room:example.org"
  access_token_env: MATRIX_ACCESS_TOKEN
quiet_windows: []
storage:
  data_dir: ./data
runtime:
  health_file: ./data/health.json
  log_level: INFO
"""
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError, match="left_spot.*polygon"):
        load_settings(path, environ=fake_environ())


def test_polygon_boundary_points_are_accepted(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8")
    config = config.replace("[300, 180]", "[0, 0]")
    config = config.replace("[650, 215]", "[1458, 806]")
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.spots.left_spot.polygon[0].x == 0
    assert settings.spots.left_spot.polygon[1].y == 806


def test_polygon_points_outside_frame_are_rejected(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("[300, 180]", "[-1, 180]")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "left_spot" in str(exc_info.value)
    assert "polygon" in str(exc_info.value)


@pytest.mark.parametrize("missing_spot", ["left_spot", "right_spot"])
def test_left_and_right_spots_are_required(tmp_path: Path, missing_spot: str) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    lines = base.splitlines()
    start = lines.index(f"  {missing_spot}:")
    end = next(
        (index for index in range(start + 1, len(lines)) if lines[index].startswith("  ") and not lines[index].startswith("    ")),
        len(lines),
    )
    path = write_config(tmp_path, "\n".join(lines[:start] + lines[end:]) + "\n")

    with pytest.raises(ConfigError, match=missing_spot):
        load_settings(path, environ=fake_environ())


def test_missing_config_path_raises_safe_config_error(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.yaml"

    with pytest.raises(ConfigError) as exc_info:
        load_settings(missing_path, environ=fake_environ())

    message = str(exc_info.value)
    assert str(missing_path) in message
    assert FAKE_RTSP_URL not in message
    assert FAKE_MATRIX_TOKEN not in message


def test_bad_yaml_raises_safe_config_error(tmp_path: Path) -> None:
    path = write_config(tmp_path, "stream: [unterminated\n")

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    message = str(exc_info.value)
    assert str(path) in message
    assert "yaml" in message.lower()
    assert FAKE_RTSP_URL not in message
    assert FAKE_MATRIX_TOKEN not in message


def test_invalid_polygon_point_shape_is_rejected(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("[300, 180]", "[300]")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "polygon" in str(exc_info.value)


@pytest.mark.parametrize(
    "field,old_value,bad_value,expected_message",
    [
        ("timezone", "America/Los_Angeles", "Not/A_Zone", "timezone"),
        ("recurrence", "monthly_weekday", "weekly", "recurrence"),
        ("weekdays", "[monday]", "[]", "weekdays"),
        ("weekdays", "[monday]", "[funday]", "weekdays"),
        ("ordinals", "[1, 3]", "[]", "ordinals"),
        ("ordinals", "[1, 3]", "[0]", "ordinals"),
        ("start", '"13:00"', '"1pm"', "start"),
        ("end", '"15:00"', '"13:00"', "end"),
        ("end", '"15:00"', '"12:59"', "end"),
    ],
)
def test_invalid_quiet_window_config_is_rejected_without_secret_leaks(
    tmp_path: Path, field: str, old_value: str, bad_value: str, expected_message: str
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(f"{field}: {old_value}", f"{field}: {bad_value}")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    message = str(exc_info.value)
    assert expected_message in message
    assert FAKE_RTSP_URL not in message
    assert FAKE_MATRIX_TOKEN not in message
    assert SECRET_MARKER not in message
