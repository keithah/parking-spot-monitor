from __future__ import annotations

from tests.support._config import *  # noqa: F403


def test_runtime_frame_interval_seconds_is_configurable_and_summarized(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("frame_interval_seconds: 30", "frame_interval_seconds: 7")
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.runtime.frame_interval_seconds == 7
    assert settings.sanitized_summary()["runtime"]["frame_interval_seconds"] == 7


def test_adaptive_runtime_settings_are_configurable_and_summarized(tmp_path: Path) -> None:
    config = (
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("  adaptive_polling_enabled: true", "  adaptive_polling_enabled: false")
        .replace("  stable_frame_interval_seconds: 60", "  stable_frame_interval_seconds: 30")
        .replace("  stable_settle_frames: 3", "  stable_settle_frames: 5")
        .replace("  debug_overlay_interval_seconds: 60", "  debug_overlay_interval_seconds: 0")
        .replace("  escalation_verification_seconds: 600", "  escalation_verification_seconds: 0")
    )
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.runtime.adaptive_polling_enabled is False
    assert settings.runtime.stable_frame_interval_seconds == settings.runtime.frame_interval_seconds == 30
    assert settings.runtime.stable_settle_frames == 5
    assert settings.runtime.debug_overlay_interval_seconds == 0
    assert settings.stream.escalation_verification_seconds == 0
    summary = settings.sanitized_summary()
    assert summary["runtime"]["adaptive_polling_enabled"] is False
    assert summary["runtime"]["stable_frame_interval_seconds"] == 30
    assert summary["runtime"]["stable_settle_frames"] == 5
    assert summary["runtime"]["debug_overlay_interval_seconds"] == 0
    assert summary["stream"]["escalation_verification_seconds"] == 0


@pytest.mark.parametrize(
    ("adaptive_polling_line", "expected_adaptive_polling"),
    [("", True), ("  adaptive_polling_enabled: false\n", False)],
)
def test_legacy_runtime_config_omitting_adaptive_intervals_preserves_slower_fixed_cadence(
    tmp_path: Path,
    adaptive_polling_line: str,
    expected_adaptive_polling: bool,
) -> None:
    config = (
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("  frame_interval_seconds: 30", "  frame_interval_seconds: 120")
        .replace("  adaptive_polling_enabled: true\n", adaptive_polling_line)
        .replace("  stable_frame_interval_seconds: 60\n", "")
        .replace("  stable_settle_frames: 3\n", "")
        .replace("  debug_overlay_interval_seconds: 60\n", "")
        .replace("  escalation_verification_seconds: 600\n", "")
    )
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.runtime.frame_interval_seconds == 120
    assert settings.runtime.stable_frame_interval_seconds == 120
    assert settings.runtime.adaptive_polling_enabled is expected_adaptive_polling
    assert settings.runtime.stable_settle_frames == 3
    assert settings.runtime.debug_overlay_interval_seconds == 60
    assert settings.stream.escalation_verification_seconds == 600


@pytest.mark.parametrize(
    ("original", "replacement", "field"),
    [
        ("  debug_overlay_interval_seconds: 60", "  debug_overlay_interval_seconds: -1", "debug_overlay_interval_seconds"),
        ("  escalation_verification_seconds: 600", "  escalation_verification_seconds: -1", "escalation_verification_seconds"),
        ("  stable_settle_frames: 3", "  stable_settle_frames: 0", "stable_settle_frames"),
        ("  stable_frame_interval_seconds: 60", "  stable_frame_interval_seconds: 29", "stable_frame_interval_seconds"),
    ],
)
def test_adaptive_runtime_settings_reject_invalid_values(
    tmp_path: Path,
    original: str,
    replacement: str,
    field: str,
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(original, replacement)
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert field in str(exc_info.value)


@pytest.mark.parametrize("bad_value", ["0", "-1"])
def test_runtime_frame_interval_seconds_must_be_positive(tmp_path: Path, bad_value: str) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("frame_interval_seconds: 30", f"frame_interval_seconds: {bad_value}")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "frame_interval_seconds" in str(exc_info.value)


@pytest.mark.parametrize("bad_value", [".nan", ".inf", "-.inf"])
@pytest.mark.parametrize(
    ("original", "field"),
    [
        ("  frame_interval_seconds: 30", "frame_interval_seconds"),
        ("  stable_frame_interval_seconds: 60", "stable_frame_interval_seconds"),
        ("  debug_overlay_interval_seconds: 60", "debug_overlay_interval_seconds"),
        ("  escalation_verification_seconds: 600", "escalation_verification_seconds"),
        ("  timeout_seconds: 10", "timeout_seconds"),
        ("  retry_backoff_seconds: 1", "retry_backoff_seconds"),
    ],
)
def test_configured_timing_intervals_must_be_finite(
    tmp_path: Path,
    original: str,
    field: str,
    bad_value: str,
) -> None:
    indentation = original[: len(original) - len(original.lstrip())]
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        original,
        f"{indentation}{field}: {bad_value}",
    )
    if field == "frame_interval_seconds":
        config = config.replace(
            "  stable_frame_interval_seconds: 60",
            f"  stable_frame_interval_seconds: {bad_value}",
        )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert field in str(exc_info.value)


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
        ("detection", "inference_image_size", "0"),
        ("detection", "spot_crop_margin_px", "-1"),
        ("detection", "open_suppression_min_confidence", "-0.1"),
        ("occupancy", "iou_threshold", "-0.1"),
        ("occupancy", "confirm_frames", "0"),
    ],
)
def test_invalid_thresholds_and_counters_are_rejected(
    tmp_path: Path, section: str, field: str, bad_value: str
) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    path = write_config(
        tmp_path,
        base.replace(f"{field}: 0.35", f"{field}: {bad_value}")
        .replace(f"{field}: 1280", f"{field}: {bad_value}")
        .replace(f"{field}: 1200", f"{field}: {bad_value}")
        .replace(f"{field}: 0.2", f"{field}: {bad_value}")
        .replace(f"{field}: 0.1", f"{field}: {bad_value}")
        .replace(f"{field}: 48", f"{field}: {bad_value}")
        .replace(f"{field}: 3", f"{field}: {bad_value}"),
    )

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
