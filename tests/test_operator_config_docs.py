from __future__ import annotations

from tests.operator_docs_helpers import assert_contains_all, read_tracked, read_yaml


def test_readme_and_compose_agree_on_service_mount_command_and_device_contract() -> None:
    readme = read_tracked("README.md")
    compose_text = read_tracked("docker-compose.yml")
    compose = read_yaml("docker-compose.yml")
    service = compose["services"]["parking-spot-monitor"]

    assert "env_file" not in service
    assert "env_file" not in compose_text
    assert service["command"] == [
        "python",
        "-m",
        "parking_spot_monitor",
        "--config",
        "/config/config.yaml",
        "--data-dir",
        "/data",
    ]
    assert "./config.yaml:/config/config.yaml:ro" in service["volumes"]
    assert "./data:/data" in service["volumes"]
    assert service["devices"] == ["/dev/dri:/dev/dri"]

    assert_contains_all(
        readme,
        [
            "parking-spot-monitor",
            "/config/config.yaml",
            "/data",
            "./config.yaml:/config/config.yaml:ro",
            "./data:/data",
            "--data-dir",
            "/dev/dri:/dev/dri",
            "No `env_file` contract in `docker-compose.yml`",
        ],
    )

def test_example_config_uses_environment_secret_names_not_live_values() -> None:
    config = read_yaml("config.yaml.example")

    assert config["stream"]["rtsp_url_env"] == "RTSP_URL"
    assert config["matrix"]["access_token_env"] == "MATRIX_ACCESS_TOKEN"
    assert "rtsp_url" not in config["stream"]
    assert "access_token" not in config["matrix"]

def test_example_config_exposes_operator_calibration_and_runtime_fields() -> None:
    config = read_yaml("config.yaml.example")

    required_groups = [
        "stream",
        "spots",
        "detection",
        "occupancy",
        "matrix",
        "quiet_windows",
        "storage",
        "runtime",
    ]
    for group in required_groups:
        assert group in config, f"missing operator config group: {group}"

    required_fields = [
        ("stream", "rtsp_url_env"),
        ("stream", "frame_width"),
        ("stream", "frame_height"),
        ("stream", "capture_timeout_seconds"),
        ("stream", "reconnect_seconds"),
        ("stream", "escalation_profile"),
        ("stream", "escalation_min_confidence"),
        ("stream", "escalation_verification_seconds"),
        ("stream", "profiles"),
        ("spots", "left_spot", "polygon"),
        ("spots", "right_spot", "polygon"),
        ("detection", "confidence_threshold"),
        ("detection", "inference_image_size"),
        ("detection", "spot_crop_inference"),
        ("detection", "spot_crop_margin_px"),
        ("detection", "open_suppression_min_confidence"),
        ("detection", "open_suppression_classes"),
        ("detection", "min_bbox_area_px"),
        ("detection", "min_polygon_overlap_ratio"),
        ("detection", "vehicle_classes"),
        ("occupancy", "iou_threshold"),
        ("occupancy", "confirm_frames"),
        ("occupancy", "release_frames"),
        ("matrix", "homeserver"),
        ("matrix", "room_id"),
        ("matrix", "access_token_env"),
        ("storage", "data_dir"),
        ("storage", "snapshots_dir"),
        ("storage", "snapshot_retention_count"),
        ("runtime", "health_file"),
        ("runtime", "frame_interval_seconds"),
        ("runtime", "occupied_frame_interval_seconds"),
        ("runtime", "adaptive_polling_enabled"),
        ("runtime", "stable_frame_interval_seconds"),
        ("runtime", "stable_settle_frames"),
        ("runtime", "debug_overlay_interval_seconds"),
    ]
    for path in required_fields:
        value = config
        for key in path:
            assert isinstance(value, dict), f"{'.'.join(path)} parent is not a mapping"
            assert key in value, f"missing operator config field: {'.'.join(path)}"
            value = value[key]

    assert config["stream"]["rtsp_url_env"] == "RTSP_URL"
    assert config["stream"]["profiles"]["high_resolution"]["rtsp_url_env"] == "RTSP_URL_4K"
    assert config["stream"]["capture_timeout_seconds"] == 15
    assert config["stream"]["escalation_verification_seconds"] == 600
    assert config["matrix"]["access_token_env"] == "MATRIX_ACCESS_TOKEN"
    assert config["runtime"]["frame_interval_seconds"] == 30
    assert config["runtime"]["occupied_frame_interval_seconds"] == 30
    assert config["runtime"]["adaptive_polling_enabled"] is True
    assert config["runtime"]["stable_frame_interval_seconds"] == 60
    assert config["runtime"]["stable_settle_frames"] == 3
    assert config["runtime"]["debug_overlay_interval_seconds"] == 60


def test_spec_documents_adaptive_runtime_rollbacks_and_disabled_periodic_work() -> None:
    spec = read_tracked("parking-spot-monitor-spec.md")

    assert_contains_all(
        spec,
        [
            "adaptive_polling_enabled: false",
            "stable_frame_interval_seconds",
            "frame_interval_seconds",
            "fixed cadence",
            "debug_overlay_interval_seconds: 0",
            "disables periodic debug overlays",
            "escalation_verification_seconds: 0",
            "disables periodic high-resolution verification",
            "transition-driven escalation remains enabled",
        ],
    )

def test_example_spot_polygons_are_in_frame_and_have_minimum_shape() -> None:
    config = read_yaml("config.yaml.example")
    width = config["stream"]["frame_width"]
    height = config["stream"]["frame_height"]

    for spot_id in ["left_spot", "right_spot"]:
        polygon = config["spots"][spot_id]["polygon"]
        assert len(polygon) >= 3, f"{spot_id} needs at least three polygon points"
        for point in polygon:
            assert isinstance(point, list), f"{spot_id} polygon point must be a YAML [x, y] list"
            assert len(point) == 2, f"{spot_id} polygon point must contain x and y"
            x, y = point
            assert 0 <= x <= width, f"{spot_id} x coordinate out of frame: {x}"
            assert 0 <= y <= height, f"{spot_id} y coordinate out of frame: {y}"
