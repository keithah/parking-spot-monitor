from __future__ import annotations

from pathlib import Path
import re

import yaml


ROOT = Path(__file__).resolve().parents[1]


def read_tracked(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def read_yaml(path: str) -> object:
    return yaml.safe_load(read_tracked(path))


def assert_contains_all(text: str, required: list[str]) -> None:
    missing = [token for token in required if token not in text]
    assert not missing, f"missing documented operator tokens: {missing}"


def read_readme_section(heading: str) -> str:
    readme = read_tracked("README.md")
    pattern = re.compile(rf"^## {re.escape(heading)}\s*$", re.MULTILINE)
    match = pattern.search(readme)
    assert match is not None, f"README.md missing section heading: ## {heading}"
    next_heading = re.search(r"^## ", readme[match.end() :], re.MULTILINE)
    section_end = match.end() + next_heading.start() if next_heading else len(readme)
    return readme[match.start() : section_end]


def assert_section_case(section: str, case_name: str, required: list[str]) -> None:
    missing = [token for token in required if token not in section]
    assert not missing, f"README.md troubleshooting case '{case_name}' missing tokens: {missing}"


def test_readme_documents_clean_machine_setup_sequence_and_operator_commands() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "cp config.yaml.example config.yaml",
            "RTSP_URL",
            "MATRIX_ACCESS_TOKEN",
            "python -m parking_spot_monitor --config config.yaml --validate-config",
            "docker build -t parking-spot-monitor:test .",
            "docker compose config --no-interpolate",
            "docker compose up parking-spot-monitor",
            "docker compose logs -f parking-spot-monitor",
            "docker compose restart parking-spot-monitor",
            "docker compose down",
            "!parking who",
            "!parking owner <spot_id>",
            "!parking wrong <spot_id|session_id>",
            "!parking profile summary <profile_id>",
            "matrix.command_authorized_senders",
        ],
    )

    sequence = [
        "cp config.yaml.example config.yaml",
        "python -m parking_spot_monitor --config config.yaml --validate-config",
        "docker build -t parking-spot-monitor:test .",
        "docker compose config --no-interpolate",
        "docker compose up parking-spot-monitor",
        "docker compose logs -f parking-spot-monitor",
        "docker compose restart parking-spot-monitor",
        "docker compose down",
    ]
    positions = [readme.index(token) for token in sequence]
    assert positions == sorted(positions)


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


def test_first_check_artifact_guidance_and_structured_events_are_documented() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "/data/latest.jpg",
            "./data/latest.jpg",
            "data/health.json",
            "python -m json.tool data/health.json",
            "find data/snapshots",
        ],
    )
    assert "startup-ready" in readme or "capture-frame-written" in readme


def test_readme_troubleshooting_covers_s04_failure_classes_with_evidence_surfaces() -> None:
    section = read_readme_section("Troubleshooting and cleanup runbook")

    required_cases = {
        "RTSP/capture failures or reconnect symptoms": [
            "RTSP/capture failures",
            "stream.reconnect_seconds",
            "docker compose logs -f parking-spot-monitor",
            "data/latest.jpg",
            "data/health.json",
            "capture-frame-written",
            "capture-all-modes-failed",
        ],
        "hardware decode/device passthrough issues": [
            "hardware decode",
            "/dev/dri:/dev/dri",
            "docker compose ps",
            "data/health.json",
            "selected_decode_mode",
        ],
        "Matrix send/upload failures": [
            "Matrix send/upload failures",
            "docker compose logs -f parking-spot-monitor",
            "data/health.json",
            "last_matrix_error",
            "matrix-send-failed",
            "matrix-delivery-failed",
        ],
        "detector misses/false negatives": [
            "detector misses",
            "false negatives",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "detection-frame-processed",
            "detection-frame-failed",
        ],
        "false positives/passing traffic": [
            "false positives",
            "passing traffic",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "data/state.json",
            "detection-frame-processed",
        ],
        "street-sweeping or quiet-window behavior": [
            "street-sweeping",
            "quiet-window",
            "data/state.json",
            "quiet-window-started",
            "quiet-window-ended",
            "occupancy-open-suppressed",
        ],
        "restart/state corruption recovery": [
            "restart/state corruption recovery",
            "docker compose restart parking-spot-monitor",
            "data/state.json",
            "quarantined",
            "state-corrupt-quarantined",
        ],
        "permissions/disk write failures": [
            "permissions/disk write failures",
            "./data:/data",
            "data/health.json",
            "health-write-failed",
            "state-save-failed",
            "debug-overlay-failed",
        ],
        "snapshot/disk cleanup": [
            "snapshot/disk cleanup",
            "data/snapshots/",
            "storage.snapshot_retention_count",
            "snapshot-retention-pruned",
            "snapshot-retention-failed",
        ],
    }

    for case_name, required in required_cases.items():
        assert_section_case(section, case_name, required)


def test_readme_non_goals_are_explicit_and_distinguished_from_local_docs_validation() -> None:
    section = read_readme_section("Non-goals and deferred capabilities")

    assert_contains_all(
        section,
        [
            "no supported web UI",
            "NVR/video archive",
            "license-plate recognition",
            "cloud AI dependency",
            "encrypted Matrix-room hardening guarantee",
            "driveway-car monitoring",
            "live-camera proof",
            "live Matrix delivery guarantee",
            "local docs alone",
        ],
    )


def test_s04_docs_contract_stays_grounded_in_tracked_source_events() -> None:
    readme = read_tracked("README.md")
    tracked_sources = "\n".join(
        read_tracked(path)
        for path in [
            "docker-compose.yml",
            "config.yaml.example",
            "parking_spot_monitor/__main__.py",
            "parking_spot_monitor/capture.py",
            "parking_spot_monitor/matrix.py",
            "parking_spot_monitor/state.py",
            "parking_spot_monitor/health.py",
            "parking_spot_monitor/debug_overlay.py",
            "parking_spot_monitor/occupancy.py",
        ]
    )

    source_backed_tokens = [
        "startup-ready",
        "capture-frame-written",
        "capture-decode-fallback",
        "capture-all-modes-failed",
        "debug-overlay-written",
        "debug-overlay-failed",
        "detection-frame-processed",
        "matrix-send-failed",
        "matrix-delivery-failed",
        "state-corrupt-quarantined",
        "state-save-failed",
        "health-write-failed",
        "snapshot-retention-pruned",
        "snapshot-retention-failed",
        "quiet-window-started",
        "quiet-window-ended",
        "occupancy-open-suppressed",
        "/dev/dri:/dev/dri",
        "snapshot_retention_count",
    ]
    for token in source_backed_tokens:
        assert token in tracked_sources, f"tracked source no longer backs documented token: {token}"
        assert token in readme, f"README.md missing source-backed operator token: {token}"


def test_docs_and_wiring_remain_secret_safe_and_do_not_embed_raw_artifact_spam() -> None:
    scanned_paths = [
        "README.md",
        "Dockerfile",
        "docker-compose.yml",
        "config.yaml.example",
        "tests/test_operator_docs.py",
    ]
    rendered = "\n".join(read_tracked(path) for path in scanned_paths)

    forbidden_live_value_markers = [
        "rt" "sp://",
        "camera-" "secret",
        "matrix-" "secret",
        "should-not-" "leak",
        "mxc" "://",
        "Authorization" ": " "Bearer",
        "Bear" "er " "syt_",
        "Trace" "back (most recent call last)",
        "BEGIN RAW " "IMAGE BYTES",
        "END RAW " "IMAGE BYTES",
        ("raw " "image bytes").upper(),
    ]
    for marker in forbidden_live_value_markers:
        assert marker not in rendered

    forbidden_live_value_patterns = {
        "concrete RTSP URL": r"rt" r"sp://[^\s)>'\"]+",
        "Matrix access token": r"(?:syt|spa|map)_[-A-Za-z0-9._=]{20,}",
        "Authorization/Bear" "er example": r"Authorization\s*:\s*Bearer\s+\S+",
        "Matrix content URI": r"mxc" r"://[^\s)>'\"]+",
        "private Matrix room id": r"![A-Za-z0-9_-]{20,}:[A-Za-z0-9.-]+",
        "traceback spam": r"Traceback \(most recent call last\)",
    }
    for marker_class, pattern in forbidden_live_value_patterns.items():
        assert re.search(pattern, rendered) is None, f"forbidden {marker_class} marker found in docs/config/wiring"


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
        ("stream", "reconnect_seconds"),
        ("spots", "left_spot", "polygon"),
        ("spots", "right_spot", "polygon"),
        ("detection", "confidence_threshold"),
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
    ]
    for path in required_fields:
        value = config
        for key in path:
            assert isinstance(value, dict), f"{'.'.join(path)} parent is not a mapping"
            assert key in value, f"missing operator config field: {'.'.join(path)}"
            value = value[key]

    assert config["stream"]["rtsp_url_env"] == "RTSP_URL"
    assert config["matrix"]["access_token_env"] == "MATRIX_ACCESS_TOKEN"


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


def test_documented_artifact_paths_and_debug_events_stay_wired_to_tracked_code() -> None:
    combined_sources = "\n".join(
        read_tracked(path)
        for path in [
            "parking_spot_monitor/paths.py",
            "parking_spot_monitor/capture.py",
            "parking_spot_monitor/debug_overlay.py",
            "parking_spot_monitor/__main__.py",
            "parking_spot_monitor/matrix.py",
        ]
    )

    assert_contains_all(
        combined_sources,
        [
            "latest.jpg",
            "debug_latest.jpg",
            "snapshots",
            "capture-frame-written",
            "debug-overlay-written",
            "debug-overlay-failed",
            "detection-frame-processed",
            "detection-frame-failed",
        ],
    )


def test_readme_calibration_artifact_and_safety_contract_is_grounded() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "config.yaml.example",
            "config.yaml",
            "RTSP_URL",
            "MATRIX_ACCESS_TOKEN",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "data/snapshots/",
            "data/health.json",
            "data/state.json",
            "debug-overlay-written",
            "capture-frame-written",
            "detection-frame-processed",
            "detection-frame-failed",
            "image payload bytes",
            "raw frames, snapshots, health/state, and redacted runtime logs local",
            "does not prove a live camera or Matrix room",
            "per-spot threshold schema",
        ],
    )

    unsupported_claim_markers = [
        "visual calibration UI",
        "validated live camera",
        "validated live Matrix",
    ]
    for marker in unsupported_claim_markers:
        assert marker not in readme
