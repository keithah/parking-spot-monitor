from __future__ import annotations

from pathlib import Path

import yaml


SECRET_LIKE_STRINGS = [
    "rtsp://",
    "camera-secret",
    "matrix-secret",
    "should-not-leak",
]

FORBIDDEN_SPAM_SENTINELS = [
    "Traceback (most recent call last)",
    "BEGIN RAW IMAGE BYTES",
    "END RAW IMAGE BYTES",
]


def test_example_config_uses_mount_relative_runtime_paths() -> None:
    config = yaml.safe_load(Path("config.yaml.example").read_text(encoding="utf-8"))

    assert config["storage"]["data_dir"] == "./data"
    assert config["storage"]["snapshots_dir"] == "snapshots"
    assert config["runtime"]["health_file"] == "health.json"


def test_readme_documents_mount_relative_runtime_path_contract() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "snapshots_dir: snapshots" in readme
    assert "health_file: health.json" in readme
    assert "relative to the effective `--data-dir`" in readme
    assert "/data/snapshots" in readme
    assert "/data/health.json" in readme
    assert "./data/snapshots" in readme
    assert "./data/health.json" in readme


def test_dockerfile_installs_runtime_and_defaults_to_package_entrypoint() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    requirements = Path("requirements.txt").read_text(encoding="utf-8")

    assert "FROM python:3.12-slim" in dockerfile or "FROM python:3.11-slim" in dockerfile
    assert "ffmpeg" in dockerfile
    assert "COPY requirements.txt ./" in dockerfile
    assert "pip install --no-cache-dir -r requirements.txt" in dockerfile
    assert "ultralytics>=8" in requirements
    assert "COPY parking_spot_monitor ./parking_spot_monitor" in dockerfile
    assert 'CMD ["python", "-m", "parking_spot_monitor", "--config", "/config/config.yaml"]' in dockerfile


def test_compose_contract_mounts_config_data_and_uses_capture_runtime() -> None:
    compose_text = Path("docker-compose.yml").read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)
    service = compose["services"]["parking-spot-monitor"]

    assert "./config.yaml:/config/config.yaml:ro" in service["volumes"]
    assert "./data:/data" in service["volumes"]
    assert service["environment"] == ["RTSP_URL", "MATRIX_ACCESS_TOKEN"]
    assert service["command"] == [
        "python",
        "-m",
        "parking_spot_monitor",
        "--config",
        "/config/config.yaml",
        "--data-dir",
        "/data",
    ]
    assert "--validate-config" not in service["command"]
    assert "devices" not in service
    assert "# devices:" in compose_text
    assert "#   - /dev/dri:/dev/dri" in compose_text
    assert "#   - ./models:/models:ro" in compose_text


def test_readme_documents_final_operator_verification_contract() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    for required in [
        "Model storage policy",
        "detection.model` accepts local model names",
        "rejects URL-like values",
        "/models/yolov8n.pt",
        "First-run Ultralytics downloads are allowed",
        "can block startup",
        "./models:/models:ro",
        "M001 keeps the container running as root",
        "non-root container hardening",
        "python -m parking_spot_monitor --config config.yaml --validate-config",
        "python -m parking_spot_monitor --config config.yaml --data-dir ./data --capture-once",
        "python scripts/verify_live_proof.py",
        "python -m json.tool data/health.json",
        "find data/snapshots",
        "docker build -t parking-spot-monitor:test .",
        "docker compose config",
        "R015 evidence",
    ]:
        assert required in readme


def test_readme_pins_health_shape_retention_and_live_proof_markers() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    for required in [
        "status",
        "iteration",
        "last_frame_at",
        "selected_decode_mode",
        "consecutive_capture_failures",
        "consecutive_detection_failures",
        "last_matrix_error",
        "retention_failure_count",
        "state_save_error",
        "last_error",
        "snapshot_retention_count: 50",
        "LIVE_PROOF_SKIPPED_CONFIG_ABSENT",
        "LIVE_RTSP_CAPTURE_OK",
        "LIVE_MATRIX_TEXT_OK",
        "LIVE_MATRIX_IMAGE_OK",
        "LIVE_RTSP_CAPTURE_FAILED",
        "LIVE_MATRIX_TEXT_FAILED",
        "LIVE_MATRIX_IMAGE_FAILED",
    ]:
        assert required in readme


def test_readme_documents_finite_validation_and_capture_smoke_commands() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "python -m parking_spot_monitor --config config.yaml --data-dir ./data --capture-once" in readme
    assert "docker compose run --rm parking-spot-monitor" in readme
    assert "--capture-once" in readme
    assert "finite capture proof" in readme
    assert "/data/latest.jpg" in readme
    assert "./data/latest.jpg" in readme
    assert "latest.jpg` is the raw full-frame camera evidence" in readme
    assert "Keep it unannotated" in readme
    assert "structured" in readme
    assert "fallback" in readme


def test_readme_documents_local_yolo_detection_and_deferred_live_tuning() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "## Local YOLO detection" in readme
    assert "ultralytics>=8" in readme
    assert "YOLO nano" in readme
    assert "detection-frame-processed" in readme
    assert "detection-frame-failed" in readme
    assert "accepted candidate summaries" in readme
    assert "rejection reason counts" in readme
    assert "Unit tests use fake YOLO result objects" in readme
    assert "normal test runs do not download weights or run real inference" in readme
    assert "Live camera accuracy proof" in readme
    assert "detection.model allowlisting" in readme
    assert "non-root container hardening" in readme
    assert "deferred to S07" in readme


def test_readme_documents_runtime_occupancy_state_and_schedule_events() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "/data/state.json" in readme
    assert "street_sweeping" in readme
    assert "first and third Monday" in readme
    assert "13:00" in readme and "15:00" in readme
    for event_name in [
        "occupancy-state-changed",
        "occupancy-open-event",
        "occupancy-open-suppressed",
        "quiet-window-started",
        "quiet-window-ended",
        "state-loaded",
        "state-saved",
        "state-corrupt-quarantined",
    ]:
        assert event_name in readme
    assert "S06" in readme
    assert "Matrix messages from these S05 event objects" in readme


def test_docker_contract_docs_and_compose_do_not_embed_secret_values() -> None:
    rendered = "\n".join(
        Path(path).read_text(encoding="utf-8") for path in ["Dockerfile", "docker-compose.yml", "README.md", ".gitignore"]
    )

    assert "config.yaml" in Path(".gitignore").read_text(encoding="utf-8")
    assert "RTSP_URL" in rendered
    assert "MATRIX_ACCESS_TOKEN" in rendered
    for secret_like in SECRET_LIKE_STRINGS:
        assert secret_like not in rendered
    for sentinel in FORBIDDEN_SPAM_SENTINELS:
        assert sentinel not in rendered
