from __future__ import annotations

from tests.support._config import *  # noqa: F403


@pytest.mark.parametrize("model_value", ["yolov8n.pt", "models/custom-detector.pt", "/models/yolov8n.pt"])
def test_detection_model_accepts_local_model_names_and_paths(tmp_path: Path, model_value: str) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "model: /models/yolov8n.pt", f"model: {model_value}"
    )
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.detection.model == model_value


@pytest.mark.parametrize("model_literal", ['""', '"   "', '"\\t"'])
def test_detection_model_rejects_empty_or_whitespace_only_values(
    tmp_path: Path, model_literal: str
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "model: /models/yolov8n.pt", f"model: {model_literal}"
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "detection.model" in str(exc_info.value)


@pytest.mark.parametrize(
    "model_value",
    [
        "https://example.org/yolov8n.pt",
        "http://example.org/yolov8n.pt",
        "s3://bucket/yolov8n.pt",
        "../models/yolov8n.pt",
        "/models/../secret.pt",
        "models/../../secret.pt",
    ],
)
def test_detection_model_rejects_urls_and_path_traversal(tmp_path: Path, model_value: str) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "model: /models/yolov8n.pt", f"model: {model_value}"
    )
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
    assert summary["detection"]["inference_image_size"] == settings.detection.inference_image_size
    assert summary["detection"]["spot_crop_inference"] == settings.detection.spot_crop_inference
    assert summary["detection"]["spot_crop_margin_px"] == settings.detection.spot_crop_margin_px
    assert summary["detection"]["open_suppression_min_confidence"] == settings.detection.open_suppression_min_confidence
    assert summary["detection"]["open_suppression_classes"] == settings.detection.open_suppression_classes
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
            "reminder_minutes_before": 60,
        }
    ]
    assert summary["storage"]["snapshot_retention_count"] == 50


def test_quiet_window_reminder_minutes_before_must_be_positive(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace("reminder_minutes_before: 60", "reminder_minutes_before: 0")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "reminder_minutes_before" in str(exc_info.value)


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
