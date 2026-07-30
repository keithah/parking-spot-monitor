from __future__ import annotations

from tests.support._config import *  # noqa: F403


def test_runtime_paths_resolve_relative_values_under_effective_data_dir() -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())

    paths = resolve_runtime_paths(settings, Path("/data"))

    assert paths.data_dir == Path("/data")
    assert paths.state_file == Path("/data/state.json")
    assert paths.latest_frame == Path("/data/latest.jpg")
    assert paths.snapshots_dir == Path("/data/snapshots")
    assert paths.health_file == Path("/data/health.json")
    assert paths.detection_lab_dir == Path("/data/detection-lab")


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
    assert settings.stream.primary_profile.name == "primary"
    assert settings.stream.primary_profile.frame_width == settings.stream.frame_width
    assert settings.stream.primary_profile.frame_height == settings.stream.frame_height
    assert set(settings.stream.profiles) == {"high_resolution"}
    assert settings.matrix.access_token.value == FAKE_MATRIX_TOKEN
    assert settings.matrix.command_poll_interval_seconds == 60
    assert settings.matrix.command_failure_cooldown_seconds == 60
    assert settings.matrix.command_failure_max_cooldown_seconds == 900
    assert settings.matrix.outbox_retry_interval_seconds == 60
    assert settings.matrix.outbox_retry_max_seconds == 900
    assert settings.matrix.unauthorized_reply_cooldown_seconds == 300
    assert settings.matrix.retry_jitter_ratio == 0.2
    assert settings.spots.left_spot.name == "Left spot"
    assert settings.spots.right_spot.name == "Right spot"
    assert settings.detection.inference_image_size == 1280
    assert settings.detection.spot_crop_inference is False
    assert settings.detection.spot_crop_margin_px == 48
    assert settings.detection.open_suppression_min_confidence == 0.1
    assert settings.detection.open_suppression_classes == ["car", "truck", "bus", "suitcase", "umbrella"]
    assert settings.runtime.adaptive_polling_enabled is True
    assert settings.runtime.stable_frame_interval_seconds == 60
    assert settings.runtime.stable_settle_frames == 3
    assert settings.runtime.debug_overlay_interval_seconds == 60
    assert settings.stream.escalation_verification_seconds == 600
    assert settings.stream.reconnect_max_seconds == 60
    assert settings.stream.reconnect_jitter_ratio == 0.2
    assert settings.matrix.command_request_timeout_seconds == 2
    assert settings.matrix.command_retry_attempts == 1
    assert settings.runtime.log_summary_interval_seconds == 900
    assert settings.runtime.decision_memory_checkpoint_interval_seconds == 300
    assert settings.runtime.decision_memory_checkpoint_max_pending_records == 50

    summary = settings.sanitized_summary()
    assert summary["stream"]["reconnect_max_seconds"] == 60
    assert summary["stream"]["reconnect_jitter_ratio"] == 0.2
    assert summary["matrix"]["command_request_timeout_seconds"] == 2
    assert summary["matrix"]["command_retry_attempts"] == 1
    assert summary["runtime"]["log_summary_interval_seconds"] == 900
    assert summary["runtime"]["decision_memory_checkpoint_interval_seconds"] == 300
    assert summary["runtime"]["decision_memory_checkpoint_max_pending_records"] == 50


@pytest.mark.parametrize(
    ("old", "new", "field"),
    [
        ("  reconnect_max_seconds: 60", "  reconnect_max_seconds: 4", "reconnect_max_seconds"),
        ("  reconnect_jitter_ratio: 0.2", "  reconnect_jitter_ratio: 1.1", "reconnect_jitter_ratio"),
        ("  command_request_timeout_seconds: 2", "  command_request_timeout_seconds: 0", "command_request_timeout_seconds"),
        ("  command_retry_attempts: 1", "  command_retry_attempts: 0", "command_retry_attempts"),
        ("  log_summary_interval_seconds: 900", "  log_summary_interval_seconds: 0", "log_summary_interval_seconds"),
        ("  decision_memory_checkpoint_interval_seconds: 300", "  decision_memory_checkpoint_interval_seconds: 0", "decision_memory_checkpoint_interval_seconds"),
        ("  decision_memory_checkpoint_max_pending_records: 50", "  decision_memory_checkpoint_max_pending_records: 0", "decision_memory_checkpoint_max_pending_records"),
    ],
)
def test_bounded_runtime_policy_rejects_invalid_values(
    tmp_path: Path, old: str, new: str, field: str
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(old, new)
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError, match=field):
        load_settings(path, environ=fake_environ())


@pytest.mark.parametrize(
    ("width", "height"),
    [(7681, 100), (100, 7681), (7680, 4321)],
)
def test_stream_geometry_rejects_more_than_8k_resource_budget(
    tmp_path: Path, width: int, height: int
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8")
    config = config.replace("frame_width: 1458", f"frame_width: {width}")
    config = config.replace("frame_height: 806", f"frame_height: {height}")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError, match="stream.*resource ceiling"):
        load_settings(path, environ=fake_environ())


def test_named_stream_profile_geometry_rejects_more_than_8k_resource_budget(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8")
    config = config.replace("frame_width: 3840", "frame_width: 7681")
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError, match="stream.*high_resolution.*resource ceiling"):
        load_settings(path, environ=fake_environ())


def test_stream_profiles_resolve_secret_env_values_and_sanitize_names_only(tmp_path: Path) -> None:
    high_url = f"high-res-camera-{SECRET_MARKER}"
    low_url = f"low-res-camera-{SECRET_MARKER}"
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "      frame_height: 2160\n",
        "\n".join(
            [
                "      frame_height: 2160",
                "    low_resolution:",
                "      rtsp_url_env: RTSP_URL_360P",
                "      frame_width: 640",
                "      frame_height: 360",
                "",
            ]
        ),
    )
    path = write_config(tmp_path, config)

    settings = load_settings(
        path,
        environ=fake_environ(RTSP_URL_4K=high_url, RTSP_URL_360P=low_url),
    )

    assert settings.stream.profile("primary").rtsp_url.value == FAKE_RTSP_URL
    assert settings.stream.profile("high_resolution").rtsp_url.value == high_url
    assert settings.stream.profile("high_resolution").frame_width == 3840
    assert settings.stream.profile("low_resolution").rtsp_url.value == low_url
    summary = settings.sanitized_summary()
    rendered = repr(summary) + settings.model_dump_json()
    assert "RTSP_URL_4K" in rendered
    assert "RTSP_URL_360P" in rendered
    assert high_url not in rendered
    assert low_url not in rendered
    assert SECRET_MARKER not in rendered


def test_stream_profile_missing_env_is_rejected_without_secret_leaks(tmp_path: Path) -> None:
    path = write_config(tmp_path, Path("config.yaml.example").read_text(encoding="utf-8"))

    with pytest.raises(ConfigError) as exc_info:
        load_settings(
            path,
            environ={"RTSP_URL": FAKE_RTSP_URL, "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN},
        )

    message = str(exc_info.value)
    assert "RTSP_URL_4K" in message
    assert FAKE_RTSP_URL not in message
    assert FAKE_MATRIX_TOKEN not in message
    assert SECRET_MARKER not in message


def test_default_config_does_not_require_low_resolution_diagnostic_stream(tmp_path: Path) -> None:
    path = write_config(tmp_path, Path("config.yaml.example").read_text(encoding="utf-8"))

    settings = load_settings(
        path,
        environ={
            "RTSP_URL": FAKE_RTSP_URL,
            "RTSP_URL_4K": f"{FAKE_RTSP_URL}-4k",
            "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN,
        },
    )

    assert set(settings.stream.profiles) == {"high_resolution"}


def test_stream_profile_shape_errors_are_rejected_at_config_boundary(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "    high_resolution:\n      rtsp_url_env: RTSP_URL_4K\n      frame_width: 3840\n      frame_height: 2160\n",
        "    high_resolution: RTSP_URL_4K\n",
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "stream.profiles.high_resolution" in str(exc_info.value)


def test_escalation_profile_must_reference_configured_stream_profile(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  escalation_profile: high_resolution\n",
        "  escalation_profile: missing_profile\n",
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "stream.escalation_profile" in str(exc_info.value)


def test_stream_profile_name_without_filename_safe_characters_is_rejected(tmp_path: Path) -> None:
    profile_name = "!@#$%"
    config = (
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("  escalation_profile: high_resolution\n", f'  escalation_profile: "{profile_name}"\n')
        .replace("    high_resolution:\n", f'    "{profile_name}":\n')
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    message = str(exc_info.value)
    assert "stream" in message
    assert "filename-safe" in message


def test_stream_profile_names_with_same_sanitized_destination_are_rejected(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  escalation_profile: high_resolution\n"
        "  escalation_min_confidence: 0.75\n"
        "  escalation_verification_seconds: 600\n"
        "  profiles:\n"
        "    high_resolution:\n"
        "      rtsp_url_env: RTSP_URL_4K\n"
        "      frame_width: 3840\n"
        "      frame_height: 2160\n",
        '  escalation_profile: "high resolution"\n'
        "  escalation_min_confidence: 0.75\n"
        "  escalation_verification_seconds: 600\n"
        "  profiles:\n"
        '    "high resolution":\n'
        "      rtsp_url_env: RTSP_URL_4K\n"
        "      frame_width: 3840\n"
        "      frame_height: 2160\n"
        '    "high/resolution":\n'
        "      rtsp_url_env: RTSP_URL_360P\n"
        "      frame_width: 640\n"
        "      frame_height: 360\n",
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    message = str(exc_info.value)
    assert "stream" in message
    assert "high resolution" in message
    assert "high/resolution" in message
    assert "same capture filename" in message
