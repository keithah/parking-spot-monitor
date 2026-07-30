from __future__ import annotations

from tests.support._config import *  # noqa: F403


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


def test_matrix_command_config_defaults_to_prefix_and_empty_allowlist_when_omitted(tmp_path: Path) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    config = base.replace('  command_prefix: "!parking"\n', "").replace("  # Empty means inbound correction mutation commands are disabled/reject-all by default.\n  command_authorized_senders: []\n", "")
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.matrix.command_prefix == "!parking"
    assert settings.matrix.command_authorized_senders == []
    matrix_summary = settings.sanitized_summary()["matrix"]
    assert matrix_summary["command_prefix"] == "!parking"
    assert matrix_summary["command_authorized_senders_count"] == 0


def test_legacy_matrix_config_omitting_command_schedule_uses_compatible_defaults(
    tmp_path: Path,
) -> None:
    base = Path("config.yaml.example").read_text(encoding="utf-8")
    config = (
        base.replace("  command_poll_interval_seconds: 60\n", "")
        .replace("  command_failure_cooldown_seconds: 60\n", "")
        .replace("  command_failure_max_cooldown_seconds: 900\n", "")
        .replace("  outbox_retry_interval_seconds: 60\n", "")
        .replace("  outbox_retry_max_seconds: 900\n", "")
        .replace("  retry_jitter_ratio: 0.2\n", "")
        .replace("  unauthorized_reply_cooldown_seconds: 300\n", "")
    )
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.matrix.command_poll_interval_seconds == 60
    assert settings.matrix.command_failure_cooldown_seconds == 60
    assert settings.matrix.command_failure_max_cooldown_seconds == 900
    assert settings.matrix.outbox_retry_interval_seconds == 60
    assert settings.matrix.outbox_retry_max_seconds == 900
    assert settings.matrix.retry_jitter_ratio == 0.2
    assert settings.matrix.unauthorized_reply_cooldown_seconds == 300


def test_matrix_command_schedule_is_configurable_and_summarized(tmp_path: Path) -> None:
    config = (
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("  command_poll_interval_seconds: 60", "  command_poll_interval_seconds: 0")
        .replace(
            "  command_failure_cooldown_seconds: 60",
            "  command_failure_cooldown_seconds: 30",
        )
        .replace(
            "  command_failure_max_cooldown_seconds: 900",
            "  command_failure_max_cooldown_seconds: 120",
        )
    )
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.matrix.command_poll_interval_seconds == 0
    assert settings.matrix.command_failure_cooldown_seconds == 30
    assert settings.matrix.command_failure_max_cooldown_seconds == 120
    summary = settings.sanitized_summary()["matrix"]
    assert summary["command_poll_interval_seconds"] == 0
    assert summary["command_failure_cooldown_seconds"] == 30
    assert summary["command_failure_max_cooldown_seconds"] == 120


def test_matrix_outbox_retry_interval_is_configurable_and_summarized(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  outbox_retry_interval_seconds: 60",
        "  outbox_retry_interval_seconds: 30",
    )
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.matrix.outbox_retry_interval_seconds == 30
    assert settings.sanitized_summary()["matrix"]["outbox_retry_interval_seconds"] == 30


def test_matrix_outbox_retry_maximum_is_configurable_and_summarized(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  outbox_retry_max_seconds: 900",
        "  outbox_retry_max_seconds: 120",
    )
    settings = load_settings(write_config(tmp_path, config), environ=fake_environ())

    assert settings.matrix.outbox_retry_max_seconds == 120
    assert settings.sanitized_summary()["matrix"]["outbox_retry_max_seconds"] == 120


def test_matrix_outbox_retry_maximum_must_cover_initial_interval(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  outbox_retry_max_seconds: 900",
        "  outbox_retry_max_seconds: 30",
    )

    with pytest.raises(ConfigError) as exc_info:
        load_settings(write_config(tmp_path, config), environ=fake_environ())

    assert "outbox_retry_max_seconds" in str(exc_info.value)


@pytest.mark.parametrize("bad_value", ["0", "-1", ".nan", ".inf"])
def test_matrix_outbox_retry_maximum_rejects_nonpositive_or_nonfinite_values(
    tmp_path: Path, bad_value: str
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  outbox_retry_max_seconds: 900",
        f"  outbox_retry_max_seconds: {bad_value}",
    )

    with pytest.raises(ConfigError) as exc_info:
        load_settings(write_config(tmp_path, config), environ=fake_environ())

    assert "outbox_retry_max_seconds" in str(exc_info.value)


@pytest.mark.parametrize("bad_value", ["0", "-1", ".nan", ".inf"])
def test_matrix_outbox_retry_interval_rejects_nonpositive_or_nonfinite_values(
    tmp_path: Path,
    bad_value: str,
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  outbox_retry_interval_seconds: 60",
        f"  outbox_retry_interval_seconds: {bad_value}",
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "outbox_retry_interval_seconds" in str(exc_info.value)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("command_poll_interval_seconds", "-1"),
        ("command_poll_interval_seconds", ".nan"),
        ("command_poll_interval_seconds", ".inf"),
        ("command_failure_cooldown_seconds", "0"),
        ("command_failure_cooldown_seconds", ".nan"),
        ("command_failure_cooldown_seconds", ".inf"),
        ("command_failure_max_cooldown_seconds", "0"),
        ("command_failure_max_cooldown_seconds", ".nan"),
        ("command_failure_max_cooldown_seconds", ".inf"),
    ],
)
def test_matrix_command_schedule_rejects_invalid_or_nonfinite_timings(
    tmp_path: Path,
    field: str,
    bad_value: str,
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        f"  {field}: " + ("900" if field.endswith("max_cooldown_seconds") else "60"),
        f"  {field}: {bad_value}",
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert field in str(exc_info.value)


def test_matrix_command_failure_maximum_cannot_be_below_initial_cooldown(
    tmp_path: Path,
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  command_failure_max_cooldown_seconds: 900",
        "  command_failure_max_cooldown_seconds: 59",
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert "command_failure_max_cooldown_seconds" in str(exc_info.value)


def test_matrix_retry_jitter_and_unauthorized_reply_cooldown_are_configurable_and_summarized(
    tmp_path: Path,
) -> None:
    config = (
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("  retry_jitter_ratio: 0.2", "  retry_jitter_ratio: 0.75")
        .replace(
            "  unauthorized_reply_cooldown_seconds: 300",
            "  unauthorized_reply_cooldown_seconds: 0",
        )
    )
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.matrix.retry_jitter_ratio == 0.75
    assert settings.matrix.unauthorized_reply_cooldown_seconds == 0
    summary = settings.sanitized_summary()["matrix"]
    assert summary["retry_jitter_ratio"] == 0.75
    assert summary["unauthorized_reply_cooldown_seconds"] == 0
    assert FAKE_MATRIX_TOKEN not in repr(summary)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("retry_jitter_ratio", "-0.1"),
        ("retry_jitter_ratio", "1.1"),
        ("retry_jitter_ratio", ".nan"),
        ("retry_jitter_ratio", ".inf"),
        ("unauthorized_reply_cooldown_seconds", "-1"),
        ("unauthorized_reply_cooldown_seconds", ".nan"),
        ("unauthorized_reply_cooldown_seconds", ".inf"),
    ],
)
def test_matrix_retry_and_reply_timings_reject_invalid_or_nonfinite_values(
    tmp_path: Path,
    field: str,
    bad_value: str,
) -> None:
    default_value = "0.2" if field == "retry_jitter_ratio" else "300"
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        f"  {field}: {default_value}",
        f"  {field}: {bad_value}",
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_settings(path, environ=fake_environ())

    assert field in str(exc_info.value)


def test_matrix_command_authorized_senders_are_configurable_without_secret_leaks(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "  command_authorized_senders: []",
        '  command_authorized_senders:\n    - "@operator:example.org"',
    )
    path = write_config(tmp_path, config)

    settings = load_settings(path, environ=fake_environ())

    assert settings.matrix.command_authorized_senders == ["@operator:example.org"]
    assert settings.sanitized_summary()["matrix"]["command_authorized_senders_count"] == 1
    assert FAKE_MATRIX_TOKEN not in repr(settings.sanitized_summary()["matrix"])
