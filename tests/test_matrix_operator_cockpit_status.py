from __future__ import annotations

from tests.support._matrix_operator_cockpit import *  # noqa: F403


def test_status_reply_contract_includes_health_loop_spots_and_freshness_without_secrets(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import format_operator_status_reply

    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)
    log_stream = StringIO()

    reply = format_operator_status_reply(
        settings=settings,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, 20, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    assert "Parking monitor status" in reply
    assert "Health: degraded" in reply
    assert "updated 20s ago" in reply
    assert "Loop: iteration 42" in reply
    assert "last frame 30s ago" in reply
    assert "frame interval 12.5s" in reply
    assert "decode mode software" in reply
    assert "left_spot" in reply and "open" in reply
    assert "right_spot" in reply and "open" in reply
    assert "capture failures 1" in reply
    assert "detection failures 2" in reply
    _assert_no_sensitive_text(reply + log_stream.getvalue())


def test_config_reply_contract_includes_safe_runtime_settings_without_secrets(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import format_operator_config_reply

    settings = _settings(tmp_path)
    log_stream = StringIO()

    reply = format_operator_config_reply(
        settings=settings,
        data_dir=tmp_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    assert "Parking monitor config" in reply
    assert "model models/yolo11n.pt" in reply
    assert "confidence threshold 0.42" in reply
    assert "crop enabled" in reply
    assert "crop margin 32px" in reply
    assert "retention 12 snapshots" in reply
    assert "quiet window street_sweeping" in reply
    assert "13:00-15:00 America/Los_Angeles" in reply
    assert "left_spot: Left curb spot" in reply
    assert "right_spot: Right curb spot" in reply
    assert "frame 1458x806" in reply
    assert "authorized senders 1" in reply
    assert "token configured" in reply
    _assert_no_sensitive_text(reply + log_stream.getvalue())


@pytest.mark.parametrize(
    "health_payload,state_payload,expected",
    [
        (None, None, ["Health: unavailable", "State: unavailable"]),
        ("not json", None, ["Health: unavailable", "State: unavailable"]),
        ({"status": "ok", "updated_at": "2026-05-18T17:00:00Z", "iteration": 1}, None, ["stale", "2h ago"]),
        (None, "not json", ["State: unavailable", "left_spot", "right_spot"]),
    ],
)
def test_status_reply_handles_missing_corrupt_and_stale_runtime_files_safely(
    tmp_path: Path,
    health_payload: Any,
    state_payload: Any,
    expected: list[str],
) -> None:
    from parking_spot_monitor.matrix import format_operator_status_reply

    settings = _settings(tmp_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    if isinstance(health_payload, dict):
        health_path.write_text(json.dumps(health_payload), encoding="utf-8")
    elif isinstance(health_payload, str):
        health_path.write_text(health_payload + FAKE_RTSP_URL + NESTED_SECRET_MARKER, encoding="utf-8")
    if isinstance(state_payload, dict):
        state_path.write_text(json.dumps(state_payload), encoding="utf-8")
    elif isinstance(state_payload, str):
        state_path.write_text(state_payload + FAKE_MATRIX_TOKEN + RAW_IMAGE_MARKER, encoding="utf-8")

    log_stream = StringIO()
    reply = format_operator_status_reply(
        settings=settings,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    for snippet in expected:
        assert snippet in reply
    _assert_no_sensitive_text(reply + log_stream.getvalue())


def test_status_and_config_replies_have_bounded_size_and_do_not_start_camera_model_or_network_work(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.matrix import format_operator_config_reply, format_operator_status_reply

    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)

    def fail_network_or_model_work(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("status/config must only read local config/runtime files")

    monkeypatch.setattr("httpx.Client.request", fail_network_or_model_work)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_network_or_model_work)

    status = format_operator_status_reply(
        settings=settings,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, 20, tzinfo=timezone.utc),
    )
    config = format_operator_config_reply(settings=settings, data_dir=tmp_path, now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc))

    assert len(status.encode("utf-8")) <= 4096
    assert len(config.encode("utf-8")) <= 4096


def test_operator_cockpit_status_summarizes_state_markers_without_large_state_dump(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_status_reply

    settings = _settings(tmp_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(
        json.dumps({"status": "ok", "updated_at": "2026-05-18T19:00:00Z", "iteration": 7}),
        encoding="utf-8",
    )
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "spots": {
                    "left_spot": {
                        "status": "occupied",
                        "hit_streak": 11,
                        "miss_streak": 0,
                        "open_event_emitted": True,
                        "large_debug_blob": RAW_IMAGE_MARKER * 100,
                    },
                    "right_spot": {"status": "empty", "hit_streak": 0, "miss_streak": 3, "open_event_emitted": False},
                },
                "active_quiet_window_ids": ["street_sweeping"],
                "quiet_window_notice_ids": ["notice-a", "notice-b"],
                "owner_quiet_window_alert_ids": ["owner-a"],
            }
        ),
        encoding="utf-8",
    )

    reply = format_operator_status_reply(
        settings=settings,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, 20, tzinfo=timezone.utc),
    )

    assert "left_spot: occupied; hit streak 11; miss streak 0; open event emitted yes" in reply
    assert "right_spot: open; hit streak 0; miss streak 3; open event emitted no" in reply
    assert "Quiet windows: active 1; notices 2; owner alerts 1" in reply
    assert "large_debug_blob" not in reply
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_config_summary_includes_required_runtime_settings(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_config_reply

    settings = _settings(tmp_path)

    reply = format_operator_config_reply(settings=settings, data_dir=tmp_path)

    assert "inference image size 960" in reply
    assert "open suppression threshold 0.18" in reply
    assert "open suppression classes" in reply
    assert "vehicle classes car, truck" in reply
    assert "iou threshold 0.31" in reply
    assert "confirm frames 4" in reply
    assert "release frames 5" in reply
    assert f"state {tmp_path / 'state.json'}" in reply
    assert f"health {tmp_path / 'health.json'}" in reply
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_negative_cases_are_redacted_and_bounded(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_status_reply

    settings = _settings(tmp_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(json.dumps([FAKE_RTSP_URL, NESTED_SECRET_MARKER]), encoding="utf-8")
    state_path.write_text(json.dumps({"schema_version": 1, "spots": [], "secret": FAKE_MATRIX_TOKEN}), encoding="utf-8")

    reply = format_operator_status_reply(
        settings=settings,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    assert "Health: unavailable (non_object_payload)" in reply
    assert "State: unavailable (schema_error)" in reply
    assert len(reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_invalid_health_timestamp_has_unknown_freshness_not_stale(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_status_reply

    settings = _settings(tmp_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(json.dumps({"status": "ok", "updated_at": "not-a-time", "iteration": 3}), encoding="utf-8")
    save_runtime_state(state_path, RuntimeState.default(["left_spot", "right_spot"]))

    reply = format_operator_status_reply(settings=settings, health_path=health_path, state_path=state_path)

    assert "Health: ok (updated unknown)" in reply
    assert "Health: ok stale" not in reply


def test_operator_cockpit_config_handles_absent_quiet_windows_and_empty_authorized_senders(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_config_reply

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
stream:
  rtsp_url_env: RTSP_URL
  frame_width: 100
  frame_height: 100
spots:
  left_spot:
    name: Left
    polygon: [[0, 0], [50, 0], [50, 50]]
  right_spot:
    name: Right
    polygon: [[50, 50], [90, 50], [90, 90]]
detection:
  model: models/yolo11n.pt
  confidence_threshold: 0.5
  min_bbox_area_px: 10
  min_polygon_overlap_ratio: 0.2
occupancy:
  iou_threshold: 0.3
  confirm_frames: 2
matrix:
  homeserver: https://matrix.example.invalid
  room_id: "!room:example.invalid"
  access_token_env: MATRIX_ACCESS_TOKEN
storage:
  data_dir: data
runtime:
  health_file: health.json
""".lstrip(),
        encoding="utf-8",
    )
    settings = load_settings(config_path, environ={"RTSP_URL": FAKE_RTSP_URL, "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN})

    reply = format_operator_config_reply(settings=settings, data_dir=tmp_path)

    assert "authorized senders 0" in reply
    assert "Quiet windows: none" in reply
    _assert_no_sensitive_text(reply)
