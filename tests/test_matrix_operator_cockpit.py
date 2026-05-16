from __future__ import annotations

import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from parking_spot_monitor.config import load_settings
from parking_spot_monitor.health import HealthStatus, write_health_status
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.state import RuntimeState, save_runtime_state


FAKE_RTSP_URL = "rtsp://operator:super-secret@camera.example.local/live"
FAKE_MATRIX_TOKEN = "matrix-token-secret-value"
RAW_IMAGE_MARKER = "RAW-JPEG-BYTES-should-never-appear"
NESTED_SECRET_MARKER = "nested-secret-marker-should-never-appear"


def _settings(tmp_path: Path) -> Any:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
stream:
  rtsp_url_env: RTSP_URL
  frame_width: 1458
  frame_height: 806
  reconnect_seconds: 7
spots:
  left_spot:
    name: Left curb spot
    polygon: [[10, 20], [300, 20], [300, 350], [10, 350]]
  right_spot:
    name: Right curb spot
    polygon: [[350, 20], [700, 20], [700, 350], [350, 350]]
detection:
  model: models/yolo11n.pt
  confidence_threshold: 0.42
  inference_image_size: 960
  spot_crop_inference: true
  spot_crop_margin_px: 32
  open_suppression_min_confidence: 0.18
  vehicle_classes: [car, truck]
  min_bbox_area_px: 1200
  min_polygon_overlap_ratio: 0.27
occupancy:
  iou_threshold: 0.31
  confirm_frames: 4
  release_frames: 5
matrix:
  homeserver: https://matrix.example.invalid
  room_id: "!room:example.invalid"
  access_token_env: MATRIX_ACCESS_TOKEN
  user_id: "@bot:example.invalid"
  command_prefix: "!parking"
  command_authorized_senders: ["@operator:example.invalid"]
  timeout_seconds: 3
  retry_attempts: 2
  retry_backoff_seconds: 0.5
quiet_windows:
  - name: street_sweeping
    timezone: America/Los_Angeles
    recurrence: monthly_weekday
    weekdays: [monday]
    ordinals: [1, 3]
    start: "13:00"
    end: "15:00"
    reminder_minutes_before: 60
storage:
  data_dir: data
  snapshots_dir: snapshots
  snapshot_retention_count: 12
runtime:
  health_file: health.json
  log_level: INFO
  startup_timeout_seconds: 45
  frame_interval_seconds: 12.5
""".lstrip(),
        encoding="utf-8",
    )
    return load_settings(
        config_path,
        environ={"RTSP_URL": FAKE_RTSP_URL, "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN},
    )


def _write_runtime_files(tmp_path: Path) -> tuple[Path, Path]:
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    write_health_status(
        health_path,
        HealthStatus(
            status="degraded",
            updated_at="2026-05-18T19:00:00Z",
            iteration=42,
            last_frame_at="2026-05-18T18:59:50Z",
            selected_decode_mode="software",
            consecutive_capture_failures=1,
            consecutive_detection_failures=2,
            last_matrix_error={
                "error_type": "timeout",
                "diagnostic": FAKE_MATRIX_TOKEN,
                "nested": {"leak": NESTED_SECRET_MARKER},
            },
            last_error={"message": "Traceback: " + FAKE_RTSP_URL},
        ),
    )
    save_runtime_state(state_path, RuntimeState.default(["left_spot", "right_spot"]))
    return health_path, state_path


def _assert_no_sensitive_text(rendered: str) -> None:
    assert FAKE_RTSP_URL not in rendered
    assert FAKE_MATRIX_TOKEN not in rendered
    assert RAW_IMAGE_MARKER not in rendered
    assert NESTED_SECRET_MARKER not in rendered
    assert "Traceback" not in rendered
    assert "super-secret" not in rendered


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



def _write_test_jpeg(path: Path, *, size: tuple[int, int] = (16, 9)) -> bytes:
    image = Image.new("RGB", size, color=(12, 34, 56))
    image.save(path, format="JPEG")
    return path.read_bytes()


def test_latest_snapshot_summary_contract_returns_text_and_raw_image_path(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import build_latest_snapshot_response

    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)
    latest_path = tmp_path / "latest.jpg"
    raw_bytes = _write_test_jpeg(latest_path, size=(16, 9))
    log_stream = StringIO()

    response = build_latest_snapshot_response(
        settings=settings,
        latest_path=latest_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, 20, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    assert response.image_path == latest_path
    assert response.image_info == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 16, "h": 9}
    assert "Parking monitor latest" in response.text
    assert "Snapshot: fresh raw latest.jpg" in response.text
    assert "16x9" in response.text
    assert "Health: degraded" in response.text
    assert "last frame 30s ago" in response.text
    assert "detection failures 2" in response.text
    assert "left_spot" in response.text and "right_spot" in response.text
    assert len(response.text.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(response.text + log_stream.getvalue())


@pytest.mark.parametrize(
    "filename,payload,expected",
    [
        ("latest.jpg", b"not a jpeg " + RAW_IMAGE_MARKER.encode("utf-8"), "invalid JPEG"),
        ("debug_latest.jpg", None, "debug overlay"),
        ("latest.jpg", b"0" * 300_001, "too large"),
    ],
)
def test_latest_snapshot_summary_rejects_invalid_debug_and_oversized_images_safely(
    tmp_path: Path,
    filename: str,
    payload: bytes | None,
    expected: str,
) -> None:
    from parking_spot_monitor.operator_cockpit import build_latest_snapshot_response

    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)
    latest_path = tmp_path / filename
    if payload is None:
        _write_test_jpeg(latest_path)
    else:
        latest_path.write_bytes(payload)
    log_stream = StringIO()

    response = build_latest_snapshot_response(
        settings=settings,
        latest_path=latest_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, 20, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "Parking monitor latest unavailable" in response.text
    assert expected in response.text
    _assert_no_sensitive_text(response.text + log_stream.getvalue())


def test_latest_snapshot_summary_handles_missing_stale_and_malformed_runtime_files_safely(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import build_latest_snapshot_response

    settings = _settings(tmp_path)
    latest_path = tmp_path / "latest.jpg"
    _write_test_jpeg(latest_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(
        json.dumps({"status": "ok", "updated_at": "2026-05-18T17:00:00Z", "iteration": 1, "secret": FAKE_RTSP_URL}),
        encoding="utf-8",
    )
    state_path.write_text("not json " + FAKE_MATRIX_TOKEN + RAW_IMAGE_MARKER, encoding="utf-8")
    log_stream = StringIO()

    response = build_latest_snapshot_response(
        settings=settings,
        latest_path=latest_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    assert response.image_path == latest_path
    assert "Health: ok stale" in response.text
    assert "2h ago" in response.text
    assert "State: unavailable" in response.text
    assert "left_spot" in response.text and "right_spot" in response.text
    _assert_no_sensitive_text(response.text + log_stream.getvalue())



def test_operator_cockpit_decision_memory_wrappers_are_bounded_redacted_and_local_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_recent_reply, format_operator_why_reply
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "suppression",
            observed_at="2026-05-18T19:00:00Z",
            spot_id="right_spot",
            summary="quiet-window suppression applied",
            details={"miss_streak": 2, "matrix_token": FAKE_MATRIX_TOKEN, "raw_image": RAW_IMAGE_MARKER},
        ),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record("alert", observed_at="2026-05-18T19:01:00Z", spot_id="right_spot", summary="alert skipped", details={"reason": "quiet_window"}),
    )

    def fail_network_or_model_work(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("why/recent must only read local decision memory")

    monkeypatch.setattr("httpx.Client.request", fail_network_or_model_work)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_network_or_model_work)

    why = format_operator_why_reply(data_dir=tmp_path, spot_id="right_spot")
    recent = format_operator_recent_reply(data_dir=tmp_path)

    assert "Parking decision memory for right_spot" in why
    assert "quiet-window suppression applied" in why
    assert "alert skipped" in recent
    assert len(why.encode("utf-8")) <= 4096
    assert len(recent.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(why + recent)


def test_operator_cockpit_decision_memory_wrappers_handle_corrupt_memory_safely(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_recent_reply, format_operator_why_reply
    from parking_spot_monitor.operator_decision_memory import decision_memory_path

    memory_path = decision_memory_path(tmp_path)
    memory_path.parent.mkdir(parents=True, exist_ok=True)
    memory_path.write_text("not json " + FAKE_RTSP_URL + " " + FAKE_MATRIX_TOKEN, encoding="utf-8")
    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)

    why = format_operator_why_reply(data_dir=tmp_path, spot_id="right_spot", logger=logger)
    recent = format_operator_recent_reply(data_dir=tmp_path, logger=logger)

    rendered = why + recent + log_stream.getvalue()
    assert "Decision memory unavailable" in why
    assert "Decision memory unavailable" in recent
    assert "no detector or camera work was run" in why
    _assert_no_sensitive_text(rendered)



def test_operator_cockpit_detection_lab_wrappers_are_bounded_redacted_and_local_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.detection_lab import DetectionLabManager, REPLAY_CONFIG_FILENAME, REPLAY_LABELS_FILENAME
    from parking_spot_monitor.operator_cockpit import format_detection_lab_run_reply, format_detection_lab_status_reply

    lab_root = tmp_path / "detection-lab"
    lab_root.mkdir()
    (lab_root / REPLAY_LABELS_FILENAME).write_text("{}", encoding="utf-8")
    (lab_root / REPLAY_CONFIG_FILENAME).write_text("{}", encoding="utf-8")

    def fail_network_or_model_work(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("lab wrappers must not run network, camera, or model work")

    monkeypatch.setattr("httpx.Client.request", fail_network_or_model_work)

    def replay_runner(inputs: dict[str, Path]) -> dict[str, Any]:
        report = inputs["job_dir"] / "replay-report.json"
        report.write_text(
            json.dumps(
                {
                    "schema_version": "test.v1",
                    "status_counts": {"passed": 2},
                    "coverage": {"assessed_frames": 2, "blocked_frames": 0, "not_assessed_frames": 0},
                    "redaction_scan": {"passed": True, "findings": []},
                    "rtsp_url": FAKE_RTSP_URL,
                    "matrix_token": FAKE_MATRIX_TOKEN,
                    "raw_image": RAW_IMAGE_MARKER,
                }
            ),
            encoding="utf-8",
        )
        return report

    manager = DetectionLabManager(tmp_path, replay_runner=replay_runner)
    run_reply = format_detection_lab_run_reply(data_dir=tmp_path, kind="replay", manager=manager)

    import time

    deadline = time.monotonic() + 2
    status_reply = format_detection_lab_status_reply(data_dir=tmp_path, job_id="latest", manager=manager)
    while time.monotonic() < deadline and "Status: succeeded" not in status_reply:
        time.sleep(0.01)
        status_reply = format_detection_lab_status_reply(data_dir=tmp_path, job_id="latest", manager=manager)

    rendered = run_reply + status_reply
    assert "Detection lab job started" in run_reply
    assert "Detection lab status" in status_reply
    assert "status counts: passed=2" in status_reply
    assert "coverage: assessed 2" in status_reply
    assert len(run_reply.encode("utf-8")) <= 4096
    assert len(status_reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(rendered)


def test_operator_cockpit_detection_lab_status_failures_are_safe_and_redacted(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_detection_lab_run_reply, format_detection_lab_status_reply

    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)

    unavailable = format_detection_lab_status_reply(data_dir=tmp_path, job_id="latest", logger=logger)
    invalid = format_detection_lab_status_reply(data_dir=tmp_path, job_id="../status.json " + FAKE_RTSP_URL, logger=logger)
    bad_kind = format_detection_lab_run_reply(data_dir=tmp_path, kind="../replay", logger=logger)

    rendered = unavailable + invalid + bad_kind + log_stream.getvalue()
    assert "Detection lab status unavailable" in unavailable
    assert "No detector, camera, shell, or live occupancy work was run" in unavailable
    assert "Detection lab run unavailable" in bad_kind
    assert len(unavailable.encode("utf-8")) <= 4096
    assert len(invalid.encode("utf-8")) <= 4096
    assert len(bad_kind.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(rendered)
