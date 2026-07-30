from __future__ import annotations

import json
import os
import warnings
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from PIL import Image

import parking_spot_monitor.operator_cockpit_snapshots as operator_cockpit_snapshots
from parking_spot_monitor.config import load_settings
from parking_spot_monitor.detector_adapter import adapt_detector
from parking_spot_monitor.health import HealthStatus, write_health_status
from parking_spot_monitor.image_budget import ImageBudgetError, JpegBudgetResult
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


def test_who_resize_uses_shared_budget_encoder_and_preserves_validation_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    seen: dict[str, object] = {}

    def fake_encoder(image: Image.Image, **kwargs: object) -> JpegBudgetResult:
        seen["image_mode"] = image.mode
        seen.update(kwargs)
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 5)

    monkeypatch.setattr(operator_cockpit_snapshots, "encode_jpeg_under_budget", fake_encoder, raising=False)
    result = operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=destination,
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert result.state == "available"
    assert result.path == destination
    assert result.info == {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360}
    assert result.freshness == "fresh"
    assert result.age == "0s ago"
    assert destination.read_bytes() == b"jpeg"
    assert seen == {
        "image_mode": "RGB",
        "max_bytes": operator_cockpit_snapshots.MAX_WHO_MATRIX_IMAGE_BYTES,
        "initial_max_dimension": operator_cockpit_snapshots.WHO_MATRIX_INITIAL_MAX_DIMENSION,
        "min_dimension": operator_cockpit_snapshots.WHO_MATRIX_MIN_DIMENSION,
        "dimension_scale": 0.85,
        "qualities": operator_cockpit_snapshots.WHO_MATRIX_JPEG_QUALITIES,
        "resampling": Image.Resampling.LANCZOS,
    }


def test_who_resize_publishes_temp_sibling_with_atomic_replace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    source_bytes = _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    destination.write_bytes(b"previous-valid-jpeg")
    real_replace = os.replace
    replacements: list[tuple[Path, Path, bytes, bytes]] = []

    def tracking_replace(source_path: str | os.PathLike[str], destination_path: str | os.PathLike[str]) -> None:
        temporary = Path(source_path)
        target = Path(destination_path)
        replacements.append((temporary, target, temporary.read_bytes(), target.read_bytes()))
        real_replace(temporary, target)

    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"new-jpeg", 640, 360, 65, 2),
        raising=False,
    )
    monkeypatch.setattr(operator_cockpit_snapshots.os, "replace", tracking_replace, raising=False)

    result = operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=destination,
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert result.path == destination
    assert destination.read_bytes() == b"new-jpeg"
    assert source.read_bytes() == source_bytes
    assert len(replacements) == 1
    temporary, target, staged_bytes, prior_bytes = replacements[0]
    assert temporary.parent == destination.parent
    assert temporary.name.startswith(f".{destination.name}.")
    assert target == destination
    assert staged_bytes == b"new-jpeg"
    assert prior_bytes == b"previous-valid-jpeg"
    assert not temporary.exists()


@pytest.mark.parametrize("failure", ["encode", "write", "fsync", "replace", "stat"])
def test_who_resize_publish_failure_preserves_previous_destination_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    old_bytes = b"previous-valid-jpeg"
    destination.write_bytes(old_bytes)
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"replacement-jpeg", 640, 360, 65, 2),
        raising=False,
    )

    if failure == "encode":
        monkeypatch.setattr(
            operator_cockpit_snapshots,
            "encode_jpeg_under_budget",
            lambda image, **kwargs: (_ for _ in ()).throw(ImageBudgetError("encode failed")),
        )
    elif failure == "write":
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "write",
            lambda fd, data: (_ for _ in ()).throw(OSError("write failed")),
            raising=False,
        )
    elif failure == "fsync":
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "fsync",
            lambda fd: (_ for _ in ()).throw(OSError("fsync failed")),
            raising=False,
        )
    elif failure == "replace":
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "replace",
            lambda source_path, destination_path: (_ for _ in ()).throw(OSError("replace failed")),
            raising=False,
        )
    else:
        real_stat = Path.stat

        def fail_temp_stat(path: Path, *args: object, **kwargs: object) -> os.stat_result:
            if path.name.startswith(f".{destination.name}."):
                raise OSError("stat failed")
            return real_stat(path, *args, **kwargs)

        monkeypatch.setattr(Path, "stat", fail_temp_stat)

    expected_error = ImageBudgetError if failure == "encode" else OSError
    with pytest.raises(expected_error, match=failure):
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=destination,
            now=datetime.now(timezone.utc),
            logger=None,
        )

    assert destination.read_bytes() == old_bytes
    assert list(tmp_path.glob(f".{destination.name}.*")) == []


def test_who_resize_drafts_before_load_reuses_rgb_source_and_closes_it(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[object] = []

    class TrackingImage:
        size = (1280, 720)
        mode = "RGB"

        def draft(self, mode: str, size: tuple[int, int]) -> None:
            events.append(("draft", mode, size))

        def load(self) -> None:
            events.append("load")

        def copy(self) -> Image.Image:
            raise AssertionError("RGB operator source must not be copied")

        def convert(self, mode: str) -> Image.Image:
            raise AssertionError(f"RGB operator source must not be converted to {mode}")

        def close(self) -> None:
            events.append("source-close")

    source_image = TrackingImage()
    monkeypatch.setattr(operator_cockpit_snapshots.Image, "open", lambda path: source_image)

    def fake_encoder(image: object, **kwargs: object) -> JpegBudgetResult:
        assert image is source_image
        events.append("encode")
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 3)

    monkeypatch.setattr(operator_cockpit_snapshots, "encode_jpeg_under_budget", fake_encoder, raising=False)
    operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        tmp_path / "latest.jpg",
        destination=tmp_path / "who_latest.jpg",
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert events == [("draft", "RGB", (960, 540)), "load", "encode", "source-close"]


def test_who_resize_converts_non_rgb_once_and_closes_both_images(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[object] = []

    class ConvertedImage:
        size = (1280, 720)
        mode = "RGB"

        def close(self) -> None:
            events.append("converted-close")

    converted = ConvertedImage()

    class SourceImage:
        size = (1280, 720)
        mode = "CMYK"

        def draft(self, mode: str, size: tuple[int, int]) -> None:
            events.append(("draft", mode, size))

        def load(self) -> None:
            events.append("load")

        def convert(self, mode: str) -> ConvertedImage:
            events.append(("convert", mode))
            return converted

        def close(self) -> None:
            events.append("source-close")

    monkeypatch.setattr(operator_cockpit_snapshots.Image, "open", lambda path: SourceImage())

    def fake_encoder(image: object, **kwargs: object) -> JpegBudgetResult:
        assert image is converted
        events.append("encode")
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 3)

    monkeypatch.setattr(operator_cockpit_snapshots, "encode_jpeg_under_budget", fake_encoder, raising=False)
    operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        tmp_path / "latest.jpg",
        destination=tmp_path / "who_latest.jpg",
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert events == [
        ("draft", "RGB", (960, 540)),
        "load",
        ("convert", "RGB"),
        "encode",
        "converted-close",
        "source-close",
    ]


def test_who_resize_logs_quality_and_attempts_without_expanding_media_info(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 7),
        raising=False,
    )
    stream = StringIO()

    result = operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=destination,
        now=datetime.now(timezone.utc),
        logger=StructuredLogger(stream=stream),
    )

    assert result.info == {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360}
    assert json.loads(stream.getvalue()) == {
        "event": "operator-who-snapshot-resized",
        "level": "INFO",
        "source_path": str(source),
        "destination_path": str(destination),
        "source_width": 1280,
        "source_height": 720,
        "output_width": 640,
        "output_height": 360,
        "byte_size": 4,
        "quality": 65,
        "attempts": 7,
    }


@pytest.mark.parametrize(
    "operational_failure",
    [
        ImageBudgetError("access_token=budget-secret"),
        OSError("Authorization: Bearer io-secret"),
        operator_cockpit_snapshots.UnidentifiedImageError("secret=unidentified"),
        Image.DecompressionBombError("password=bomb-secret"),
        Image.DecompressionBombWarning("token=warning-secret"),
    ],
)
def test_who_resize_expected_operational_failure_returns_safe_unavailable_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operational_failure: BaseException,
) -> None:
    source = tmp_path / "latest.jpg"
    source.write_bytes(b"oversized")
    stream = StringIO()
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "_validate_latest_snapshot",
        lambda path, **kwargs: operator_cockpit_snapshots.LatestSnapshotValidation(state="error", error_type="too large"),
    )
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "_resize_who_snapshot_for_matrix",
        lambda *args, **kwargs: (_ for _ in ()).throw(operational_failure),
    )

    result = operator_cockpit_snapshots._prepare_who_snapshot_for_matrix(
        source,
        data_dir=tmp_path,
        now=datetime.now(timezone.utc),
        logger=StructuredLogger(stream=stream),
    )

    assert result == operator_cockpit_snapshots.LatestSnapshotValidation(state="error", error_type="resize failed")
    rendered = stream.getvalue()
    assert "operator-who-snapshot-unavailable" in rendered
    assert "resize_failed" in rendered
    assert "secret" not in rendered


@pytest.mark.parametrize(
    "unexpected",
    [AssertionError("encoder invariant failed"), MemoryError("allocation invariant failed")],
)
def test_who_snapshot_availability_boundary_does_not_hide_unexpected_resize_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    unexpected: BaseException,
) -> None:
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "_prepare_who_snapshot_for_matrix",
        lambda *args, **kwargs: (_ for _ in ()).throw(unexpected),
    )

    with pytest.raises(type(unexpected)) as exc_info:
        operator_cockpit_snapshots.build_who_snapshot_response(
            settings=object(),
            data_dir=tmp_path,
            base_text="Parking monitor who",
            capture_func=lambda *args, **kwargs: SimpleNamespace(
                latest_path=tmp_path / "latest.jpg",
                timestamp="2026-05-16T17:42:39Z",
            ),
        )

    assert exc_info.value is unexpected


@pytest.mark.parametrize(
    ("capture_result", "expected_reason"),
    [
        (OSError("Authorization: Bearer capture-secret"), "OSError"),
        (SimpleNamespace(timestamp="2026-05-16T17:42:39Z"), "AttributeError"),
    ],
)
def test_who_snapshot_unexpected_capture_or_result_failure_keeps_safe_text_fallback(
    tmp_path: Path,
    capture_result: object,
    expected_reason: str,
) -> None:
    stream = StringIO()

    def capture_func(*args: object, **kwargs: object) -> object:
        if isinstance(capture_result, BaseException):
            raise capture_result
        return capture_result

    response = operator_cockpit_snapshots.build_who_snapshot_response(
        settings=object(),
        data_dir=tmp_path,
        base_text="Parking monitor who\n- left_spot: occupied",
        capture_func=capture_func,
        logger=StructuredLogger(stream=stream),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert f"Snapshot: fresh capture unavailable ({expected_reason}); no live state was changed." in response.text
    assert "left_spot: occupied" in response.text
    rendered = response.text + stream.getvalue()
    assert "capture-secret" not in rendered
    assert "operator-who-snapshot-unavailable" in rendered


def test_who_resize_promotes_real_pillow_decompression_warning_at_availability_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(12, 12))
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 100)
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "_validate_latest_snapshot",
        lambda path, **kwargs: operator_cockpit_snapshots.LatestSnapshotValidation(state="error", error_type="too large"),
    )
    stream = StringIO()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = operator_cockpit_snapshots._prepare_who_snapshot_for_matrix(
            source,
            data_dir=tmp_path,
            now=datetime.now(timezone.utc),
            logger=StructuredLogger(stream=stream),
        )

    assert result == operator_cockpit_snapshots.LatestSnapshotValidation(state="error", error_type="resize failed")
    assert caught == []
    rendered = stream.getvalue()
    assert "resize_failed" in rendered
    assert "DecompressionBombWarning" in rendered


def test_who_resize_logs_temp_cleanup_failure_without_masking_primary_publish_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    old_bytes = b"previous-valid-jpeg"
    destination.write_bytes(old_bytes)
    primary = OSError("fsync primary failure")
    real_unlink = Path.unlink
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"replacement-jpeg", 640, 360, 65, 2),
    )
    monkeypatch.setattr(
        operator_cockpit_snapshots.os,
        "fsync",
        lambda fd: (_ for _ in ()).throw(primary),
    )

    def fail_temp_unlink(path: Path, *args: object, **kwargs: object) -> None:
        if path.name.startswith(f".{destination.name}."):
            raise OSError("token=cleanup-secret")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_temp_unlink)
    stream = StringIO()

    with pytest.raises(OSError) as exc_info:
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=destination,
            now=datetime.now(timezone.utc),
            logger=StructuredLogger(stream=stream),
        )

    assert exc_info.value is primary
    assert destination.read_bytes() == old_bytes
    temporary_files = list(tmp_path.glob(f".{destination.name}.*"))
    assert len(temporary_files) == 1
    rendered = stream.getvalue()
    assert "temp_cleanup_failed" in rendered
    assert "OSError" in rendered
    assert "cleanup-secret" not in rendered
    real_unlink(temporary_files[0])


def test_who_resize_preserves_existing_destination_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 2),
    )
    existing = tmp_path / "who_latest.jpg"
    existing.write_bytes(b"old")
    existing.chmod(0o640)

    operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=existing,
        now=datetime.now(timezone.utc),
        logger=None,
    )
    assert existing.stat().st_mode & 0o777 == 0o640


@pytest.mark.parametrize(("process_umask", "expected_mode"), [(0o077, 0o600), (0o022, 0o644)])
def test_who_resize_new_destination_mode_matches_path_write_bytes_umask_semantics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    process_umask: int,
    expected_mode: int,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 2),
    )
    new_destination = tmp_path / "new" / "who_latest.jpg"
    previous_umask = os.umask(process_umask)
    try:
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=new_destination,
            now=datetime.now(timezone.utc),
            logger=None,
        )
    finally:
        os.umask(previous_umask)

    assert new_destination.stat().st_mode & 0o777 == expected_mode


@pytest.mark.parametrize("failure", ["destination-stat", "fchmod"])
def test_who_resize_mode_preparation_failure_preserves_old_destination_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    old_bytes = b"previous-valid-jpeg"
    destination.write_bytes(old_bytes)
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"replacement-jpeg", 640, 360, 65, 2),
    )
    if failure == "destination-stat":
        real_stat = Path.stat

        def fail_destination_stat(path: Path, *args: object, **kwargs: object) -> os.stat_result:
            if path == destination:
                raise OSError("destination-stat failed")
            return real_stat(path, *args, **kwargs)

        monkeypatch.setattr(Path, "stat", fail_destination_stat)
    else:
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "fchmod",
            lambda fd, mode: (_ for _ in ()).throw(OSError("fchmod failed")),
            raising=False,
        )

    with pytest.raises(OSError, match=failure):
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=destination,
            now=datetime.now(timezone.utc),
            logger=None,
        )

    assert destination.read_bytes() == old_bytes
    assert list(tmp_path.glob(f".{destination.name}.*")) == []


def test_who_resize_retries_partial_writes_until_complete(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    payload = b"partial-write-jpeg"
    real_write = os.write
    write_sizes: list[int] = []
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(payload, 640, 360, 65, 2),
    )

    def partial_write(file_descriptor: int, data: object) -> int:
        chunk = bytes(data)[:3]
        write_sizes.append(len(chunk))
        return real_write(file_descriptor, chunk)

    monkeypatch.setattr(operator_cockpit_snapshots.os, "write", partial_write)

    operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=destination,
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert destination.read_bytes() == payload
    assert write_sizes == [3, 3, 3, 3, 3, 3]


@pytest.mark.parametrize("write_failure", ["zero-progress", "interrupted"])
def test_who_resize_zero_progress_or_interrupted_write_preserves_old_destination_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    write_failure: str,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    old_bytes = b"previous-valid-jpeg"
    destination.write_bytes(old_bytes)
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"replacement-jpeg", 640, 360, 65, 2),
    )
    if write_failure == "zero-progress":
        monkeypatch.setattr(operator_cockpit_snapshots.os, "write", lambda fd, data: 0)
        expected = "no progress"
    else:
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "write",
            lambda fd, data: (_ for _ in ()).throw(InterruptedError("interrupted")),
        )
        expected = "interrupted"

    with pytest.raises(OSError, match=expected):
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=destination,
            now=datetime.now(timezone.utc),
            logger=None,
        )

    assert destination.read_bytes() == old_bytes
    assert list(tmp_path.glob(f".{destination.name}.*")) == []


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




def test_operator_cockpit_confidence_summary_reports_artifact_derived_spot_signals_and_delivery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_confidence_reply
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    settings = _settings(tmp_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(
        json.dumps(
            {
                "status": "degraded",
                "updated_at": "2026-05-18T19:00:00Z",
                "last_matrix_error": {
                    "error_type": "timeout",
                    "diagnostic": FAKE_MATRIX_TOKEN,
                    "path": str(tmp_path),
                    "nested": {"secret": NESTED_SECRET_MARKER},
                },
            }
        ),
        encoding="utf-8",
    )
    original_state = json.dumps(
        {
            "schema_version": 1,
            "spots": {
                "left_spot": {"status": "occupied", "hit_streak": 4, "miss_streak": 0, "open_event_emitted": False},
                "right_spot": {"status": "empty", "hit_streak": 0, "miss_streak": 5, "open_event_emitted": True},
            },
        },
        sort_keys=True,
    )
    state_path.write_text(original_state, encoding="utf-8")
    original_state_mtime_ns = state_path.stat().st_mtime_ns
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    (frames_dir / "20260518T185800Z.jpg").write_bytes((RAW_IMAGE_MARKER * 20).encode("utf-8"))
    (frames_dir / "20260518T185900Z.jpg").write_bytes((RAW_IMAGE_MARKER * 20).encode("utf-8"))
    (frames_dir / "not-a-timestamp.jpg").write_bytes((RAW_IMAGE_MARKER * 20).encode("utf-8"))
    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "confidence_dip",
            observed_at="2026-05-18T18:59:10Z",
            spot_id="left_spot",
            summary="confidence dipped below stable threshold",
            details={"token": FAKE_MATRIX_TOKEN, "raw_image": RAW_IMAGE_MARKER},
        ),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record("suppression", observed_at="2026-05-18T18:59:20Z", spot_id="right_spot", summary="quiet-window suppression active"),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record("command_outcome", observed_at="2026-05-18T18:59:30Z", summary="Matrix status command delivered"),
    )

    def fail_side_effect(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("confidence summary must only read local artifacts")

    monkeypatch.setattr("parking_spot_monitor.capture.capture_latest", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.state.save_runtime_state", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_side_effect)
    monkeypatch.setattr("PIL.Image.open", fail_side_effect)

    reply = format_operator_confidence_reply(
        settings=settings,
        data_dir=tmp_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    assert "Parking confidence report" in reply
    assert "conservative and artifact-derived" in reply
    assert "not a calibrated model score" in reply
    assert "left_spot: stable occupied" in reply
    assert "hit streak 4/4" in reply
    assert "right_spot: stable open" in reply
    assert "miss streak 5/5" in reply
    assert "confidence_dip: confidence dipped below stable threshold" in reply
    assert "suppression: quiet-window suppression active" in reply
    assert "retained timestamped frames 2" in reply
    assert "newest 1m ago" in reply
    assert "filename scan only; image bytes were not opened; ignored 1" in reply
    assert "last Matrix error: timeout" in reply
    assert "recent delivery memory: command_outcome: Matrix status command delivered" in reply
    assert "Read-only: no detector, camera, media upload, alert emission, or state mutation was run." in reply
    assert state_path.read_text(encoding="utf-8") == original_state
    assert state_path.stat().st_mtime_ns == original_state_mtime_ns
    assert len(reply.encode("utf-8")) <= 4096
    assert str(tmp_path) not in reply
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_confidence_summary_degrades_for_missing_and_malformed_artifacts(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_confidence_reply
    from parking_spot_monitor.operator_decision_memory import decision_memory_path

    settings = _settings(tmp_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text("not json " + FAKE_MATRIX_TOKEN + " " + str(tmp_path), encoding="utf-8")
    state_path.write_text(json.dumps({"schema_version": 1, "spots": [], "secret": FAKE_RTSP_URL}), encoding="utf-8")
    memory_path = decision_memory_path(tmp_path)
    memory_path.write_text("not json " + RAW_IMAGE_MARKER + " " + NESTED_SECRET_MARKER, encoding="utf-8")

    reply = format_operator_confidence_reply(
        settings=settings,
        data_dir=tmp_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    assert "Parking confidence report" in reply
    assert "left_spot: unavailable" in reply
    assert "right_spot: unavailable" in reply
    assert "State artifacts: unavailable (schema_error); configured spot fallbacks shown." in reply
    assert "decision memory unavailable (JSONDecodeError)" in reply
    assert "Timeline health:" in reply
    assert "unavailable (missing timeline frames directory)" in reply
    assert "health unavailable (JSONDecodeError); Matrix error status unknown" in reply
    assert "delivery memory unavailable" in reply
    assert "Traceback" not in reply
    assert str(tmp_path) not in reply
    assert len(reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(reply)

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




def test_operator_cockpit_incident_review_attaches_nearest_timeline_frame_and_memory(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import build_incident_review_response
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    far = frames_dir / "20260518T021000Z.jpg"
    near = frames_dir / "20260518T023900Z.jpg"
    _write_test_jpeg(far, size=(16, 9))
    noisy = Image.effect_noise((1280, 720), 80).convert("RGB")
    noisy.save(near, format="JPEG", quality=95)
    assert near.stat().st_size > 300_000
    assert append_decision_memory_record(
        decision_memory_path(tmp_path),
        make_decision_memory_record(
            "miss",
            observed_at="2026-05-18T02:39:15Z",
            spot_id="left_spot",
            summary="runtime state stayed occupied despite operator report",
        ),
    )

    response = build_incident_review_response(
        data_dir=tmp_path,
        spot_id="left_spot",
        time_text="7:39pm",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert response.image_path is not None
    assert response.image_path.name == "incident_left_spot.jpg"
    assert response.image_path.stat().st_size <= 300_000
    assert response.image_info is not None
    assert response.image_info["size"] == response.image_path.stat().st_size
    assert response.image_info["w"] < 1280
    assert response.image_info["h"] < 720
    assert "Incident review: left_spot around 2026-05-17 7:39 PM PDT" in response.text
    assert "Nearest retained frame: 2026-05-17 7:39 PM PDT" in response.text
    assert "runtime state stayed occupied" in response.text
    assert "No detector, camera, Matrix send, or state mutation was run." in response.text
    _assert_no_sensitive_text(response.text)


def test_operator_cockpit_incident_review_handles_missing_timeline_safely(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import build_incident_review_response

    response = build_incident_review_response(
        data_dir=tmp_path,
        spot_id="left_spot",
        time_text="7:39pm",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "Incident review: left_spot" in response.text
    assert "Nearest retained frame: unavailable" in response.text
    assert "No retained timeline frames were found." in response.text



def _write_incident_runtime_state(path: Path, *, corrupt: bool = False) -> str:
    if corrupt:
        payload = "not json " + FAKE_MATRIX_TOKEN + " " + RAW_IMAGE_MARKER
        path.write_text(payload, encoding="utf-8")
        return payload
    payload = {
        "schema_version": 1,
        "spots": {
            "left_spot": {
                "status": "occupied",
                "hit_streak": 4,
                "miss_streak": 0,
                "open_event_emitted": False,
                "last_bbox": [30, 40, 260, 320],
            },
            "right_spot": {
                "status": "empty",
                "hit_streak": 0,
                "miss_streak": 5,
                "open_event_emitted": True,
            },
        },
    }
    rendered = json.dumps(payload, sort_keys=True)
    path.write_text(rendered, encoding="utf-8")
    return rendered


def _write_incident_timeline_frame(tmp_path: Path, name: str = "20260518T023900Z.jpg", *, corrupt: bool = False) -> Path:
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    frame = frames_dir / name
    if corrupt:
        frame.write_bytes(b"not a jpeg " + RAW_IMAGE_MARKER.encode("utf-8"))
    else:
        _write_test_jpeg(frame, size=(1458, 806))
    return frame


class _IncidentReplayDetector:
    def __init__(self, detections: list[Any] | Exception) -> None:
        self.detections = detections
        self.calls: list[dict[str, Any]] = []

    def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None, inference_image_size: int | None = None) -> list[Any]:
        self.calls.append({"frame_path": Path(frame_path), "confidence_threshold": confidence_threshold, "inference_image_size": inference_image_size})
        if isinstance(self.detections, Exception):
            raise self.detections
        return self.detections


def test_operator_cockpit_incident_review_contract_replays_detector_and_simulates_state_without_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from parking_spot_monitor.detection import VehicleDetection
    from parking_spot_monitor.operator_cockpit import build_incident_review_response
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    settings = _settings(tmp_path)
    frame = _write_incident_timeline_frame(tmp_path)
    state_path = tmp_path / "state.json"
    original_state = _write_incident_runtime_state(state_path)
    original_state_mtime_ns = state_path.stat().st_mtime_ns
    detector = _IncidentReplayDetector([VehicleDetection(class_name="car", confidence=0.91, bbox=(25, 30, 275, 325))])
    assert append_decision_memory_record(
        decision_memory_path(tmp_path),
        make_decision_memory_record(
            "accepted_evidence",
            observed_at="2026-05-18T02:39:10Z",
            spot_id="left_spot",
            summary="left_spot accepted vehicle evidence before operator review",
            details={"matrix_token": FAKE_MATRIX_TOKEN, "raw_image": RAW_IMAGE_MARKER},
        ),
    )

    def fail_side_effect(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("incident review must not capture, write state, alert, or mutate vehicle history")

    monkeypatch.setattr("parking_spot_monitor.capture.capture_latest", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.state.save_runtime_state", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_side_effect)

    response = build_incident_review_response(
        settings=settings,
        data_dir=tmp_path,
        state_path=state_path,
        spot_id="left_spot",
        time_text="7:39pm",
        detector=adapt_detector(detector),
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert detector.calls == [{"frame_path": frame, "confidence_threshold": 0.42, "inference_image_size": 960}]
    assert state_path.read_text(encoding="utf-8") == original_state
    assert state_path.stat().st_mtime_ns == original_state_mtime_ns
    assert response.image_path is not None
    assert response.image_path.name == "incident_left_spot.jpg"
    assert response.image_info is not None
    assert response.image_info["mimetype"] == "image/jpeg"
    assert response.image_info["size"] == response.image_path.stat().st_size
    assert "Detector replay:" in response.text
    assert "left_spot: accepted car confidence 0.91" in response.text
    assert "right_spot: rejected centroid_outside" in response.text
    assert "State simulation:" in response.text
    assert "left_spot: occupied would remain occupied" in response.text
    assert "no live state was changed" in response.text
    assert "left_spot accepted vehicle evidence before operator review" in response.text
    assert len(response.text.encode("utf-8")) <= 4096
    assert str(tmp_path) not in response.text
    assert "Traceback" not in response.text
    _assert_no_sensitive_text(response.text)


@pytest.mark.parametrize(
    "case,corrupt_frame,corrupt_state,detections,expected",
    [
        ("no_evidence", False, False, [], ["Detector replay: no vehicle evidence", "would increment miss streak"]),
        ("corrupt_frame", True, False, [], ["Nearest retained frame: unavailable", "corrupt_frame"]),
        (
            "detector_exception",
            False,
            False,
            RuntimeError("predict failed token=" + FAKE_MATRIX_TOKEN + " raw " + RAW_IMAGE_MARKER),
            ["Detector replay unavailable", "RuntimeError"],
        ),
        ("corrupt_state", False, True, [], ["Runtime state unavailable", "JSONDecodeError", "simulated from unknown/default state"]),
    ],
)
def test_operator_cockpit_incident_review_contract_degrades_safely_for_negative_paths(
    tmp_path: Path,
    case: str,
    corrupt_frame: bool,
    corrupt_state: bool,
    detections: list[Any] | Exception,
    expected: list[str],
) -> None:
    from parking_spot_monitor.operator_cockpit import build_incident_review_response

    settings = _settings(tmp_path)
    _write_incident_timeline_frame(tmp_path, corrupt=corrupt_frame)
    state_path = tmp_path / "state.json"
    original_state = _write_incident_runtime_state(state_path, corrupt=corrupt_state)
    original_state_mtime_ns = state_path.stat().st_mtime_ns
    detector = _IncidentReplayDetector(detections)

    response = build_incident_review_response(
        settings=settings,
        data_dir=tmp_path,
        state_path=state_path,
        spot_id="left_spot",
        time_text="7:39pm",
        detector=adapt_detector(detector),
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert state_path.read_text(encoding="utf-8") == original_state
    assert state_path.stat().st_mtime_ns == original_state_mtime_ns
    assert len(response.text.encode("utf-8")) <= 4096
    assert str(tmp_path) not in response.text
    assert "Traceback" not in response.text
    for snippet in expected:
        assert snippet in response.text, case
    _assert_no_sensitive_text(response.text)

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


def test_build_who_snapshot_response_captures_once_and_attaches_validated_image(tmp_path: Path) -> None:
    from parking_spot_monitor.capture import DecodeMode, FrameCaptureResult, FrameGeometry
    from parking_spot_monitor.operator_cockpit import build_who_snapshot_response

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = len(_write_test_jpeg(latest_path, size=(13, 9)))
    calls: list[tuple[object, Path]] = []

    def capture_func(settings: object, data_dir: Path, **kwargs: object) -> FrameCaptureResult:
        calls.append((settings, Path(data_dir)))
        return FrameCaptureResult(
            timestamp="2026-05-16T17:42:39Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.25,
            byte_size=raw_bytes,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(13, 9)),
        )

    settings = object()
    response = build_who_snapshot_response(
        settings=settings,
        data_dir=tmp_path,
        base_text="Parking monitor who\n- left_spot: occupied — unknown vehicle",
        capture_func=capture_func,
        now=datetime(2026, 5, 16, 17, 42, 40, tzinfo=timezone.utc),
    )

    assert calls == [(settings, tmp_path)]
    assert response.image_path == latest_path
    assert response.image_info == {"mimetype": "image/jpeg", "size": raw_bytes, "w": 13, "h": 9}
    assert response.text.startswith("Parking monitor who\nSnapshot: fresh capture")
    assert "left_spot: occupied" in response.text


def test_build_who_snapshot_response_resizes_oversized_fresh_capture_for_matrix(tmp_path: Path) -> None:
    from parking_spot_monitor.capture import DecodeMode, FrameCaptureResult, FrameGeometry
    from parking_spot_monitor.operator_cockpit import build_who_snapshot_response

    latest_path = tmp_path / "latest.jpg"
    noisy = Image.effect_noise((1280, 720), 80).convert("RGB")
    noisy.save(latest_path, format="JPEG", quality=95)
    raw_size = latest_path.stat().st_size
    assert raw_size > 300_000

    def capture_func(settings: object, data_dir: Path, **kwargs: object) -> FrameCaptureResult:
        return FrameCaptureResult(
            timestamp="2026-05-16T17:42:39Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.25,
            byte_size=raw_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1280, 720)),
        )

    response = build_who_snapshot_response(
        settings=object(),
        data_dir=tmp_path,
        base_text="Active parking sessions:\n- left_spot: occupied — unknown vehicle",
        capture_func=capture_func,
        now=datetime(2026, 5, 16, 17, 42, 40, tzinfo=timezone.utc),
    )

    assert response.image_path is not None
    assert response.image_path != latest_path
    assert response.image_path.name == "who_latest.jpg"
    assert response.image_path.stat().st_size <= 300_000
    assert latest_path.stat().st_size == raw_size
    assert response.image_info is not None
    assert response.image_info["size"] == response.image_path.stat().st_size
    assert response.image_info["w"] < 1280
    assert response.image_info["h"] < 720
    assert "Snapshot: fresh capture" in response.text
    assert "too large" not in response.text



def test_build_who_snapshot_response_falls_back_to_text_on_capture_failure(tmp_path: Path) -> None:
    from parking_spot_monitor.capture import CaptureError, DecodeMode
    from parking_spot_monitor.operator_cockpit import build_who_snapshot_response

    def capture_func(settings: object, data_dir: Path, **kwargs: object) -> object:
        raise CaptureError(
            reason="ffmpeg-timeout",
            mode=DecodeMode.SOFTWARE,
            output_path=tmp_path / "latest.jpg",
            message="capture failed with token syt_secret_matrix_token",
            timeout_seconds=2,
        )

    response = build_who_snapshot_response(
        settings=object(),
        data_dir=tmp_path,
        base_text="Parking monitor who\n- right_spot: open; no active vehicle session",
        capture_func=capture_func,
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "Snapshot: fresh capture unavailable (ffmpeg-timeout); no live state was changed." in response.text
    assert "right_spot: open" in response.text
    assert "syt_secret" not in response.text


ANALYTICS_CONTRACT_XFAIL = pytest.mark.xfail(
    reason="S03 T03 specifies the future operator analytics cockpit formatter contract.",
    strict=True,
)


def _write_vehicle_history_session(
    data_dir: Path,
    *,
    state: str,
    session_id: str,
    spot_id: str,
    started_at: str,
    ended_at: str | None,
    duration_seconds: int | None,
    secret_marker: str | None = None,
) -> Path:
    session_dir = data_dir / "vehicle-history" / "sessions" / state
    session_dir.mkdir(parents=True, exist_ok=True)
    path = session_dir / f"{session_id}.json"
    payload = {
        "schema_version": 1,
        "session_id": session_id,
        "spot_id": spot_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "start_event": {
            "spot_id": spot_id,
            "event_type": "occupied",
            "observed_at": started_at,
            "snapshot_path": f"snapshots/{session_id}.jpg",
            "diagnostic": secret_marker,
        },
        "close_event": None
        if ended_at is None
        else {
            "spot_id": spot_id,
            "event_type": "open",
            "observed_at": ended_at,
            "snapshot_path": f"snapshots/{session_id}-closed.jpg",
            "diagnostic": secret_marker,
        },
        "source_snapshot_path": f"snapshots/{session_id}.jpg",
        "candidate_summary": {"raw_image": RAW_IMAGE_MARKER, "token": secret_marker},
        "occupied_snapshot_path": None,
        "occupied_crop_path": None,
        "profile_id": None,
        "profile_confidence": None,
        "created_at": started_at,
        "updated_at": ended_at or started_at,
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def test_operator_cockpit_analytics_reply_formats_archive_metrics_and_window_labels(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_analytics_reply

    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="left-morning",
        spot_id="left_spot",
        started_at="2026-05-20T08:00:00Z",
        ended_at="2026-05-20T10:00:00Z",
        duration_seconds=7_200,
    )
    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="right-short",
        spot_id="right_spot",
        started_at="2026-05-19T09:00:00Z",
        ended_at="2026-05-19T09:30:00Z",
        duration_seconds=1_800,
    )
    _write_vehicle_history_session(
        tmp_path,
        state="active",
        session_id="left-active",
        spot_id="left_spot",
        started_at="2026-05-20T11:00:00Z",
        ended_at=None,
        duration_seconds=None,
    )

    reply = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="today",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert "Parking occupancy analytics" in reply
    assert "Window: today" in reply
    assert "Range: 2026-05-20T00:00:00Z → 2026-05-20T12:00:00Z" in reply
    assert "Source: local vehicle-history sessions" in reply
    assert "Totals\n- Sessions: 2\n- Closed: 1\n- Active: 1\n- Currently occupied spots: 1" in reply
    assert "Spots\nleft_spot" in reply
    assert "- Sessions: 2" in reply
    assert "- Active: 1" in reply
    assert "- Status: occupied" in reply
    assert "- Occupied: 3h" in reply
    assert "- Average dwell: 2h" in reply
    assert "right_spot" not in reply
    assert "Read-only\nScanned local vehicle-history JSON only. No detector, camera, Matrix media upload, alert emission, or state mutation was run." in reply
    assert reply.count("No detector, camera, Matrix media upload, alert emission, or state mutation was run.") == 1
    assert len(reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_analytics_reply_degrades_for_empty_sparse_and_malformed_archive_data(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_analytics_reply

    empty = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="7d",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert "Parking occupancy analytics" in empty
    assert "Window: 7d" in empty
    assert "No vehicle-history sessions overlap the selected window." in empty
    assert "sparse" in empty.lower() or "limited history" in empty.lower()
    assert len(empty.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(empty)

    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="one-good",
        spot_id="left_spot",
        started_at="2026-05-20T08:00:00Z",
        ended_at="2026-05-20T08:30:00Z",
        duration_seconds=1_800,
        secret_marker=FAKE_MATRIX_TOKEN,
    )
    bad_dir = tmp_path / "vehicle-history" / "sessions" / "closed"
    bad_dir.mkdir(parents=True, exist_ok=True)
    (bad_dir / "malformed.json").write_text("not json " + FAKE_RTSP_URL + " " + RAW_IMAGE_MARKER, encoding="utf-8")

    malformed = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="today",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert "left_spot" in malformed
    assert "Only 1 qualifying vehicle-history session" in malformed
    assert "malformed" in malformed.lower()
    assert "ignored" in malformed.lower() or "quarantined" in malformed.lower()
    assert "No detector, camera, Matrix media upload, alert emission, or state mutation was run." in malformed
    assert len(malformed.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(malformed)


def test_operator_cockpit_analytics_reply_sanitizes_archive_spot_ids(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_analytics_reply

    raw_spot_id = "/var/lib/parking/private/latest.jpg"
    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="path-like-spot",
        spot_id=raw_spot_id,
        started_at="2026-05-20T08:00:00Z",
        ended_at="2026-05-20T08:30:00Z",
        duration_seconds=1_800,
    )

    reply = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="today",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert raw_spot_id not in reply
    assert "/var/lib" not in reply
    assert "unknown_spot" in reply
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_analytics_reply_is_read_only_local_and_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_analytics_reply

    for index in range(80):
        _write_vehicle_history_session(
            tmp_path,
            state="closed",
            session_id=f"left-{index:03d}",
            spot_id="left_spot" if index % 2 == 0 else "right_spot",
            started_at=f"2026-05-20T{index % 12:02d}:00:00Z",
            ended_at=f"2026-05-20T{index % 12:02d}:15:00Z",
            duration_seconds=900,
            secret_marker=FAKE_MATRIX_TOKEN,
        )
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(json.dumps({"status": "ok", "secret": FAKE_MATRIX_TOKEN}), encoding="utf-8")
    state_path.write_text(json.dumps({"spots": {"left_spot": {"status": "open"}}}), encoding="utf-8")
    original_health = health_path.read_text(encoding="utf-8")
    original_state = state_path.read_text(encoding="utf-8")

    def fail_side_effect(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("analytics must use only local vehicle-history archive data")

    monkeypatch.setattr("parking_spot_monitor.capture.capture_latest", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.state.save_runtime_state", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.upload_image", fail_side_effect)
    reply = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="30d",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
        detector=fail_side_effect,
        health_path=health_path,
        state_path=state_path,
    )

    assert "Parking occupancy analytics" in reply
    assert "Window: 30d" in reply
    assert "left_spot" in reply
    assert "right_spot" in reply
    assert "local vehicle-history sessions" in reply
    assert "health" not in reply.lower()
    assert health_path.read_text(encoding="utf-8") == original_health
    assert state_path.read_text(encoding="utf-8") == original_state
    assert len(reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(reply)


def test_matrix_operator_context_routes_parsed_analytics_command_to_cockpit(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixOperatorCockpitContext, parse_matrix_command

    command = parse_matrix_command("!parking analytics 30d")
    assert command.action == "analytics"
    assert command.subject_id == "30d"

    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="left-history",
        spot_id="left_spot",
        started_at="2026-05-19T08:00:00Z",
        ended_at="2026-05-19T09:00:00Z",
        duration_seconds=3_600,
    )
    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)
    context = MatrixOperatorCockpitContext(
        settings=settings,
        data_dir=tmp_path,
        health_path=health_path,
        state_path=state_path,
    )

    response = context.analytics_reply(
        command.subject_id or "7d",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "Parking occupancy analytics" in response.text
    assert "Window: 30d" in response.text
    assert "left-history" not in response.text
    assert "left_spot" in response.text
    assert len(response.text.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(response.text)
