from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
from PIL import Image

from parking_spot_monitor.matrix import (
    MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
    MONITOR_STARTED_EVENT_TYPE,
    OCCUPIED_SPOT_EVENT_TYPE,
    OPEN_SPOT_EVENT_TYPE,
    MatrixClient,
    MatrixDelivery,
    MatrixError,
    format_lifecycle_notice,
    format_live_proof_text,
    format_occupied_spot_alert,
    format_open_spot_alert,
    format_quiet_window_notice,
    format_owner_vehicle_quiet_window_alert,
    monitor_lifecycle_event,
    monitor_lifecycle_event_id,
    owner_vehicle_quiet_window_event_id,
    occupied_spot_event_id,
    prepare_event_snapshot,
    prune_event_snapshots,
)


ACCESS_TOKEN = "secret-token-value"
HOMESERVER = "https://matrix.example.org/"
ROOM_ID = "!parking-room:example.org"
TXN_ID = "txn/with space?"


def make_client(handler: httpx.MockTransport) -> MatrixClient:
    http_client = httpx.Client(transport=handler)
    return MatrixClient(homeserver=HOMESERVER, access_token=ACCESS_TOKEN, timeout_seconds=2, http_client=http_client)


def request_json(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content.decode("utf-8"))


def test_send_text_puts_room_message_with_encoded_segments_and_returns_event_id() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"event_id": "$event:example.org"})

    client = make_client(httpx.MockTransport(handler))

    event_id = client.send_text(room_id=ROOM_ID, txn_id=TXN_ID, body="Parking spot is open")

    assert event_id == "$event:example.org"
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "PUT"
    assert request.url.raw_path.decode("ascii") == "/_matrix/client/v3/rooms/%21parking-room%3Aexample.org/send/m.room.message/txn%2Fwith%20space%3F"
    assert request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
    assert request.headers["content-type"] == "application/json"
    assert request_json(request) == {"msgtype": "m.text", "body": "Parking spot is open"}


def test_upload_image_posts_media_with_filename_query_and_returns_content_uri() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"content_uri": "mxc://example.org/media-id"})

    client = make_client(httpx.MockTransport(handler))

    content_uri = client.upload_image(filename="snapshot 1.jpg", data=b"jpeg-bytes", content_type="image/jpeg")

    assert content_uri == "mxc://example.org/media-id"
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert request.url.path == "/_matrix/media/v3/upload"
    assert request.url.params["filename"] == "snapshot 1.jpg"
    assert request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
    assert request.headers["content-type"] == "image/jpeg"
    assert request.content == b"jpeg-bytes"


def test_send_image_puts_image_room_message_with_info_and_returns_event_id() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"event_id": "$image-event:example.org"})

    client = make_client(httpx.MockTransport(handler))
    info = {"mimetype": "image/jpeg", "size": 1234, "w": 1458, "h": 806}

    event_id = client.send_image(
        room_id=ROOM_ID,
        txn_id="image-txn",
        body="Raw full-frame snapshot",
        content_uri="mxc://example.org/media-id",
        info=info,
    )

    assert event_id == "$image-event:example.org"
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "PUT"
    assert request.url.raw_path.decode("ascii") == "/_matrix/client/v3/rooms/%21parking-room%3Aexample.org/send/m.room.message/image-txn"
    assert request_json(request) == {
        "msgtype": "m.image",
        "body": "Raw full-frame snapshot",
        "url": "mxc://example.org/media-id",
        "info": info,
    }


def test_matrix_delivery_live_proof_sends_labelled_text_and_raw_image(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(source, size=(8, 6))
    seen: list[dict[str, Any]] = []

    class FakeClient:
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            seen.append({"kind": "text", "room_id": room_id, "txn_id": txn_id, "body": body})
            return "$text:example.org"

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            seen.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
            return "mxc://example.org/live-proof"

        def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
            seen.append(
                {
                    "kind": "image",
                    "room_id": room_id,
                    "txn_id": txn_id,
                    "body": body,
                    "content_uri": content_uri,
                    "info": dict(info),
                }
            )
            return "$image:example.org"

    delivery = MatrixDelivery(
        client=FakeClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=None,  # type: ignore[arg-type]
    )

    delivery.send_live_proof(latest_path=source, observed_at="2026-05-18T19:00:00Z", selected_mode="software")

    assert [item["kind"] for item in seen] == ["text", "upload", "image"]
    assert seen[0]["txn_id"] == "live-proof:2026-05-18T19:00:00Z:text"
    assert seen[0]["body"] == "LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at 2026-05-18 12:00:00 PM PDT (decode mode: software)."
    assert seen[1]["content_type"] == "image/jpeg"
    assert seen[1]["data"] == raw_bytes
    assert seen[2]["txn_id"] == "live-proof:2026-05-18T19:00:00Z:image"
    assert seen[2]["body"].startswith("LIVE PROOF / TEST IMAGE: raw full-frame camera snapshot")
    assert seen[2]["info"] == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 8, "h": 6}


def test_format_live_proof_text_is_visibly_labelled() -> None:
    assert format_live_proof_text(observed_at="2026-05-18T19:00:00Z", selected_mode="software") == (
        "LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at 2026-05-18 12:00:00 PM PDT (decode mode: software)."
    )


@pytest.mark.parametrize("status_code", [401, 403, 429, 500])
def test_matrix_error_contains_safe_http_diagnostics_without_token_or_raw_body(status_code: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            json={"errcode": "M_FORBIDDEN", "error": f"denied {ACCESS_TOKEN}"},
            request=request,
        )

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(MatrixError) as exc_info:
        client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")

    error = exc_info.value
    rendered = str(error) + repr(error.diagnostics)
    assert error.diagnostics["status_code"] == status_code
    assert error.diagnostics["errcode"] == "M_FORBIDDEN"
    assert error.diagnostics["error_type"] == "http_status"
    assert ACCESS_TOKEN not in rendered
    assert "denied" not in rendered


def test_matrix_error_reports_timeout_safely() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException(f"timed out with {ACCESS_TOKEN}", request=request)

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(MatrixError) as exc_info:
        client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")

    rendered = str(exc_info.value) + repr(exc_info.value.diagnostics)
    assert exc_info.value.diagnostics["error_type"] == "timeout"
    assert ACCESS_TOKEN not in rendered


@pytest.mark.parametrize(
    "operation,response_json,missing_key",
    [
        ("send_text", {}, "event_id"),
        ("upload_image", {}, "content_uri"),
    ],
)
def test_matrix_error_reports_malformed_responses_without_raw_body(
    operation: str, response_json: dict[str, Any], missing_key: str
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={**response_json, "leak": ACCESS_TOKEN}, request=request)

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(MatrixError) as exc_info:
        if operation == "send_text":
            client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")
        else:
            client.upload_image(filename="snapshot.jpg", data=b"jpeg-bytes", content_type="image/jpeg")

    rendered = str(exc_info.value) + repr(exc_info.value.diagnostics)
    assert exc_info.value.diagnostics["error_type"] == "malformed_response"
    assert exc_info.value.diagnostics["missing_key"] == missing_key
    assert ACCESS_TOKEN not in rendered
    assert "leak" not in rendered


def test_send_text_rejects_empty_body_before_http_request() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("empty text should not be sent to Matrix")

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(ValueError, match="body must be non-empty"):
        client.send_text(room_id=ROOM_ID, txn_id="txn", body="  ")


def test_matrix_config_summary_includes_timeout_retry_and_backoff() -> None:
    from parking_spot_monitor.config import load_settings

    settings = load_settings(
        "config.yaml.example",
        environ={"RTSP_URL": "rtsp://camera", "MATRIX_ACCESS_TOKEN": ACCESS_TOKEN},
    )

    summary = settings.sanitized_summary()["matrix"]

    assert summary["timeout_seconds"] == 10
    assert summary["retry_attempts"] == 3
    assert summary["retry_backoff_seconds"] == 1
    assert ACCESS_TOKEN not in repr(summary)


def write_jpeg(path: Path, *, size: tuple[int, int] = (4, 3)) -> bytes:
    image = Image.new("RGB", size, color=(25, 50, 75))
    image.save(path, format="JPEG")
    return path.read_bytes()


def open_event(snapshot_path: Path | str = "unused.jpg") -> dict[str, Any]:
    return {
        "event_type": OPEN_SPOT_EVENT_TYPE,
        "spot_id": "left_spot",
        "previous_status": "occupied",
        "new_status": "empty",
        "observed_at": datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc),
        "snapshot_path": str(snapshot_path),
    }


def occupied_event(snapshot_path: Path | str = "unused.jpg") -> dict[str, Any]:
    return {
        "event_type": OCCUPIED_SPOT_EVENT_TYPE,
        "spot_id": "left_spot",
        "observed_at": datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc),
        "occupied_snapshot_path": str(snapshot_path),
        "likely_vehicle": {
            "label": "silver hatchback",
            "profile_id": "prof_repeat",
            "match_status": "matched",
            "confidence": 0.92,
        },
        "vehicle_history_estimate": {
            "status": "estimated",
            "profile_id": "prof_repeat",
            "sample_count": 4,
            "confidence": "medium",
            "dwell_range": {"lower_seconds": 3600, "upper_seconds": 5400, "typical_seconds": 4500},
            "leave_time_window": {
                "start_minute": 23 * 60 + 45,
                "end_minute": 15,
                "typical_minute": 0,
                "crosses_midnight": True,
            },
        },
    }


def test_occupied_spot_event_id_uses_event_type_spot_and_normalized_observed_at() -> None:
    event = occupied_event()

    assert occupied_spot_event_id(event) == "occupancy-occupied-event:left_spot:2026-05-18T20:01:02Z"


def test_format_occupied_spot_alert_includes_vehicle_and_estimate_context_without_unsafe_fields(tmp_path: Path) -> None:
    event = occupied_event(tmp_path / "latest.jpg") | {
        "occupied_crop_path": "/tmp/crop.jpg",
        "descriptor": {"histogram": [1, 2, 3]},
        "raw_bytes": b"jpeg",
        "rtsp_url": "rtsp://user:pass@example/camera",
        "ocr_text": "ABC1234",
        "matrix_token": ACCESS_TOKEN,
    }

    body = format_occupied_spot_alert(event)

    assert body == (
        "Parking spot occupied: left_spot at 2026-05-18 1:01:02 PM PDT\n"
        "Likely vehicle: silver hatchback (profile prof_repeat)\n"
        "Match: matched, confidence 0.92\n"
        "Estimated dwell: 1 hr–1 hr 30 min (typical 1 hr 15 min)\n"
        "Usual leave window: 11:45 PM–12:15 AM (typical 12:00 AM; crosses midnight)\n"
        "History: 4 samples, estimate confidence medium"
    )
    rendered = body.lower()
    assert "crop" not in rendered
    assert "descriptor" not in rendered
    assert "histogram" not in rendered
    assert "rtsp" not in rendered
    assert "abc1234" not in rendered
    assert ACCESS_TOKEN not in body


def test_format_occupied_spot_alert_is_honest_about_insufficient_history() -> None:
    event = occupied_event()
    event["likely_vehicle"] = {"profile_id": "prof_repeat", "match_status": "new_profile", "confidence": None}
    event["vehicle_history_estimate"] = {
        "status": "insufficient_history",
        "reason": "insufficient-samples",
        "profile_id": "prof_repeat",
        "sample_count": 1,
        "confidence": "low",
        "dwell_range": None,
        "leave_time_window": None,
    }

    assert format_occupied_spot_alert(event) == "Parking spot occupied: left_spot at 2026-05-18 1:01:02 PM PDT"


def test_format_occupied_spot_alert_omits_unavailable_new_profile_history_noise() -> None:
    event = occupied_event()
    event["likely_vehicle"] = {
        "profile_id": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "match_status": "new_profile",
        "confidence": 1,
    }
    event["vehicle_history_estimate"] = {
        "status": "insufficient_history",
        "reason": "insufficient-samples",
        "profile_id": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "sample_count": 0,
        "confidence": "low",
        "dwell_range": None,
        "leave_time_window": None,
    }

    assert format_occupied_spot_alert(event) == "Parking spot occupied: left_spot at 2026-05-18 1:01:02 PM PDT"


def test_format_occupied_spot_alert_omits_low_confidence_profile_only_estimate_noise() -> None:
    event = occupied_event()
    event["spot_id"] = "right_spot"
    event["observed_at"] = "2026-05-12T17:16:48.322925-07:00"
    event["likely_vehicle"] = {
        "label": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "profile_id": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "match_status": "matched",
        "confidence": 0.82,
    }
    event["vehicle_history_estimate"] = {
        "status": "estimated",
        "profile_id": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "sample_count": 2,
        "confidence": "low",
        "dwell_range": {"lower_seconds": 8700, "upper_seconds": 18600, "typical_seconds": 13500},
        "leave_time_window": {
            "start_minute": 21 * 60 + 15,
            "end_minute": 0,
            "typical_minute": 22 * 60 + 45,
            "crosses_midnight": True,
        },
    }

    assert format_occupied_spot_alert(event) == "Parking spot occupied: right_spot at 2026-05-12 5:16:48 PM PDT"



def test_matrix_delivery_open_alert_uploads_resized_image_without_mutating_retained_raw_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    noisy = Image.effect_noise((1280, 720), 80).convert("RGB")
    noisy.save(source, format="JPEG", quality=95)
    raw_bytes = source.read_bytes()
    assert len(raw_bytes) > 300_000
    seen: list[dict[str, Any]] = []

    class FakeClient:
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            seen.append({"kind": "text", "room_id": room_id, "txn_id": txn_id, "body": body})
            return "$text:example.org"

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            seen.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
            return "mxc://example.org/open"

        def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
            seen.append({
                "kind": "image",
                "room_id": room_id,
                "txn_id": txn_id,
                "body": body,
                "content_uri": content_uri,
                "info": dict(info),
            })
            return "$image:example.org"

    from parking_spot_monitor.logging import StructuredLogger

    delivery = MatrixDelivery(
        client=FakeClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=StructuredLogger(),
    )

    snapshot = delivery.send_open_spot_alert(open_event(source))

    uploads = [item for item in seen if item["kind"] == "upload"]
    images = [item for item in seen if item["kind"] == "image"]
    assert len(uploads) == 1
    assert len(images) == 1
    assert snapshot.path.read_bytes() == raw_bytes
    assert uploads[0]["filename"] == snapshot.filename
    assert uploads[0]["content_type"] == "image/jpeg"
    assert uploads[0]["data"] != raw_bytes
    assert len(uploads[0]["data"]) <= 300_000
    assert images[0]["info"]["size"] == len(uploads[0]["data"])
    assert images[0]["info"]["w"] < 1280
    assert images[0]["info"]["h"] < 720
    assert images[0]["body"].startswith("Raw full-frame snapshot for left_spot")

def test_matrix_delivery_occupied_alert_sends_text_upload_and_raw_occupied_image(tmp_path: Path) -> None:
    source = tmp_path / "occupied.jpg"
    raw_bytes = write_jpeg(source, size=(9, 7))
    seen: list[dict[str, Any]] = []

    class FakeClient:
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            seen.append({"kind": "text", "room_id": room_id, "txn_id": txn_id, "body": body})
            return "$text:example.org"

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            seen.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
            return "mxc://example.org/occupied"

        def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
            seen.append(
                {
                    "kind": "image",
                    "room_id": room_id,
                    "txn_id": txn_id,
                    "body": body,
                    "content_uri": content_uri,
                    "info": dict(info),
                }
            )
            return "$image:example.org"

    delivery = MatrixDelivery(
        client=FakeClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=None,  # type: ignore[arg-type]
    )

    snapshot = delivery.send_occupied_spot_alert(occupied_event(source))

    event_id = "occupancy-occupied-event:left_spot:2026-05-18T20:01:02Z"
    assert [item["kind"] for item in seen] == ["text", "upload", "image"]
    assert seen[0]["txn_id"] == f"{event_id}:text"
    assert "Parking spot occupied: left_spot" in seen[0]["body"]
    assert seen[1]["content_type"] == "image/jpeg"
    assert seen[1]["data"] == raw_bytes
    assert seen[1]["filename"] == "occupancy-occupied-event-left-spot-2026-05-18t20-01-02z.jpg"
    assert snapshot.path == tmp_path / "snapshots" / "occupancy-occupied-event-left-spot-2026-05-18t20-01-02z.jpg"
    assert snapshot.filename == "occupancy-occupied-event-left-spot-2026-05-18t20-01-02z.jpg"
    assert seen[2]["txn_id"] == f"{event_id}:image"
    assert seen[2]["body"] == "Raw occupied full-frame snapshot for left_spot at 2026-05-18 1:01:02 PM PDT"
    assert seen[2]["content_uri"] == "mxc://example.org/occupied"
    assert seen[2]["info"] == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 9, "h": 7}


def test_matrix_delivery_occupied_alert_rejects_invalid_snapshot_source(tmp_path: Path) -> None:
    source = tmp_path / "debug_latest.jpg"
    write_jpeg(source)

    class TextOnlyClient:
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            return "$text:example.org"

    delivery = MatrixDelivery(
        client=TextOnlyClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=None,  # type: ignore[arg-type]
    )

    with pytest.raises(MatrixError) as exc_info:
        delivery.send_occupied_spot_alert(occupied_event(source))

    assert exc_info.value.diagnostics["error_type"] == "snapshot_invalid_source"
    assert exc_info.value.diagnostics["event_type"] == OCCUPIED_SPOT_EVENT_TYPE

def test_prepare_event_snapshot_copies_raw_latest_jpeg_with_metadata_and_stable_alert_payload(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(source, size=(8, 6))
    observed_at = datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc)

    snapshot = prepare_event_snapshot(
        source_path=source,
        data_dir=tmp_path / "data",
        snapshots_dir=tmp_path / "matrix-snapshots",
        event_type="occupancy-open-event",
        event_id="open:left spot/../A?token=secret",
        spot_id="left spot/../A?token=secret",
        observed_at=observed_at,
    )

    assert snapshot.path.parent == tmp_path / "matrix-snapshots"
    assert snapshot.path.read_bytes() == raw_bytes
    assert snapshot.filename == "occupancy-open-event-left-spot-a-token-redacted-2026-05-18t20-01-02z.jpg"
    assert snapshot.txn_id == "snapshot-occupancy-open-event-left-spot-a-token-redacted-2026-05-18t20-01-02z"
    assert snapshot.info == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 8, "h": 6}
    assert snapshot.body == "Raw full-frame snapshot for left spot/../A?token=<redacted> at 2026-05-18T20:01:02+00:00"
    assert snapshot.log_context == {
        "event_type": "occupancy-open-event",
        "event_id": "open:left spot/../A?token=<redacted>",
        "spot_id": "left spot/../A?token=<redacted>",
        "source_path": str(source),
        "snapshot_path": str(snapshot.path),
        "byte_size": len(raw_bytes),
        "mimetype": "image/jpeg",
        "width": 8,
        "height": 6,
    }

    assert format_open_spot_alert(
        {"spot_id": "left_spot", "observed_at": observed_at, "snapshot_path": str(snapshot.path)}
    ) == "Parking spot open: left_spot at 2026-05-18 1:01:02 PM PDT"


def test_format_open_spot_alert_displays_12_hour_string_in_los_angeles_time() -> None:
    assert format_open_spot_alert({"spot_id": "right_spot", "observed_at": "2026-05-12T16:04:08.223073+00:00"}) == (
        "Parking spot open: right_spot at 2026-05-12 9:04:08 AM PDT"
    )


def test_monitor_lifecycle_event_uses_canonical_observed_at_format() -> None:
    observed_at = datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc)
    event = monitor_lifecycle_event(MONITOR_STARTED_EVENT_TYPE, observed_at)

    assert event["observed_at"] == "2026-05-18T18:00:00Z"
    assert event["event_id"] == "parking-monitor-started:2026-05-18T18:00:00Z"


def test_monitor_lifecycle_event_id_includes_signal_when_present() -> None:
    assert (
        monitor_lifecycle_event_id(
            event_type=MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
            observed_at="2026-05-18T18:01:00Z",
            signal="SIGTERM",
        )
        == "parking-monitor-shutdown-requested:SIGTERM:2026-05-18T18:01:00Z"
    )


def test_format_lifecycle_notice_formats_started_and_shutdown_events() -> None:
    assert (
        format_lifecycle_notice(
            {
                "event_type": MONITOR_STARTED_EVENT_TYPE,
                "observed_at": "2026-05-18T18:00:00Z",
            }
        )
        == "Parking monitor started at 2026-05-18 11:00:00 AM PDT."
    )
    assert (
        format_lifecycle_notice(
            {
                "event_type": MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
                "observed_at": "2026-05-18T18:01:00Z",
                "signal": "SIGTERM",
            }
        )
        == "Parking monitor shutdown requested by SIGTERM at 2026-05-18 11:01:00 AM PDT."
    )


def test_format_lifecycle_notice_rejects_unsupported_event_type() -> None:
    with pytest.raises(MatrixError) as exc_info:
        format_lifecycle_notice({"event_type": "parking-monitor-unknown", "observed_at": "2026-05-18T18:00:00Z"})

    assert exc_info.value.diagnostics["error_type"] == "unsupported_lifecycle_event"


def test_prepare_event_snapshot_uses_data_dir_snapshots_fallback_and_sanitizes_ids(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)

    snapshot = prepare_event_snapshot(
        source_path=source,
        data_dir=tmp_path / "data-root",
        snapshots_dir=None,
        event_type="occupancy/open event",
        event_id="spot#1 / event",
        spot_id=None,
        observed_at="2026-05-18T20:01:02Z",
    )

    assert snapshot.path == tmp_path / "data-root" / "snapshots" / "occupancy-open-event-spot-1-event-2026-05-18t20-01-02z.jpg"
    assert "/" not in snapshot.filename
    assert "#" not in snapshot.txn_id
    assert snapshot.info["mimetype"] == "image/jpeg"


def test_prune_event_snapshots_removes_oldest_matching_files_only_and_logs_counts(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.logging import StructuredLogger

    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    oldest = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    middle = snapshot_root / "quiet-window-started-street-sweeping-2026-05-18t21-00-00z.jpg"
    newest = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t22-00-00z.jpg"
    unrelated = snapshot_root / "latest.jpg"
    malformed = snapshot_root / "occupancy-open-event-left-spot-not-a-time.jpg"
    for index, path in enumerate([oldest, middle, newest, unrelated, malformed], start=1):
        path.write_bytes(b"x" * index)
        # Deliberately force mtime ordering to differ from lexical names only slightly.
        path.touch()

    result = prune_event_snapshots(snapshot_root, retention_count=2, logger=StructuredLogger())

    output = capsys.readouterr().err
    assert result.pruned_count == 1
    assert result.pruned_bytes == 1
    assert not oldest.exists()
    assert middle.exists()
    assert newest.exists()
    assert unrelated.exists()
    assert malformed.exists()
    assert '"event":"snapshot-retention-pruned"' in output
    assert '"pruned_count":1' in output
    assert '"retained_count":2' in output


@pytest.mark.parametrize("count", [0, 1, 2])
def test_prune_event_snapshots_keeps_files_when_at_or_under_limit(tmp_path: Path, count: int, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.logging import StructuredLogger

    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    files = [snapshot_root / f"occupancy-open-event-left-spot-2026-05-18t20-0{index}-00z.jpg" for index in range(count)]
    for path in files:
        path.write_bytes(b"jpeg")

    result = prune_event_snapshots(snapshot_root, retention_count=2, logger=StructuredLogger())

    assert result.pruned_count == 0
    assert result.pruned_bytes == 0
    assert all(path.exists() for path in files)
    assert "snapshot-retention-pruned" not in capsys.readouterr().err


def test_prune_event_snapshots_treats_missing_directory_as_empty(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.logging import StructuredLogger

    result = prune_event_snapshots(tmp_path / "missing", retention_count=2, logger=StructuredLogger())

    assert result.pruned_count == 0
    assert result.pruned_bytes == 0
    assert "snapshot-retention" not in capsys.readouterr().err


def test_prune_event_snapshots_logs_safe_failure_without_raising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor.logging import StructuredLogger

    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    oldest = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    newest = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t21-00-00z.jpg"
    oldest.write_bytes(b"old")
    newest.write_bytes(b"new")

    def fail_unlink(self: Path) -> None:
        raise PermissionError("permission denied token=secret raw_image_bytes abc")

    monkeypatch.setattr(Path, "unlink", fail_unlink)

    result = prune_event_snapshots(snapshot_root, retention_count=1, logger=StructuredLogger())

    output = capsys.readouterr().err
    assert result.pruned_count == 0
    assert oldest.exists()
    assert newest.exists()
    assert '"event":"snapshot-retention-failed"' in output
    assert '"error_type":"PermissionError"' in output
    assert "secret" not in output
    assert "raw_image_bytes abc" not in output


def test_prepare_event_snapshot_prunes_after_copy_without_removing_current_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(source)
    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    old = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    old.write_bytes(b"old")

    snapshot = prepare_event_snapshot(
        source_path=source,
        data_dir=tmp_path,
        snapshots_dir=snapshot_root,
        event_type="occupancy-open-event",
        event_id="event-1",
        spot_id="left_spot",
        observed_at="2026-05-18T20:01:02Z",
        snapshot_retention_count=1,
    )

    assert not old.exists()
    assert snapshot.path.exists()
    assert snapshot.path.read_bytes() == raw_bytes


def test_prepare_event_snapshot_rejects_debug_latest_as_matrix_evidence(tmp_path: Path) -> None:
    source = tmp_path / "debug_latest.jpg"
    write_jpeg(source)

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=tmp_path / "snapshots",
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_invalid_source"
    assert exc_info.value.diagnostics["source_path"] == str(source)
    assert not any((tmp_path / "snapshots").glob("*.jpg"))


def test_prepare_event_snapshot_reports_missing_source_without_deleting_raw_source(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=None,
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_copy_failed"
    assert exc_info.value.diagnostics["source_path"] == str(source)
    assert exc_info.value.diagnostics["snapshot_path"].endswith("left-spot-2026-05-18t20-01-02z.jpg")


def test_prepare_event_snapshot_rejects_non_image_bytes_without_claiming_jpeg_metadata(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    source.write_bytes(b"not a jpeg")

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=tmp_path / "snapshots",
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert source.read_bytes() == b"not a jpeg"
    assert exc_info.value.diagnostics["error_type"] == "snapshot_metadata_failed"
    assert exc_info.value.diagnostics["source_path"] == str(source)
    assert not (tmp_path / "snapshots" / "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg").exists()
    assert "mimetype" not in exc_info.value.diagnostics


def test_quiet_notice_text_is_deterministic_and_contextual() -> None:
    assert format_quiet_window_notice(
        {
            "event_type": "quiet-window-upcoming",
            "event_id": "quiet-window-upcoming:street_sweeping:2026-05-18:13:00-15:00:60m",
            "window_id": "street_sweeping:2026-05-18:13:00-15:00",
            "reminder_minutes_before": 60,
        }
    ) == "Street sweeping starts in 1 hour: street_sweeping:2026-05-18:13:00-15:00"
    assert format_quiet_window_notice(
        {
            "event_type": "quiet-window-started",
            "event_id": "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00",
            "window_id": "street_sweeping:2026-05-18:13:00-15:00",
        }
    ) == "Street sweeping started: street_sweeping:2026-05-18:13:00-15:00"
    assert format_quiet_window_notice(
        {
            "event_type": "quiet-window-ended",
            "event_id": "quiet-window-ended:street_sweeping:2026-05-18:13:00-15:00",
            "window_id": "street_sweeping:2026-05-18:13:00-15:00",
        }
    ) == "Street sweeping ended: street_sweeping:2026-05-18:13:00-15:00"


def test_owner_vehicle_quiet_window_alert_text_and_event_id_are_concise() -> None:
    event = {
        "event_type": "owner-vehicle-quiet-window-alert",
        "spot_id": "right_spot",
        "observed_at": "2026-05-18T20:05:06Z",
        "window_id": "street_sweeping:2026-05-18:13:00-15:00",
        "profile_id": "prof_tesla",
        "owner_vehicle": {
            "label": "Keith's black Tesla",
            "description": "black Tesla, tinted windows, roof rack",
        },
    }

    assert owner_vehicle_quiet_window_event_id(event) == (
        "owner-vehicle-quiet-window-alert:right_spot:prof_tesla:street_sweeping:2026-05-18:13:00-15:00"
    )
    assert format_owner_vehicle_quiet_window_alert(event) == (
        "Street cleaning alert: Keith's black Tesla is parked in right_spot at "
        "2026-05-18 1:05:06 PM PDT during street_sweeping:2026-05-18:13:00-15:00."
    )

def test_send_text_retries_transient_http_statuses_and_logs_retry_decisions() -> None:
    from io import StringIO
    from parking_spot_monitor.logging import StructuredLogger

    seen_statuses = [500, 429, 200]
    sleeps: list[float] = []
    stream = StringIO()

    def handler(request: httpx.Request) -> httpx.Response:
        status = seen_statuses.pop(0)
        if status == 200:
            return httpx.Response(200, json={"event_id": "$event:example.org"}, request=request)
        return httpx.Response(status, json={"errcode": "M_LIMIT_EXCEEDED", "error": f"raw {ACCESS_TOKEN}"}, request=request)

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=3,
        retry_backoff_seconds=0.25,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        logger=StructuredLogger(stream=stream),
    )

    assert client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open") == "$event:example.org"

    output = stream.getvalue()
    records = [json.loads(line) for line in output.splitlines()]
    assert sleeps == [0.25, 0.25]
    assert [record["event"] for record in records] == ["matrix-request-retry", "matrix-request-retry"]
    assert [record["attempt"] for record in records] == [1, 2]
    assert [record["next_attempt"] for record in records] == [2, 3]
    assert all(record["error_type"] == "http_status" for record in records)
    assert all(record["status_code"] in {500, 429} for record in records)
    assert ACCESS_TOKEN not in output
    assert "raw" not in output
    assert "Authorization" not in output


def test_send_text_retries_timeout_then_succeeds_without_leaking_exception_text() -> None:
    from io import StringIO
    from parking_spot_monitor.logging import StructuredLogger

    calls = 0
    sleeps: list[float] = []
    stream = StringIO()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.TimeoutException(f"timed out bearer {ACCESS_TOKEN}", request=request)
        return httpx.Response(200, json={"event_id": "$event:example.org"}, request=request)

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=2,
        retry_backoff_seconds=0.5,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        logger=StructuredLogger(stream=stream),
    )

    assert client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open") == "$event:example.org"
    assert sleeps == [0.5]
    output = stream.getvalue()
    assert '"event":"matrix-request-retry"' in output
    assert '"error_type":"timeout"' in output
    assert ACCESS_TOKEN not in output
    assert "bearer" not in output.lower()


def test_upload_image_retries_malformed_response_then_succeeds() -> None:
    responses = [
        httpx.Response(200, content=b"not json"),
        httpx.Response(200, json={"content_uri": "mxc://example.org/media-id"}),
    ]
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        response = responses.pop(0)
        response.request = request
        return response

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=2,
        retry_backoff_seconds=0.1,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
    )

    assert client.upload_image(filename="snapshot.jpg", data=b"jpeg", content_type="image/jpeg") == "mxc://example.org/media-id"
    assert sleeps == [0.1]


def test_retry_attempts_one_raises_final_error_without_sleep_or_retry_log() -> None:
    from io import StringIO
    from parking_spot_monitor.logging import StructuredLogger

    sleeps: list[float] = []
    stream = StringIO()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errcode": "M_UNKNOWN", "error": f"body {ACCESS_TOKEN}"}, request=request)

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=1,
        retry_backoff_seconds=99,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        logger=StructuredLogger(stream=stream),
    )

    with pytest.raises(MatrixError) as exc_info:
        client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")

    rendered = str(exc_info.value) + repr(exc_info.value.diagnostics) + stream.getvalue()
    assert sleeps == []
    assert stream.getvalue() == ""
    assert exc_info.value.diagnostics["attempt"] == 1
    assert exc_info.value.diagnostics["status_code"] == 500
    assert ACCESS_TOKEN not in rendered
    assert "body" not in rendered


def test_sync_extracts_only_joined_room_text_events_and_requires_next_batch() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "next_batch": "s2",
                "rooms": {
                    "join": {
                        ROOM_ID: {
                            "timeline": {
                                "events": [
                                    {"type": "m.room.message", "event_id": "$1", "sender": "@op:example", "content": {"msgtype": "m.text", "body": "!parking profile summary prof_a"}},
                                    {"type": "m.room.message", "event_id": "$2", "sender": "@op:example", "content": {"msgtype": "m.image", "body": "image"}},
                                    {"type": "m.reaction", "event_id": "$3", "sender": "@op:example", "content": {}},
                                ]
                            }
                        },
                        "!other:example": {"timeline": {"events": [{"type": "m.room.message", "event_id": "$4", "sender": "@op:example", "content": {"msgtype": "m.text", "body": "wrong room"}}]}},
                    }
                },
            },
            request=request,
        )

    client = make_client(httpx.MockTransport(handler))

    result = client.sync(room_id=ROOM_ID, since="s1", timeout_ms=123, limit=7)

    assert result.next_batch == "s2"
    assert [(event.event_id, event.sender, event.body) for event in result.events] == [("$1", "@op:example", "!parking profile summary prof_a")]
    assert seen[0].method == "GET"
    assert seen[0].url.path == "/_matrix/client/v3/sync"
    assert seen[0].url.params["since"] == "s1"
    assert seen[0].url.params["timeout"] == "123"
    assert seen[0].url.params["limit"] == "7"


def test_sync_malformed_response_diagnostics_are_redacted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"rooms": {"join": {}}, "leak": ACCESS_TOKEN}, request=request)

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(MatrixError) as exc_info:
        client.sync(room_id=ROOM_ID)

    rendered = str(exc_info.value) + repr(exc_info.value.diagnostics)
    assert exc_info.value.diagnostics["error_type"] == "malformed_response"
    assert exc_info.value.diagnostics["missing_key"] == "next_batch"
    assert ACCESS_TOKEN not in rendered
    assert "leak" not in rendered


def test_parse_matrix_commands_are_strict_and_normalize_labels() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    assert parse_matrix_command("  !parking   profile   rename   prof_abc   Blue    hatchback  ").label == "Blue hatchback"
    merge = parse_matrix_command("!parking profile merge prof_source prof_target")
    assert (merge.action, merge.source_profile_id, merge.target_profile_id) == ("merge_profiles", "prof_source", "prof_target")
    wrong = parse_matrix_command("!parking wrong sess_123")
    assert (wrong.action, wrong.subject_id) == ("wrong_match", "sess_123")
    owner = parse_matrix_command("!parking owner right_spot")
    assert (owner.action, owner.subject_id) == ("assign_owner", "right_spot")
    who = parse_matrix_command("!parking who")
    assert who.action == "active_spot_assignments"
    help_command = parse_matrix_command("!parking help")
    assert help_command.action == "help"
    summary = parse_matrix_command("!parking profile summary prof_target")
    assert (summary.action, summary.profile_id) == ("profile_summary", "prof_target")
    correct = parse_matrix_command("  !parking   correct   left_spot   open  ")
    assert (correct.action, correct.spot_id, correct.actual_state) == ("correct_spot_state", "left_spot", "open")
    occupied_correct = parse_matrix_command("!parking correct right_spot occupied")
    assert (occupied_correct.action, occupied_correct.spot_id, occupied_correct.actual_state) == ("correct_spot_state", "right_spot", "occupied")
    false_alert = parse_matrix_command("  !parking   false-alert   right_spot   occupied  ")
    assert (false_alert.action, false_alert.spot_id, false_alert.actual_state) == ("correct_spot_state", "right_spot", "occupied")
    learn = parse_matrix_command("  !parking   learn   left_spot   occupied   at   2026-05-18T19:00:00Z  ")
    assert (learn.action, learn.spot_id, learn.actual_state, learn.subject_id) == ("learn_label", "left_spot", "occupied", "2026-05-18T19:00:00Z")
    missed_alert = parse_matrix_command("  !parking   missed-alert   left_spot   open   at   2026-05-18T19:00:00Z  ")
    assert (missed_alert.action, missed_alert.spot_id, missed_alert.actual_state, missed_alert.subject_id) == ("learn_label", "left_spot", "open", "2026-05-18T19:00:00Z")

    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking profile merge prof_a prof_b extra")
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking profile rename badid label")
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking profile summary prof_a extra")
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking unknown")
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking profile rename prof_a " + "x" * 161)
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("   ")

    rejected_correct_commands = [
        "!parking correct",
        "!parking correct left_spot",
        "!parking correct left_spot open extra",
        "!parking correct left_spot closed",
        "!parking correct ../left_spot open",
        "!parking correct left/spot occupied",
        "!parking correct . open",
        "!parking false-alert",
        "!parking false-alert left_spot",
        "!parking false-alert left_spot open extra",
        "!parking false-alert left_spot closed",
        "!parking false-alert ../left_spot open",
        "!parking false-alert left/spot occupied",
        "!parking false-alert . open",
    ]
    for body in rejected_correct_commands:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)

    rejected_learn_commands = [
        "!parking learn",
        "!parking learn left_spot",
        "!parking learn left_spot open",
        "!parking learn left_spot open 2026-05-18T19:00:00Z",
        "!parking learn left_spot closed at 2026-05-18T19:00:00Z",
        "!parking learn ../left_spot open at 2026-05-18T19:00:00Z",
        "!parking learn left_spot open at",
        "!parking learn left_spot open at 2026-05-18T19:00:00Z extra",
        "!parking missed-alert",
        "!parking missed-alert left_spot",
        "!parking missed-alert left_spot open",
        "!parking missed-alert left_spot open 2026-05-18T19:00:00Z",
        "!parking missed-alert left_spot closed at 2026-05-18T19:00:00Z",
        "!parking missed-alert ../left_spot open at 2026-05-18T19:00:00Z",
        "!parking missed-alert left_spot open at",
        "!parking missed-alert left_spot open at 2026-05-18T19:00:00Z extra",
    ]
    for body in rejected_learn_commands:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_parse_matrix_operator_cockpit_commands_are_exact_and_bounded() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    status = parse_matrix_command("  !parking   status  ")
    config = parse_matrix_command("\n!parking config\t")

    assert status.action == "status"
    assert config.action == "config"

    rejected = [
        "!parking status now",
        "!parking config verbose",
        "!parking stat",
        "!parking settings",
        "!park status",
        "parking status",
        "!!parking status",
        "!parking",
        "!parking status " + "x" * 513,
    ]
    for body in rejected:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


class FakeCorrection:
    def __init__(self, correction_id: str = "corr_1", matrix_event_id: str | None = None) -> None:
        self.correction_id = correction_id
        self.matrix_event_id = matrix_event_id


class FakeSession:
    def __init__(self, session_id: str, spot_id: str = "left_spot") -> None:
        self.session_id = session_id
        self.spot_id = spot_id
        self.started_at = session_id
        self.ended_at = session_id


class FakeCommandArchive:
    def __init__(self, cursor: dict[str, str] | None = None) -> None:
        self.cursor = cursor
        self.cursor_writes: list[dict[str, str]] = []
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.corrections: list[FakeCorrection] = []
        self.sessions = [FakeSession("sess_current", "left_spot")]

    def read_matrix_cursor(self) -> dict[str, str] | None:
        return self.cursor

    def write_matrix_cursor(self, state: dict[str, str]) -> None:
        self.cursor_writes.append(state)
        self.cursor = state

    def load_corrections(self) -> list[FakeCorrection]:
        return self.corrections

    def rename_profile(self, *args: Any, **kwargs: Any) -> FakeCorrection:
        self.calls.append(("rename_profile", args, kwargs))
        correction = FakeCorrection("rename_1", kwargs.get("matrix_event_id"))
        self.corrections.append(correction)
        return correction

    def merge_profiles(self, *args: Any, **kwargs: Any) -> FakeCorrection:
        self.calls.append(("merge_profiles", args, kwargs))
        correction = FakeCorrection("merge_1", kwargs.get("matrix_event_id"))
        self.corrections.append(correction)
        return correction

    def mark_wrong_match(self, *args: Any, **kwargs: Any) -> FakeCorrection:
        self.calls.append(("mark_wrong_match", args, kwargs))
        correction = FakeCorrection("wrong_1", kwargs.get("matrix_event_id"))
        self.corrections.append(correction)
        return correction

    def profile_summary(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("profile_summary", args, kwargs))
        correction = FakeCorrection("summary_1", kwargs.get("matrix_event_id"))
        self.corrections.append(correction)
        return {"profile_id": args[0], "label": "Blue hatchback", "closed_session_count": 2, "active_session_count": 1, "wrong_match_excluded_session_count": 0, "estimate_status": "estimated", "estimate_sample_count": 3}

    def assign_owner_profile_to_active_spot(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("assign_owner_profile_to_active_spot", args, kwargs))
        return type("Assignment", (), {"session_id": "sess_current", "profile_id": "prof_owner", "profile_confidence": 1.0})()

    def active_spot_assignments(self) -> list[dict[str, Any]]:
        self.calls.append(("active_spot_assignments", (), {}))
        return [
            {
                "spot_id": "left_spot",
                "session_id": "sess_left",
                "profile_id": None,
                "profile_label": None,
                "profile_confidence": None,
                "is_owner": False,
                "owner_label": None,
                "profile_sample_count": None,
            },
            {
                "spot_id": "right_spot",
                "session_id": "sess_current",
                "profile_id": "prof_owner",
                "profile_label": "Keith's black Tesla",
                "profile_confidence": 1.0,
                "is_owner": True,
                "owner_label": "Keith's black Tesla",
                "profile_sample_count": 7,
            },
        ]

    def load_active_sessions(self) -> list[FakeSession]:
        return self.sessions

    def list_closed_sessions(self) -> list[FakeSession]:
        return []


def test_active_spot_assignments_reply_includes_occupied_and_open_durations() -> None:
    from parking_spot_monitor.matrix_cockpit import _format_active_spot_assignments_reply

    reply = _format_active_spot_assignments_reply(
        [
            {
                "spot_id": "left_spot",
                "status": "occupied",
                "session_id": "sess_left",
                "profile_id": None,
                "profile_label": None,
                "profile_confidence": None,
                "profile_sample_count": None,
                "started_at": "2026-05-17T15:30:00Z",
            },
            {
                "spot_id": "right_spot",
                "status": "open",
                "last_status_changed_at": "2026-05-17T16:35:00Z",
            },
        ],
        now="2026-05-17T17:40:00Z",
    )

    assert "left_spot: occupied for 2 hr 10 min — unknown vehicle — session sess_left" in reply
    assert "right_spot: open for 1 hr 5 min" in reply



def test_active_spot_assignments_merge_runtime_open_spots_with_duration(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixOperatorCockpitContext
    from parking_spot_monitor.matrix_cockpit import _active_spot_assignments_with_runtime_status
    from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
    from parking_spot_monitor.state import RuntimeState, save_runtime_state

    settings = type(
        "Settings",
        (),
        {
            "spots": type("Spots", (), {"left_spot": object(), "right_spot": object()})(),
        },
    )()
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, last_status_changed_at="2026-05-17T15:30:00Z"),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY, last_status_changed_at="2026-05-17T16:35:00Z"),
            }
        ),
    )
    context = MatrixOperatorCockpitContext(settings=settings, data_dir=tmp_path, health_path=tmp_path / "health.json", state_path=state_path)

    assignments = _active_spot_assignments_with_runtime_status(
        [
            {
                "spot_id": "left_spot",
                "session_id": "sess_left",
                "profile_id": None,
                "started_at": "2026-05-17T15:30:00Z",
            }
        ],
        cockpit_context=context,
    )

    assert assignments == [
        {
            "spot_id": "left_spot",
            "session_id": "sess_left",
            "profile_id": None,
            "started_at": "2026-05-17T15:30:00Z",
            "status": "occupied",
            "last_status_changed_at": "2026-05-17T15:30:00Z",
        },
        {
            "spot_id": "right_spot",
            "status": "open",
            "last_status_changed_at": "2026-05-17T16:35:00Z",
        },
    ]



def test_command_service_bootstraps_cursor_without_processing_backlog() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s2", events=(MatrixTextEvent(event_id="$old", sender="@op:example", room_id=ROOM_ID, body="!parking profile rename prof_a Blue"),))

        def send_text(self, **kwargs: Any) -> str:
            raise AssertionError("bootstrap must not reply to backlog")

    archive = FakeCommandArchive(cursor=None)
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@op:example"], bot_user_id="@bot:example")  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.bootstrapped is True
    assert result.processed_count == 0
    assert archive.calls == []
    assert archive.cursor_writes == [{"next_batch": "s2"}]


def test_command_service_authorizes_applies_and_replies_safely() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$self", sender="@bot:example", room_id=ROOM_ID, body="!parking profile rename prof_a Self"),
                    MatrixTextEvent(event_id="$deny", sender="@intruder:example", room_id=ROOM_ID, body="!parking profile rename prof_a Secret"),
                    MatrixTextEvent(event_id="$rename", sender="@op:example", room_id=ROOM_ID, body="!parking profile rename prof_a Blue hatchback"),
                    MatrixTextEvent(event_id="$merge", sender="@op:example", room_id=ROOM_ID, body="!parking profile merge prof_a prof_b"),
                    MatrixTextEvent(event_id="$wrong", sender="@op:example", room_id=ROOM_ID, body="!parking wrong left_spot"),
                    MatrixTextEvent(event_id="$owner", sender="@op:example", room_id=ROOM_ID, body="!parking owner right_spot"),
                    MatrixTextEvent(event_id="$who", sender="@op:example", room_id=ROOM_ID, body="!parking who"),
                    MatrixTextEvent(event_id="$help", sender="@op:example", room_id=ROOM_ID, body="!parking help"),
                    MatrixTextEvent(event_id="$summary", sender="@op:example", room_id=ROOM_ID, body="!parking profile summary prof_b"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@op:example"], bot_user_id="@bot:example")  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.processed_count == 7
    assert result.error_count == 1
    assert [call[0] for call in archive.calls] == ["rename_profile", "merge_profiles", "mark_wrong_match", "assign_owner_profile_to_active_spot", "active_spot_assignments", "profile_summary"]
    assert archive.calls[0][1] == ("prof_a", "Blue hatchback")
    assert archive.calls[0][2]["matrix_event_id"] == "$rename"
    assert archive.calls[2][1] == ("sess_current",)
    assert archive.cursor_writes[-1] == {"next_batch": "s3"}
    assert len(replies) == 8
    rendered_replies = "\n".join(reply["body"] for reply in replies)
    assert "not authorized" in rendered_replies
    assert "Owner vehicle assigned to right_spot" in rendered_replies
    assert "left_spot: occupied — unknown vehicle" in rendered_replies
    assert "right_spot: occupied — Keith's black Tesla — confidence 1.00 — samples 7" in rendered_replies
    assert "!parking help" in rendered_replies
    assert "!parking confidence" in rendered_replies
    assert "!parking analytics [today|7d|30d|all]" in rendered_replies
    assert "!parking owner <spot_id>" in rendered_replies
    assert "Profile prof_b: Blue hatchback" in rendered_replies
    assert ACCESS_TOKEN not in rendered_replies


def test_command_service_fails_corrections_when_duplicate_check_is_unavailable() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$rename", sender="@op:example", room_id=ROOM_ID, body="!parking profile rename prof_a Blue"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    class Archive(FakeCommandArchive):
        def load_corrections(self) -> list[FakeCorrection]:
            raise PermissionError("corrections unreadable")

    archive = Archive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@op:example"], bot_user_id="@bot:example")  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert archive.calls == []
    assert replies == [{"room_id": ROOM_ID, "txn_id": "command:$rename", "body": "Command failed: PermissionError"}]


def test_command_service_authorized_correct_records_feedback_label() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$correct", sender="@op:example", room_id=ROOM_ID, body="!parking correct left_spot open"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    class FeedbackLabeler:
        def record_correction(self, **kwargs: Any) -> Any:
            calls.append(dict(kwargs))
            return type("FeedbackResult", (), {"reply_text": "Parking correction recorded for left_spot: actual open."})()

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$correct"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        feedback_labeler=FeedbackLabeler(),
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert archive.calls == []
    assert calls == [
        {
            "spot_id": "left_spot",
            "actual_state": "open",
            "matrix_event_id": "$correct",
            "matrix_sender": "@op:example",
            "matrix_room_id": ROOM_ID,
        }
    ]
    assert replies == [{"room_id": ROOM_ID, "txn_id": "command:$correct", "body": "Parking correction recorded for left_spot: actual open."}]


def test_command_service_authorized_learn_routes_to_labeler_with_runtime_context() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []
    calls: list[dict[str, Any]] = []
    settings = type("Settings", (), {"spots": type("Spots", (), {"left_spot": object()})()})()
    state_path = Path("/tmp/state.json")
    detector = object()

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$learn", sender="@op:example", room_id=ROOM_ID, body="!parking learn left_spot occupied at 2026-05-18T19:00:00Z"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    class FeedbackLabeler:
        def record_learn_label(self, **kwargs: Any) -> Any:
            calls.append(dict(kwargs))
            return type("LearnResult", (), {"reply_text": "Parking learn label recorded\nSpot: left_spot\nTarget: occupied"})()

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        feedback_labeler=FeedbackLabeler(),
        cockpit_context=MatrixOperatorCockpitContext(settings=settings, data_dir=Path("/tmp"), health_path=Path("/tmp/health.json"), state_path=state_path, incident_detector=detector),
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert calls == [
        {
            "spot_id": "left_spot",
            "target_state": "occupied",
            "requested_time": "2026-05-18T19:00:00Z",
            "settings": settings,
            "state_path": state_path,
            "detector": detector,
            "matrix_event_id": "$learn",
            "matrix_sender": "@op:example",
            "matrix_room_id": ROOM_ID,
        }
    ]
    assert replies == [{"room_id": ROOM_ID, "txn_id": "command:$learn", "body": "Parking learn label recorded\nSpot: left_spot\nTarget: occupied"}]


def test_command_service_explicit_feedback_aliases_are_authorized_idempotent_and_text_only() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []
    correction_event_ids: set[str] = set()
    learn_event_ids: set[str] = set()
    calls: list[tuple[str, dict[str, Any]]] = []
    settings = type("Settings", (), {"spots": type("Spots", (), {"left_spot": object(), "right_spot": object()})()})()
    state_path = Path("/tmp/state.json")
    detector = object()

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$false", sender="@op:example", room_id=ROOM_ID, body="!parking false-alert left_spot open"),
                    MatrixTextEvent(event_id="$false", sender="@op:example", room_id=ROOM_ID, body="!parking false-alert left_spot open"),
                    MatrixTextEvent(event_id="$missed", sender="@op:example", room_id=ROOM_ID, body="!parking missed-alert right_spot occupied at 2026-05-18T19:00:00Z"),
                    MatrixTextEvent(event_id="$missed", sender="@op:example", room_id=ROOM_ID, body="!parking missed-alert right_spot occupied at 2026-05-18T19:00:00Z"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("feedback command replies must be text only")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("feedback command replies must be text only")

    class FeedbackLabeler:
        def record_correction(self, **kwargs: Any) -> Any:
            calls.append(("correction", dict(kwargs)))
            duplicate = kwargs["matrix_event_id"] in correction_event_ids
            correction_event_ids.add(kwargs["matrix_event_id"])
            text = "Command already applied; acknowledgement repeated." if duplicate else "Parking correction recorded\n- spot: left_spot\n- actual: open"
            return type("FeedbackResult", (), {"reply_text": text})()

        def record_learn_label(self, **kwargs: Any) -> Any:
            calls.append(("learn", dict(kwargs)))
            duplicate = kwargs["matrix_event_id"] in learn_event_ids
            learn_event_ids.add(kwargs["matrix_event_id"])
            text = "Command already applied; learn acknowledgement repeated." if duplicate else "Parking learn label recorded\n- spot: right_spot\n- target: occupied"
            return type("LearnResult", (), {"reply_text": text})()

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        feedback_labeler=FeedbackLabeler(),
        cockpit_context=MatrixOperatorCockpitContext(settings=settings, data_dir=Path("/tmp"), health_path=Path("/tmp/health.json"), state_path=state_path, incident_detector=detector),
    )

    result = service.poll_once()

    assert result.processed_count == 4
    assert result.error_count == 0
    assert [kind for kind, _ in calls] == ["correction", "correction", "learn", "learn"]
    assert calls[0][1] == {
        "spot_id": "left_spot",
        "actual_state": "open",
        "matrix_event_id": "$false",
        "matrix_sender": "@op:example",
        "matrix_room_id": ROOM_ID,
    }
    assert calls[2][1] == {
        "spot_id": "right_spot",
        "target_state": "occupied",
        "requested_time": "2026-05-18T19:00:00Z",
        "settings": settings,
        "state_path": state_path,
        "detector": detector,
        "matrix_event_id": "$missed",
        "matrix_sender": "@op:example",
        "matrix_room_id": ROOM_ID,
    }
    assert [reply["txn_id"] for reply in replies] == ["command:$false", "command:$false", "command:$missed", "command:$missed"]
    assert [reply["body"] for reply in replies] == [
        "Parking correction recorded\n- spot: left_spot\n- actual: open",
        "Command already applied; acknowledgement repeated.",
        "Parking learn label recorded\n- spot: right_spot\n- target: occupied",
        "Command already applied; learn acknowledgement repeated.",
    ]
    assert all(isinstance(reply["body"], str) and len(reply["body"].encode("utf-8")) <= 4096 for reply in replies)


def test_command_service_explicit_feedback_aliases_reject_unauthorized_and_malformed_before_labeler() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []
    labeler_calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$denied-false", sender="@intruder:example", room_id=ROOM_ID, body="!parking false-alert left_spot open"),
                    MatrixTextEvent(event_id="$denied-missed", sender="@intruder:example", room_id=ROOM_ID, body="!parking missed-alert left_spot open at 2026-05-18T19:00:00Z"),
                    MatrixTextEvent(event_id="$bad-false", sender="@op:example", room_id=ROOM_ID, body="!parking false-alert left_spot closed"),
                    MatrixTextEvent(event_id="$bad-missed", sender="@op:example", room_id=ROOM_ID, body="!parking missed-alert left_spot open 2026-05-18T19:00:00Z"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("rejection replies must be text only")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("rejection replies must be text only")

    class FeedbackLabeler:
        def record_correction(self, **kwargs: Any) -> Any:
            labeler_calls.append(dict(kwargs))
            raise AssertionError("rejected false-alert command must not reach labeler")

        def record_learn_label(self, **kwargs: Any) -> Any:
            labeler_calls.append(dict(kwargs))
            raise AssertionError("rejected missed-alert command must not reach labeler")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        feedback_labeler=FeedbackLabeler(),
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 4
    assert labeler_calls == []
    assert [reply["body"] for reply in replies] == [
        "Command rejected: sender is not authorized.",
        "Command rejected: sender is not authorized.",
        "Command rejected: invalid actual state",
        "Command rejected: usage: !parking missed-alert <spot_id> <open|occupied> at <time>",
    ]
    assert all(reply["room_id"] == ROOM_ID and reply["txn_id"].startswith("command:") for reply in replies)
    assert all(len(reply["body"].encode("utf-8")) <= 4096 for reply in replies)


def test_command_service_learn_rejects_unauthorized_malformed_and_missing_labeler() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    labeler_calls: list[dict[str, Any]] = []
    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$denied", sender="@intruder:example", room_id=ROOM_ID, body="!parking learn left_spot open at 2026-05-18T19:00:00Z"),
                    MatrixTextEvent(event_id="$malformed", sender="@op:example", room_id=ROOM_ID, body="!parking learn left_spot open 2026-05-18T19:00:00Z"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    class FeedbackLabeler:
        def record_learn_label(self, **kwargs: Any) -> Any:
            labeler_calls.append(dict(kwargs))
            raise AssertionError("rejected learn command must not reach labeler")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        feedback_labeler=FeedbackLabeler(),
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 2
    assert labeler_calls == []
    assert replies == [
        {"room_id": ROOM_ID, "txn_id": "command:$denied", "body": "Command rejected: sender is not authorized."},
        {"room_id": ROOM_ID, "txn_id": "command:$malformed", "body": "Command rejected: usage: !parking learn <spot_id> <open|occupied> at <time>"},
    ]

    missing_replies: list[dict[str, Any]] = []

    class MissingLabelerClient:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s4",
                events=(MatrixTextEvent(event_id="$missing", sender="@op:example", room_id=ROOM_ID, body="!parking learn left_spot open at 2026-05-18T19:00:00Z"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            missing_replies.append(dict(kwargs))
            return "$reply"

    missing = MatrixCommandService(
        client=MissingLabelerClient(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s3"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
    )

    missing_result = missing.poll_once()

    assert missing_result.processed_count == 0
    assert missing_result.error_count == 1
    assert missing_replies == [{"room_id": ROOM_ID, "txn_id": "command:$missing", "body": "Command failed: RuntimeError"}]


def test_command_service_correct_requires_authorization_and_configured_labeler() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    unauthorized_replies: list[dict[str, Any]] = []
    labeler_calls: list[dict[str, Any]] = []

    class UnauthorizedClient:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$denied", sender="@intruder:example", room_id=ROOM_ID, body="!parking correct left_spot occupied"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            unauthorized_replies.append(dict(kwargs))
            return "$reply"

    class FeedbackLabeler:
        def record_correction(self, **kwargs: Any) -> Any:
            labeler_calls.append(dict(kwargs))
            raise AssertionError("unauthorized correction must not reach labeler")

    unauthorized = MatrixCommandService(
        client=UnauthorizedClient(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        feedback_labeler=FeedbackLabeler(),
    )

    unauthorized_result = unauthorized.poll_once()

    assert unauthorized_result.processed_count == 0
    assert unauthorized_result.error_count == 1
    assert labeler_calls == []
    assert unauthorized_replies == [{"room_id": ROOM_ID, "txn_id": "command:$denied", "body": "Command rejected: sender is not authorized."}]

    missing_labeler_replies: list[dict[str, Any]] = []

    class MissingLabelerClient:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$missing", sender="@op:example", room_id=ROOM_ID, body="!parking correct left_spot occupied"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            missing_labeler_replies.append(dict(kwargs))
            return "$reply"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    missing_labeler = MatrixCommandService(
        client=MissingLabelerClient(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
    )

    missing_labeler_result = missing_labeler.poll_once()

    assert missing_labeler_result.processed_count == 0
    assert missing_labeler_result.error_count == 1
    assert archive.calls == []
    assert missing_labeler_replies == [{"room_id": ROOM_ID, "txn_id": "command:$missing", "body": "Command failed: RuntimeError"}]


def test_command_service_default_empty_allowlist_rejects_mutations() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$rename", sender="@op:example", room_id=ROOM_ID, body="!parking profile rename prof_a Blue"),))

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=[])  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert archive.calls == []
    assert replies == ["Command rejected: sender is not authorized."]


def test_command_service_rejects_unauthorized_status_before_application() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$status", sender="@intruder:example", room_id=ROOM_ID, body="!parking status"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    class Service(MatrixCommandService):
        def _apply_command(self, *args: Any, **kwargs: Any) -> str:
            raise AssertionError("unauthorized status must be rejected before application")

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = Service(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@operator:example"])  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert archive.calls == []
    assert replies == [{"room_id": ROOM_ID, "txn_id": "command:$status", "body": "Command rejected: sender is not authorized."}]


def test_command_service_authorized_status_and_config_reply_via_command_txn_path() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    applied_actions: list[str] = []
    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$status", sender="@operator:example", room_id=ROOM_ID, body="!parking status"),
                    MatrixTextEvent(event_id="$config", sender="@operator:example", room_id=ROOM_ID, body="!parking config"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    def cockpit_provider(action: str, **kwargs: str) -> str:
        applied_actions.append(action)
        return f"reply for {action}"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 2
    assert result.error_count == 0
    assert applied_actions == ["status", "config"]
    assert replies == [
        {"room_id": ROOM_ID, "txn_id": "command:$status", "body": "reply for status"},
        {"room_id": ROOM_ID, "txn_id": "command:$config", "body": "reply for config"},
    ]


def test_command_service_status_and_config_use_cockpit_provider_without_archive_corrections() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []
    provider_actions: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$repeat", sender="@op:example", room_id=ROOM_ID, body="!parking status"),
                    MatrixTextEvent(event_id="$repeat", sender="@op:example", room_id=ROOM_ID, body="!parking config"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    def cockpit_provider(action: str) -> str:
        provider_actions.append(action)
        return f"cockpit {action} reply"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$repeat"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 2
    assert result.error_count == 0
    assert provider_actions == ["status", "config"]
    assert archive.calls == []
    assert [reply["body"] for reply in replies] == ["cockpit status reply", "cockpit config reply"]


def test_command_service_status_provider_failure_replies_safe_failure() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$status", sender="@op:example", room_id=ROOM_ID, body="!parking status"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    def failing_provider(action: str) -> str:
        raise RuntimeError(f"boom {ACCESS_TOKEN} {action}")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=failing_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == [{"room_id": ROOM_ID, "txn_id": "command:$status", "body": "Command failed: RuntimeError"}]
    assert ACCESS_TOKEN not in replies[0]["body"]


def test_command_service_missing_cockpit_provider_is_deterministic_configuration_failure() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$config", sender="@op:example", room_id=ROOM_ID, body="!parking config"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == ["Command failed: RuntimeError"]


def test_parse_matrix_latest_command_is_exact_and_rejects_arguments() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    latest = parse_matrix_command("  !parking   latest  ")

    assert latest.action == "latest"
    for body in ["!parking latest now", "!parking latest debug", "!parking latest ../debug_latest.jpg"]:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)



def test_parse_matrix_analytics_command_accepts_bounded_windows_and_rejects_extra_args() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    default = parse_matrix_command("  !parking   analytics  ")
    assert (default.action, default.subject_id) == ("analytics", "7d")

    for window in ["today", "7d", "30d", "all"]:
        command = parse_matrix_command(f"!parking analytics {window}")
        assert (command.action, command.subject_id) == ("analytics", window)

    rejected = [
        "!parking analytics today extra",
        "!parking analytics yesterday",
        "!parking analytics 90d",
        "!parking analytics ../sessions",
        "!parking analytics " + "x" * 513,
    ]
    for body in rejected:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_parse_matrix_confidence_command_is_exact_and_rejects_arguments() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    confidence = parse_matrix_command("  !parking   confidence  ")

    assert confidence.action == "confidence"
    for body in ["!parking confidence now", "!parking confidence verbose", "!parking confidence ../state.json"]:
        with pytest.raises(MatrixCommandParseError) as exc_info:
            parse_matrix_command(body)
        assert "usage: !parking confidence" in str(exc_info.value)


def test_command_service_authorized_latest_sends_text_and_one_raw_image_without_archive_correction(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(latest_path, size=(11, 7))
    calls: list[dict[str, Any]] = []
    provider_actions: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$latest1", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),
                    MatrixTextEvent(event_id="$latest1", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/latest"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    def cockpit_provider(action: str) -> MatrixCommandResponse:
        provider_actions.append(action)
        return MatrixCommandResponse(
            text="Parking monitor latest\nSnapshot: fresh raw latest.jpg; 11x7; 632 bytes",
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 11, "h": 7},
        )

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$latest1"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 2
    assert result.error_count == 0
    assert provider_actions == ["latest", "latest"]
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text", "upload", "image", "text", "upload", "image"]
    assert calls[0]["txn_id"] == "command:$latest1:text"
    assert calls[0]["body"].startswith("Parking monitor latest")
    assert calls[1]["filename"] == "latest.jpg"
    assert calls[1]["content_type"] == "image/jpeg"
    assert calls[1]["data"] == raw_bytes
    assert calls[2]["txn_id"] == "command:$latest1:image"
    assert calls[2]["body"] == "Raw full-frame latest.jpg evidence"
    assert calls[2]["info"] == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 11, "h": 7}


def test_command_service_resizes_oversized_command_image_before_upload(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent
    from parking_spot_monitor.matrix_snapshots import MAX_MATRIX_UPLOAD_IMAGE_BYTES

    latest_path = tmp_path / "latest.jpg"
    noisy = Image.effect_noise((1458, 806), 90).convert("RGB")
    noisy.save(latest_path, format="JPEG", quality=95)
    assert latest_path.stat().st_size > MAX_MATRIX_UPLOAD_IMAGE_BYTES
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$latest-large", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/latest"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=lambda action: MatrixCommandResponse(
            text="Parking monitor latest",
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": latest_path.stat().st_size, "w": 1458, "h": 806},
        ),
    )

    result = service.poll_once()

    upload = next(call for call in calls if call["kind"] == "upload")
    image = next(call for call in calls if call["kind"] == "image")
    assert result.error_count == 0
    assert len(upload["data"]) <= MAX_MATRIX_UPLOAD_IMAGE_BYTES
    assert image["info"]["size"] == len(upload["data"])
    assert image["info"]["w"] <= 960


def test_command_service_close_closes_owned_matrix_client() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService

    class Client:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    client = Client()
    service = MatrixCommandService(
        client=client,  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
    )

    service.close()

    assert client.closed is True


def test_command_service_who_can_send_active_assignments_with_fresh_snapshot(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(latest_path, size=(13, 9))
    calls: list[dict[str, Any]] = []
    provider_inputs: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$who", sender="@op:example", room_id=ROOM_ID, body="!parking who"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/who"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    def who_snapshot_provider(base_text: str) -> MatrixCommandResponse:
        provider_inputs.append(base_text)
        return MatrixCommandResponse(
            text="Parking monitor who\nSnapshot: fresh capture at 2026-05-16 10:42:39 AM PDT\n\n" + "\n".join(base_text.splitlines()[1:]),
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 13, "h": 9},
        )

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=who_snapshot_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert provider_inputs and provider_inputs[0].startswith("Active parking sessions:")
    assert [call["kind"] for call in calls] == ["text", "upload", "image"]
    assert calls[0]["txn_id"] == "command:$who:text"
    assert "Snapshot: fresh capture" in calls[0]["body"]
    assert calls[1]["filename"] == "latest.jpg"
    assert calls[1]["data"] == raw_bytes
    assert calls[2]["txn_id"] == "command:$who:image"
    assert calls[2]["body"] == "Raw full-frame latest.jpg evidence"

def test_command_service_latest_failure_and_unauthorized_latest_are_text_only(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$deny", sender="@intruder:example", room_id=ROOM_ID, body="!parking latest"),
                    MatrixTextEvent(event_id="$latest", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),
                    MatrixTextEvent(event_id="$status", sender="@op:example", room_id=ROOM_ID, body="!parking status"),
                    MatrixTextEvent(event_id="$config", sender="@op:example", room_id=ROOM_ID, body="!parking config"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("text-only latest/status/config replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("text-only latest/status/config replies must not send media")

    def cockpit_provider(action: str) -> str | MatrixCommandResponse:
        if action == "latest":
            return MatrixCommandResponse(text="Parking monitor latest unavailable: latest.jpg missing", image_path=None, image_info=None)
        return f"cockpit {action} reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 3
    assert result.error_count == 1
    assert [call["body"] for call in calls] == [
        "Command rejected: sender is not authorized.",
        "Parking monitor latest unavailable: latest.jpg missing",
        "cockpit status reply",
        "cockpit config reply",
    ]
    assert all(call["kind"] == "text" for call in calls)


def test_parse_matrix_why_explain_and_recent_commands_are_exact_and_bounded() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    why = parse_matrix_command("  !parking   why   right_spot  ")
    explain = parse_matrix_command("  !parking   explain   right_spot  ")
    recent = parse_matrix_command("\n!parking recent\t")

    assert (why.action, why.spot_id) == ("why", "right_spot")
    assert (explain.action, explain.spot_id) == ("explain", "right_spot")
    assert recent.action == "recent"

    rejected = [
        "!parking why",
        "!parking why right_spot extra",
        "!parking why ../state.json",
        "!parking why /tmp/right_spot",
        "!parking why " + "x" * 161,
        "!parking explain",
        "!parking explain right_spot extra",
        "!parking explain ../state.json",
        "!parking explain /tmp/right_spot",
        "!parking explain " + "x" * 161,
        "!parking recent now",
        "!parking recent verbose",
        "!parking at",
        "!parking at 7:39pm",
        "!parking at 7:39pm ../state.json",
        "!parking at 7:39pm left_spot extra",
    ]
    for body in rejected:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_command_service_why_explain_recent_use_provider_text_only_repeatably_without_archive_correction() -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []
    provider_calls: list[tuple[str, str | None]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$why", sender="@op:example", room_id=ROOM_ID, body="!parking why right_spot"),
                    MatrixTextEvent(event_id="$why", sender="@op:example", room_id=ROOM_ID, body="!parking why right_spot"),
                    MatrixTextEvent(event_id="$explain", sender="@op:example", room_id=ROOM_ID, body="!parking explain right_spot"),
                    MatrixTextEvent(event_id="$recent", sender="@op:example", room_id=ROOM_ID, body="!parking recent"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("why/recent replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("why/recent replies must not send media")

    def cockpit_provider(action: str, *, spot_id: str | None = None) -> MatrixCommandResponse:
        provider_calls.append((action, spot_id))
        text = f"decision {action}" + (f" {spot_id}" if spot_id else "")
        return MatrixCommandResponse(text=text)

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$why"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 4
    assert result.error_count == 0
    assert provider_calls == [("why", "right_spot"), ("why", "right_spot"), ("explain", "right_spot"), ("recent", None)]
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text", "text", "text", "text"]
    assert [call["body"] for call in calls] == ["decision why right_spot", "decision why right_spot", "decision explain right_spot", "decision recent"]
    assert [call["txn_id"] for call in calls] == ["command:$why", "command:$why", "command:$explain", "command:$recent"]



def test_command_service_incident_review_uses_cockpit_context_with_image_response(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    image_path = tmp_path / "incident_left_spot.jpg"
    write_jpeg(image_path, size=(11, 7))
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$at", sender="@op:example", room_id=ROOM_ID, body="!parking at 7:39pm left_spot"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/incident"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    class Context:
        def incident_review_reply(
            self,
            *,
            spot_id: str,
            incident_time: str,
            logger: Any | None = None,
        ) -> MatrixCommandResponse:
            del logger
            assert spot_id == "left_spot"
            assert incident_time == "7:39pm"
            return MatrixCommandResponse(
                text="Incident review: left_spot around 7:39pm",
                image_path=image_path,
                image_info={"mimetype": "image/jpeg", "size": image_path.stat().st_size, "w": 11, "h": 7},
            )

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_context=Context(),  # type: ignore[arg-type]
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert [call["kind"] for call in calls] == ["text", "upload", "image"]
    assert calls[0]["txn_id"] == "command:$at:text"
    assert calls[1]["filename"] == "incident_left_spot.jpg"
    assert calls[2]["txn_id"] == "command:$at:image"



def test_command_service_incident_review_real_context_replays_detector_sends_safe_media_and_avoids_live_side_effects(tmp_path: Path) -> None:
    from parking_spot_monitor.config import load_settings
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent
    from parking_spot_monitor.state import RuntimeState, save_runtime_state
    from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
    from parking_spot_monitor.detection import VehicleDetection

    config_path = tmp_path / "config.yaml"
    config_path.write_text(Path("config.yaml.example").read_text(encoding="utf-8"), encoding="utf-8")
    settings = load_settings(config_path, environ={"RTSP_URL": "rtsp://operator:secret@camera/live", "MATRIX_ACCESS_TOKEN": ACCESS_TOKEN})
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    frame = frames_dir / "20260518T023900Z.jpg"
    raw_bytes = write_jpeg(frame, size=(1458, 806))
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=4, miss_streak=0),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY, hit_streak=0, miss_streak=5),
            }
        ),
    )
    state_before = state_path.read_text(encoding="utf-8")
    state_mtime_before_ns = state_path.stat().st_mtime_ns

    class Detector:
        calls: list[Path] = []

        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None, inference_image_size: int | None = None) -> list[VehicleDetection]:
            self.calls.append(Path(frame_path))
            return [VehicleDetection(class_name="car", confidence=0.91, bbox=(25, 30, 275, 325))]

    detector = Detector()
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$at", sender="@op:example", room_id=ROOM_ID, body="!parking at 7:39pm left_spot"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/incident"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    context = MatrixOperatorCockpitContext(
        settings=settings,
        data_dir=tmp_path,
        health_path=tmp_path / "health.json",
        state_path=state_path,
        incident_detector=detector,
    )
    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_context=context,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert detector.calls == [frame]
    assert archive.calls == []
    assert state_path.read_text(encoding="utf-8") == state_before
    assert state_path.stat().st_mtime_ns == state_mtime_before_ns
    assert [call["kind"] for call in calls] == ["text", "upload", "image"]
    assert "Detector replay:" in calls[0]["body"]
    assert "left_spot: accepted car confidence 0.91" in calls[0]["body"]
    assert "State simulation:" in calls[0]["body"]
    assert len(calls[0]["body"].encode("utf-8")) <= 4096
    assert calls[1]["filename"] == "incident_left_spot.jpg"
    assert calls[1]["content_type"] == "image/jpeg"
    assert calls[1]["data"] != raw_bytes
    assert len(calls[1]["data"]) <= 300_000
    assert calls[2]["txn_id"] == "command:$at:image"
    assert calls[2]["info"]["mimetype"] == "image/jpeg"
    rendered = repr(calls)
    assert ACCESS_TOKEN not in rendered
    assert "rtsp://" not in rendered
    assert "raw_image" not in rendered
    assert "Traceback" not in rendered
    assert str(tmp_path) not in calls[0]["body"]
    assert str(tmp_path) not in calls[1]["filename"]
    assert str(tmp_path) not in calls[2]["body"]

def test_command_service_rejects_unauthorized_why_and_explain_before_memory_or_provider_paths() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$why", sender="@intruder:example", room_id=ROOM_ID, body="!parking why right_spot"),
                    MatrixTextEvent(event_id="$explain", sender="@intruder:example", room_id=ROOM_ID, body="!parking explain right_spot"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

    def cockpit_provider(action: str, *, spot_id: str | None = None) -> str:
        raise AssertionError("unauthorized why/explain must not touch provider or memory paths")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 2
    assert replies == ["Command rejected: sender is not authorized.", "Command rejected: sender is not authorized."]


def test_command_service_confidence_context_reads_local_artifacts_text_only(tmp_path: Path) -> None:
    from parking_spot_monitor.config import load_settings
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent
    from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record
    from parking_spot_monitor.state import RuntimeState, save_runtime_state

    config_path = tmp_path / "config.yaml"
    config_path.write_text(Path("config.yaml.example").read_text(encoding="utf-8"), encoding="utf-8")
    settings = load_settings(config_path, environ={"RTSP_URL": "rtsp://operator:secret@camera/live", "MATRIX_ACCESS_TOKEN": ACCESS_TOKEN})
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=4, miss_streak=0),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY, hit_streak=0, miss_streak=5),
            }
        ),
    )
    state_before = state_path.read_text(encoding="utf-8")
    state_mtime_before_ns = state_path.stat().st_mtime_ns
    health_path = tmp_path / "health.json"
    health_path.write_text(json.dumps({"last_matrix_error": {"error_type": "timeout", "token": ACCESS_TOKEN}}), encoding="utf-8")
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    (frames_dir / "20260518T190000Z.jpg").write_bytes(b"not image bytes; filename-only confidence scan")
    assert append_decision_memory_record(
        decision_memory_path(tmp_path),
        make_decision_memory_record(
            "confidence_dip",
            observed_at="2026-05-18T19:01:00Z",
            spot_id="left_spot",
            summary="weak contrast near bumper",
            details={"token": ACCESS_TOKEN, "rtsp_url": "rtsp://user:pass@example/camera"},
        ),
    )
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$confidence", sender="@op:example", room_id=ROOM_ID, body="!parking confidence"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("confidence replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("confidence replies must not send media")

    context = MatrixOperatorCockpitContext(settings=settings, data_dir=tmp_path, health_path=health_path, state_path=state_path)
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_context=context,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert service.archive.calls == []
    assert state_path.read_text(encoding="utf-8") == state_before
    assert state_path.stat().st_mtime_ns == state_mtime_before_ns
    assert [call["kind"] for call in calls] == ["text"]
    body = calls[0]["body"]
    assert calls[0]["txn_id"] == "command:$confidence"
    assert "Parking confidence report" in body
    assert "Spot stability:" in body
    assert "Weak evidence:" in body
    assert "confidence_dip: weak contrast near bumper" in body
    assert "Timeline health:" in body
    assert "Matrix delivery:" in body
    assert "last Matrix error: timeout" in body
    assert "Read-only: no detector, camera, media upload, alert emission, or state mutation was run." in body
    assert len(body.encode("utf-8")) <= 4096
    assert ACCESS_TOKEN not in body
    assert "rtsp://" not in body
    assert str(tmp_path) not in body



def test_command_service_analytics_windows_route_to_cockpit_provider_text_only_without_archive_corrections() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []
    provider_calls: list[tuple[str, str | None]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$analytics-default", sender="@op:example", room_id=ROOM_ID, body="!parking analytics"),
                    MatrixTextEvent(event_id="$analytics-today", sender="@op:example", room_id=ROOM_ID, body="!parking analytics today"),
                    MatrixTextEvent(event_id="$analytics-7d", sender="@op:example", room_id=ROOM_ID, body="!parking analytics 7d"),
                    MatrixTextEvent(event_id="$analytics-30d", sender="@op:example", room_id=ROOM_ID, body="!parking analytics 30d"),
                    MatrixTextEvent(event_id="$analytics-all", sender="@op:example", room_id=ROOM_ID, body="!parking analytics all"),
                    MatrixTextEvent(event_id="$analytics-bad", sender="@op:example", room_id=ROOM_ID, body="!parking analytics today extra"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("analytics replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("analytics replies must not send media")

    def cockpit_provider(action: str, *, analytics_window: str | None = None) -> str:
        provider_calls.append((action, analytics_window))
        return f"analytics reply for {analytics_window}"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$analytics-default"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 5
    assert result.error_count == 1
    assert provider_calls == [("analytics", "7d"), ("analytics", "today"), ("analytics", "7d"), ("analytics", "30d"), ("analytics", "all")]
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text", "text", "text", "text", "text", "text"]
    assert [call["txn_id"] for call in calls] == [
        "command:$analytics-default",
        "command:$analytics-today",
        "command:$analytics-7d",
        "command:$analytics-30d",
        "command:$analytics-all",
        "command:$analytics-bad",
    ]
    assert [call["body"] for call in calls[:5]] == [
        "analytics reply for 7d",
        "analytics reply for today",
        "analytics reply for 7d",
        "analytics reply for 30d",
        "analytics reply for all",
    ]
    assert calls[5]["body"] == "Command rejected: usage: !parking analytics [today|7d|30d|all]"


def test_command_service_rejects_unauthorized_analytics_before_provider_or_artifacts() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$analytics", sender="@intruder:example", room_id=ROOM_ID, body="!parking analytics all"),))

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("unauthorized analytics replies must not upload media")

    def cockpit_provider(action: str, **kwargs: Any) -> str:
        raise AssertionError("unauthorized analytics must not touch cockpit provider or artifact paths")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == ["Command rejected: sender is not authorized."]


def test_command_service_parse_errors_use_configured_command_prefix() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$analytics", sender="@op:example", room_id=ROOM_ID, body=".park analytics tomorrow"),
                    MatrixTextEvent(event_id="$confidence", sender="@op:example", room_id=ROOM_ID, body=".park confidence now"),
                    MatrixTextEvent(event_id="$learn", sender="@op:example", room_id=ROOM_ID, body=".park learn left_spot open 2026-05-18T19:00:00Z"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(str(kwargs["body"]))
            return "$reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        command_prefix=".park",
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 3
    assert replies == [
        "Command rejected: usage: .park analytics [today|7d|30d|all]",
        "Command rejected: usage: .park confidence",
        "Command rejected: usage: .park learn <spot_id> <open|occupied> at <time>",
    ]


def test_command_service_analytics_context_reads_vehicle_history_text_only_and_does_not_mutate_archive(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent

    closed_dir = tmp_path / "vehicle-history" / "sessions" / "closed"
    active_dir = tmp_path / "vehicle-history" / "sessions" / "active"
    closed_dir.mkdir(parents=True)
    active_dir.mkdir(parents=True)
    closed_session = closed_dir / "sess_closed.json"
    active_session = active_dir / "sess_active.json"
    malformed = closed_dir / "malformed.json"
    closed_session.write_text(
        json.dumps(
            {
                "session_id": "sess_closed",
                "spot_id": "left_spot",
                "started_at": "2026-05-18T08:00:00Z",
                "ended_at": "2026-05-18T09:30:00Z",
                "duration_seconds": 5400,
            }
        ),
        encoding="utf-8",
    )
    active_session.write_text(
        json.dumps({"session_id": "sess_active", "spot_id": "right_spot", "started_at": "2026-05-19T10:00:00Z", "ended_at": None}),
        encoding="utf-8",
    )
    malformed.write_text("{not json", encoding="utf-8")
    before = {path: (path.read_text(encoding="utf-8"), path.stat().st_mtime_ns) for path in [closed_session, active_session, malformed]}
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$analytics", sender="@op:example", room_id=ROOM_ID, body="!parking analytics all"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("analytics context replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("analytics context replies must not send media")

    context = MatrixOperatorCockpitContext(
        settings=object(),
        data_dir=tmp_path,
        health_path=tmp_path / "health.json",
        state_path=tmp_path / "state.json",
    )
    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_context=context,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text"]
    assert calls[0]["txn_id"] == "command:$analytics"
    body = calls[0]["body"]
    assert "Parking occupancy analytics" in body
    assert "Window: all" in body
    assert "left_spot\n- Sessions: 1" in body
    assert "\n\nright_spot\n- Sessions: 1" in body
    assert "malformed vehicle-history session ignored" in body
    assert "No detector, camera, Matrix media upload, alert emission, or state mutation was run." in body
    assert len(body.encode("utf-8")) <= 4096
    assert ACCESS_TOKEN not in body
    assert str(tmp_path) not in body
    for path, (content, mtime_ns) in before.items():
        assert path.read_text(encoding="utf-8") == content
        assert path.stat().st_mtime_ns == mtime_ns


def test_command_service_rejects_unauthorized_confidence_before_provider_or_artifacts() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$confidence", sender="@intruder:example", room_id=ROOM_ID, body="!parking confidence"),))

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("unauthorized confidence replies must not upload media")

    def cockpit_provider(action: str, **kwargs: Any) -> str:
        raise AssertionError("unauthorized confidence must not touch cockpit provider or artifact paths")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == ["Command rejected: sender is not authorized."]


def test_command_service_why_explain_recent_context_reads_decision_memory_safely_text_only(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "accepted_evidence",
            observed_at="2026-05-18T19:00:00Z",
            spot_id="right_spot",
            summary="accepted parked vehicle evidence",
            details={"hit_streak": 4, "token": ACCESS_TOKEN, "rtsp_url": "rtsp://user:pass@example/camera"},
        ),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record("command_outcome", observed_at="2026-05-18T19:01:00Z", summary="command processed", details={"outcome": "ok"}),
    )
    state_path = tmp_path / "state.json"
    state_path.write_text('{"schema_version":"test","state_by_spot":{}}', encoding="utf-8")
    state_before = state_path.read_text(encoding="utf-8")
    state_mtime_before_ns = state_path.stat().st_mtime_ns
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$why", sender="@op:example", room_id=ROOM_ID, body="!parking why right_spot"),
                    MatrixTextEvent(event_id="$explain", sender="@op:example", room_id=ROOM_ID, body="!parking explain right_spot"),
                    MatrixTextEvent(event_id="$unknown", sender="@op:example", room_id=ROOM_ID, body="!parking explain unknown_spot"),
                    MatrixTextEvent(event_id="$recent", sender="@op:example", room_id=ROOM_ID, body="!parking recent"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("why/recent context replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("why/recent context replies must not send media")

    context = MatrixOperatorCockpitContext(
        settings=object(),
        data_dir=tmp_path,
        health_path=tmp_path / "health.json",
        state_path=state_path,
    )
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_context=context,
    )

    result = service.poll_once()
    rendered = "\n".join(call["body"] for call in calls)

    assert result.processed_count == 4
    assert result.error_count == 0
    assert service.archive.calls == []
    assert state_path.read_text(encoding="utf-8") == state_before
    assert state_path.stat().st_mtime_ns == state_mtime_before_ns
    assert "Parking decision memory for right_spot" in rendered
    assert "accepted parked vehicle evidence" in rendered
    assert "hit_streak: 4" in rendered
    assert "Parking decision memory for unknown_spot" in rendered
    assert "No recent decision memory for this spot" in rendered
    assert "Parking decision memory recent" in rendered
    assert "command_outcome" in rendered
    assert ACCESS_TOKEN not in rendered
    assert "rtsp://" not in rendered
    assert all(call["kind"] == "text" for call in calls)
    assert all(len(call["body"].encode("utf-8")) <= 4096 for call in calls)


def test_command_service_recent_missing_context_is_safe_configuration_failure() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$recent", sender="@op:example", room_id=ROOM_ID, body="!parking recent"),))

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == ["Command failed: RuntimeError"]


def test_parse_matrix_lab_commands_are_exact_and_reject_untrusted_arguments() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    replay = parse_matrix_command("  !parking   lab   run   replay  ")
    tuning = parse_matrix_command("!parking lab run tuning")
    status = parse_matrix_command("!parking lab status")
    latest = parse_matrix_command("!parking lab status latest")
    specific = parse_matrix_command("!parking lab status lab-20260518T190000Z-abcdef12")

    assert (replay.action, replay.lab_kind) == ("lab_run", "replay")
    assert (tuning.action, tuning.lab_kind) == ("lab_run", "tuning")
    assert (status.action, status.lab_job_id) == ("lab_status", "latest")
    assert (latest.action, latest.lab_job_id) == ("lab_status", "latest")
    assert (specific.action, specific.lab_job_id) == ("lab_status", "lab-20260518T190000Z-abcdef12")

    rejected = [
        "!parking lab",
        "!parking lab run",
        "!parking lab run replay now",
        "!parking lab run /tmp/replay",
        "!parking lab run ../replay",
        "!parking lab run unknown",
        "!parking lab status latest extra",
        "!parking lab status ../status.json",
        "!parking lab status /tmp/status.json",
        "!parking lab status lab-20260518T190000Z-ABCDEF12",
        "!parking lab status lab-20260518T190000Z-abc",
        "!parking lab status " + "x" * 600,
    ]
    for body in rejected:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_command_service_lab_commands_use_provider_text_only_repeatably_without_archive_correction() -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []
    provider_calls: list[tuple[str, str | None, str | None]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$lab", sender="@op:example", room_id=ROOM_ID, body="!parking lab run replay"),
                    MatrixTextEvent(event_id="$lab", sender="@op:example", room_id=ROOM_ID, body="!parking lab run replay"),
                    MatrixTextEvent(event_id="$status", sender="@op:example", room_id=ROOM_ID, body="!parking lab status lab-20260518T190000Z-abcdef12"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("lab command replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("lab command replies must not send media")

    def cockpit_provider(action: str, *, lab_kind: str | None = None, lab_job_id: str | None = None) -> MatrixCommandResponse:
        provider_calls.append((action, lab_kind, lab_job_id))
        if action == "lab_run":
            return MatrixCommandResponse(text=f"Detection lab job started\nKind: {lab_kind}\nJob: lab-20260518T190000Z-abcdef12")
        return MatrixCommandResponse(text=f"Detection lab status\nJob: {lab_job_id}\nStatus: succeeded")

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$lab"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 3
    assert result.error_count == 0
    assert provider_calls == [
        ("lab_run", "replay", None),
        ("lab_run", "replay", None),
        ("lab_status", None, "lab-20260518T190000Z-abcdef12"),
    ]
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text", "text", "text"]
    assert [call["txn_id"] for call in calls] == ["command:$lab", "command:$lab", "command:$status"]
    assert all("Detection lab" in call["body"] for call in calls)


def test_command_service_rejects_unauthorized_lab_before_provider_or_paths() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$run", sender="@intruder:example", room_id=ROOM_ID, body="!parking lab run replay"),
                    MatrixTextEvent(event_id="$status", sender="@intruder:example", room_id=ROOM_ID, body="!parking lab status latest"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("unauthorized lab replies must not upload media")

    def cockpit_provider(action: str, **kwargs: Any) -> str:
        raise AssertionError("unauthorized lab command must not touch provider or lab paths")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 2
    assert replies == ["Command rejected: sender is not authorized.", "Command rejected: sender is not authorized."]


def test_command_service_lab_context_routes_to_manager_safely_text_only(tmp_path: Path) -> None:
    from parking_spot_monitor.detection_lab import REPLAY_CONFIG_FILENAME, REPLAY_LABELS_FILENAME
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent

    lab_root = tmp_path / "detection-lab"
    lab_root.mkdir()
    (lab_root / REPLAY_LABELS_FILENAME).write_text("{}", encoding="utf-8")
    (lab_root / REPLAY_CONFIG_FILENAME).write_text("{}", encoding="utf-8")
    calls: list[dict[str, Any]] = []

    def replay_runner(inputs: dict[str, Path]) -> dict[str, Any]:
        report = inputs["job_dir"] / "replay-report.json"
        report.write_text(
            json.dumps(
                {
                    "schema_version": "test.v1",
                    "status_counts": {"passed": 2, "failed": 1},
                    "coverage": {"assessed_frames": 3, "blocked_frames": 0, "not_assessed_frames": 0},
                    "redaction_scan": {"passed": True, "findings": []},
                    "token": ACCESS_TOKEN,
                }
            ),
            encoding="utf-8",
        )
        return report

    from parking_spot_monitor.detection_lab import DetectionLabManager

    manager = DetectionLabManager(tmp_path, replay_runner=replay_runner)

    class Client:
        poll = 0

        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            self.poll += 1
            if self.poll == 1:
                return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$run", sender="@op:example", room_id=ROOM_ID, body="!parking lab run replay"),))
            return MatrixSyncResult(next_batch="s4", events=(MatrixTextEvent(event_id="$status", sender="@op:example", room_id=ROOM_ID, body="!parking lab status latest"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("lab context replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("lab context replies must not send media")

    context = MatrixOperatorCockpitContext(
        settings=object(),
        data_dir=tmp_path,
        health_path=tmp_path / "health.json",
        state_path=tmp_path / "state.json",
        detection_lab_manager=manager,
    )
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_context=context,
    )

    first = service.poll_once()
    import time

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        status_text = context.lab_status_reply("latest").text
        if "Detection lab status\n" in status_text and "Status: succeeded" in status_text:
            break
        time.sleep(0.01)
    second = service.poll_once()
    rendered = "\n".join(call["body"] for call in calls)

    assert first.processed_count == 1
    assert second.processed_count == 1
    assert "Detection lab job started" in rendered
    assert "Detection lab status" in rendered
    assert "passed=2" in rendered
    assert "coverage: assessed 3" in rendered
    assert ACCESS_TOKEN not in rendered
    assert all(call["kind"] == "text" for call in calls)
    assert all(len(call["body"].encode("utf-8")) <= 4096 for call in calls)



@pytest.mark.parametrize(
    "body",
    [
        "!parking status extra",
        "!parking config verbose",
        "!parking latest ../debug_latest.jpg",
        "!parking why ../state.json",
        "!parking explain ../state.json",
        "!parking explain right_spot extra",
        "!parking recent now",
        "!parking confidence now",
        "!parking lab run replay; rm -rf /",
        "!parking lab status ../../status.json",
        "!parking profile summary prof_a extra",
        "!parking profile merge prof_a prof_a",
        "!parking owner ../left_spot",
        "!parking who now",
        "!parking help please",
    ],
)
def test_command_service_malformed_authorized_commands_fail_closed_before_provider_or_archive(body: str) -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$bad", sender="@op:example", room_id=ROOM_ID, body=body),))

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("malformed commands must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("malformed commands must not send media")

    def cockpit_provider(action: str, **kwargs: Any) -> str:
        raise AssertionError("malformed commands must not reach cockpit provider")

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert archive.calls == []
    assert len(replies) == 1
    assert replies[0].startswith("Command rejected: ")
    assert ACCESS_TOKEN not in replies[0]


def test_command_service_latest_media_delivery_failure_is_sanitized_text_failure(tmp_path: Path) -> None:
    from io import StringIO

    from parking_spot_monitor.logging import StructuredLogger
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixError, MatrixSyncResult, MatrixTextEvent

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(latest_path, size=(9, 5))
    log_stream = StringIO()
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$latest", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", "filename": kwargs["filename"], "data_len": len(kwargs["data"])})
            raise MatrixError("upload failed", error_type="http_status", status_code=500, access_token=ACCESS_TOKEN, response_body="raw body " + ACCESS_TOKEN)

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("image event must not be sent after upload failure")

    def cockpit_provider(action: str) -> MatrixCommandResponse:
        assert action == "latest"
        return MatrixCommandResponse(
            text="Parking monitor latest\nSnapshot: fresh raw latest.jpg; 9x5",
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 9, "h": 5},
        )

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        cockpit_provider=cockpit_provider,
        logger=StructuredLogger(stream=log_stream),
    )

    result = service.poll_once()
    rendered = json.dumps(calls) + log_stream.getvalue()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert [call["kind"] for call in calls] == ["text", "upload", "text"]
    assert calls[0]["txn_id"] == "command:$latest:text"
    assert calls[2] == {"kind": "text", "room_id": ROOM_ID, "txn_id": "command:$latest", "body": "Command failed: MatrixError"}
    assert ACCESS_TOKEN not in rendered
    assert "raw body" not in rendered
    assert raw_bytes.hex() not in rendered


def test_default_matrix_command_service_factory_wires_resolved_outbox_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.__main__ import _default_matrix_command_service_factory
    from parking_spot_monitor.config import load_settings
    from parking_spot_monitor.paths import resolve_runtime_paths

    config_path = tmp_path / "config.yaml"
    config_text = Path("config.yaml.example").read_text(encoding="utf-8").replace(
        "command_authorized_senders: []",
        "command_authorized_senders: ['@op:example']",
    )
    config_path.write_text(config_text, encoding="utf-8")
    settings = load_settings(config_path, environ={"RTSP_URL": "rtsp://operator:secret@camera/live", "MATRIX_ACCESS_TOKEN": ACCESS_TOKEN})
    paths = resolve_runtime_paths(settings, tmp_path)

    service = _default_matrix_command_service_factory(settings, tmp_path, logger=None, archive=FakeCommandArchive())  # type: ignore[arg-type]

    assert service is not None
    assert service.cockpit_context is not None
    assert service.cockpit_context.matrix_outbox_path == paths.matrix_outbox_file
