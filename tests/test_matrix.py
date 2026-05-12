from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
from PIL import Image

from parking_spot_monitor.matrix import (
    OCCUPIED_SPOT_EVENT_TYPE,
    MatrixClient,
    MatrixDelivery,
    MatrixError,
    format_live_proof_text,
    format_occupied_spot_alert,
    format_open_spot_alert,
    format_quiet_window_notice,
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
    assert seen[0]["body"] == "LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at 2026-05-18T12:00:00-07:00 (decode mode: software)."
    assert seen[1]["content_type"] == "image/jpeg"
    assert seen[1]["data"] == raw_bytes
    assert seen[2]["txn_id"] == "live-proof:2026-05-18T19:00:00Z:image"
    assert seen[2]["body"].startswith("LIVE PROOF / TEST IMAGE: raw full-frame camera snapshot")
    assert seen[2]["info"] == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 8, "h": 6}


def test_format_live_proof_text_is_visibly_labelled() -> None:
    assert format_live_proof_text(observed_at="2026-05-18T19:00:00Z", selected_mode="software") == (
        "LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at 2026-05-18T12:00:00-07:00 (decode mode: software)."
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
        "Parking spot occupied: left_spot at 2026-05-18T13:01:02-07:00\n"
        "Likely vehicle: silver hatchback (profile prof_repeat)\n"
        "Match: matched, confidence 0.92\n"
        "Estimated dwell: 1 hr–1 hr 30 min (typical 1 hr 15 min)\n"
        "Usual leave window: 23:45–00:15 (typical 00:00; crosses midnight)\n"
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

    assert format_occupied_spot_alert(event) == (
        "Parking spot occupied: left_spot at 2026-05-18T13:01:02-07:00\n"
        "Likely vehicle: unknown vehicle (profile prof_repeat)\n"
        "Match: new_profile, confidence unknown\n"
        "Estimate unavailable: insufficient-samples\n"
        "History: 1 sample, estimate confidence low"
    )


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

    delivery.send_occupied_spot_alert(occupied_event(source))

    event_id = "occupancy-occupied-event:left_spot:2026-05-18T20:01:02Z"
    assert [item["kind"] for item in seen] == ["text", "upload", "image"]
    assert seen[0]["txn_id"] == f"{event_id}:text"
    assert "Parking spot occupied: left_spot" in seen[0]["body"]
    assert seen[1]["content_type"] == "image/jpeg"
    assert seen[1]["data"] == raw_bytes
    assert seen[1]["filename"] == "occupancy-occupied-event-left-spot-2026-05-18t20-01-02z.jpg"
    assert seen[2]["txn_id"] == f"{event_id}:image"
    assert seen[2]["body"] == "Raw occupied full-frame snapshot for left_spot at 2026-05-18T13:01:02-07:00"
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
    ) == "Parking spot open: left_spot at 2026-05-18T13:01:02-07:00"


def test_format_open_spot_alert_displays_iso_string_in_los_angeles_time() -> None:
    assert format_open_spot_alert({"spot_id": "right_spot", "observed_at": "2026-05-12T16:04:08.223073+00:00"}) == (
        "Parking spot open: right_spot at 2026-05-12T09:04:08.223073-07:00"
    )


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
    summary = parse_matrix_command("!parking profile summary prof_target")
    assert (summary.action, summary.profile_id) == ("profile_summary", "prof_target")

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

    def load_active_sessions(self) -> list[FakeSession]:
        return self.sessions

    def list_closed_sessions(self) -> list[FakeSession]:
        return []


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
                    MatrixTextEvent(event_id="$summary", sender="@op:example", room_id=ROOM_ID, body="!parking profile summary prof_b"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@op:example"], bot_user_id="@bot:example")  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.processed_count == 4
    assert result.error_count == 1
    assert [call[0] for call in archive.calls] == ["rename_profile", "merge_profiles", "mark_wrong_match", "profile_summary"]
    assert archive.calls[0][1] == ("prof_a", "Blue hatchback")
    assert archive.calls[0][2]["matrix_event_id"] == "$rename"
    assert archive.calls[2][1] == ("sess_current",)
    assert archive.cursor_writes[-1] == {"next_batch": "s3"}
    assert len(replies) == 5
    rendered_replies = "\n".join(reply["body"] for reply in replies)
    assert "not authorized" in rendered_replies
    assert "Profile prof_b: Blue hatchback" in rendered_replies
    assert ACCESS_TOKEN not in rendered_replies


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
