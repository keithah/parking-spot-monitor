from __future__ import annotations

import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery
from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix import MatrixError

ROOM_ID = "!parking-room:example.org"
EVENT_ID = "occupancy-open-event:left_spot:2026-05-18T20:01:02Z"


def write_jpeg(path: Path, *, size: tuple[int, int] = (8, 6), color: tuple[int, int, int] = (25, 50, 75)) -> bytes:
    image = Image.new("RGB", size, color=color)
    image.save(path, format="JPEG")
    return path.read_bytes()


def open_event(snapshot_path: Path) -> dict[str, Any]:
    return {
        "event_type": "occupancy-open-event",
        "spot_id": "left_spot",
        "previous_status": "occupied",
        "new_status": "empty",
        "observed_at": datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc),
        "snapshot_path": str(snapshot_path),
    }


def occupied_event(snapshot_path: Path) -> dict[str, Any]:
    return {
        "event_type": "occupancy-occupied-event",
        "spot_id": "left_spot",
        "previous_status": "empty",
        "new_status": "occupied",
        "observed_at": datetime(2026, 5, 20, 21, 22, 54, tzinfo=timezone.utc),
        "source_timestamp": "2026-05-20T21:22:54Z",
        "event_id": "occupancy-state-changed:left_spot:2026-05-20T21:22:54Z",
        "session_id": "sess_left-spot_2026-05-20t21-22-54-187227-00-00",
        "occupied_snapshot_path": str(snapshot_path),
        "likely_vehicle": {"label": "unknown vehicle"},
        "vehicle_history_estimate": {"status": "insufficient_history", "sample_count": 0},
    }


class FakeMatrixClient:
    def __init__(self, *, fail: dict[str, Exception] | None = None, on_send_text: Any | None = None) -> None:
        self.fail = fail or {}
        self.on_send_text = on_send_text
        self.calls: list[dict[str, Any]] = []

    def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
        if self.on_send_text is not None:
            self.on_send_text()
        self.calls.append({"kind": "text", "room_id": room_id, "txn_id": txn_id, "body": body})
        if "text" in self.fail:
            raise self.fail["text"]
        return "$text:example.org"

    def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
        self.calls.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
        if "upload" in self.fail:
            raise self.fail["upload"]
        return "mxc://example.org/open"

    def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
        self.calls.append(
            {
                "kind": "image",
                "room_id": room_id,
                "txn_id": txn_id,
                "body": body,
                "content_uri": content_uri,
                "info": dict(info),
            }
        )
        if "image" in self.fail:
            raise self.fail["image"]
        return "$image:example.org"


def make_delivery(
    tmp_path: Path,
    client: FakeMatrixClient,
    *,
    stream: StringIO | None = None,
    snapshot_retention_count: int = 50,
) -> MatrixOutboxDelivery:
    return MatrixOutboxDelivery(
        client=client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
        logger=StructuredLogger(stream=stream) if stream is not None else None,
        snapshot_retention_count=snapshot_retention_count,
    )


def test_occupied_alert_sends_immediately_without_open_outbox_record(tmp_path: Path) -> None:
    source = tmp_path / "occupied.jpg"
    source_bytes = write_jpeg(source, color=(110, 25, 80))
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)

    snapshot = delivery.send_occupied_spot_alert(occupied_event(source))

    assert [call["kind"] for call in client.calls] == ["text", "upload", "image"]
    assert client.calls[0]["txn_id"].startswith("occupancy-occupied-event:left_spot:")
    assert client.calls[0]["txn_id"].endswith(":text")
    assert client.calls[1]["filename"].startswith("occupancy-occupied-event-left-spot-")
    assert client.calls[1]["data"] == source_bytes
    assert client.calls[1]["content_type"] == "image/jpeg"
    assert client.calls[2]["txn_id"].startswith("occupancy-occupied-event:left_spot:")
    assert client.calls[2]["txn_id"].endswith(":image")
    assert snapshot.path.exists()
    assert snapshot.path.name.startswith("occupancy-occupied-event-left-spot-")
    assert LocalOutbox(tmp_path / "matrix-outbox.json").list_records() == []


def test_text_failure_persists_three_phase_record_before_network_and_leaves_text_pending(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"

    def assert_enqueued_before_network() -> None:
        persisted = LocalOutbox(store_path).list_records()
        assert len(persisted) == 1
        assert persisted[0].phase_states == {"text": "pending", "upload": "pending", "image": "pending"}

    client = FakeMatrixClient(
        fail={"text": MatrixError("timeout access_token=secret", error_type="timeout")},
        on_send_text=assert_enqueued_before_network,
    )
    delivery = make_delivery(tmp_path, client)

    result = delivery.send_open_spot_alert(open_event(source))

    assert result.retrying_count == 1
    [record] = LocalOutbox(store_path).list_records()
    assert record.state == "retrying"
    assert record.retry_reason == "matrix_text_timeout"
    assert record.phase_states == {"text": "pending", "upload": "pending", "image": "pending"}
    assert [call["kind"] for call in client.calls] == ["text"]



def test_text_retry_uploads_retained_event_snapshot_not_changed_latest(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    original_bytes = write_jpeg(source, color=(25, 50, 75))
    store_path = tmp_path / "matrix-outbox.json"

    first_client = FakeMatrixClient(fail={"text": MatrixError("timeout", error_type="timeout")})
    make_delivery(tmp_path, first_client).send_open_spot_alert(open_event(source))

    [retrying] = LocalOutbox(store_path).list_records()
    retained_path = Path(str(retrying.intent.metadata["retained_snapshot_path"]))
    assert retained_path.exists()
    assert retained_path.read_bytes() == original_bytes

    changed_bytes = write_jpeg(source, color=(200, 10, 10))
    assert changed_bytes != original_bytes

    second_client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=second_client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
    )

    result = restarted.drain_outbox()

    assert result.delivered_count == 1
    uploads = [call for call in second_client.calls if call["kind"] == "upload"]
    assert len(uploads) == 1
    assert uploads[0]["data"] == original_bytes
    assert uploads[0]["data"] != changed_bytes


def test_occupied_alert_retention_preserves_retryable_open_outbox_evidence(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    first_bytes = write_jpeg(source, color=(25, 50, 75))
    store_path = tmp_path / "matrix-outbox.json"

    first_client = FakeMatrixClient(fail={"text": MatrixError("timeout", error_type="timeout")})
    make_delivery(tmp_path, first_client, snapshot_retention_count=1).send_open_spot_alert(open_event(source))
    [retrying] = LocalOutbox(store_path).list_records()
    protected_path = Path(str(retrying.intent.metadata["retained_snapshot_path"]))
    assert protected_path.exists()
    assert protected_path.read_bytes() == first_bytes

    occupied_source = tmp_path / "occupied.jpg"
    write_jpeg(occupied_source, color=(90, 20, 120))
    make_delivery(tmp_path, FakeMatrixClient(), snapshot_retention_count=1).send_occupied_spot_alert(occupied_event(occupied_source))

    assert protected_path.exists()
    assert protected_path.read_bytes() == first_bytes


def test_snapshot_retention_preserves_retryable_outbox_evidence_while_pruning_unprotected(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    first_bytes = write_jpeg(source, color=(25, 50, 75))
    store_path = tmp_path / "matrix-outbox.json"

    first_client = FakeMatrixClient(fail={"text": MatrixError("timeout", error_type="timeout")})
    make_delivery(tmp_path, first_client, snapshot_retention_count=1).send_open_spot_alert(open_event(source))
    [retrying] = LocalOutbox(store_path).list_records()
    protected_path = Path(str(retrying.intent.metadata["retained_snapshot_path"]))
    assert protected_path.exists()
    assert protected_path.read_bytes() == first_bytes

    unrelated_old = tmp_path / "snapshots" / "occupancy-open-event-left-spot-2026-05-18t20-02-00z.jpg"
    write_jpeg(source, color=(10, 200, 10))
    make_delivery(tmp_path, FakeMatrixClient(), snapshot_retention_count=1).send_open_spot_alert(
        open_event(source) | {"observed_at": datetime(2026, 5, 18, 20, 2, 0, tzinfo=timezone.utc)}
    )
    assert unrelated_old.exists()

    write_jpeg(source, color=(10, 10, 200))
    make_delivery(tmp_path, FakeMatrixClient(), snapshot_retention_count=1).send_open_spot_alert(
        open_event(source) | {"observed_at": datetime(2026, 5, 18, 20, 3, 0, tzinfo=timezone.utc)}
    )

    newest = tmp_path / "snapshots" / "occupancy-open-event-left-spot-2026-05-18t20-03-00z.jpg"
    assert protected_path.exists()
    assert protected_path.read_bytes() == first_bytes
    assert newest.exists()
    assert not unrelated_old.exists()

def test_upload_failure_after_text_leaves_text_delivered_and_upload_pending_across_restart(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    first_client = FakeMatrixClient(fail={"upload": MatrixError("upload failed bearer secret", error_type="timeout")})

    make_delivery(tmp_path, first_client).send_open_spot_alert(open_event(source))

    [failed] = LocalOutbox(store_path).list_records()
    assert failed.state == "retrying"
    assert failed.retry_reason == "matrix_upload_timeout"
    assert failed.phase_states == {"text": "delivered", "upload": "pending", "image": "pending"}
    assert failed.phase_results["text"] == {"matrix_event_id": "$text:example.org"}
    assert [call["kind"] for call in first_client.calls] == ["text", "upload"]

    second_client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=second_client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
    )

    result = restarted.drain_outbox()

    assert result.delivered_count == 1
    [delivered] = LocalOutbox(store_path).list_records()
    assert delivered.state == "delivered"
    assert delivered.phase_states == {"text": "delivered", "upload": "delivered", "image": "delivered"}
    assert [call["kind"] for call in second_client.calls] == ["upload", "image"]
    assert second_client.calls[0]["content_type"] == "image/jpeg"
    assert second_client.calls[1]["txn_id"] == f"{EVENT_ID}:image"


def test_image_failure_after_upload_stores_content_uri_and_restart_does_not_reupload(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    first_client = FakeMatrixClient(fail={"image": MatrixError("send failed", error_type="timeout")})

    make_delivery(tmp_path, first_client).send_open_spot_alert(open_event(source))

    [failed] = LocalOutbox(store_path).list_records()
    assert failed.state == "retrying"
    assert failed.retry_reason == "matrix_image_timeout"
    assert failed.phase_states == {"text": "delivered", "upload": "delivered", "image": "pending"}
    assert failed.phase_results["upload"]["content_uri"] == "mxc://example.org/open"
    assert failed.phase_results["upload"]["body"].startswith("Raw full-frame snapshot for left_spot")
    assert [call["kind"] for call in first_client.calls] == ["text", "upload", "image"]

    second_client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=second_client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
    )

    result = restarted.drain_outbox()

    assert result.delivered_count == 1
    assert [call["kind"] for call in second_client.calls] == ["image"]
    assert second_client.calls[0]["content_uri"] == "mxc://example.org/open"
    [delivered] = LocalOutbox(store_path).list_records()
    assert delivered.state == "delivered"


def test_delivered_records_and_phases_are_not_sent_again(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    client = FakeMatrixClient()
    make_delivery(tmp_path, client).send_open_spot_alert(open_event(source))
    assert [call["kind"] for call in client.calls] == ["text", "upload", "image"]

    restarted_client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=restarted_client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
    )

    result = restarted.drain_outbox()

    assert result.attempted_count == 0
    assert restarted_client.calls == []


def test_retry_logs_use_safe_reason_codes_and_redact_unsafe_diagnostics(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    stream = StringIO()
    client = FakeMatrixClient(
        fail={"text": MatrixError("Authorization: Bearer secret-token", error_type="timeout", access_token="secret-token")}
    )

    make_delivery(tmp_path, client, stream=stream).send_open_spot_alert(open_event(source))

    output = stream.getvalue()
    records = [json.loads(line) for line in output.splitlines()]
    events = [record["event"] for record in records]
    assert "matrix-outbox-enqueued" in events
    assert "matrix-outbox-drain-started" in events
    assert "matrix-outbox-phase-attempt" in events
    assert "matrix-outbox-phase-retryable-failure" in events
    assert any(record.get("reason") == "matrix_text_timeout" for record in records)
    assert "secret-token" not in output
    assert "Authorization" not in output
    assert "Bearer" not in output


def test_non_retryable_matrix_4xx_dead_letters_and_is_not_drained_after_restart(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    client = FakeMatrixClient(
        fail={
            "text": MatrixError(
                "Authorization: Bearer secret-token",
                error_type="http_status",
                status_code=401,
                errcode="M_FORBIDDEN",
            )
        }
    )

    result = make_delivery(tmp_path, client).send_open_spot_alert(open_event(source))

    assert result.attempted_count == 1
    assert result.delivered_count == 0
    assert result.retrying_count == 0
    [dead] = LocalOutbox(store_path).list_records()
    assert dead.state == "dead_lettered"
    assert dead.dead_letter_reason == "matrix_text_http_401"
    assert dead.retry_reason is None
    assert dead.phase_states == {"text": "failed", "upload": "pending", "image": "pending"}
    summary = LocalOutbox(store_path).status_summary()
    assert summary["counts_by_state"] == {"dead_lettered": 1}
    assert summary["dead_letter_reason_counts"] == {"matrix_text_http_401": 1}
    rendered = json.dumps(summary)
    assert "secret-token" not in rendered
    assert "Authorization" not in rendered
    assert "Bearer" not in rendered

    restarted_client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=restarted_client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
    )

    restart_result = restarted.drain_outbox()

    assert restart_result.attempted_count == 0
    assert restarted_client.calls == []


@pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
def test_retryable_matrix_statuses_remain_retrying(tmp_path: Path, status_code: int) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    client = FakeMatrixClient(
        fail={"text": MatrixError("temporary", error_type="http_status", status_code=status_code)}
    )

    result = make_delivery(tmp_path, client).send_open_spot_alert(open_event(source))

    assert result.retrying_count == 1
    [record] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert record.state == "retrying"
    assert record.retry_reason == f"matrix_text_http_{status_code}"
    assert record.dead_letter_reason is None


def test_malformed_persisted_upload_result_dead_letters_image_phase_safely(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    first_client = FakeMatrixClient(fail={"image": MatrixError("timeout", error_type="timeout")})
    make_delivery(tmp_path, first_client).send_open_spot_alert(open_event(source))
    payload = json.loads(store_path.read_text(encoding="utf-8"))
    for phase in payload["items"][0]["phases"]:
        if phase["phase"] == "upload":
            phase["result"] = {"content_uri": "mxc://example.org/open", "body": "ok"}
    store_path.write_text(json.dumps(payload), encoding="utf-8")

    restarted = MatrixOutboxDelivery(
        client=FakeMatrixClient(),
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
    )

    result = restarted.drain_outbox()

    assert result.retrying_count == 0
    [dead] = LocalOutbox(store_path).list_records()
    assert dead.state == "dead_lettered"
    assert dead.dead_letter_reason == "matrix_image_upload_result_malformed"
    assert dead.phase_states == {"text": "delivered", "upload": "delivered", "image": "failed"}


def test_missing_retained_snapshot_evidence_dead_letters_upload_without_raw_path_leak(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    first_client = FakeMatrixClient(fail={"text": MatrixError("timeout", error_type="timeout")})
    make_delivery(tmp_path, first_client).send_open_spot_alert(open_event(source))
    [retrying] = LocalOutbox(store_path).list_records()
    retained_path = Path(str(retrying.intent.metadata["retained_snapshot_path"]))
    retained_path.unlink()
    stream = StringIO()

    restarted = MatrixOutboxDelivery(
        client=FakeMatrixClient(),
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
        logger=StructuredLogger(stream=stream),
    )

    result = restarted.drain_outbox()

    assert result.retrying_count == 0
    [dead] = LocalOutbox(store_path).list_records()
    assert dead.state == "dead_lettered"
    assert dead.dead_letter_reason == "matrix_upload_snapshot_missing_source"
    assert dead.phase_states == {"text": "delivered", "upload": "failed", "image": "pending"}
    rendered_summary = json.dumps(LocalOutbox(store_path).status_summary())
    assert str(retained_path) not in rendered_summary
    assert str(retained_path) not in stream.getvalue()


def test_malformed_response_remains_retryable_not_dead_lettered(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    client = FakeMatrixClient(fail={"image": MatrixError("bad json", error_type="malformed_response")})

    result = make_delivery(tmp_path, client).send_open_spot_alert(open_event(source))

    assert result.retrying_count == 1
    [record] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert record.state == "retrying"
    assert record.retry_reason == "matrix_image_malformed_response"
    assert record.dead_letter_reason is None
    assert record.phase_states["image"] == "pending"
