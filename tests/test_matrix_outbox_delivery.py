from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery, MatrixOutboxDrainResult
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
    def __init__(
        self,
        *,
        fail: dict[str, Exception] | None = None,
        on_send_text: Any | None = None,
        on_send_image: Any | None = None,
    ) -> None:
        self.fail = fail or {}
        self.on_send_text = on_send_text
        self.on_send_image = on_send_image
        self.calls: list[dict[str, Any]] = []
        self.closed = False

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
        if self.on_send_image is not None:
            self.on_send_image()
        return "$image:example.org"

    def close(self) -> None:
        self.closed = True


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


def test_occupied_alert_queues_image_outbox_record_without_network(tmp_path: Path) -> None:
    source = tmp_path / "occupied.jpg"
    write_jpeg(source, color=(110, 25, 80))
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)

    record = delivery.send_occupied_spot_alert(occupied_event(source))

    assert client.calls == []
    assert record.phase_states == {"text": "pending", "upload": "pending", "image": "pending"}
    [persisted] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert persisted.id == record.id
    assert persisted.intent.event_id.startswith("occupancy-occupied-event:left_spot:")
    assert persisted.intent.body.startswith("Parking spot occupied: left_spot")
    assert persisted.intent.phase == "text"
    assert str(persisted.intent.metadata["event_type"]) == "occupancy-occupied-event"
    retained_path = Path(str(persisted.intent.metadata["retained_snapshot_path"]))
    assert retained_path.exists()
    assert retained_path.name.startswith("occupancy-occupied-event-left-spot-")


def test_occupied_alert_drains_as_single_image_message_with_alert_body(tmp_path: Path) -> None:
    source = tmp_path / "occupied.jpg"
    write_jpeg(source, color=(110, 25, 80))
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)
    event = occupied_event(source)

    delivery.enqueue_occupied_spot_alert(event)
    result = delivery.drain_outbox()

    assert result.delivered_count == 1
    assert [call["kind"] for call in client.calls] == ["text", "upload", "image"]
    assert client.calls[0]["body"] == "Parking spot occupied: left_spot at 2026-05-20 2:22:54 PM PDT"
    image_call = client.calls[2]
    assert image_call["body"].startswith("Raw full-frame snapshot for left_spot")
    [persisted] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert persisted.phase_states == {"text": "delivered", "upload": "delivered", "image": "delivered"}


def test_matrix_outbox_delivery_close_closes_owned_client(tmp_path: Path) -> None:
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)

    delivery.close()

    assert client.closed is True


def test_worker_is_singleton_wakes_on_enqueue_and_drains_one_record(tmp_path: Path) -> None:
    delivered = threading.Event()
    client = FakeMatrixClient(on_send_image=delivered.set)
    delivery = make_delivery(tmp_path, client)
    delivery.start_worker(retry_interval_seconds=60)
    worker = delivery.worker_thread

    delivery.start_worker(retry_interval_seconds=60)
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    delivery.enqueue_open_spot_alert(open_event(source))

    assert delivery.worker_thread is worker
    assert delivered.wait(2), "worker did not deliver the enqueued snapshot alert"
    assert [call["kind"] for call in client.calls] == ["text", "upload", "image"]
    health = delivery.outbox_health_summary()
    assert health["worker_running"] is True
    assert health["worker_last_attempt_at"] is not None
    delivery.close()
    assert worker.is_alive() is False


def test_worker_requests_at_most_one_record_per_drain_pass(tmp_path: Path) -> None:
    drained = threading.Event()

    class RecordingDelivery(MatrixOutboxDelivery):
        def __init__(self) -> None:
            super().__init__(
                client=FakeMatrixClient(),
                room_id=ROOM_ID,
                data_dir=tmp_path,
                snapshots_dir=tmp_path / "snapshots",
                outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
            )
            self.max_records: list[int | None] = []

        def drain_outbox(
            self,
            *,
            record_id: str | None = None,
            max_records: int | None = None,
        ) -> MatrixOutboxDrainResult:
            self.max_records.append(max_records)
            drained.set()
            return MatrixOutboxDrainResult(0, 0, 0)

    delivery = RecordingDelivery()
    try:
        delivery.start_worker(retry_interval_seconds=60)
        assert drained.wait(2), "worker did not perform its initial bounded drain"
        assert delivery.max_records == [1]
    finally:
        delivery.close()


def test_manual_drain_cannot_duplicate_a_worker_owned_phase(tmp_path: Path) -> None:
    first_send_entered = threading.Event()
    release_first_send = threading.Event()
    duplicate_send = threading.Event()

    class BlockingFirstSendClient(FakeMatrixClient):
        def __init__(self) -> None:
            super().__init__()
            self.send_count = 0
            self.send_lock = threading.Lock()

        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            with self.send_lock:
                self.send_count += 1
                send_count = self.send_count
            if send_count == 1:
                first_send_entered.set()
                assert release_first_send.wait(2)
            else:
                duplicate_send.set()
            return super().send_text(room_id=room_id, txn_id=txn_id, body=body)

    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    client = BlockingFirstSendClient()
    delivery = make_delivery(tmp_path, client)
    delivery.start_worker(retry_interval_seconds=60)
    record = delivery.enqueue_open_spot_alert(open_event(source))
    assert first_send_entered.wait(2)
    manual_finished = threading.Event()

    def drain_manually() -> None:
        delivery.drain_outbox(record_id=record.id)
        manual_finished.set()

    manual = threading.Thread(target=drain_manually, name="manual-outbox-drain")
    manual.start()
    try:
        assert not duplicate_send.wait(0.1), "manual drain duplicated the worker's in-flight text phase"
        release_first_send.set()
        assert manual_finished.wait(2)
        assert client.send_count == 1
    finally:
        release_first_send.set()
        manual.join(timeout=2)
        delivery.close()


def test_idle_worker_waits_without_polling_the_outbox_filesystem(tmp_path: Path) -> None:
    first_read = threading.Event()
    repeated_read = threading.Event()

    class CountingOutbox(LocalOutbox):
        def __init__(self, path: Path) -> None:
            super().__init__(path)
            self.list_calls = 0

        def list_records(self, state: Any | None = None) -> list[Any]:
            self.list_calls += 1
            (first_read if self.list_calls == 1 else repeated_read).set()
            return super().list_records(state)

    delivery = MatrixOutboxDelivery(
        client=FakeMatrixClient(),
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=CountingOutbox(tmp_path / "matrix-outbox.json"),
    )
    try:
        delivery.start_worker(retry_interval_seconds=60)
        assert first_read.wait(2), "worker did not inspect durable startup work"
        assert not repeated_read.wait(0.1), "idle worker polled the outbox instead of waiting"
    finally:
        delivery.close()


def test_retryable_failure_waits_before_worker_retries(tmp_path: Path) -> None:
    first_attempt = threading.Event()
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    client = FakeMatrixClient(
        fail={"text": MatrixError("timeout token=never-report", error_type="timeout")},
        on_send_text=first_attempt.set,
    )
    delivery = make_delivery(tmp_path, client)
    try:
        delivery.start_worker(retry_interval_seconds=3_600)
        delivery.enqueue_open_spot_alert(open_event(source))
        assert first_attempt.wait(2), "worker did not make its first Matrix attempt"
        assert not threading.Event().wait(0.1)
        assert [call["kind"] for call in client.calls].count("text") == 1
    finally:
        delivery.close()


def test_worker_survives_unexpected_drain_failure_and_health_redacts_error_details(tmp_path: Path) -> None:
    recovered = threading.Event()

    class FlakyDelivery(MatrixOutboxDelivery):
        def __init__(self) -> None:
            super().__init__(
                client=FakeMatrixClient(),
                room_id=ROOM_ID,
                data_dir=tmp_path,
                snapshots_dir=tmp_path / "snapshots",
                outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
            )
            self.drain_calls = 0

        def drain_outbox(
            self,
            *,
            record_id: str | None = None,
            max_records: int | None = None,
        ) -> MatrixOutboxDrainResult:
            self.drain_calls += 1
            if self.drain_calls == 1:
                raise RuntimeError("Authorization: Bearer worker-secret")
            recovered.set()
            return MatrixOutboxDrainResult(0, 0, 0)

    delivery = FlakyDelivery()
    try:
        delivery.start_worker(retry_interval_seconds=0.01)
        assert recovered.wait(2), "worker died after an unexpected drain failure"
        health = delivery.outbox_health_summary()
        assert health["worker_running"] is True
        assert health["worker_last_error_type"] == "RuntimeError"
        assert "worker-secret" not in json.dumps(health)
        assert "Authorization" not in json.dumps(health)
        assert "Bearer" not in json.dumps(health)
    finally:
        delivery.close()


def test_worker_survives_logger_shutdown_during_bounded_close(tmp_path: Path) -> None:
    delivered = threading.Event()

    class ClosedLogger:
        def info(self, _event: str, **_fields: Any) -> None:
            raise ValueError("I/O operation on closed file")

        warning = info

    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    delivery = make_delivery(tmp_path, FakeMatrixClient(on_send_image=delivered.set))
    delivery.enqueue_open_spot_alert(open_event(source))
    delivery.logger = ClosedLogger()  # type: ignore[assignment]
    try:
        delivery.start_worker(retry_interval_seconds=0.01)
        assert delivered.wait(2), "closed logging sink killed the delivery worker"
        assert delivery.worker_thread is not None
        assert delivery.worker_thread.is_alive() is True
    finally:
        delivery.close()


def test_close_bounds_worker_join_before_closing_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import parking_monitor.matrix_outbox_delivery as delivery_module

    entered_send = threading.Event()
    release_send = threading.Event()

    class BlockingClient(FakeMatrixClient):
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            entered_send.set()
            if not release_send.wait(2):
                raise AssertionError("test did not release blocked Matrix call")
            return super().send_text(room_id=room_id, txn_id=txn_id, body=body)

        def close(self) -> None:
            super().close()
            release_send.set()

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            if self.closed:
                raise AssertionError("worker performed upload after client close")
            return super().upload_image(filename=filename, data=data, content_type=content_type)

    monkeypatch.setattr(delivery_module, "_WORKER_JOIN_TIMEOUT_SECONDS", 0.05)
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    client = BlockingClient()
    delivery = make_delivery(tmp_path, client)
    delivery.start_worker(retry_interval_seconds=60)
    delivery.enqueue_open_spot_alert(open_event(source))
    assert entered_send.wait(2), "worker did not enter the blocking Matrix call"

    started = time.monotonic()
    delivery.close()
    elapsed = time.monotonic() - started

    assert elapsed < 1
    assert client.closed is True
    assert delivery.worker_thread is not None
    delivery.worker_thread.join(timeout=2)
    assert delivery.worker_thread.is_alive() is False


def test_enqueue_text_notice_is_durable_and_preserves_text_only_transaction_id(tmp_path: Path) -> None:
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)
    event = {
        "event_type": "quiet-window-started",
        "event_id": "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00",
        "window_id": "street_sweeping:2026-05-18:13:00-15:00",
    }

    record = delivery.enqueue_text_notice("quiet-window-started", event)

    assert client.calls == []
    assert record.phase_states == {"text": "pending"}
    delivery.drain_outbox()
    assert client.calls == [
        {
            "kind": "text",
            "room_id": ROOM_ID,
            "txn_id": event["event_id"],
            "body": "Street sweeping started: street_sweeping:2026-05-18:13:00-15:00",
        }
    ]


def test_open_alert_queues_image_outbox_record_without_network(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)

    record = delivery.enqueue_open_spot_alert(open_event(source))

    assert client.calls == []
    assert record.phase_states == {"text": "pending", "upload": "pending", "image": "pending"}
    assert record.intent.body == "Parking spot open: left_spot at 2026-05-18 1:01:02 PM PDT"
    assert record.intent.phase == "text"



def test_text_retry_uploads_retained_event_snapshot_not_changed_latest(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    original_bytes = write_jpeg(source, color=(25, 50, 75))
    store_path = tmp_path / "matrix-outbox.json"

    first_client = FakeMatrixClient(fail={"upload": MatrixError("timeout", error_type="timeout")})
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

    first_client = FakeMatrixClient(fail={"upload": MatrixError("timeout", error_type="timeout")})
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


def test_drain_outbox_respects_max_records_budget(tmp_path: Path) -> None:
    first = tmp_path / "first.jpg"
    second = tmp_path / "second.jpg"
    write_jpeg(first, color=(25, 50, 75))
    write_jpeg(second, color=(90, 20, 120))
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    delivery.enqueue_open_spot_alert(open_event(first))
    delivery.enqueue_occupied_spot_alert(occupied_event(second))

    drain_client = FakeMatrixClient()
    limited = MatrixOutboxDelivery(
        client=drain_client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
    )

    result = limited.drain_outbox(max_records=1)

    assert result.attempted_count == 1
    assert result.delivered_count == 1
    assert [call["kind"] for call in drain_client.calls] == ["text", "upload", "image"]
    records = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert [record.state for record in records] == ["delivered", "pending"]


def test_snapshot_retention_preserves_retryable_outbox_evidence_while_pruning_unprotected(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    first_bytes = write_jpeg(source, color=(25, 50, 75))
    store_path = tmp_path / "matrix-outbox.json"

    first_client = FakeMatrixClient(fail={"upload": MatrixError("timeout", error_type="timeout")})
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

def test_upload_failure_leaves_upload_pending_across_restart(tmp_path: Path) -> None:
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
        fail={"upload": MatrixError("Authorization: Bearer secret-token", error_type="timeout", access_token="secret-token")}
    )

    make_delivery(tmp_path, client, stream=stream).send_open_spot_alert(open_event(source))

    output = stream.getvalue()
    records = [json.loads(line) for line in output.splitlines()]
    events = [record["event"] for record in records]
    assert "matrix-outbox-enqueued" in events
    assert "matrix-outbox-drain-started" in events
    assert "matrix-outbox-phase-attempt" in events
    assert "matrix-outbox-phase-retryable-failure" in events
    assert any(record.get("reason") == "matrix_upload_timeout" for record in records)
    assert "secret-token" not in output
    assert "Authorization" not in output
    assert "Bearer" not in output


def test_non_retryable_matrix_4xx_dead_letters_and_is_not_drained_after_restart(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    client = FakeMatrixClient(
        fail={
            "image": MatrixError(
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
    assert dead.dead_letter_reason == "matrix_image_http_401"
    assert dead.retry_reason is None
    assert dead.phase_states == {"text": "delivered", "upload": "delivered", "image": "failed"}
    summary = LocalOutbox(store_path).status_summary()
    assert summary["counts_by_state"] == {"dead_lettered": 1}
    assert summary["dead_letter_reason_counts"] == {"matrix_image_http_401": 1}
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
        fail={"image": MatrixError("temporary", error_type="http_status", status_code=status_code)}
    )

    result = make_delivery(tmp_path, client).send_open_spot_alert(open_event(source))

    assert result.retrying_count == 1
    [record] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert record.state == "retrying"
    assert record.retry_reason == f"matrix_image_http_{status_code}"
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
    first_client = FakeMatrixClient(fail={"upload": MatrixError("timeout", error_type="timeout")})
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
