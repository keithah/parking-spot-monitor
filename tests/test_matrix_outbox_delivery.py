from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
import httpx
from PIL import Image

import parking_spot_monitor.matrix_snapshots as matrix_snapshots
from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery, MatrixOutboxDrainResult
from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.image_budget import JpegBudgetResult
from parking_spot_monitor.jpeg_artifacts import JpegDecodeError
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix import MatrixError
from parking_spot_monitor.matrix_alerts import MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE, monitor_lifecycle_event
from parking_spot_monitor.matrix_client import MatrixClient

ROOM_ID = "!parking-room:example.org"
EVENT_ID = "occupancy-open-event:left_spot:2026-05-18T20:01:02Z"
RETRY_DUE_NOW = datetime(2100, 1, 1, tzinfo=timezone.utc)


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

    def cancel_pending(self) -> None:
        self.close()


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


def test_retry_failure_persists_per_record_exponential_schedule_across_restart(
    tmp_path: Path,
) -> None:
    now = [datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)]
    path = tmp_path / "matrix-outbox.json"
    client = FakeMatrixClient(fail={"text": TimeoutError("timeout")})
    delivery = MatrixOutboxDelivery(
        client=client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(path),
        utc_now=lambda: now[0],
        random_unit=lambda: 0,
    )
    record = delivery.enqueue_text_notice(
        "quiet-window-started",
        {"event_type": "quiet-window-started", "event_id": "retry-schedule", "window_id": "w"},
    )

    delivery.drain_outbox(record_id=record.id)
    [first] = LocalOutbox(path).list_records()
    assert first.retry_attempt_count == 1
    assert first.retry_due_at == "2026-07-30T12:01:00Z"
    assert LocalOutbox(path).next_due_record(now[0]) is None

    now[0] = datetime(2026, 7, 30, 12, 1, tzinfo=timezone.utc)
    restarted = MatrixOutboxDelivery(
        client=client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(path),
        utc_now=lambda: now[0],
        random_unit=lambda: 0,
    )
    restarted.drain_outbox(record_id=record.id)
    [second] = LocalOutbox(path).list_records()
    assert second.retry_attempt_count == 2
    assert second.retry_due_at == "2026-07-30T12:03:00Z"


def test_public_drain_does_not_bypass_persisted_retry_due_even_by_id(tmp_path: Path) -> None:
    now = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
    client = FakeMatrixClient()
    delivery = MatrixOutboxDelivery(
        client=client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
        utc_now=lambda: now,
    )
    record = delivery.enqueue_text_notice(
        "quiet-window-started",
        {"event_type": "quiet-window-started", "event_id": "future-retry", "window_id": "w"},
    )
    delivery.outbox.mark_retrying(
        record.id,
        reason="timeout",
        retry_due_at="2026-07-30T13:00:00Z",
        retry_attempt_count=1,
    )

    all_result = delivery.drain_outbox()
    id_result = delivery.drain_outbox(record_id=record.id)

    assert all_result.attempted_count == 0
    assert id_result.attempted_count == 0
    assert client.calls == []


def test_duplicate_send_does_not_bypass_existing_record_retry_due(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    client = FakeMatrixClient(fail={"upload": MatrixError("timeout", error_type="timeout")})
    delivery = MatrixOutboxDelivery(
        client=client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
        utc_now=lambda: datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc),
        random_unit=lambda: 0,
    )
    first = delivery.send_open_spot_alert(open_event(source))
    assert first.retrying_count == 1
    client.fail.clear()
    client.calls.clear()

    duplicate = delivery.send_open_spot_alert(open_event(source))

    assert duplicate.attempted_count == 0
    assert client.calls == []


def test_delivery_publishes_once_per_durable_phase_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    record = delivery.enqueue_open_spot_alert(open_event(source))
    original = delivery.outbox._persist_records
    calls = 0

    def counted(records):
        nonlocal calls
        calls += 1
        return original(records)

    monkeypatch.setattr(delivery.outbox, "_persist_records", counted)
    delivered = delivery.drain_outbox(record_id=record.id)

    assert delivered.delivered_count == 1
    assert calls == 3


def test_retryable_phase_failure_publishes_one_retry_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    delivery = make_delivery(tmp_path, FakeMatrixClient(fail={"text": TimeoutError("timeout")}))
    record = delivery.enqueue_text_notice(
        "quiet-window-started",
        {"event_type": "quiet-window-started", "event_id": "retry-write-count", "window_id": "w"},
    )
    original = delivery.outbox._persist_records
    calls = 0

    def counted(records):
        nonlocal calls
        calls += 1
        return original(records)

    monkeypatch.setattr(delivery.outbox, "_persist_records", counted)
    delivery.drain_outbox(record_id=record.id)

    assert calls == 1


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


def test_oversized_outbox_snapshot_preserves_upload_and_persisted_info_schema(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    assert source.stat().st_size > matrix_snapshots.MAX_MATRIX_UPLOAD_IMAGE_BYTES
    monkeypatch.setattr(
        matrix_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 6),
    )
    client = FakeMatrixClient()

    result = make_delivery(tmp_path, client).send_open_spot_alert(open_event(source))

    assert result.delivered_count == 1
    assert [call["kind"] for call in client.calls] == ["text", "upload", "image"]
    assert client.calls[0]["body"] == "Parking spot open: left_spot at 2026-05-18 1:01:02 PM PDT"
    assert client.calls[1] == {
        "kind": "upload",
        "filename": "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg",
        "data": b"jpeg",
        "content_type": "image/jpeg",
    }
    assert client.calls[2]["body"] == "Raw full-frame snapshot for left_spot at 2026-05-18T20:01:02+00:00"
    assert client.calls[2]["info"] == {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360}
    [persisted] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert persisted.phase_results["upload"] == {
        "content_uri": "mxc://example.org/open",
        "filename": "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg",
        "body": "Raw full-frame snapshot for left_spot at 2026-05-18T20:01:02+00:00",
        "info": {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360},
    }


def test_matrix_resize_uses_shared_decoder_and_preserves_matrix_error_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called: list[Path] = []

    class FailingDecoder:
        def __enter__(self) -> object:
            raise JpegDecodeError("read_failed")

        def __exit__(self, *args: object) -> None:
            pass

    def fail(path: Path, *, initial_max_dimension: int) -> FailingDecoder:
        called.append(path)
        return FailingDecoder()

    monkeypatch.setattr(matrix_snapshots, "open_decoded_rgb_jpeg", fail, raising=False)
    source = tmp_path / "oversized.jpg"
    write_jpeg(source)

    with pytest.raises(MatrixError) as caught:
        matrix_snapshots._resize_jpeg_for_matrix_upload(source)

    assert caught.value.diagnostics["error_type"] == "snapshot_resize_failed"
    assert called == [source]


def test_upload_retry_reuses_persisted_derivative_after_restart(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    store_path = tmp_path / "matrix-outbox.json"
    first_client = FakeMatrixClient(fail={"upload": MatrixError("timeout", error_type="timeout")})
    first = make_delivery(tmp_path, first_client)

    record = first.enqueue_open_spot_alert(open_event(source))
    first.drain_outbox(record_id=record.id)

    [persisted] = LocalOutbox(store_path).list_records()
    derivative = Path(str(persisted.intent.metadata["upload_derivative_path"]))
    retained = Path(str(persisted.intent.metadata["retained_snapshot_path"]))
    info = persisted.intent.metadata["upload_derivative_info"]
    before = derivative.read_bytes()
    assert derivative.parent == tmp_path / "snapshots" / ".upload-derivatives"
    assert derivative.name == retained.name
    assert list((tmp_path / "snapshots").glob("occupancy-open-event-left-spot-*.jpg")) == [retained]
    source.unlink()
    retained.write_bytes(b"changed after derivative selection")

    second_client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=second_client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
        utc_now=lambda: RETRY_DUE_NOW,
    )
    delivered = restarted.drain_outbox(record_id=record.id)

    assert delivered.delivered_count == 1
    assert [call for call in second_client.calls if call["kind"] == "upload"][0]["data"] == before
    assert derivative.read_bytes() == before
    assert info == {
        "mimetype": "image/jpeg",
        "size": len(before),
        "w": 960,
        "h": 540,
    }


def test_snapshot_retention_keeps_pending_derivative_then_prunes_terminal_pair(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    first_client = FakeMatrixClient(fail={"upload": MatrixError("timeout", error_type="timeout")})
    first = make_delivery(tmp_path, first_client, snapshot_retention_count=1)
    retrying = first.send_open_spot_alert(open_event(source))
    assert retrying.retrying_count == 1
    [record] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    retained = Path(str(record.intent.metadata["retained_snapshot_path"]))
    derivative = Path(str(record.intent.metadata["upload_derivative_path"]))
    assert retained.exists() and derivative.exists()

    later = open_event(source) | {"observed_at": datetime(2026, 5, 18, 20, 2, 0, tzinfo=timezone.utc)}
    make_delivery(tmp_path, FakeMatrixClient(), snapshot_retention_count=1).send_open_spot_alert(later)
    assert retained.exists() and derivative.exists()

    restarted = MatrixOutboxDelivery(
        client=FakeMatrixClient(), room_id=ROOM_ID, data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots", outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
        snapshot_retention_count=1, utc_now=lambda: RETRY_DUE_NOW,
    )
    restarted.drain_outbox(record_id=record.id)
    newest = open_event(source) | {"observed_at": datetime(2026, 5, 18, 20, 3, 0, tzinfo=timezone.utc)}
    make_delivery(tmp_path, FakeMatrixClient(), snapshot_retention_count=1).send_open_spot_alert(newest)

    assert not retained.exists()
    assert not derivative.exists()


def test_legacy_upload_regenerates_and_persists_derivative_before_network(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    store_path = tmp_path / "matrix-outbox.json"
    first = make_delivery(tmp_path, FakeMatrixClient())
    record = first.enqueue_open_spot_alert(open_event(source))
    legacy_metadata = dict(record.intent.metadata)
    old_derivative = Path(str(legacy_metadata.pop("upload_derivative_path")))
    legacy_metadata.pop("upload_derivative_info")
    old_derivative.unlink()
    first.outbox.update_intent_metadata(record.id, legacy_metadata)

    class InspectingClient(FakeMatrixClient):
        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            [persisted] = LocalOutbox(store_path).list_records()
            path = Path(str(persisted.intent.metadata["upload_derivative_path"]))
            assert path.read_bytes() == data
            assert persisted.intent.metadata["upload_derivative_info"] == {
                "mimetype": "image/jpeg", "size": len(data), "w": 960, "h": 540,
            }
            return super().upload_image(filename=filename, data=data, content_type=content_type)

    restarted = MatrixOutboxDelivery(
        client=InspectingClient(), room_id=ROOM_ID, data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots", outbox=LocalOutbox(store_path),
    )

    assert restarted.drain_outbox(record_id=record.id).delivered_count == 1


def test_upload_rejects_out_of_contract_derivative_path_without_reading_it(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    record = delivery.enqueue_open_spot_alert(open_event(source))
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"must not be uploaded")
    metadata = dict(record.intent.metadata)
    metadata["upload_derivative_path"] = str(outside)
    delivery.outbox.update_intent_metadata(record.id, metadata)

    result = delivery.drain_outbox(record_id=record.id)

    assert result.retrying_count == 0
    [dead] = LocalOutbox(store_path).list_records()
    assert dead.state == "dead_lettered"
    assert dead.dead_letter_reason == "matrix_upload_snapshot_resize_failed"
    assert outside.read_bytes() == b"must not be uploaded"
    assert not [call for call in delivery.client.calls if call["kind"] == "upload"]


def test_duplicate_enqueue_does_not_replace_immutable_upload_derivative(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    first = delivery.enqueue_open_spot_alert(open_event(source))
    derivative = Path(str(first.intent.metadata["upload_derivative_path"]))
    selected = derivative.read_bytes()
    write_jpeg(source, color=(200, 10, 10))

    duplicate = delivery.enqueue_open_spot_alert(open_event(source))

    assert duplicate.id == first.id
    assert derivative.read_bytes() == selected


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
        delivery.enqueue_text_notice(
            "quiet-window-started",
            {"event_type": "quiet-window-started", "event_id": "bounded-drain", "window_id": "w"},
        )
        delivery.start_worker(retry_interval_seconds=60)
        assert drained.wait(2), "worker did not perform its initial bounded drain"
        assert delivery.max_records
        assert set(delivery.max_records) == {1}
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

        def next_due_record(self, now: datetime) -> Any | None:
            self.list_calls += 1
            (first_read if self.list_calls == 1 else repeated_read).set()
            return super().next_due_record(now)

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
        delivery.start_worker(retry_interval_seconds=600)
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
        delivery.enqueue_text_notice(
            "quiet-window-started",
            {"event_type": "quiet-window-started", "event_id": "flaky-worker", "window_id": "w"},
        )
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


def test_worker_survives_unexpected_post_pass_summary_failure_and_paces_retry(tmp_path: Path) -> None:
    summary_failed = threading.Event()
    resumed = threading.Event()

    class SummaryFailureOutbox(LocalOutbox):
        def __init__(self, path: Path) -> None:
            super().__init__(path)
            self.summary_calls = 0

        def compact_status_summary(self) -> dict[str, Any]:
            self.summary_calls += 1
            if self.summary_calls == 1:
                summary_failed.set()
                raise RuntimeError("Authorization: Bearer summary-secret")
            return super().compact_status_summary()

    class RecordingDelivery(MatrixOutboxDelivery):
        def __init__(self, outbox: LocalOutbox) -> None:
            super().__init__(
                client=FakeMatrixClient(),
                room_id=ROOM_ID,
                data_dir=tmp_path,
                snapshots_dir=tmp_path / "snapshots",
                outbox=outbox,
            )
            self.drain_calls = 0

        def drain_outbox(
            self,
            *,
            record_id: str | None = None,
            max_records: int | None = None,
        ) -> MatrixOutboxDrainResult:
            self.drain_calls += 1
            if self.drain_calls == 2:
                resumed.set()
            return MatrixOutboxDrainResult(0, 0, 0)

    delivery = RecordingDelivery(SummaryFailureOutbox(tmp_path / "matrix-outbox.json"))
    try:
        delivery.enqueue_text_notice(
            "quiet-window-started",
            {"event_type": "quiet-window-started", "event_id": "summary-worker", "window_id": "w"},
        )
        delivery.start_worker(retry_interval_seconds=0.2)
        assert summary_failed.wait(2), "worker did not reach post-pass outbox summarization"
        assert not resumed.wait(0.05), "worker retried immediately after an unexpected summary failure"
        assert resumed.wait(2), "worker died after an unexpected post-pass summary failure"
        health = delivery.outbox_health_summary()
        assert health["worker_running"] is True
        assert health["worker_last_error_type"] == "RuntimeError"
        assert "summary-secret" not in json.dumps(health)
    finally:
        delivery.close()


def test_worker_survives_unexpected_cooldown_selection_failure_and_paces_retry(tmp_path: Path) -> None:
    cooldown_ready = threading.Event()
    selection_failed = threading.Event()
    resumed = threading.Event()

    class SelectionFailureOutbox(LocalOutbox):
        def __init__(self, path: Path) -> None:
            super().__init__(path)
            self.retrying = True
            self.selection_calls = 0

        def next_due_record(self, now: datetime) -> Any | None:
            self.selection_calls += 1
            if self.selection_calls == 2:
                selection_failed.set()
                raise RuntimeError("access_token=selection-secret")
            return super().next_due_record(now)

        def compact_status_summary(self) -> dict[str, Any]:
            cooldown_ready.set()
            return {"counts_by_state": {"retrying": 1 if self.retrying else 0}}

    class RecordingDelivery(MatrixOutboxDelivery):
        def __init__(self, outbox: SelectionFailureOutbox) -> None:
            super().__init__(
                client=FakeMatrixClient(),
                room_id=ROOM_ID,
                data_dir=tmp_path,
                snapshots_dir=tmp_path / "snapshots",
                outbox=outbox,
            )
            self.selection_outbox = outbox
            self.drain_calls = 0

        def drain_outbox(
            self,
            *,
            record_id: str | None = None,
            max_records: int | None = None,
        ) -> MatrixOutboxDrainResult:
            self.drain_calls += 1
            if self.drain_calls == 1:
                return MatrixOutboxDrainResult(1, 0, 1)
            self.selection_outbox.retrying = False
            resumed.set()
            return MatrixOutboxDrainResult(0, 0, 0)

    outbox = SelectionFailureOutbox(tmp_path / "matrix-outbox.json")
    delivery = RecordingDelivery(outbox)
    try:
        delivery.enqueue_text_notice(
            "quiet-window-started",
            {
                "event_type": "quiet-window-started",
                "event_id": "selection-worker",
                "window_id": "selection-worker",
            },
        )
        delivery.start_worker(retry_interval_seconds=0.2)
        assert cooldown_ready.wait(2), "worker did not enter retry cooldown"
        assert selection_failed.wait(2), "worker did not select pending work during cooldown"
        assert not resumed.wait(0.05), "worker retried immediately after an unexpected selection failure"
        assert resumed.wait(2), "worker died after an unexpected cooldown selection failure"
        health = delivery.outbox_health_summary()
        assert health["worker_running"] is True
        assert health["worker_last_error_type"] == "RuntimeError"
        assert "selection-secret" not in json.dumps(health)
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


def test_concurrent_first_start_cannot_clear_close_stop_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import parking_monitor.matrix_outbox_delivery as delivery_module

    clear_entered = threading.Event()
    release_clear = threading.Event()
    lifecycle_contended = threading.Event()

    class ContentionRecordingLock:
        def __init__(self) -> None:
            self.lock = threading.Lock()
            self.owner: int | None = None

        def acquire(self) -> bool:
            current = threading.get_ident()
            if self.owner is not None and self.owner != current:
                lifecycle_contended.set()
            acquired = self.lock.acquire()
            if acquired:
                self.owner = current
            return acquired

        def release(self) -> None:
            self.owner = None
            self.lock.release()

        def __enter__(self) -> ContentionRecordingLock:
            self.acquire()
            return self

        def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
            self.release()

    class BlockingClearEvent:
        def __init__(self) -> None:
            self.event = threading.Event()

        def clear(self) -> None:
            clear_entered.set()
            assert release_clear.wait(2), "test did not release worker stop clear"
            self.event.clear()

        def is_set(self) -> bool:
            return self.event.is_set()

        def set(self) -> None:
            self.event.set()

        def wait(self, timeout: float | None = None) -> bool:
            return self.event.wait(timeout)

    class WaitingDelivery(MatrixOutboxDelivery):
        def _worker_main(self) -> None:
            self._stop_event.wait()

    monkeypatch.setattr(delivery_module, "_WORKER_JOIN_TIMEOUT_SECONDS", 0.05)
    delivery = WaitingDelivery(
        client=FakeMatrixClient(),
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
    )
    delivery._worker_lock = ContentionRecordingLock()  # type: ignore[assignment]
    delivery._stop_event = BlockingClearEvent()  # type: ignore[assignment]
    errors: list[BaseException] = []

    def start() -> None:
        try:
            delivery.start_worker(retry_interval_seconds=60)
        except BaseException as exc:
            errors.append(exc)

    def close() -> None:
        try:
            delivery.close()
        except BaseException as exc:
            errors.append(exc)

    starter = threading.Thread(target=start, name="concurrent-worker-start")
    closer = threading.Thread(target=close, name="concurrent-worker-close")
    starter.start()
    assert clear_entered.wait(2), "first worker start did not reach stop clear"
    closer.start()
    assert lifecycle_contended.wait(2), "close did not contend with first worker start"
    release_clear.set()
    starter.join(timeout=2)
    closer.join(timeout=2)
    worker = delivery.worker_thread
    try:
        assert starter.is_alive() is False
        assert closer.is_alive() is False
        assert errors == []
        assert worker is not None
        assert worker.is_alive() is False
        assert delivery._stop_event.is_set() is True
        with pytest.raises(RuntimeError, match="after close"):
            delivery.start_worker(retry_interval_seconds=60)
    finally:
        delivery._stop_event.set()
        delivery._wake_event.set()
        if worker is not None:
            worker.join(timeout=2)


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


def test_lifecycle_notice_survives_close_and_restart_exactly_once(tmp_path: Path) -> None:
    event = monitor_lifecycle_event(
        MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
        datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc),
        signal="SIGTERM",
    )
    first_client = FakeMatrixClient()
    first = make_delivery(tmp_path, first_client)

    record = first.enqueue_lifecycle_notice(event)
    first.close()

    assert first_client.calls == []
    [persisted] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert persisted.id == record.id
    assert persisted.state == "pending"

    restarted_client = FakeMatrixClient()
    restarted = make_delivery(tmp_path, restarted_client)
    assert restarted.drain_outbox().delivered_count == 1
    assert restarted.drain_outbox().attempted_count == 0
    assert [call["txn_id"] for call in restarted_client.calls] == [event["event_id"]]


def test_close_cancels_pending_client_once_and_is_idempotent(tmp_path: Path) -> None:
    class CancelRecordingClient(FakeMatrixClient):
        def __init__(self) -> None:
            super().__init__()
            self.cancel_calls = 0
            self.close_calls = 0

        def cancel_pending(self) -> None:
            self.cancel_calls += 1

        def close(self) -> None:
            self.close_calls += 1
            super().close()

    client = CancelRecordingClient()
    delivery = make_delivery(tmp_path, client)

    delivery.close()
    delivery.close()

    assert client.cancel_calls == 1
    assert client.close_calls == 1


def test_worker_stop_between_phases_does_not_start_next_phase(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    stopped = threading.Event()
    delivery: MatrixOutboxDelivery

    def stop_after_text() -> None:
        delivery.close()
        stopped.set()

    client = FakeMatrixClient(on_send_text=stop_after_text)
    delivery = make_delivery(tmp_path, client)
    delivery.start_worker(retry_interval_seconds=60)
    delivery.enqueue_open_spot_alert(open_event(source))

    assert stopped.wait(1)
    worker = delivery.worker_thread
    assert worker is not None
    worker.join(1)
    assert worker.is_alive() is False
    assert [call["kind"] for call in client.calls] == ["text"]
    [persisted] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert persisted.phase_states == {
        "text": "delivered",
        "upload": "pending",
        "image": "pending",
    }


def test_manual_drain_close_between_phases_does_not_start_next_phase(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    entered_text = threading.Event()
    release_text = threading.Event()
    upload_started = threading.Event()

    class BlockingClient(FakeMatrixClient):
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            entered_text.set()
            assert release_text.wait(1)
            return super().send_text(room_id=room_id, txn_id=txn_id, body=body)

        def cancel_pending(self) -> None:
            release_text.set()

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            upload_started.set()
            return super().upload_image(filename=filename, data=data, content_type=content_type)

    client = BlockingClient()
    delivery = make_delivery(tmp_path, client)
    delivery.enqueue_open_spot_alert(open_event(source))
    drain = threading.Thread(target=delivery.drain_outbox, name="manual-outbox-drain")
    drain.start()
    assert entered_text.wait(1)

    delivery.close()
    drain.join(1)

    assert drain.is_alive() is False
    assert upload_started.is_set() is False
    assert [call["kind"] for call in client.calls] == ["text"]


def test_matrix_client_cancel_interrupts_retry_wait_with_safe_error() -> None:
    attempted = threading.Event()
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        attempted.set()
        return httpx.Response(503, json={"errcode": "M_UNAVAILABLE"}, request=request)

    client = MatrixClient(
        homeserver="https://matrix.example.org",
        access_token="test-token",
        retry_attempts=3,
        retry_backoff_seconds=60,
        retry_jitter_ratio=0,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    errors: list[MatrixError] = []

    def send() -> None:
        try:
            client.send_text(room_id=ROOM_ID, txn_id="cancelled", body="lifecycle")
        except MatrixError as exc:
            errors.append(exc)

    thread = threading.Thread(target=send)
    thread.start()
    assert attempted.wait(1)
    started = time.monotonic()
    client.cancel_pending()
    thread.join(1)

    assert time.monotonic() - started < 1
    assert thread.is_alive() is False
    assert attempts == 1
    assert len(errors) == 1
    assert errors[0].diagnostics["error_type"] == "cancelled"


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
        utc_now=lambda: RETRY_DUE_NOW,
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
    assert sorted(record.state for record in records) == ["delivered", "pending"]


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
        utc_now=lambda: RETRY_DUE_NOW,
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
        utc_now=lambda: RETRY_DUE_NOW,
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


def test_empty_outbox_drain_uses_debug_instead_of_info(tmp_path: Path) -> None:
    info_stream = StringIO()
    info_result = make_delivery(tmp_path, FakeMatrixClient(), stream=info_stream).drain_outbox(max_records=1)

    assert info_result == MatrixOutboxDrainResult(attempted_count=0, delivered_count=0, retrying_count=0)
    assert info_stream.getvalue() == ""

    debug_stream = StringIO()
    delivery = MatrixOutboxDelivery(
        client=FakeMatrixClient(),
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "debug-matrix-outbox.json"),
        logger=StructuredLogger(level="DEBUG", stream=debug_stream),
    )

    delivery.drain_outbox(max_records=1)

    records = [json.loads(line) for line in debug_stream.getvalue().splitlines()]
    assert [(record["event"], record["level"]) for record in records] == [
        ("matrix-outbox-drain-started", "DEBUG"),
        ("matrix-outbox-drain-finished", "DEBUG"),
    ]


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
        utc_now=lambda: RETRY_DUE_NOW,
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
        utc_now=lambda: RETRY_DUE_NOW,
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
