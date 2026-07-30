from __future__ import annotations

from tests.support._matrix_outbox_delivery import *  # noqa: F403


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
