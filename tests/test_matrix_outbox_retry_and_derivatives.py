from __future__ import annotations

from tests.support._matrix_outbox_delivery import *  # noqa: F403


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
    resized_payload = jpeg_bytes(size=(640, 360))
    monkeypatch.setattr(
        matrix_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(resized_payload, 640, 360, 65, 6),
    )
    client = FakeMatrixClient()

    result = make_delivery(tmp_path, client).send_open_spot_alert(open_event(source))

    assert result.delivered_count == 1
    assert [call["kind"] for call in client.calls] == ["text", "upload", "image"]
    assert client.calls[0]["body"] == "Parking spot open: left_spot at 2026-05-18 1:01:02 PM PDT"
    assert client.calls[1] == {
        "kind": "upload",
        "filename": "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg",
        "data": resized_payload,
        "content_type": "image/jpeg",
    }
    assert client.calls[2]["body"] == "Raw full-frame snapshot for left_spot at 2026-05-18T20:01:02+00:00"
    assert client.calls[2]["info"] == {
        "mimetype": "image/jpeg", "size": len(resized_payload), "w": 640, "h": 360,
    }
    [persisted] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert persisted.phase_results["upload"] == {
        "content_uri": "mxc://example.org/open",
        "filename": "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg",
        "body": "Raw full-frame snapshot for left_spot at 2026-05-18T20:01:02+00:00",
        "info": {"mimetype": "image/jpeg", "size": len(resized_payload), "w": 640, "h": 360},
    }


def test_matrix_resize_uses_shared_decoder_and_preserves_matrix_error_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called: list[bytes] = []

    class FailingDecoder:
        def __enter__(self) -> object:
            raise JpegDecodeError("read_failed")

        def __exit__(self, *args: object) -> None:
            pass

    def fail(payload: bytes, *, initial_max_dimension: int) -> FailingDecoder:
        called.append(payload)
        return FailingDecoder()

    monkeypatch.setattr(matrix_snapshots, "open_decoded_rgb_jpeg_bytes", fail)
    source = tmp_path / "oversized.jpg"
    payload = write_jpeg(source)

    with pytest.raises(MatrixError) as caught:
        matrix_snapshots._resize_jpeg_for_matrix_upload(source)

    assert caught.value.diagnostics["error_type"] == "snapshot_resize_failed"
    assert called == [payload]


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
    assert {key: info[key] for key in ("mimetype", "size", "w", "h")} == {
        "mimetype": "image/jpeg",
        "size": len(before),
        "w": 960,
        "h": 540,
    }
    assert isinstance(info["sha256"], str) and len(info["sha256"]) == 64


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


def test_new_derivative_directory_fsyncs_root_before_open_and_outbox_attach(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    root = tmp_path / "snapshots"
    events: list[str] = []
    real_mkdir, real_open, real_fsync = os.mkdir, os.open, os.fsync
    real_attach = delivery.outbox.attach_upload_derivative

    def tracking_mkdir(path: object, *args: object, **kwargs: object) -> None:
        real_mkdir(path, *args, **kwargs)
        if path == matrix_upload_derivatives.DERIVATIVE_DIRECTORY:
            events.append("mkdir-child")

    def tracking_open(path: object, *args: object, **kwargs: object) -> int:
        descriptor = real_open(path, *args, **kwargs)
        if path == matrix_upload_derivatives.DERIVATIVE_DIRECTORY:
            events.append("open-child")
        return descriptor

    def tracking_fsync(descriptor: int) -> None:
        if Path(f"/proc/self/fd/{descriptor}").resolve() == root.resolve():
            events.append("fsync-root")
        real_fsync(descriptor)

    def tracking_attach(*args: object, **kwargs: object) -> object:
        events.append("attach-outbox")
        return real_attach(*args, **kwargs)

    monkeypatch.setattr(matrix_snapshot_storage.os, "mkdir", tracking_mkdir)
    monkeypatch.setattr(matrix_snapshot_storage.os, "open", tracking_open)
    monkeypatch.setattr(matrix_snapshot_storage.os, "fsync", tracking_fsync)
    monkeypatch.setattr(delivery.outbox, "attach_upload_derivative", tracking_attach)

    delivery.enqueue_open_spot_alert(open_event(source))

    mkdir_index = events.index("mkdir-child")
    assert events[mkdir_index : mkdir_index + 4] == [
        "mkdir-child",
        "fsync-root",
        "open-child",
        "attach-outbox",
    ]

    events.clear()
    derivative_payload = jpeg_bytes(size=(8, 6))
    matrix_upload_derivatives.publish_upload_derivative(
        root,
        "existing-child.jpg",
        data=derivative_payload,
        info={"mimetype": "image/jpeg", "size": len(derivative_payload), "w": 8, "h": 6},
    )
    assert "fsync-root" not in events


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
    rewrite_first_outbox_metadata(store_path, lambda metadata: metadata.update(legacy_metadata))
    rewrite_first_outbox_metadata(
        store_path,
        lambda metadata: (metadata.pop("upload_derivative_path", None), metadata.pop("upload_derivative_info", None)),
    )

    class InspectingClient(FakeMatrixClient):
        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            [persisted] = LocalOutbox(store_path).list_records()
            path = Path(str(persisted.intent.metadata["upload_derivative_path"]))
            assert path.read_bytes() == data
            derivative_info = persisted.intent.metadata["upload_derivative_info"]
            assert {key: derivative_info[key] for key in ("mimetype", "size", "w", "h")} == {
                "mimetype": "image/jpeg", "size": len(data), "w": 960, "h": 540,
            }
            assert isinstance(derivative_info["sha256"], str) and len(derivative_info["sha256"]) == 64
            return super().upload_image(filename=filename, data=data, content_type=content_type)

    restarted = MatrixOutboxDelivery(
        client=InspectingClient(), room_id=ROOM_ID, data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots", outbox=LocalOutbox(store_path),
    )

    assert restarted.drain_outbox(record_id=record.id).delivered_count == 1


def test_concurrent_legacy_upload_preparation_attaches_derivative_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source, size=(640, 360))
    store_path = tmp_path / "matrix-outbox.json"
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    record = delivery.enqueue_open_spot_alert(open_event(source))
    derivative = Path(str(record.intent.metadata["upload_derivative_path"]))
    derivative.unlink()
    rewrite_first_outbox_metadata(
        store_path,
        lambda metadata: (metadata.pop("upload_derivative_path", None), metadata.pop("upload_derivative_info", None)),
    )
    outbox = LocalOutbox(store_path)
    restarted = MatrixOutboxDelivery(
        client=FakeMatrixClient(),
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=outbox,
    )
    original_attach = outbox.attach_upload_derivative
    attach_calls = 0
    attach_lock = threading.Lock()

    def counted_attach(*args: Any, **kwargs: Any) -> Any:
        nonlocal attach_calls
        with attach_lock:
            attach_calls += 1
        return original_attach(*args, **kwargs)

    monkeypatch.setattr(outbox, "attach_upload_derivative", counted_attach)
    results: list[Any] = []
    errors: list[BaseException] = []

    def prepare() -> None:
        try:
            results.append(restarted._snapshot_artifacts.prepare_upload(record))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=prepare) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(2)

    assert errors == []
    assert len(results) == 2
    assert attach_calls == 1
    assert results[0].data == results[1].data
    [persisted] = outbox.list_records()
    assert Path(str(persisted.intent.metadata["upload_derivative_path"])).exists()


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
    rewrite_first_outbox_metadata(store_path, lambda persisted: persisted.update(metadata))
    delivery = MatrixOutboxDelivery(
        client=delivery.client, room_id=ROOM_ID, data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots", outbox=LocalOutbox(store_path),
    )

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


def test_concurrent_duplicate_enqueue_publishes_canonical_and_derivative_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source, size=(640, 360))
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    original = delivery._snapshot_artifacts.prepare_retained_snapshot
    first_entered = threading.Event()
    second_entered = threading.Event()
    release = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def blocked_prepare(**kwargs: Any) -> Any:
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        (first_entered if call_number == 1 else second_entered).set()
        assert release.wait(2)
        return original(**kwargs)

    monkeypatch.setattr(delivery._snapshot_artifacts, "prepare_retained_snapshot", blocked_prepare)
    results: list[Any] = []
    threads = [threading.Thread(target=lambda: results.append(delivery.enqueue_open_spot_alert(open_event(source)))) for _ in range(2)]
    threads[0].start()
    assert first_entered.wait(2)
    threads[1].start()
    try:
        assert not second_entered.wait(0.1), "duplicate publication entered concurrently"
    finally:
        release.set()
        for thread in threads:
            thread.join(2)

    assert calls == 1
    assert len(results) == 2
    assert results[0].id == results[1].id
    [persisted] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    derivative = Path(str(persisted.intent.metadata["upload_derivative_path"]))
    assert derivative.exists()
    assert persisted.intent.metadata["upload_derivative_info"]["size"] == derivative.stat().st_size


def test_drain_waits_for_enqueue_to_attach_the_initial_upload_derivative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source, size=(640, 360))
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)
    original_upload = matrix_outbox_snapshots._matrix_snapshot_upload
    first_entered = threading.Event()
    second_entered = threading.Event()
    release = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def blocked_upload(snapshot: Any, *, logger: Any) -> Any:
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        (first_entered if call_number == 1 else second_entered).set()
        assert release.wait(2)
        return original_upload(snapshot, logger=logger)

    monkeypatch.setattr(matrix_outbox_snapshots, "_matrix_snapshot_upload", blocked_upload)
    enqueue_results: list[Any] = []
    enqueue_errors: list[BaseException] = []
    drain_results: list[MatrixOutboxDrainResult] = []
    drain_errors: list[BaseException] = []

    def enqueue() -> None:
        try:
            enqueue_results.append(delivery.enqueue_open_spot_alert(open_event(source)))
        except BaseException as exc:
            enqueue_errors.append(exc)

    enqueue_thread = threading.Thread(target=enqueue)
    enqueue_thread.start()
    assert first_entered.wait(2)
    [visible] = delivery.outbox.list_records()

    def drain() -> None:
        try:
            drain_results.append(delivery.drain_outbox(record_id=visible.id))
        except BaseException as exc:
            drain_errors.append(exc)

    drain_thread = threading.Thread(target=drain)
    drain_thread.start()
    try:
        assert not second_entered.wait(0.1), "drain regenerated a derivative before enqueue attached it"
    finally:
        release.set()
        enqueue_thread.join(2)
        drain_thread.join(2)

    assert not enqueue_thread.is_alive() and not drain_thread.is_alive()
    assert enqueue_errors == []
    assert drain_errors == []
    assert calls == 1
    assert len(enqueue_results) == 1
    assert drain_results == [MatrixOutboxDrainResult(attempted_count=1, delivered_count=1, retrying_count=0)]
    assert len([call for call in client.calls if call["kind"] == "upload"]) == 1
    [persisted] = delivery.outbox.list_records()
    assert persisted.id == enqueue_results[0].id
    assert persisted.state == "delivered"


def test_unrelated_snapshot_enqueues_generate_derivatives_concurrently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source, size=(640, 360))
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    original_upload = matrix_outbox_snapshots._matrix_snapshot_upload
    entered = [threading.Event(), threading.Event()]
    release = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def blocked_upload(snapshot: Any, *, logger: Any) -> Any:
        nonlocal calls
        with calls_lock:
            call_number = calls
            calls += 1
        entered[call_number].set()
        assert release.wait(2)
        return original_upload(snapshot, logger=logger)

    monkeypatch.setattr(matrix_outbox_snapshots, "_matrix_snapshot_upload", blocked_upload)
    events = [
        open_event(source),
        open_event(source) | {"observed_at": datetime(2026, 5, 18, 20, 1, 3, tzinfo=timezone.utc)},
    ]
    results: list[Any] = []
    errors: list[BaseException] = []

    def enqueue(event: dict[str, Any]) -> None:
        try:
            results.append(delivery.enqueue_open_spot_alert(event))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=enqueue, args=(event,)) for event in events]
    for thread in threads:
        thread.start()
    try:
        assert entered[0].wait(2)
        assert entered[1].wait(2), "unrelated event publication was globally serialized"
    finally:
        release.set()
        for thread in threads:
            thread.join(2)

    assert errors == []
    assert len(results) == 2
    assert len({record.id for record in results}) == 2


def test_restart_rejects_same_size_corrupt_derivative_by_digest(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    store_path = tmp_path / "matrix-outbox.json"
    first = make_delivery(tmp_path, FakeMatrixClient(fail={"upload": MatrixError("timeout", error_type="timeout")}))
    first.send_open_spot_alert(open_event(source))
    [record] = LocalOutbox(store_path).list_records()
    info = record.intent.metadata["upload_derivative_info"]
    assert isinstance(info, dict)
    assert isinstance(info["sha256"], str) and len(info["sha256"]) == 64
    derivative = Path(str(record.intent.metadata["upload_derivative_path"]))
    payload = bytearray(derivative.read_bytes())
    payload[len(payload) // 2] ^= 1
    derivative.write_bytes(payload)
    client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=client, room_id=ROOM_ID, data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots", outbox=LocalOutbox(store_path), utc_now=lambda: RETRY_DUE_NOW,
    )

    result = restarted.drain_outbox(record_id=record.id)

    assert result.retrying_count == 0
    assert not [call for call in client.calls if call["kind"] == "upload"]
    [dead] = LocalOutbox(store_path).list_records()
    assert dead.state == "dead_lettered"


def test_restart_rejects_persisted_derivative_without_digest(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    record = delivery.enqueue_open_spot_alert(open_event(source))
    rewrite_first_outbox_metadata(
        store_path,
        lambda metadata: metadata["upload_derivative_info"].pop("sha256"),
    )
    client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=client,
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(store_path),
    )

    result = restarted.drain_outbox(record_id=record.id)

    assert result.retrying_count == 0
    assert not [call for call in client.calls if call["kind"] == "upload"]
    [dead] = LocalOutbox(store_path).list_records()
    assert dead.state == "dead_lettered"


def test_persisted_paths_cannot_redirect_upload_outside_configured_snapshot_root(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    store_path = tmp_path / "matrix-outbox.json"
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    record = delivery.enqueue_open_spot_alert(open_event(source))
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_raw = outside / "event.jpg"
    outside_raw.write_bytes(b"outside raw")
    outside_derivative = outside / "event-upload.jpg"
    outside_derivative.write_bytes(b"outside derivative")
    rewrite_first_outbox_metadata(
        store_path,
        lambda metadata: metadata.update(
            {
                "retained_snapshot_path": str(outside_raw),
                "retained_snapshot_filename": "../outside/event.jpg",
                "upload_derivative_path": str(outside_derivative),
                "upload_derivative_info": {"mimetype": "image/jpeg", "size": len(b"outside derivative"), "w": 8, "h": 6},
            }
        ),
    )
    client = FakeMatrixClient()
    restarted = MatrixOutboxDelivery(
        client=client, room_id=ROOM_ID, data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots", outbox=LocalOutbox(store_path),
    )

    restarted.drain_outbox(record_id=record.id)

    assert not [call for call in client.calls if call["kind"] == "upload"]
    assert outside_raw.read_bytes() == b"outside raw"
    assert outside_derivative.read_bytes() == b"outside derivative"


def test_symlinked_derivative_parent_is_rejected_without_following(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    record = delivery.enqueue_open_spot_alert(open_event(source))
    derivative = Path(str(record.intent.metadata["upload_derivative_path"]))
    external = tmp_path / "external-derivatives"
    external.mkdir()
    moved = external / derivative.name
    derivative.replace(moved)
    derivative.parent.rmdir()
    derivative.parent.symlink_to(external, target_is_directory=True)

    result = delivery.drain_outbox(record_id=record.id)

    assert result.retrying_count == 0
    assert not [call for call in delivery.client.calls if call["kind"] == "upload"]
    assert moved.exists()


def test_retention_keeps_raw_when_derivative_cleanup_transiently_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "snapshots"
    root.mkdir()
    raw = root / "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg"
    write_jpeg(raw)
    newer = root / "occupancy-open-event-left-spot-2026-05-18t20-02-02z.jpg"
    write_jpeg(newer, color=(90, 20, 120))
    derivative = root / ".upload-derivatives" / raw.name
    derivative.parent.mkdir()
    write_jpeg(derivative)
    def fail_derivative(_root: Path, _filename: str) -> int:
        raise OSError("transient derivative cleanup failure")

    monkeypatch.setattr(matrix_snapshots, "delete_upload_derivative", fail_derivative)

    result = matrix_snapshots.prune_event_snapshots(root, retention_count=1, logger=None, current_snapshot=None)
    assert result.pruned_count == 0
    assert raw.exists()
    assert derivative.exists()


def test_retention_typed_derivative_failure_keeps_pair_and_reports_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "snapshots"
    root.mkdir()
    raw = root / "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg"
    newer = root / "occupancy-open-event-left-spot-2026-05-18t20-02-02z.jpg"
    write_jpeg(raw)
    write_jpeg(newer, color=(90, 20, 120))
    derivative = root / ".upload-derivatives" / raw.name
    derivative.parent.mkdir()
    write_jpeg(derivative)

    monkeypatch.setattr(
        matrix_snapshots,
        "delete_upload_derivative",
        lambda *_args, **_kwargs: matrix_snapshot_storage.OwnedArtifactDeleteResult(
            status="failed", bytes_deleted=0
        ),
    )

    result = matrix_snapshots.prune_event_snapshots(root, retention_count=1, logger=None)

    assert result.pruned_count == 0
    assert result.pruned_bytes == 0
    assert result.failed_count == 1
    assert result.retained_count == 2
    assert raw.exists()
    assert derivative.exists()
