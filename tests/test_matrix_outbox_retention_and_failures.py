from __future__ import annotations

from tests.support._matrix_outbox_delivery import *  # noqa: F403


@pytest.mark.parametrize(
    "error_type",
    [
        "snapshot_copy_failed",
        "snapshot_invalid_source",
        "snapshot_metadata_failed",
        "snapshot_resize_failed",
    ],
)
def test_occupied_snapshot_preparation_failure_persists_and_drains_text_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_type: str,
) -> None:
    from parking_spot_monitor.matrix_alerts import occupied_spot_event_id

    source = tmp_path / "occupied.jpg"
    write_jpeg(source)
    client = FakeMatrixClient()
    stream = StringIO()
    delivery = make_delivery(tmp_path, client, stream=stream)
    event = occupied_event(source)

    def fail_snapshot_enqueue(**_kwargs: object) -> object:
        raise MatrixError(
            "snapshot preparation failed bearer secret-must-not-persist",
            error_type=error_type,
        )

    monkeypatch.setattr(delivery._snapshot_artifacts, "enqueue", fail_snapshot_enqueue)

    record = delivery.enqueue_occupied_spot_alert(event)

    assert record.state == "pending"
    assert record.intent.event_id == occupied_spot_event_id(event)
    assert record.phase_states == {"text": "pending"}
    assert record.intent.metadata == {
        "event_type": "occupancy-occupied-event",
        "spot_id": "left_spot",
        "observed_at": "2026-05-20T21:22:54Z",
        "snapshot_degraded_reason": error_type,
    }
    result = delivery.drain_outbox(record_id=record.id)
    assert result.delivered_count == 1
    assert [call["kind"] for call in client.calls] == ["text"]
    assert "secret-must-not-persist" not in stream.getvalue()
    [persisted] = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()
    assert persisted.phase_states == {"text": "delivered"}


def test_occupied_fallback_record_wins_when_snapshot_source_becomes_valid(
    tmp_path: Path,
) -> None:
    source = tmp_path / "occupied.jpg"
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)
    event = occupied_event(source)

    fallback = delivery.enqueue_occupied_spot_alert(event)
    first_drain = delivery.drain_outbox(record_id=fallback.id)
    write_jpeg(source)

    repeated = delivery.enqueue_occupied_spot_alert(event)
    second_drain = delivery.drain_outbox(record_id=repeated.id)

    assert first_drain.delivered_count == 1
    assert repeated.id == fallback.id
    assert repeated.phase_states == {"text": "delivered"}
    assert second_drain.attempted_count == 0
    assert len(delivery.outbox.list_records()) == 1
    assert [call["kind"] for call in client.calls] == ["text"]
    assert client.calls[0]["txn_id"] == fallback.intent.event_id


def test_occupied_snapshot_fallback_does_not_replace_existing_durable_snapshot_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_monitor.outbox import AlertIntent
    from parking_spot_monitor.matrix_alerts import occupied_spot_event_id

    source = tmp_path / "occupied.jpg"
    write_jpeg(source)
    delivery = make_delivery(tmp_path, FakeMatrixClient())
    event = occupied_event(source)
    event_id = occupied_spot_event_id(event)
    existing = delivery.outbox.enqueue_with_phases(
        AlertIntent(
            event_id=event_id,
            phase="text",
            room_id=ROOM_ID,
            body="Parking spot occupied: left_spot",
            metadata={"event_type": "occupancy-occupied-event"},
        ),
        ("text", "upload", "image"),
    )

    def fail_after_durable_enqueue(**_kwargs: object) -> object:
        raise MatrixError("resize failed", error_type="snapshot_resize_failed")

    monkeypatch.setattr(delivery._snapshot_artifacts, "enqueue", fail_after_durable_enqueue)

    record = delivery.enqueue_occupied_spot_alert(event)

    assert record.id == existing.id
    assert record.phase_states == {"text": "pending", "upload": "pending", "image": "pending"}
    assert len(delivery.outbox.list_records()) == 1


def test_occupied_text_fallback_does_not_swallow_outbox_persistence_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "occupied.jpg"
    write_jpeg(source)
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)

    def fail_persistence(_intent: AlertIntent, _phases: object) -> object:
        raise OSError("outbox persistence failed")

    monkeypatch.setattr(delivery.outbox, "enqueue_with_phases", fail_persistence)

    with pytest.raises(OSError, match="outbox persistence failed"):
        delivery.enqueue_occupied_spot_alert(occupied_event(source))

    assert client.calls == []
    assert delivery.outbox.list_records() == []


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
