from __future__ import annotations

from tests.support._startup import *  # noqa: F403


def test_dispatch_matrix_open_alert_feedback_uses_retained_snapshot_not_latest(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    latest_path = tmp_path / "latest.jpg"
    Image.new("RGB", (10, 8), (12, 34, 56)).save(latest_path, format="JPEG")
    retained_path = tmp_path / "snapshots" / "occupancy-open-event-left-spot-retained.jpg"
    retained_path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (17, 11), (78, 90, 12)).save(retained_path, format="JPEG")

    class RetainedSnapshotDelivery:
        def __init__(self) -> None:
            self.open_alerts: list[dict[str, Any]] = []

        def enqueue_open_spot_alert(self, event: dict[str, Any]) -> MatrixSnapshot:
            self.open_alerts.append(dict(event))
            return MatrixSnapshot(
                path=retained_path,
                filename=retained_path.name,
                txn_id="snapshot-retained",
                body="retained open alert snapshot",
                info={"mimetype": "image/jpeg", "size": retained_path.stat().st_size, "w": 17, "h": 11},
                log_context={"snapshot_path": str(retained_path)},
            )

    event = {
        "event_type": "occupancy-open-event",
        "event_id": "occupancy-open-event:left_spot:2026-05-18T20:01:02Z",
        "spot_id": "left_spot",
        "observed_at": "2026-05-18T20:01:02Z",
        "snapshot_path": str(latest_path),
    }

    error = dispatch_matrix_event(
        RetainedSnapshotDelivery(),
        "occupancy-open-event",
        event,
        logger=StructuredLogger(),
        decision_memory_path=tmp_path / "operator-decision-memory.json",
    )

    assert error is None
    result = OperatorFeedbackLabeler(data_dir=tmp_path).record_correction(
        spot_id="left_spot",
        actual_state="occupied",
        matrix_event_id="$correction",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        corrected_at="2026-05-18T20:02:00Z",
    )

    assert result.recorded is True
    assert result.reported_state == "open"
    assert result.evidence.available is True
    assert result.evidence.validated_jpeg is True
    assert result.evidence.path == "snapshots/occupancy-open-event-left-spot-retained.jpg"
    assert result.evidence.path != str(latest_path)
    assert result.evidence.width == 17
    assert result.evidence.height == 11

    loaded = load_feedback_labels(feedback_labels_path(tmp_path))
    assert loaded.state == "available"
    assert len(loaded.labels) == 1
    assert loaded.labels[0].evidence.available is True
    assert loaded.labels[0].evidence.validated_jpeg is True
    assert loaded.labels[0].evidence.path == "snapshots/occupancy-open-event-left-spot-retained.jpg"


def test_dispatch_matrix_open_alert_enqueues_without_immediate_network_when_outbox_supported(tmp_path: Path) -> None:
    class EnqueueOnlyDelivery:
        def __init__(self) -> None:
            self.enqueued: list[dict[str, Any]] = []
            self.sent: list[dict[str, Any]] = []

        def enqueue_open_spot_alert(self, event: dict[str, Any]) -> object:
            self.enqueued.append(dict(event))
            return object()

        def send_open_spot_alert(self, event: dict[str, Any]) -> None:
            self.sent.append(dict(event))
            raise AssertionError("frame dispatch should not perform open-alert network drain")

    event = {
        "event_type": "occupancy-open-event",
        "event_id": "occupancy-open-event:left_spot:2026-05-18T20:01:02Z",
        "spot_id": "left_spot",
        "observed_at": "2026-05-18T20:01:02Z",
        "snapshot_path": str(tmp_path / "latest.jpg"),
    }
    delivery = EnqueueOnlyDelivery()

    error = dispatch_matrix_event(
        delivery,
        "occupancy-open-event",
        event,
        logger=StructuredLogger(),
        decision_memory_path=tmp_path / "operator-decision-memory.json",
    )

    assert error is None
    assert delivery.enqueued == [event]
    assert delivery.sent == []


def test_dispatch_matrix_open_alert_enqueue_records_queued_memory(tmp_path: Path) -> None:
    class EnqueueOnlyDelivery:
        def enqueue_open_spot_alert(self, event: dict[str, Any]) -> object:
            return object()

        def send_open_spot_alert(self, event: dict[str, Any]) -> None:
            raise AssertionError("frame dispatch should not drain open alerts")

    event = {
        "event_type": "occupancy-open-event",
        "event_id": "occupancy-open-event:left_spot:2026-05-18T20:01:02Z",
        "spot_id": "left_spot",
        "observed_at": "2026-05-18T20:01:02Z",
        "snapshot_path": str(tmp_path / "latest.jpg"),
    }
    memory_path = tmp_path / "operator-decision-memory.json"

    error = dispatch_matrix_event(
        EnqueueOnlyDelivery(),
        "occupancy-open-event",
        event,
        logger=StructuredLogger(),
        decision_memory_path=memory_path,
    )

    records = json.loads(memory_path.read_text(encoding="utf-8"))["records"]
    assert error is None
    assert len(records) == 1
    assert records[0]["summary"] == "occupancy-open-event queued"
    assert records[0]["details"]["outcome"] == "queued"
    assert records[0]["details"]["reason"] == "outbox_enqueue"


def test_dispatch_matrix_alert_flushes_service_decision_store_immediately(tmp_path: Path) -> None:
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
    from parking_spot_monitor.operator_decision_memory import make_decision_memory_record

    memory_path = tmp_path / "operator-decision-memory.json"
    store = DecisionMemoryStore(
        memory_path,
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=50,
    )
    store.append(
        make_decision_memory_record("miss", spot_id="left_spot", summary="routine"),
        durability="routine",
    )
    event = {
        "event_type": "occupancy-state-changed",
        "event_id": "state:left_spot:1",
        "spot_id": "left_spot",
    }

    dispatch_matrix_event(
        None,
        event["event_type"],
        event,
        logger=StructuredLogger(),
        decision_memory_store=store,
    )

    assert [record.summary for record in load_decision_memory(memory_path).records] == [
        "routine",
        "occupancy-state-changed skipped",
    ]


def test_dispatch_matrix_occupied_alert_uses_durable_snapshot_enqueue(tmp_path: Path) -> None:
    class EnqueueOnlyDelivery:
        def __init__(self) -> None:
            self.enqueued: list[dict[str, Any]] = []

        def enqueue_occupied_spot_alert(self, event: dict[str, Any]) -> object:
            self.enqueued.append(dict(event))
            return object()

        def send_occupied_spot_alert(self, event: dict[str, Any]) -> None:
            raise AssertionError("frame dispatch must not send an occupied alert immediately")

    source = tmp_path / "occupied.jpg"
    Image.new("RGB", (10, 8), (12, 34, 56)).save(source, format="JPEG")
    event = {
        "event_type": "occupancy-occupied-event",
        "event_id": "occupancy-state-changed:left_spot:2026-05-18T20:01:02Z",
        "spot_id": "left_spot",
        "observed_at": "2026-05-18T20:01:02Z",
        "occupied_snapshot_path": str(source),
    }
    delivery = EnqueueOnlyDelivery()

    error = dispatch_matrix_event(delivery, event["event_type"], event, logger=StructuredLogger())

    assert error is None
    assert delivery.enqueued == [event]


@pytest.mark.parametrize(
    ("event_name", "event"),
    [
        (
            "quiet-window-started",
            {
                "event_type": "quiet-window-started",
                "event_id": "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00",
                "window_id": "street_sweeping:2026-05-18:13:00-15:00",
            },
        ),
        (
            "owner-vehicle-quiet-window-alert",
            {
                "event_type": "owner-vehicle-quiet-window-alert",
                "event_id": "owner-vehicle-quiet-window-alert:left_spot:prof-owner:window-1",
                "spot_id": "left_spot",
                "profile_id": "prof-owner",
                "window_id": "window-1",
                "observed_at": "2026-05-18T20:01:02Z",
                "owner_vehicle": {"label": "owner car"},
            },
        ),
    ],
)
def test_dispatch_matrix_frame_text_notices_use_durable_enqueue(
    event_name: str,
    event: dict[str, Any],
) -> None:
    class EnqueueOnlyDelivery:
        def __init__(self) -> None:
            self.enqueued: list[tuple[str, dict[str, Any]]] = []

        def enqueue_text_notice(self, queued_name: str, queued_event: dict[str, Any]) -> object:
            self.enqueued.append((queued_name, dict(queued_event)))
            return object()

        def send_quiet_window_notice(self, event: dict[str, Any]) -> None:
            raise AssertionError("frame dispatch must not send a quiet-window notice immediately")

        def send_owner_vehicle_quiet_window_alert(self, event: dict[str, Any]) -> None:
            raise AssertionError("frame dispatch must not send an owner notice immediately")

    delivery = EnqueueOnlyDelivery()

    error = dispatch_matrix_event(delivery, event_name, event, logger=StructuredLogger())

    assert error is None
    assert delivery.enqueued == [(event_name, event)]


def test_frame_update_network_delivery_runs_only_on_the_outbox_worker(tmp_path: Path) -> None:
    from parking_spot_monitor.runtime_state_update import _update_runtime_state_for_frame

    sent = threading.Event()

    class ThreadTrackingClient(FakeMatrixClient):
        def __init__(self) -> None:
            super().__init__()
            self.send_threads: list[str] = []

        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            self.send_threads.append(threading.current_thread().name)
            result = super().send_text(room_id=room_id, txn_id=txn_id, body=body)
            sent.set()
            return result

    settings = load_settings("config.yaml.example", environ=fake_environ())
    detection_result = DetectionFilterResult(
        by_spot={
            "left_spot": SpotDetectionResult(spot_id="left_spot", accepted=None, rejected=[]),
            "right_spot": SpotDetectionResult(spot_id="right_spot", accepted=None, rejected=[]),
        },
        rejection_counts={},
    )
    client = ThreadTrackingClient()
    delivery = MatrixOutboxDelivery(
        client=client,
        room_id="!room:example.org",
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        outbox=LocalOutbox(tmp_path / "matrix-outbox.json"),
    )
    delivery.start_worker(retry_interval_seconds=60)
    try:
        update = _update_runtime_state_for_frame(
            settings=settings,
            runtime_state=RuntimeState.default(["left_spot", "right_spot"]),
            detection_result=detection_result,
            observed_at=datetime(2026, 5, 18, 20, 30, tzinfo=timezone.utc),
            snapshot_path=str(tmp_path / "latest.jpg"),
            logger=StructuredLogger(),
            matrix_delivery=delivery,
            state_path=tmp_path / "state.json",
            configured_spot_ids=["left_spot", "right_spot"],
            owner_vehicle_snapshot_provider=OwnerVehicleRuntimeCache(
                tmp_path / "owner-vehicles.json",
                logger=StructuredLogger(),
            ),
        )

        assert update.matrix_errors == []
        assert sent.wait(2), "worker did not deliver the durable frame notice"
        assert client.send_threads == ["matrix-outbox-delivery"]
    finally:
        delivery.close()

    [record] = delivery.outbox.list_records()
    assert record.intent.event_id == "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00"
    assert record.phase_states == {"text": "delivered"}


@pytest.mark.parametrize("bootstrapped", [False, True])
def test_zero_count_matrix_command_success_uses_debug(
    bootstrapped: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from parking_spot_monitor.runtime_commands import _poll_matrix_commands_once

    class NoopCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult(bootstrapped=bootstrapped)

    _poll_matrix_commands_once(NoopCommandService(), logger=StructuredLogger(level="DEBUG"), iteration=1)

    success = [
        record
        for record in json_records(combined_output(capsys))
        if record.get("event") == "matrix-command-poll-succeeded"
    ]
    assert [record["level"] for record in success] == ["DEBUG"]


def test_nonzero_matrix_command_success_remains_info(capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.runtime_commands import _poll_matrix_commands_once

    class ProcessedCommandService:
        def poll_once(self) -> FakeCommandPollResult:
            return FakeCommandPollResult(processed_count=1)

    _poll_matrix_commands_once(ProcessedCommandService(), logger=StructuredLogger(), iteration=1)

    success = [
        record
        for record in json_records(combined_output(capsys))
        if record.get("event") == "matrix-command-poll-succeeded"
    ]
    assert [record["level"] for record in success] == ["INFO"]
