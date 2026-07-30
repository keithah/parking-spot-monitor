from __future__ import annotations

from tests.support._matrix import *  # noqa: F403


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
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@op:example"], who_snapshot_provider=lambda base_reply: base_reply, bot_user_id="@bot:example")  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.bootstrapped is True
    assert result.processed_count == 0
    assert archive.calls == []
    assert archive.cursor_writes == [{"next_batch": "s2"}]


def test_command_service_bootstrap_sync_remains_info_transition() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult

    stream = StringIO()
    service = MatrixCommandService(
        client=object(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor=None),
        room_id=ROOM_ID,
        authorized_senders=(),
        who_snapshot_provider=lambda base_reply: base_reply,
        logger=StructuredLogger(level="DEBUG", stream=stream),
    )

    service.apply_sync_result(MatrixSyncResult(next_batch="s1", events=()))

    sync_records = [
        record
        for record in map(json.loads, stream.getvalue().splitlines())
        if record["event"] == "matrix-command-sync"
    ]
    assert sync_records == [
        {
            "event": "matrix-command-sync",
            "level": "INFO",
            "phase": "bootstrap",
            "next_batch_present": True,
            "processed_count": 0,
            "ignored_count": 0,
        }
    ]


@pytest.mark.parametrize(
    ("event", "authorized_senders", "expected_counts", "expected_level"),
    [
        (None, (), (0, 0, 0), "DEBUG"),
        (
            MatrixTextEvent(event_id="$processed", sender="@op:example", room_id=ROOM_ID, body="!parking help"),
            ("@op:example",),
            (1, 0, 0),
            "INFO",
        ),
        (
            MatrixTextEvent(event_id="$ignored", sender="@op:example", room_id=ROOM_ID, body="ordinary text"),
            ("@op:example",),
            (0, 1, 0),
            "INFO",
        ),
        (
            MatrixTextEvent(event_id="$error", sender="@intruder:example", room_id=ROOM_ID, body="!parking help"),
            ("@op:example",),
            (0, 0, 1),
            "INFO",
        ),
    ],
    ids=("empty", "processed", "ignored", "error"),
)
def test_command_service_apply_sync_log_level_depends_on_work_counts(
    event: MatrixTextEvent | None,
    authorized_senders: tuple[str, ...],
    expected_counts: tuple[int, int, int],
    expected_level: str,
) -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult

    class Client:
        def send_text(self, **_kwargs: Any) -> str:
            return "$reply"

    stream = StringIO()
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s0"}),
        room_id=ROOM_ID,
        authorized_senders=authorized_senders,
        who_snapshot_provider=lambda base_reply: base_reply,
        logger=StructuredLogger(level="DEBUG", stream=stream),
    )

    result = service.apply_sync_result(
        MatrixSyncResult(next_batch="s1", events=() if event is None else (event,))
    )

    assert (result.processed_count, result.ignored_count, result.error_count) == expected_counts
    sync_records = [
        record
        for record in map(json.loads, stream.getvalue().splitlines())
        if record["event"] == "matrix-command-sync"
    ]
    assert len(sync_records) == 1
    assert sync_records[0]["level"] == expected_level


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
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@op:example"], who_snapshot_provider=lambda base_reply: base_reply, bot_user_id="@bot:example")  # type: ignore[arg-type]

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


def test_command_service_reports_processed_read_only_commands() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$help", sender="@op:example", room_id=ROOM_ID, body="!parking help"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert len(replies) == 1


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
        def correction_event_seen(self, event_id: str) -> bool:
            raise PermissionError("corrections unreadable")

    archive = Archive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@op:example"], who_snapshot_provider=lambda base_reply: base_reply, bot_user_id="@bot:example")  # type: ignore[arg-type]

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
        who_snapshot_provider=lambda base_reply: base_reply,
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
        who_snapshot_provider=lambda base_reply: base_reply,
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
