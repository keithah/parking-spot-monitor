from __future__ import annotations

from tests.support._matrix import *  # noqa: F403
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
        who_snapshot_provider=lambda base_reply: base_reply,
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
        who_snapshot_provider=lambda base_reply: base_reply,
        feedback_labeler=FeedbackLabeler(),
        unauthorized_reply_cooldown_seconds=0,
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
        who_snapshot_provider=lambda base_reply: base_reply,
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
        who_snapshot_provider=lambda base_reply: base_reply,
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
        who_snapshot_provider=lambda base_reply: base_reply,
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
        who_snapshot_provider=lambda base_reply: base_reply,
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
    service = MatrixCommandService(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=[], who_snapshot_provider=lambda base_reply: base_reply)  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert archive.calls == []
    assert replies == ["Command rejected: sender is not authorized."]
