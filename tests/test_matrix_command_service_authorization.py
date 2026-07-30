from __future__ import annotations

from tests.support._matrix import *  # noqa: F403
def test_command_service_rejects_unauthorized_status_before_application() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$status", sender="@intruder:example", room_id=ROOM_ID, body="!parking status"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    class Service(MatrixCommandService):
        def _apply_command(self, *args: Any, **kwargs: Any) -> str:
            raise AssertionError("unauthorized status must be rejected before application")

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = Service(client=Client(), archive=archive, room_id=ROOM_ID, authorized_senders=["@operator:example"], who_snapshot_provider=lambda base_reply: base_reply)  # type: ignore[arg-type]

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert archive.calls == []
    assert replies == [{"room_id": ROOM_ID, "txn_id": "command:$status", "body": "Command rejected: sender is not authorized."}]


def test_command_service_unauthorized_rejection_cooldown_consumes_every_event() -> None:
    from io import StringIO

    from parking_spot_monitor.logging import StructuredLogger
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    now = 100.0
    replies: list[dict[str, Any]] = []
    log_stream = StringIO()
    sync_results = [
        MatrixSyncResult(
            next_batch="s3",
            events=(
                MatrixTextEvent(event_id="$denied-1", sender="@intruder:example", room_id=ROOM_ID, body="!parking status"),
                MatrixTextEvent(event_id="$denied-2", sender="@intruder:example", room_id=ROOM_ID, body="!parking status"),
            ),
        ),
        MatrixSyncResult(
            next_batch="s4",
            events=(
                MatrixTextEvent(event_id="$denied-3", sender="@intruder:example", room_id=ROOM_ID, body="!parking status"),
            ),
        ),
    ]

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return sync_results.pop(0)

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        unauthorized_reply_cooldown_seconds=300,
        monotonic=lambda: now,
        logger=StructuredLogger(stream=log_stream),
    )

    first_result = service.poll_once()
    now = 400.0
    later_result = service.poll_once()

    assert first_result.error_count == 2
    assert later_result.error_count == 1
    assert archive.cursor_writes[-2:] == [{"next_batch": "s3"}, {"next_batch": "s4"}]
    assert replies == [
        {"room_id": ROOM_ID, "txn_id": "command:$denied-1", "body": "Command rejected: sender is not authorized."},
        {"room_id": ROOM_ID, "txn_id": "command:$denied-3", "body": "Command rejected: sender is not authorized."},
    ]
    denial_records = [
        record
        for record in (json.loads(line) for line in log_stream.getvalue().splitlines())
        if record["event"] == "matrix-command-denied"
    ]
    assert [record["event_id"] for record in denial_records] == ["$denied-1", "$denied-3"]


def test_command_service_authorized_status_and_config_reply_via_command_txn_path() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    applied_actions: list[str] = []
    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$status", sender="@operator:example", room_id=ROOM_ID, body="!parking status"),
                    MatrixTextEvent(event_id="$config", sender="@operator:example", room_id=ROOM_ID, body="!parking config"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    def cockpit_provider(action: str, **kwargs: str) -> str:
        applied_actions.append(action)
        return f"reply for {action}"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 2
    assert result.error_count == 0
    assert applied_actions == ["status", "config"]
    assert replies == [
        {"room_id": ROOM_ID, "txn_id": "command:$status", "body": "reply for status"},
        {"room_id": ROOM_ID, "txn_id": "command:$config", "body": "reply for config"},
    ]


def test_command_service_status_and_config_use_cockpit_provider_without_archive_corrections() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []
    provider_actions: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$repeat", sender="@op:example", room_id=ROOM_ID, body="!parking status"),
                    MatrixTextEvent(event_id="$repeat", sender="@op:example", room_id=ROOM_ID, body="!parking config"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    def cockpit_provider(action: str) -> str:
        provider_actions.append(action)
        return f"cockpit {action} reply"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$repeat"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 2
    assert result.error_count == 0
    assert provider_actions == ["status", "config"]
    assert archive.calls == []
    assert [reply["body"] for reply in replies] == ["cockpit status reply", "cockpit config reply"]


def test_command_service_status_provider_failure_replies_safe_failure() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$status", sender="@op:example", room_id=ROOM_ID, body="!parking status"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(dict(kwargs))
            return "$reply"

    def failing_provider(action: str) -> str:
        raise RuntimeError(f"boom {ACCESS_TOKEN} {action}")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=failing_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == [{"room_id": ROOM_ID, "txn_id": "command:$status", "body": "Command failed: RuntimeError"}]
    assert ACCESS_TOKEN not in replies[0]["body"]


def test_command_service_missing_cockpit_provider_is_deterministic_configuration_failure() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(MatrixTextEvent(event_id="$config", sender="@op:example", room_id=ROOM_ID, body="!parking config"),),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == ["Command failed: RuntimeError"]
