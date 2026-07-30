from __future__ import annotations

from tests.support._matrix import *  # noqa: F403


def test_quiet_notice_text_is_deterministic_and_contextual() -> None:
    assert format_quiet_window_notice(
        {
            "event_type": "quiet-window-upcoming",
            "event_id": "quiet-window-upcoming:street_sweeping:2026-05-18:13:00-15:00:60m",
            "window_id": "street_sweeping:2026-05-18:13:00-15:00",
            "reminder_minutes_before": 60,
        }
    ) == "Street sweeping starts in 1 hour: street_sweeping:2026-05-18:13:00-15:00"
    assert format_quiet_window_notice(
        {
            "event_type": "quiet-window-started",
            "event_id": "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00",
            "window_id": "street_sweeping:2026-05-18:13:00-15:00",
        }
    ) == "Street sweeping started: street_sweeping:2026-05-18:13:00-15:00"
    assert format_quiet_window_notice(
        {
            "event_type": "quiet-window-ended",
            "event_id": "quiet-window-ended:street_sweeping:2026-05-18:13:00-15:00",
            "window_id": "street_sweeping:2026-05-18:13:00-15:00",
        }
    ) == "Street sweeping ended: street_sweeping:2026-05-18:13:00-15:00"


def test_owner_vehicle_quiet_window_alert_text_and_event_id_are_concise() -> None:
    event = {
        "event_type": "owner-vehicle-quiet-window-alert",
        "spot_id": "right_spot",
        "observed_at": "2026-05-18T20:05:06Z",
        "window_id": "street_sweeping:2026-05-18:13:00-15:00",
        "profile_id": "prof_tesla",
        "owner_vehicle": {
            "label": "Keith's black Tesla",
            "description": "black Tesla, tinted windows, roof rack",
        },
    }

    assert owner_vehicle_quiet_window_event_id(event) == (
        "owner-vehicle-quiet-window-alert:right_spot:prof_tesla:street_sweeping:2026-05-18:13:00-15:00"
    )
    assert format_owner_vehicle_quiet_window_alert(event) == (
        "Street cleaning alert: Keith's black Tesla is parked in right_spot at "
        "2026-05-18 1:05:06 PM PDT during street_sweeping:2026-05-18:13:00-15:00."
    )


def test_send_text_retries_transient_http_statuses_and_logs_retry_decisions() -> None:
    from io import StringIO
    from parking_spot_monitor.logging import StructuredLogger

    seen_statuses = [500, 429, 200]
    sleeps: list[float] = []
    stream = StringIO()

    def handler(request: httpx.Request) -> httpx.Response:
        status = seen_statuses.pop(0)
        if status == 200:
            return httpx.Response(200, json={"event_id": "$event:example.org"}, request=request)
        return httpx.Response(status, json={"errcode": "M_LIMIT_EXCEEDED", "error": f"raw {ACCESS_TOKEN}"}, request=request)

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=3,
        retry_backoff_seconds=0.25,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        random_unit=lambda: 0.0,
        logger=StructuredLogger(stream=stream),
    )

    assert client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open") == "$event:example.org"

    output = stream.getvalue()
    records = [json.loads(line) for line in output.splitlines()]
    assert sleeps == [0.25, 0.5]
    assert [record["event"] for record in records] == ["matrix-request-retry", "matrix-request-retry"]
    assert [record["attempt"] for record in records] == [1, 2]
    assert [record["next_attempt"] for record in records] == [2, 3]
    assert all(record["error_type"] == "http_status" for record in records)
    assert all(record["status_code"] in {500, 429} for record in records)
    assert ACCESS_TOKEN not in output
    assert "raw" not in output
    assert "Authorization" not in output


def test_send_text_retries_timeout_then_succeeds_without_leaking_exception_text() -> None:
    from io import StringIO
    from parking_spot_monitor.logging import StructuredLogger

    calls = 0
    sleeps: list[float] = []
    stream = StringIO()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.TimeoutException(f"timed out bearer {ACCESS_TOKEN}", request=request)
        return httpx.Response(200, json={"event_id": "$event:example.org"}, request=request)

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=2,
        retry_backoff_seconds=0.5,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        random_unit=lambda: 0.0,
        logger=StructuredLogger(stream=stream),
    )

    assert client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open") == "$event:example.org"
    assert sleeps == [0.5]
    output = stream.getvalue()
    assert '"event":"matrix-request-retry"' in output
    assert '"error_type":"timeout"' in output
    assert ACCESS_TOKEN not in output
    assert "bearer" not in output.lower()


def test_upload_image_retries_malformed_response_then_succeeds() -> None:
    responses = [
        httpx.Response(200, content=b"not json"),
        httpx.Response(200, json={"content_uri": "mxc://example.org/media-id"}),
    ]
    sleeps: list[float] = []
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        response = responses.pop(0)
        response.request = request
        return response

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=2,
        retry_backoff_seconds=0.1,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        random_unit=lambda: 0.0,
    )

    assert client.upload_image(filename="snapshot.jpg", data=b"jpeg", content_type="image/jpeg") == "mxc://example.org/media-id"
    assert sleeps == [0.1]
    assert [request.headers["content-type"] for request in seen] == ["image/jpeg", "image/jpeg"]


def test_retry_attempts_one_raises_final_error_without_sleep_or_retry_log() -> None:
    from io import StringIO
    from parking_spot_monitor.logging import StructuredLogger

    sleeps: list[float] = []
    stream = StringIO()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"errcode": "M_UNKNOWN", "error": f"body {ACCESS_TOKEN}"}, request=request)

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=1,
        retry_backoff_seconds=99,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        logger=StructuredLogger(stream=stream),
    )

    with pytest.raises(MatrixError) as exc_info:
        client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")

    rendered = str(exc_info.value) + repr(exc_info.value.diagnostics) + stream.getvalue()
    assert sleeps == []
    assert stream.getvalue() == ""
    assert exc_info.value.diagnostics["attempt"] == 1
    assert exc_info.value.diagnostics["status_code"] == 500
    assert ACCESS_TOKEN not in rendered
    assert "body" not in rendered


def test_sync_extracts_only_joined_room_text_events_and_requires_next_batch() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "next_batch": "s2",
                "rooms": {
                    "join": {
                        ROOM_ID: {
                            "timeline": {
                                "events": [
                                    {"type": "m.room.message", "event_id": "$1", "sender": "@op:example", "content": {"msgtype": "m.text", "body": "!parking profile summary prof_a"}},
                                    {"type": "m.room.message", "event_id": "$2", "sender": "@op:example", "content": {"msgtype": "m.image", "body": "image"}},
                                    {"type": "m.reaction", "event_id": "$3", "sender": "@op:example", "content": {}},
                                ]
                            }
                        },
                        "!other:example": {"timeline": {"events": [{"type": "m.room.message", "event_id": "$4", "sender": "@op:example", "content": {"msgtype": "m.text", "body": "wrong room"}}]}},
                    }
                },
            },
            request=request,
        )

    client = make_client(httpx.MockTransport(handler))

    result = client.sync(room_id=ROOM_ID, since="s1", timeout_ms=123, limit=7)

    assert result.next_batch == "s2"
    assert [(event.event_id, event.sender, event.body) for event in result.events] == [("$1", "@op:example", "!parking profile summary prof_a")]
    assert seen[0].method == "GET"
    assert seen[0].url.path == "/_matrix/client/v3/sync"
    assert seen[0].url.params["since"] == "s1"
    assert seen[0].url.params["timeout"] == "123"
    assert seen[0].url.params["limit"] == "7"


def test_sync_malformed_response_diagnostics_are_redacted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"rooms": {"join": {}}, "leak": ACCESS_TOKEN}, request=request)

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(MatrixError) as exc_info:
        client.sync(room_id=ROOM_ID)

    rendered = str(exc_info.value) + repr(exc_info.value.diagnostics)
    assert exc_info.value.diagnostics["error_type"] == "malformed_response"
    assert exc_info.value.diagnostics["missing_key"] == "next_batch"
    assert ACCESS_TOKEN not in rendered
    assert "leak" not in rendered


def test_parse_matrix_commands_are_strict_and_normalize_labels() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    assert parse_matrix_command("  !parking   profile   rename   prof_abc   Blue    hatchback  ").label == "Blue hatchback"
    merge = parse_matrix_command("!parking profile merge prof_source prof_target")
    assert (merge.action, merge.source_profile_id, merge.target_profile_id) == ("merge_profiles", "prof_source", "prof_target")
    wrong = parse_matrix_command("!parking wrong sess_123")
    assert (wrong.action, wrong.subject_id) == ("wrong_match", "sess_123")
    owner = parse_matrix_command("!parking owner right_spot")
    assert (owner.action, owner.subject_id) == ("assign_owner", "right_spot")
    who = parse_matrix_command("!parking who")
    assert who.action == "active_spot_assignments"
    help_command = parse_matrix_command("!parking help")
    assert help_command.action == "help"
    summary = parse_matrix_command("!parking profile summary prof_target")
    assert (summary.action, summary.profile_id) == ("profile_summary", "prof_target")
    correct = parse_matrix_command("  !parking   correct   left_spot   open  ")
    assert (correct.action, correct.spot_id, correct.actual_state) == ("correct_spot_state", "left_spot", "open")
    occupied_correct = parse_matrix_command("!parking correct right_spot occupied")
    assert (occupied_correct.action, occupied_correct.spot_id, occupied_correct.actual_state) == ("correct_spot_state", "right_spot", "occupied")
    false_alert = parse_matrix_command("  !parking   false-alert   right_spot   occupied  ")
    assert (false_alert.action, false_alert.spot_id, false_alert.actual_state) == ("correct_spot_state", "right_spot", "occupied")
    learn = parse_matrix_command("  !parking   learn   left_spot   occupied   at   2026-05-18T19:00:00Z  ")
    assert (learn.action, learn.spot_id, learn.actual_state, learn.subject_id) == ("learn_label", "left_spot", "occupied", "2026-05-18T19:00:00Z")
    missed_alert = parse_matrix_command("  !parking   missed-alert   left_spot   open   at   2026-05-18T19:00:00Z  ")
    assert (missed_alert.action, missed_alert.spot_id, missed_alert.actual_state, missed_alert.subject_id) == ("learn_label", "left_spot", "open", "2026-05-18T19:00:00Z")

    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking profile merge prof_a prof_b extra")
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking profile rename badid label")
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking profile summary prof_a extra")
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking unknown")
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("!parking profile rename prof_a " + "x" * 161)
    with pytest.raises(MatrixCommandParseError):
        parse_matrix_command("   ")

    rejected_correct_commands = [
        "!parking correct",
        "!parking correct left_spot",
        "!parking correct left_spot open extra",
        "!parking correct left_spot closed",
        "!parking correct ../left_spot open",
        "!parking correct left/spot occupied",
        "!parking correct . open",
        "!parking false-alert",
        "!parking false-alert left_spot",
        "!parking false-alert left_spot open extra",
        "!parking false-alert left_spot closed",
        "!parking false-alert ../left_spot open",
        "!parking false-alert left/spot occupied",
        "!parking false-alert . open",
    ]
    for body in rejected_correct_commands:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)

    rejected_learn_commands = [
        "!parking learn",
        "!parking learn left_spot",
        "!parking learn left_spot open",
        "!parking learn left_spot open 2026-05-18T19:00:00Z",
        "!parking learn left_spot closed at 2026-05-18T19:00:00Z",
        "!parking learn ../left_spot open at 2026-05-18T19:00:00Z",
        "!parking learn left_spot open at",
        "!parking learn left_spot open at 2026-05-18T19:00:00Z extra",
        "!parking missed-alert",
        "!parking missed-alert left_spot",
        "!parking missed-alert left_spot open",
        "!parking missed-alert left_spot open 2026-05-18T19:00:00Z",
        "!parking missed-alert left_spot closed at 2026-05-18T19:00:00Z",
        "!parking missed-alert ../left_spot open at 2026-05-18T19:00:00Z",
        "!parking missed-alert left_spot open at",
        "!parking missed-alert left_spot open at 2026-05-18T19:00:00Z extra",
    ]
    for body in rejected_learn_commands:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_parse_matrix_operator_cockpit_commands_are_exact_and_bounded() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    status = parse_matrix_command("  !parking   status  ")
    config = parse_matrix_command("\n!parking config\t")

    assert status.action == "status"
    assert config.action == "config"

    rejected = [
        "!parking status now",
        "!parking config verbose",
        "!parking stat",
        "!parking settings",
        "!park status",
        "parking status",
        "!!parking status",
        "!parking",
        "!parking status " + "x" * 513,
    ]
    for body in rejected:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)
