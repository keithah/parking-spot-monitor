from __future__ import annotations

from tests.support._matrix import *  # noqa: F403


def test_send_text_puts_room_message_with_encoded_segments_and_returns_event_id() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"event_id": "$event:example.org"})

    client = make_client(httpx.MockTransport(handler))

    event_id = client.send_text(room_id=ROOM_ID, txn_id=TXN_ID, body="Parking spot is open")

    assert event_id == "$event:example.org"
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "PUT"
    assert request.url.raw_path.decode("ascii") == "/_matrix/client/v3/rooms/%21parking-room%3Aexample.org/send/m.room.message/txn%2Fwith%20space%3F"
    assert request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
    assert request.headers["content-type"] == "application/json"
    assert request_json(request) == {"msgtype": "m.text", "body": "Parking spot is open"}


def test_sync_retries_transient_matrix_statuses() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(500, json={"errcode": "M_UNAVAILABLE"}, request=request)
        return httpx.Response(200, json={"next_batch": "batch-2", "rooms": {"join": {ROOM_ID: {"timeline": {"events": []}}}}})

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=2,
        retry_backoff_seconds=0.25,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        random_unit=lambda: 0.0,
    )

    result = client.sync(room_id=ROOM_ID, timeout_ms=0)

    assert result.next_batch == "batch-2"
    assert attempts == 2
    assert sleeps == [0.25]


def test_matrix_retry_honors_server_retry_after_ms_before_local_backoff() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, json={"errcode": "M_LIMIT_EXCEEDED", "retry_after_ms": 1750}, request=request)
        return httpx.Response(200, json={"event_id": "$event:example.org"}, request=request)

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        timeout_seconds=2,
        retry_attempts=2,
        retry_backoff_seconds=0.25,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
    )

    event_id = client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")

    assert event_id == "$event:example.org"
    assert sleeps == [1.75]


def test_retry_jitter_is_bounded_and_retry_after_is_minimum() -> None:
    from parking_spot_monitor.matrix_client import retry_delay

    assert retry_delay(
        attempt=1,
        backoff_seconds=10,
        retry_after_seconds=12,
        jitter_ratio=0.2,
        random_unit=lambda: 0.0,
    ) == 12
    assert retry_delay(
        attempt=1,
        backoff_seconds=10,
        retry_after_seconds=None,
        jitter_ratio=0.2,
        random_unit=lambda: 1.0,
    ) == 12


def test_matrix_client_retry_applies_injected_positive_jitter() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(500, json={"errcode": "M_UNAVAILABLE"}, request=request)
        return httpx.Response(200, json={"event_id": "$event:example.org"}, request=request)

    client = MatrixClient(
        homeserver=HOMESERVER,
        access_token=ACCESS_TOKEN,
        retry_attempts=2,
        retry_backoff_seconds=10,
        retry_jitter_ratio=0.2,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        random_unit=lambda: 1.0,
    )

    event_id = client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")

    assert event_id == "$event:example.org"
    assert attempts == 2
    assert sleeps == [12]


def test_sender_reply_limiter_admits_once_per_cooldown_and_stays_bounded() -> None:
    from parking_spot_monitor.matrix_commands import SenderReplyLimiter

    now = 100.0
    limiter = SenderReplyLimiter(
        cooldown_seconds=300,
        monotonic=lambda: now,
        max_senders=256,
    )

    assert limiter.admit("@sender:example.org") is True
    assert limiter.admit("@sender:example.org") is False
    now = 400.0
    assert limiter.admit("@sender:example.org") is True
    for index in range(300):
        limiter.admit(f"@sender-{index}:example.org")
    assert limiter.sender_count == 256


def test_sender_reply_limiter_zero_cooldown_admits_every_reply() -> None:
    from parking_spot_monitor.matrix_commands import SenderReplyLimiter

    limiter = SenderReplyLimiter(
        cooldown_seconds=0,
        monotonic=lambda: 100.0,
        max_senders=256,
    )

    assert limiter.admit("@sender:example.org") is True
    assert limiter.admit("@sender:example.org") is True


def test_sender_reply_limiter_evicts_least_recently_admitted_sender() -> None:
    from parking_spot_monitor.matrix_commands import SenderReplyLimiter

    now = 100.0
    limiter = SenderReplyLimiter(
        cooldown_seconds=300,
        monotonic=lambda: now,
        max_senders=256,
    )
    for index in range(256):
        assert limiter.admit(f"@sender-{index}:example.org") is True
    now = 400.0
    assert limiter.admit("@sender-0:example.org") is True
    assert limiter.admit("@new-sender:example.org") is True

    assert limiter.admit("@sender-1:example.org") is True
    assert limiter.admit("@sender-0:example.org") is False


def test_upload_image_posts_media_with_filename_query_and_returns_content_uri() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"content_uri": "mxc://example.org/media-id"})

    client = make_client(httpx.MockTransport(handler))

    content_uri = client.upload_image(filename="snapshot 1.jpg", data=b"jpeg-bytes", content_type="image/jpeg")

    assert content_uri == "mxc://example.org/media-id"
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert request.url.path == "/_matrix/media/v3/upload"
    assert request.url.params["filename"] == "snapshot 1.jpg"
    assert request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
    assert request.headers["content-type"] == "image/jpeg"
    assert request.content == b"jpeg-bytes"


def test_send_image_puts_image_room_message_with_info_and_returns_event_id() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"event_id": "$image-event:example.org"})

    client = make_client(httpx.MockTransport(handler))
    info = {"mimetype": "image/jpeg", "size": 1234, "w": 1458, "h": 806}

    event_id = client.send_image(
        room_id=ROOM_ID,
        txn_id="image-txn",
        body="Raw full-frame snapshot",
        content_uri="mxc://example.org/media-id",
        info=info,
    )

    assert event_id == "$image-event:example.org"
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "PUT"
    assert request.url.raw_path.decode("ascii") == "/_matrix/client/v3/rooms/%21parking-room%3Aexample.org/send/m.room.message/image-txn"
    assert request_json(request) == {
        "msgtype": "m.image",
        "body": "Raw full-frame snapshot",
        "url": "mxc://example.org/media-id",
        "info": info,
    }


def test_matrix_delivery_live_proof_sends_labelled_text_and_raw_image(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(source, size=(8, 6))
    seen: list[dict[str, Any]] = []

    class FakeClient:
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            seen.append({"kind": "text", "room_id": room_id, "txn_id": txn_id, "body": body})
            return "$text:example.org"

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            seen.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
            return "mxc://example.org/live-proof"

        def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
            seen.append(
                {
                    "kind": "image",
                    "room_id": room_id,
                    "txn_id": txn_id,
                    "body": body,
                    "content_uri": content_uri,
                    "info": dict(info),
                }
            )
            return "$image:example.org"

    delivery = MatrixDelivery(
        client=FakeClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=StructuredLogger(),
    )

    delivery.send_live_proof(latest_path=source, observed_at="2026-05-18T19:00:00Z", selected_mode="software")

    assert [item["kind"] for item in seen] == ["text", "upload", "image"]
    assert seen[0]["txn_id"] == "live-proof:2026-05-18T19:00:00Z:text"
    assert seen[0]["body"] == "LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at 2026-05-18 12:00:00 PM PDT (decode mode: software)."
    assert seen[1]["content_type"] == "image/jpeg"
    assert seen[1]["data"] == raw_bytes
    assert seen[2]["txn_id"] == "live-proof:2026-05-18T19:00:00Z:image"
    assert seen[2]["body"].startswith("LIVE PROOF / TEST IMAGE: raw full-frame camera snapshot")
    assert seen[2]["info"] == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 8, "h": 6}


def test_matrix_delivery_live_proof_image_uses_shared_upload_helper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source, size=(8, 6))
    seen: list[dict[str, Any]] = []

    class FakeClient:
        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            seen.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
            return "mxc://example.org/live-proof"

        def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
            seen.append({"kind": "image", "info": dict(info), "content_uri": content_uri})
            return "$image:example.org"

    def fake_upload(snapshot: Any, *, logger: Any) -> dict[str, Any]:
        return {"data": b"resized", "info": {"mimetype": "image/jpeg", "size": 7, "w": 4, "h": 3}}

    monkeypatch.setattr("parking_spot_monitor.matrix_delivery._matrix_snapshot_upload", fake_upload)
    delivery = MatrixDelivery(
        client=FakeClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=StructuredLogger(),
    )

    delivery.send_live_proof_image(latest_path=source, observed_at="2026-05-18T19:00:00Z", selected_mode="software")

    assert seen[0]["data"] == b"resized"
    assert seen[1]["info"] == {"mimetype": "image/jpeg", "size": 7, "w": 4, "h": 3}


def test_format_live_proof_text_is_visibly_labelled() -> None:
    assert format_live_proof_text(observed_at="2026-05-18T19:00:00Z", selected_mode="software") == (
        "LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at 2026-05-18 12:00:00 PM PDT (decode mode: software)."
    )


@pytest.mark.parametrize("status_code", [401, 403, 429, 500])
def test_matrix_error_contains_safe_http_diagnostics_without_token_or_raw_body(status_code: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            json={"errcode": "M_FORBIDDEN", "error": f"denied {ACCESS_TOKEN}"},
            request=request,
        )

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(MatrixError) as exc_info:
        client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")

    error = exc_info.value
    rendered = str(error) + repr(error.diagnostics)
    assert error.diagnostics["status_code"] == status_code
    assert error.diagnostics["errcode"] == "M_FORBIDDEN"
    assert error.diagnostics["error_type"] == "http_status"
    assert ACCESS_TOKEN not in rendered
    assert "denied" not in rendered


def test_matrix_error_reports_timeout_safely() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException(f"timed out with {ACCESS_TOKEN}", request=request)

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(MatrixError) as exc_info:
        client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")

    rendered = str(exc_info.value) + repr(exc_info.value.diagnostics)
    assert exc_info.value.diagnostics["error_type"] == "timeout"
    assert ACCESS_TOKEN not in rendered


@pytest.mark.parametrize(
    "operation,response_json,missing_key",
    [
        ("send_text", {}, "event_id"),
        ("upload_image", {}, "content_uri"),
    ],
)
def test_matrix_error_reports_malformed_responses_without_raw_body(
    operation: str, response_json: dict[str, Any], missing_key: str
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={**response_json, "leak": ACCESS_TOKEN}, request=request)

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(MatrixError) as exc_info:
        if operation == "send_text":
            client.send_text(room_id=ROOM_ID, txn_id="txn", body="Parking spot is open")
        else:
            client.upload_image(filename="snapshot.jpg", data=b"jpeg-bytes", content_type="image/jpeg")

    rendered = str(exc_info.value) + repr(exc_info.value.diagnostics)
    assert exc_info.value.diagnostics["error_type"] == "malformed_response"
    assert exc_info.value.diagnostics["missing_key"] == missing_key
    assert ACCESS_TOKEN not in rendered
    assert "leak" not in rendered


def test_send_text_rejects_empty_body_before_http_request() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("empty text should not be sent to Matrix")

    client = make_client(httpx.MockTransport(handler))

    with pytest.raises(ValueError, match="body must be non-empty"):
        client.send_text(room_id=ROOM_ID, txn_id="txn", body="  ")


def test_matrix_config_summary_includes_timeout_retry_and_backoff() -> None:
    from parking_spot_monitor.config import load_settings

    settings = load_settings(
        "config.yaml.example",
        environ=stream_env("rtsp://camera"),
    )

    summary = settings.sanitized_summary()["matrix"]

    assert summary["timeout_seconds"] == 10
    assert summary["retry_attempts"] == 3
    assert summary["retry_backoff_seconds"] == 1
    assert summary["retry_jitter_ratio"] == 0.2
    assert summary["unauthorized_reply_cooldown_seconds"] == 300
    assert ACCESS_TOKEN not in repr(summary)
