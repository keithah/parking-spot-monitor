from __future__ import annotations

from tests.support._matrix import *  # noqa: F403


def test_command_service_rejects_unauthorized_analytics_before_provider_or_artifacts() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$analytics", sender="@intruder:example", room_id=ROOM_ID, body="!parking analytics all"),))

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("unauthorized analytics replies must not upload media")

    def cockpit_provider(action: str, **kwargs: Any) -> str:
        raise AssertionError("unauthorized analytics must not touch cockpit provider or artifact paths")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == ["Command rejected: sender is not authorized."]


def test_command_service_parse_errors_use_configured_command_prefix() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$analytics", sender="@op:example", room_id=ROOM_ID, body=".park analytics tomorrow"),
                    MatrixTextEvent(event_id="$confidence", sender="@op:example", room_id=ROOM_ID, body=".park confidence now"),
                    MatrixTextEvent(event_id="$learn", sender="@op:example", room_id=ROOM_ID, body=".park learn left_spot open 2026-05-18T19:00:00Z"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(str(kwargs["body"]))
            return "$reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        command_prefix=".park",
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 3
    assert replies == [
        "Command rejected: usage: .park analytics [today|7d|30d|all]",
        "Command rejected: usage: .park confidence",
        "Command rejected: usage: .park learn <spot_id> <open|occupied> at <time>",
    ]


def test_command_service_analytics_context_reads_vehicle_history_text_only_and_does_not_mutate_archive(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent

    closed_dir = tmp_path / "vehicle-history" / "sessions" / "closed"
    active_dir = tmp_path / "vehicle-history" / "sessions" / "active"
    closed_dir.mkdir(parents=True)
    active_dir.mkdir(parents=True)
    closed_session = closed_dir / "sess_closed.json"
    active_session = active_dir / "sess_active.json"
    malformed = closed_dir / "malformed.json"
    closed_session.write_text(
        json.dumps(
            {
                "session_id": "sess_closed",
                "spot_id": "left_spot",
                "started_at": "2026-05-18T08:00:00Z",
                "ended_at": "2026-05-18T09:30:00Z",
                "duration_seconds": 5400,
            }
        ),
        encoding="utf-8",
    )
    active_session.write_text(
        json.dumps({"session_id": "sess_active", "spot_id": "right_spot", "started_at": "2026-05-19T10:00:00Z", "ended_at": None}),
        encoding="utf-8",
    )
    malformed.write_text("{not json", encoding="utf-8")
    before = {path: (path.read_text(encoding="utf-8"), path.stat().st_mtime_ns) for path in [closed_session, active_session, malformed]}
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$analytics", sender="@op:example", room_id=ROOM_ID, body="!parking analytics all"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("analytics context replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("analytics context replies must not send media")

    context = MatrixOperatorCockpitContext(
        settings=object(),
        data_dir=tmp_path,
        health_path=tmp_path / "health.json",
        state_path=tmp_path / "state.json",
    )
    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_context=context,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text"]
    assert calls[0]["txn_id"] == "command:$analytics"
    body = calls[0]["body"]
    assert "Parking occupancy analytics" in body
    assert "Window: all" in body
    assert "left_spot\n- Sessions: 1" in body
    assert "\n\nright_spot\n- Sessions: 1" in body
    assert "malformed vehicle-history session ignored" in body
    assert "No detector, camera, Matrix media upload, alert emission, or state mutation was run." in body
    assert len(body.encode("utf-8")) <= 4096
    assert ACCESS_TOKEN not in body
    assert str(tmp_path) not in body
    for path, (content, mtime_ns) in before.items():
        assert path.read_text(encoding="utf-8") == content
        assert path.stat().st_mtime_ns == mtime_ns


def test_command_service_rejects_unauthorized_confidence_before_provider_or_artifacts() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$confidence", sender="@intruder:example", room_id=ROOM_ID, body="!parking confidence"),))

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("unauthorized confidence replies must not upload media")

    def cockpit_provider(action: str, **kwargs: Any) -> str:
        raise AssertionError("unauthorized confidence must not touch cockpit provider or artifact paths")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert replies == ["Command rejected: sender is not authorized."]


def test_command_service_why_explain_recent_context_reads_decision_memory_safely_text_only(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "accepted_evidence",
            observed_at="2026-05-18T19:00:00Z",
            spot_id="right_spot",
            summary="accepted parked vehicle evidence",
            details={"hit_streak": 4, "token": ACCESS_TOKEN, "rtsp_url": "rtsp://user:pass@example/camera"},
        ),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record("command_outcome", observed_at="2026-05-18T19:01:00Z", summary="command processed", details={"outcome": "ok"}),
    )
    state_path = tmp_path / "state.json"
    state_path.write_text('{"schema_version":"test","state_by_spot":{}}', encoding="utf-8")
    state_before = state_path.read_text(encoding="utf-8")
    state_mtime_before_ns = state_path.stat().st_mtime_ns
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$why", sender="@op:example", room_id=ROOM_ID, body="!parking why right_spot"),
                    MatrixTextEvent(event_id="$explain", sender="@op:example", room_id=ROOM_ID, body="!parking explain right_spot"),
                    MatrixTextEvent(event_id="$unknown", sender="@op:example", room_id=ROOM_ID, body="!parking explain unknown_spot"),
                    MatrixTextEvent(event_id="$recent", sender="@op:example", room_id=ROOM_ID, body="!parking recent"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("why/recent context replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("why/recent context replies must not send media")

    context = MatrixOperatorCockpitContext(
        settings=object(),
        data_dir=tmp_path,
        health_path=tmp_path / "health.json",
        state_path=state_path,
    )
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_context=context,
    )

    result = service.poll_once()
    rendered = "\n".join(call["body"] for call in calls)

    assert result.processed_count == 4
    assert result.error_count == 0
    assert service.archive.calls == []
    assert state_path.read_text(encoding="utf-8") == state_before
    assert state_path.stat().st_mtime_ns == state_mtime_before_ns
    assert "Parking decision memory for right_spot" in rendered
    assert "accepted parked vehicle evidence" in rendered
    assert "hit_streak: 4" in rendered
    assert "Parking decision memory for unknown_spot" in rendered
    assert "No recent decision memory for this spot" in rendered
    assert "Parking decision memory recent" in rendered
    assert "command_outcome" in rendered
    assert ACCESS_TOKEN not in rendered
    assert "rtsp://" not in rendered
    assert all(call["kind"] == "text" for call in calls)
    assert all(len(call["body"].encode("utf-8")) <= 4096 for call in calls)


def test_command_service_recent_missing_context_is_safe_configuration_failure() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$recent", sender="@op:example", room_id=ROOM_ID, body="!parking recent"),))

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


def test_parse_matrix_lab_commands_are_exact_and_reject_untrusted_arguments() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    replay = parse_matrix_command("  !parking   lab   run   replay  ")
    tuning = parse_matrix_command("!parking lab run tuning")
    status = parse_matrix_command("!parking lab status")
    latest = parse_matrix_command("!parking lab status latest")
    specific = parse_matrix_command("!parking lab status lab-20260518T190000Z-abcdef12")

    assert (replay.action, replay.lab_kind) == ("lab_run", "replay")
    assert (tuning.action, tuning.lab_kind) == ("lab_run", "tuning")
    assert (status.action, status.lab_job_id) == ("lab_status", "latest")
    assert (latest.action, latest.lab_job_id) == ("lab_status", "latest")
    assert (specific.action, specific.lab_job_id) == ("lab_status", "lab-20260518T190000Z-abcdef12")

    rejected = [
        "!parking lab",
        "!parking lab run",
        "!parking lab run replay now",
        "!parking lab run /tmp/replay",
        "!parking lab run ../replay",
        "!parking lab run unknown",
        "!parking lab status latest extra",
        "!parking lab status ../status.json",
        "!parking lab status /tmp/status.json",
        "!parking lab status lab-20260518T190000Z-ABCDEF12",
        "!parking lab status lab-20260518T190000Z-abc",
        "!parking lab status " + "x" * 600,
    ]
    for body in rejected:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_command_service_lab_commands_use_provider_text_only_repeatably_without_archive_correction() -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []
    provider_calls: list[tuple[str, str | None, str | None]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$lab", sender="@op:example", room_id=ROOM_ID, body="!parking lab run replay"),
                    MatrixTextEvent(event_id="$lab", sender="@op:example", room_id=ROOM_ID, body="!parking lab run replay"),
                    MatrixTextEvent(event_id="$status", sender="@op:example", room_id=ROOM_ID, body="!parking lab status lab-20260518T190000Z-abcdef12"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("lab command replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("lab command replies must not send media")

    def cockpit_provider(action: str, *, lab_kind: str | None = None, lab_job_id: str | None = None) -> MatrixCommandResponse:
        provider_calls.append((action, lab_kind, lab_job_id))
        if action == "lab_run":
            return MatrixCommandResponse(text=f"Detection lab job started\nKind: {lab_kind}\nJob: lab-20260518T190000Z-abcdef12")
        return MatrixCommandResponse(text=f"Detection lab status\nJob: {lab_job_id}\nStatus: succeeded")

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$lab"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 3
    assert result.error_count == 0
    assert provider_calls == [
        ("lab_run", "replay", None),
        ("lab_run", "replay", None),
        ("lab_status", None, "lab-20260518T190000Z-abcdef12"),
    ]
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text", "text", "text"]
    assert [call["txn_id"] for call in calls] == ["command:$lab", "command:$lab", "command:$status"]
    assert all("Detection lab" in call["body"] for call in calls)


def test_command_service_rejects_unauthorized_lab_before_provider_or_paths() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$run", sender="@intruder:example", room_id=ROOM_ID, body="!parking lab run replay"),
                    MatrixTextEvent(event_id="$status", sender="@intruder:example", room_id=ROOM_ID, body="!parking lab status latest"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("unauthorized lab replies must not upload media")

    def cockpit_provider(action: str, **kwargs: Any) -> str:
        raise AssertionError("unauthorized lab command must not touch provider or lab paths")

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@operator:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
        unauthorized_reply_cooldown_seconds=0,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 2
    assert replies == ["Command rejected: sender is not authorized.", "Command rejected: sender is not authorized."]


def test_command_service_lab_context_routes_to_manager_safely_text_only(tmp_path: Path) -> None:
    from parking_spot_monitor.detection_lab import REPLAY_CONFIG_FILENAME, REPLAY_LABELS_FILENAME
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent

    lab_root = tmp_path / "detection-lab"
    lab_root.mkdir()
    (lab_root / REPLAY_LABELS_FILENAME).write_text("{}", encoding="utf-8")
    (lab_root / REPLAY_CONFIG_FILENAME).write_text("{}", encoding="utf-8")
    calls: list[dict[str, Any]] = []

    def replay_runner(inputs: dict[str, Path]) -> dict[str, Any]:
        report = inputs["job_dir"] / "replay-report.json"
        report.write_text(
            json.dumps(
                {
                    "schema_version": "test.v1",
                    "status_counts": {"passed": 2, "failed": 1},
                    "coverage": {"assessed_frames": 3, "blocked_frames": 0, "not_assessed_frames": 0},
                    "redaction_scan": {"passed": True, "findings": []},
                    "token": ACCESS_TOKEN,
                }
            ),
            encoding="utf-8",
        )
        return report

    from parking_spot_monitor.detection_lab import DetectionLabManager

    manager = DetectionLabManager(tmp_path, replay_runner=replay_runner)

    class Client:
        poll = 0

        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            self.poll += 1
            if self.poll == 1:
                return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$run", sender="@op:example", room_id=ROOM_ID, body="!parking lab run replay"),))
            return MatrixSyncResult(next_batch="s4", events=(MatrixTextEvent(event_id="$status", sender="@op:example", room_id=ROOM_ID, body="!parking lab status latest"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("lab context replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("lab context replies must not send media")

    context = MatrixOperatorCockpitContext(
        settings=object(),
        data_dir=tmp_path,
        health_path=tmp_path / "health.json",
        state_path=tmp_path / "state.json",
        detection_lab_manager=manager,
    )
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_context=context,
    )

    first = service.poll_once()
    import time

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        status_text = context.lab_status_reply("latest").text
        if "Detection lab status\n" in status_text and "Status: succeeded" in status_text:
            break
        time.sleep(0.01)
    second = service.poll_once()
    rendered = "\n".join(call["body"] for call in calls)

    assert first.processed_count == 1
    assert second.processed_count == 1
    assert "Detection lab job started" in rendered
    assert "Detection lab status" in rendered
    assert "passed=2" in rendered
    assert "coverage: assessed 3" in rendered
    assert ACCESS_TOKEN not in rendered
    assert all(call["kind"] == "text" for call in calls)
    assert all(len(call["body"].encode("utf-8")) <= 4096 for call in calls)


@pytest.mark.parametrize(
    "body",
    [
        "!parking status extra",
        "!parking config verbose",
        "!parking latest ../debug_latest.jpg",
        "!parking why ../state.json",
        "!parking explain ../state.json",
        "!parking explain right_spot extra",
        "!parking recent now",
        "!parking confidence now",
        "!parking lab run replay; rm -rf /",
        "!parking lab status ../../status.json",
        "!parking profile summary prof_a extra",
        "!parking profile merge prof_a prof_a",
        "!parking owner ../left_spot",
        "!parking who now",
        "!parking help please",
    ],
)
def test_command_service_malformed_authorized_commands_fail_closed_before_provider_or_archive(body: str) -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$bad", sender="@op:example", room_id=ROOM_ID, body=body),))

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("malformed commands must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("malformed commands must not send media")

    def cockpit_provider(action: str, **kwargs: Any) -> str:
        raise AssertionError("malformed commands must not reach cockpit provider")

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert archive.calls == []
    assert len(replies) == 1
    assert replies[0].startswith("Command rejected: ")
    assert ACCESS_TOKEN not in replies[0]


def test_command_service_latest_media_delivery_failure_is_sanitized_text_failure(tmp_path: Path) -> None:
    from io import StringIO

    from parking_spot_monitor.logging import StructuredLogger
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixError, MatrixSyncResult, MatrixTextEvent

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(latest_path, size=(9, 5))
    log_stream = StringIO()
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$latest", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", "filename": kwargs["filename"], "data_len": len(kwargs["data"])})
            raise MatrixError("upload failed", error_type="http_status", status_code=500, access_token=ACCESS_TOKEN, response_body="raw body " + ACCESS_TOKEN)

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("image event must not be sent after upload failure")

    def cockpit_provider(action: str) -> MatrixCommandResponse:
        assert action == "latest"
        return MatrixCommandResponse(
            text="Parking monitor latest\nSnapshot: fresh raw latest.jpg; 9x5",
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 9, "h": 5},
        )

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
        logger=StructuredLogger(stream=log_stream),
    )

    result = service.poll_once()
    rendered = json.dumps(calls) + log_stream.getvalue()

    assert result.processed_count == 0
    assert result.error_count == 1
    assert [call["kind"] for call in calls] == ["text", "upload", "text"]
    assert calls[0]["txn_id"] == "command:$latest:text"
    assert calls[2] == {"kind": "text", "room_id": ROOM_ID, "txn_id": "command:$latest", "body": "Command failed: MatrixError"}
    assert ACCESS_TOKEN not in rendered
    assert "raw body" not in rendered
    assert raw_bytes.hex() not in rendered


def test_default_matrix_factories_wire_safety_config_and_resolved_outbox_path(tmp_path: Path) -> None:
    from parking_spot_monitor.__main__ import _default_matrix_command_service_factory, _default_matrix_delivery_factory
    from parking_spot_monitor.config import load_settings
    from parking_spot_monitor.paths import resolve_runtime_paths

    config_path = tmp_path / "config.yaml"
    config_text = (
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("command_authorized_senders: []", "command_authorized_senders: ['@op:example']")
        .replace("retry_jitter_ratio: 0.2", "retry_jitter_ratio: 0.75")
        .replace("unauthorized_reply_cooldown_seconds: 300", "unauthorized_reply_cooldown_seconds: 0")
    )
    config_path.write_text(config_text, encoding="utf-8")
    settings = load_settings(config_path, environ=stream_env())
    paths = resolve_runtime_paths(settings, tmp_path)

    service = _default_matrix_command_service_factory(
        settings,
        tmp_path,
        logger=None,
        archive=FakeCommandArchive(),
        incident_detector=object(),
    )  # type: ignore[arg-type]
    delivery = _default_matrix_delivery_factory(settings, tmp_path, logger=None)  # type: ignore[arg-type]

    assert service is not None
    assert service.cockpit_context is not None
    assert service.cockpit_context.matrix_outbox_path == paths.matrix_outbox_file
    assert service.client.retry_jitter_ratio == 0.75
    assert service.unauthorized_reply_cooldown_seconds == 0
    assert delivery.client.retry_jitter_ratio == 0.75
