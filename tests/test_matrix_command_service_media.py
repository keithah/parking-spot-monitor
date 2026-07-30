from __future__ import annotations

from tests.support._matrix import *  # noqa: F403


def test_parse_matrix_latest_command_is_exact_and_rejects_arguments() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    latest = parse_matrix_command("  !parking   latest  ")

    assert latest.action == "latest"
    for body in ["!parking latest now", "!parking latest debug", "!parking latest ../debug_latest.jpg"]:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_parse_matrix_analytics_command_accepts_bounded_windows_and_rejects_extra_args() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    default = parse_matrix_command("  !parking   analytics  ")
    assert (default.action, default.subject_id) == ("analytics", "7d")

    for window in ["today", "7d", "30d", "all"]:
        command = parse_matrix_command(f"!parking analytics {window}")
        assert (command.action, command.subject_id) == ("analytics", window)

    rejected = [
        "!parking analytics today extra",
        "!parking analytics yesterday",
        "!parking analytics 90d",
        "!parking analytics ../sessions",
        "!parking analytics " + "x" * 513,
    ]
    for body in rejected:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_parse_matrix_confidence_command_is_exact_and_rejects_arguments() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    confidence = parse_matrix_command("  !parking   confidence  ")

    assert confidence.action == "confidence"
    for body in ["!parking confidence now", "!parking confidence verbose", "!parking confidence ../state.json"]:
        with pytest.raises(MatrixCommandParseError) as exc_info:
            parse_matrix_command(body)
        assert "usage: !parking confidence" in str(exc_info.value)


def test_command_service_authorized_latest_sends_text_and_one_raw_image_without_archive_correction(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(latest_path, size=(11, 7))
    calls: list[dict[str, Any]] = []
    provider_actions: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$latest1", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),
                    MatrixTextEvent(event_id="$latest1", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/latest"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    def cockpit_provider(action: str) -> MatrixCommandResponse:
        provider_actions.append(action)
        return MatrixCommandResponse(
            text="Parking monitor latest\nSnapshot: fresh raw latest.jpg; 11x7; 632 bytes",
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 11, "h": 7},
        )

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$latest1"))
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
    assert provider_actions == ["latest", "latest"]
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text", "upload", "image", "text", "upload", "image"]
    assert calls[0]["txn_id"] == "command:$latest1:text"
    assert calls[0]["body"].startswith("Parking monitor latest")
    assert calls[1]["filename"] == "latest.jpg"
    assert calls[1]["content_type"] == "image/jpeg"
    assert calls[1]["data"] == raw_bytes
    assert calls[2]["txn_id"] == "command:$latest1:image"
    assert calls[2]["body"] == "Raw full-frame latest.jpg evidence"
    assert calls[2]["info"] == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 11, "h": 7}


def test_command_service_resizes_oversized_command_image_before_upload(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent
    from parking_spot_monitor.matrix_snapshots import MAX_MATRIX_UPLOAD_IMAGE_BYTES

    latest_path = tmp_path / "latest.jpg"
    noisy = Image.effect_noise((1458, 806), 90).convert("RGB")
    noisy.save(latest_path, format="JPEG", quality=95)
    assert latest_path.stat().st_size > MAX_MATRIX_UPLOAD_IMAGE_BYTES
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$latest-large", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/latest"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=lambda action: MatrixCommandResponse(
            text="Parking monitor latest",
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": latest_path.stat().st_size, "w": 1458, "h": 806},
        ),
    )

    result = service.poll_once()

    upload = next(call for call in calls if call["kind"] == "upload")
    image = next(call for call in calls if call["kind"] == "image")
    assert result.error_count == 0
    assert len(upload["data"]) <= MAX_MATRIX_UPLOAD_IMAGE_BYTES
    assert image["info"]["size"] == len(upload["data"])
    assert image["info"]["w"] <= 960


def test_command_service_close_closes_owned_matrix_client() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService

    class Client:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    client = Client()
    service = MatrixCommandService(
        client=client,  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
    )

    service.close()

    assert client.closed is True


def test_matrix_command_service_requires_who_snapshot_provider() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService

    with pytest.raises(TypeError, match="who_snapshot_provider"):
        MatrixCommandService(
            client=object(),  # type: ignore[arg-type]
            archive=object(),  # type: ignore[arg-type]
            room_id=ROOM_ID,
            authorized_senders=["@op:example"],
        )


def test_command_service_who_can_send_active_assignments_with_fresh_snapshot(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(latest_path, size=(13, 9))
    calls: list[dict[str, Any]] = []
    provider_inputs: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$who", sender="@op:example", room_id=ROOM_ID, body="!parking who"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/who"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    def who_snapshot_provider(base_text: str) -> MatrixCommandResponse:
        provider_inputs.append(base_text)
        return MatrixCommandResponse(
            text="Parking monitor who\nSnapshot: fresh capture at 2026-05-16 10:42:39 AM PDT\n\n" + "\n".join(base_text.splitlines()[1:]),
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 13, "h": 9},
        )

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=who_snapshot_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert len(provider_inputs) == 1
    assert provider_inputs[0].startswith("Active parking sessions:")
    assert [call["kind"] for call in calls] == ["text", "upload", "image"]
    assert calls[0]["txn_id"] == "command:$who:text"
    assert "Snapshot: fresh capture" in calls[0]["body"]
    assert calls[1]["filename"] == "latest.jpg"
    assert calls[1]["data"] == raw_bytes
    assert calls[2]["txn_id"] == "command:$who:image"
    assert calls[2]["body"] == "Raw full-frame latest.jpg evidence"


def test_command_service_latest_failure_and_unauthorized_latest_are_text_only(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$deny", sender="@intruder:example", room_id=ROOM_ID, body="!parking latest"),
                    MatrixTextEvent(event_id="$latest", sender="@op:example", room_id=ROOM_ID, body="!parking latest"),
                    MatrixTextEvent(event_id="$status", sender="@op:example", room_id=ROOM_ID, body="!parking status"),
                    MatrixTextEvent(event_id="$config", sender="@op:example", room_id=ROOM_ID, body="!parking config"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("text-only latest/status/config replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("text-only latest/status/config replies must not send media")

    def cockpit_provider(action: str) -> str | MatrixCommandResponse:
        if action == "latest":
            return MatrixCommandResponse(text="Parking monitor latest unavailable: latest.jpg missing", image_path=None, image_info=None)
        return f"cockpit {action} reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 3
    assert result.error_count == 1
    assert [call["body"] for call in calls] == [
        "Command rejected: sender is not authorized.",
        "Parking monitor latest unavailable: latest.jpg missing",
        "cockpit status reply",
        "cockpit config reply",
    ]
    assert all(call["kind"] == "text" for call in calls)


def test_parse_matrix_why_explain_and_recent_commands_are_exact_and_bounded() -> None:
    from parking_spot_monitor.matrix import MatrixCommandParseError, parse_matrix_command

    why = parse_matrix_command("  !parking   why   right_spot  ")
    explain = parse_matrix_command("  !parking   explain   right_spot  ")
    recent = parse_matrix_command("\n!parking recent\t")

    assert (why.action, why.spot_id) == ("why", "right_spot")
    assert (explain.action, explain.spot_id) == ("explain", "right_spot")
    assert recent.action == "recent"

    rejected = [
        "!parking why",
        "!parking why right_spot extra",
        "!parking why ../state.json",
        "!parking why /tmp/right_spot",
        "!parking why " + "x" * 161,
        "!parking explain",
        "!parking explain right_spot extra",
        "!parking explain ../state.json",
        "!parking explain /tmp/right_spot",
        "!parking explain " + "x" * 161,
        "!parking recent now",
        "!parking recent verbose",
        "!parking at",
        "!parking at 7:39pm",
        "!parking at 7:39pm ../state.json",
        "!parking at 7:39pm left_spot extra",
    ]
    for body in rejected:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(body)


def test_command_service_why_explain_recent_use_provider_text_only_repeatably_without_archive_correction() -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []
    provider_calls: list[tuple[str, str | None]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$why", sender="@op:example", room_id=ROOM_ID, body="!parking why right_spot"),
                    MatrixTextEvent(event_id="$why", sender="@op:example", room_id=ROOM_ID, body="!parking why right_spot"),
                    MatrixTextEvent(event_id="$explain", sender="@op:example", room_id=ROOM_ID, body="!parking explain right_spot"),
                    MatrixTextEvent(event_id="$recent", sender="@op:example", room_id=ROOM_ID, body="!parking recent"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("why/recent replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("why/recent replies must not send media")

    def cockpit_provider(action: str, *, spot_id: str | None = None) -> MatrixCommandResponse:
        provider_calls.append((action, spot_id))
        text = f"decision {action}" + (f" {spot_id}" if spot_id else "")
        return MatrixCommandResponse(text=text)

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$why"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 4
    assert result.error_count == 0
    assert provider_calls == [("why", "right_spot"), ("why", "right_spot"), ("explain", "right_spot"), ("recent", None)]
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text", "text", "text", "text"]
    assert [call["body"] for call in calls] == ["decision why right_spot", "decision why right_spot", "decision explain right_spot", "decision recent"]
    assert [call["txn_id"] for call in calls] == ["command:$why", "command:$why", "command:$explain", "command:$recent"]


def test_command_service_incident_review_uses_cockpit_context_with_image_response(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    image_path = tmp_path / "incident_left_spot.jpg"
    write_jpeg(image_path, size=(11, 7))
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$at", sender="@op:example", room_id=ROOM_ID, body="!parking at 7:39pm left_spot"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/incident"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    class Context:
        def incident_review_reply(
            self,
            *,
            spot_id: str,
            incident_time: str,
            logger: Any | None = None,
        ) -> MatrixCommandResponse:
            del logger
            assert spot_id == "left_spot"
            assert incident_time == "7:39pm"
            return MatrixCommandResponse(
                text="Incident review: left_spot around 7:39pm",
                image_path=image_path,
                image_info={"mimetype": "image/jpeg", "size": image_path.stat().st_size, "w": 11, "h": 7},
            )

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_context=Context(),  # type: ignore[arg-type]
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert [call["kind"] for call in calls] == ["text", "upload", "image"]
    assert calls[0]["txn_id"] == "command:$at:text"
    assert calls[1]["filename"] == "incident_left_spot.jpg"
    assert calls[2]["txn_id"] == "command:$at:image"


def test_command_service_incident_review_real_context_replays_detector_sends_safe_media_and_avoids_live_side_effects(tmp_path: Path) -> None:
    from parking_spot_monitor.config import load_settings
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent
    from parking_spot_monitor.state import RuntimeState, save_runtime_state
    from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
    from parking_spot_monitor.detection import VehicleDetection

    config_path = tmp_path / "config.yaml"
    config_path.write_text(Path("config.yaml.example").read_text(encoding="utf-8"), encoding="utf-8")
    settings = load_settings(config_path, environ=stream_env())
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    frame = frames_dir / "20260518T023900Z.jpg"
    raw_bytes = write_jpeg(frame, size=(1458, 806))
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=4, miss_streak=0),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY, hit_streak=0, miss_streak=5),
            }
        ),
    )
    state_before = state_path.read_text(encoding="utf-8")
    state_mtime_before_ns = state_path.stat().st_mtime_ns

    class Detector:
        calls: list[Path] = []

        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None, inference_image_size: int | None = None) -> list[VehicleDetection]:
            self.calls.append(Path(frame_path))
            return [VehicleDetection(class_name="car", confidence=0.91, bbox=(25, 30, 275, 325))]

    detector = Detector()
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$at", sender="@op:example", room_id=ROOM_ID, body="!parking at 7:39pm left_spot"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/incident"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    context = MatrixOperatorCockpitContext(
        settings=settings,
        data_dir=tmp_path,
        health_path=tmp_path / "health.json",
        state_path=state_path,
        incident_detector=adapt_detector(detector),
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
    assert detector.calls == [frame]
    assert archive.calls == []
    assert state_path.read_text(encoding="utf-8") == state_before
    assert state_path.stat().st_mtime_ns == state_mtime_before_ns
    assert [call["kind"] for call in calls] == ["text", "upload", "image"]
    assert "Detector replay:" in calls[0]["body"]
    assert "left_spot: accepted car confidence 0.91" in calls[0]["body"]
    assert "State simulation:" in calls[0]["body"]
    assert len(calls[0]["body"].encode("utf-8")) <= 4096
    assert calls[1]["filename"] == "incident_left_spot.jpg"
    assert calls[1]["content_type"] == "image/jpeg"
    assert calls[1]["data"] != raw_bytes
    assert len(calls[1]["data"]) <= 300_000
    assert calls[2]["txn_id"] == "command:$at:image"
    assert calls[2]["info"]["mimetype"] == "image/jpeg"
    rendered = repr(calls)
    assert ACCESS_TOKEN not in rendered
    assert "rtsp://" not in rendered
    assert "raw_image" not in rendered
    assert "Traceback" not in rendered
    assert str(tmp_path) not in calls[0]["body"]
    assert str(tmp_path) not in calls[1]["filename"]
    assert str(tmp_path) not in calls[2]["body"]


def test_command_service_rejects_unauthorized_why_and_explain_before_memory_or_provider_paths() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$why", sender="@intruder:example", room_id=ROOM_ID, body="!parking why right_spot"),
                    MatrixTextEvent(event_id="$explain", sender="@intruder:example", room_id=ROOM_ID, body="!parking explain right_spot"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

    def cockpit_provider(action: str, *, spot_id: str | None = None) -> str:
        raise AssertionError("unauthorized why/explain must not touch provider or memory paths")

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


def test_command_service_confidence_context_reads_local_artifacts_text_only(tmp_path: Path) -> None:
    from parking_spot_monitor.config import load_settings
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixOperatorCockpitContext, MatrixSyncResult, MatrixTextEvent
    from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record
    from parking_spot_monitor.state import RuntimeState, save_runtime_state

    config_path = tmp_path / "config.yaml"
    config_path.write_text(Path("config.yaml.example").read_text(encoding="utf-8"), encoding="utf-8")
    settings = load_settings(config_path, environ=stream_env())
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=4, miss_streak=0),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY, hit_streak=0, miss_streak=5),
            }
        ),
    )
    state_before = state_path.read_text(encoding="utf-8")
    state_mtime_before_ns = state_path.stat().st_mtime_ns
    health_path = tmp_path / "health.json"
    health_path.write_text(json.dumps({"last_matrix_error": {"error_type": "timeout", "token": ACCESS_TOKEN}}), encoding="utf-8")
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    (frames_dir / "20260518T190000Z.jpg").write_bytes(b"not image bytes; filename-only confidence scan")
    assert append_decision_memory_record(
        decision_memory_path(tmp_path),
        make_decision_memory_record(
            "confidence_dip",
            observed_at="2026-05-18T19:01:00Z",
            spot_id="left_spot",
            summary="weak contrast near bumper",
            details={"token": ACCESS_TOKEN, "rtsp_url": "rtsp://user:pass@example/camera"},
        ),
    )
    calls: list[dict[str, Any]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$confidence", sender="@op:example", room_id=ROOM_ID, body="!parking confidence"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("confidence replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("confidence replies must not send media")

    context = MatrixOperatorCockpitContext(settings=settings, data_dir=tmp_path, health_path=health_path, state_path=state_path)
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_context=context,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert service.archive.calls == []
    assert state_path.read_text(encoding="utf-8") == state_before
    assert state_path.stat().st_mtime_ns == state_mtime_before_ns
    assert [call["kind"] for call in calls] == ["text"]
    body = calls[0]["body"]
    assert calls[0]["txn_id"] == "command:$confidence"
    assert "Parking confidence report" in body
    assert "Spot stability:" in body
    assert "Weak evidence:" in body
    assert "confidence_dip: weak contrast near bumper" in body
    assert "Timeline health:" in body
    assert "Matrix delivery:" in body
    assert "last Matrix error: timeout" in body
    assert "Read-only: no detector, camera, media upload, alert emission, or state mutation was run." in body
    assert len(body.encode("utf-8")) <= 4096
    assert ACCESS_TOKEN not in body
    assert "rtsp://" not in body
    assert str(tmp_path) not in body


def test_command_service_analytics_windows_route_to_cockpit_provider_text_only_without_archive_corrections() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []
    provider_calls: list[tuple[str, str | None]] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$analytics-default", sender="@op:example", room_id=ROOM_ID, body="!parking analytics"),
                    MatrixTextEvent(event_id="$analytics-today", sender="@op:example", room_id=ROOM_ID, body="!parking analytics today"),
                    MatrixTextEvent(event_id="$analytics-7d", sender="@op:example", room_id=ROOM_ID, body="!parking analytics 7d"),
                    MatrixTextEvent(event_id="$analytics-30d", sender="@op:example", room_id=ROOM_ID, body="!parking analytics 30d"),
                    MatrixTextEvent(event_id="$analytics-all", sender="@op:example", room_id=ROOM_ID, body="!parking analytics all"),
                    MatrixTextEvent(event_id="$analytics-bad", sender="@op:example", room_id=ROOM_ID, body="!parking analytics today extra"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            raise AssertionError("analytics replies must not upload media")

        def send_image(self, **kwargs: Any) -> str:
            raise AssertionError("analytics replies must not send media")

    def cockpit_provider(action: str, *, analytics_window: str | None = None) -> str:
        provider_calls.append((action, analytics_window))
        return f"analytics reply for {analytics_window}"

    archive = FakeCommandArchive(cursor={"next_batch": "s2"})
    archive.corrections.append(FakeCorrection("existing", matrix_event_id="$analytics-default"))
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=lambda base_reply: base_reply,
        cockpit_provider=cockpit_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 5
    assert result.error_count == 1
    assert provider_calls == [("analytics", "7d"), ("analytics", "today"), ("analytics", "7d"), ("analytics", "30d"), ("analytics", "all")]
    assert archive.calls == []
    assert [call["kind"] for call in calls] == ["text", "text", "text", "text", "text", "text"]
    assert [call["txn_id"] for call in calls] == [
        "command:$analytics-default",
        "command:$analytics-today",
        "command:$analytics-7d",
        "command:$analytics-30d",
        "command:$analytics-all",
        "command:$analytics-bad",
    ]
    assert [call["body"] for call in calls[:5]] == [
        "analytics reply for 7d",
        "analytics reply for today",
        "analytics reply for 7d",
        "analytics reply for 30d",
        "analytics reply for all",
    ]
    assert calls[5]["body"] == "Command rejected: usage: !parking analytics [today|7d|30d|all]"
