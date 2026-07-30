from __future__ import annotations

from tests.support._matrix_operator_cockpit import *  # noqa: F403


@pytest.mark.parametrize(
    "filename,payload,expected",
    [
        ("latest.jpg", b"not a jpeg " + RAW_IMAGE_MARKER.encode("utf-8"), "invalid JPEG"),
        ("debug_latest.jpg", None, "debug overlay"),
        ("latest.jpg", b"0" * 300_001, "too large"),
    ],
)
def test_latest_snapshot_summary_rejects_invalid_debug_and_oversized_images_safely(
    tmp_path: Path,
    filename: str,
    payload: bytes | None,
    expected: str,
) -> None:
    from parking_spot_monitor.operator_cockpit import build_latest_snapshot_response

    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)
    latest_path = tmp_path / filename
    if payload is None:
        _write_test_jpeg(latest_path)
    else:
        latest_path.write_bytes(payload)
    log_stream = StringIO()

    response = build_latest_snapshot_response(
        settings=settings,
        latest_path=latest_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, 20, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "Parking monitor latest unavailable" in response.text
    assert expected in response.text
    _assert_no_sensitive_text(response.text + log_stream.getvalue())


@pytest.mark.parametrize(
    "bomb",
    [
        Image.DecompressionBombError("access_token=latest-bomb-secret"),
        Image.DecompressionBombWarning("token=latest-warning-secret"),
    ],
)
def test_latest_snapshot_command_redacts_decompression_bombs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bomb: BaseException,
) -> None:
    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)
    latest_path = tmp_path / "latest.jpg"
    _write_test_jpeg(latest_path)
    stream = StringIO()
    monkeypatch.setattr(operator_cockpit_snapshots.Image, "open", lambda path: (_ for _ in ()).throw(bomb))

    response = operator_cockpit_snapshots.build_latest_snapshot_response(
        settings=settings,
        latest_path=latest_path,
        health_path=health_path,
        state_path=state_path,
        logger=StructuredLogger(stream=stream),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "invalid JPEG" in response.text
    rendered = response.text + stream.getvalue()
    assert bomb.__class__.__name__ in rendered
    assert "bomb-secret" not in rendered
    assert "warning-secret" not in rendered


@pytest.mark.parametrize(
    "bomb",
    [
        Image.DecompressionBombError("access_token=who-bomb-secret"),
        Image.DecompressionBombWarning("token=who-warning-secret"),
    ],
)
def test_who_snapshot_command_redacts_decompression_bombs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bomb: BaseException,
) -> None:
    latest_path = tmp_path / "latest.jpg"
    _write_test_jpeg(latest_path)
    stream = StringIO()
    monkeypatch.setattr(operator_cockpit_snapshots.Image, "open", lambda path: (_ for _ in ()).throw(bomb))

    response = operator_cockpit_snapshots.build_who_snapshot_response(
        settings=_settings(tmp_path),
        data_dir=tmp_path,
        base_text="Parking monitor who",
        capture_func=lambda *args, **kwargs: SimpleNamespace(
            latest_path=latest_path,
            timestamp="2026-05-16T17:42:39Z",
        ),
        logger=StructuredLogger(stream=stream),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "fresh capture unavailable (invalid JPEG)" in response.text
    rendered = response.text + stream.getvalue()
    assert bomb.__class__.__name__ in rendered
    assert "bomb-secret" not in rendered
    assert "warning-secret" not in rendered


def test_latest_snapshot_summary_handles_missing_stale_and_malformed_runtime_files_safely(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import build_latest_snapshot_response

    settings = _settings(tmp_path)
    latest_path = tmp_path / "latest.jpg"
    _write_test_jpeg(latest_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(
        json.dumps({"status": "ok", "updated_at": "2026-05-18T17:00:00Z", "iteration": 1, "secret": FAKE_RTSP_URL}),
        encoding="utf-8",
    )
    state_path.write_text("not json " + FAKE_MATRIX_TOKEN + RAW_IMAGE_MARKER, encoding="utf-8")
    log_stream = StringIO()

    response = build_latest_snapshot_response(
        settings=settings,
        latest_path=latest_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    assert response.image_path == latest_path
    assert "Health: ok stale" in response.text
    assert "2h ago" in response.text
    assert "State: unavailable" in response.text
    assert "left_spot" in response.text and "right_spot" in response.text
    _assert_no_sensitive_text(response.text + log_stream.getvalue())


def test_operator_cockpit_confidence_summary_reports_artifact_derived_spot_signals_and_delivery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_confidence_reply
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    settings = _settings(tmp_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(
        json.dumps(
            {
                "status": "degraded",
                "updated_at": "2026-05-18T19:00:00Z",
                "last_matrix_error": {
                    "error_type": "timeout",
                    "diagnostic": FAKE_MATRIX_TOKEN,
                    "path": str(tmp_path),
                    "nested": {"secret": NESTED_SECRET_MARKER},
                },
            }
        ),
        encoding="utf-8",
    )
    original_state = json.dumps(
        {
            "schema_version": 1,
            "spots": {
                "left_spot": {"status": "occupied", "hit_streak": 4, "miss_streak": 0, "open_event_emitted": False},
                "right_spot": {"status": "empty", "hit_streak": 0, "miss_streak": 5, "open_event_emitted": True},
            },
        },
        sort_keys=True,
    )
    state_path.write_text(original_state, encoding="utf-8")
    original_state_mtime_ns = state_path.stat().st_mtime_ns
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    (frames_dir / "20260518T185800Z.jpg").write_bytes((RAW_IMAGE_MARKER * 20).encode("utf-8"))
    (frames_dir / "20260518T185900Z.jpg").write_bytes((RAW_IMAGE_MARKER * 20).encode("utf-8"))
    (frames_dir / "not-a-timestamp.jpg").write_bytes((RAW_IMAGE_MARKER * 20).encode("utf-8"))
    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "confidence_dip",
            observed_at="2026-05-18T18:59:10Z",
            spot_id="left_spot",
            summary="confidence dipped below stable threshold",
            details={"token": FAKE_MATRIX_TOKEN, "raw_image": RAW_IMAGE_MARKER},
        ),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record("suppression", observed_at="2026-05-18T18:59:20Z", spot_id="right_spot", summary="quiet-window suppression active"),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record("command_outcome", observed_at="2026-05-18T18:59:30Z", summary="Matrix status command delivered"),
    )

    def fail_side_effect(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("confidence summary must only read local artifacts")

    monkeypatch.setattr("parking_spot_monitor.capture.capture_latest", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.state.save_runtime_state", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_side_effect)
    monkeypatch.setattr("PIL.Image.open", fail_side_effect)

    reply = format_operator_confidence_reply(
        settings=settings,
        data_dir=tmp_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    assert "Parking confidence report" in reply
    assert "conservative and artifact-derived" in reply
    assert "not a calibrated model score" in reply
    assert "left_spot: stable occupied" in reply
    assert "hit streak 4/4" in reply
    assert "right_spot: stable open" in reply
    assert "miss streak 5/5" in reply
    assert "confidence_dip: confidence dipped below stable threshold" in reply
    assert "suppression: quiet-window suppression active" in reply
    assert "retained timestamped frames 2" in reply
    assert "newest 1m ago" in reply
    assert "filename scan only; image bytes were not opened; ignored 1" in reply
    assert "last Matrix error: timeout" in reply
    assert "recent delivery memory: command_outcome: Matrix status command delivered" in reply
    assert "Read-only: no detector, camera, media upload, alert emission, or state mutation was run." in reply
    assert state_path.read_text(encoding="utf-8") == original_state
    assert state_path.stat().st_mtime_ns == original_state_mtime_ns
    assert len(reply.encode("utf-8")) <= 4096
    assert str(tmp_path) not in reply
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_confidence_summary_degrades_for_missing_and_malformed_artifacts(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_confidence_reply
    from parking_spot_monitor.operator_decision_memory import decision_memory_path

    settings = _settings(tmp_path)
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text("not json " + FAKE_MATRIX_TOKEN + " " + str(tmp_path), encoding="utf-8")
    state_path.write_text(json.dumps({"schema_version": 1, "spots": [], "secret": FAKE_RTSP_URL}), encoding="utf-8")
    memory_path = decision_memory_path(tmp_path)
    memory_path.write_text("not json " + RAW_IMAGE_MARKER + " " + NESTED_SECRET_MARKER, encoding="utf-8")

    reply = format_operator_confidence_reply(
        settings=settings,
        data_dir=tmp_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    assert "Parking confidence report" in reply
    assert "left_spot: unavailable" in reply
    assert "right_spot: unavailable" in reply
    assert "State artifacts: unavailable (schema_error); configured spot fallbacks shown." in reply
    assert "decision memory unavailable (JSONDecodeError)" in reply
    assert "Timeline health:" in reply
    assert "unavailable (missing timeline frames directory)" in reply
    assert "health unavailable (JSONDecodeError); Matrix error status unknown" in reply
    assert "delivery memory unavailable" in reply
    assert "Traceback" not in reply
    assert str(tmp_path) not in reply
    assert len(reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_decision_memory_wrappers_are_bounded_redacted_and_local_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_recent_reply, format_operator_why_reply
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "suppression",
            observed_at="2026-05-18T19:00:00Z",
            spot_id="right_spot",
            summary="quiet-window suppression applied",
            details={"miss_streak": 2, "matrix_token": FAKE_MATRIX_TOKEN, "raw_image": RAW_IMAGE_MARKER},
        ),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record("alert", observed_at="2026-05-18T19:01:00Z", spot_id="right_spot", summary="alert skipped", details={"reason": "quiet_window"}),
    )

    def fail_network_or_model_work(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("why/recent must only read local decision memory")

    monkeypatch.setattr("httpx.Client.request", fail_network_or_model_work)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_network_or_model_work)

    why = format_operator_why_reply(data_dir=tmp_path, spot_id="right_spot")
    recent = format_operator_recent_reply(data_dir=tmp_path)

    assert "Parking decision memory for right_spot" in why
    assert "quiet-window suppression applied" in why
    assert "alert skipped" in recent
    assert len(why.encode("utf-8")) <= 4096
    assert len(recent.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(why + recent)


def test_operator_cockpit_decision_memory_wrappers_handle_corrupt_memory_safely(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_recent_reply, format_operator_why_reply
    from parking_spot_monitor.operator_decision_memory import decision_memory_path

    memory_path = decision_memory_path(tmp_path)
    memory_path.parent.mkdir(parents=True, exist_ok=True)
    memory_path.write_text("not json " + FAKE_RTSP_URL + " " + FAKE_MATRIX_TOKEN, encoding="utf-8")
    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)

    why = format_operator_why_reply(data_dir=tmp_path, spot_id="right_spot", logger=logger)
    recent = format_operator_recent_reply(data_dir=tmp_path, logger=logger)

    rendered = why + recent + log_stream.getvalue()
    assert "Decision memory unavailable" in why
    assert "Decision memory unavailable" in recent
    assert "no detector or camera work was run" in why
    _assert_no_sensitive_text(rendered)
