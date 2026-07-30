from __future__ import annotations

from tests.support._matrix_operator_cockpit import *  # noqa: F403


def test_operator_cockpit_analytics_reply_formats_archive_metrics_and_window_labels(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_analytics_reply

    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="left-morning",
        spot_id="left_spot",
        started_at="2026-05-20T08:00:00Z",
        ended_at="2026-05-20T10:00:00Z",
        duration_seconds=7_200,
    )
    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="right-short",
        spot_id="right_spot",
        started_at="2026-05-19T09:00:00Z",
        ended_at="2026-05-19T09:30:00Z",
        duration_seconds=1_800,
    )
    _write_vehicle_history_session(
        tmp_path,
        state="active",
        session_id="left-active",
        spot_id="left_spot",
        started_at="2026-05-20T11:00:00Z",
        ended_at=None,
        duration_seconds=None,
    )

    reply = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="today",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert "Parking occupancy analytics" in reply
    assert "Window: today" in reply
    assert "Range: 2026-05-20T00:00:00Z → 2026-05-20T12:00:00Z" in reply
    assert "Source: local vehicle-history sessions" in reply
    assert "Totals\n- Sessions: 2\n- Closed: 1\n- Active: 1\n- Currently occupied spots: 1" in reply
    assert "Spots\nleft_spot" in reply
    assert "- Sessions: 2" in reply
    assert "- Active: 1" in reply
    assert "- Status: occupied" in reply
    assert "- Occupied: 3h" in reply
    assert "- Average dwell: 2h" in reply
    assert "right_spot" not in reply
    assert "Read-only\nScanned local vehicle-history JSON only. No detector, camera, Matrix media upload, alert emission, or state mutation was run." in reply
    assert reply.count("No detector, camera, Matrix media upload, alert emission, or state mutation was run.") == 1
    assert len(reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_analytics_reply_degrades_for_empty_sparse_and_malformed_archive_data(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_analytics_reply

    empty = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="7d",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert "Parking occupancy analytics" in empty
    assert "Window: 7d" in empty
    assert "No vehicle-history sessions overlap the selected window." in empty
    assert "sparse" in empty.lower() or "limited history" in empty.lower()
    assert len(empty.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(empty)

    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="one-good",
        spot_id="left_spot",
        started_at="2026-05-20T08:00:00Z",
        ended_at="2026-05-20T08:30:00Z",
        duration_seconds=1_800,
        secret_marker=FAKE_MATRIX_TOKEN,
    )
    bad_dir = tmp_path / "vehicle-history" / "sessions" / "closed"
    bad_dir.mkdir(parents=True, exist_ok=True)
    (bad_dir / "malformed.json").write_text("not json " + FAKE_RTSP_URL + " " + RAW_IMAGE_MARKER, encoding="utf-8")

    malformed = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="today",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert "left_spot" in malformed
    assert "Only 1 qualifying vehicle-history session" in malformed
    assert "malformed" in malformed.lower()
    assert "ignored" in malformed.lower() or "quarantined" in malformed.lower()
    assert "No detector, camera, Matrix media upload, alert emission, or state mutation was run." in malformed
    assert len(malformed.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(malformed)


def test_operator_cockpit_analytics_reply_sanitizes_archive_spot_ids(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_analytics_reply

    raw_spot_id = "/var/lib/parking/private/latest.jpg"
    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="path-like-spot",
        spot_id=raw_spot_id,
        started_at="2026-05-20T08:00:00Z",
        ended_at="2026-05-20T08:30:00Z",
        duration_seconds=1_800,
    )

    reply = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="today",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert raw_spot_id not in reply
    assert "/var/lib" not in reply
    assert "unknown_spot" in reply
    _assert_no_sensitive_text(reply)


def test_operator_cockpit_analytics_reply_is_read_only_local_and_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.operator_cockpit import format_operator_analytics_reply

    for index in range(80):
        _write_vehicle_history_session(
            tmp_path,
            state="closed",
            session_id=f"left-{index:03d}",
            spot_id="left_spot" if index % 2 == 0 else "right_spot",
            started_at=f"2026-05-20T{index % 12:02d}:00:00Z",
            ended_at=f"2026-05-20T{index % 12:02d}:15:00Z",
            duration_seconds=900,
            secret_marker=FAKE_MATRIX_TOKEN,
        )
    health_path = tmp_path / "health.json"
    state_path = tmp_path / "state.json"
    health_path.write_text(json.dumps({"status": "ok", "secret": FAKE_MATRIX_TOKEN}), encoding="utf-8")
    state_path.write_text(json.dumps({"spots": {"left_spot": {"status": "open"}}}), encoding="utf-8")
    original_health = health_path.read_text(encoding="utf-8")
    original_state = state_path.read_text(encoding="utf-8")

    def fail_side_effect(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("analytics must use only local vehicle-history archive data")

    monkeypatch.setattr("parking_spot_monitor.capture.capture_latest", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.state.save_runtime_state", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.upload_image", fail_side_effect)
    reply = format_operator_analytics_reply(
        data_dir=tmp_path,
        window="30d",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
        detector=fail_side_effect,
        health_path=health_path,
        state_path=state_path,
    )

    assert "Parking occupancy analytics" in reply
    assert "Window: 30d" in reply
    assert "left_spot" in reply
    assert "right_spot" in reply
    assert "local vehicle-history sessions" in reply
    assert "health" not in reply.lower()
    assert health_path.read_text(encoding="utf-8") == original_health
    assert state_path.read_text(encoding="utf-8") == original_state
    assert len(reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(reply)


def test_matrix_operator_context_routes_parsed_analytics_command_to_cockpit(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixOperatorCockpitContext, parse_matrix_command

    command = parse_matrix_command("!parking analytics 30d")
    assert command.action == "analytics"
    assert command.subject_id == "30d"

    _write_vehicle_history_session(
        tmp_path,
        state="closed",
        session_id="left-history",
        spot_id="left_spot",
        started_at="2026-05-19T08:00:00Z",
        ended_at="2026-05-19T09:00:00Z",
        duration_seconds=3_600,
    )
    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)
    context = MatrixOperatorCockpitContext(
        settings=settings,
        data_dir=tmp_path,
        health_path=health_path,
        state_path=state_path,
    )

    response = context.analytics_reply(
        command.subject_id or "7d",
        now=datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "Parking occupancy analytics" in response.text
    assert "Window: 30d" in response.text
    assert "left-history" not in response.text
    assert "left_spot" in response.text
    assert len(response.text.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(response.text)
