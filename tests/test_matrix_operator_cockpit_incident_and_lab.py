from __future__ import annotations

from tests.support._matrix_operator_cockpit import *  # noqa: F403


def test_operator_cockpit_incident_review_attaches_nearest_timeline_frame_and_memory(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import build_incident_review_response
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    far = frames_dir / "20260518T021000Z.jpg"
    near = frames_dir / "20260518T023900Z.jpg"
    _write_test_jpeg(far, size=(16, 9))
    noisy = Image.effect_noise((1280, 720), 80).convert("RGB")
    noisy.save(near, format="JPEG", quality=95)
    assert near.stat().st_size > 300_000
    assert append_decision_memory_record(
        decision_memory_path(tmp_path),
        make_decision_memory_record(
            "miss",
            observed_at="2026-05-18T02:39:15Z",
            spot_id="left_spot",
            summary="runtime state stayed occupied despite operator report",
        ),
    )

    response = build_incident_review_response(
        data_dir=tmp_path,
        spot_id="left_spot",
        time_text="7:39pm",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert response.image_path is not None
    assert response.image_path.name == "incident_left_spot.jpg"
    assert response.image_path.stat().st_size <= 300_000
    assert response.image_info is not None
    assert response.image_info["size"] == response.image_path.stat().st_size
    assert response.image_info["w"] < 1280
    assert response.image_info["h"] < 720
    assert "Incident review: left_spot around 2026-05-17 7:39 PM PDT" in response.text
    assert "Nearest retained frame: 2026-05-17 7:39 PM PDT" in response.text
    assert "runtime state stayed occupied" in response.text
    assert "No detector, camera, Matrix send, or state mutation was run." in response.text
    _assert_no_sensitive_text(response.text)


def test_operator_cockpit_incident_review_handles_missing_timeline_safely(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import build_incident_review_response

    response = build_incident_review_response(
        data_dir=tmp_path,
        spot_id="left_spot",
        time_text="7:39pm",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "Incident review: left_spot" in response.text
    assert "Nearest retained frame: unavailable" in response.text
    assert "No retained timeline frames were found." in response.text


def test_operator_cockpit_incident_review_contract_replays_detector_and_simulates_state_without_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from parking_spot_monitor.detection import VehicleDetection
    from parking_spot_monitor.operator_cockpit import build_incident_review_response
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record

    settings = _settings(tmp_path)
    frame = _write_incident_timeline_frame(tmp_path)
    state_path = tmp_path / "state.json"
    original_state = _write_incident_runtime_state(state_path)
    original_state_mtime_ns = state_path.stat().st_mtime_ns
    detector = _IncidentReplayDetector([VehicleDetection(class_name="car", confidence=0.91, bbox=(25, 30, 275, 325))])
    assert append_decision_memory_record(
        decision_memory_path(tmp_path),
        make_decision_memory_record(
            "accepted_evidence",
            observed_at="2026-05-18T02:39:10Z",
            spot_id="left_spot",
            summary="left_spot accepted vehicle evidence before operator review",
            details={"matrix_token": FAKE_MATRIX_TOKEN, "raw_image": RAW_IMAGE_MARKER},
        ),
    )

    def fail_side_effect(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("incident review must not capture, write state, alert, or mutate vehicle history")

    monkeypatch.setattr("parking_spot_monitor.capture.capture_latest", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.state.save_runtime_state", fail_side_effect)
    monkeypatch.setattr("parking_spot_monitor.matrix.MatrixClient.send_text", fail_side_effect)

    response = build_incident_review_response(
        settings=settings,
        data_dir=tmp_path,
        state_path=state_path,
        spot_id="left_spot",
        time_text="7:39pm",
        detector=adapt_detector(detector),
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert detector.calls == [{"frame_path": frame, "confidence_threshold": 0.42, "inference_image_size": 960}]
    assert state_path.read_text(encoding="utf-8") == original_state
    assert state_path.stat().st_mtime_ns == original_state_mtime_ns
    assert response.image_path is not None
    assert response.image_path.name == "incident_left_spot.jpg"
    assert response.image_info is not None
    assert response.image_info["mimetype"] == "image/jpeg"
    assert response.image_info["size"] == response.image_path.stat().st_size
    assert "Detector replay:" in response.text
    assert "left_spot: accepted car confidence 0.91" in response.text
    assert "right_spot: rejected centroid_outside" in response.text
    assert "State simulation:" in response.text
    assert "left_spot: occupied would remain occupied" in response.text
    assert "no live state was changed" in response.text
    assert "left_spot accepted vehicle evidence before operator review" in response.text
    assert len(response.text.encode("utf-8")) <= 4096
    assert str(tmp_path) not in response.text
    assert "Traceback" not in response.text
    _assert_no_sensitive_text(response.text)


@pytest.mark.parametrize(
    "case,corrupt_frame,corrupt_state,detections,expected",
    [
        ("no_evidence", False, False, [], ["Detector replay: no vehicle evidence", "would increment miss streak"]),
        ("corrupt_frame", True, False, [], ["Nearest retained frame: unavailable", "corrupt_frame"]),
        (
            "detector_exception",
            False,
            False,
            RuntimeError("predict failed token=" + FAKE_MATRIX_TOKEN + " raw " + RAW_IMAGE_MARKER),
            ["Detector replay unavailable", "RuntimeError"],
        ),
        ("corrupt_state", False, True, [], ["Runtime state unavailable", "JSONDecodeError", "simulated from unknown/default state"]),
    ],
)
def test_operator_cockpit_incident_review_contract_degrades_safely_for_negative_paths(
    tmp_path: Path,
    case: str,
    corrupt_frame: bool,
    corrupt_state: bool,
    detections: list[Any] | Exception,
    expected: list[str],
) -> None:
    from parking_spot_monitor.operator_cockpit import build_incident_review_response

    settings = _settings(tmp_path)
    _write_incident_timeline_frame(tmp_path, corrupt=corrupt_frame)
    state_path = tmp_path / "state.json"
    original_state = _write_incident_runtime_state(state_path, corrupt=corrupt_state)
    original_state_mtime_ns = state_path.stat().st_mtime_ns
    detector = _IncidentReplayDetector(detections)

    response = build_incident_review_response(
        settings=settings,
        data_dir=tmp_path,
        state_path=state_path,
        spot_id="left_spot",
        time_text="7:39pm",
        detector=adapt_detector(detector),
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert state_path.read_text(encoding="utf-8") == original_state
    assert state_path.stat().st_mtime_ns == original_state_mtime_ns
    assert len(response.text.encode("utf-8")) <= 4096
    assert str(tmp_path) not in response.text
    assert "Traceback" not in response.text
    for snippet in expected:
        assert snippet in response.text, case
    _assert_no_sensitive_text(response.text)


def test_operator_cockpit_detection_lab_wrappers_are_bounded_redacted_and_local_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.detection_lab import DetectionLabManager, REPLAY_CONFIG_FILENAME, REPLAY_LABELS_FILENAME
    from parking_spot_monitor.operator_cockpit import format_detection_lab_run_reply, format_detection_lab_status_reply

    lab_root = tmp_path / "detection-lab"
    lab_root.mkdir()
    (lab_root / REPLAY_LABELS_FILENAME).write_text("{}", encoding="utf-8")
    (lab_root / REPLAY_CONFIG_FILENAME).write_text("{}", encoding="utf-8")

    def fail_network_or_model_work(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("lab wrappers must not run network, camera, or model work")

    monkeypatch.setattr("httpx.Client.request", fail_network_or_model_work)

    def replay_runner(inputs: dict[str, Path]) -> dict[str, Any]:
        report = inputs["job_dir"] / "replay-report.json"
        report.write_text(
            json.dumps(
                {
                    "schema_version": "test.v1",
                    "status_counts": {"passed": 2},
                    "coverage": {"assessed_frames": 2, "blocked_frames": 0, "not_assessed_frames": 0},
                    "redaction_scan": {"passed": True, "findings": []},
                    "rtsp_url": FAKE_RTSP_URL,
                    "matrix_token": FAKE_MATRIX_TOKEN,
                    "raw_image": RAW_IMAGE_MARKER,
                }
            ),
            encoding="utf-8",
        )
        return report

    manager = DetectionLabManager(tmp_path, replay_runner=replay_runner)
    run_reply = format_detection_lab_run_reply(data_dir=tmp_path, kind="replay", manager=manager)

    import time

    deadline = time.monotonic() + 2
    status_reply = format_detection_lab_status_reply(data_dir=tmp_path, job_id="latest", manager=manager)
    while time.monotonic() < deadline and "Status: succeeded" not in status_reply:
        time.sleep(0.01)
        status_reply = format_detection_lab_status_reply(data_dir=tmp_path, job_id="latest", manager=manager)

    rendered = run_reply + status_reply
    assert "Detection lab job started" in run_reply
    assert "Detection lab status" in status_reply
    assert "status counts: passed=2" in status_reply
    assert "coverage: assessed 2" in status_reply
    assert len(run_reply.encode("utf-8")) <= 4096
    assert len(status_reply.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(rendered)


def test_operator_cockpit_detection_lab_status_failures_are_safe_and_redacted(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import format_detection_lab_run_reply, format_detection_lab_status_reply

    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)

    unavailable = format_detection_lab_status_reply(data_dir=tmp_path, job_id="latest", logger=logger)
    invalid = format_detection_lab_status_reply(data_dir=tmp_path, job_id="../status.json " + FAKE_RTSP_URL, logger=logger)
    bad_kind = format_detection_lab_run_reply(data_dir=tmp_path, kind="../replay", logger=logger)

    rendered = unavailable + invalid + bad_kind + log_stream.getvalue()
    assert "Detection lab status unavailable" in unavailable
    assert "No detector, camera, shell, or live occupancy work was run" in unavailable
    assert "Detection lab run unavailable" in bad_kind
    assert len(unavailable.encode("utf-8")) <= 4096
    assert len(invalid.encode("utf-8")) <= 4096
    assert len(bad_kind.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(rendered)


def test_build_who_snapshot_response_captures_once_and_attaches_validated_image(tmp_path: Path) -> None:
    from parking_spot_monitor.capture import DecodeMode, FrameCaptureResult, FrameGeometry
    from parking_spot_monitor.operator_cockpit import build_who_snapshot_response

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = len(_write_test_jpeg(latest_path, size=(13, 9)))
    calls: list[tuple[object, Path]] = []

    def capture_func(settings: object, data_dir: Path, **kwargs: object) -> FrameCaptureResult:
        calls.append((settings, Path(data_dir)))
        return FrameCaptureResult(
            timestamp="2026-05-16T17:42:39Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.25,
            byte_size=raw_bytes,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(13, 9)),
        )

    settings = object()
    response = build_who_snapshot_response(
        settings=settings,
        data_dir=tmp_path,
        base_text="Parking monitor who\n- left_spot: occupied — unknown vehicle",
        capture_func=capture_func,
        now=datetime(2026, 5, 16, 17, 42, 40, tzinfo=timezone.utc),
    )

    assert calls == [(settings, tmp_path)]
    assert response.image_path == latest_path
    assert response.image_info == {"mimetype": "image/jpeg", "size": raw_bytes, "w": 13, "h": 9}
    assert response.text.startswith("Parking monitor who\nSnapshot: fresh capture")
    assert "left_spot: occupied" in response.text


def test_build_who_snapshot_response_resizes_oversized_fresh_capture_for_matrix(tmp_path: Path) -> None:
    from parking_spot_monitor.capture import DecodeMode, FrameCaptureResult, FrameGeometry
    from parking_spot_monitor.operator_cockpit import build_who_snapshot_response

    latest_path = tmp_path / "latest.jpg"
    noisy = Image.effect_noise((1280, 720), 80).convert("RGB")
    noisy.save(latest_path, format="JPEG", quality=95)
    raw_size = latest_path.stat().st_size
    assert raw_size > 300_000

    def capture_func(settings: object, data_dir: Path, **kwargs: object) -> FrameCaptureResult:
        return FrameCaptureResult(
            timestamp="2026-05-16T17:42:39Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.25,
            byte_size=raw_size,
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1280, 720)),
        )

    response = build_who_snapshot_response(
        settings=object(),
        data_dir=tmp_path,
        base_text="Active parking sessions:\n- left_spot: occupied — unknown vehicle",
        capture_func=capture_func,
        now=datetime(2026, 5, 16, 17, 42, 40, tzinfo=timezone.utc),
    )

    assert response.image_path is not None
    assert response.image_path != latest_path
    assert response.image_path.name == "who_latest.jpg"
    assert response.image_path.stat().st_size <= 300_000
    assert latest_path.stat().st_size == raw_size
    assert response.image_info is not None
    assert response.image_info["size"] == response.image_path.stat().st_size
    assert response.image_info["w"] < 1280
    assert response.image_info["h"] < 720
    assert "Snapshot: fresh capture" in response.text
    assert "too large" not in response.text


def test_build_who_snapshot_response_falls_back_to_text_on_capture_failure(tmp_path: Path) -> None:
    from parking_spot_monitor.capture import CaptureError, DecodeMode
    from parking_spot_monitor.operator_cockpit import build_who_snapshot_response

    def capture_func(settings: object, data_dir: Path, **kwargs: object) -> object:
        raise CaptureError(
            reason="ffmpeg-timeout",
            mode=DecodeMode.SOFTWARE,
            output_path=tmp_path / "latest.jpg",
            message="capture failed with token syt_secret_matrix_token",
            timeout_seconds=2,
        )

    response = build_who_snapshot_response(
        settings=object(),
        data_dir=tmp_path,
        base_text="Parking monitor who\n- right_spot: open; no active vehicle session",
        capture_func=capture_func,
    )

    assert response.image_path is None
    assert response.image_info is None
    assert "Snapshot: fresh capture unavailable (ffmpeg-timeout); no live state was changed." in response.text
    assert "right_spot: open" in response.text
    assert "syt_secret" not in response.text
