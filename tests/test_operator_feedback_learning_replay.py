from __future__ import annotations

from tests.support._operator_feedback import *  # noqa: F403


def test_labeler_records_learn_label_from_retained_timeline_replay_without_state_side_effects(tmp_path: Path) -> None:
    from parking_spot_monitor.detection import VehicleDetection
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, load_decision_memory, make_decision_memory_record
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    settings = _learn_settings(tmp_path)
    frame = _write_learn_timeline_frame(tmp_path)
    state_path = tmp_path / "state.json"
    original_state = _write_learn_state(state_path)
    original_mtime_ns = state_path.stat().st_mtime_ns
    vehicle_history_path = tmp_path / "vehicle-history" / "corrections" / "events.jsonl"
    vehicle_history_path.parent.mkdir(parents=True)
    vehicle_history_path.write_text('{"existing":"vehicle history must stay unchanged"}\n', encoding="utf-8")
    original_vehicle_history = vehicle_history_path.read_text(encoding="utf-8")
    original_vehicle_history_mtime_ns = vehicle_history_path.stat().st_mtime_ns
    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "alert",
            observed_at="2026-05-18T02:38:00Z",
            spot_id="left_spot",
            summary="pre-existing open alert history must stay unchanged",
            details={"event_type": "occupancy-open-event", "event_id": "open-alert-before-learn", "outcome": "sent"},
        ),
    )
    detector = _LearnReplayDetector([VehicleDetection(class_name="car", confidence=0.91, bbox=(25, 30, 275, 325))])

    result = OperatorFeedbackLabeler(data_dir=tmp_path).record_learn_label(
        spot_id="left_spot",
        target_state="occupied",
        requested_time="7:39pm",
        settings=settings,
        state_path=state_path,
        detector=adapt_detector(detector),
        matrix_event_id="$learn-record",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert result.recorded is True
    assert result.error_type is None
    assert result.evidence.available is True
    assert result.evidence.validated_jpeg is True
    assert result.evidence.kind == "timeline_frame"
    assert result.evidence.path == "timeline/frames/20260518T023900Z.jpg"
    assert result.evidence.width == 1458
    assert result.evidence.height == 806
    assert "Parking learn label recorded" in result.reply_text
    assert "linked evidence: retained timeline frame" in result.reply_text
    assert "Detector replay:" in "\n".join(result.replay_context)
    assert detector.calls == [{"frame_path": frame, "confidence_threshold": 0.42, "inference_image_size": 960}]
    assert state_path.read_text(encoding="utf-8") == original_state
    assert state_path.stat().st_mtime_ns == original_mtime_ns
    assert vehicle_history_path.read_text(encoding="utf-8") == original_vehicle_history
    assert vehicle_history_path.stat().st_mtime_ns == original_vehicle_history_mtime_ns
    _assert_no_sensitive_text(result.reply_text)

    loaded = load_feedback_labels(feedback_labels_path(tmp_path))
    assert loaded.state == "available"
    assert len(loaded.labels) == 1
    label = loaded.labels[0]
    assert label.label_type == "learn"
    assert label.target_state == "occupied"
    assert label.evidence.path == "timeline/frames/20260518T023900Z.jpg"
    assert label.source_metadata is not None
    assert label.source_metadata["frame_delta_seconds"] == "0"
    assert label.feedback_category == "missed_alert"
    assert label.feedback_category_details == {"target_state": "occupied"}
    rendered = feedback_labels_path(tmp_path).read_text(encoding="utf-8")
    assert str(tmp_path) not in rendered
    _assert_no_sensitive_text(rendered)

    loaded_memory = load_decision_memory(memory_path)
    alert_records = [record for record in loaded_memory.records if record.kind == "alert"]
    feedback_records = [record for record in loaded_memory.records if record.kind == "feedback"]
    assert len(alert_records) == 1
    assert alert_records[0].summary == "pre-existing open alert history must stay unchanged"
    assert alert_records[0].details is not None
    assert alert_records[0].details["event_id"] == "open-alert-before-learn"
    assert len(feedback_records) == 1
    assert feedback_records[0].details is not None
    assert feedback_records[0].details["label_id"] == label.label_id
    assert feedback_records[0].details["feedback_category"] == "missed_alert"
    assert feedback_records[0].details["feedback_category_details"] == {"target_state": "occupied", "requested_at": "2026-05-18T02:39:00Z"}


def test_labeler_repeats_duplicate_learn_ack_without_duplicate_label_or_memory(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_decision_memory import load_decision_memory
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    settings = _learn_settings(tmp_path)
    _write_learn_timeline_frame(tmp_path)
    state_path = tmp_path / "state.json"
    _write_learn_state(state_path)
    labeler = OperatorFeedbackLabeler(data_dir=tmp_path)

    first = labeler.record_learn_label(
        spot_id="left_spot",
        target_state="open",
        requested_time="7:39pm",
        settings=settings,
        state_path=state_path,
        detector=adapt_detector(_LearnReplayDetector([])),
        matrix_event_id="$duplicate-learn",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )
    second = labeler.record_learn_label(
        spot_id="right_spot",
        target_state="occupied",
        requested_time="7:39pm",
        settings=settings,
        state_path=state_path,
        detector=adapt_detector(_LearnReplayDetector([])),
        matrix_event_id="$duplicate-learn",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert first.recorded is True
    assert second.recorded is True
    assert second.duplicate is True
    assert "already applied" in second.reply_text.lower()
    assert second.spot_id == "left_spot"
    assert second.target_state == "open"
    loaded_duplicate_labels = load_feedback_labels(feedback_labels_path(tmp_path)).labels
    assert len(loaded_duplicate_labels) == 1
    assert loaded_duplicate_labels[0].feedback_category == "missed_alert"
    assert loaded_duplicate_labels[0].feedback_category_details == {"target_state": "open"}
    feedback_records = [record for record in load_decision_memory(tmp_path / "operator-decision-memory.json").records if record.kind == "feedback"]
    assert len(feedback_records) == 1


@pytest.mark.parametrize(
    "case,corrupt_frame,corrupt_state,detections,expected_reason,recorded",
    [
        ("missing_timeline", False, False, [], "timeline_missing", False),
        ("corrupt_frame", True, False, [], "invalid_jpeg", False),
        ("detector_exception", False, False, RuntimeError("predict failed " + FAKE_MATRIX_TOKEN + RAW_IMAGE_MARKER), "RuntimeError", True),
        ("corrupt_state", False, True, [], "JSONDecodeError", True),
    ],
)
def test_labeler_learn_label_degrades_safely_for_timeline_detector_state_and_store_failures(
    tmp_path: Path,
    case: str,
    corrupt_frame: bool,
    corrupt_state: bool,
    detections: list[Any] | Exception,
    expected_reason: str,
    recorded: bool,
) -> None:
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path

    settings = _learn_settings(tmp_path)
    if case != "missing_timeline":
        _write_learn_timeline_frame(tmp_path, corrupt=corrupt_frame)
    state_path = tmp_path / "state.json"
    original_state = _write_learn_state(state_path, corrupt=corrupt_state)
    original_mtime_ns = state_path.stat().st_mtime_ns

    result = OperatorFeedbackLabeler(data_dir=tmp_path).record_learn_label(
        spot_id="left_spot",
        target_state="open",
        requested_time="7:39pm",
        settings=settings,
        state_path=state_path,
        detector=adapt_detector(_LearnReplayDetector(detections)),
        matrix_event_id=f"$learn-{case}",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert result.recorded is recorded
    assert expected_reason in result.degradation_reasons or result.error_type == expected_reason
    assert expected_reason in result.reply_text or result.error_type == expected_reason
    assert state_path.read_text(encoding="utf-8") == original_state
    assert state_path.stat().st_mtime_ns == original_mtime_ns
    rendered = result.reply_text
    if feedback_labels_path(tmp_path).exists():
        rendered += feedback_labels_path(tmp_path).read_text(encoding="utf-8")
    _assert_no_sensitive_text(rendered)


def test_labeler_learn_label_reports_feedback_store_unavailable_without_memory_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from parking_spot_monitor.operator_decision_memory import load_decision_memory
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path

    settings = _learn_settings(tmp_path)
    _write_learn_timeline_frame(tmp_path)
    state_path = tmp_path / "state.json"
    _write_learn_state(state_path)
    def fail_append(*args: Any, **kwargs: Any) -> object:
        from parking_spot_monitor.operator_feedback import FeedbackAppendResult

        return FeedbackAppendResult(status="failed")

    monkeypatch.setattr("parking_spot_monitor.operator_feedback.append_feedback_label", fail_append)

    result = OperatorFeedbackLabeler(data_dir=tmp_path).record_learn_label(
        spot_id="left_spot",
        target_state="open",
        requested_time="7:39pm",
        settings=settings,
        state_path=state_path,
        detector=adapt_detector(_LearnReplayDetector([])),
        matrix_event_id="$learn-store-fails",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        now=datetime(2026, 5, 18, 4, 0, tzinfo=timezone.utc),
    )

    assert result.recorded is False
    assert result.error_type == "feedback_store_unavailable"
    assert "Feedback store unavailable" in result.reply_text
    loaded_memory = load_decision_memory(tmp_path / "operator-decision-memory.json")
    assert loaded_memory.state == "missing"
