from __future__ import annotations

from tests.support._operator_feedback import *  # noqa: F403


def test_make_label_id_is_deterministic_from_timestamp_spot_and_matrix_event() -> None:
    from parking_spot_monitor.operator_feedback import make_label_id

    corrected_at = datetime(2026, 5, 16, 17, 42, 39, tzinfo=timezone.utc)

    first = make_label_id(corrected_at=corrected_at, spot_id="left spot", matrix_event_id="$event-1")
    second = make_label_id(corrected_at=corrected_at, spot_id="left spot", matrix_event_id="$event-1")
    different_event = make_label_id(corrected_at=corrected_at, spot_id="left spot", matrix_event_id="$event-2")

    assert first == second
    assert first != different_event
    assert first.startswith("feedback-20260516T174239Z-left_spot-")


def test_feedback_label_serialization_rejects_invalid_reported_or_actual_state() -> None:
    from parking_spot_monitor.operator_feedback import FeedbackLabelSchemaError

    with pytest.raises(FeedbackLabelSchemaError):
        _sample_feedback_label(reported_state="blocked").to_json_dict()

    with pytest.raises(FeedbackLabelSchemaError):
        _sample_feedback_label(actual_state="available").to_json_dict()


def test_feedback_label_category_metadata_is_optional_backward_compatible_and_validated() -> None:
    from parking_spot_monitor.operator_feedback import FeedbackLabelSchemaError

    legacy_payload = _sample_feedback_label().to_json_dict()
    assert "feedback_category" not in legacy_payload
    assert "feedback_category_details" not in legacy_payload

    categorized = _sample_feedback_label(
        feedback_category="false_alert",
        feedback_category_details={
            "reported_state": "occupied",
            "actual_state": "open",
            "operator": "@operator:example",
        },
    ).to_json_dict()

    assert categorized["feedback_category"] == "false_alert"
    assert categorized["feedback_category_details"]["reported_state"] == "occupied"
    assert categorized["feedback_category_details"]["actual_state"] == "open"
    assert categorized["feedback_category_details"]["operator"].startswith("sha256:")

    with pytest.raises(FeedbackLabelSchemaError):
        _sample_feedback_label(feedback_category="generic_feedback").to_json_dict()


def test_feedback_serialization_consumes_only_limit_plus_one_for_sequences_and_metadata() -> None:
    from parking_spot_monitor import operator_feedback_models

    replay_context = CountingSequence(f"line {index}" for index in range(100))
    source_metadata = CountingMapping(
        [("sender", "@operator:example")]
        + [(f"metadata-{index}", index) for index in range(30)]
    )

    payload = _sample_feedback_label(
        label_type="learn",
        target_state="open",
        learned_at="2026-05-16T18:00:00Z",
        replay_context=replay_context,
        source_metadata=source_metadata,
    ).to_json_dict()

    assert replay_context.consumed == operator_feedback_models.MAX_REPLAY_CONTEXT_LINES + 1
    assert source_metadata.consumed == operator_feedback_models.MAX_METADATA_ITEMS + 1
    assert payload["replay_context"] == [f"line {index}" for index in range(12)]
    assert list(payload["source_metadata"]) == ["sender"] + [f"metadata-{index}" for index in range(15)]
    assert payload["source_metadata"]["sender"].startswith("sha256:")


def test_feedback_label_load_quarantines_invalid_state(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import load_feedback_labels

    path = tmp_path / "operator-feedback-labels.json"
    raw_label = _sample_feedback_label().to_json_dict() | {"actual_state": "blocked"}
    payload = {"schema_version": 1, "labels": [raw_label]}
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_feedback_labels(path)

    assert loaded.state == "unavailable"
    assert loaded.error_type == "FeedbackLabelSchemaError"
    assert loaded.quarantined_path is not None
    assert loaded.quarantined_path.exists()
    assert not path.exists()


def test_feedback_store_appends_sanitized_label_and_loads_tail(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import (
        FeedbackEvidence,
        FeedbackLabel,
        append_feedback_label,
        feedback_labels_path,
        load_feedback_labels,
    )

    path = feedback_labels_path(tmp_path)
    label = FeedbackLabel(
        label_id="feedback-20260516T174239Z-left_spot-abc12345",
        spot_id="left_spot",
        reported_state="occupied",
        actual_state="open",
        source="matrix_command",
        operator_sender_hash="sha256:operator",
        corrected_at="2026-05-16T17:42:39Z",
        reported_at="2026-05-15T21:42:39Z",
        alert_event_type="occupancy-occupied-event",
        alert_event_id="$alert",
        evidence=FeedbackEvidence(
            kind="alert_snapshot",
            path="snapshots/occupied.jpg",
            available=True,
            validated_jpeg=True,
            width=11,
            height=7,
            byte_size=633,
            error_type=None,
        ),
        notes=f"bad alert {FAKE_RTSP_URL} {FAKE_MATRIX_TOKEN} {RAW_IMAGE_MARKER} {TRACEBACK_TEXT}",
    )

    assert append_feedback_label(path, label)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert len(payload["labels"]) == 1
    stored = path.read_text(encoding="utf-8")
    _assert_no_sensitive_text(stored)

    loaded = load_feedback_labels(path)
    assert loaded.state == "available"
    assert len(loaded.labels) == 1
    assert loaded.labels[0].spot_id == "left_spot"
    assert loaded.labels[0].actual_state == "open"
    assert loaded.labels[0].evidence.available is True
    assert loaded.labels[0].evidence.width == 11


def test_feedback_store_retention_and_idempotent_matrix_event(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import FeedbackEvidence, FeedbackLabel, append_feedback_label, load_feedback_labels

    path = tmp_path / "operator-feedback-labels.json"
    evidence = FeedbackEvidence(kind="none", path=None, available=False, validated_jpeg=False, width=None, height=None, byte_size=None, error_type="missing")
    for index in range(5):
        assert append_feedback_label(
            path,
            FeedbackLabel(
                label_id=f"feedback-20260516T17423{index}Z-left_spot-abc12345",
                spot_id="left_spot",
                reported_state="occupied",
                actual_state="open",
                source="matrix_command",
                operator_sender_hash="sha256:operator",
                corrected_at=f"2026-05-16T17:42:3{index}Z",
                reported_at="2026-05-15T21:42:39Z",
                alert_event_type="occupancy-occupied-event",
                alert_event_id=f"$alert-{index}",
                evidence=evidence,
                notes="",
                matrix_event_id=f"$correct-{index}",
            ),
            max_labels=3,
        )

    assert append_feedback_label(
        path,
        FeedbackLabel(
            label_id="feedback-duplicate",
            spot_id="right_spot",
            reported_state="open",
            actual_state="occupied",
            source="matrix_command",
            operator_sender_hash="sha256:operator",
            corrected_at="2026-05-16T17:43:00Z",
            reported_at="2026-05-15T21:43:00Z",
            alert_event_type="occupancy-open-event",
            alert_event_id="$alert-duplicate",
            evidence=evidence,
            notes="",
            matrix_event_id="$correct-4",
        ),
        max_labels=3,
    )

    loaded = load_feedback_labels(path)
    assert [label.matrix_event_id for label in loaded.labels] == ["$correct-2", "$correct-3", "$correct-4"]
    assert [label.label_id for label in loaded.labels] == [
        "feedback-20260516T174232Z-left_spot-abc12345",
        "feedback-20260516T174233Z-left_spot-abc12345",
        "feedback-20260516T174234Z-left_spot-abc12345",
    ]


def test_feedback_store_quarantines_corrupt_and_oversized_without_leaking(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import load_feedback_labels

    path = tmp_path / "operator-feedback-labels.json"
    logger_stream = StringIO()
    logger = StructuredLogger(stream=logger_stream)

    path.write_text("not json " + FAKE_RTSP_URL + " " + FAKE_MATRIX_TOKEN, encoding="utf-8")
    corrupt = load_feedback_labels(path, logger=logger)
    assert corrupt.state == "unavailable"
    assert corrupt.quarantined_path is not None
    assert not path.exists()

    path.write_text("x" * 128, encoding="utf-8")
    oversized = load_feedback_labels(path, max_file_bytes=16, logger=logger)
    assert oversized.state == "unavailable"
    assert oversized.error_type == "oversized"
    assert oversized.quarantined_path is not None
    _assert_no_sensitive_text(logger_stream.getvalue())
