from __future__ import annotations

from tests.support._operator_feedback import *  # noqa: F403


def test_labeler_records_correction_from_latest_alert_with_retained_snapshot(tmp_path: Path) -> None:
    from parking_spot_monitor.decision_memory_store import DecisionMemoryStore
    from parking_spot_monitor.operator_decision_memory import decision_memory_path, load_decision_memory, make_decision_memory_record
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    snapshot_path = tmp_path / "snapshots" / "occupancy-occupied-event-left_spot.jpg"
    byte_size = _write_jpeg(snapshot_path, size=(13, 9))
    memory_path = decision_memory_path(tmp_path)
    store = DecisionMemoryStore(
        memory_path,
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=50,
    )
    assert store.append(
        make_decision_memory_record(
            "alert",
            observed_at="2026-05-15T21:42:39Z",
            spot_id="left_spot",
            summary="occupancy-occupied-event sent",
            details={
                "event_type": "occupancy-occupied-event",
                "event_id": "occupancy-occupied-event:left_spot:2026-05-15T21:42:39Z",
                "outcome": "sent",
                "snapshot_path": "snapshots/occupancy-occupied-event-left_spot.jpg",
            },
        ),
        durability="immediate",
    )

    result = OperatorFeedbackLabeler(data_dir=tmp_path, decision_memory_store=store).record_correction(
        spot_id="left_spot",
        actual_state="open",
        matrix_event_id="$correct",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        corrected_at="2026-05-16T17:42:39Z",
    )

    assert result.recorded is True
    assert result.reported_state == "occupied"
    assert result.evidence.available is True
    assert result.evidence.validated_jpeg is True
    assert result.evidence.path == "snapshots/occupancy-occupied-event-left_spot.jpg"
    assert result.evidence.width == 13
    assert result.evidence.height == 9
    assert result.evidence.byte_size == byte_size
    assert "Parking correction recorded" in result.reply_text
    assert "linked evidence: retained alert snapshot" in result.reply_text

    loaded = load_feedback_labels(feedback_labels_path(tmp_path))
    assert len(loaded.labels) == 1
    assert loaded.labels[0].reported_state == "occupied"
    assert loaded.labels[0].actual_state == "open"
    assert loaded.labels[0].feedback_category == "false_alert"
    assert loaded.labels[0].feedback_category_details == {"reported_state": "occupied", "actual_state": "open"}
    assert loaded.labels[0].operator_sender_hash.startswith("sha256:")
    assert "@operator:example" not in (tmp_path / "operator-feedback-labels.json").read_text(encoding="utf-8")
    assert load_decision_memory(memory_path).records[-1].kind == "feedback"


def test_labeler_repeats_duplicate_correction_ack_without_duplicate_label_or_memory(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, load_decision_memory, make_decision_memory_record
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "alert",
            observed_at="2026-05-15T21:42:39Z",
            spot_id="left_spot",
            summary="occupancy-occupied-event sent",
            details={
                "event_type": "occupancy-occupied-event",
                "event_id": "sent-occupied-alert",
                "outcome": "sent",
                "snapshot_path": "snapshots/missing.jpg",
            },
        ),
    )
    labeler = OperatorFeedbackLabeler(data_dir=tmp_path)

    first = labeler.record_correction(
        spot_id="left_spot",
        actual_state="open",
        matrix_event_id="$duplicate-correct",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        corrected_at="2026-05-16T17:42:39Z",
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "alert",
            observed_at="2026-05-15T21:45:00Z",
            spot_id="left_spot",
            summary="occupancy-open-event sent",
            details={
                "event_type": "occupancy-open-event",
                "event_id": "sent-open-alert",
                "outcome": "sent",
                "snapshot_path": "snapshots/missing-open.jpg",
            },
        ),
    )
    second = labeler.record_correction(
        spot_id="left_spot",
        actual_state="occupied",
        matrix_event_id="$duplicate-correct",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        corrected_at="2026-05-16T17:43:39Z",
    )

    assert first.recorded is True
    assert second.recorded is True
    assert "already applied" in second.reply_text.lower()
    assert "acknowledgement repeated" in second.reply_text.lower()
    assert second.reported_state == "occupied"
    assert second.actual_state == "open"

    loaded_labels = load_feedback_labels(feedback_labels_path(tmp_path))
    assert len(loaded_labels.labels) == 1

    loaded_memory = load_decision_memory(memory_path)
    feedback_records = [record for record in loaded_memory.records if record.kind == "feedback"]
    assert len(feedback_records) == 1
    assert feedback_records[0].details is not None
    assert feedback_records[0].details["feedback_category"] == "false_alert"
    assert feedback_records[0].details["feedback_category_details"] == {"reported_state": "occupied", "actual_state": "open"}


def test_labeler_binds_correction_to_latest_sent_alert_not_newer_failed_alert(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    memory_path = decision_memory_path(tmp_path)
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "alert",
            observed_at="2026-05-15T21:40:00Z",
            spot_id="left_spot",
            summary="occupancy-occupied-event sent",
            details={
                "event_type": "occupancy-occupied-event",
                "event_id": "sent-occupied-alert",
                "outcome": "sent",
                "snapshot_path": "snapshots/sent-occupied.jpg",
            },
        ),
    )
    assert append_decision_memory_record(
        memory_path,
        make_decision_memory_record(
            "alert",
            observed_at="2026-05-15T21:45:00Z",
            spot_id="left_spot",
            summary="occupancy-open-event failed",
            details={
                "event_type": "occupancy-open-event",
                "event_id": "failed-open-alert",
                "outcome": "failed",
                "snapshot_path": "snapshots/failed-open.jpg",
            },
        ),
    )

    result = OperatorFeedbackLabeler(data_dir=tmp_path).record_correction(
        spot_id="left_spot",
        actual_state="open",
        matrix_event_id="$correct",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        corrected_at="2026-05-16T17:42:39Z",
    )

    assert result.recorded is True
    assert result.reported_state == "occupied"

    loaded = load_feedback_labels(feedback_labels_path(tmp_path))
    assert loaded.state == "available"
    assert len(loaded.labels) == 1
    assert loaded.labels[0].reported_state == "occupied"
    assert loaded.labels[0].alert_event_id == "sent-occupied-alert"


def test_labeler_rejects_when_no_recent_alert_exists(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path

    result = OperatorFeedbackLabeler(data_dir=tmp_path).record_correction(
        spot_id="left_spot",
        actual_state="open",
        matrix_event_id="$correct",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        corrected_at="2026-05-16T17:42:39Z",
    )

    assert result.recorded is False
    assert result.error_type == "no_recent_alert"
    assert "Parking correction not recorded" in result.reply_text
    assert "No recent alert was found for left_spot" in result.reply_text
    assert not feedback_labels_path(tmp_path).exists()


def test_labeler_records_with_unavailable_evidence_when_snapshot_is_pruned(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler

    assert append_decision_memory_record(
        decision_memory_path(tmp_path),
        make_decision_memory_record(
            "alert",
            observed_at="2026-05-15T21:42:39Z",
            spot_id="right_spot",
            summary="occupancy-open-event sent",
            details={
                "event_type": "occupancy-open-event",
                "event_id": "occupancy-open-event:right_spot:2026-05-15T21:42:39Z",
                "outcome": "sent",
                "snapshot_path": "snapshots/missing.jpg",
            },
        ),
    )

    result = OperatorFeedbackLabeler(data_dir=tmp_path).record_correction(
        spot_id="right_spot",
        actual_state="occupied",
        matrix_event_id="$correct",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        corrected_at="2026-05-16T17:42:39Z",
    )

    assert result.recorded is True
    assert result.reported_state == "open"
    assert result.evidence.available is False
    assert result.evidence.error_type == "missing"
    assert "linked evidence: unavailable" in result.reply_text
