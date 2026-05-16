from __future__ import annotations

import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pytest

from parking_spot_monitor.logging import StructuredLogger

FAKE_RTSP_URL = "rtsp://user:pass@example.local/live"
FAKE_MATRIX_TOKEN = "syt_secret_matrix_token"
RAW_IMAGE_MARKER = "\xff\xd8\xff\xe0 raw image bytes"
TRACEBACK_TEXT = "Traceback (most recent call last): secret stack"


def _assert_no_sensitive_text(rendered: str) -> None:
    assert "user:pass" not in rendered
    assert FAKE_MATRIX_TOKEN not in rendered
    assert RAW_IMAGE_MARKER not in rendered
    assert "\xff\xd8" not in rendered
    assert "\xff" not in rendered
    assert "\\u00ff" not in rendered
    assert "\\u00d8" not in rendered.lower()
    assert "Traceback" not in rendered


def _sample_feedback_label(**overrides):
    from parking_spot_monitor.operator_feedback import FeedbackEvidence, FeedbackLabel

    values = {
        "label_id": "feedback-20260516T174239Z-left_spot-abc12345",
        "spot_id": "left_spot",
        "reported_state": "occupied",
        "actual_state": "open",
        "source": "matrix_command",
        "operator_sender_hash": "sha256:operator",
        "corrected_at": "2026-05-16T17:42:39Z",
        "reported_at": "2026-05-15T21:42:39Z",
        "alert_event_type": "occupancy-occupied-event",
        "alert_event_id": "$alert",
        "evidence": FeedbackEvidence(
            kind="alert_snapshot",
            path="snapshots/occupied.jpg",
            available=True,
            validated_jpeg=True,
            width=11,
            height=7,
            byte_size=633,
            error_type=None,
        ),
        "notes": "",
        "matrix_event_id": "$correct",
    }
    values.update(overrides)
    return FeedbackLabel(**values)


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


def _write_jpeg(path: Path, *, size: tuple[int, int] = (11, 7)) -> int:
    from PIL import Image

    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", size, color=(128, 64, 32))
    image.save(path, format="JPEG")
    return path.stat().st_size


def test_labeler_records_correction_from_latest_alert_with_retained_snapshot(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    snapshot_path = tmp_path / "snapshots" / "occupancy-occupied-event-left_spot.jpg"
    byte_size = _write_jpeg(snapshot_path, size=(13, 9))
    assert append_decision_memory_record(
        decision_memory_path(tmp_path),
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
    assert loaded.labels[0].operator_sender_hash.startswith("sha256:")
    assert "@operator:example" not in (tmp_path / "operator-feedback-labels.json").read_text(encoding="utf-8")


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
