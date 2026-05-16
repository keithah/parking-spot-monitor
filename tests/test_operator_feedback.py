from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

from parking_spot_monitor.logging import StructuredLogger

FAKE_RTSP_URL = "rtsp://user:pass@example.local/live"
FAKE_MATRIX_TOKEN = "syt_secret_matrix_token"
RAW_IMAGE_MARKER = "\xff\xd8\xff\xe0 raw image bytes"
TRACEBACK_TEXT = "Traceback (most recent call last): secret stack"


def _assert_no_sensitive_text(rendered: str) -> None:
    assert "user:pass" not in rendered
    assert FAKE_MATRIX_TOKEN not in rendered
    assert RAW_IMAGE_MARKER not in rendered
    assert "Traceback" not in rendered


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
