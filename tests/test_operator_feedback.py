from __future__ import annotations

import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

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
    assert loaded.labels[0].feedback_category == "false_alert"
    assert loaded.labels[0].feedback_category_details == {"reported_state": "occupied", "actual_state": "open"}
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


def test_learn_feedback_label_serializes_safe_replay_context_and_hashes_matrix_ids(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import (
        FeedbackEvidence,
        append_feedback_label,
        feedback_labels_path,
        load_feedback_labels,
        make_learn_feedback_label,
    )

    path = feedback_labels_path(tmp_path)
    label = make_learn_feedback_label(
        spot_id="left_spot",
        target_state="open",
        learned_at="2026-05-16T18:00:00Z",
        matrix_event_id="$learn-1",
        matrix_sender="@operator:example",
        matrix_room_id="!room:example",
        evidence=FeedbackEvidence(
            kind="timeline_frame",
            path="timeline/left_spot/2026-05-16T18-00-00Z.jpg",
            available=True,
            validated_jpeg=True,
            width=640,
            height=360,
            byte_size=12345,
        ),
        replay_context=[
            f"detector used {FAKE_RTSP_URL}",
            f"model output {RAW_IMAGE_MARKER}",
            TRACEBACK_TEXT,
        ],
        degradation_reasons=[f"detector warning {FAKE_MATRIX_TOKEN}"],
        source_metadata={
            "command": "learn",
            "sender": "@operator:example",
            "room": "!room:example",
            "token": FAKE_MATRIX_TOKEN,
        },
    )

    result = append_feedback_label(path, label)

    assert result.status == "appended"
    rendered = path.read_text(encoding="utf-8")
    _assert_no_sensitive_text(rendered)
    assert "@operator:example" not in rendered
    assert "!room:example" not in rendered
    assert "sha256:" in rendered

    payload = json.loads(rendered)
    stored = payload["labels"][0]
    assert stored["label_type"] == "learn"
    assert stored["source"] == "matrix_learn_command"
    assert stored["target_state"] == "open"
    assert stored["learned_at"] == "2026-05-16T18:00:00Z"
    assert stored["evidence"]["kind"] == "timeline_frame"
    assert stored["evidence"]["path"] == "timeline/left_spot/2026-05-16T18-00-00Z.jpg"
    assert len(stored["replay_context"]) == 3
    assert stored["source_metadata"]["command"] == "learn"
    assert stored["feedback_category"] == "missed_alert"
    assert stored["feedback_category_details"] == {"target_state": "open"}

    loaded = load_feedback_labels(path)
    assert loaded.state == "available"
    assert len(loaded.labels) == 1
    loaded_label = loaded.labels[0]
    assert loaded_label.label_type == "learn"
    assert loaded_label.target_state == "open"
    assert loaded_label.matrix_event_id == "$learn-1"
    assert loaded_label.operator_sender_hash.startswith("sha256:")
    assert loaded_label.matrix_room_id_hash is not None
    assert loaded_label.matrix_room_id_hash.startswith("sha256:")


def test_learn_feedback_label_rejects_invalid_state_and_unsafe_evidence_path() -> None:
    from parking_spot_monitor.operator_feedback import FeedbackEvidence, FeedbackLabelSchemaError, make_learn_feedback_label

    with pytest.raises(FeedbackLabelSchemaError):
        make_learn_feedback_label(
            spot_id="left_spot",
            target_state="blocked",
            learned_at="2026-05-16T18:00:00Z",
            matrix_event_id="$learn-1",
            matrix_sender="@operator:example",
            matrix_room_id="!room:example",
            evidence=FeedbackEvidence("timeline_frame", "timeline/frame.jpg", True, True, 1, 1, 10),
        ).to_json_dict()

    with pytest.raises(FeedbackLabelSchemaError):
        make_learn_feedback_label(
            spot_id="left_spot",
            target_state="open",
            learned_at="2026-05-16T18:00:00Z",
            matrix_event_id="$learn-1",
            matrix_sender="@operator:example",
            matrix_room_id="!room:example",
            evidence=FeedbackEvidence("timeline_frame", "/private/frame.jpg", True, True, 1, 1, 10),
        ).to_json_dict()


def test_learn_feedback_label_retention_duplicate_and_old_correction_payload_compatibility(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_feedback import (
        FeedbackEvidence,
        append_feedback_label,
        load_feedback_labels,
        make_learn_feedback_label,
    )

    path = tmp_path / "operator-feedback-labels.json"
    old_correction = _sample_feedback_label().to_json_dict()
    old_correction.pop("label_type", None)
    path.write_text(json.dumps({"schema_version": 1, "labels": [old_correction]}), encoding="utf-8")

    loaded_old = load_feedback_labels(path)
    assert loaded_old.state == "available"
    assert loaded_old.labels[0].label_type == "correction"

    evidence = FeedbackEvidence("timeline_frame", "timeline/frame.jpg", True, True, 10, 10, 100)
    for index in range(4):
        assert append_feedback_label(
            path,
            make_learn_feedback_label(
                spot_id="left_spot",
                target_state="occupied" if index % 2 else "open",
                learned_at=f"2026-05-16T18:00:0{index}Z",
                matrix_event_id=f"$learn-{index}",
                matrix_sender="@operator:example",
                matrix_room_id="!room:example",
                evidence=evidence,
                replay_context=[f"line {index}"],
            ),
            max_labels=3,
        )

    duplicate = append_feedback_label(
        path,
        make_learn_feedback_label(
            spot_id="right_spot",
            target_state="open",
            learned_at="2026-05-16T18:01:00Z",
            matrix_event_id="$learn-3",
            matrix_sender="@operator:example",
            matrix_room_id="!room:example",
            evidence=evidence,
        ),
        max_labels=3,
    )

    assert duplicate.status == "duplicate"
    loaded = load_feedback_labels(path)
    assert [label.matrix_event_id for label in loaded.labels] == ["$learn-1", "$learn-2", "$learn-3"]
    assert all(label.label_type == "learn" for label in loaded.labels)


def _learn_settings(tmp_path: Path) -> Any:
    from parking_spot_monitor.config import load_settings

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
stream:
  rtsp_url_env: RTSP_URL
  frame_width: 1458
  frame_height: 806
spots:
  left_spot:
    name: Left curb spot
    polygon: [[10, 20], [300, 20], [300, 350], [10, 350]]
  right_spot:
    name: Right curb spot
    polygon: [[350, 20], [700, 20], [700, 350], [350, 350]]
detection:
  model: models/yolo11n.pt
  confidence_threshold: 0.42
  inference_image_size: 960
  open_suppression_min_confidence: 0.18
  vehicle_classes: [car, truck]
  min_bbox_area_px: 1200
  min_polygon_overlap_ratio: 0.27
occupancy:
  iou_threshold: 0.31
  confirm_frames: 4
  release_frames: 5
matrix:
  homeserver: https://matrix.example.invalid
  room_id: "!room:example.invalid"
  access_token_env: MATRIX_ACCESS_TOKEN
storage:
  data_dir: data
runtime:
  health_file: health.json
""".lstrip(),
        encoding="utf-8",
    )
    return load_settings(config_path, environ={"RTSP_URL": FAKE_RTSP_URL, "MATRIX_ACCESS_TOKEN": FAKE_MATRIX_TOKEN})


def _write_learn_timeline_frame(tmp_path: Path, name: str = "20260518T023900Z.jpg", *, corrupt: bool = False) -> Path:
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    frame = frames_dir / name
    if corrupt:
        frame.write_bytes(b"not a jpeg " + RAW_IMAGE_MARKER.encode("utf-8"))
    else:
        _write_jpeg(frame, size=(1458, 806))
    return frame


def _write_learn_state(path: Path, *, corrupt: bool = False) -> str:
    if corrupt:
        payload = "not json " + FAKE_MATRIX_TOKEN + " " + RAW_IMAGE_MARKER
        path.write_text(payload, encoding="utf-8")
        return payload
    payload = {
        "schema_version": 1,
        "spots": {
            "left_spot": {"status": "occupied", "hit_streak": 4, "miss_streak": 0, "open_event_emitted": False},
            "right_spot": {"status": "empty", "hit_streak": 0, "miss_streak": 5, "open_event_emitted": True},
        },
    }
    rendered = json.dumps(payload, sort_keys=True)
    path.write_text(rendered, encoding="utf-8")
    return rendered


class _LearnReplayDetector:
    def __init__(self, detections: list[Any] | Exception) -> None:
        self.detections = detections
        self.calls: list[dict[str, Any]] = []

    def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None, inference_image_size: int | None = None) -> list[Any]:
        self.calls.append({"frame_path": Path(frame_path), "confidence_threshold": confidence_threshold, "inference_image_size": inference_image_size})
        if isinstance(self.detections, Exception):
            raise self.detections
        return self.detections


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
        detector=detector,
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
        detector=_LearnReplayDetector([]),
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
        detector=_LearnReplayDetector([]),
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
        detector=_LearnReplayDetector(detections),
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
        detector=_LearnReplayDetector([]),
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
