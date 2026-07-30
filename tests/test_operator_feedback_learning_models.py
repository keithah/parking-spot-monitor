from __future__ import annotations

from tests.support._operator_feedback import *  # noqa: F403


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
