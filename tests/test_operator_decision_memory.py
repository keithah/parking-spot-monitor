from __future__ import annotations

import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import (
    SCHEMA_VERSION,
    append_decision_memory_record,
    decision_memory_path,
    format_recent_reply,
    format_why_reply,
    load_decision_memory,
    make_decision_memory_record,
)

FAKE_RTSP_URL = "rtsp://operator:super-secret@camera.example.local/live"
FAKE_MATRIX_TOKEN = "matrix-token-secret-value"
RAW_IMAGE_MARKER = "\xff\xd8RAW-JPEG-BYTES-should-never-appear"
NESTED_SECRET_MARKER = "nested-secret-marker-should-never-appear"
TRACEBACK_TEXT = "Traceback (most recent call last): boom"


def _memory_path(tmp_path: Path) -> Path:
    return decision_memory_path(tmp_path / "runtime")


def _record(kind: str, spot_id: str | None, summary: str, **details: object):
    return make_decision_memory_record(
        kind,
        observed_at=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
        spot_id=spot_id,
        summary=summary,
        details=details,
    )


def _assert_no_sensitive_text(rendered: str) -> None:
    assert FAKE_RTSP_URL not in rendered
    assert FAKE_MATRIX_TOKEN not in rendered
    assert RAW_IMAGE_MARKER not in rendered
    assert NESTED_SECRET_MARKER not in rendered
    assert "Traceback" not in rendered
    assert "super-secret" not in rendered


def test_append_load_and_format_why_for_spot_decision_contract(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)
    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)

    assert append_decision_memory_record(
        path,
        _record(
            "accepted_evidence",
            "right_spot",
            "accepted parked vehicle evidence",
            status="occupied",
            hit_streak=4,
            miss_streak=0,
            confidence=0.91,
            raw_image=RAW_IMAGE_MARKER,
            token=FAKE_MATRIX_TOKEN,
        ),
        logger=logger,
    )
    assert append_decision_memory_record(
        path,
        _record("miss", "left_spot", "no candidate for left spot", status="empty", miss_streak=5),
        logger=logger,
    )

    loaded = load_decision_memory(path, logger=logger)
    assert loaded.state == "available"
    assert [record.kind for record in loaded.records] == ["accepted_evidence", "miss"]

    reply = format_why_reply(path, "right_spot", logger=logger)
    assert "Parking decision memory for right_spot" in reply
    assert "accepted_evidence" in reply
    assert "accepted parked vehicle evidence" in reply
    assert "hit_streak: 4" in reply
    assert "left_spot" not in reply
    _assert_no_sensitive_text(reply + path.read_text(encoding="utf-8") + log_stream.getvalue())


def test_recent_timeline_includes_supported_record_kinds_and_is_bounded(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)
    kinds = [
        "accepted_evidence",
        "rejected_evidence",
        "miss",
        "confidence_dip",
        "suppression",
        "alert",
        "command_outcome",
        "lab_outcome",
    ]
    for index, kind in enumerate(kinds):
        assert append_decision_memory_record(
            path,
            _record(kind, "right_spot" if index % 2 == 0 else None, f"summary {index}", outcome="ok"),
            max_records=20,
        )

    reply = format_recent_reply(path, max_records=4, max_reply_bytes=600)
    assert "Parking decision memory recent" in reply
    assert "command_outcome" in reply
    assert "lab_outcome" in reply
    assert "accepted_evidence" not in reply
    assert len(reply.encode("utf-8")) <= 600


def test_missing_unknown_empty_and_invalid_spot_replies_are_safe(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)

    assert "Decision memory unavailable" in format_recent_reply(path)
    assert "no detector or camera work was run" in format_why_reply(path, "right_spot")

    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"schema_version": SCHEMA_VERSION, "records": []}), encoding="utf-8")
    assert "No recent decision memory" in format_recent_reply(path)
    assert "No recent decision memory for this spot" in format_why_reply(path, "right_spot")
    assert "Invalid spot id" in format_why_reply(path, "../state.json")


def test_corrupt_unsupported_and_oversized_files_are_quarantined_without_leaking(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)
    path.parent.mkdir(parents=True)
    log_stream = StringIO()
    logger = StructuredLogger(stream=log_stream)

    path.write_text("not json " + FAKE_RTSP_URL + " " + TRACEBACK_TEXT, encoding="utf-8")
    corrupt = load_decision_memory(path, logger=logger)
    assert corrupt.state == "unavailable"
    assert corrupt.quarantined_path is not None
    assert not path.exists()

    path.write_text(json.dumps({"schema_version": 999, "records": []}), encoding="utf-8")
    unsupported = load_decision_memory(path, logger=logger)
    assert unsupported.state == "unavailable"
    assert unsupported.quarantined_path is not None

    path.write_text("x" * 128, encoding="utf-8")
    oversized = load_decision_memory(path, max_file_bytes=16, logger=logger)
    assert oversized.state == "unavailable"
    assert oversized.error_type == "oversized"
    assert oversized.quarantined_path is not None
    _assert_no_sensitive_text(log_stream.getvalue())


def test_retention_trimming_and_bounded_load_tail(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)
    for index in range(8):
        assert append_decision_memory_record(
            path,
            _record("command_outcome", None, f"command {index}", outcome="ok"),
            max_records=3,
        )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert len(payload["records"]) == 3
    assert [record["summary"] for record in payload["records"]] == ["command 5", "command 6", "command 7"]

    loaded = load_decision_memory(path, max_records=2)
    assert [record.summary for record in loaded.records] == ["command 6", "command 7"]


def test_nested_secret_traceback_and_binary_like_values_are_redacted_and_clipped(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)
    long_text = "detail " * 300
    assert append_decision_memory_record(
        path,
        _record(
            "alert",
            "right_spot",
            "alert sent " + FAKE_RTSP_URL + " " + TRACEBACK_TEXT,
            outcome="sent",
            nested={"password": NESTED_SECRET_MARKER, "token": FAKE_MATRIX_TOKEN, "text": long_text},
            bytes=RAW_IMAGE_MARKER,
            error_type=TRACEBACK_TEXT,
        ),
    )

    stored = path.read_text(encoding="utf-8")
    reply = format_recent_reply(path, max_reply_bytes=900)
    assert "alert sent rtsp://<redacted>" in reply
    assert len(reply.encode("utf-8")) <= 900
    _assert_no_sensitive_text(stored + reply)


def test_append_failure_returns_false_and_logs_diagnostic(tmp_path: Path) -> None:
    parent_file = tmp_path / "not-a-directory"
    parent_file.write_text("blocking parent", encoding="utf-8")
    log_stream = StringIO()

    result = append_decision_memory_record(
        parent_file / "memory.json",
        _record("command_outcome", None, "cannot write below a file parent", error_type="NotADirectoryError"),
        logger=StructuredLogger(stream=log_stream),
    )

    assert result is False
    assert "operator-decision-memory-append-failed" in log_stream.getvalue()
    _assert_no_sensitive_text(log_stream.getvalue())

def test_recent_formats_lab_outcome_safe_details(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)
    assert append_decision_memory_record(
        path,
        make_decision_memory_record(
            "lab_outcome",
            observed_at=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
            summary="detection lab replay succeeded",
            details={
                "job_id": "lab-20260518T190000Z-abcdef12",
                "kind": "replay",
                "status": "succeeded",
                "phase": "complete",
                "report_path": "detection-lab/jobs/lab-20260518T190000Z-abcdef12/replay-report.json",
                "status_counts": {"occupied": 3, "open": 2},
                "coverage": {"assessed_frames": 5, "blocked_frames": 0, "not_assessed_frames": 1},
                "error_message": FAKE_RTSP_URL,
            },
        ),
    )

    reply = format_recent_reply(path, max_records=1, max_reply_bytes=1200)

    assert "lab_outcome" in reply
    assert "job_id: lab-20260518T190000Z-abcdef12" in reply
    assert "phase: complete" in reply
    assert "report_path: detection-lab/jobs/lab-20260518T190000Z-abcdef12/replay-report.json" in reply
    assert "status_counts:" in reply
    assert "coverage:" in reply
    _assert_no_sensitive_text(reply + path.read_text(encoding="utf-8"))

