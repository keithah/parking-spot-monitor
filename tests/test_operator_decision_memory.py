from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

import parking_spot_monitor.operator_decision_memory as decision_memory
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


class CountingSequence(Sequence[object]):
    def __init__(self, values: Iterable[object]) -> None:
        self._values = tuple(values)
        self.consumed = 0

    def __getitem__(self, index):
        return self._values[index]

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self):
        for item in self._values:
            self.consumed += 1
            yield item


class CountingList(list[object]):
    def __init__(self, values: Iterable[object]) -> None:
        super().__init__(values)
        self.consumed = 0

    def __iter__(self):
        for item in super().__iter__():
            self.consumed += 1
            yield item


class CountingMapping(Mapping[str, object]):
    def __init__(self, values: Iterable[tuple[str, object]]) -> None:
        self._values = dict(values)
        self.consumed = 0

    def __getitem__(self, key: str) -> object:
        return self._values[key]

    def __iter__(self):
        for key in self._values:
            self.consumed += 1
            yield key

    def __len__(self) -> int:
        return len(self._values)


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


def test_decision_memory_sanitizer_consumes_only_limit_plus_one_per_collection() -> None:
    sequence = CountingSequence(range(100))
    nested_mapping = CountingMapping(
        [("sequence", sequence), ("token", NESTED_SECRET_MARKER)]
        + [(f"nested-{index}", index) for index in range(30)]
    )
    details = CountingMapping(
        [("nested", nested_mapping)]
        + [(f"detail-{index}", index) for index in range(30)]
    )

    record = make_decision_memory_record("alert", details=details)

    assert details.consumed == decision_memory.MAX_MAPPING_ITEMS + 1
    assert nested_mapping.consumed == decision_memory.MAX_MAPPING_ITEMS + 1
    assert sequence.consumed == decision_memory.MAX_SEQUENCE_ITEMS + 1
    assert record.details is not None
    assert record.details["nested"]["token"] == "<redacted>"
    assert record.details["nested"]["sequence"][-1] == "<truncated>"
    assert record.details["nested"]["truncated"] is True
    assert record.details["truncated"] is True


def test_decision_memory_formatting_consumes_only_limit_plus_one_per_collection() -> None:
    sequence = CountingList(range(100))
    mapping = CountingMapping((f"key-{index}", index) for index in range(100))

    rendered_sequence = decision_memory._format_detail_value(sequence)
    rendered_mapping = decision_memory._format_detail_value(mapping)

    assert sequence.consumed == decision_memory._MAX_FORMAT_ITEMS + 1
    assert mapping.consumed == decision_memory._MAX_FORMAT_ITEMS + 1
    assert rendered_sequence == "0, 1, 2, 3, 4, 5, ..."
    assert rendered_mapping == "key-0=0; key-1=1; key-2=2; key-3=3; key-4=4; key-5=5; ..."


def test_batch_append_persists_multiple_records_once(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    records = [
        _record("command_outcome", None, "first"),
        _record("command_outcome", None, "second"),
    ]

    with patch(
        "parking_spot_monitor.operator_decision_memory._write_memory",
        wraps=decision_memory._write_memory,
    ) as write:
        assert decision_memory.append_decision_memory_records(path, records)

    assert write.call_count == 1
    assert [record.summary for record in load_decision_memory(path).records] == ["first", "second"]


def test_atomic_write_orders_permissions_file_fsync_replace_and_directory_fsync(
    tmp_path: Path,
) -> None:
    path = decision_memory_path(tmp_path)
    operations: list[str] = []
    directory_fds: set[int] = set()
    real_open = os.open
    real_close = os.close
    real_fchmod = os.fchmod
    real_fsync = os.fsync
    real_replace = os.replace

    def tracked_open(target, flags, *args, **kwargs):
        file_descriptor = real_open(target, flags, *args, **kwargs)
        if (
            isinstance(target, (str, bytes, os.PathLike))
            and Path(target) == path.parent
            and flags & os.O_DIRECTORY
        ):
            directory_fds.add(file_descriptor)
            operations.append("open-directory")
        return file_descriptor

    def tracked_close(file_descriptor: int) -> None:
        if file_descriptor in directory_fds:
            operations.append("close-directory")
            directory_fds.remove(file_descriptor)
        real_close(file_descriptor)

    def tracked_fchmod(file_descriptor: int, mode: int) -> None:
        operations.append("permissions")
        real_fchmod(file_descriptor, mode)

    def tracked_fsync(file_descriptor: int) -> None:
        operations.append(
            "fsync-directory" if file_descriptor in directory_fds else "fsync-file"
        )
        real_fsync(file_descriptor)

    def tracked_replace(source, destination) -> None:
        operations.append("replace")
        real_replace(source, destination)

    with (
        patch.object(os, "open", side_effect=tracked_open),
        patch.object(os, "close", side_effect=tracked_close),
        patch.object(os, "fchmod", side_effect=tracked_fchmod),
        patch.object(os, "fsync", side_effect=tracked_fsync),
        patch.object(os, "replace", side_effect=tracked_replace),
    ):
        assert append_decision_memory_record(
            path, _record("command_outcome", None, "durable")
        )

    assert operations == [
        "permissions",
        "fsync-file",
        "replace",
        "open-directory",
        "fsync-directory",
        "close-directory",
    ]
    assert [record.summary for record in load_decision_memory(path).records] == [
        "durable"
    ]


def test_concurrent_batch_append_preserves_both_writers(tmp_path: Path) -> None:
    for iteration in range(20):
        path = decision_memory_path(tmp_path / str(iteration))
        barrier = Barrier(2)

        def append(summary: str) -> bool:
            barrier.wait()
            return decision_memory.append_decision_memory_records(
                path,
                [_record("command_outcome", None, summary)],
            )

        summaries = [f"first-{iteration}", f"second-{iteration}"]
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(append, summaries))

        assert results == [True, True]
        assert {record.summary for record in load_decision_memory(path).records} == set(summaries)


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
        "feedback",
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
    assert "feedback" in reply
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


def test_why_reply_includes_rich_safe_decision_evidence_details(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)
    long_snapshot_ref = "snapshot-" + ("frame-" * 120) + FAKE_MATRIX_TOKEN
    records = [
        _record(
            "accepted_evidence",
            "right_spot",
            "accepted vehicle candidate",
            status="occupied",
            candidate_id="candidate-42",
            confidence=0.93,
            threshold=0.7,
            snapshot_ref=long_snapshot_ref,
            rtsp_url=FAKE_RTSP_URL,
        ),
        _record(
            "rejected_evidence",
            "right_spot",
            "rejected low-confidence vehicle candidate",
            status="open",
            candidate_id="candidate-99",
            rejection_reason="below occupancy threshold",
            confidence=0.41,
            threshold=0.7,
            snapshot_ref="snapshot-rejected-001",
        ),
        _record(
            "suppression",
            "right_spot",
            "suppressed alert while operator override is active",
            suppressed_reason="operator_override",
            suppression_until="2026-05-18T20:00:00Z",
            snapshot_ref="snapshot-suppressed-001",
        ),
        _record(
            "alert",
            "right_spot",
            "alert emitted for occupied status",
            alert="sent",
            alert_channel="matrix",
            snapshot_ref="snapshot-alert-001",
            token=FAKE_MATRIX_TOKEN,
        ),
        _record(
            "feedback",
            "right_spot",
            "operator marked the decision as wrong",
            feedback_label="false_positive",
            previous_status="occupied",
            new_status="open",
            rejection_reason="operator correction",
            snapshot_ref="snapshot-feedback-001",
        ),
    ]
    for record in records:
        assert append_decision_memory_record(path, record)

    reply = format_why_reply(path, "right_spot", max_records=10, max_reply_bytes=2400)

    assert "accepted_evidence" in reply
    assert "rejected_evidence" in reply
    assert "suppression" in reply
    assert "alert" in reply
    assert "feedback" in reply
    assert "candidate_id: candidate-42" in reply
    assert "candidate_id: candidate-99" in reply
    assert "threshold: 0.7" in reply
    assert "confidence: 0.93" in reply
    assert "confidence: 0.41" in reply
    assert "rejection_reason: below occupancy threshold" in reply
    assert "suppression_until: 2026-05-18T20:00:00Z" in reply
    assert "alert_channel: matrix" in reply
    assert "feedback_label: false_positive" in reply
    assert "snapshot_ref: snapshot-" in reply
    assert "snapshot_ref: snapshot-rejected-001" in reply
    assert len(reply.encode("utf-8")) <= 2400
    _assert_no_sensitive_text(reply + path.read_text(encoding="utf-8"))


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
    assert "status_counts: occupied=3; open=2" in reply
    assert "coverage: assessed_frames=5; blocked_frames=0; not_assessed_frames=1" in reply
    _assert_no_sensitive_text(reply + path.read_text(encoding="utf-8"))


def test_why_reply_recursively_formats_only_whitelisted_safe_details(tmp_path: Path) -> None:
    path = _memory_path(tmp_path)
    assert append_decision_memory_record(
        path,
        make_decision_memory_record(
            "feedback",
            observed_at=datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
            spot_id="right_spot",
            summary="operator learn label recorded",
            details={
                "label_id": "label-123",
                "label_type": "learn",
                "target_state": "occupied",
                "evidence_available": True,
                "degradation_reasons": ["missing_state", "replay_clipped", "extra-detail"],
                "unlisted_safe_but_private_context": "do not render this arbitrary detail",
                "Authorization": "Bearer should-never-render",
                "debug_dump": {"trace": TRACEBACK_TEXT, "rtsp": FAKE_RTSP_URL},
            },
        ),
    )

    reply = format_why_reply(path, "right_spot", max_records=1, max_reply_bytes=1200)

    assert "label_id: label-123" in reply
    assert "label_type: learn" in reply
    assert "target_state: occupied" in reply
    assert "evidence_available: True" in reply
    assert "degradation_reasons: missing_state, replay_clipped, extra-detail" in reply
    assert "unlisted_safe_but_private_context" not in reply
    assert "debug_dump" not in reply
    assert "Authorization" not in reply
    _assert_no_sensitive_text(reply + path.read_text(encoding="utf-8"))
