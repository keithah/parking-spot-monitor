# Operator Feedback Labels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add authorized Matrix operator feedback labels for wrong spot-state reports and attach a fresh on-demand camera snapshot to `!parking who` replies.

**Architecture:** Keep Matrix parsing and authorization in `parking_spot_monitor.matrix`, add a focused `parking_spot_monitor.operator_feedback` module for durable feedback-label persistence and alert-evidence resolution, and reuse the existing Matrix command image-upload path for `!parking who`. Runtime wiring passes a feedback labeler and a bounded fresh-capture provider into `MatrixCommandService`; feedback labels never mutate live occupancy state, vehicle identity state, detector thresholds, or model configuration.

**Tech Stack:** Python 3.11+, pytest, Pillow JPEG validation, existing Matrix Client-Server API wrapper, existing FFmpeg `capture_latest` path, existing structured logging/redaction utilities.

---

## File structure

- Create `parking_spot_monitor/operator_feedback.py`
  - Owns `operator-feedback-labels.json` schema, atomic append/load, retention, redaction, alert-memory lookup, JPEG evidence validation, and user-facing correction reply formatting.
  - Exposes `OperatorFeedbackLabeler.record_correction(...)` as the only high-level write API used by Matrix commands.
- Create `tests/test_operator_feedback.py`
  - Unit tests for label storage, redaction, retention, alert resolution, snapshot validation, no-recent-alert handling, and decision-memory command outcome recording.
- Modify `parking_spot_monitor/operator_decision_memory.py`
  - Add a supported `feedback` record kind so recent memory can show correction outcomes without downgrading to generic command output.
- Modify `parking_spot_monitor/__main__.py`
  - Add snapshot path metadata to alert decision-memory details.
  - Wire `OperatorFeedbackLabeler` and a `!parking who` fresh-capture provider into the default Matrix command service.
- Modify `parking_spot_monitor/matrix.py`
  - Add `actual_state` to `MatrixCommand`.
  - Parse `!parking correct <spot_id> <open|occupied>`.
  - Route authorized corrections to the feedback labeler.
  - Enrich `!parking who` responses with an optional image provider.
  - Update help text.
- Modify `parking_spot_monitor/operator_cockpit.py`
  - Add a bounded `build_who_snapshot_response(...)` helper that runs one `capture_latest(...)`, validates the resulting `latest.jpg`, and returns a `MatrixCommandResponse` with the original active-session summary plus snapshot metadata.
- Modify `tests/test_matrix.py`
  - Parser, service, authorization, image reply, duplicate handling, and help tests.
- Modify `tests/test_matrix_operator_cockpit.py`
  - Fresh capture success/failure tests for the new helper.
- Modify `tests/test_operator_decision_memory.py`
  - Supported `feedback` record kind coverage.
- Modify `tests/test_startup.py`
  - Runtime wiring test for the default command service.
- Modify `README.md`
  - Operator docs for `!parking correct` and updated `!parking who` behavior.
- Modify `tests/test_operator_docs.py`
  - Documentation contract assertions.

## Implementation notes

- Use TDD for each task: write the named failing test first, run it to see the expected failure, implement only the minimal code for that task, rerun the focused test, then commit.
- Do not use broad live Docker proof for this feature. The final verification is the Python test suite plus targeted unit tests that prove capture is bounded and injected/fakeable.
- Do not log or store raw image bytes, RTSP URLs, Matrix tokens, Authorization headers, raw Matrix response bodies, or tracebacks.
- Do not make `!parking correct` change live state. It only appends feedback-label and decision-memory records.
- Do not make `!parking who` run detector/model inference. It only performs one frame capture and uploads the resulting validated JPEG when available.

---

### Task 1: Add durable operator feedback label storage

**Files:**
- Create: `parking_spot_monitor/operator_feedback.py`
- Create: `tests/test_operator_feedback.py`

- [ ] **Step 1: Write failing storage tests**

Add this initial test file:

```python
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
```

- [ ] **Step 2: Run storage tests to verify they fail**

Run:

```bash
python -m pytest tests/test_operator_feedback.py -q
```

Expected: fail during import with `ModuleNotFoundError: No module named 'parking_spot_monitor.operator_feedback'`.

- [ ] **Step 3: Create `operator_feedback.py` with storage primitives**

Create `parking_spot_monitor/operator_feedback.py` with these public types and functions. Keep helper implementations local to the module.

```python
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value

SCHEMA_VERSION = 1
FEEDBACK_LABELS_FILENAME = "operator-feedback-labels.json"
MAX_FEEDBACK_FILE_BYTES = 512_000
MAX_LABELS = 500
MAX_TEXT_FIELD_CHARS = 500

FeedbackLoadState = Literal["available", "missing", "unavailable"]
SpotState = Literal["open", "occupied"]


class FeedbackLabelSchemaError(ValueError):
    """Raised when persisted operator feedback labels are not supported."""


@dataclass(frozen=True)
class FeedbackEvidence:
    kind: str
    path: str | None
    available: bool
    validated_jpeg: bool
    width: int | None
    height: int | None
    byte_size: int | None
    error_type: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": _clip_text(self.kind, 80),
            "available": bool(self.available),
            "validated_jpeg": bool(self.validated_jpeg),
        }
        if self.path:
            payload["path"] = _safe_relative_path(self.path)
        if self.width is not None:
            payload["width"] = _positive_int(self.width)
        if self.height is not None:
            payload["height"] = _positive_int(self.height)
        if self.byte_size is not None:
            payload["byte_size"] = _positive_int(self.byte_size)
        if self.error_type:
            payload["error_type"] = _clip_text(self.error_type, 80)
        return payload


@dataclass(frozen=True)
class FeedbackLabel:
    label_id: str
    spot_id: str
    reported_state: SpotState
    actual_state: SpotState
    source: str
    operator_sender_hash: str
    corrected_at: str
    reported_at: str | None
    alert_event_type: str | None
    alert_event_id: str | None
    evidence: FeedbackEvidence
    notes: str = ""
    matrix_event_id: str | None = None
    matrix_room_id_hash: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "label_id": _safe_identifier(self.label_id, "feedback"),
            "spot_id": _safe_identifier(self.spot_id, "unknown_spot"),
            "reported_state": _state(self.reported_state),
            "actual_state": _state(self.actual_state),
            "source": _clip_text(self.source, 80),
            "operator_sender_hash": _clip_text(self.operator_sender_hash, 80),
            "corrected_at": _timestamp_text(self.corrected_at),
            "evidence": self.evidence.to_json_dict(),
        }
        if self.reported_at:
            payload["reported_at"] = _timestamp_text(self.reported_at)
        if self.alert_event_type:
            payload["alert_event_type"] = _clip_text(self.alert_event_type, 120)
        if self.alert_event_id:
            payload["alert_event_id"] = _clip_text(self.alert_event_id, 160)
        if self.notes:
            payload["notes"] = _clip_text(self.notes, MAX_TEXT_FIELD_CHARS)
        if self.matrix_event_id:
            payload["matrix_event_id"] = _clip_text(self.matrix_event_id, 160)
        if self.matrix_room_id_hash:
            payload["matrix_room_id_hash"] = _clip_text(self.matrix_room_id_hash, 80)
        return redact_diagnostic_value(payload)  # type: ignore[return-value]


@dataclass(frozen=True)
class FeedbackLabelLoad:
    state: FeedbackLoadState
    labels: tuple[FeedbackLabel, ...] = ()
    error_type: str | None = None
    quarantined_path: Path | None = None


def feedback_labels_path(data_dir: str | Path) -> Path:
    return Path(data_dir) / FEEDBACK_LABELS_FILENAME


def hash_operator_identifier(value: str | None) -> str:
    digest = hashlib.sha256((value or "").encode("utf-8", errors="replace")).hexdigest()
    return f"sha256:{digest}"


def make_label_id(*, corrected_at: str, spot_id: str, matrix_event_id: str | None) -> str:
    seed = f"{corrected_at}|{spot_id}|{matrix_event_id or ''}"
    suffix = hashlib.sha256(seed.encode("utf-8", errors="replace")).hexdigest()[:8]
    compact_time = reformat_timestamp_for_id(corrected_at)
    return f"feedback-{compact_time}-{_safe_identifier(spot_id, 'spot')}-{suffix}"


def reformat_timestamp_for_id(value: str) -> str:
    text = _timestamp_text(value)
    return text.replace("-", "").replace(":", "").replace("+00:00", "Z")


def append_feedback_label(
    path: str | Path,
    label: FeedbackLabel | Mapping[str, Any],
    *,
    max_labels: int = MAX_LABELS,
    max_file_bytes: int = MAX_FEEDBACK_FILE_BYTES,
    logger: StructuredLogger | None = None,
) -> bool:
    label_path = Path(path)
    try:
        new_label = _label_from_any(label)
        loaded = load_feedback_labels(label_path, max_file_bytes=max_file_bytes, logger=logger)
        retained = [existing for existing in loaded.labels if not _same_matrix_event(existing, new_label)]
        retained.append(new_label)
        retained = retained[-_positive_limit(max_labels, MAX_LABELS) :]
        _write_labels(label_path, retained)
    except Exception as exc:
        _log(logger, "warning", "operator-feedback-label-append-failed", path=label_path, error_type=type(exc).__name__, error=str(exc))
        return False
    _log(logger, "info", "operator-feedback-label-appended", path=label_path, label_count=len(retained), spot_id=new_label.spot_id)
    return True


def load_feedback_labels(
    path: str | Path,
    *,
    max_labels: int = MAX_LABELS,
    max_file_bytes: int = MAX_FEEDBACK_FILE_BYTES,
    logger: StructuredLogger | None = None,
) -> FeedbackLabelLoad:
    label_path = Path(path)
    if not label_path.exists():
        return FeedbackLabelLoad(state="missing")
    try:
        size = label_path.stat().st_size
    except OSError as exc:
        _log(logger, "warning", "operator-feedback-label-load-failed", path=label_path, phase="stat", error_type=type(exc).__name__, error=str(exc))
        return FeedbackLabelLoad(state="unavailable", error_type=type(exc).__name__)
    if size > max_file_bytes:
        quarantined = _quarantine_file(label_path)
        _log(logger, "warning", "operator-feedback-label-quarantined", path=label_path, quarantine_path=quarantined, phase="size", error_type="oversized")
        return FeedbackLabelLoad(state="unavailable", error_type="oversized", quarantined_path=quarantined)
    try:
        with label_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        labels = _labels_from_payload(payload)
    except (OSError, json.JSONDecodeError, FeedbackLabelSchemaError) as exc:
        quarantined = _quarantine_file(label_path)
        _log(logger, "warning", "operator-feedback-label-quarantined", path=label_path, quarantine_path=quarantined, phase="load", error_type=type(exc).__name__, error=str(exc))
        return FeedbackLabelLoad(state="unavailable", error_type=type(exc).__name__, quarantined_path=quarantined)
    return FeedbackLabelLoad(state="available", labels=tuple(labels[-_positive_limit(max_labels, MAX_LABELS) :]))
```

Then implement the private helpers used above in the same module:

```python
def _write_labels(path: Path, labels: Sequence[FeedbackLabel]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": SCHEMA_VERSION, "labels": [label.to_json_dict() for label in labels]}
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent, prefix=f".{path.name}.", suffix=".tmp") as handle:
            temp_path = Path(handle.name)
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_path, 0o644)
        os.replace(temp_path, path)
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise


def _labels_from_payload(payload: Any) -> list[FeedbackLabel]:
    if not isinstance(payload, Mapping):
        raise FeedbackLabelSchemaError("feedback label payload must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise FeedbackLabelSchemaError("unsupported feedback label schema_version")
    raw_labels = payload.get("labels")
    if not isinstance(raw_labels, list):
        raise FeedbackLabelSchemaError("feedback labels must be a list")
    return [_label_from_any(item) for item in raw_labels]


def _label_from_any(value: FeedbackLabel | Mapping[str, Any]) -> FeedbackLabel:
    if isinstance(value, FeedbackLabel):
        return FeedbackLabel(**value.to_json_dict())
    if not isinstance(value, Mapping):
        raise FeedbackLabelSchemaError("feedback label must be an object")
    evidence = _evidence_from_any(value.get("evidence"))
    return FeedbackLabel(
        label_id=_required_text(value, "label_id"),
        spot_id=_safe_identifier(_required_text(value, "spot_id"), "unknown_spot"),
        reported_state=_state(_required_text(value, "reported_state")),
        actual_state=_state(_required_text(value, "actual_state")),
        source=_clip_text(value.get("source", "matrix_command"), 80),
        operator_sender_hash=_clip_text(value.get("operator_sender_hash", "sha256:"), 80),
        corrected_at=_timestamp_text(value.get("corrected_at")),
        reported_at=_optional_timestamp_text(value.get("reported_at")),
        alert_event_type=_optional_text(value.get("alert_event_type"), 120),
        alert_event_id=_optional_text(value.get("alert_event_id"), 160),
        evidence=evidence,
        notes=_clip_text(value.get("notes", ""), MAX_TEXT_FIELD_CHARS),
        matrix_event_id=_optional_text(value.get("matrix_event_id"), 160),
        matrix_room_id_hash=_optional_text(value.get("matrix_room_id_hash"), 80),
    )


def _evidence_from_any(value: Any) -> FeedbackEvidence:
    if isinstance(value, FeedbackEvidence):
        return FeedbackEvidence(**value.to_json_dict())
    if not isinstance(value, Mapping):
        raise FeedbackLabelSchemaError("feedback evidence must be an object")
    return FeedbackEvidence(
        kind=_clip_text(value.get("kind", "none"), 80),
        path=_optional_text(value.get("path"), 300),
        available=value.get("available") is True,
        validated_jpeg=value.get("validated_jpeg") is True,
        width=_optional_positive_int(value.get("width")),
        height=_optional_positive_int(value.get("height")),
        byte_size=_optional_positive_int(value.get("byte_size")),
        error_type=_optional_text(value.get("error_type"), 80),
    )


def _same_matrix_event(existing: FeedbackLabel, new_label: FeedbackLabel) -> bool:
    return bool(existing.matrix_event_id and new_label.matrix_event_id and existing.matrix_event_id == new_label.matrix_event_id)


def _state(value: object) -> SpotState:
    text = str(value).strip().lower()
    if text in {"open", "occupied"}:
        return text  # type: ignore[return-value]
    raise FeedbackLabelSchemaError("feedback state must be open or occupied")


def _required_text(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise FeedbackLabelSchemaError(f"feedback label {key} is required")
    return _clip_text(value, MAX_TEXT_FIELD_CHARS)


def _optional_text(value: Any, limit: int) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return _clip_text(value, limit)


def _timestamp_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str) and value.strip():
        text = value.strip()
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    raise FeedbackLabelSchemaError("timestamp is required")


def _optional_timestamp_text(value: object) -> str | None:
    if value is None or value == "":
        return None
    return _timestamp_text(value)


def _safe_identifier(value: object, default: str) -> str:
    text = redact_diagnostic_text(value).strip()[:160]
    return text if text and all(ch.isalnum() or ch in "._:-" for ch in text) else default


def _safe_relative_path(value: object) -> str:
    text = redact_diagnostic_text(value).strip()[:300]
    path = Path(text)
    if path.is_absolute() or ".." in path.parts:
        return "unavailable"
    return text


def _clip_text(value: object, limit: int) -> str:
    text = redact_diagnostic_text(value).replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _positive_int(value: object) -> int:
    result = int(value)
    if result <= 0:
        raise FeedbackLabelSchemaError("positive integer required")
    return result


def _optional_positive_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return _positive_int(value)
    except (TypeError, ValueError, FeedbackLabelSchemaError):
        return None


def _positive_limit(value: int, default: int) -> int:
    return value if isinstance(value, int) and value > 0 else default


def _quarantine_file(path: Path) -> Path | None:
    quarantine_path = path.with_suffix(path.suffix + ".quarantined")
    try:
        os.replace(path, quarantine_path)
        return quarantine_path
    except OSError:
        return None


def _log(logger: StructuredLogger | None, level: str, event_name: str, **fields: Any) -> None:
    if logger is None:
        return
    log = getattr(logger, level)
    log(event_name, **redact_diagnostic_value(fields))
```

- [ ] **Step 4: Run storage tests to verify they pass**

Run:

```bash
python -m pytest tests/test_operator_feedback.py -q
```

Expected: all tests in `tests/test_operator_feedback.py` pass.

- [ ] **Step 5: Commit storage module**

Run:

```bash
git add parking_spot_monitor/operator_feedback.py tests/test_operator_feedback.py
git commit -m "feat: add operator feedback label store"
```

---

### Task 2: Resolve correction evidence from decision memory and alert snapshots

**Files:**
- Modify: `parking_spot_monitor/operator_feedback.py`
- Modify: `tests/test_operator_feedback.py`
- Modify: `parking_spot_monitor/__main__.py`
- Test: `tests/test_operator_feedback.py`

- [ ] **Step 1: Write failing evidence-resolution tests**

Append these tests to `tests/test_operator_feedback.py`:

```python
from PIL import Image


def _write_jpeg(path: Path, *, size: tuple[int, int] = (11, 7)) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", size, color=(128, 64, 32))
    image.save(path, format="JPEG")
    return path.stat().st_size


def test_labeler_records_correction_from_latest_alert_with_retained_snapshot(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_decision_memory import append_decision_memory_record, decision_memory_path, make_decision_memory_record
    from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler, feedback_labels_path, load_feedback_labels

    snapshots_dir = tmp_path / "snapshots"
    snapshot_path = snapshots_dir / "occupancy-occupied-event-left_spot.jpg"
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
```

- [ ] **Step 2: Run evidence tests to verify they fail**

Run:

```bash
python -m pytest tests/test_operator_feedback.py::test_labeler_records_correction_from_latest_alert_with_retained_snapshot tests/test_operator_feedback.py::test_labeler_rejects_when_no_recent_alert_exists tests/test_operator_feedback.py::test_labeler_records_with_unavailable_evidence_when_snapshot_is_pruned -q
```

Expected: fail because `OperatorFeedbackLabeler` is not defined.

- [ ] **Step 3: Implement alert evidence resolution and labeler**

Extend `parking_spot_monitor/operator_feedback.py` with these imports:

```python
from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.operator_decision_memory import (
    append_decision_memory_record,
    decision_memory_path,
    load_decision_memory,
    make_decision_memory_record,
)
```

Add these dataclasses and labeler:

```python
@dataclass(frozen=True)
class AlertEvidenceCandidate:
    spot_id: str
    reported_state: SpotState
    reported_at: str | None
    alert_event_type: str | None
    alert_event_id: str | None
    snapshot_path: str | None


@dataclass(frozen=True)
class FeedbackRecordResult:
    recorded: bool
    reply_text: str
    spot_id: str
    actual_state: SpotState
    reported_state: SpotState | None = None
    evidence: FeedbackEvidence = FeedbackEvidence(kind="none", path=None, available=False, validated_jpeg=False, width=None, height=None, byte_size=None, error_type="missing")
    label_id: str | None = None
    error_type: str | None = None


class OperatorFeedbackLabeler:
    def __init__(self, *, data_dir: str | Path, logger: StructuredLogger | None = None) -> None:
        self.data_dir = Path(data_dir)
        self.logger = logger
        self.labels_path = feedback_labels_path(self.data_dir)
        self.memory_path = decision_memory_path(self.data_dir)

    def record_correction(
        self,
        *,
        spot_id: str,
        actual_state: str,
        matrix_event_id: str,
        matrix_sender: str,
        matrix_room_id: str,
        corrected_at: datetime | str | None = None,
    ) -> FeedbackRecordResult:
        safe_spot = _safe_identifier(spot_id, "")
        if not safe_spot:
            return FeedbackRecordResult(recorded=False, reply_text="Parking correction not recorded\nInvalid spot id.", spot_id="", actual_state="open", error_type="invalid_spot")
        state = _state(actual_state)
        corrected_text = _timestamp_text(corrected_at or datetime.now(timezone.utc))
        candidate = resolve_latest_alert_candidate(self.memory_path, safe_spot, logger=self.logger)
        if candidate is None:
            reply = f"Parking correction not recorded\nNo recent alert was found for {safe_spot}; use !parking latest or !parking who to inspect current evidence."
            return FeedbackRecordResult(recorded=False, reply_text=reply, spot_id=safe_spot, actual_state=state, error_type="no_recent_alert")

        evidence = validate_feedback_evidence(data_dir=self.data_dir, snapshot_path=candidate.snapshot_path, logger=self.logger)
        label_id = make_label_id(corrected_at=corrected_text, spot_id=safe_spot, matrix_event_id=matrix_event_id)
        label = FeedbackLabel(
            label_id=label_id,
            spot_id=safe_spot,
            reported_state=candidate.reported_state,
            actual_state=state,
            source="matrix_command",
            operator_sender_hash=hash_operator_identifier(matrix_sender),
            corrected_at=corrected_text,
            reported_at=candidate.reported_at,
            alert_event_type=candidate.alert_event_type,
            alert_event_id=candidate.alert_event_id,
            evidence=evidence,
            notes="",
            matrix_event_id=matrix_event_id,
            matrix_room_id_hash=hash_operator_identifier(matrix_room_id),
        )
        recorded = append_feedback_label(self.labels_path, label, logger=self.logger)
        if not recorded:
            reply = "Parking correction not recorded\nFeedback store unavailable (feedback_store_unavailable)."
            return FeedbackRecordResult(recorded=False, reply_text=reply, spot_id=safe_spot, actual_state=state, reported_state=candidate.reported_state, evidence=evidence, error_type="feedback_store_unavailable")

        append_decision_memory_record(
            self.memory_path,
            make_decision_memory_record(
                "feedback",
                observed_at=corrected_text,
                spot_id=safe_spot,
                summary=f"operator correction recorded: reported {candidate.reported_state}; actual {state}",
                details={
                    "label_id": label_id,
                    "reported_state": candidate.reported_state,
                    "actual_state": state,
                    "alert_event_type": candidate.alert_event_type,
                    "alert_event_id": candidate.alert_event_id,
                    "evidence_available": evidence.available,
                    "evidence_error_type": evidence.error_type,
                },
            ),
            logger=self.logger,
        )
        reply = format_correction_reply(safe_spot, candidate.reported_state, state, evidence)
        return FeedbackRecordResult(recorded=True, reply_text=reply, spot_id=safe_spot, actual_state=state, reported_state=candidate.reported_state, evidence=evidence, label_id=label_id)
```

Add these resolver helpers:

```python
def resolve_latest_alert_candidate(path: str | Path, spot_id: str, *, logger: StructuredLogger | None = None) -> AlertEvidenceCandidate | None:
    loaded = load_decision_memory(path, logger=logger)
    if loaded.state != "available":
        return None
    for record in reversed(loaded.records):
        if record.kind != "alert" or record.spot_id != spot_id:
            continue
        details = record.details if isinstance(record.details, Mapping) else {}
        event_type = details.get("event_type")
        reported_state = _reported_state_from_event_type(event_type)
        if reported_state is None:
            continue
        return AlertEvidenceCandidate(
            spot_id=spot_id,
            reported_state=reported_state,
            reported_at=record.observed_at,
            alert_event_type=_optional_text(event_type, 120),
            alert_event_id=_optional_text(details.get("event_id"), 160),
            snapshot_path=_optional_text(details.get("snapshot_path"), 300),
        )
    return None


def _reported_state_from_event_type(value: object) -> SpotState | None:
    text = str(value or "")
    if text == "occupancy-occupied-event":
        return "occupied"
    if text == "occupancy-open-event":
        return "open"
    return None


def validate_feedback_evidence(*, data_dir: str | Path, snapshot_path: str | None, logger: StructuredLogger | None = None) -> FeedbackEvidence:
    if not snapshot_path:
        return FeedbackEvidence(kind="alert_snapshot", path=None, available=False, validated_jpeg=False, width=None, height=None, byte_size=None, error_type="missing")
    safe_relative = _safe_relative_path(snapshot_path)
    if safe_relative == "unavailable":
        return FeedbackEvidence(kind="alert_snapshot", path=None, available=False, validated_jpeg=False, width=None, height=None, byte_size=None, error_type="unsafe_path")
    base = Path(data_dir).resolve()
    candidate = (base / safe_relative).resolve()
    try:
        candidate.relative_to(base)
    except ValueError:
        return FeedbackEvidence(kind="alert_snapshot", path=None, available=False, validated_jpeg=False, width=None, height=None, byte_size=None, error_type="unsafe_path")
    if not candidate.exists():
        return FeedbackEvidence(kind="alert_snapshot", path=safe_relative, available=False, validated_jpeg=False, width=None, height=None, byte_size=None, error_type="missing")
    try:
        byte_size = candidate.stat().st_size
        with Image.open(candidate) as image:
            image.verify()
        with Image.open(candidate) as image:
            width, height = image.size
    except (OSError, UnidentifiedImageError) as exc:
        _log(logger, "warning", "operator-feedback-evidence-invalid", path=safe_relative, error_type=type(exc).__name__)
        return FeedbackEvidence(kind="alert_snapshot", path=safe_relative, available=False, validated_jpeg=False, width=None, height=None, byte_size=None, error_type="invalid_jpeg")
    return FeedbackEvidence(kind="alert_snapshot", path=safe_relative, available=True, validated_jpeg=True, width=width, height=height, byte_size=byte_size, error_type=None)


def format_correction_reply(spot_id: str, reported_state: str, actual_state: str, evidence: FeedbackEvidence) -> str:
    if evidence.available and evidence.validated_jpeg:
        evidence_line = "linked evidence: retained alert snapshot"
    else:
        reason = evidence.error_type or "unavailable"
        evidence_line = f"linked evidence: unavailable; alert snapshot was not retained ({reason})"
    return (
        "Parking correction recorded\n"
        f"- spot: {spot_id}\n"
        f"- reported: {reported_state}\n"
        f"- actual: {actual_state}\n"
        f"- {evidence_line}\n"
        "- next: run !parking lab run replay after labels are reviewed"
    )
```

- [ ] **Step 4: Add snapshot metadata to alert decision memory**

In `parking_spot_monitor/__main__.py`, update `_append_matrix_event_memory(...)` so the `details` mapping includes snapshot path fields already present on runtime event payloads:

```python
        details={
            "event_type": event_name,
            "event_id": event.get("event_id"),
            "outcome": outcome,
            "reason": reason,
            "error_type": error_type,
            "suppressed_reason": event.get("suppressed_reason"),
            "snapshot_path": event.get("snapshot_path") or event.get("occupied_snapshot_path"),
        },
```

- [ ] **Step 5: Support `feedback` decision-memory records**

In `parking_spot_monitor/operator_decision_memory.py`, update the `RecordKind` literal and `_SUPPORTED_KINDS` set to include `"feedback"`:

```python
RecordKind = Literal[
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

_SUPPORTED_KINDS = {
    "accepted_evidence",
    "rejected_evidence",
    "miss",
    "confidence_dip",
    "suppression",
    "alert",
    "command_outcome",
    "lab_outcome",
    "feedback",
}
```

- [ ] **Step 6: Run evidence tests to verify they pass**

Run:

```bash
python -m pytest tests/test_operator_feedback.py tests/test_operator_decision_memory.py -q
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit evidence resolution**

Run:

```bash
git add parking_spot_monitor/operator_feedback.py parking_spot_monitor/operator_decision_memory.py parking_spot_monitor/__main__.py tests/test_operator_feedback.py tests/test_operator_decision_memory.py
git commit -m "feat: record operator spot-state feedback labels"
```

---

### Task 3: Add Matrix `!parking correct` command

**Files:**
- Modify: `parking_spot_monitor/matrix.py`
- Modify: `tests/test_matrix.py`

- [ ] **Step 1: Write failing parser tests**

Extend `test_parse_matrix_commands_are_strict_and_normalize_labels` in `tests/test_matrix.py` with:

```python
    correct = parse_matrix_command("!parking correct left_spot open")
    assert (correct.action, correct.spot_id, correct.actual_state) == ("correct_spot_state", "left_spot", "open")
    correct_occupied = parse_matrix_command("  !parking   correct   right_spot   occupied  ")
    assert (correct_occupied.action, correct_occupied.spot_id, correct_occupied.actual_state) == ("correct_spot_state", "right_spot", "occupied")
```

Add these rejected command cases to the same test:

```python
    for rejected_correct in [
        "!parking correct",
        "!parking correct left_spot",
        "!parking correct left_spot empty",
        "!parking correct left_spot open extra",
        "!parking correct ../state.json open",
        "!parking correct /tmp/left_spot open",
    ]:
        with pytest.raises(MatrixCommandParseError):
            parse_matrix_command(rejected_correct)
```

- [ ] **Step 2: Run parser test to verify it fails**

Run:

```bash
python -m pytest tests/test_matrix.py::test_parse_matrix_commands_are_strict_and_normalize_labels -q
```

Expected: fail because `actual_state` is not a `MatrixCommand` field and `correct` is unknown.

- [ ] **Step 3: Implement parser support**

In `parking_spot_monitor/matrix.py`, add `actual_state` to `MatrixCommand`:

```python
@dataclass(frozen=True)
class MatrixCommand:
    """Parsed operator command with validated, non-secret arguments."""

    action: str
    profile_id: str | None = None
    label: str | None = None
    source_profile_id: str | None = None
    target_profile_id: str | None = None
    subject_id: str | None = None
    spot_id: str | None = None
    actual_state: str | None = None
    lab_kind: str | None = None
    lab_job_id: str | None = None
```

Add a state validator near `_validate_spot_id`:

```python
def _validate_actual_state(value: str) -> str:
    state = value.strip().lower()
    if state not in {"open", "occupied"}:
        raise MatrixCommandParseError("actual state must be open or occupied")
    return state
```

Add this parse branch before `wrong`, `owner`, and `who` branches:

```python
    if parts[1] == "correct":
        if len(parts) != 4:
            raise MatrixCommandParseError("usage: !parking correct <spot_id> <open|occupied>")
        return MatrixCommand(action="correct_spot_state", spot_id=_validate_spot_id(parts[2]), actual_state=_validate_actual_state(parts[3]))
```

- [ ] **Step 4: Run parser test to verify it passes**

Run:

```bash
python -m pytest tests/test_matrix.py::test_parse_matrix_commands_are_strict_and_normalize_labels -q
```

Expected: pass.

- [ ] **Step 5: Write failing command-service tests**

Add this test after `test_command_service_authorizes_applies_and_replies_safely`:

```python
def test_command_service_authorized_correct_records_feedback_label() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    calls: list[dict[str, Any]] = []

    class FeedbackLabeler:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def record_correction(self, **kwargs: Any) -> Any:
            self.calls.append(dict(kwargs))
            return type(
                "Result",
                (),
                {
                    "reply_text": "Parking correction recorded\n- spot: left_spot\n- reported: occupied\n- actual: open\n- linked evidence: retained alert snapshot",
                },
            )()

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$correct", sender="@op:example", room_id=ROOM_ID, body="!parking correct left_spot open"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append(dict(kwargs))
            return "$reply"

    labeler = FeedbackLabeler()
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        feedback_labeler=labeler,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert labeler.calls == [
        {
            "spot_id": "left_spot",
            "actual_state": "open",
            "matrix_event_id": "$correct",
            "matrix_sender": "@op:example",
            "matrix_room_id": ROOM_ID,
        }
    ]
    expected_reply = "Parking correction recorded\n- spot: left_spot\n- reported: occupied\n- actual: open\n- linked evidence: retained alert snapshot"
    assert calls == [{"room_id": ROOM_ID, "txn_id": "command:$correct", "body": expected_reply}]


def test_command_service_correct_requires_authorization_and_configured_labeler() -> None:
    from parking_spot_monitor.matrix import MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    replies: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(
                next_batch="s3",
                events=(
                    MatrixTextEvent(event_id="$deny", sender="@intruder:example", room_id=ROOM_ID, body="!parking correct left_spot open"),
                    MatrixTextEvent(event_id="$missing", sender="@op:example", room_id=ROOM_ID, body="!parking correct left_spot open"),
                ),
            )

        def send_text(self, **kwargs: Any) -> str:
            replies.append(kwargs["body"])
            return "$reply"

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
    )

    result = service.poll_once()

    assert result.processed_count == 0
    assert result.error_count == 2
    assert replies == ["Command rejected: sender is not authorized.", "Command failed: RuntimeError"]
```

- [ ] **Step 6: Run command-service tests to verify they fail**

Run:

```bash
python -m pytest tests/test_matrix.py::test_command_service_authorized_correct_records_feedback_label tests/test_matrix.py::test_command_service_correct_requires_authorization_and_configured_labeler -q
```

Expected: fail because `MatrixCommandService.__init__` does not accept `feedback_labeler` and `_apply_command` does not handle `correct_spot_state`.

- [ ] **Step 7: Implement command-service support**

In `MatrixCommandService.__init__`, add a `feedback_labeler` parameter and assignment:

```python
        feedback_labeler: Any | None = None,
```

```python
        self.feedback_labeler = feedback_labeler
```

In `_apply_command`, add this branch before correction idempotency for vehicle-history commands:

```python
        if command.action == "correct_spot_state":
            if self.feedback_labeler is None:
                raise RuntimeError("operator feedback labeler is not configured")
            assert command.spot_id is not None and command.actual_state is not None
            result = self.feedback_labeler.record_correction(
                spot_id=command.spot_id,
                actual_state=command.actual_state,
                matrix_event_id=event.event_id,
                matrix_sender=event.sender,
                matrix_room_id=event.room_id,
            )
            return str(getattr(result, "reply_text", "Parking correction recorded"))
```

Update `_format_command_help_reply` by inserting:

```python
        f"{command_prefix} correct <spot_id> <open|occupied> — record that the latest report for a spot was wrong\n"
```

- [ ] **Step 8: Run Matrix command tests**

Run:

```bash
python -m pytest tests/test_matrix.py::test_parse_matrix_commands_are_strict_and_normalize_labels tests/test_matrix.py::test_command_service_authorized_correct_records_feedback_label tests/test_matrix.py::test_command_service_correct_requires_authorization_and_configured_labeler tests/test_matrix.py::test_command_service_authorizes_applies_and_replies_safely -q
```

Expected: all selected tests pass.

- [ ] **Step 9: Commit Matrix correction command**

Run:

```bash
git add parking_spot_monitor/matrix.py tests/test_matrix.py
git commit -m "feat: add Matrix spot correction command"
```

---

### Task 4: Add fresh snapshot image support to `!parking who`

**Files:**
- Modify: `parking_spot_monitor/operator_cockpit.py`
- Modify: `parking_spot_monitor/matrix.py`
- Modify: `tests/test_matrix_operator_cockpit.py`
- Modify: `tests/test_matrix.py`

- [ ] **Step 1: Write failing cockpit snapshot helper tests**

Append these tests to `tests/test_matrix_operator_cockpit.py`:

```python
def test_build_who_snapshot_response_captures_once_and_attaches_validated_image(tmp_path: Path) -> None:
    from parking_spot_monitor.capture import DecodeMode, FrameCaptureResult
    from parking_spot_monitor.operator_cockpit import build_who_snapshot_response

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = _write_jpeg(latest_path, size=(13, 9))
    calls: list[tuple[object, Path]] = []

    def capture_func(settings: object, data_dir: Path, **kwargs: object) -> FrameCaptureResult:
        calls.append((settings, Path(data_dir)))
        return FrameCaptureResult(
            timestamp="2026-05-16T17:42:39Z",
            latest_path=latest_path,
            selected_mode=DecodeMode.SOFTWARE,
            duration_seconds=0.25,
            byte_size=raw_bytes,
        )

    response = build_who_snapshot_response(
        settings=object(),
        data_dir=tmp_path,
        base_text="Parking monitor who\n- left_spot: occupied — unknown vehicle",
        capture_func=capture_func,
        now=datetime(2026, 5, 16, 17, 42, 40, tzinfo=timezone.utc),
    )

    assert len(calls) == 1
    assert response.image_path == latest_path
    assert response.image_info == {"mimetype": "image/jpeg", "size": raw_bytes, "w": 13, "h": 9}
    assert response.text.startswith("Parking monitor who\nSnapshot: fresh capture")
    assert "left_spot: occupied" in response.text


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
```

If `tests/test_matrix_operator_cockpit.py` does not already import `datetime`, `timezone`, or `Path`, add imports at the top:

```python
from datetime import datetime, timezone
from pathlib import Path
```

- [ ] **Step 2: Run cockpit helper tests to verify they fail**

Run:

```bash
python -m pytest tests/test_matrix_operator_cockpit.py::test_build_who_snapshot_response_captures_once_and_attaches_validated_image tests/test_matrix_operator_cockpit.py::test_build_who_snapshot_response_falls_back_to_text_on_capture_failure -q
```

Expected: fail because `build_who_snapshot_response` is not defined.

- [ ] **Step 3: Implement `build_who_snapshot_response`**

In `parking_spot_monitor/operator_cockpit.py`, import capture types:

```python
from parking_spot_monitor.capture import CaptureError, capture_latest
from parking_spot_monitor.matrix import JPEG_MIMETYPE, MatrixCommandResponse
```

The direct import is the planned implementation path because `parking_spot_monitor.matrix` does not import `parking_spot_monitor.operator_cockpit`. Keep the import at module scope so the helper can return the existing `MatrixCommandResponse` type used by command image replies.

Add this helper below `build_latest_snapshot_response`:

```python
def build_who_snapshot_response(
    *,
    settings: RuntimeSettings,
    data_dir: str | Path,
    base_text: str,
    capture_func: Any = capture_latest,
    now: datetime | None = None,
    logger: StructuredLogger | None = None,
) -> MatrixCommandResponse:
    """Return a Matrix who reply enriched with one bounded fresh capture when available."""

    observed_now = _utc_now(now)
    try:
        capture = capture_func(settings, Path(data_dir), logger=logger)
        snapshot = _validate_latest_snapshot(capture.latest_path, now=observed_now, logger=logger)
    except CaptureError as exc:
        reason = redact_diagnostic_text(exc.reason or type(exc).__name__)
        _log_snapshot_failure(logger, "operator-who-capture-failed", reason=reason, error_type=type(exc).__name__)
        return MatrixCommandResponse(text=_prepend_who_snapshot_line(base_text, f"Snapshot: fresh capture unavailable ({reason}); no live state was changed."), image_path=None, image_info=None)
    except Exception as exc:
        reason = redact_diagnostic_text(type(exc).__name__)
        _log_snapshot_failure(logger, "operator-who-capture-failed", reason=reason, error_type=type(exc).__name__)
        return MatrixCommandResponse(text=_prepend_who_snapshot_line(base_text, f"Snapshot: fresh capture unavailable ({reason}); no live state was changed."), image_path=None, image_info=None)

    if snapshot.state != "available" or snapshot.path is None or snapshot.info is None:
        reason = snapshot.error_type or "unavailable"
        return MatrixCommandResponse(text=_prepend_who_snapshot_line(base_text, f"Snapshot: fresh capture unavailable ({reason}); no live state was changed."), image_path=None, image_info=None)

    info = dict(snapshot.info)
    line = f"Snapshot: fresh capture at {_display_time(capture.timestamp)}"
    return MatrixCommandResponse(text=_prepend_who_snapshot_line(base_text, line), image_path=snapshot.path, image_info=info)
```

Add local helpers:

```python
def _prepend_who_snapshot_line(base_text: str, snapshot_line: str) -> str:
    lines = base_text.splitlines()
    if not lines:
        return _bounded_reply(["Parking monitor who", snapshot_line])
    return _bounded_reply([lines[0], snapshot_line, "", *lines[1:]])


def _display_time(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %-I:%M:%S %p %Z")
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %-I:%M:%S %p %Z")
        except ValueError:
            return redact_diagnostic_text(value)[:80]
    return "unknown"


def _log_snapshot_failure(logger: StructuredLogger | None, event_name: str, **fields: Any) -> None:
    if logger is not None:
        logger.warning(event_name, **redact_diagnostic_value(fields))
```

If Linux `strftime("%-I")` support is a concern in tests, reuse the project’s existing display-time formatter from `parking_spot_monitor.matrix` instead of defining `_display_time`.

- [ ] **Step 4: Run cockpit helper tests to verify they pass**

Run:

```bash
python -m pytest tests/test_matrix_operator_cockpit.py::test_build_who_snapshot_response_captures_once_and_attaches_validated_image tests/test_matrix_operator_cockpit.py::test_build_who_snapshot_response_falls_back_to_text_on_capture_failure -q
```

Expected: pass.

- [ ] **Step 5: Write failing Matrix service who-image test**

Add this test near existing latest image command tests in `tests/test_matrix.py`:

```python
def test_command_service_who_can_send_active_assignments_with_fresh_snapshot(tmp_path: Path) -> None:
    from parking_spot_monitor.matrix import MatrixCommandResponse, MatrixCommandService, MatrixSyncResult, MatrixTextEvent

    latest_path = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(latest_path, size=(13, 9))
    calls: list[dict[str, Any]] = []
    provider_inputs: list[str] = []

    class Client:
        def sync(self, **kwargs: Any) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s3", events=(MatrixTextEvent(event_id="$who", sender="@op:example", room_id=ROOM_ID, body="!parking who"),))

        def send_text(self, **kwargs: Any) -> str:
            calls.append({"kind": "text", **dict(kwargs)})
            return "$text"

        def upload_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "upload", **dict(kwargs)})
            return "mxc://example.org/who"

        def send_image(self, **kwargs: Any) -> str:
            calls.append({"kind": "image", **dict(kwargs)})
            return "$image"

    def who_snapshot_provider(base_text: str) -> MatrixCommandResponse:
        provider_inputs.append(base_text)
        return MatrixCommandResponse(
            text="Parking monitor who\nSnapshot: fresh capture at 2026-05-16 10:42:39 AM PDT\n\n" + "\n".join(base_text.splitlines()[1:]),
            image_path=latest_path,
            image_info={"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 13, "h": 9},
        )

    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=FakeCommandArchive(cursor={"next_batch": "s2"}),
        room_id=ROOM_ID,
        authorized_senders=["@op:example"],
        who_snapshot_provider=who_snapshot_provider,
    )

    result = service.poll_once()

    assert result.processed_count == 1
    assert result.error_count == 0
    assert provider_inputs and provider_inputs[0].startswith("Parking monitor who")
    assert [call["kind"] for call in calls] == ["text", "upload", "image"]
    assert calls[0]["txn_id"] == "command:$who:text"
    assert "Snapshot: fresh capture" in calls[0]["body"]
    assert calls[1]["filename"] == "latest.jpg"
    assert calls[1]["data"] == raw_bytes
    assert calls[2]["txn_id"] == "command:$who:image"
    assert calls[2]["body"] == "Raw full-frame latest.jpg evidence"
```

- [ ] **Step 6: Run who-image service test to verify it fails**

Run:

```bash
python -m pytest tests/test_matrix.py::test_command_service_who_can_send_active_assignments_with_fresh_snapshot -q
```

Expected: fail because `MatrixCommandService.__init__` does not accept `who_snapshot_provider`.

- [ ] **Step 7: Implement who snapshot provider support**

In `MatrixCommandService.__init__`, add:

```python
        who_snapshot_provider: Callable[[str], str | MatrixCommandResponse] | None = None,
```

Assign it:

```python
        self.who_snapshot_provider = who_snapshot_provider
```

Update the `active_spot_assignments` branch:

```python
        if command.action == "active_spot_assignments":
            base_reply = _format_active_spot_assignments_reply(self.archive.active_spot_assignments())
            if self.who_snapshot_provider is not None:
                return self.who_snapshot_provider(base_reply)
            return base_reply
```

Update `_format_command_help_reply` line for who:

```python
        f"{command_prefix} who — list active parking sessions by spot and attach a fresh current snapshot when configured\n"
```

- [ ] **Step 8: Run Matrix who tests**

Run:

```bash
python -m pytest tests/test_matrix.py::test_command_service_who_can_send_active_assignments_with_fresh_snapshot tests/test_matrix.py::test_command_service_authorizes_applies_and_replies_safely -q
```

Expected: both tests pass; the existing `!parking who` behavior remains text-only when no provider is configured.

- [ ] **Step 9: Commit who snapshot support**

Run:

```bash
git add parking_spot_monitor/operator_cockpit.py parking_spot_monitor/matrix.py tests/test_matrix_operator_cockpit.py tests/test_matrix.py
git commit -m "feat: attach fresh snapshot to parking who command"
```

---

### Task 5: Wire runtime defaults

**Files:**
- Modify: `parking_spot_monitor/__main__.py`
- Modify: `tests/test_startup.py`

- [ ] **Step 1: Write failing runtime wiring test**

Extend the existing startup test that inspects `_default_matrix_command_service_factory` or add this test to `tests/test_startup.py` near the current cockpit context assertion:

```python
def test_default_matrix_command_service_wires_feedback_and_who_snapshot(tmp_path: Path) -> None:
    from parking_spot_monitor.__main__ import _default_matrix_command_service_factory
    from parking_spot_monitor.logging import StructuredLogger
    from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

    settings = _runtime_settings_with_matrix_commands(tmp_path, authorized_senders=["@op:example"])
    archive = VehicleHistoryArchive(tmp_path / "vehicle-history")
    service = _default_matrix_command_service_factory(settings, tmp_path, StructuredLogger(), archive)

    assert service is not None
    assert service.feedback_labeler is not None
    assert service.who_snapshot_provider is not None
    assert service.cockpit_context is not None
```

Use the existing `load_settings("config.yaml.example", environ=fake_environ())` pattern from `test_default_matrix_command_service_wires_detection_lab_to_effective_paths_and_memory`, then apply `settings.matrix.model_copy(update={"command_authorized_senders": ["@op:example"]})` before calling `_default_matrix_command_service_factory`.

- [ ] **Step 2: Run runtime wiring test to verify it fails**

Run:

```bash
python -m pytest tests/test_startup.py::test_default_matrix_command_service_wires_feedback_and_who_snapshot -q
```

Expected: fail because the default factory does not pass `feedback_labeler` or `who_snapshot_provider`.

- [ ] **Step 3: Implement runtime wiring**

In `parking_spot_monitor/__main__.py`, import:

```python
from parking_spot_monitor.operator_cockpit import build_who_snapshot_response
from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler
```

If `build_who_snapshot_response` is already imported through an existing grouped import, add it to that import group.

In `_default_matrix_command_service_factory`, create the labeler and provider after `paths` is resolved:

```python
    feedback_labeler = OperatorFeedbackLabeler(data_dir=paths.data_dir, logger=logger)

    def who_snapshot_provider(base_text: str) -> MatrixCommandResponse:
        return build_who_snapshot_response(settings=settings, data_dir=paths.data_dir, base_text=base_text, logger=logger)
```

Pass both into `MatrixCommandService`:

```python
        feedback_labeler=feedback_labeler,
        who_snapshot_provider=who_snapshot_provider,
```

If `MatrixCommandResponse` is only needed for annotation and causes an import cycle, remove the return type annotation from the nested function.

- [ ] **Step 4: Run runtime wiring test to verify it passes**

Run:

```bash
python -m pytest tests/test_startup.py::test_default_matrix_command_service_wires_feedback_and_who_snapshot -q
```

Expected: pass.

- [ ] **Step 5: Run related Matrix/startup tests**

Run:

```bash
python -m pytest tests/test_matrix.py tests/test_matrix_operator_cockpit.py tests/test_startup.py -q
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit runtime wiring**

Run:

```bash
git add parking_spot_monitor/__main__.py tests/test_startup.py
git commit -m "feat: wire operator feedback runtime commands"
```

---

### Task 6: Document operator commands

**Files:**
- Modify: `README.md`
- Modify: `tests/test_operator_docs.py`

- [ ] **Step 1: Write failing docs contract tests**

Add or extend a docs test in `tests/test_operator_docs.py`:

```python
def test_operator_docs_include_feedback_correction_and_who_snapshot_contract() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "!parking correct <spot_id> <open|occupied>" in readme
    assert "operator-feedback-labels.json" in readme
    assert "reported" in readme and "actual" in readme
    assert "!parking who" in readme
    assert "fresh" in readme and "snapshot" in readme
    assert "does not mutate live occupancy state" in readme
    assert "does not automatically change detector thresholds" in readme
```

If `Path` is not imported in that file, add:

```python
from pathlib import Path
```

- [ ] **Step 2: Run docs test to verify it fails**

Run:

```bash
python -m pytest tests/test_operator_docs.py::test_operator_docs_include_feedback_correction_and_who_snapshot_contract -q
```

Expected: fail because README does not document the new command yet.

- [ ] **Step 3: Update README operator command docs**

In `README.md`, update the Matrix cockpit command section currently describing `!parking who`, `!parking owner`, `!parking wrong`, and detection lab commands. Add this text near the command list:

```markdown
Use `!parking correct <spot_id> <open|occupied>` when the latest Matrix report for a spot is wrong. For example, if the monitor says `Parking spot occupied: left_spot` but the alert image shows the left spot is empty, send `!parking correct left_spot open`. The command is available only to authorized Matrix command senders, records a bounded local label in `data/operator-feedback-labels.json`, links that label to the most recent retained alert snapshot for the spot when available, and adds a compact feedback outcome to decision memory. The label stores metadata such as reported state, actual state, correction time, alert event type, and safe JPEG metadata; it does not store image bytes, camera URLs, Matrix tokens, raw Matrix response bodies, or tracebacks.

Feedback labels are training and replay evidence only. `!parking correct` does not mutate live occupancy state, vehicle-history identity state, quiet-window markers, detector thresholds, polygons, model configuration, or Matrix alert delivery. It also does not automatically change detector thresholds or train a model; reviewed labels must flow through the detection-lab replay/tuning evidence gate before any production tuning change.

Use `!parking who` to list active parking sessions by spot and request a fresh current snapshot. The command attempts one bounded camera capture, sends the existing active-session summary, and attaches the validated raw full-frame JPEG when capture succeeds. If capture fails or the JPEG is invalid, the text reply still includes the active-session summary plus a safe `Snapshot: fresh capture unavailable (...)` diagnostic; the command does not run detector/model inference or mutate live occupancy state.
```

- [ ] **Step 4: Run docs test to verify it passes**

Run:

```bash
python -m pytest tests/test_operator_docs.py::test_operator_docs_include_feedback_correction_and_who_snapshot_contract -q
```

Expected: pass.

- [ ] **Step 5: Commit docs**

Run:

```bash
git add README.md tests/test_operator_docs.py
git commit -m "docs: document parking correction feedback commands"
```

---

### Task 7: Final verification and cleanup

**Files:**
- No planned source edits unless verification reveals a failure.

- [ ] **Step 1: Run focused feature tests**

Run:

```bash
python -m pytest tests/test_operator_feedback.py tests/test_operator_decision_memory.py tests/test_matrix.py tests/test_matrix_operator_cockpit.py tests/test_startup.py tests/test_operator_docs.py -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run full suite**

Run:

```bash
python -m pytest
```

Expected: all tests pass.

- [ ] **Step 3: Inspect final diff**

Run:

```bash
git status --short
git log --oneline --decorate -8
git diff --stat HEAD~6..HEAD
```

Expected: working tree clean after the task commits; recent commits correspond to this plan.

- [ ] **Step 4: If any verification fails, fix before completion**

Use the failing test output to make the smallest root-cause fix, then rerun the exact failing command. Do not mark the implementation complete until the focused command and full `python -m pytest` pass.

- [ ] **Step 5: Final implementation commit if needed**

If verification fixes required an additional commit, run:

```bash
git add parking_spot_monitor tests README.md
git commit -m "fix: stabilize operator feedback command verification"
```

If no files changed during final verification, do not create an empty commit.

## Self-review notes

- Spec coverage: this plan covers `!parking correct <spot_id> <open|occupied>`, structured local labels, alert snapshot linkage, no live-state mutation, fresh `!parking who` capture, safe diagnostics, docs, and tests.
- Scope boundary: automatic training, automatic threshold changes, per-spot runtime thresholds, and reviewed detection-lab import are excluded from this implementation plan and remain outside the approved first implementation.
- Type consistency: the plan uses `MatrixCommand.actual_state`, `MatrixCommandService.feedback_labeler`, `MatrixCommandService.who_snapshot_provider`, `OperatorFeedbackLabeler.record_correction(...)`, and `MatrixCommandResponse` consistently across tasks.
