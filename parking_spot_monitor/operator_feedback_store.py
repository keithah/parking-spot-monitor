from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_value
from parking_spot_monitor.operator_feedback_models import (
    SCHEMA_VERSION,
    MAX_FEEDBACK_FILE_BYTES,
    MAX_FEEDBACK_LABELS,
    FeedbackAppendResult,
    FeedbackLabel,
    FeedbackLabelLoad,
    FeedbackLabelSchemaError,
    feedback_label_from_any,
    optional_feedback_text,
    positive_feedback_limit,
)


def append_feedback_label(
    path: str | Path,
    label: FeedbackLabel | Mapping[str, Any],
    *,
    max_labels: int = MAX_FEEDBACK_LABELS,
    max_file_bytes: int = MAX_FEEDBACK_FILE_BYTES,
    logger: StructuredLogger | None = None,
) -> FeedbackAppendResult:
    """Append one sanitized feedback label with atomic write and bounded retention."""

    labels_path = Path(path)
    try:
        new_label = feedback_label_from_any(label)
        loaded = load_feedback_labels(labels_path, max_labels=max_labels, max_file_bytes=max_file_bytes, logger=logger)
        retained = list(loaded.labels)
        if new_label.matrix_event_id and any(existing.matrix_event_id == new_label.matrix_event_id for existing in retained):
            _log(logger, "debug", "operator-feedback-label-duplicate-skipped", path=labels_path, matrix_event_id=new_label.matrix_event_id)
            return FeedbackAppendResult(status="duplicate", label_id=new_label.label_id)
        retained.append(new_label)
        retained = retained[-positive_feedback_limit(max_labels, MAX_FEEDBACK_LABELS) :]
        _write_feedback_labels(labels_path, retained)
    except Exception as exc:
        _log(logger, "warning", "operator-feedback-label-append-failed", path=labels_path, error_type=type(exc).__name__, error=str(exc))
        return FeedbackAppendResult(status="failed")

    _log(logger, "debug", "operator-feedback-label-appended", path=labels_path, label_count=len(retained), label_id=new_label.label_id)
    return FeedbackAppendResult(status="appended", label_id=new_label.label_id)


def load_feedback_labels(
    path: str | Path,
    *,
    max_labels: int = MAX_FEEDBACK_LABELS,
    max_file_bytes: int = MAX_FEEDBACK_FILE_BYTES,
    logger: StructuredLogger | None = None,
) -> FeedbackLabelLoad:
    """Load a bounded tail of feedback labels, quarantining corrupt or oversized files."""

    labels_path = Path(path)
    if not labels_path.exists():
        _log(logger, "debug", "operator-feedback-labels-load-missing", path=labels_path)
        return FeedbackLabelLoad(state="missing")

    try:
        size = labels_path.stat().st_size
    except OSError as exc:
        _log(logger, "warning", "operator-feedback-labels-load-failed", path=labels_path, phase="stat", error_type=type(exc).__name__, error=str(exc))
        return FeedbackLabelLoad(state="unavailable", error_type=type(exc).__name__)

    if size > max_file_bytes:
        quarantined = _quarantine_feedback_file(labels_path)
        _log(logger, "warning", "operator-feedback-labels-quarantined", path=labels_path, quarantine_path=quarantined, phase="size", error_type="oversized")
        return FeedbackLabelLoad(state="unavailable", error_type="oversized", quarantined_path=quarantined)

    try:
        with labels_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        labels = _feedback_labels_from_payload(payload)
    except (OSError, json.JSONDecodeError, FeedbackLabelSchemaError) as exc:
        quarantined = _quarantine_feedback_file(labels_path)
        _log(logger, "warning", "operator-feedback-labels-quarantined", path=labels_path, quarantine_path=quarantined, phase="load", error_type=type(exc).__name__, error=str(exc))
        return FeedbackLabelLoad(state="unavailable", error_type=type(exc).__name__, quarantined_path=quarantined)

    bounded = tuple(labels[-positive_feedback_limit(max_labels, MAX_FEEDBACK_LABELS) :])
    _log(logger, "debug", "operator-feedback-labels-loaded", path=labels_path, label_count=len(bounded), state="available")
    return FeedbackLabelLoad(state="available", labels=bounded)


def find_feedback_label_by_matrix_event_id(
    path: str | Path,
    matrix_event_id: str | None,
    *,
    logger: StructuredLogger | None = None,
) -> FeedbackLabel | None:
    """Return the stored feedback label for an already-processed Matrix event id."""

    safe_event_id = optional_feedback_text(matrix_event_id, limit=180)
    if not safe_event_id:
        return None
    loaded = load_feedback_labels(path, logger=logger)
    if loaded.state != "available":
        return None
    for label in reversed(loaded.labels):
        if label.matrix_event_id == safe_event_id:
            return label
    return None


def _write_feedback_labels(path: Path, labels: Sequence[FeedbackLabel]) -> None:
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


def _feedback_labels_from_payload(payload: Any) -> list[FeedbackLabel]:
    if not isinstance(payload, Mapping):
        raise FeedbackLabelSchemaError("feedback label payload must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise FeedbackLabelSchemaError("unsupported feedback label schema_version")
    raw_labels = payload.get("labels")
    if not isinstance(raw_labels, list):
        raise FeedbackLabelSchemaError("feedback label labels must be a list")
    if len(raw_labels) > MAX_FEEDBACK_LABELS * 10:
        raise FeedbackLabelSchemaError("feedback label count exceeds validation bound")
    return [feedback_label_from_any(item) for item in raw_labels]


def _quarantine_feedback_file(path: Path) -> Path | None:
    quarantine_path = path.with_name(f"{path.name}.quarantine")
    try:
        os.replace(path, quarantine_path)
        return quarantine_path
    except OSError:
        return None


def _log(logger: StructuredLogger | None, level: str, event: str, **fields: Any) -> None:
    if logger is None:
        return
    getattr(logger, level)(event, **redact_diagnostic_value(fields))
