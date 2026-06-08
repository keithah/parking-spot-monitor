from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from parking_spot_monitor.logging import redact_diagnostic_text

SCHEMA_VERSION = 1
FEEDBACK_LABELS_FILENAME = "operator-feedback-labels.json"
MAX_FEEDBACK_FILE_BYTES = 512_000
MAX_FEEDBACK_LABELS = 500
MAX_TEXT_FIELD_CHARS = 500
VALID_FEEDBACK_STATES = frozenset({"open", "occupied"})

LoadState = Literal["available", "missing", "unavailable"]
SpotState = Literal["open", "occupied"]
FeedbackAppendStatus = Literal["appended", "duplicate", "failed"]
FeedbackLabelType = Literal["correction", "learn"]

MAX_METADATA_ITEMS = 16
MAX_REPLAY_CONTEXT_LINES = 12
MAX_REPLAY_LINE_CHARS = 240
MAX_DEGRADATION_REASONS = 8
_SENSITIVE_TOKEN_PATTERN = re.compile(r"(?i)\b(?:syt|mxt|ghp|glpat|sk|xox[baprs])-?[a-z0-9_./+=-]{8,}\b")
_RAW_IMAGE_PREFIX_PATTERN = re.compile(r"\xff\xd8[\s\S]*")


class FeedbackLabelSchemaError(ValueError):
    """Raised when persisted operator feedback labels use an unsafe schema."""


@dataclass(frozen=True)
class FeedbackEvidence:
    """Safe metadata about evidence linked to an operator feedback label."""

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
            "kind": clip_feedback_text(self.kind, 80),
            "path": optional_feedback_path_text(self.path),
            "available": bool(self.available),
            "validated_jpeg": bool(self.validated_jpeg),
            "width": _optional_non_negative_int(self.width),
            "height": _optional_non_negative_int(self.height),
            "byte_size": _optional_non_negative_int(self.byte_size),
        }
        if self.error_type is not None:
            payload["error_type"] = clip_feedback_text(self.error_type, 80)
        return payload


@dataclass(frozen=True)
class FeedbackLabel:
    """Schema-stable operator label for correction and learn-command replay review."""

    label_id: str
    spot_id: str
    reported_state: str
    actual_state: str
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
    label_type: FeedbackLabelType = "correction"
    target_state: str | None = None
    learned_at: str | None = None
    replay_context: tuple[str, ...] = ()
    degradation_reasons: tuple[str, ...] = ()
    source_metadata: Mapping[str, Any] | None = None
    feedback_category: str | None = None
    feedback_category_details: Mapping[str, Any] | None = None

    def to_json_dict(self) -> dict[str, Any]:
        label_type = _feedback_label_type(self.label_type)
        payload: dict[str, Any] = {
            "label_id": required_feedback_text(self.label_id, "label_id", limit=160),
            "spot_id": required_feedback_text(self.spot_id, "spot_id", limit=80),
            "reported_state": feedback_state(self.reported_state, "reported_state"),
            "actual_state": feedback_state(self.actual_state, "actual_state"),
            "source": required_feedback_text(self.source, "source", limit=80),
            "operator_sender_hash": required_feedback_text(self.operator_sender_hash, "operator_sender_hash", limit=120),
            "corrected_at": required_feedback_text(self.corrected_at, "corrected_at", limit=80),
            "reported_at": optional_feedback_text(self.reported_at, limit=80),
            "alert_event_type": optional_feedback_text(self.alert_event_type, limit=120),
            "alert_event_id": optional_feedback_text(self.alert_event_id, limit=180),
            "evidence": self.evidence.to_json_dict(),
            "notes": clip_feedback_text(self.notes, MAX_TEXT_FIELD_CHARS),
            "label_type": label_type,
        }
        if self.matrix_event_id is not None:
            payload["matrix_event_id"] = optional_feedback_text(self.matrix_event_id, limit=180)
        if self.matrix_room_id_hash is not None:
            payload["matrix_room_id_hash"] = optional_feedback_text(self.matrix_room_id_hash, limit=120)
        if self.feedback_category is not None:
            payload["feedback_category"] = _safe_feedback_category(self.feedback_category)
        if self.feedback_category_details is not None:
            payload["feedback_category_details"] = _safe_metadata(self.feedback_category_details)
        if label_type == "learn":
            payload["target_state"] = feedback_state(self.target_state, "target_state")
            payload["learned_at"] = required_feedback_text(self.learned_at or self.corrected_at, "learned_at", limit=80)
            payload["replay_context"] = safe_feedback_text_list(self.replay_context, max_items=MAX_REPLAY_CONTEXT_LINES, item_limit=MAX_REPLAY_LINE_CHARS)
            payload["degradation_reasons"] = safe_feedback_text_list(self.degradation_reasons, max_items=MAX_DEGRADATION_REASONS, item_limit=120)
            payload["source_metadata"] = _safe_metadata(self.source_metadata)
        return payload


@dataclass(frozen=True)
class FeedbackLabelLoad:
    """Bounded feedback-label load result with explicit unavailable diagnostics."""

    state: LoadState
    labels: tuple[FeedbackLabel, ...] = ()
    error_type: str | None = None
    quarantined_path: Path | None = None


@dataclass(frozen=True)
class FeedbackAppendResult:
    """Outcome from appending an operator feedback label."""

    status: FeedbackAppendStatus
    label_id: str | None = None

    def __bool__(self) -> bool:
        return self.status != "failed"


@dataclass(frozen=True)
class AlertEvidenceCandidate:
    """Latest alert memory usable as correction evidence."""

    spot_id: str
    reported_state: SpotState
    reported_at: str | None
    alert_event_type: str | None
    alert_event_id: str | None
    snapshot_path: str | None


@dataclass(frozen=True)
class FeedbackRecordResult:
    """Result returned to Matrix command handling after a correction attempt."""

    recorded: bool
    reply_text: str
    spot_id: str
    actual_state: SpotState
    reported_state: SpotState | None = None
    evidence: FeedbackEvidence = FeedbackEvidence(
        kind="none",
        path=None,
        available=False,
        validated_jpeg=False,
        width=None,
        height=None,
        byte_size=None,
        error_type="missing",
    )
    label_id: str | None = None
    error_type: str | None = None


@dataclass(frozen=True)
class LearnLabelRecordResult:
    """Result returned after recording a learn-command replay label."""

    recorded: bool
    reply_text: str
    spot_id: str
    target_state: SpotState
    requested_at: str | None = None
    evidence: FeedbackEvidence = FeedbackEvidence(
        kind="timeline_frame",
        path=None,
        available=False,
        validated_jpeg=False,
        width=None,
        height=None,
        byte_size=None,
        error_type="missing",
    )
    replay_context: tuple[str, ...] = ()
    degradation_reasons: tuple[str, ...] = ()
    label_id: str | None = None
    error_type: str | None = None
    duplicate: bool = False


def feedback_label_from_any(value: FeedbackLabel | Mapping[str, Any]) -> FeedbackLabel:
    if isinstance(value, FeedbackLabel):
        return FeedbackLabel(**value.to_json_dict() | {"evidence": FeedbackEvidence(**value.evidence.to_json_dict())})
    if not isinstance(value, Mapping):
        raise FeedbackLabelSchemaError("feedback label must be an object")
    evidence_value = value.get("evidence")
    if not isinstance(evidence_value, Mapping):
        raise FeedbackLabelSchemaError("feedback label evidence must be an object")
    evidence = FeedbackEvidence(
        kind=required_feedback_text(evidence_value.get("kind"), "evidence.kind", limit=80),
        path=optional_feedback_path_text(evidence_value.get("path")),
        available=bool(evidence_value.get("available", False)),
        validated_jpeg=bool(evidence_value.get("validated_jpeg", False)),
        width=_optional_non_negative_int(evidence_value.get("width")),
        height=_optional_non_negative_int(evidence_value.get("height")),
        byte_size=_optional_non_negative_int(evidence_value.get("byte_size")),
        error_type=optional_feedback_text(evidence_value.get("error_type"), limit=80),
    )
    label_type = _feedback_label_type(value.get("label_type", "correction"))
    return FeedbackLabel(
        label_id=required_feedback_text(value.get("label_id"), "label_id", limit=160),
        spot_id=required_feedback_text(value.get("spot_id"), "spot_id", limit=80),
        reported_state=feedback_state(value.get("reported_state"), "reported_state"),
        actual_state=feedback_state(value.get("actual_state"), "actual_state"),
        source=required_feedback_text(value.get("source"), "source", limit=80),
        operator_sender_hash=required_feedback_text(value.get("operator_sender_hash"), "operator_sender_hash", limit=120),
        corrected_at=required_feedback_text(value.get("corrected_at"), "corrected_at", limit=80),
        reported_at=optional_feedback_text(value.get("reported_at"), limit=80),
        alert_event_type=optional_feedback_text(value.get("alert_event_type"), limit=120),
        alert_event_id=optional_feedback_text(value.get("alert_event_id"), limit=180),
        evidence=evidence,
        notes=optional_feedback_text(value.get("notes"), limit=MAX_TEXT_FIELD_CHARS) or "",
        matrix_event_id=optional_feedback_text(value.get("matrix_event_id"), limit=180),
        matrix_room_id_hash=optional_feedback_text(value.get("matrix_room_id_hash"), limit=120),
        label_type=label_type,
        target_state=feedback_state(value.get("target_state"), "target_state") if label_type == "learn" else optional_feedback_text(value.get("target_state"), limit=40),
        learned_at=required_feedback_text(value.get("learned_at") or value.get("corrected_at"), "learned_at", limit=80) if label_type == "learn" else optional_feedback_text(value.get("learned_at"), limit=80),
        replay_context=tuple(safe_feedback_text_list(value.get("replay_context", ()), max_items=MAX_REPLAY_CONTEXT_LINES, item_limit=MAX_REPLAY_LINE_CHARS)) if label_type == "learn" else (),
        degradation_reasons=tuple(safe_feedback_text_list(value.get("degradation_reasons", ()), max_items=MAX_DEGRADATION_REASONS, item_limit=120)) if label_type == "learn" else (),
        source_metadata=_safe_metadata(value.get("source_metadata")) if label_type == "learn" else None,
        feedback_category=_safe_feedback_category(value.get("feedback_category")) if value.get("feedback_category") is not None else None,
        feedback_category_details=_safe_metadata(value.get("feedback_category_details")) if value.get("feedback_category_details") is not None else None,
    )


def _safe_feedback_category(value: object) -> str:
    category = required_feedback_text(value, "feedback_category", limit=80)
    if category not in {"false_alert", "missed_alert"}:
        raise FeedbackLabelSchemaError("feedback label feedback_category must be false_alert or missed_alert")
    return category


def _feedback_label_type(value: object) -> FeedbackLabelType:
    label_type = required_feedback_text(value, "label_type", limit=40)
    if label_type == "correction":
        return "correction"
    if label_type == "learn":
        return "learn"
    raise FeedbackLabelSchemaError("feedback label label_type must be correction or learn")


def feedback_state(value: object, field: str) -> str:
    state = required_feedback_text(value, field, limit=40)
    if state not in VALID_FEEDBACK_STATES:
        raise FeedbackLabelSchemaError(f"feedback label {field} must be open or occupied")
    return state


def safe_feedback_text_list(value: object, *, max_items: int, item_limit: int) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise FeedbackLabelSchemaError("feedback label replay fields must be lists")
    bounded: list[str] = []
    for item in list(value)[:positive_feedback_limit(max_items, max_items)]:
        text = optional_feedback_text(item, limit=item_limit)
        if text:
            bounded.append(text)
    return bounded


def _safe_metadata(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise FeedbackLabelSchemaError("feedback label source_metadata must be an object")
    sanitized: dict[str, str] = {}
    for raw_key, raw_value in list(value.items())[:MAX_METADATA_ITEMS]:
        key = optional_feedback_text(raw_key, limit=80)
        if not key:
            continue
        sanitized[key] = _safe_metadata_value(key, raw_value)
    return sanitized


def _safe_metadata_value(key: str, value: object) -> str:
    text = optional_feedback_text(value, limit=160) or ""
    lower_key = key.lower()
    if lower_key in {"sender", "room", "matrix_sender", "matrix_room_id", "operator"} or text.startswith(("@", "!")):
        return hash_operator_identifier(text)
    return text


def hash_operator_identifier(identifier: object) -> str:
    """Return a stable non-reversible hash for a Matrix sender or operator identifier."""

    text = optional_feedback_text(identifier, limit=4096) or ""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def required_feedback_text(value: object, field: str, *, limit: int) -> str:
    text = optional_feedback_text(value, limit=limit)
    if not text:
        raise FeedbackLabelSchemaError(f"feedback label {field} is required")
    return text


def optional_feedback_path_text(value: object) -> str | None:
    text = optional_feedback_text(value, limit=240)
    if text is None:
        return None
    if text.startswith("/") or ".." in Path(text).parts:
        raise FeedbackLabelSchemaError("feedback label evidence path must be relative and local")
    return text


def optional_feedback_text(value: object, *, limit: int) -> str | None:
    if value is None:
        return None
    return clip_feedback_text(value, limit)


def clip_feedback_text(value: object, limit: int) -> str:
    redacted = redact_diagnostic_text(value)
    redacted = _SENSITIVE_TOKEN_PATTERN.sub("<redacted>", redacted)
    redacted = _RAW_IMAGE_PREFIX_PATTERN.sub("<binary redacted>", redacted)
    encoded = redacted.encode("utf-8")
    if len(encoded) <= limit:
        return redacted
    return encoded[: max(0, limit - 3)].decode("utf-8", errors="ignore") + "..."


def _optional_non_negative_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise FeedbackLabelSchemaError("boolean is not a valid integer metadata value")
    try:
        integer = int(value)
    except (TypeError, ValueError) as exc:
        raise FeedbackLabelSchemaError("invalid integer metadata value") from exc
    if integer < 0:
        raise FeedbackLabelSchemaError("integer metadata value must be non-negative")
    return integer


def positive_feedback_limit(value: int, default: int) -> int:
    if isinstance(value, bool) or value <= 0:
        return default
    return value
