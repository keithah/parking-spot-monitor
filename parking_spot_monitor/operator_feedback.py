from __future__ import annotations

import hashlib
import json
import os
import re
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
MAX_FEEDBACK_LABELS = 500
MAX_TEXT_FIELD_CHARS = 500

LoadState = Literal["available", "missing", "unavailable"]

_SENSITIVE_TOKEN_PATTERN = re.compile(r"(?i)\b(?:syt|mxt|ghp|glpat|sk|xox[baprs])-?[a-z0-9_./+=-]{8,}\b")
_RAW_IMAGE_PREFIX_PATTERN = re.compile(r"[\xff\xd8][\xff\xe0].*", re.IGNORECASE)
_SAFE_ID_PATTERN = re.compile(r"[^A-Za-z0-9_.:-]+")


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
            "kind": _clip_text(self.kind, 80),
            "path": _safe_optional_path_text(self.path),
            "available": bool(self.available),
            "validated_jpeg": bool(self.validated_jpeg),
            "width": _optional_non_negative_int(self.width),
            "height": _optional_non_negative_int(self.height),
            "byte_size": _optional_non_negative_int(self.byte_size),
        }
        if self.error_type is not None:
            payload["error_type"] = _clip_text(self.error_type, 80)
        return payload


@dataclass(frozen=True)
class FeedbackLabel:
    """Schema-stable operator correction label for later replay or tuning review."""

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

    def to_json_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "label_id": _safe_required_text(self.label_id, "label_id", limit=160),
            "spot_id": _safe_required_text(self.spot_id, "spot_id", limit=80),
            "reported_state": _safe_required_text(self.reported_state, "reported_state", limit=40),
            "actual_state": _safe_required_text(self.actual_state, "actual_state", limit=40),
            "source": _safe_required_text(self.source, "source", limit=80),
            "operator_sender_hash": _safe_required_text(self.operator_sender_hash, "operator_sender_hash", limit=120),
            "corrected_at": _safe_required_text(self.corrected_at, "corrected_at", limit=80),
            "reported_at": _safe_optional_text(self.reported_at, limit=80),
            "alert_event_type": _safe_optional_text(self.alert_event_type, limit=120),
            "alert_event_id": _safe_optional_text(self.alert_event_id, limit=180),
            "evidence": self.evidence.to_json_dict(),
            "notes": _clip_text(self.notes, MAX_TEXT_FIELD_CHARS),
        }
        if self.matrix_event_id is not None:
            payload["matrix_event_id"] = _safe_optional_text(self.matrix_event_id, limit=180)
        return payload


@dataclass(frozen=True)
class FeedbackLabelLoad:
    """Bounded feedback-label load result with explicit unavailable diagnostics."""

    state: LoadState
    labels: tuple[FeedbackLabel, ...] = ()
    error_type: str | None = None
    quarantined_path: Path | None = None


def feedback_labels_path(data_dir: str | Path) -> Path:
    """Return the durable operator-feedback artifact path for a runtime data directory."""

    return Path(data_dir) / FEEDBACK_LABELS_FILENAME


def hash_operator_identifier(identifier: object) -> str:
    """Return a stable non-reversible hash for a Matrix sender or operator identifier."""

    text = _safe_optional_text(identifier, limit=4096) or ""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def reformat_timestamp_for_id(value: datetime | str | None) -> str:
    """Format a timestamp as a compact UTC token suitable for feedback label IDs."""

    if isinstance(value, datetime):
        selected = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return selected.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if isinstance(value, str) and value.strip():
        text = value.strip()
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            token = _SAFE_ID_PATTERN.sub("", redact_diagnostic_text(text))
            return token[:32] or reformat_timestamp_for_id(None)
        selected = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return selected.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def make_label_id(*, corrected_at: datetime | str | None, spot_id: str, operator_identifier: object) -> str:
    """Create a deterministic, safe label id prefix from timestamp, spot, and operator."""

    safe_spot = _SAFE_ID_PATTERN.sub("_", _safe_required_text(spot_id, "spot_id", limit=80)).strip("_") or "spot"
    operator_hash = hash_operator_identifier(operator_identifier).split(":", 1)[1][:8]
    return f"feedback-{reformat_timestamp_for_id(corrected_at)}-{safe_spot}-{operator_hash}"


def append_feedback_label(
    path: str | Path,
    label: FeedbackLabel | Mapping[str, Any],
    *,
    max_labels: int = MAX_FEEDBACK_LABELS,
    max_file_bytes: int = MAX_FEEDBACK_FILE_BYTES,
    logger: StructuredLogger | None = None,
) -> bool:
    """Append one sanitized feedback label with atomic write and bounded retention."""

    labels_path = Path(path)
    try:
        new_label = _label_from_any(label)
        loaded = load_feedback_labels(labels_path, max_labels=max_labels, max_file_bytes=max_file_bytes, logger=logger)
        retained = list(loaded.labels)
        if new_label.matrix_event_id and any(existing.matrix_event_id == new_label.matrix_event_id for existing in retained):
            _log(logger, "debug", "operator-feedback-label-duplicate-skipped", path=labels_path, matrix_event_id=new_label.matrix_event_id)
            return True
        retained.append(new_label)
        retained = retained[-_positive_limit(max_labels, MAX_FEEDBACK_LABELS) :]
        _write_feedback_labels(labels_path, retained)
    except Exception as exc:
        _log(logger, "warning", "operator-feedback-label-append-failed", path=labels_path, error_type=type(exc).__name__, error=str(exc))
        return False

    _log(logger, "debug", "operator-feedback-label-appended", path=labels_path, label_count=len(retained), label_id=new_label.label_id)
    return True


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
        quarantined = _quarantine_file(labels_path)
        _log(logger, "warning", "operator-feedback-labels-quarantined", path=labels_path, quarantine_path=quarantined, phase="size", error_type="oversized")
        return FeedbackLabelLoad(state="unavailable", error_type="oversized", quarantined_path=quarantined)

    try:
        with labels_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        labels = _labels_from_payload(payload)
    except (OSError, json.JSONDecodeError, FeedbackLabelSchemaError) as exc:
        quarantined = _quarantine_file(labels_path)
        _log(logger, "warning", "operator-feedback-labels-quarantined", path=labels_path, quarantine_path=quarantined, phase="load", error_type=type(exc).__name__, error=str(exc))
        return FeedbackLabelLoad(state="unavailable", error_type=type(exc).__name__, quarantined_path=quarantined)

    bounded = tuple(labels[-_positive_limit(max_labels, MAX_FEEDBACK_LABELS) :])
    _log(logger, "debug", "operator-feedback-labels-loaded", path=labels_path, label_count=len(bounded), state="available")
    return FeedbackLabelLoad(state="available", labels=bounded)


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


def _labels_from_payload(payload: Any) -> list[FeedbackLabel]:
    if not isinstance(payload, Mapping):
        raise FeedbackLabelSchemaError("feedback label payload must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise FeedbackLabelSchemaError("unsupported feedback label schema_version")
    raw_labels = payload.get("labels")
    if not isinstance(raw_labels, list):
        raise FeedbackLabelSchemaError("feedback label labels must be a list")
    if len(raw_labels) > MAX_FEEDBACK_LABELS * 10:
        raise FeedbackLabelSchemaError("feedback label count exceeds validation bound")
    labels: list[FeedbackLabel] = []
    for item in raw_labels:
        labels.append(_label_from_any(item))
    return labels


def _label_from_any(value: FeedbackLabel | Mapping[str, Any]) -> FeedbackLabel:
    if isinstance(value, FeedbackLabel):
        return FeedbackLabel(**value.to_json_dict() | {"evidence": FeedbackEvidence(**value.evidence.to_json_dict())})
    if not isinstance(value, Mapping):
        raise FeedbackLabelSchemaError("feedback label must be an object")
    evidence_value = value.get("evidence")
    if not isinstance(evidence_value, Mapping):
        raise FeedbackLabelSchemaError("feedback label evidence must be an object")
    evidence = FeedbackEvidence(
        kind=_safe_required_text(evidence_value.get("kind"), "evidence.kind", limit=80),
        path=_safe_optional_path_text(evidence_value.get("path")),
        available=bool(evidence_value.get("available", False)),
        validated_jpeg=bool(evidence_value.get("validated_jpeg", False)),
        width=_optional_non_negative_int(evidence_value.get("width")),
        height=_optional_non_negative_int(evidence_value.get("height")),
        byte_size=_optional_non_negative_int(evidence_value.get("byte_size")),
        error_type=_safe_optional_text(evidence_value.get("error_type"), limit=80),
    )
    return FeedbackLabel(
        label_id=_safe_required_text(value.get("label_id"), "label_id", limit=160),
        spot_id=_safe_required_text(value.get("spot_id"), "spot_id", limit=80),
        reported_state=_safe_required_text(value.get("reported_state"), "reported_state", limit=40),
        actual_state=_safe_required_text(value.get("actual_state"), "actual_state", limit=40),
        source=_safe_required_text(value.get("source"), "source", limit=80),
        operator_sender_hash=_safe_required_text(value.get("operator_sender_hash"), "operator_sender_hash", limit=120),
        corrected_at=_safe_required_text(value.get("corrected_at"), "corrected_at", limit=80),
        reported_at=_safe_optional_text(value.get("reported_at"), limit=80),
        alert_event_type=_safe_optional_text(value.get("alert_event_type"), limit=120),
        alert_event_id=_safe_optional_text(value.get("alert_event_id"), limit=180),
        evidence=evidence,
        notes=_safe_optional_text(value.get("notes"), limit=MAX_TEXT_FIELD_CHARS) or "",
        matrix_event_id=_safe_optional_text(value.get("matrix_event_id"), limit=180),
    )


def _safe_required_text(value: object, field: str, *, limit: int) -> str:
    text = _safe_optional_text(value, limit=limit)
    if not text:
        raise FeedbackLabelSchemaError(f"feedback label {field} is required")
    return text


def _safe_optional_path_text(value: object) -> str | None:
    text = _safe_optional_text(value, limit=240)
    if text is None:
        return None
    if text.startswith("/") or ".." in Path(text).parts:
        raise FeedbackLabelSchemaError("feedback label evidence path must be relative and local")
    return text


def _safe_optional_text(value: object, *, limit: int) -> str | None:
    if value is None:
        return None
    return _clip_text(value, limit)


def _clip_text(value: object, limit: int) -> str:
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


def _positive_limit(value: int, default: int) -> int:
    if isinstance(value, bool) or value <= 0:
        return default
    return value


def _quarantine_file(path: Path) -> Path | None:
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
