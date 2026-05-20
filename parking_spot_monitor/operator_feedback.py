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

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.incident_review import build_incident_replay
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.operator_decision_memory import (
    append_decision_memory_record,
    decision_memory_path,
    load_decision_memory,
    make_decision_memory_record,
)

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

MAX_REPLAY_CONTEXT_LINES = 12
MAX_REPLAY_LINE_CHARS = 240
MAX_DEGRADATION_REASONS = 8
MAX_METADATA_ITEMS = 16

_SENSITIVE_TOKEN_PATTERN = re.compile(r"(?i)\b(?:syt|mxt|ghp|glpat|sk|xox[baprs])-?[a-z0-9_./+=-]{8,}\b")
_RAW_IMAGE_PREFIX_PATTERN = re.compile(r"\xff\xd8[\s\S]*")
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
            "label_id": _safe_required_text(self.label_id, "label_id", limit=160),
            "spot_id": _safe_required_text(self.spot_id, "spot_id", limit=80),
            "reported_state": _feedback_state(self.reported_state, "reported_state"),
            "actual_state": _feedback_state(self.actual_state, "actual_state"),
            "source": _safe_required_text(self.source, "source", limit=80),
            "operator_sender_hash": _safe_required_text(self.operator_sender_hash, "operator_sender_hash", limit=120),
            "corrected_at": _safe_required_text(self.corrected_at, "corrected_at", limit=80),
            "reported_at": _safe_optional_text(self.reported_at, limit=80),
            "alert_event_type": _safe_optional_text(self.alert_event_type, limit=120),
            "alert_event_id": _safe_optional_text(self.alert_event_id, limit=180),
            "evidence": self.evidence.to_json_dict(),
            "notes": _clip_text(self.notes, MAX_TEXT_FIELD_CHARS),
            "label_type": label_type,
        }
        if self.matrix_event_id is not None:
            payload["matrix_event_id"] = _safe_optional_text(self.matrix_event_id, limit=180)
        if self.matrix_room_id_hash is not None:
            payload["matrix_room_id_hash"] = _safe_optional_text(self.matrix_room_id_hash, limit=120)
        if self.feedback_category is not None:
            payload["feedback_category"] = _safe_feedback_category(self.feedback_category)
        if self.feedback_category_details is not None:
            payload["feedback_category_details"] = _safe_metadata(self.feedback_category_details)
        if label_type == "learn":
            payload["target_state"] = _feedback_state(self.target_state, "target_state")
            payload["learned_at"] = _safe_required_text(self.learned_at or self.corrected_at, "learned_at", limit=80)
            payload["replay_context"] = _safe_text_list(self.replay_context, max_items=MAX_REPLAY_CONTEXT_LINES, item_limit=MAX_REPLAY_LINE_CHARS)
            payload["degradation_reasons"] = _safe_text_list(self.degradation_reasons, max_items=MAX_DEGRADATION_REASONS, item_limit=120)
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


class OperatorFeedbackLabeler:
    """High-level API for Matrix operator spot-state correction labels."""

    def __init__(self, *, data_dir: str | Path, snapshots_dir: str | Path | None = None, logger: StructuredLogger | None = None) -> None:
        self.data_dir = Path(data_dir)
        self.snapshots_dir = Path(snapshots_dir) if snapshots_dir is not None else None
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
        safe_spot = _safe_spot_id(spot_id)
        if safe_spot is None:
            return FeedbackRecordResult(
                recorded=False,
                reply_text="Parking correction not recorded\nInvalid spot id.",
                spot_id="",
                actual_state="open",
                error_type="invalid_spot",
            )
        existing_label = find_feedback_label_by_matrix_event_id(self.labels_path, matrix_event_id, logger=self.logger)
        if existing_label is not None:
            reported_state = _feedback_state(existing_label.reported_state, "reported_state")
            stored_actual_state = _feedback_state(existing_label.actual_state, "actual_state")
            return FeedbackRecordResult(
                recorded=True,
                reply_text=format_duplicate_correction_reply(existing_label.spot_id, reported_state, stored_actual_state, existing_label.evidence),
                spot_id=existing_label.spot_id,
                actual_state=stored_actual_state,
                reported_state=reported_state,
                evidence=existing_label.evidence,
                label_id=existing_label.label_id,
            )

        state = _feedback_state(actual_state, "actual_state")
        corrected_text = _feedback_timestamp_text(corrected_at)
        candidate = resolve_latest_alert_candidate(self.memory_path, safe_spot, logger=self.logger)
        if candidate is None:
            reply = (
                "Parking correction not recorded\n"
                f"No recent alert was found for {safe_spot}; use !parking latest or !parking who to inspect current evidence."
            )
            return FeedbackRecordResult(
                recorded=False,
                reply_text=reply,
                spot_id=safe_spot,
                actual_state=state,
                error_type="no_recent_alert",
            )

        evidence = validate_feedback_evidence(
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            snapshot_path=candidate.snapshot_path,
            logger=self.logger,
        )
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
            matrix_event_id=matrix_event_id,
            matrix_room_id_hash=hash_operator_identifier(matrix_room_id),
            feedback_category="false_alert",
            feedback_category_details={"reported_state": candidate.reported_state, "actual_state": state},
        )
        append_result = append_feedback_label(self.labels_path, label, logger=self.logger)
        if not append_result:
            reply = "Parking correction not recorded\nFeedback store unavailable (feedback_store_unavailable)."
            return FeedbackRecordResult(
                recorded=False,
                reply_text=reply,
                spot_id=safe_spot,
                actual_state=state,
                reported_state=candidate.reported_state,
                evidence=evidence,
                error_type="feedback_store_unavailable",
            )
        if append_result.status == "duplicate":
            return FeedbackRecordResult(
                recorded=True,
                reply_text=format_duplicate_correction_reply(safe_spot, candidate.reported_state, state, evidence),
                spot_id=safe_spot,
                actual_state=state,
                reported_state=candidate.reported_state,
                evidence=evidence,
                label_id=label_id,
            )

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
                    "feedback_category": "false_alert",
                    "feedback_category_details": {"reported_state": candidate.reported_state, "actual_state": state},
                },
            ),
            logger=self.logger,
        )
        return FeedbackRecordResult(
            recorded=True,
            reply_text=format_correction_reply(safe_spot, candidate.reported_state, state, evidence),
            spot_id=safe_spot,
            actual_state=state,
            reported_state=candidate.reported_state,
            evidence=evidence,
            label_id=label_id,
        )

    def record_learn_label(
        self,
        *,
        spot_id: str,
        target_state: str,
        requested_time: datetime | str,
        settings: Any | None,
        state_path: str | Path | None,
        detector: Any | None,
        matrix_event_id: str,
        matrix_sender: str,
        matrix_room_id: str,
        learned_at: datetime | str | None = None,
        now: datetime | None = None,
    ) -> LearnLabelRecordResult:
        """Record a learn-command label from retained timeline evidence and copied-state replay only."""

        safe_spot = _safe_spot_id(spot_id)
        if safe_spot is None or not _learn_spot_is_configured(settings, safe_spot):
            return LearnLabelRecordResult(
                recorded=False,
                reply_text="Parking learn label not recorded\nInvalid spot id.",
                spot_id="",
                target_state="open",
                error_type="invalid_spot",
            )
        try:
            state = _feedback_state(target_state, "target_state")
        except FeedbackLabelSchemaError:
            return LearnLabelRecordResult(
                recorded=False,
                reply_text="Parking learn label not recorded\nInvalid target state; use open or occupied.",
                spot_id=safe_spot,
                target_state="open",
                error_type="invalid_state",
            )

        existing_label = find_feedback_label_by_matrix_event_id(self.labels_path, matrix_event_id, logger=self.logger)
        if existing_label is not None:
            existing_state = _feedback_state(existing_label.target_state or existing_label.actual_state, "target_state")
            return LearnLabelRecordResult(
                recorded=True,
                reply_text=format_duplicate_learn_reply(existing_label),
                spot_id=existing_label.spot_id,
                target_state=existing_state,
                requested_at=existing_label.learned_at or existing_label.corrected_at,
                evidence=existing_label.evidence,
                replay_context=tuple(existing_label.replay_context),
                degradation_reasons=tuple(existing_label.degradation_reasons),
                label_id=existing_label.label_id,
                duplicate=True,
            )

        try:
            target_time = _parse_learn_requested_time(requested_time, now=now)
        except ValueError:
            return LearnLabelRecordResult(
                recorded=False,
                reply_text="Parking learn label not recorded\nInvalid time; use ISO time or h:mmam/pm.",
                spot_id=safe_spot,
                target_state=state,
                error_type="invalid_time",
            )

        nearest = _nearest_learn_timeline_frame(self.data_dir, target_time)
        if nearest is None:
            evidence = FeedbackEvidence("timeline_frame", None, False, False, None, None, None, "missing")
            return LearnLabelRecordResult(
                recorded=False,
                reply_text=format_learn_reply(safe_spot, state, evidence, (), ("timeline_missing",), recorded=False),
                spot_id=safe_spot,
                target_state=state,
                requested_at=target_time.isoformat().replace("+00:00", "Z"),
                evidence=evidence,
                degradation_reasons=("timeline_missing",),
                error_type="timeline_missing",
            )

        frame_path, frame_time = nearest
        evidence = validate_timeline_feedback_evidence(data_dir=self.data_dir, frame_path=frame_path, logger=self.logger)
        if not (evidence.available and evidence.validated_jpeg):
            reasons = tuple(_learn_degradation_reasons(evidence=evidence, replay=None))
            return LearnLabelRecordResult(
                recorded=False,
                reply_text=format_learn_reply(safe_spot, state, evidence, (), reasons, recorded=False),
                spot_id=safe_spot,
                target_state=state,
                requested_at=target_time.isoformat().replace("+00:00", "Z"),
                evidence=evidence,
                degradation_reasons=reasons,
                error_type=evidence.error_type or "invalid_evidence",
            )

        replay = build_incident_replay(
            settings=settings,
            frame_path=frame_path,
            frame_time=frame_time,
            requested_spot_id=safe_spot,
            state_path=state_path,
            detector=detector,
        )
        if replay.unavailable_reason == "corrupt_frame":
            evidence = FeedbackEvidence(
                "timeline_frame",
                evidence.path,
                False,
                False,
                evidence.width,
                evidence.height,
                evidence.byte_size,
                "corrupt_frame",
            )
            reasons = ("corrupt_frame",)
            return LearnLabelRecordResult(
                recorded=False,
                reply_text=format_learn_reply(safe_spot, state, evidence, replay.lines, reasons, recorded=False),
                spot_id=safe_spot,
                target_state=state,
                requested_at=target_time.isoformat().replace("+00:00", "Z"),
                evidence=evidence,
                replay_context=tuple(replay.lines),
                degradation_reasons=reasons,
                error_type="corrupt_frame",
            )

        reasons = tuple(_learn_degradation_reasons(evidence=evidence, replay=replay))
        learned_text = _feedback_timestamp_text(learned_at or target_time)
        delta_seconds = abs(int((frame_time - target_time).total_seconds()))
        source_metadata = {
            "command": "learn",
            "requested_at": target_time.isoformat().replace("+00:00", "Z"),
            "frame_observed_at": frame_time.isoformat().replace("+00:00", "Z"),
            "frame_delta_seconds": str(delta_seconds),
            "replay_unavailable_reason": replay.unavailable_reason or "",
            "detector_error_type": replay.detector_error_type or "",
            "state_error_type": replay.state_error_type or "",
        }
        label = make_learn_feedback_label(
            spot_id=safe_spot,
            target_state=state,
            learned_at=learned_text,
            matrix_event_id=matrix_event_id,
            matrix_sender=matrix_sender,
            matrix_room_id=matrix_room_id,
            evidence=evidence,
            replay_context=replay.lines,
            degradation_reasons=reasons,
            source_metadata=source_metadata,
        )
        append_result = append_feedback_label(self.labels_path, label, logger=self.logger)
        if not append_result:
            return LearnLabelRecordResult(
                recorded=False,
                reply_text="Parking learn label not recorded\nFeedback store unavailable (feedback_store_unavailable).",
                spot_id=safe_spot,
                target_state=state,
                requested_at=target_time.isoformat().replace("+00:00", "Z"),
                evidence=evidence,
                replay_context=tuple(replay.lines),
                degradation_reasons=reasons,
                error_type="feedback_store_unavailable",
            )
        if append_result.status == "duplicate":
            return LearnLabelRecordResult(
                recorded=True,
                reply_text=format_learn_reply(safe_spot, state, evidence, replay.lines, reasons, recorded=True, duplicate=True),
                spot_id=safe_spot,
                target_state=state,
                requested_at=target_time.isoformat().replace("+00:00", "Z"),
                evidence=evidence,
                replay_context=tuple(replay.lines),
                degradation_reasons=reasons,
                label_id=label.label_id,
                duplicate=True,
            )

        append_decision_memory_record(
            self.memory_path,
            make_decision_memory_record(
                "feedback",
                observed_at=learned_text,
                spot_id=safe_spot,
                summary=f"operator learn label recorded: target {state}; replay {'degraded' if reasons else 'available'}",
                details={
                    "label_id": label.label_id,
                    "label_type": "learn",
                    "target_state": state,
                    "evidence_available": evidence.available,
                    "evidence_path": evidence.path,
                    "replay_line_count": len(replay.lines),
                    "degradation_reasons": list(reasons),
                    "feedback_category": "missed_alert",
                    "feedback_category_details": {"target_state": state, "requested_at": target_time.isoformat().replace("+00:00", "Z")},
                },
            ),
            logger=self.logger,
        )
        return LearnLabelRecordResult(
            recorded=True,
            reply_text=format_learn_reply(safe_spot, state, evidence, replay.lines, reasons, recorded=True),
            spot_id=safe_spot,
            target_state=state,
            requested_at=target_time.isoformat().replace("+00:00", "Z"),
            evidence=evidence,
            replay_context=tuple(replay.lines),
            degradation_reasons=reasons,
            label_id=label.label_id,
        )


def feedback_labels_path(data_dir: str | Path) -> Path:
    """Return the durable operator-feedback artifact path for a runtime data directory."""

    return Path(data_dir) / FEEDBACK_LABELS_FILENAME


def validate_timeline_feedback_evidence(
    *,
    data_dir: str | Path,
    frame_path: str | Path | None,
    logger: StructuredLogger | None = None,
) -> FeedbackEvidence:
    """Validate safe metadata for one retained timeline JPEG frame."""

    if frame_path is None:
        return FeedbackEvidence("timeline_frame", None, False, False, None, None, None, "missing")
    root = Path(data_dir).resolve()
    path = Path(frame_path)
    try:
        resolved = path.resolve()
        relative = resolved.relative_to(root).as_posix()
        safe_relative = _safe_optional_path_text(relative)
    except (OSError, ValueError, FeedbackLabelSchemaError):
        return FeedbackEvidence("timeline_frame", None, False, False, None, None, None, "unsafe_path")
    if safe_relative is None or not safe_relative.startswith("timeline/frames/"):
        return FeedbackEvidence("timeline_frame", safe_relative, False, False, None, None, None, "unsafe_path")
    try:
        byte_size = resolved.stat().st_size
        with Image.open(resolved) as image:
            image.verify()
        with Image.open(resolved) as image:
            width, height = image.size
            if image.format != "JPEG" or width <= 0 or height <= 0:
                return FeedbackEvidence("timeline_frame", safe_relative, False, False, None, None, byte_size, "invalid_jpeg")
    except (OSError, UnidentifiedImageError, ValueError) as exc:
        _log(logger, "warning", "operator-feedback-timeline-evidence-invalid", path=safe_relative, error_type=type(exc).__name__)
        return FeedbackEvidence("timeline_frame", safe_relative, False, False, None, None, None, "invalid_jpeg")
    return FeedbackEvidence("timeline_frame", safe_relative, True, True, width, height, byte_size, None)


def format_learn_reply(
    spot_id: str,
    target_state: str,
    evidence: FeedbackEvidence,
    replay_context: Sequence[str],
    degradation_reasons: Sequence[str],
    *,
    recorded: bool,
    duplicate: bool = False,
) -> str:
    """Format a bounded operator-visible learn-label acknowledgement."""

    if duplicate:
        heading = "Command already applied; learn acknowledgement repeated."
    else:
        heading = "Parking learn label recorded" if recorded else "Parking learn label not recorded"
    if evidence.available and evidence.validated_jpeg:
        evidence_line = f"linked evidence: retained timeline frame ({evidence.width}x{evidence.height})"
    else:
        evidence_line = f"linked evidence: unavailable; retained timeline frame unavailable ({evidence.error_type or 'unavailable'})"
    replay_line = "replay: available" if replay_context else "replay: unavailable"
    safe_reasons = _safe_text_list(degradation_reasons, max_items=MAX_DEGRADATION_REASONS, item_limit=120)
    if safe_reasons:
        replay_line += "; degraded " + ", ".join(safe_reasons[:MAX_DEGRADATION_REASONS])
    return _bounded_feedback_reply([
        heading,
        f"- spot: {spot_id}",
        f"- target: {target_state}",
        f"- {evidence_line}",
        f"- {replay_line}",
        "- next: run !parking lab run replay after labels are reviewed",
    ])


def format_duplicate_learn_reply(label: FeedbackLabel) -> str:
    """Format an idempotent acknowledgement for a replayed Matrix learn event."""

    return format_learn_reply(
        label.spot_id,
        label.target_state or label.actual_state,
        label.evidence,
        label.replay_context,
        label.degradation_reasons,
        recorded=True,
        duplicate=True,
    )


def _parse_learn_requested_time(value: datetime | str, *, now: datetime | None) -> datetime:
    if isinstance(value, datetime):
        selected = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return selected.astimezone(timezone.utc)
    from parking_spot_monitor.operator_cockpit import _parse_incident_time

    return _parse_incident_time(str(value), now=_utc_now(now))


def _nearest_learn_timeline_frame(data_dir: str | Path, target_time: datetime) -> tuple[Path, datetime] | None:
    from parking_spot_monitor.operator_cockpit import _nearest_timeline_frame

    return _nearest_timeline_frame(Path(data_dir), target_time)


def _utc_now(value: datetime | None) -> datetime:
    selected = value if value is not None else datetime.now(timezone.utc)
    if selected.tzinfo is None:
        return selected.replace(tzinfo=timezone.utc)
    return selected.astimezone(timezone.utc)


def _learn_spot_is_configured(settings: Any | None, spot_id: str) -> bool:
    spots = getattr(settings, "spots", None)
    if spots is None:
        return spot_id in {"left_spot", "right_spot"}
    return any(getattr(spots, candidate, None) is not None and spot_id == candidate for candidate in ("left_spot", "right_spot"))


def _learn_degradation_reasons(*, evidence: FeedbackEvidence, replay: Any | None) -> list[str]:
    reasons: list[str] = []
    if not evidence.available or not evidence.validated_jpeg:
        reasons.append(evidence.error_type or "evidence_unavailable")
    if replay is not None:
        for value in (getattr(replay, "unavailable_reason", None), getattr(replay, "detector_error_type", None), getattr(replay, "state_error_type", None)):
            text = _safe_optional_text(value, limit=120)
            if text and text not in reasons:
                reasons.append(text)
    return reasons[:MAX_DEGRADATION_REASONS]


def _bounded_feedback_reply(lines: Sequence[str]) -> str:
    rendered = redact_diagnostic_text("\n".join(_clip_text(line, MAX_REPLAY_LINE_CHARS) for line in lines[:MAX_REPLAY_CONTEXT_LINES]))
    encoded = rendered.encode("utf-8")
    if len(encoded) <= 4096:
        return rendered
    return encoded[:4093].decode("utf-8", errors="ignore") + "..."


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


def make_label_id(*, corrected_at: datetime | str | None, spot_id: str, matrix_event_id: str | None) -> str:
    """Create a deterministic, safe label id from timestamp, spot, and Matrix event id."""

    timestamp = reformat_timestamp_for_id(corrected_at)
    safe_spot = _SAFE_ID_PATTERN.sub("_", _safe_required_text(spot_id, "spot_id", limit=80)).strip("_") or "spot"
    event_id = _safe_optional_text(matrix_event_id, limit=180) or ""
    suffix_material = "\0".join((timestamp, safe_spot, event_id))
    suffix = hashlib.sha256(suffix_material.encode("utf-8")).hexdigest()[:8]
    return f"feedback-{timestamp}-{safe_spot}-{suffix}"


def make_learn_feedback_label(
    *,
    spot_id: str,
    target_state: str,
    learned_at: datetime | str | None,
    matrix_event_id: str,
    matrix_sender: str,
    matrix_room_id: str,
    evidence: FeedbackEvidence,
    replay_context: Sequence[str] = (),
    degradation_reasons: Sequence[str] = (),
    source_metadata: Mapping[str, Any] | None = None,
) -> FeedbackLabel:
    """Build a sanitized learn-command label linked to retained evidence and replay context."""

    safe_spot = _safe_spot_id(spot_id)
    if safe_spot is None:
        raise FeedbackLabelSchemaError("feedback label spot_id is required")
    state = _feedback_state(target_state, "target_state")
    learned_text = _feedback_timestamp_text(learned_at)
    return FeedbackLabel(
        label_id=make_label_id(corrected_at=learned_text, spot_id=safe_spot, matrix_event_id=matrix_event_id),
        spot_id=safe_spot,
        reported_state=state,
        actual_state=state,
        source="matrix_learn_command",
        operator_sender_hash=hash_operator_identifier(matrix_sender),
        corrected_at=learned_text,
        reported_at=learned_text,
        alert_event_type=None,
        alert_event_id=None,
        evidence=evidence,
        matrix_event_id=matrix_event_id,
        matrix_room_id_hash=hash_operator_identifier(matrix_room_id),
        label_type="learn",
        target_state=state,
        learned_at=learned_text,
        replay_context=tuple(replay_context),
        degradation_reasons=tuple(degradation_reasons),
        source_metadata=source_metadata,
        feedback_category="missed_alert",
        feedback_category_details={"target_state": state},
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
        new_label = _label_from_any(label)
        loaded = load_feedback_labels(labels_path, max_labels=max_labels, max_file_bytes=max_file_bytes, logger=logger)
        retained = list(loaded.labels)
        if new_label.matrix_event_id and any(existing.matrix_event_id == new_label.matrix_event_id for existing in retained):
            _log(logger, "debug", "operator-feedback-label-duplicate-skipped", path=labels_path, matrix_event_id=new_label.matrix_event_id)
            return FeedbackAppendResult(status="duplicate", label_id=new_label.label_id)
        retained.append(new_label)
        retained = retained[-_positive_limit(max_labels, MAX_FEEDBACK_LABELS) :]
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


def find_feedback_label_by_matrix_event_id(path: str | Path, matrix_event_id: str | None, *, logger: StructuredLogger | None = None) -> FeedbackLabel | None:
    """Return the stored feedback label for an already-processed Matrix event id."""

    safe_event_id = _safe_optional_text(matrix_event_id, limit=180)
    if not safe_event_id:
        return None
    loaded = load_feedback_labels(path, logger=logger)
    if loaded.state != "available":
        return None
    for label in reversed(loaded.labels):
        if label.matrix_event_id == safe_event_id:
            return label
    return None


def resolve_latest_alert_candidate(path: str | Path, spot_id: str, *, logger: StructuredLogger | None = None) -> AlertEvidenceCandidate | None:
    """Return the newest alert memory record for a spot with a recognized reported state."""

    loaded = load_decision_memory(path, logger=logger)
    if loaded.state != "available":
        return None
    for record in reversed(loaded.records):
        if record.kind != "alert" or record.spot_id != spot_id:
            continue
        details = record.details if isinstance(record.details, Mapping) else {}
        if details.get("outcome") != "sent":
            continue
        event_type = details.get("event_type")
        reported_state = _reported_state_from_event_type(event_type)
        if reported_state is None:
            continue
        return AlertEvidenceCandidate(
            spot_id=spot_id,
            reported_state=reported_state,
            reported_at=record.observed_at,
            alert_event_type=_safe_optional_text(event_type, limit=120),
            alert_event_id=_safe_optional_text(details.get("event_id"), limit=180),
            snapshot_path=_safe_optional_text(details.get("retained_snapshot_path") or details.get("snapshot_path"), limit=240),
        )
    return None


def validate_feedback_evidence(
    *,
    data_dir: str | Path,
    snapshot_path: str | None,
    snapshots_dir: str | Path | None = None,
    logger: StructuredLogger | None = None,
) -> FeedbackEvidence:
    """Validate safe, local retained alert snapshot metadata without reading image bytes into labels."""

    if not snapshot_path:
        return FeedbackEvidence("alert_snapshot", None, False, False, None, None, None, "missing")
    try:
        safe_relative = _safe_optional_path_text(snapshot_path)
    except FeedbackLabelSchemaError:
        return FeedbackEvidence("alert_snapshot", None, False, False, None, None, None, "unsafe_path")
    if safe_relative is None:
        return FeedbackEvidence("alert_snapshot", None, False, False, None, None, None, "missing")

    candidate = _resolve_feedback_evidence_path(data_dir=Path(data_dir), snapshots_dir=snapshots_dir, safe_relative=safe_relative)
    if candidate is None:
        return FeedbackEvidence("alert_snapshot", safe_relative, False, False, None, None, None, "missing")

    try:
        byte_size = candidate.stat().st_size
        with Image.open(candidate) as image:
            image.verify()
        with Image.open(candidate) as image:
            width, height = image.size
            if image.format != "JPEG":
                return FeedbackEvidence("alert_snapshot", safe_relative, False, False, None, None, byte_size, "invalid_jpeg")
    except (OSError, UnidentifiedImageError) as exc:
        _log(logger, "warning", "operator-feedback-evidence-invalid", path=safe_relative, error_type=type(exc).__name__)
        return FeedbackEvidence("alert_snapshot", safe_relative, False, False, None, None, None, "invalid_jpeg")

    return FeedbackEvidence("alert_snapshot", safe_relative, True, True, width, height, byte_size, None)


def _resolve_feedback_evidence_path(*, data_dir: Path, snapshots_dir: str | Path | None, safe_relative: str) -> Path | None:
    """Resolve a safe relative alert snapshot path under accepted runtime evidence roots."""

    relative = Path(safe_relative)
    roots: list[Path] = [data_dir.resolve()]
    if snapshots_dir is not None:
        root = Path(snapshots_dir).resolve()
        roots.append(root)
        roots.append(root.parent)
    else:
        roots.append((data_dir / "snapshots").resolve())

    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            continue
        if candidate.exists():
            return candidate
    return None


def format_duplicate_correction_reply(spot_id: str, reported_state: str, actual_state: str, evidence: FeedbackEvidence) -> str:
    """Format an idempotent acknowledgement for a replayed Matrix correction event."""

    return format_correction_reply(spot_id, reported_state, actual_state, evidence).replace(
        "Parking correction recorded",
        "Command already applied; acknowledgement repeated.",
        1,
    )


def format_correction_reply(spot_id: str, reported_state: str, actual_state: str, evidence: FeedbackEvidence) -> str:
    """Format a bounded operator-visible correction acknowledgement."""

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


def _reported_state_from_event_type(value: object) -> SpotState | None:
    text = str(value or "")
    if text == "occupancy-occupied-event":
        return "occupied"
    if text == "occupancy-open-event":
        return "open"
    return None


def _safe_spot_id(value: str) -> str | None:
    try:
        text = _safe_required_text(value, "spot_id", limit=80)
    except FeedbackLabelSchemaError:
        return None
    if text.startswith("/") or "\\" in text or ".." in Path(text).parts:
        return None
    return text


def _feedback_timestamp_text(value: datetime | str | None) -> str:
    if isinstance(value, datetime):
        selected = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return selected.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str) and value.strip():
        text = value.strip()
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        parsed = datetime.fromisoformat(normalized)
        selected = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return selected.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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
    label_type = _feedback_label_type(value.get("label_type", "correction"))
    return FeedbackLabel(
        label_id=_safe_required_text(value.get("label_id"), "label_id", limit=160),
        spot_id=_safe_required_text(value.get("spot_id"), "spot_id", limit=80),
        reported_state=_feedback_state(value.get("reported_state"), "reported_state"),
        actual_state=_feedback_state(value.get("actual_state"), "actual_state"),
        source=_safe_required_text(value.get("source"), "source", limit=80),
        operator_sender_hash=_safe_required_text(value.get("operator_sender_hash"), "operator_sender_hash", limit=120),
        corrected_at=_safe_required_text(value.get("corrected_at"), "corrected_at", limit=80),
        reported_at=_safe_optional_text(value.get("reported_at"), limit=80),
        alert_event_type=_safe_optional_text(value.get("alert_event_type"), limit=120),
        alert_event_id=_safe_optional_text(value.get("alert_event_id"), limit=180),
        evidence=evidence,
        notes=_safe_optional_text(value.get("notes"), limit=MAX_TEXT_FIELD_CHARS) or "",
        matrix_event_id=_safe_optional_text(value.get("matrix_event_id"), limit=180),
        matrix_room_id_hash=_safe_optional_text(value.get("matrix_room_id_hash"), limit=120),
        label_type=label_type,
        target_state=_feedback_state(value.get("target_state"), "target_state") if label_type == "learn" else _safe_optional_text(value.get("target_state"), limit=40),
        learned_at=_safe_required_text(value.get("learned_at") or value.get("corrected_at"), "learned_at", limit=80) if label_type == "learn" else _safe_optional_text(value.get("learned_at"), limit=80),
        replay_context=tuple(_safe_text_list(value.get("replay_context", ()), max_items=MAX_REPLAY_CONTEXT_LINES, item_limit=MAX_REPLAY_LINE_CHARS)) if label_type == "learn" else (),
        degradation_reasons=tuple(_safe_text_list(value.get("degradation_reasons", ()), max_items=MAX_DEGRADATION_REASONS, item_limit=120)) if label_type == "learn" else (),
        source_metadata=_safe_metadata(value.get("source_metadata")) if label_type == "learn" else None,
        feedback_category=_safe_feedback_category(value.get("feedback_category")) if value.get("feedback_category") is not None else None,
        feedback_category_details=_safe_metadata(value.get("feedback_category_details")) if value.get("feedback_category_details") is not None else None,
    )


def _safe_feedback_category(value: object) -> str:
    category = _safe_required_text(value, "feedback_category", limit=80)
    if category not in {"false_alert", "missed_alert"}:
        raise FeedbackLabelSchemaError("feedback label feedback_category must be false_alert or missed_alert")
    return category


def _feedback_label_type(value: object) -> FeedbackLabelType:
    label_type = _safe_required_text(value, "label_type", limit=40)
    if label_type not in {"correction", "learn"}:
        raise FeedbackLabelSchemaError("feedback label label_type must be correction or learn")
    return label_type  # type: ignore[return-value]


def _feedback_state(value: object, field: str) -> str:
    state = _safe_required_text(value, field, limit=40)
    if state not in VALID_FEEDBACK_STATES:
        raise FeedbackLabelSchemaError(f"feedback label {field} must be open or occupied")
    return state


def _safe_text_list(value: object, *, max_items: int, item_limit: int) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise FeedbackLabelSchemaError("feedback label replay fields must be lists")
    bounded: list[str] = []
    for item in list(value)[:_positive_limit(max_items, max_items)]:
        text = _safe_optional_text(item, limit=item_limit)
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
        key = _safe_optional_text(raw_key, limit=80)
        if not key:
            continue
        sanitized[key] = _safe_metadata_value(key, raw_value)
    return sanitized


def _safe_metadata_value(key: str, value: object) -> str:
    text = _safe_optional_text(value, limit=160) or ""
    lower_key = key.lower()
    if lower_key in {"sender", "room", "matrix_sender", "matrix_room_id", "operator"} or text.startswith(("@", "!")):
        return hash_operator_identifier(text)
    return text


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
