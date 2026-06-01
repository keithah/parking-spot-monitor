from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.incident_review import build_incident_replay
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.operator_decision_memory import (
    append_decision_memory_record,
    decision_memory_path,
    load_decision_memory,
    make_decision_memory_record,
)
from parking_spot_monitor.operator_feedback_models import (
    FEEDBACK_LABELS_FILENAME,
    MAX_FEEDBACK_FILE_BYTES,
    MAX_FEEDBACK_LABELS,
    MAX_DEGRADATION_REASONS,
    MAX_REPLAY_CONTEXT_LINES,
    MAX_REPLAY_LINE_CHARS,
    AlertEvidenceCandidate,
    FeedbackAppendResult,
    FeedbackEvidence,
    FeedbackLabel,
    FeedbackLabelLoad,
    FeedbackLabelSchemaError,
    FeedbackRecordResult,
    LearnLabelRecordResult,
    _clip_text,
    _feedback_state,
    _label_from_any,
    _labels_from_payload,
    _positive_limit,
    _quarantine_file,
    _safe_optional_path_text,
    _safe_optional_text,
    _safe_required_text,
    _safe_text_list,
    _write_feedback_labels,
    hash_operator_identifier,
)
from parking_spot_monitor.operator_timeline import nearest_timeline_frame, parse_incident_time


_SAFE_ID_PATTERN = re.compile(r"[^A-Za-z0-9_.:-]+")










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
    return parse_incident_time(str(value), now=_utc_now(now))


def _nearest_learn_timeline_frame(data_dir: str | Path, target_time: datetime) -> tuple[Path, datetime] | None:
    return nearest_timeline_frame(Path(data_dir), target_time)


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

















def _log(logger: StructuredLogger | None, level: str, event: str, **fields: Any) -> None:
    if logger is None:
        return
    getattr(logger, level)(event, **redact_diagnostic_value(fields))
