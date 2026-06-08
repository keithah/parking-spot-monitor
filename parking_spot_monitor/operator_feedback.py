from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor.incident_review import build_incident_replay
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.operator_decision_memory import (
    append_decision_memory_record,
    decision_memory_path,
    make_decision_memory_record,
)
from parking_spot_monitor.operator_feedback_alerts import (
    reported_state_from_event_type as _reported_state_from_event_type,
    resolve_latest_alert_candidate,
)
from parking_spot_monitor.operator_feedback_evidence import (
    resolve_feedback_evidence_path as _resolve_feedback_evidence_path,
    validate_feedback_evidence,
    validate_timeline_feedback_evidence,
)
from parking_spot_monitor.operator_feedback_labels import (
    feedback_timestamp_text as _feedback_timestamp_text,
    make_label_id,
    make_learn_feedback_label,
    reformat_timestamp_for_id,
    safe_spot_id as _safe_spot_id,
)
from parking_spot_monitor.operator_feedback_models import (
    FEEDBACK_LABELS_FILENAME,
    MAX_DEGRADATION_REASONS,
    FeedbackAppendResult,
    FeedbackEvidence,
    FeedbackLabel,
    FeedbackLabelSchemaError,
    FeedbackRecordResult,
    LearnLabelRecordResult,
    feedback_state,
    optional_feedback_text,
    hash_operator_identifier,
)
from parking_spot_monitor.operator_feedback_replies import (
    bounded_feedback_reply as _bounded_feedback_reply,
    format_correction_reply,
    format_duplicate_correction_reply,
    format_duplicate_learn_reply,
    format_learn_reply,
)
from parking_spot_monitor.operator_feedback_store import append_feedback_label, find_feedback_label_by_matrix_event_id, load_feedback_labels
from parking_spot_monitor.operator_timeline import nearest_timeline_frame, parse_incident_time


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
            reported_state = feedback_state(existing_label.reported_state, "reported_state")
            stored_actual_state = feedback_state(existing_label.actual_state, "actual_state")
            return FeedbackRecordResult(
                recorded=True,
                reply_text=format_duplicate_correction_reply(existing_label.spot_id, reported_state, stored_actual_state, existing_label.evidence),
                spot_id=existing_label.spot_id,
                actual_state=stored_actual_state,
                reported_state=reported_state,
                evidence=existing_label.evidence,
                label_id=existing_label.label_id,
            )

        state = feedback_state(actual_state, "actual_state")
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
            state = feedback_state(target_state, "target_state")
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
            existing_state = feedback_state(existing_label.target_state or existing_label.actual_state, "target_state")
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
            text = optional_feedback_text(value, limit=120)
            if text and text not in reasons:
                reasons.append(text)
    return reasons[:MAX_DEGRADATION_REASONS]
