from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import redact_diagnostic_text
from parking_spot_monitor.operator_feedback_models import (
    FeedbackEvidence,
    FeedbackLabel,
    FeedbackLabelSchemaError,
    feedback_state,
    hash_operator_identifier,
    optional_feedback_text,
    required_feedback_text,
)


_SAFE_ID_PATTERN = re.compile(r"[^A-Za-z0-9_.:-]+")


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
    safe_spot = _SAFE_ID_PATTERN.sub("_", required_feedback_text(spot_id, "spot_id", limit=80)).strip("_") or "spot"
    event_id = optional_feedback_text(matrix_event_id, limit=180) or ""
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

    safe_spot = safe_spot_id(spot_id)
    if safe_spot is None:
        raise FeedbackLabelSchemaError("feedback label spot_id is required")
    state = feedback_state(target_state, "target_state")
    learned_text = feedback_timestamp_text(learned_at)
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


def safe_spot_id(value: str) -> str | None:
    try:
        text = required_feedback_text(value, "spot_id", limit=80)
    except FeedbackLabelSchemaError:
        return None
    if text.startswith("/") or "\\" in text or ".." in Path(text).parts:
        return None
    return text


def feedback_timestamp_text(value: datetime | str | None) -> str:
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
