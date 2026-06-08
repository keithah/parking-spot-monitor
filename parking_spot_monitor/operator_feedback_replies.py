from __future__ import annotations

from collections.abc import Sequence

from parking_spot_monitor.logging import redact_diagnostic_text
from parking_spot_monitor.operator_feedback_models import (
    MAX_DEGRADATION_REASONS,
    MAX_REPLAY_CONTEXT_LINES,
    MAX_REPLAY_LINE_CHARS,
    FeedbackEvidence,
    FeedbackLabel,
    clip_feedback_text,
    safe_feedback_text_list,
)


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
    safe_reasons = safe_feedback_text_list(degradation_reasons, max_items=MAX_DEGRADATION_REASONS, item_limit=120)
    if safe_reasons:
        replay_line += "; degraded " + ", ".join(safe_reasons[:MAX_DEGRADATION_REASONS])
    return bounded_feedback_reply([
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


def bounded_feedback_reply(lines: Sequence[str]) -> str:
    rendered = redact_diagnostic_text("\n".join(clip_feedback_text(line, MAX_REPLAY_LINE_CHARS) for line in lines[:MAX_REPLAY_CONTEXT_LINES]))
    encoded = rendered.encode("utf-8")
    if len(encoded) <= 4096:
        return rendered
    return encoded[:4093].decode("utf-8", errors="ignore") + "..."
