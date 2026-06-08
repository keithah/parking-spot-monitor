from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.operator_cockpit_shared import MAX_LINES_PER_SECTION, int_value, log_load_problem, mapping_value, text_value


def matrix_outbox_status_lines(matrix_outbox_path: str | Path | None, *, logger: StructuredLogger | None) -> list[str]:
    """Return concise, redacted Matrix outbox lines from LocalOutbox summary fields only."""

    if matrix_outbox_path is None:
        return ["- outbox status unavailable (path not configured)."]
    path = Path(matrix_outbox_path)
    try:
        summary = LocalOutbox(path).status_summary()
    except FileNotFoundError:
        log_load_problem(logger, label="matrix_outbox", reason="missing", error_type="missing")
        return ["- outbox empty (file missing)."]
    except Exception as exc:
        error_type = redact_diagnostic_text(exc.__class__.__name__)
        log_load_problem(logger, label="matrix_outbox", reason="summary_error", error_type=error_type)
        return [f"- outbox status unavailable ({error_type})."]
    return _format_matrix_outbox_summary_lines(summary)


def _format_matrix_outbox_summary_lines(summary: Mapping[str, Any]) -> list[str]:
    total = int_value(summary.get("total"))
    counts = mapping_value(summary.get("counts_by_state"))
    state_order = ("pending", "retrying", "delivered", "failed", "dead_lettered")
    if total == 0:
        lines = ["- outbox empty."]
    else:
        count_text = ", ".join(f"{state.replace('_', '-')} {int_value(counts.get(state))}" for state in state_order)
        lines = [f"- outbox total {total}: {count_text}."]
        phase_counts = _outbox_phase_counts(summary)
        if phase_counts:
            lines.append("- phase states: " + "; ".join(_format_phase_count(phase, states) for phase, states in phase_counts[:6]) + ".")
        retry_reasons = _reason_count_line("retry reasons", summary.get("retry_reason_counts"))
        if retry_reasons is not None:
            lines.append(retry_reasons)
        dead_reasons = _reason_count_line("dead-letter reasons", summary.get("dead_letter_reason_counts"))
        if dead_reasons is not None:
            lines.append(dead_reasons)
        lines.extend(_outbox_item_lines(summary))

    recovery = mapping_value(summary.get("recovery"))
    quarantined = int_value(recovery.get("quarantined_count"))
    recovered = int_value(recovery.get("recovered_count"))
    if quarantined or recovered:
        reasons = mapping_value(recovery.get("reason_counts"))
        reason_text = _format_reason_counts(reasons) if reasons else "none"
        lines.append(f"- recovery: recovered {recovered}; quarantined {quarantined}; reasons {reason_text}.")
    return lines[:MAX_LINES_PER_SECTION]


def _outbox_phase_counts(summary: Mapping[str, Any]) -> list[tuple[str, dict[str, int]]]:
    counts: dict[str, dict[str, int]] = {}
    items = summary.get("items")
    if not isinstance(items, list):
        return []
    for item in items[:MAX_LINES_PER_SECTION]:
        if not isinstance(item, Mapping):
            continue
        phases = item.get("phases")
        if not isinstance(phases, list):
            continue
        for phase_item in phases[:6]:
            phase_map = mapping_value(phase_item)
            phase = text_value(phase_map.get("phase"), default="unknown")
            state = text_value(phase_map.get("state"), default="unknown")
            if phase == "unknown" or state == "unknown":
                continue
            state_counts = counts.setdefault(phase, {})
            state_counts[state] = state_counts.get(state, 0) + 1
    return sorted(counts.items())


def _format_phase_count(phase: str, states: Mapping[str, int]) -> str:
    state_order = ("pending", "delivered", "failed")
    rendered = ", ".join(f"{state} {int_value(states.get(state))}" for state in state_order if int_value(states.get(state)))
    return f"{text_value(phase)} {rendered or 'none'}"


def _reason_count_line(label: str, value: Any) -> str | None:
    counts = mapping_value(value)
    if not counts:
        return None
    return f"- {label}: {_format_reason_counts(counts)}."


def _format_reason_counts(counts: Mapping[str, Any]) -> str:
    return ", ".join(f"{text_value(key)} {int_value(value)}" for key, value in list(counts.items())[:8]) or "none"


def _outbox_item_lines(summary: Mapping[str, Any]) -> list[str]:
    items = summary.get("items")
    if not isinstance(items, list):
        return []
    lines: list[str] = []
    for item in items[: min(5, MAX_LINES_PER_SECTION)]:
        if not isinstance(item, Mapping):
            continue
        state = text_value(item.get("state"))
        phase = text_value(item.get("phase"))
        parts = [f"state {state}", f"phase {phase}"]
        retry_reason = text_value(item.get("retry_reason"), default="")
        dead_reason = text_value(item.get("dead_letter_reason"), default="")
        if retry_reason:
            parts.append(f"retry {retry_reason}")
        if dead_reason:
            parts.append(f"dead-letter {dead_reason}")
        phase_states = []
        phases = item.get("phases")
        if isinstance(phases, list):
            for phase_item in phases[:3]:
                phase_map = mapping_value(phase_item)
                phase_states.append(f"{text_value(phase_map.get('phase'))}={text_value(phase_map.get('state'))}")
        if phase_states:
            parts.append("phases " + ", ".join(phase_states))
        lines.append("- record: " + "; ".join(parts) + ".")
    if len(items) > len(lines):
        lines.append(f"- records truncated: showing {len(lines)} of {len(items)}.")
    return lines
