from __future__ import annotations

import json
import heapq
import re
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy_analytics import analyze_occupancy
from parking_spot_monitor.operator_cockpit_shared import (
    MAX_FILE_BYTES,
    MAX_LINES_PER_SECTION,
    bounded_reply,
    log_load_problem,
    mapping_value,
    text_value,
    utc_now,
)


def format_operator_analytics_reply(
    *,
    data_dir: str | Path,
    window: str = "7d",
    now: datetime | None = None,
    detector: Any | None = None,
    health_path: str | Path | None = None,
    state_path: str | Path | None = None,
    logger: StructuredLogger | None = None,
) -> str:
    """Format bounded spot-level historical occupancy analytics from local vehicle-history JSON only."""

    del detector, health_path, state_path
    observed_now = utc_now(now)
    root = Path(data_dir)
    closed_sessions, closed_invalid = _load_vehicle_history_session_dicts(root / "vehicle-history" / "sessions" / "closed", logger=logger)
    active_sessions, active_invalid = _load_vehicle_history_session_dicts(root / "vehicle-history" / "sessions" / "active", logger=logger)
    invalid_placeholders = [{} for _ in range(closed_invalid + active_invalid)]
    try:
        result = analyze_occupancy(
            [*closed_sessions, *invalid_placeholders],
            active_sessions=active_sessions,
            window=window,
            now=observed_now,
        )
    except ValueError:
        safe_window = text_value(window, default="unknown")
        return bounded_reply([
            "Parking occupancy analytics unavailable",
            f"Window: {safe_window}",
            "Unsupported window; use today, 7d, 30d, or all.",
            "No detector, camera, Matrix media upload, alert emission, or state mutation was run.",
        ])

    lines = [
        "Parking occupancy analytics",
        f"Window: {result.window.label}",
        f"Range: {result.window.started_at or 'beginning'} → {result.window.ended_at}",
        "Source: local vehicle-history sessions; sparse local history can make these metrics noisy.",
        "",
        "Totals",
        f"- Sessions: {result.session_count}",
        f"- Closed: {result.closed_session_count}",
        f"- Active: {result.active_session_count}",
        f"- Currently occupied spots: {result.current_occupied_spot_count}",
    ]

    spot_labels = {metric.spot_id: _analytics_spot_label(metric.spot_id) for metric in result.spots.values()}
    if result.spots:
        lines.extend(["", "Spots"])
        for index, metric in enumerate(list(result.spots.values())[:MAX_LINES_PER_SECTION]):
            if index:
                lines.append("")
            state = "occupied" if metric.currently_occupied else "open"
            dwell = _duration_label(metric.average_dwell_seconds) if metric.average_dwell_seconds is not None else "n/a"
            longest = _duration_label(metric.longest_session_seconds) if metric.longest_session_seconds is not None else "n/a"
            lines.extend([
                spot_labels.get(metric.spot_id, "unknown_spot"),
                f"- Sessions: {metric.session_count}",
                f"- Active: {metric.active_session_count}",
                f"- Status: {state}",
                f"- Occupied: {_duration_label(metric.occupied_duration_seconds)}",
                f"- Average dwell: {dwell}",
                f"- Longest dwell: {longest}",
            ])
    else:
        lines.extend(["", "No vehicle-history sessions overlap the selected window."])

    diagnostics = [*result.diagnostics]
    for metric in result.spots.values():
        diagnostics.extend(metric.diagnostics)
    if diagnostics:
        lines.append("Caveats:")
        seen: set[str] = set()
        for diagnostic in diagnostics[:MAX_LINES_PER_SECTION]:
            message = _analytics_diagnostic_message(mapping_value(diagnostic).get("message"), spot_labels)
            if message in seen:
                continue
            seen.add(message)
            lines.append(f"- {message}")
    if closed_invalid + active_invalid:
        count = closed_invalid + active_invalid
        noun = "session" if count == 1 else "sessions"
        lines.append(f"- {count} malformed vehicle-history {noun} ignored during local archive scan.")

    lines.extend([
        "",
        "Read-only",
        "Scanned local vehicle-history JSON only. No detector, camera, Matrix media upload, alert emission, or state mutation was run.",
    ])
    return bounded_reply(lines)


_SAFE_ANALYTICS_SPOT_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")


def _analytics_spot_label(value: object) -> str:
    text = text_value(value, default="unknown_spot")[:80]
    if not _SAFE_ANALYTICS_SPOT_RE.fullmatch(text):
        return "unknown_spot"
    if ".." in text:
        return "unknown_spot"
    return text


def _analytics_diagnostic_message(value: object, spot_labels: Mapping[str, str]) -> str:
    message = text_value(value, default="analytics caveat unavailable")
    for raw, safe in sorted(spot_labels.items(), key=lambda item: len(item[0]), reverse=True):
        if raw and raw != safe:
            message = message.replace(raw, safe)
    return message


def _load_vehicle_history_session_dicts(directory: Path, *, logger: StructuredLogger | None) -> tuple[list[Mapping[str, Any]], int]:
    limit = MAX_LINES_PER_SECTION * 20
    try:
        candidates = (path for path in directory.glob("*.json") if path.is_file())
        paths = heapq.nlargest(limit + 1, candidates, key=_safe_mtime_ns)
    except OSError as exc:
        log_load_problem(logger, label="vehicle_history", reason="scan_error", error_type=exc.__class__.__name__)
        return [], 1
    records: list[Mapping[str, Any]] = []
    invalid_count = 0
    for path in paths[:limit]:
        try:
            if path.stat().st_size > MAX_FILE_BYTES:
                invalid_count += 1
                continue
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            invalid_count += 1
            continue
        if isinstance(payload, Mapping):
            records.append(dict(payload))
        else:
            invalid_count += 1
    if len(paths) > limit:
        invalid_count += 1
    return records, invalid_count


def _safe_mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return -1


def _duration_label(seconds: int | None) -> str:
    if seconds is None:
        return "n/a"
    remaining = max(0, int(seconds))
    if remaining < 60:
        return f"{remaining}s"
    minutes = remaining // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 48:
        extra_minutes = minutes % 60
        return f"{hours}h {extra_minutes}m" if extra_minutes else f"{hours}h"
    days = hours // 24
    extra_hours = hours % 24
    return f"{days}d {extra_hours}h" if extra_hours else f"{days}d"
