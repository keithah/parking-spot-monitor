from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value

SCHEMA_VERSION = 1
DECISION_MEMORY_FILENAME = "operator-decision-memory.json"
MAX_MEMORY_FILE_BYTES = 256_000
MAX_RECORDS = 200
MAX_RECENT_RECORDS = 12
MAX_WHY_RECORDS = 6
MAX_REPLY_BYTES = 4096
MAX_TEXT_FIELD_CHARS = 500
MAX_SEQUENCE_ITEMS = 12
MAX_MAPPING_ITEMS = 24

RecordKind = Literal[
    "accepted_evidence",
    "rejected_evidence",
    "miss",
    "confidence_dip",
    "suppression",
    "alert",
    "command_outcome",
    "lab_outcome",
]
LoadState = Literal["available", "missing", "unavailable", "partial"]

_SUPPORTED_KINDS = {
    "accepted_evidence",
    "rejected_evidence",
    "miss",
    "confidence_dip",
    "suppression",
    "alert",
    "command_outcome",
    "lab_outcome",
}


class DecisionMemorySchemaError(ValueError):
    """Raised when persisted operator decision memory is not supported."""


@dataclass(frozen=True)
class DecisionMemoryRecord:
    """Sanitized, schema-stable operator decision memory entry."""

    kind: str
    observed_at: str
    spot_id: str | None = None
    summary: str = ""
    details: Mapping[str, Any] | None = None

    def to_json_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "observed_at": self.observed_at,
            "summary": self.summary,
        }
        if self.spot_id:
            payload["spot_id"] = self.spot_id
        if self.details:
            payload["details"] = dict(self.details)
        return payload


@dataclass(frozen=True)
class DecisionMemoryLoad:
    """Bounded load result for callers that must render safe unavailable states."""

    state: LoadState
    records: tuple[DecisionMemoryRecord, ...] = ()
    error_type: str | None = None
    quarantined_path: Path | None = None


def decision_memory_path(data_dir: str | Path) -> Path:
    """Return the bounded operator decision-memory artifact path for a runtime data directory."""

    return Path(data_dir) / DECISION_MEMORY_FILENAME


def make_decision_memory_record(
    kind: RecordKind | str,
    *,
    observed_at: datetime | str | None = None,
    spot_id: str | None = None,
    summary: object = "",
    details: Mapping[str, Any] | None = None,
) -> DecisionMemoryRecord:
    """Create a redacted record, clipping nested diagnostic detail before persistence."""

    kind_text = redact_diagnostic_text(kind)[:80]
    if kind_text not in _SUPPORTED_KINDS:
        kind_text = "command_outcome"
    return DecisionMemoryRecord(
        kind=kind_text,
        observed_at=_observed_at_text(observed_at),
        spot_id=_safe_spot_id(spot_id),
        summary=_clip_text(summary, MAX_TEXT_FIELD_CHARS),
        details=_sanitize_details(details) if details else None,
    )


def append_decision_memory_record(
    path: str | Path,
    record: DecisionMemoryRecord | Mapping[str, Any],
    *,
    max_records: int = MAX_RECORDS,
    max_file_bytes: int = MAX_MEMORY_FILE_BYTES,
    logger: StructuredLogger | None = None,
) -> bool:
    """Append one sanitized record with bounded retention; failures are logged and non-fatal."""

    memory_path = Path(path)
    try:
        new_record = _record_from_any(record)
        loaded = load_decision_memory(memory_path, max_file_bytes=max_file_bytes, logger=logger)
        retained = list(loaded.records)
        retained.append(new_record)
        retained = retained[-_positive_limit(max_records, MAX_RECORDS) :]
        _write_memory(memory_path, retained)
    except Exception as exc:
        _log(logger, "warning", "operator-decision-memory-append-failed", path=memory_path, error_type=type(exc).__name__, error=str(exc))
        return False

    _log(logger, "debug", "operator-decision-memory-appended", path=memory_path, record_count=len(retained), kind=new_record.kind)
    return True


def load_decision_memory(
    path: str | Path,
    *,
    max_records: int = MAX_RECORDS,
    max_file_bytes: int = MAX_MEMORY_FILE_BYTES,
    logger: StructuredLogger | None = None,
) -> DecisionMemoryLoad:
    """Load a bounded tail of decision-memory records, quarantining unsafe artifacts."""

    memory_path = Path(path)
    if not memory_path.exists():
        _log(logger, "debug", "operator-decision-memory-load-missing", path=memory_path)
        return DecisionMemoryLoad(state="missing")

    try:
        size = memory_path.stat().st_size
    except OSError as exc:
        _log(logger, "warning", "operator-decision-memory-load-failed", path=memory_path, phase="stat", error_type=type(exc).__name__, error=str(exc))
        return DecisionMemoryLoad(state="unavailable", error_type=type(exc).__name__)

    if size > max_file_bytes:
        quarantined = _quarantine_file(memory_path)
        _log(logger, "warning", "operator-decision-memory-quarantined", path=memory_path, quarantine_path=quarantined, phase="size", error_type="oversized")
        return DecisionMemoryLoad(state="unavailable", error_type="oversized", quarantined_path=quarantined)

    try:
        with memory_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        records = _records_from_payload(payload)
    except (OSError, json.JSONDecodeError, DecisionMemorySchemaError) as exc:
        quarantined = _quarantine_file(memory_path)
        _log(logger, "warning", "operator-decision-memory-quarantined", path=memory_path, quarantine_path=quarantined, phase="load", error_type=type(exc).__name__, error=str(exc))
        return DecisionMemoryLoad(state="unavailable", error_type=type(exc).__name__, quarantined_path=quarantined)

    bounded = tuple(records[-_positive_limit(max_records, MAX_RECORDS) :])
    _log(logger, "debug", "operator-decision-memory-loaded", path=memory_path, record_count=len(bounded), state="available")
    return DecisionMemoryLoad(state="available", records=bounded)


def format_why_reply(
    path: str | Path,
    spot_id: str,
    *,
    max_records: int = MAX_WHY_RECORDS,
    max_reply_bytes: int = MAX_REPLY_BYTES,
    logger: StructuredLogger | None = None,
) -> str:
    """Format bounded recent decision evidence for one configured spot."""

    safe_spot = _safe_spot_id(spot_id)
    if not safe_spot:
        return "Parking decision memory unavailable\nInvalid spot id."

    loaded = load_decision_memory(path, logger=logger)
    heading = f"Parking decision memory for {safe_spot}"
    if loaded.state != "available":
        suffix = f" ({loaded.error_type})" if loaded.error_type else ""
        return _bounded_reply([heading, f"Decision memory unavailable{suffix}; no detector or camera work was run."], max_reply_bytes)

    matches = [record for record in loaded.records if record.spot_id == safe_spot]
    if not matches:
        return _bounded_reply([heading, "No recent decision memory for this spot."], max_reply_bytes)

    lines = [heading]
    for record in matches[-_positive_limit(max_records, MAX_WHY_RECORDS) :]:
        lines.extend(_format_record_lines(record, include_spot=False))
    return _bounded_reply(lines, max_reply_bytes)


def format_recent_reply(
    path: str | Path,
    *,
    max_records: int = MAX_RECENT_RECORDS,
    max_reply_bytes: int = MAX_REPLY_BYTES,
    logger: StructuredLogger | None = None,
) -> str:
    """Format a bounded timeline of recent operator decision-memory records."""

    loaded = load_decision_memory(path, logger=logger)
    if loaded.state != "available":
        suffix = f" ({loaded.error_type})" if loaded.error_type else ""
        return _bounded_reply(["Parking decision memory recent", f"Decision memory unavailable{suffix}; no detector or camera work was run."], max_reply_bytes)
    if not loaded.records:
        return "Parking decision memory recent\nNo recent decision memory."

    lines = ["Parking decision memory recent"]
    for record in loaded.records[-_positive_limit(max_records, MAX_RECENT_RECORDS) :]:
        lines.extend(_format_record_lines(record, include_spot=True))
    return _bounded_reply(lines, max_reply_bytes)


def _write_memory(path: Path, records: Sequence[DecisionMemoryRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": SCHEMA_VERSION, "records": [record.to_json_dict() for record in records]}
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


def _records_from_payload(payload: Any) -> list[DecisionMemoryRecord]:
    if not isinstance(payload, Mapping):
        raise DecisionMemorySchemaError("decision memory payload must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise DecisionMemorySchemaError("unsupported decision memory schema_version")
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raise DecisionMemorySchemaError("decision memory records must be a list")
    if len(raw_records) > MAX_RECORDS * 10:
        raise DecisionMemorySchemaError("decision memory record count exceeds validation bound")
    records: list[DecisionMemoryRecord] = []
    for item in raw_records:
        try:
            records.append(_record_from_any(item))
        except DecisionMemorySchemaError:
            continue
    return records


def _record_from_any(value: DecisionMemoryRecord | Mapping[str, Any]) -> DecisionMemoryRecord:
    if isinstance(value, DecisionMemoryRecord):
        return make_decision_memory_record(value.kind, observed_at=value.observed_at, spot_id=value.spot_id, summary=value.summary, details=value.details)
    if not isinstance(value, Mapping):
        raise DecisionMemorySchemaError("decision memory record must be an object")
    return make_decision_memory_record(
        str(value.get("kind", "command_outcome")),
        observed_at=value.get("observed_at"),
        spot_id=value.get("spot_id") if value.get("spot_id") is not None else None,
        summary=value.get("summary", ""),
        details=value.get("details") if isinstance(value.get("details"), Mapping) else None,
    )


def _sanitize_details(value: Mapping[str, Any]) -> Mapping[str, Any]:
    redacted = redact_diagnostic_value(value)
    sanitized = _bound_value(redacted, depth=0)
    return sanitized if isinstance(sanitized, Mapping) else {}


def _bound_value(value: Any, *, depth: int) -> Any:
    if depth >= 4:
        return "<truncated>"
    if isinstance(value, Mapping):
        bounded: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= MAX_MAPPING_ITEMS:
                bounded["truncated"] = True
                break
            bounded[_clip_text(key, 80)] = _bound_value(item, depth=depth + 1)
        return bounded
    if isinstance(value, list | tuple | set | frozenset):
        items = list(value)
        bounded_items = [_bound_value(item, depth=depth + 1) for item in items[:MAX_SEQUENCE_ITEMS]]
        if len(items) > len(bounded_items):
            bounded_items.append("<truncated>")
        return bounded_items
    if isinstance(value, bytes | bytearray):
        return "<binary redacted>"
    if isinstance(value, str):
        return _clip_text(value, MAX_TEXT_FIELD_CHARS)
    if isinstance(value, bool) or value is None or isinstance(value, int | float):
        return value
    return _clip_text(value, MAX_TEXT_FIELD_CHARS)


def _format_record_lines(record: DecisionMemoryRecord, *, include_spot: bool) -> list[str]:
    subject = f" {record.spot_id}" if include_spot and record.spot_id else ""
    lines = [f"- {record.observed_at} {record.kind}{subject}: {_clip_text(record.summary, 220)}"]
    details = record.details or {}
    keys = (
        "status",
        "previous_status",
        "new_status",
        "hit_streak",
        "miss_streak",
        "reason",
        "alert",
        "outcome",
        "error_type",
        "suppressed_reason",
    )
    if record.kind == "lab_outcome":
        keys = (
            "job_id",
            "kind",
            "status",
            "phase",
            "report_path",
            "status_counts",
            "coverage",
            "decision",
            "metric_delta_totals",
            "error_code",
            "error_message",
        )
    for key in keys:
        if key in details:
            lines.append(f"  {key}: {_clip_text(details[key], 160)}")
    return lines


def _bounded_reply(lines: Sequence[str], max_reply_bytes: int) -> str:
    rendered = redact_diagnostic_text("\n".join(redact_diagnostic_text(line) for line in lines[: MAX_RECENT_RECORDS * 4 + 2]))
    encoded = rendered.encode("utf-8")
    limit = _positive_limit(max_reply_bytes, MAX_REPLY_BYTES)
    if len(encoded) <= limit:
        return rendered
    return encoded[: max(0, limit - 3)].decode("utf-8", errors="ignore") + "..."


def _observed_at_text(value: datetime | str | None) -> str:
    if isinstance(value, datetime):
        selected = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return selected.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str) and value:
        return redact_diagnostic_text(value)[:80]
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_spot_id(value: str | None) -> str | None:
    if value is None:
        return None
    text = redact_diagnostic_text(value).strip()
    if not text or len(text) > 80 or any(part in text for part in ("/", "\\", "..")):
        return None
    return text


def _clip_text(value: object, limit: int) -> str:
    text = redact_diagnostic_text(value)
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    return encoded[: max(0, limit - 3)].decode("utf-8", errors="ignore") + "..."


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
    getattr(logger, level)(event, **fields)
