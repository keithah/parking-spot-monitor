from __future__ import annotations

import json
import os
import tempfile
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from parking_spot_monitor.decision_memory_publication import (
    ConditionalPublication,
    MAX_CONFLICT_FILES,
    SourceSignature,
    clear_decision_memory_conflict,
    decision_memory_conflict_files,
    link_exclusive,
    publish_decision_memory_bytes,
    publish_decision_memory_conflict_bytes,
    read_decision_memory_source,
    rename_exchange,
)
from parking_spot_monitor.diagnostic_bounding import take_bounded
from parking_spot_monitor.logging import (
    StructuredLogger,
    is_secret_diagnostic_value,
    redact_diagnostic_text,
)

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
    "feedback",
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
    "feedback",
}

_SAFE_DETAIL_KEYS = (
    "status",
    "previous_status",
    "new_status",
    "candidate_id",
    "class_name",
    "confidence",
    "threshold",
    "rejection_reason",
    "bbox_area_px",
    "overlap_ratio",
    "snapshot_ref",
    "snapshot_path",
    "retained_snapshot_path",
    "suppression_until",
    "alert_channel",
    "event_type",
    "event_id",
    "feedback_label",
    "label_id",
    "label_type",
    "reported_state",
    "actual_state",
    "target_state",
    "evidence_available",
    "evidence_error_type",
    "evidence_path",
    "replay_line_count",
    "degradation_reasons",
    "feedback_category",
    "feedback_category_details",
    "hit_streak",
    "miss_streak",
    "reason",
    "alert",
    "outcome",
    "error_type",
    "suppressed_reason",
    "quiet_window_active",
)

_SAFE_LAB_DETAIL_KEYS = (
    "job_id",
    "kind",
    "status",
    "phase",
    "created_at",
    "updated_at",
    "report_path",
    "status_counts",
    "coverage",
    "decision",
    "metric_delta_totals",
    "shared_threshold_sufficiency",
    "redaction",
    "missing_inputs",
    "error_code",
    "error_message",
)

_MAX_FORMAT_DEPTH = 3
_MAX_FORMAT_ITEMS = 6
_MAX_FORMAT_TEXT_CHARS = 160
_LEGACY_APPEND_ATTEMPTS = 2
_MEMORY_WRITE_LOCK = threading.RLock()
_UNCONDITIONAL_WRITE = object()
_conditional_exchange = rename_exchange
_conditional_link = link_exclusive


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
    source_signature: SourceSignature | None = None
    conflict_signatures: tuple[tuple[Path, SourceSignature], ...] = ()


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

    return append_decision_memory_records(
        path,
        (record,),
        max_records=max_records,
        max_file_bytes=max_file_bytes,
        logger=logger,
    )


def append_decision_memory_records(
    path: str | Path,
    records: Sequence[DecisionMemoryRecord | Mapping[str, Any]],
    *,
    max_records: int = MAX_RECORDS,
    max_file_bytes: int = MAX_MEMORY_FILE_BYTES,
    logger: StructuredLogger | None = None,
) -> bool:
    """Append sanitized records atomically with bounded retention; failures are non-fatal."""

    memory_path = Path(path)
    try:
        sanitized = [_record_from_any(record) for record in records]
        if not sanitized:
            return True
        from parking_spot_monitor.decision_memory_store import DecisionMemoryStore

        store = DecisionMemoryStore(
            memory_path,
            checkpoint_interval_seconds=300,
            checkpoint_max_pending_records=max(1, len(sanitized)),
            max_records=_positive_limit(max_records, MAX_RECORDS),
            max_file_bytes=min(
                _positive_limit(max_file_bytes, MAX_MEMORY_FILE_BYTES),
                MAX_MEMORY_FILE_BYTES,
            ),
            logger=logger,
        )
        persisted = store.extend(sanitized, durability="immediate")
        for _attempt in range(_LEGACY_APPEND_ATTEMPTS - 1):
            if persisted:
                break
            persisted = store.flush()
        if not persisted:
            return False
        retained = store.records
    except Exception as exc:
        _log(logger, "warning", "operator-decision-memory-append-failed", path=memory_path, error_type=type(exc).__name__, error=str(exc))
        return False

    _log(
        logger,
        "debug",
        "operator-decision-memory-appended",
        path=memory_path,
        record_count=len(retained),
        kind=sanitized[-1].kind,
    )
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
    raw: bytes | None
    source_signature: SourceSignature | None
    try:
        raw, source_signature = read_decision_memory_source(memory_path, max_file_bytes)
    except FileNotFoundError:
        raw = None
        source_signature = None
    except OverflowError:
        quarantined = _quarantine_file(memory_path)
        _log(logger, "warning", "operator-decision-memory-quarantined", path=memory_path, quarantine_path=quarantined, phase="size", error_type="oversized")
        return DecisionMemoryLoad(state="unavailable", error_type="oversized", quarantined_path=quarantined)
    except OSError as exc:
        _log(logger, "warning", "operator-decision-memory-load-failed", path=path, phase="read", error_type=type(exc).__name__, error=str(exc))
        return DecisionMemoryLoad(state="unavailable", error_type=type(exc).__name__)
    records: list[DecisionMemoryRecord] = []
    if raw is not None:
        try:
            payload = json.loads(raw.decode("utf-8"))
            records = _records_from_payload(payload)
        except (UnicodeError, json.JSONDecodeError, DecisionMemorySchemaError) as exc:
            quarantined = _quarantine_file(memory_path)
            _log(logger, "warning", "operator-decision-memory-quarantined", path=memory_path, quarantine_path=quarantined, phase="load", error_type=type(exc).__name__, error=str(exc))
            return DecisionMemoryLoad(state="unavailable", error_type=type(exc).__name__, quarantined_path=quarantined)

    try:
        conflicts = decision_memory_conflict_files(memory_path)
        conflict_payloads: list[
            tuple[Path, SourceSignature, list[DecisionMemoryRecord]]
        ] = []
        for conflict in conflicts:
            conflict_raw, conflict_signature = read_decision_memory_source(
                conflict, max_file_bytes
            )
            conflict_payload = json.loads(conflict_raw.decode("utf-8"))
            conflict_payloads.append(
                (
                    conflict,
                    conflict_signature,
                    _records_from_payload(conflict_payload),
                )
            )
    except (OSError, OverflowError, UnicodeError, json.JSONDecodeError, DecisionMemorySchemaError) as exc:
        _log(
            logger,
            "warning",
            "operator-decision-memory-conflict-load-failed",
            path=memory_path,
            error_type=type(exc).__name__,
        )
        return DecisionMemoryLoad(state="unavailable", error_type=type(exc).__name__)

    source_payloads = list(conflict_payloads)
    if source_signature is not None:
        source_payloads.append((memory_path, source_signature, records))
    source_payloads.sort(key=lambda item: (item[1][4], item[0].name))
    merged_records = [
        record for _path, _signature, items in source_payloads for record in items
    ]
    conflict_signatures = [
        (conflict, signature) for conflict, signature, _items in conflict_payloads
    ]
    if raw is None and not conflict_signatures:
        _log(logger, "debug", "operator-decision-memory-load-missing", path=path)
        return DecisionMemoryLoad(state="missing")

    bounded = _deduplicated_records(
        merged_records,
        max_records=_positive_limit(max_records, MAX_RECORDS),
    )
    _log(logger, "debug", "operator-decision-memory-loaded", path=memory_path, record_count=len(bounded), state="available")
    return DecisionMemoryLoad(
        state="available",
        records=bounded,
        source_signature=source_signature,
        conflict_signatures=tuple(conflict_signatures),
    )


def clear_decision_memory_conflicts(
    conflicts: Sequence[tuple[Path, SourceSignature]],
    *,
    max_file_bytes: int = MAX_MEMORY_FILE_BYTES,
) -> bool:
    cleared = True
    for conflict, signature in conflicts:
        try:
            cleared = clear_decision_memory_conflict(
                conflict,
                signature,
                max_file_bytes,
            ) and cleared
        except OSError:
            cleared = False
    return cleared


def compact_decision_memory_conflicts(
    path: Path,
    records: Sequence[DecisionMemoryRecord],
    conflicts: Sequence[tuple[Path, SourceSignature]],
    *,
    max_records: int = MAX_RECORDS,
    max_file_bytes: int = MAX_MEMORY_FILE_BYTES,
) -> tuple[tuple[Path, SourceSignature], ...]:
    if len(conflicts) < max(2, MAX_CONFLICT_FILES // 2):
        return tuple(conflicts)
    _bounded, encoded = _bounded_memory_payload(
        records,
        max_records=max_records,
        max_file_bytes=max_file_bytes,
    )
    replacement, signature = publish_decision_memory_conflict_bytes(
        path,
        encoded,
        max_file_bytes=max_file_bytes,
        replace=conflicts[0][0],
    )
    remaining: list[tuple[Path, SourceSignature]] = [(replacement, signature)]
    for conflict, conflict_signature in conflicts[1:]:
        try:
            if not clear_decision_memory_conflict(
                conflict, conflict_signature, max_file_bytes
            ):
                remaining.append((conflict, conflict_signature))
        except OSError:
            remaining.append((conflict, conflict_signature))
    return tuple(remaining)


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


def _write_memory(
    path: Path,
    records: Sequence[DecisionMemoryRecord],
    *,
    expected_signature: SourceSignature | None | object = _UNCONDITIONAL_WRITE,
    max_file_bytes: int = MAX_MEMORY_FILE_BYTES,
) -> ConditionalPublication | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _encode_memory_payload(records)
    if len(encoded) > max_file_bytes:
        raise OverflowError("decision memory publication exceeds byte limit")
    if expected_signature is not _UNCONDITIONAL_WRITE:
        return publish_decision_memory_bytes(
            path,
            encoded,
            expected_signature=expected_signature,  # type: ignore[arg-type]
            max_file_bytes=max_file_bytes,
            exchange=_conditional_exchange,
            exclusive_link=_conditional_link,
        )
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("wb", delete=False, dir=path.parent, prefix=f".{path.name}.", suffix=".tmp") as handle:
            temp_path = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fchmod(handle.fileno(), 0o644)
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise
    return None


def _encode_memory_payload(records: Sequence[DecisionMemoryRecord]) -> bytes:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "records": [record.to_json_dict() for record in records],
    }
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _bounded_memory_payload(
    records: Sequence[DecisionMemoryRecord],
    *,
    max_records: int,
    max_file_bytes: int,
) -> tuple[tuple[DecisionMemoryRecord, ...], bytes]:
    retained = tuple(records)[-_positive_limit(max_records, MAX_RECORDS) :]
    encoded = _encode_memory_payload(retained)
    if len(encoded) <= max_file_bytes:
        return retained, encoded
    if retained and len(_encode_memory_payload(retained[-1:])) > max_file_bytes:
        raise OverflowError("decision memory record exceeds byte limit")
    if not retained:
        raise OverflowError("decision memory byte limit cannot contain schema")
    low = 1
    high = len(retained) - 1
    while low < high:
        removed = (low + high) // 2
        candidate = retained[removed:]
        if len(_encode_memory_payload(candidate)) <= max_file_bytes:
            high = removed
        else:
            low = removed + 1
    retained = retained[low:]
    encoded = _encode_memory_payload(retained)
    if len(encoded) > max_file_bytes:
        raise OverflowError("decision memory publication exceeds byte limit")
    return retained, encoded


def _fsync_directory(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    file_descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(file_descriptor)
    finally:
        os.close(file_descriptor)


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


def _deduplicated_records(
    records: Sequence[DecisionMemoryRecord],
    *,
    max_records: int,
) -> tuple[DecisionMemoryRecord, ...]:
    unique: dict[str, DecisionMemoryRecord] = {}
    for record in records:
        key = json.dumps(
            record.to_json_dict(),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        unique[key] = record
    ordered = sorted(unique.values(), key=_record_observation_key)
    return tuple(ordered[-max_records:])


def _record_observation_key(record: DecisionMemoryRecord) -> tuple[int, float, str]:
    try:
        parsed = datetime.fromisoformat(record.observed_at.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return 1, parsed.timestamp(), record.observed_at
    except (OverflowError, ValueError):
        return 0, 0.0, record.observed_at


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
    sanitized = _bound_value(value, depth=0)
    return sanitized if isinstance(sanitized, Mapping) else {}


def _bound_value(value: Any, *, depth: int) -> Any:
    if depth >= 4:
        return "<truncated>"
    if isinstance(value, Mapping):
        bounded: dict[str, Any] = {}
        entries, truncated = take_bounded(value.items(), MAX_MAPPING_ITEMS)
        for key, item in entries:
            bounded[_clip_text(key, 80)] = (
                "<redacted>"
                if is_secret_diagnostic_value(key, item)
                else _bound_value(item, depth=depth + 1)
            )
        if truncated:
            bounded["truncated"] = True
        return bounded
    if (isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray)) or isinstance(value, set | frozenset):
        items, truncated = take_bounded(value, MAX_SEQUENCE_ITEMS)
        bounded_items = [_bound_value(item, depth=depth + 1) for item in items]
        if truncated:
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
    for key in _safe_detail_keys_for_record(record):
        if key in details:
            lines.append(f"  {key}: {_format_detail_value(details[key])}")
    return lines


def _safe_detail_keys_for_record(record: DecisionMemoryRecord) -> tuple[str, ...]:
    if record.kind == "lab_outcome":
        return _SAFE_LAB_DETAIL_KEYS
    return _SAFE_DETAIL_KEYS


def _format_detail_value(value: Any, *, depth: int = 0) -> str:
    """Return one redacted, bounded line fragment for a whitelisted detail value."""

    if depth >= _MAX_FORMAT_DEPTH:
        return "<truncated>"
    if isinstance(value, Mapping):
        parts: list[str] = []
        entries, truncated = take_bounded(value.items(), _MAX_FORMAT_ITEMS)
        for key, item in entries:
            safe_key = _clip_text(key, 48)
            parts.append(f"{safe_key}={_format_detail_value(item, depth=depth + 1)}")
        if truncated:
            parts.append("...")
        return _clip_text("; ".join(parts), _MAX_FORMAT_TEXT_CHARS)
    if isinstance(value, list | tuple | set | frozenset):
        items, truncated = take_bounded(value, _MAX_FORMAT_ITEMS)
        rendered = [_format_detail_value(item, depth=depth + 1) for item in items]
        if truncated:
            rendered.append("...")
        return _clip_text(", ".join(rendered), _MAX_FORMAT_TEXT_CHARS)
    if isinstance(value, bytes | bytearray):
        return "<binary redacted>"
    return _clip_text(value, _MAX_FORMAT_TEXT_CHARS)


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
