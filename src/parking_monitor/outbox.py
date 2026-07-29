"""Secret-safe durable Matrix alert outbox primitives.

The outbox boundary records an alert intent before any network delivery happens.
It stores only sanitized, JSON-serializable delivery data and derives stable Matrix
transaction IDs from logical alert identity so retries across restarts are
idempotent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import threading
from collections.abc import Callable, Sequence
from typing import Any, BinaryIO, Literal

OutboxState = Literal["pending", "retrying", "delivered", "failed", "dead_lettered"]
PhaseState = Literal["pending", "delivered", "failed"]
MatrixPhase = Literal["text", "upload", "image"]

_SCHEMA_VERSION = 1
_VALID_STATES: set[str] = {"pending", "retrying", "delivered", "failed", "dead_lettered"}
_TERMINAL_STATES: set[str] = {"delivered", "failed", "dead_lettered"}
_RETRYABLE_STATES: set[str] = {"pending", "retrying"}
_VALID_PHASES: set[str] = {"text", "upload", "image"}
_VALID_PHASE_STATES: set[str] = {"pending", "delivered", "failed"}
_MAX_QUARANTINE_FILES = 20
_MAX_OUTBOX_FILE_BYTES = 5_000_000
_SECRET_KEY_RE = re.compile(
    r"(access[_-]?token|authorization|auth[_-]?header|bearer|password|secret|cookie|rtsp|image[_-]?bytes|raw[_-]?image|exception|traceback|error[_-]?message)",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = re.compile(
    r"(rtsp://|authorization\s*:|bearer\s+[a-z0-9._~+/=-]+|access[_-]?token\s*[=:]|traceback \(most recent call last\)|exception:)",
    re.IGNORECASE,
)
_REASON_CODE_RE = re.compile(r"[^a-z0-9_.-]+")

JsonScalar = str | int | float | bool | None
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
PhaseResult = dict[str, JsonValue]


class OutboxError(Exception):
    """Base error for local outbox operations."""


class SecretBearingIntentError(OutboxError, ValueError):
    """Raised when an alert intent contains fields unsafe to persist."""


class OutboxPersistenceError(OutboxError, OSError):
    """Raised when a local outbox write cannot be made durable."""


class OutboxRecoveryError(OutboxError, ValueError):
    """Raised internally when persisted outbox data cannot be trusted."""


class OutboxTransitionError(OutboxError, ValueError):
    """Raised when a requested local state or phase transition is invalid."""


class _RecordValidationError(OutboxRecoveryError):
    """Raised when one persisted record cannot be safely interpreted."""


@dataclass(frozen=True)
class RecoveryEvent:
    """Secret-safe metadata for a recovered or quarantined persisted payload."""

    reason: str
    count: int = 1
    quarantine_path: str | None = None

    def to_json(self) -> dict[str, JsonValue]:
        payload: dict[str, JsonValue] = {"reason": self.reason, "count": self.count}
        if self.quarantine_path is not None:
            payload["quarantine_path"] = self.quarantine_path
        return payload


@dataclass(frozen=True)
class RecoveryResult:
    """Safe reload diagnostics exposed without raw corrupt payload contents."""

    recovered_count: int = 0
    quarantined_count: int = 0
    events: tuple[RecoveryEvent, ...] = ()

    @property
    def reason_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for event in self.events:
            counts[event.reason] = counts.get(event.reason, 0) + event.count
        return counts

    def with_event(self, event: RecoveryEvent) -> "RecoveryResult":
        return RecoveryResult(
            recovered_count=self.recovered_count,
            quarantined_count=self.quarantined_count + event.count,
            events=(*self.events, event),
        )

    def with_recovered_count(self, recovered_count: int) -> "RecoveryResult":
        return RecoveryResult(
            recovered_count=recovered_count,
            quarantined_count=self.quarantined_count,
            events=self.events,
        )

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "recovered_count": self.recovered_count,
            "quarantined_count": self.quarantined_count,
            "reason_counts": self.reason_counts,
            "events": [event.to_json() for event in self.events],
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_utc_timestamp(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


@dataclass(frozen=True)
class AlertIntent:
    """Matrix alert data safe to persist after validation.

    ``event_id`` identifies the logical parking event, while ``phase`` names the
    downstream Matrix delivery phase such as ``text`` or ``image``. ``room_id``
    may be persisted only when it is already part of the configured delivery
    intent. Secrets, diagnostic strings, RTSP URLs, auth material, and image
    bytes are rejected before any JSON is written.
    """

    event_id: str
    phase: str
    body: str
    room_id: str | None = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)

    def sanitized(self) -> "AlertIntent":
        event_id = _required_clean_string("event_id", self.event_id)
        phase = _required_clean_string("phase", self.phase)
        if phase not in _VALID_PHASES:
            raise ValueError("phase must be one of: image, text, upload")
        body = _required_clean_string("body", self.body)
        room_id = _optional_clean_string("room_id", self.room_id)
        metadata = _sanitize_mapping(self.metadata, path="metadata")
        return AlertIntent(
            event_id=event_id,
            phase=phase,
            room_id=room_id,
            body=body,
            metadata=metadata,
        )

    def to_json(self) -> dict[str, JsonValue]:
        sanitized = self.sanitized()
        payload: dict[str, JsonValue] = {
            "event_id": sanitized.event_id,
            "phase": sanitized.phase,
            "body": sanitized.body,
        }
        if sanitized.room_id is not None:
            payload["room_id"] = sanitized.room_id
        if sanitized.metadata:
            payload["metadata"] = sanitized.metadata
        return payload

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "AlertIntent":
        _require_mapping(payload, "intent")
        return cls(
            event_id=_require_string(payload, "event_id", path="intent"),
            phase=_require_string(payload, "phase", path="intent"),
            body=_require_string(payload, "body", path="intent"),
            room_id=_optional_string(payload, "room_id", path="intent"),
            metadata=_optional_mapping(payload, "metadata", path="intent"),
        ).sanitized()


@dataclass(frozen=True)
class OutboxRecord:
    """Persisted durable outbox item."""

    id: str
    transaction_id: str
    intent: AlertIntent
    state: OutboxState = "pending"
    created_at: str = field(default_factory=_utc_now)
    updated_at: str = field(default_factory=_utc_now)
    retry_reason: str | None = None
    dead_letter_reason: str | None = None
    phase_states: dict[str, PhaseState] = field(default_factory=dict)
    phase_updated_at: dict[str, str] = field(default_factory=dict)
    phase_results: dict[str, PhaseResult] = field(default_factory=dict)

    def __post_init__(self) -> None:
        phases = self.phase_states or {self.intent.phase: "pending"}
        normalized: dict[str, PhaseState] = {}
        for phase, state in phases.items():
            if phase not in _VALID_PHASES:
                raise _RecordValidationError("invalid_phase")
            if state not in _VALID_PHASE_STATES:
                raise _RecordValidationError("invalid_phase_state")
            normalized[phase] = state  # type: ignore[assignment]
        timestamps = {phase: self.phase_updated_at.get(phase, self.updated_at) for phase in normalized}
        results: dict[str, PhaseResult] = {}
        for phase, result in (self.phase_results or {}).items():
            if phase not in _VALID_PHASES or phase not in normalized:
                raise _RecordValidationError("invalid_phase_result")
            sanitized_result = _sanitize_phase_result(result, path=f"phase_results.{phase}")
            if sanitized_result:
                results[phase] = sanitized_result
        object.__setattr__(self, "phase_states", normalized)
        object.__setattr__(self, "phase_updated_at", timestamps)
        object.__setattr__(self, "phase_results", results)

    def to_json(self) -> dict[str, JsonValue]:
        payload: dict[str, JsonValue] = {
            "id": self.id,
            "matrix_transaction_id": self.transaction_id,
            "intent": self.intent.to_json(),
            "state": self.state,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "phases": [
                {
                    "phase": phase,
                    "state": self.phase_states[phase],
                    "updated_at": self.phase_updated_at.get(phase, self.updated_at),
                    **({"result": self.phase_results[phase]} if phase in self.phase_results else {}),
                }
                for phase in sorted(self.phase_states)
            ],
        }
        if self.retry_reason is not None:
            payload["retry_reason"] = self.retry_reason
        if self.dead_letter_reason is not None:
            payload["dead_letter_reason"] = self.dead_letter_reason
        return payload

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "OutboxRecord":
        try:
            _require_mapping(payload, "record")
            state = _optional_string(payload, "state", path="record") or "pending"
            if state not in _VALID_STATES:
                raise _RecordValidationError("invalid_state")
            intent = AlertIntent.from_json(_require_mapping(payload.get("intent"), "record.intent"))
            phase_states, phase_updated_at, phase_results = _parse_phase_payload(payload, fallback_phase=intent.phase)
            return cls(
                id=_require_string(payload, "id", path="record"),
                transaction_id=_require_string(payload, "matrix_transaction_id", path="record"),
                intent=intent,
                state=state,  # type: ignore[arg-type]
                created_at=_require_string(payload, "created_at", path="record"),
                updated_at=_require_string(payload, "updated_at", path="record"),
                retry_reason=_optional_reason(payload, "retry_reason", path="record"),
                dead_letter_reason=_optional_reason(payload, "dead_letter_reason", path="record"),
                phase_states=phase_states,
                phase_updated_at=phase_updated_at,
                phase_results=phase_results,
            )
        except SecretBearingIntentError as exc:
            raise _RecordValidationError("unsafe_record_content") from exc
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, _RecordValidationError):
                raise
            raise _RecordValidationError("malformed_record") from exc


@dataclass(frozen=True)
class OutboxRetentionPolicy:
    """Bounds local outbox growth while preserving retryable work first."""

    max_records: int | None = 1000
    max_terminal_age_seconds: int | None = None

    def __post_init__(self) -> None:
        if self.max_records is not None and self.max_records < 1:
            raise ValueError("max_records must be at least 1")
        if self.max_terminal_age_seconds is not None and self.max_terminal_age_seconds < 0:
            raise ValueError("max_terminal_age_seconds must not be negative")


class LocalOutbox:
    """JSON-file backed Matrix alert outbox."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        max_records: int | None = 1000,
        max_terminal_age_seconds: int | None = None,
        retention: OutboxRetentionPolicy | None = None,
    ) -> None:
        self.path = Path(path)
        self.retention = retention or OutboxRetentionPolicy(
            max_records=max_records,
            max_terminal_age_seconds=max_terminal_age_seconds,
        )
        self._lock: threading.RLock = threading.RLock()
        self._records: list[OutboxRecord] = []
        self._index_by_id: dict[str, int] = {}
        self.recovery = RecoveryResult()
        with self._lock:
            records, self.recovery = self._load_records()
            self._set_records(records)
            pruned = self._apply_retention(self._records)
            if pruned != self._records:
                self._persist_records(pruned)
                self._set_records(pruned)

    def enqueue(self, intent: AlertIntent) -> OutboxRecord:
        """Persist a pending item unless the same logical alert already exists."""
        sanitized = intent.sanitized()
        return self.enqueue_with_phases(sanitized, (sanitized.phase,))

    def enqueue_with_phases(
        self,
        intent: AlertIntent,
        phases: Sequence[MatrixPhase | str],
    ) -> OutboxRecord:
        """Persist a pending item with every requested delivery phase in one write."""
        sanitized = intent.sanitized()
        requested_phases = tuple(phases) or (sanitized.phase,)
        for phase in requested_phases:
            if phase not in _VALID_PHASES:
                raise OutboxTransitionError("unknown_phase")

        with self._lock:
            item_id = derive_outbox_item_id(sanitized)
            if item_id in self._index_by_id:
                return self._find_record(item_id)

            now = _utc_now()
            phase_states: dict[str, PhaseState] = {str(phase): "pending" for phase in requested_phases}
            phase_updated_at = {phase: now for phase in phase_states}
            record = OutboxRecord(
                id=item_id,
                transaction_id=derive_matrix_transaction_id(sanitized),
                intent=sanitized,
                state="pending",
                created_at=now,
                updated_at=now,
                phase_states=phase_states,
                phase_updated_at=phase_updated_at,
                phase_results={},
            )
            updated_records = self._apply_retention([*self._records, record])
            self._persist_records(updated_records)
            self._set_records(updated_records)
            return self._find_record(record.id)

    def list_records(self, state: OutboxState | None = None) -> list[OutboxRecord]:
        with self._lock:
            if state is None:
                return list(self._records)
            return [record for record in self._records if record.state == state]

    def list_pending(self) -> list[OutboxRecord]:
        return self.list_records("pending")

    def mark_retrying(self, record_id: str, *, reason: str) -> OutboxRecord:
        """Record a retryable local delivery failure using a safe reason code."""
        return self._transition_record(record_id, state="retrying", retry_reason=_safe_reason_code(reason))

    def mark_delivered(self, record_id: str) -> OutboxRecord:
        """Mark the whole outbox item delivered once its required phases are done."""
        return self._transition_record(record_id, state="delivered", retry_reason=None)

    def mark_failed(self, record_id: str, *, reason: str) -> OutboxRecord:
        """Mark a terminal non-retryable failure without retrying forever."""
        return self._transition_record(record_id, state="failed", dead_letter_reason=_safe_reason_code(reason))

    def mark_dead_lettered(self, record_id: str, *, reason: str) -> OutboxRecord:
        """Move a permanently failing record to the dead-letter state with a reason code."""
        return self._transition_record(record_id, state="dead_lettered", dead_letter_reason=_safe_reason_code(reason))

    def ensure_phase_pending(self, record_id: str, phase: MatrixPhase | str) -> OutboxRecord:
        """Idempotently add a pending Matrix delivery phase to an existing retryable record."""
        return self._transition_phase(record_id, phase=phase, phase_state="pending")

    def mark_phase_delivered(
        self,
        record_id: str,
        phase: MatrixPhase | str,
        *,
        result: dict[str, JsonValue] | None = None,
    ) -> OutboxRecord:
        """Idempotently mark one Matrix delivery phase complete with optional safe result metadata."""
        return self._transition_phase(record_id, phase=phase, phase_state="delivered", result=result)

    def mark_phase_failed(self, record_id: str, phase: MatrixPhase | str, *, reason: str) -> OutboxRecord:
        """Mark one Matrix delivery phase failed and dead-letter the record safely."""
        return self._transition_phase(
            record_id,
            phase=phase,
            phase_state="failed",
            dead_letter_reason=_safe_reason_code(reason),
        )

    def status_summary(self) -> dict[str, JsonValue]:
        return self._status_summary(include_items=True)

    def compact_status_summary(self) -> dict[str, JsonValue]:
        return self._status_summary(include_items=False)

    def _status_summary(self, *, include_items: bool) -> dict[str, JsonValue]:
        with self._lock:
            counts: dict[str, int] = {}
            retry_reason_counts: dict[str, int] = {}
            dead_letter_reason_counts: dict[str, int] = {}
            items: list[dict[str, JsonValue]] = []
            timestamps: list[str] = []
            for record in self._records:
                counts[record.state] = counts.get(record.state, 0) + 1
                timestamps.extend((record.created_at, record.updated_at))
                if record.retry_reason is not None:
                    retry_reason_counts[record.retry_reason] = retry_reason_counts.get(record.retry_reason, 0) + 1
                if record.dead_letter_reason is not None:
                    dead_letter_reason_counts[record.dead_letter_reason] = dead_letter_reason_counts.get(record.dead_letter_reason, 0) + 1
                if include_items:
                    items.append(self._status_item(record))
            summary: dict[str, JsonValue] = {
                "path": str(self.path),
                "schema_version": _SCHEMA_VERSION,
                "total": len(self._records),
                "counts_by_state": counts,
                "oldest_timestamp": min(timestamps) if timestamps else None,
                "newest_timestamp": max(timestamps) if timestamps else None,
                "retry_reason_counts": retry_reason_counts,
                "dead_letter_reason_counts": dead_letter_reason_counts,
                "recovery": self.recovery.to_json(),
            }
            if include_items:
                summary["items"] = items
            return summary

    def _status_item(self, record: OutboxRecord) -> dict[str, JsonValue]:
        return {
            "id": record.id,
            "matrix_transaction_id": record.transaction_id,
            "state": record.state,
            "phase": record.intent.phase,
            "phases": [
                {
                    "phase": phase,
                    "state": record.phase_states[phase],
                    "updated_at": record.phase_updated_at.get(phase, record.updated_at),
                    **({"result": record.phase_results[phase]} if phase in record.phase_results else {}),
                }
                for phase in sorted(record.phase_states)
            ],
            "retry_reason": record.retry_reason,
            "dead_letter_reason": record.dead_letter_reason,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }

    def _transition_record(
        self,
        record_id: str,
        *,
        state: OutboxState,
        retry_reason: str | None = None,
        dead_letter_reason: str | None = None,
    ) -> OutboxRecord:
        with self._lock:
            record = self._find_record(record_id)
            if record.state in _TERMINAL_STATES and state != record.state:
                if not (record.state == "failed" and state == "dead_lettered"):
                    raise OutboxTransitionError("terminal_record_cannot_transition")
            if state == "retrying" and record.state not in _RETRYABLE_STATES:
                raise OutboxTransitionError("terminal_record_cannot_retry")
            if record.state == state and retry_reason == record.retry_reason and dead_letter_reason == record.dead_letter_reason:
                return record
            now = _utc_now()
            updated = OutboxRecord(
                id=record.id,
                transaction_id=record.transaction_id,
                intent=record.intent,
                state=state,
                created_at=record.created_at,
                updated_at=now,
                retry_reason=retry_reason,
                dead_letter_reason=dead_letter_reason or record.dead_letter_reason,
                phase_states=record.phase_states,
                phase_updated_at=record.phase_updated_at,
                phase_results=record.phase_results,
            )
            return self._replace_record(updated)

    def _transition_phase(
        self,
        record_id: str,
        *,
        phase: MatrixPhase | str,
        phase_state: PhaseState,
        result: dict[str, JsonValue] | None = None,
        dead_letter_reason: str | None = None,
    ) -> OutboxRecord:
        if phase not in _VALID_PHASES:
            raise OutboxTransitionError("unknown_phase")
        if phase_state not in _VALID_PHASE_STATES:
            raise OutboxTransitionError("unknown_phase_state")
        sanitized_result = _sanitize_phase_result(result, path=f"phase_results.{phase}") if result is not None else None
        with self._lock:
            record = self._find_record(record_id)
            if record.state in {"failed", "dead_lettered"}:
                raise OutboxTransitionError("terminal_record_cannot_transition")
            existing_result = record.phase_results.get(str(phase), {})
            if record.phase_states.get(phase) == phase_state and dead_letter_reason is None:
                if sanitized_result is None or sanitized_result == existing_result:
                    return record
                if existing_result:
                    raise OutboxTransitionError("delivered_phase_result_cannot_change")
            now = _utc_now()
            phase_states = dict(record.phase_states)
            phase_states[phase] = phase_state
            phase_updated_at = dict(record.phase_updated_at)
            phase_updated_at[phase] = now
            phase_results = dict(record.phase_results)
            if sanitized_result:
                phase_results[str(phase)] = sanitized_result
            record_state: OutboxState = record.state
            if phase_state == "failed":
                record_state = "dead_lettered" if dead_letter_reason is not None else "failed"
            elif phase_states and all(state == "delivered" for state in phase_states.values()):
                record_state = "delivered"
            updated = OutboxRecord(
                id=record.id,
                transaction_id=record.transaction_id,
                intent=record.intent,
                state=record_state,
                created_at=record.created_at,
                updated_at=now,
                retry_reason=None if dead_letter_reason is not None else record.retry_reason,
                dead_letter_reason=dead_letter_reason or record.dead_letter_reason,
                phase_states=phase_states,
                phase_updated_at=phase_updated_at,
                phase_results=phase_results,
            )
            return self._replace_record(updated)

    def _find_record(self, record_id: str) -> OutboxRecord:
        with self._lock:
            try:
                return self._records[self._index_by_id[record_id]]
            except KeyError as exc:
                raise OutboxTransitionError("unknown_record") from exc

    def _replace_record(self, updated: OutboxRecord) -> OutboxRecord:
        with self._lock:
            try:
                index = self._index_by_id[updated.id]
            except KeyError as exc:
                raise OutboxTransitionError("unknown_record") from exc
            records = list(self._records)
            records[index] = updated
            records = self._apply_retention(records)
            self._persist_records(records)
            self._set_records(records)
            return self._find_record(updated.id)

    def _set_records(self, records: list[OutboxRecord]) -> None:
        with self._lock:
            self._records = records
            self._index_by_id = {record.id: index for index, record in enumerate(records)}

    def _apply_retention(self, records: list[OutboxRecord]) -> list[OutboxRecord]:
        retained = list(records)
        if self.retention.max_terminal_age_seconds is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.retention.max_terminal_age_seconds)
            retained = [
                record
                for record in retained
                if record.state not in _TERMINAL_STATES
                or (parsed := _parse_utc_timestamp(record.updated_at)) is None
                or parsed >= cutoff
            ]
        if self.retention.max_records is not None and len(retained) > self.retention.max_records:
            indexed = list(enumerate(retained))
            indexed.sort(key=lambda item: (_retention_prune_rank(item[1]), item[1].updated_at, item[0]))
            remove_count = len(retained) - self.retention.max_records
            remove_indexes = {index for index, _record in indexed[:remove_count]}
            retained = [record for index, record in enumerate(retained) if index not in remove_indexes]
        return retained

    def _load_records(self) -> tuple[list[OutboxRecord], RecoveryResult]:
        if not self.path.exists():
            return [], RecoveryResult()

        try:
            if self.path.stat().st_size > _MAX_OUTBOX_FILE_BYTES:
                recovery = RecoveryResult().with_event(
                    self._quarantine_file(self.path, reason="oversized_file", suffix="json")
                )
                return [], recovery
        except OSError as exc:
            raise OutboxPersistenceError("failed to inspect local outbox payload") from exc

        raw = self.path.read_bytes()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            recovery = RecoveryResult().with_event(
                self._quarantine_bytes(raw, reason="invalid_json", suffix="json")
            )
            return [], recovery

        if not isinstance(payload, dict):
            recovery = RecoveryResult().with_event(
                self._quarantine_json(payload, reason="invalid_top_level_schema")
            )
            return [], recovery

        if payload.get("schema_version") != _SCHEMA_VERSION:
            recovery = RecoveryResult().with_event(
                self._quarantine_json(payload, reason="unsupported_schema_version")
            )
            return [], recovery

        items = payload.get("items")
        if not isinstance(items, list):
            recovery = RecoveryResult().with_event(
                self._quarantine_json(payload, reason="invalid_items_schema")
            )
            return [], recovery

        records: list[OutboxRecord] = []
        recovery = RecoveryResult()
        for item in items:
            try:
                records.append(OutboxRecord.from_json(_require_mapping(item, "record")))
            except _RecordValidationError as exc:
                recovery = recovery.with_event(
                    self._quarantine_json(item, reason=str(exc) or "malformed_record")
                )
            except (TypeError, ValueError):
                recovery = recovery.with_event(
                    self._quarantine_json(item, reason="malformed_record")
                )
        return records, recovery.with_recovered_count(len(records))

    def _persist_records(self, records: list[OutboxRecord]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": _SCHEMA_VERSION,
            "items": [record.to_json() for record in records],
        }
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                tmp_path = handle.name
                json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.path)
            _fsync_directory(self.path.parent)
        except OSError as exc:
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            raise OutboxPersistenceError("failed to persist local outbox record") from exc

    def _quarantine_bytes(self, payload: bytes, *, reason: str, suffix: str = "bin") -> RecoveryEvent:
        digest = hashlib.sha256(payload).hexdigest()[:16]
        quarantine_path = self._quarantine_dir() / f"{reason}-{digest}.{suffix}.bad"
        self._atomic_quarantine_write(quarantine_path, lambda handle: handle.write(payload))
        return RecoveryEvent(reason=reason, quarantine_path=str(quarantine_path))

    def _quarantine_json(self, payload: Any, *, reason: str) -> RecoveryEvent:
        serialized = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8") + b"\n"
        return self._quarantine_bytes(serialized, reason=reason, suffix="json")

    def _quarantine_file(self, source: Path, *, reason: str, suffix: str) -> RecoveryEvent:
        stat = source.stat()
        digest_source = f"{source.name}:{stat.st_size}:{stat.st_mtime_ns}".encode("utf-8")
        digest = hashlib.sha256(digest_source).hexdigest()[:16]
        quarantine_path = self._quarantine_dir() / f"{reason}-{digest}.{suffix}.bad"

        def copy_source(handle: BinaryIO) -> None:
            with source.open("rb") as reader:
                shutil.copyfileobj(reader, handle, length=1024 * 1024)

        self._atomic_quarantine_write(quarantine_path, copy_source)
        return RecoveryEvent(reason=reason, quarantine_path=str(quarantine_path))

    def _quarantine_dir(self) -> Path:
        return self.path.parent / f".{self.path.stem}-quarantine"

    def _atomic_quarantine_write(self, path: Path, writer: Callable[[BinaryIO], None]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "wb",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                tmp_path = handle.name
                writer(handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
            _fsync_directory(path.parent)
            _prune_quarantine(path.parent, max_files=_MAX_QUARANTINE_FILES)
        except OSError as exc:
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            raise OutboxPersistenceError("failed to quarantine local outbox payload") from exc


def derive_outbox_item_id(intent: AlertIntent) -> str:
    return "outbox_" + _stable_digest(intent)[:32]


def derive_matrix_transaction_id(intent: AlertIntent) -> str:
    return "psm_" + _stable_digest(intent)[:48]


def _stable_digest(intent: AlertIntent) -> str:
    payload = intent.sanitized().to_json()
    stable = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def _required_clean_string(field_name: str, value: str) -> str:
    cleaned = _optional_clean_string(field_name, value)
    if cleaned is None:
        raise ValueError(f"{field_name} must not be empty")
    return cleaned


def _optional_clean_string(field_name: str, value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    cleaned = value.strip()
    if not cleaned:
        return None
    _reject_secret_key(field_name)
    _reject_secret_value(cleaned, path=field_name)
    return cleaned


def _sanitize_mapping(metadata: dict[str, JsonValue], *, path: str) -> dict[str, JsonValue]:
    if metadata is None:
        return {}
    if not isinstance(metadata, dict):
        raise TypeError(f"{path} must be a mapping")
    sanitized: dict[str, JsonValue] = {}
    for key, value in metadata.items():
        if not isinstance(key, str):
            raise TypeError(f"{path} keys must be strings")
        key = key.strip()
        if not key:
            continue
        _reject_secret_key(key)
        cleaned = _sanitize_json_value(value, path=f"{path}.{key}")
        if cleaned in (None, "", [], {}):
            continue
        sanitized[key] = cleaned
    return sanitized


def _sanitize_json_value(value: Any, *, path: str) -> JsonValue:
    if isinstance(value, bytes | bytearray | memoryview):
        raise SecretBearingIntentError(f"{path} contains binary data that cannot be persisted")
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        _reject_secret_value(cleaned, path=path)
        return cleaned
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, list | tuple):
        cleaned_list = [_sanitize_json_value(item, path=path) for item in value]
        return [item for item in cleaned_list if item not in (None, "", [], {})]
    if isinstance(value, dict):
        return _sanitize_mapping(value, path=path)
    raise TypeError(f"{path} must be JSON-serializable")


def _parse_phase_payload(
    payload: dict[str, Any], *, fallback_phase: str
) -> tuple[dict[str, PhaseState], dict[str, str], dict[str, PhaseResult]]:
    phases = payload.get("phases")
    if phases is None:
        return {fallback_phase: "pending"}, {}, {}
    if not isinstance(phases, list):
        raise _RecordValidationError("invalid_phase_schema")
    phase_states: dict[str, PhaseState] = {}
    phase_updated_at: dict[str, str] = {}
    phase_results: dict[str, PhaseResult] = {}
    for phase_item in phases:
        phase_payload = _require_mapping(phase_item, "record.phases[]")
        phase = _require_string(phase_payload, "phase", path="record.phases[]")
        if phase not in _VALID_PHASES:
            raise _RecordValidationError("invalid_phase")
        state = _optional_string(phase_payload, "state", path="record.phases[]") or "pending"
        if state not in _VALID_PHASE_STATES:
            raise _RecordValidationError("invalid_phase_state")
        phase_states[phase] = state  # type: ignore[assignment]
        updated_at = _optional_string(phase_payload, "updated_at", path="record.phases[]")
        if updated_at is not None:
            phase_updated_at[phase] = updated_at
        result = phase_payload.get("result")
        if result is not None:
            sanitized_result = _sanitize_phase_result(result, path=f"record.phases[].{phase}.result")
            if sanitized_result:
                phase_results[phase] = sanitized_result
    return phase_states or {fallback_phase: "pending"}, phase_updated_at, phase_results


def _sanitize_phase_result(result: Any, *, path: str) -> PhaseResult:
    if result is None:
        return {}
    if not isinstance(result, dict):
        raise TypeError(f"{path} must be a mapping")
    return _sanitize_mapping(result, path=path)

def _require_mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _RecordValidationError("malformed_record")
    return value


def _optional_mapping(payload: dict[str, Any], key: str, *, path: str) -> dict[str, JsonValue]:
    value = payload.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError(f"{path}.{key} must be a mapping")
    return value


def _require_string(payload: dict[str, Any], key: str, *, path: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}.{key} must be a non-empty string")
    return value


def _optional_string(payload: dict[str, Any], key: str, *, path: str) -> str | None:
    if key not in payload or payload[key] is None:
        return None
    value = payload[key]
    if not isinstance(value, str):
        raise TypeError(f"{path}.{key} must be a string")
    return value


def _optional_reason(payload: dict[str, Any], key: str, *, path: str) -> str | None:
    raw = _optional_string(payload, key, path=path)
    if raw is None:
        return None
    return _safe_reason_code(raw)


def _safe_reason_code(reason: str) -> str:
    if not isinstance(reason, str):
        raise TypeError("reason must be a string")
    raw = reason.strip()
    if not raw:
        return "unspecified"
    if _SECRET_KEY_RE.search(raw) or _SECRET_VALUE_RE.search(raw):
        return "redacted"
    cleaned = _REASON_CODE_RE.sub("_", raw.lower()).strip("_")
    return cleaned[:64] or "unspecified"


def _reject_secret_key(key: str) -> None:
    if _SECRET_KEY_RE.search(key):
        raise SecretBearingIntentError("intent contains a field that is unsafe to persist")


def _reject_secret_value(value: str, *, path: str) -> None:
    if _SECRET_VALUE_RE.search(value):
        raise SecretBearingIntentError(f"{path} contains a value that is unsafe to persist")


def _retention_prune_rank(record: OutboxRecord) -> int:
    if record.state in {"delivered", "failed", "dead_lettered"}:
        return 0
    return 1


def _fsync_directory(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _prune_quarantine(path: Path, *, max_files: int) -> None:
    files = sorted(
        (item for item in path.iterdir() if item.is_file()),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for stale in files[max_files:]:
        try:
            stale.unlink()
        except OSError:
            pass
