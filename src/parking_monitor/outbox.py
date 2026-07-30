"""Compatibility facade for the durable schema-v1 Matrix outbox."""

from __future__ import annotations

from collections.abc import Sequence
import copy
from dataclasses import replace
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import threading

from parking_monitor.outbox_models import (
    AlertIntent,
    JsonScalar,
    JsonValue,
    MatrixPhase,
    OutboxError,
    OutboxPersistenceError,
    OutboxPostCommitPersistenceError,
    OutboxRecord,
    OutboxRecoveryError,
    OutboxRetentionPolicy,
    OutboxRetryPolicy,
    OutboxState,
    OutboxTransitionError,
    PhaseResult,
    PhaseState,
    RecordValidationError,
    RetrySchedule,
    RETRYABLE_STATES,
    SCHEMA_VERSION,
    SecretBearingIntentError,
    TERMINAL_STATES,
    VALID_PHASES,
    VALID_PHASE_STATES,
    due_record_sort_key,
    format_utc_timestamp,
    is_record_due,
    parse_utc_timestamp,
    require_utc_datetime,
    safe_reason_code,
    sanitize_phase_result,
    status_item,
    utc_now_text,
)
from parking_monitor.outbox_storage import (
    MAX_OUTBOX_FILE_BYTES,
    RecoveryEvent,
    RecoveryResult,
    apply_retention,
    fsync_directory,
    load_records,
    persist_records,
)
from parking_monitor.outbox_derivatives import OutboxDerivativeMixin
from parking_monitor.outbox_lookup import OutboxLookupMixin, build_event_index

# Compatibility aliases retained for callers and existing failure-injection tests.
_SCHEMA_VERSION = SCHEMA_VERSION
_MAX_OUTBOX_FILE_BYTES = MAX_OUTBOX_FILE_BYTES
_OutboxPostCommitPersistenceError = OutboxPostCommitPersistenceError
_RecordValidationError = RecordValidationError
_fsync_directory = fsync_directory
_safe_reason_code = safe_reason_code
_parse_utc_timestamp = parse_utc_timestamp


class LocalOutbox(OutboxDerivativeMixin, OutboxLookupMixin):
    """Thread-safe JSON-file backed Matrix alert outbox."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        max_records: int | None = 1000,
        max_terminal_age_seconds: int | None = None,
        retention: OutboxRetentionPolicy | None = None,
    ) -> None:
        self.path = Path(path)
        self.retention = retention or OutboxRetentionPolicy(max_records, max_terminal_age_seconds)
        self._lock: threading.RLock = threading.RLock()
        self._records: list[OutboxRecord] = []
        self._index_by_id: dict[str, int] = {}
        self._indices_by_event_id: dict[str, tuple[int, ...]] = {}
        self._revision = 0
        self._compact_summary_cache: tuple[int, str] | None = None
        with self._lock:
            records, self.recovery = self._load_records()
            self._set_records(records)
            pruned = self._apply_retention(records)
            if pruned != records:
                self._persist_and_set_records(pruned)

    @property
    def revision(self) -> int:
        with self._lock:
            return self._revision

    def enqueue(self, intent: AlertIntent) -> OutboxRecord:
        sanitized = intent.sanitized()
        return self.enqueue_with_phases(sanitized, (sanitized.phase,))

    def enqueue_with_phases(
        self, intent: AlertIntent, phases: Sequence[MatrixPhase | str]
    ) -> OutboxRecord:
        sanitized = intent.sanitized()
        requested = tuple(phases) or (sanitized.phase,)
        if any(phase not in VALID_PHASES for phase in requested):
            raise OutboxTransitionError("unknown_phase")
        with self._lock:
            item_id = derive_outbox_item_id(sanitized)
            if item_id in self._index_by_id:
                return self._find_record(item_id)
            now = utc_now_text()
            phase_states: dict[str, PhaseState] = {str(phase): "pending" for phase in requested}
            record = OutboxRecord(
                id=item_id,
                transaction_id=derive_matrix_transaction_id(sanitized),
                intent=sanitized,
                created_at=now,
                updated_at=now,
                phase_states=phase_states,
                phase_updated_at={phase: now for phase in phase_states},
            )
            self._persist_and_set_records(self._apply_retention([*self._records, record]))
            return self._find_record(record.id)

    def list_records(self, state: OutboxState | None = None) -> list[OutboxRecord]:
        with self._lock:
            if state is None:
                return copy.deepcopy(self._records)
            return copy.deepcopy([record for record in self._records if record.state == state])

    def list_pending(self) -> list[OutboxRecord]:
        return self.list_records("pending")

    def next_due_record(self, now: datetime) -> OutboxRecord | None:
        records = self.due_records(now, max_records=1)
        return records[0] if records else None

    def due_records(
        self,
        now: datetime,
        *,
        record_id: str | None = None,
        max_records: int | None = None,
    ) -> list[OutboxRecord]:
        now_utc = require_utc_datetime(now)
        with self._lock:
            eligible = [
                record
                for record in self._records
                if is_record_due(record, now_utc) and (record_id is None or record.id == record_id)
            ]
            eligible.sort(key=due_record_sort_key)
            selected = eligible if max_records is None else eligible[: max(0, int(max_records))]
            return copy.deepcopy(selected)

    def next_retry_due_at(self) -> datetime | None:
        with self._lock:
            due = [
                parsed
                for record in self._records
                if record.state == "retrying" and record.retry_due_at is not None
                if (parsed := parse_utc_timestamp(record.retry_due_at)) is not None
            ]
            return min(due, default=None)

    def mark_retrying(
        self,
        record_id: str,
        *,
        reason: str,
        retry_due_at: str | None = None,
        retry_attempt_count: int | None = None,
    ) -> OutboxRecord:
        with self._lock:
            current = self._find_record(record_id)
            count = current.retry_attempt_count if retry_attempt_count is None else retry_attempt_count
            return self._transition_record(
                record_id,
                state="retrying",
                retry_reason=safe_reason_code(reason),
                retry_attempt_count=count,
                retry_due_at=retry_due_at,
            )

    def mark_delivered(self, record_id: str) -> OutboxRecord:
        return self._transition_record(record_id, state="delivered")

    def mark_failed(self, record_id: str, *, reason: str) -> OutboxRecord:
        return self._transition_record(record_id, state="failed", dead_letter_reason=safe_reason_code(reason))

    def mark_dead_lettered(self, record_id: str, *, reason: str) -> OutboxRecord:
        return self._transition_record(
            record_id, state="dead_lettered", dead_letter_reason=safe_reason_code(reason)
        )

    def ensure_phase_pending(self, record_id: str, phase: MatrixPhase | str) -> OutboxRecord:
        return self._transition_phase(record_id, phase=phase, phase_state="pending")

    def mark_phase_delivered(
        self,
        record_id: str,
        phase: MatrixPhase | str,
        *,
        result: dict[str, JsonValue] | None = None,
    ) -> OutboxRecord:
        if result is None:
            return self._transition_phase(record_id, phase=phase, phase_state="delivered")
        return self.apply_phase_result(record_id, phase, delivered_result=result)

    def mark_phase_failed(
        self, record_id: str, phase: MatrixPhase | str, *, reason: str
    ) -> OutboxRecord:
        return self.apply_phase_result(record_id, phase, terminal_reason=safe_reason_code(reason))

    def apply_phase_result(
        self,
        record_id: str,
        phase: MatrixPhase | str,
        *,
        delivered_result: PhaseResult | None = None,
        retry: RetrySchedule | None = None,
        terminal_reason: str | None = None,
    ) -> OutboxRecord:
        if sum(value is not None for value in (delivered_result, retry, terminal_reason)) != 1:
            raise OutboxTransitionError("exactly_one_phase_outcome_required")
        if retry is not None:
            return self._apply_retry(record_id, phase, retry)
        if terminal_reason is not None:
            return self._transition_phase(
                record_id,
                phase=phase,
                phase_state="failed",
                dead_letter_reason=safe_reason_code(terminal_reason),
            )
        return self._transition_phase(
            record_id,
            phase=phase,
            phase_state="delivered",
            result=delivered_result,
        )

    def status_summary(self) -> dict[str, JsonValue]:
        return self._status_summary(include_items=True)

    def compact_status_summary(self) -> dict[str, JsonValue]:
        with self._lock:
            if self._compact_summary_cache is None or self._compact_summary_cache[0] != self._revision:
                summary = self._status_summary(include_items=False)
                self._compact_summary_cache = (
                    self._revision,
                    json.dumps(summary, sort_keys=True, separators=(",", ":")),
                )
            return json.loads(self._compact_summary_cache[1])

    def _status_summary(self, *, include_items: bool) -> dict[str, JsonValue]:
        with self._lock:
            counts: dict[str, int] = {}
            retry_counts: dict[str, int] = {}
            dead_counts: dict[str, int] = {}
            timestamps: list[str] = []
            items: list[dict[str, JsonValue]] = []
            for record in self._records:
                counts[record.state] = counts.get(record.state, 0) + 1
                timestamps.extend((record.created_at, record.updated_at))
                if record.retry_reason:
                    retry_counts[record.retry_reason] = retry_counts.get(record.retry_reason, 0) + 1
                if record.dead_letter_reason:
                    dead_counts[record.dead_letter_reason] = dead_counts.get(record.dead_letter_reason, 0) + 1
                if include_items:
                    items.append(status_item(record))
            summary: dict[str, JsonValue] = {
                "path": str(self.path),
                "schema_version": SCHEMA_VERSION,
                "total": len(self._records),
                "counts_by_state": counts,
                "oldest_timestamp": min(timestamps) if timestamps else None,
                "newest_timestamp": max(timestamps) if timestamps else None,
                "retry_reason_counts": retry_counts,
                "dead_letter_reason_counts": dead_counts,
                "recovery": self.recovery.to_json(),
            }
            if include_items:
                summary["items"] = items
            return summary

    def _apply_retry(self, record_id: str, phase: MatrixPhase | str, retry: RetrySchedule) -> OutboxRecord:
        if phase not in VALID_PHASES:
            raise OutboxTransitionError("unknown_phase")
        with self._lock:
            record = self._find_record(record_id)
            if record.state not in RETRYABLE_STATES:
                raise OutboxTransitionError("terminal_record_cannot_retry")
            if phase not in record.phase_states:
                raise OutboxTransitionError("unknown_phase")
            if record.phase_states[phase] == "delivered":
                return record
            updated = replace(
                record,
                state="retrying",
                updated_at=utc_now_text(),
                retry_reason=retry.reason,
                retry_attempt_count=retry.attempt_count,
                retry_due_at=retry.due_at,
            )
            return self._replace_record(updated)

    def _transition_record(
        self,
        record_id: str,
        *,
        state: OutboxState,
        retry_reason: str | None = None,
        retry_attempt_count: int = 0,
        retry_due_at: str | None = None,
        dead_letter_reason: str | None = None,
    ) -> OutboxRecord:
        with self._lock:
            record = self._find_record(record_id)
            if record.state in TERMINAL_STATES and state != record.state:
                if not (record.state == "failed" and state == "dead_lettered"):
                    raise OutboxTransitionError("terminal_record_cannot_transition")
            if state == "retrying" and record.state not in RETRYABLE_STATES:
                raise OutboxTransitionError("terminal_record_cannot_retry")
            updated = replace(
                record,
                state=state,
                updated_at=utc_now_text(),
                retry_reason=retry_reason,
                retry_attempt_count=retry_attempt_count,
                retry_due_at=retry_due_at,
                dead_letter_reason=dead_letter_reason or record.dead_letter_reason,
            )
            if updated == record:
                return record
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
        if phase not in VALID_PHASES:
            raise OutboxTransitionError("unknown_phase")
        if phase_state not in VALID_PHASE_STATES:
            raise OutboxTransitionError("unknown_phase_state")
        sanitized = sanitize_phase_result(result, path=f"phase_results.{phase}") if result is not None else None
        with self._lock:
            record = self._find_record(record_id)
            if record.state in {"failed", "dead_lettered"}:
                raise OutboxTransitionError("terminal_record_cannot_transition")
            existing = record.phase_results.get(str(phase), {})
            if record.phase_states.get(phase) == phase_state and dead_letter_reason is None:
                if sanitized is None or sanitized == existing:
                    return record
                if existing:
                    raise OutboxTransitionError("delivered_phase_result_cannot_change")
            now = utc_now_text()
            states = dict(record.phase_states)
            states[str(phase)] = phase_state
            timestamps = dict(record.phase_updated_at)
            timestamps[str(phase)] = now
            results = dict(record.phase_results)
            if sanitized:
                results[str(phase)] = sanitized
            state: OutboxState = record.state
            if phase_state == "failed":
                state = "dead_lettered" if dead_letter_reason else "failed"
            elif states and all(value == "delivered" for value in states.values()):
                state = "delivered"
            updated = replace(
                record,
                state=state,
                updated_at=now,
                retry_reason=None,
                retry_attempt_count=0,
                retry_due_at=None,
                dead_letter_reason=dead_letter_reason or record.dead_letter_reason,
                phase_states=states,
                phase_updated_at=timestamps,
                phase_results=results,
            )
            return self._replace_record(updated)

    def _find_record(self, record_id: str) -> OutboxRecord:
        try:
            return copy.deepcopy(self._records[self._index_by_id[record_id]])
        except KeyError as exc:
            raise OutboxTransitionError("unknown_record") from exc

    def _replace_record(self, updated: OutboxRecord) -> OutboxRecord:
        try:
            index = self._index_by_id[updated.id]
        except KeyError as exc:
            raise OutboxTransitionError("unknown_record") from exc
        records = list(self._records)
        records[index] = updated
        self._persist_and_set_records(self._apply_retention(records))
        return self._find_record(updated.id)

    def _persist_and_set_records(self, records: list[OutboxRecord]) -> None:
        try:
            self._persist_records(records)
        except OutboxPostCommitPersistenceError:
            self._set_records(records, mutated=True)
            raise
        self._set_records(records, mutated=True)

    def _set_records(self, records: list[OutboxRecord], *, mutated: bool = False) -> None:
        self._records = records
        self._index_by_id = {record.id: index for index, record in enumerate(records)}
        self._indices_by_event_id = build_event_index(records)
        if mutated:
            self._revision += 1
            self._compact_summary_cache = None

    def _apply_retention(self, records: list[OutboxRecord]) -> list[OutboxRecord]:
        return apply_retention(records, self.retention)

    def _load_records(self) -> tuple[list[OutboxRecord], RecoveryResult]:
        return load_records(
            self.path,
            max_bytes=_MAX_OUTBOX_FILE_BYTES,
            fsync_directory=_fsync_directory,
        )

    def _persist_records(self, records: list[OutboxRecord]) -> None:
        persist_records(self.path, records, fsync_directory=_fsync_directory)


def derive_outbox_item_id(intent: AlertIntent) -> str:
    return "outbox_" + _stable_digest(intent)[:32]


def derive_matrix_transaction_id(intent: AlertIntent) -> str:
    return "psm_" + _stable_digest(intent)[:48]


def _stable_digest(intent: AlertIntent) -> str:
    stable = json.dumps(intent.sanitized().to_json(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()
