"""Validated, secret-safe value types for the schema-v1 Matrix outbox."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import isfinite
from numbers import Real
import re
from typing import Any, Literal

OutboxState = Literal["pending", "retrying", "delivered", "failed", "dead_lettered"]
PhaseState = Literal["pending", "delivered", "failed"]
MatrixPhase = Literal["text", "upload", "image"]
JsonScalar = str | int | float | bool | None
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
PhaseResult = dict[str, JsonValue]

SCHEMA_VERSION = 1
VALID_STATES = frozenset({"pending", "retrying", "delivered", "failed", "dead_lettered"})
TERMINAL_STATES = frozenset({"delivered", "failed", "dead_lettered"})
RETRYABLE_STATES = frozenset({"pending", "retrying"})
VALID_PHASES = frozenset({"text", "upload", "image"})
VALID_PHASE_STATES = frozenset({"pending", "delivered", "failed"})
MAX_RETRY_ATTEMPT_COUNT = 1_000_000

_SECRET_KEY_RE = re.compile(
    r"(access[_-]?token|authorization|auth[_-]?header|bearer|password|secret|cookie|rtsp|image[_-]?bytes|raw[_-]?image|exception|traceback|error[_-]?message)",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = re.compile(
    r"(rtsp://|authorization\s*:|bearer\s+[a-z0-9._~+/=-]+|access[_-]?token\s*[=:]|traceback \(most recent call last\)|exception:)",
    re.IGNORECASE,
)
_REASON_CODE_RE = re.compile(r"[^a-z0-9_.-]+")


class OutboxError(Exception):
    """Base error for local outbox operations."""


class SecretBearingIntentError(OutboxError, ValueError):
    """Raised when an alert intent contains fields unsafe to persist."""


class OutboxPersistenceError(OutboxError, OSError):
    """Raised when an outbox write cannot be made durable."""


class OutboxPostCommitPersistenceError(OutboxPersistenceError):
    """Raised when replace succeeded but directory synchronization failed."""


class OutboxRecoveryError(OutboxError, ValueError):
    """Raised when persisted outbox data cannot be trusted."""


class OutboxTransitionError(OutboxError, ValueError):
    """Raised when a requested state or phase transition is invalid."""


class RecordValidationError(OutboxRecoveryError):
    """Raised when one persisted record cannot be safely interpreted."""


def utc_now_text() -> str:
    return format_utc_timestamp(datetime.now(timezone.utc))


def format_utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError("timestamp must be UTC")
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc_timestamp(value: str) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        return None
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class OutboxRetryPolicy:
    initial_seconds: float
    max_seconds: float
    jitter_ratio: float

    def __post_init__(self) -> None:
        values = (self.initial_seconds, self.max_seconds, self.jitter_ratio)
        if any(isinstance(value, bool) or not isinstance(value, Real) or not isfinite(value) for value in values):
            raise ValueError("retry policy values must be finite real numbers")
        if self.initial_seconds <= 0 or self.max_seconds <= 0:
            raise ValueError("retry intervals must be positive")
        if self.max_seconds < self.initial_seconds:
            raise ValueError("max_seconds must cover initial_seconds")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")

    def delay_seconds(self, attempt_count: int, *, random_unit: float) -> float:
        _validate_retry_count(attempt_count)
        if attempt_count < 1:
            raise ValueError("attempt_count must be positive")
        if isinstance(random_unit, bool) or not isinstance(random_unit, Real) or not isfinite(random_unit) or not 0 <= random_unit <= 1:
            raise ValueError("random_unit must be finite and between 0 and 1")
        base = min(self.initial_seconds * (2.0 ** min(attempt_count - 1, 1023)), self.max_seconds)
        return min(base * (1 + self.jitter_ratio * random_unit), self.max_seconds)


@dataclass(frozen=True, slots=True)
class RetrySchedule:
    due_at: str
    attempt_count: int
    reason: str

    def __post_init__(self) -> None:
        if parse_utc_timestamp(self.due_at) is None:
            raise ValueError("due_at must be a UTC timestamp")
        _validate_retry_count(self.attempt_count)
        if self.attempt_count < 1:
            raise ValueError("attempt_count must be positive")
        object.__setattr__(self, "reason", safe_reason_code(self.reason))


@dataclass(frozen=True)
class AlertIntent:
    event_id: str
    phase: str
    body: str
    room_id: str | None = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)

    def sanitized(self) -> "AlertIntent":
        phase = _required_clean_string("phase", self.phase)
        if phase not in VALID_PHASES:
            raise ValueError("phase must be one of: image, text, upload")
        return AlertIntent(
            event_id=_required_clean_string("event_id", self.event_id),
            phase=phase,
            body=_required_clean_string("body", self.body),
            room_id=_optional_clean_string("room_id", self.room_id),
            metadata=sanitize_mapping(self.metadata, path="metadata"),
        )

    def to_json(self) -> dict[str, JsonValue]:
        value = self.sanitized()
        payload: dict[str, JsonValue] = {
            "event_id": value.event_id,
            "phase": value.phase,
            "body": value.body,
        }
        if value.room_id is not None:
            payload["room_id"] = value.room_id
        if value.metadata:
            payload["metadata"] = value.metadata
        return payload

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "AlertIntent":
        require_mapping(payload, "intent")
        return cls(
            event_id=require_string(payload, "event_id", path="intent"),
            phase=require_string(payload, "phase", path="intent"),
            body=require_string(payload, "body", path="intent"),
            room_id=optional_string(payload, "room_id", path="intent"),
            metadata=optional_mapping(payload, "metadata", path="intent"),
        ).sanitized()


@dataclass(frozen=True)
class OutboxRecord:
    id: str
    transaction_id: str
    intent: AlertIntent
    state: OutboxState = "pending"
    created_at: str = field(default_factory=utc_now_text)
    updated_at: str = field(default_factory=utc_now_text)
    retry_reason: str | None = None
    retry_attempt_count: int = 0
    retry_due_at: str | None = None
    dead_letter_reason: str | None = None
    phase_states: dict[str, PhaseState] = field(default_factory=dict)
    phase_updated_at: dict[str, str] = field(default_factory=dict)
    phase_results: dict[str, PhaseResult] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.state not in VALID_STATES:
            raise RecordValidationError("invalid_state")
        _validate_retry_count(self.retry_attempt_count)
        if self.retry_due_at is not None and parse_utc_timestamp(self.retry_due_at) is None:
            raise RecordValidationError("invalid_retry_due_at")
        if self.state in TERMINAL_STATES and self.retry_due_at is not None:
            raise RecordValidationError("terminal_retry_due_at")
        phases = self.phase_states or {self.intent.phase: "pending"}
        normalized: dict[str, PhaseState] = {}
        for phase, state in phases.items():
            if phase not in VALID_PHASES:
                raise RecordValidationError("invalid_phase")
            if state not in VALID_PHASE_STATES:
                raise RecordValidationError("invalid_phase_state")
            normalized[phase] = state  # type: ignore[assignment]
        timestamps = {phase: self.phase_updated_at.get(phase, self.updated_at) for phase in normalized}
        results: dict[str, PhaseResult] = {}
        for phase, result in self.phase_results.items():
            if phase not in VALID_PHASES or phase not in normalized:
                raise RecordValidationError("invalid_phase_result")
            sanitized = sanitize_phase_result(result, path=f"phase_results.{phase}")
            if sanitized:
                results[phase] = sanitized
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
        if self.retry_attempt_count:
            payload["retry_attempt_count"] = self.retry_attempt_count
        if self.retry_due_at is not None:
            payload["retry_due_at"] = self.retry_due_at
        if self.dead_letter_reason is not None:
            payload["dead_letter_reason"] = self.dead_letter_reason
        return payload

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "OutboxRecord":
        try:
            require_mapping(payload, "record")
            state = optional_string(payload, "state", path="record") or "pending"
            intent = AlertIntent.from_json(require_mapping(payload.get("intent"), "record.intent"))
            phase_states, phase_updated_at, phase_results = _parse_phase_payload(payload, intent.phase)
            return cls(
                id=require_string(payload, "id", path="record"),
                transaction_id=require_string(payload, "matrix_transaction_id", path="record"),
                intent=intent,
                state=state,  # type: ignore[arg-type]
                created_at=require_string(payload, "created_at", path="record"),
                updated_at=require_string(payload, "updated_at", path="record"),
                retry_reason=optional_reason(payload, "retry_reason", path="record"),
                retry_attempt_count=optional_non_negative_int(payload, "retry_attempt_count", default=0),
                retry_due_at=optional_utc_timestamp(payload, "retry_due_at"),
                dead_letter_reason=optional_reason(payload, "dead_letter_reason", path="record"),
                phase_states=phase_states,
                phase_updated_at=phase_updated_at,
                phase_results=phase_results,
            )
        except SecretBearingIntentError as exc:
            raise RecordValidationError("unsafe_record_content") from exc
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, RecordValidationError):
                raise
            raise RecordValidationError("malformed_record") from exc


@dataclass(frozen=True)
class OutboxRetentionPolicy:
    max_records: int | None = 1000
    max_terminal_age_seconds: int | None = None

    def __post_init__(self) -> None:
        if self.max_records is not None and self.max_records < 1:
            raise ValueError("max_records must be at least 1")
        if self.max_terminal_age_seconds is not None and self.max_terminal_age_seconds < 0:
            raise ValueError("max_terminal_age_seconds must not be negative")


def safe_reason_code(reason: str) -> str:
    if not isinstance(reason, str):
        raise TypeError("reason must be a string")
    raw = reason.strip()
    if not raw:
        return "unspecified"
    if _SECRET_KEY_RE.search(raw) or _SECRET_VALUE_RE.search(raw):
        return "redacted"
    return _REASON_CODE_RE.sub("_", raw.lower()).strip("_")[:64] or "unspecified"


def sanitize_mapping(metadata: dict[str, JsonValue], *, path: str) -> dict[str, JsonValue]:
    if metadata is None:
        return {}
    if not isinstance(metadata, dict):
        raise TypeError(f"{path} must be a mapping")
    sanitized: dict[str, JsonValue] = {}
    for raw_key, value in metadata.items():
        if not isinstance(raw_key, str):
            raise TypeError(f"{path} keys must be strings")
        key = raw_key.strip()
        if not key:
            continue
        _reject_secret_key(key)
        cleaned = _sanitize_json_value(value, path=f"{path}.{key}")
        if cleaned not in (None, "", [], {}):
            sanitized[key] = cleaned
    return sanitized


def sanitize_phase_result(result: Any, *, path: str) -> PhaseResult:
    if result is None:
        return {}
    if not isinstance(result, dict):
        raise TypeError(f"{path} must be a mapping")
    return sanitize_mapping(result, path=path)


def status_item(record: OutboxRecord) -> dict[str, JsonValue]:
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
        "retry_attempt_count": record.retry_attempt_count,
        "retry_due_at": record.retry_due_at,
        "dead_letter_reason": record.dead_letter_reason,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def require_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError("now must be UTC")
    return value.astimezone(timezone.utc)

def is_record_due(record: OutboxRecord, now: datetime) -> bool:
    if record.state == "pending" or (record.state == "retrying" and record.retry_due_at is None):
        return True
    due = parse_utc_timestamp(record.retry_due_at or "")
    return record.state == "retrying" and due is not None and due <= now

def due_record_sort_key(record: OutboxRecord) -> tuple[int, datetime, str, str]:
    immediate = datetime.min.replace(tzinfo=timezone.utc)
    rank = 0 if record.state == "pending" else 1 if record.retry_due_at is None else 2
    due_at = parse_utc_timestamp(record.retry_due_at or "") if rank == 2 else immediate
    return (rank, due_at or immediate, record.created_at, record.id)

def require_mapping(value: Any, _path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RecordValidationError("malformed_record")
    return value


def require_string(payload: dict[str, Any], key: str, *, path: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}.{key} must be a non-empty string")
    return value


def optional_string(payload: dict[str, Any], key: str, *, path: str) -> str | None:
    if key not in payload or payload[key] is None:
        return None
    value = payload[key]
    if not isinstance(value, str):
        raise TypeError(f"{path}.{key} must be a string")
    return value


def optional_mapping(payload: dict[str, Any], key: str, *, path: str) -> dict[str, JsonValue]:
    value = payload.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError(f"{path}.{key} must be a mapping")
    return value


def optional_reason(payload: dict[str, Any], key: str, *, path: str) -> str | None:
    value = optional_string(payload, key, path=path)
    return None if value is None else safe_reason_code(value)


def optional_non_negative_int(payload: dict[str, Any], key: str, *, default: int) -> int:
    value = payload.get(key, default)
    _validate_retry_count(value)
    return value


def optional_utc_timestamp(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or parse_utc_timestamp(value) is None:
        raise RecordValidationError("invalid_retry_due_at")
    return value


def _validate_retry_count(value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_RETRY_ATTEMPT_COUNT:
        raise RecordValidationError("invalid_retry_attempt_count")


def _parse_phase_payload(
    payload: dict[str, Any], fallback_phase: str
) -> tuple[dict[str, PhaseState], dict[str, str], dict[str, PhaseResult]]:
    phases = payload.get("phases")
    if phases is None:
        return {fallback_phase: "pending"}, {}, {}
    if not isinstance(phases, list):
        raise RecordValidationError("invalid_phase_schema")
    states: dict[str, PhaseState] = {}
    timestamps: dict[str, str] = {}
    results: dict[str, PhaseResult] = {}
    for item in phases:
        phase_payload = require_mapping(item, "record.phases[]")
        phase = require_string(phase_payload, "phase", path="record.phases[]")
        state = optional_string(phase_payload, "state", path="record.phases[]") or "pending"
        if phase not in VALID_PHASES:
            raise RecordValidationError("invalid_phase")
        if state not in VALID_PHASE_STATES:
            raise RecordValidationError("invalid_phase_state")
        states[phase] = state  # type: ignore[assignment]
        updated_at = optional_string(phase_payload, "updated_at", path="record.phases[]")
        if updated_at is not None:
            timestamps[phase] = updated_at
        if "result" in phase_payload:
            result = sanitize_phase_result(phase_payload["result"], path=f"record.phases[].{phase}.result")
            if result:
                results[phase] = result
    return states or {fallback_phase: "pending"}, timestamps, results


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
        values = [_sanitize_json_value(item, path=path) for item in value]
        return [item for item in values if item not in (None, "", [], {})]
    if isinstance(value, dict):
        return sanitize_mapping(value, path=path)
    raise TypeError(f"{path} must be JSON-serializable")


def _reject_secret_key(key: str) -> None:
    if _SECRET_KEY_RE.search(key):
        raise SecretBearingIntentError("intent contains a field that is unsafe to persist")


def _reject_secret_value(value: str, *, path: str) -> None:
    if _SECRET_VALUE_RE.search(value):
        raise SecretBearingIntentError(f"{path} contains a value that is unsafe to persist")
