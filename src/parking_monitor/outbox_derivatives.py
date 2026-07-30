"""Typed schema-v1 transport metadata mutation for pending Matrix uploads."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Protocol

from parking_monitor.outbox_models import AlertIntent, JsonValue, OutboxRecord, OutboxTransitionError, TERMINAL_STATES, utc_now_text


class _OutboxOwner(Protocol):
    _lock: Any

    def _find_record(self, record_id: str) -> OutboxRecord: ...

    def _replace_record(self, updated: OutboxRecord) -> OutboxRecord: ...


class OutboxDerivativeMixin:
    """Attach immutable upload evidence once without changing record identity."""

    def attach_upload_derivative(
        self: _OutboxOwner,
        record_id: str,
        *,
        path: str,
        info: dict[str, JsonValue],
    ) -> OutboxRecord:
        validated_info = _validate_derivative(path, info)
        with self._lock:
            record = self._find_record(record_id)
            if record.state in TERMINAL_STATES:
                raise OutboxTransitionError("terminal_record_cannot_attach_upload_derivative")
            metadata = dict(record.intent.metadata)
            existing_path = metadata.get("upload_derivative_path")
            existing_info = metadata.get("upload_derivative_info")
            if existing_path is not None or existing_info is not None:
                if existing_path == path and existing_info == validated_info:
                    return record
                raise OutboxTransitionError("upload_derivative_already_attached")
            metadata.update({"upload_derivative_path": path, "upload_derivative_info": validated_info})
            intent: AlertIntent = replace(record.intent, metadata=metadata).sanitized()
            return self._replace_record(replace(record, intent=intent, updated_at=utc_now_text()))


def _validate_derivative(path: str, info: dict[str, JsonValue]) -> dict[str, JsonValue]:
    required = {"mimetype", "size", "w", "h", "sha256"}
    if not isinstance(path, str) or not path or set(info) != required or info.get("mimetype") != "image/jpeg":
        raise OutboxTransitionError("invalid_upload_derivative")
    dimensions = (info.get("size"), info.get("w"), info.get("h"))
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in dimensions):
        raise OutboxTransitionError("invalid_upload_derivative")
    digest = info.get("sha256")
    if not isinstance(digest, str) or len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise OutboxTransitionError("invalid_upload_derivative")
    return dict(info)
