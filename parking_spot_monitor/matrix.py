from __future__ import annotations

import re
import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx
from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text

CLIENT_API_PREFIX = "/_matrix/client/v3"
MEDIA_API_PREFIX = "/_matrix/media/v3"
JPEG_MIMETYPE = "image/jpeg"
OPEN_SPOT_EVENT_TYPE = "occupancy-open-event"
OCCUPIED_SPOT_EVENT_TYPE = "occupancy-occupied-event"
DISPLAY_TIMEZONE = ZoneInfo("America/Los_Angeles")


@dataclass(frozen=True)
class MatrixTextEvent:
    """Safe inbound Matrix text event projected from /sync."""

    event_id: str
    sender: str
    room_id: str
    body: str


@dataclass(frozen=True)
class MatrixSyncResult:
    """Safe bounded result from one Matrix /sync poll."""

    next_batch: str
    events: tuple[MatrixTextEvent, ...]


@dataclass(frozen=True)
class MatrixCommand:
    """Parsed operator command with validated, non-secret arguments."""

    action: str
    profile_id: str | None = None
    label: str | None = None
    source_profile_id: str | None = None
    target_profile_id: str | None = None
    subject_id: str | None = None


@dataclass(frozen=True)
class MatrixCommandPollResult:
    """Metadata-only summary of one command poll."""

    next_batch: str
    processed_count: int
    ignored_count: int
    error_count: int
    bootstrapped: bool = False


@dataclass(frozen=True)
class MatrixSnapshot:
    """Event-specific raw snapshot prepared for Matrix media upload."""

    path: Path
    filename: str
    txn_id: str
    body: str
    info: dict[str, int | str]
    log_context: dict[str, Any]


@dataclass(frozen=True)
class SnapshotRetentionResult:
    """Safe summary of an event snapshot retention pruning attempt."""

    pruned_count: int = 0
    pruned_bytes: int = 0
    retained_count: int = 0
    failed_count: int = 0


class MatrixError(RuntimeError):
    """Safe Matrix delivery error with structured, redacted diagnostics."""

    def __init__(self, message: str, **diagnostics: Any) -> None:
        self.message = redact_diagnostic_text(message)
        self.diagnostics = _sanitize_diagnostics(diagnostics)
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class MatrixDelivery:
    """Runtime delivery façade for parking events sent to one Matrix room."""

    def __init__(
        self,
        *,
        client: MatrixClient,
        room_id: str,
        data_dir: str | Path,
        snapshots_dir: str | Path | None,
        logger: StructuredLogger,
        snapshot_retention_count: int = 50,
    ) -> None:
        self.client = client
        self.room_id = room_id
        self.data_dir = Path(data_dir)
        self.snapshots_dir = Path(snapshots_dir) if snapshots_dir is not None else None
        self.logger = logger
        self.snapshot_retention_count = snapshot_retention_count

    def send_quiet_window_notice(self, event: Mapping[str, Any]) -> None:
        event_id = _require_non_empty("event_id", str(event.get("event_id", "")))
        self.client.send_text(room_id=self.room_id, txn_id=event_id, body=format_quiet_window_notice(event))

    def send_open_spot_alert(self, event: Mapping[str, Any]) -> None:
        event_id = open_spot_event_id(event)
        self.client.send_text(
            room_id=self.room_id,
            txn_id=f"{event_id}:text",
            body=format_open_spot_alert(event),
        )
        snapshot = prepare_event_snapshot(
            source_path=str(event.get("snapshot_path", "")),
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type=OPEN_SPOT_EVENT_TYPE,
            event_id=event_id,
            spot_id=str(event.get("spot_id", "")),
            observed_at=event.get("observed_at"),
            snapshot_retention_count=self.snapshot_retention_count,
            logger=self.logger,
            retention_trigger="matrix-event",
        )
        self.logger.info("matrix-snapshot-copied", **snapshot.log_context, txn_id=snapshot.txn_id)
        content_uri = self.client.upload_image(
            filename=snapshot.filename,
            data=snapshot.path.read_bytes(),
            content_type=JPEG_MIMETYPE,
        )
        self.client.send_image(
            room_id=self.room_id,
            txn_id=f"{event_id}:image",
            body=snapshot.body,
            content_uri=content_uri,
            info=snapshot.info,
        )

    def send_occupied_spot_alert(self, event: Mapping[str, Any]) -> None:
        event_id = occupied_spot_event_id(event)
        spot_id = _require_non_empty("spot_id", str(event.get("spot_id", "")))
        observed_at = event.get("observed_at")
        source_path = str(event.get("occupied_snapshot_path", ""))
        if not source_path.strip():
            raise MatrixError(
                "Matrix occupied snapshot path is required",
                error_type="snapshot_missing_source",
                event_type=OCCUPIED_SPOT_EVENT_TYPE,
                event_id=event_id,
                spot_id=spot_id,
            )

        if self.logger is not None:
            self.logger.info(
                "matrix-send-attempt",
                event_type=OCCUPIED_SPOT_EVENT_TYPE,
                event_id=event_id,
                spot_id=spot_id,
                operation="occupied-alert",
            )
        try:
            self.client.send_text(
                room_id=self.room_id,
                txn_id=f"{event_id}:text",
                body=format_occupied_spot_alert(event),
            )
            snapshot = prepare_event_snapshot(
                source_path=source_path,
                data_dir=self.data_dir,
                snapshots_dir=self.snapshots_dir,
                event_type=OCCUPIED_SPOT_EVENT_TYPE,
                event_id=event_id,
                spot_id=spot_id,
                observed_at=observed_at,
                snapshot_retention_count=self.snapshot_retention_count,
                logger=self.logger,
                retention_trigger="matrix-event",
            )
            if self.logger is not None:
                self.logger.info("matrix-snapshot-copied", **snapshot.log_context, txn_id=snapshot.txn_id)
            content_uri = self.client.upload_image(
                filename=snapshot.filename,
                data=snapshot.path.read_bytes(),
                content_type=JPEG_MIMETYPE,
            )
            self.client.send_image(
                room_id=self.room_id,
                txn_id=f"{event_id}:image",
                body=_occupied_snapshot_body(spot_id=spot_id, observed_at=observed_at),
                content_uri=content_uri,
                info=snapshot.info,
            )
        except Exception as exc:
            if self.logger is not None:
                self.logger.warning(
                    "matrix-send-failed",
                    event_type=OCCUPIED_SPOT_EVENT_TYPE,
                    event_id=event_id,
                    spot_id=spot_id,
                    operation="occupied-alert",
                    error_type=exc.__class__.__name__,
                )
            raise
        if self.logger is not None:
            self.logger.info(
                "matrix-send-succeeded",
                event_type=OCCUPIED_SPOT_EVENT_TYPE,
                event_id=event_id,
                spot_id=spot_id,
                operation="occupied-alert",
            )

    def send_live_proof(self, *, latest_path: str | Path, observed_at: object, selected_mode: object) -> None:
        self.send_live_proof_text(observed_at=observed_at, selected_mode=selected_mode)
        self.send_live_proof_image(latest_path=latest_path, observed_at=observed_at, selected_mode=selected_mode)

    def send_live_proof_text(self, *, observed_at: object, selected_mode: object) -> str:
        txn_base = live_proof_event_id(observed_at)
        return self.client.send_text(
            room_id=self.room_id,
            txn_id=f"{txn_base}:text",
            body=format_live_proof_text(observed_at=observed_at, selected_mode=selected_mode),
        )

    def send_live_proof_image(self, *, latest_path: str | Path, observed_at: object, selected_mode: object) -> str:
        txn_base = live_proof_event_id(observed_at)
        snapshot = prepare_event_snapshot(
            source_path=latest_path,
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type="live-proof",
            event_id=txn_base,
            spot_id="camera",
            observed_at=observed_at,
            snapshot_retention_count=self.snapshot_retention_count,
            logger=self.logger,
            retention_trigger="live-proof",
        )
        if self.logger is not None:
            self.logger.info("matrix-live-proof-snapshot-copied", **snapshot.log_context, txn_id=snapshot.txn_id)
        content_uri = self.client.upload_image(
            filename=snapshot.filename,
            data=snapshot.path.read_bytes(),
            content_type=JPEG_MIMETYPE,
        )
        return self.client.send_image(
            room_id=self.room_id,
            txn_id=f"{txn_base}:image",
            body=format_live_proof_image_body(observed_at=observed_at),
            content_uri=content_uri,
            info=snapshot.info,
        )


class MatrixClient:
    """Synchronous Matrix Client-Server API boundary for alerts."""

    def __init__(
        self,
        *,
        homeserver: str,
        access_token: str,
        timeout_seconds: float = 10,
        retry_attempts: int = 1,
        retry_backoff_seconds: float = 0,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        logger: StructuredLogger | None = None,
    ) -> None:
        self.homeserver = homeserver.rstrip("/")
        self.access_token = access_token
        self.timeout_seconds = timeout_seconds
        self.retry_attempts = max(1, retry_attempts)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self._sleep = sleep
        self._logger = logger
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(base_url=self.homeserver, timeout=timeout_seconds)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> MatrixClient:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def sync(self, *, room_id: str, since: str | None = None, timeout_ms: int = 0, limit: int = 20) -> MatrixSyncResult:
        """Poll Matrix /sync and return only safe text events for one joined room."""

        room_id = _require_non_empty("room_id", room_id)
        params: dict[str, Any] = {"timeout": max(0, int(timeout_ms)), "limit": max(1, min(int(limit), 100))}
        if since is not None and since.strip():
            params["since"] = since
        response = self._request_once("GET", f"{CLIENT_API_PREFIX}/sync", attempt=1, params=params)
        try:
            payload = response.json()
        except ValueError as exc:
            raise MatrixError(
                "Matrix sync response was not valid JSON",
                error_type="malformed_response",
                operation="sync",
                status_code=response.status_code,
                missing_key="next_batch",
            ) from exc
        return _parse_sync_response(payload, room_id=room_id, operation="sync", status_code=response.status_code)

    def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
        body = _require_non_empty("body", body)
        path = _room_message_path(room_id, txn_id)
        response_key = self._request_required_key(
            operation="send_text",
            response_key="event_id",
            method="PUT",
            path=path,
            json={"msgtype": "m.text", "body": body},
        )
        return response_key

    def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
        filename = _require_non_empty("filename", filename)
        content_type = _require_non_empty("content_type", content_type)
        return self._request_required_key(
            operation="upload_image",
            response_key="content_uri",
            method="POST",
            path=f"{MEDIA_API_PREFIX}/upload",
            params={"filename": filename},
            content=data,
            headers={"Content-Type": content_type},
        )

    def send_image(
        self,
        *,
        room_id: str,
        txn_id: str,
        body: str,
        content_uri: str,
        info: Mapping[str, Any],
    ) -> str:
        body = _require_non_empty("body", body)
        content_uri = _require_non_empty("content_uri", content_uri)
        path = _room_message_path(room_id, txn_id)
        return self._request_required_key(
            operation="send_image",
            response_key="event_id",
            method="PUT",
            path=path,
            json={"msgtype": "m.image", "body": body, "url": content_uri, "info": dict(info)},
        )

    def _request_required_key(self, *, operation: str, response_key: str, method: str, path: str, **kwargs: Any) -> str:
        last_error: MatrixError | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = self._request_once(method, path, attempt=attempt, **kwargs)
                return _require_response_key(response, response_key, operation=operation, attempt=attempt)
            except MatrixError as exc:
                last_error = exc
                if not self._should_retry(exc, attempt):
                    raise
                self._log_retry_decision(error=exc, operation=operation, path=path, attempt=attempt)
                if self.retry_backoff_seconds:
                    self._sleep(self.retry_backoff_seconds)
        if last_error is not None:
            raise last_error
        raise MatrixError("Matrix request failed", error_type="request_error", operation=operation, path=path)

    def _request_once(self, method: str, path: str, *, attempt: int, **kwargs: Any) -> httpx.Response:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        headers.update(kwargs.pop("headers", {}))
        try:
            response = self._client.request(
                method,
                self.homeserver + path if not path.startswith("http") else path,
                headers=headers,
                timeout=self.timeout_seconds,
                **kwargs,
            )
        except httpx.TimeoutException as exc:
            raise MatrixError(
                "Matrix request timed out",
                error_type="timeout",
                operation=method,
                path=path,
                attempt=attempt,
                exception_type=exc.__class__.__name__,
            ) from exc
        except httpx.RequestError as exc:
            raise MatrixError(
                "Matrix request failed",
                error_type="request_error",
                operation=method,
                path=path,
                attempt=attempt,
                exception_type=exc.__class__.__name__,
            ) from exc

        if response.status_code >= 400:
            raise _http_status_error(response, method=method, path=path, attempt=attempt)
        return response

    def _should_retry(self, error: MatrixError, attempt: int) -> bool:
        if attempt >= self.retry_attempts:
            return False
        error_type = error.diagnostics.get("error_type")
        if error_type in {"timeout", "request_error", "malformed_response"}:
            return True
        if error_type == "http_status":
            return error.diagnostics.get("status_code") in {429, 500, 502, 503, 504}
        return False

    def _log_retry_decision(self, *, error: MatrixError, operation: str, path: str, attempt: int) -> None:
        if self._logger is None:
            return
        diagnostics = dict(error.diagnostics)
        diagnostics.pop("operation", None)
        diagnostics.pop("path", None)
        diagnostics.pop("attempt", None)
        self._logger.info(
            "matrix-request-retry",
            operation=operation,
            path=path,
            attempt=attempt,
            next_attempt=attempt + 1,
            max_attempts=self.retry_attempts,
            backoff_seconds=self.retry_backoff_seconds,
            **diagnostics,
        )



class MatrixCommandParseError(ValueError):
    """Safe parse error for an inbound Matrix command."""


class MatrixCommandService:
    """Poll Matrix commands, authorize them, and apply archive corrections."""

    def __init__(
        self,
        *,
        client: MatrixClient,
        archive: Any,
        room_id: str,
        authorized_senders: list[str] | tuple[str, ...],
        command_prefix: str = "!parking",
        bot_user_id: str | None = None,
        logger: StructuredLogger | None = None,
        sync_timeout_ms: int = 0,
        sync_limit: int = 20,
    ) -> None:
        self.client = client
        self.archive = archive
        self.room_id = _require_non_empty("room_id", room_id)
        self.authorized_senders = frozenset(sender for sender in authorized_senders if sender)
        self.command_prefix = _require_non_empty("command_prefix", command_prefix)
        self.bot_user_id = bot_user_id
        self.logger = logger
        self.sync_timeout_ms = sync_timeout_ms
        self.sync_limit = sync_limit

    def poll_once(self) -> MatrixCommandPollResult:
        cursor = self.archive.read_matrix_cursor()
        since = cursor.get("next_batch") if isinstance(cursor, Mapping) else None
        result = self.client.sync(room_id=self.room_id, since=since, timeout_ms=self.sync_timeout_ms, limit=self.sync_limit)
        if not since:
            self.archive.write_matrix_cursor({"next_batch": result.next_batch})
            self._log("info", "matrix-command-sync", phase="bootstrap", next_batch_present=True, processed_count=0, ignored_count=len(result.events))
            return MatrixCommandPollResult(next_batch=result.next_batch, processed_count=0, ignored_count=len(result.events), error_count=0, bootstrapped=True)

        processed_count = 0
        ignored_count = 0
        error_count = 0
        for event in result.events:
            outcome = self._handle_event(event)
            if outcome == "processed":
                processed_count += 1
            elif outcome == "error":
                error_count += 1
            else:
                ignored_count += 1
        self.archive.write_matrix_cursor({"next_batch": result.next_batch})
        self._log("info", "matrix-command-sync", phase="apply", next_batch_present=True, processed_count=processed_count, ignored_count=ignored_count, error_count=error_count)
        return MatrixCommandPollResult(next_batch=result.next_batch, processed_count=processed_count, ignored_count=ignored_count, error_count=error_count)

    def _handle_event(self, event: MatrixTextEvent) -> str:
        context = {"phase": "command", "sender": event.sender, "event_id": event.event_id, "room_id": event.room_id}
        if event.room_id != self.room_id:
            self._log("info", "matrix-command-ignored", reason="wrong-room", **context)
            return "ignored"
        if self.bot_user_id and event.sender == self.bot_user_id:
            self._log("info", "matrix-command-ignored", reason="self-message", **context)
            return "ignored"
        if not event.body.strip().startswith(self.command_prefix):
            return "ignored"
        if event.sender not in self.authorized_senders:
            self._log("warning", "matrix-command-denied", reason="unauthorized-sender", **context)
            self._send_reply(event, "Command rejected: sender is not authorized.")
            return "error"
        try:
            command = parse_matrix_command(event.body, command_prefix=self.command_prefix)
        except MatrixCommandParseError as exc:
            self._log("warning", "matrix-command-parse-failed", reason=str(exc), **context)
            self._send_reply(event, f"Command rejected: {exc}")
            return "error"
        try:
            reply = self._apply_command(command, event=event)
        except Exception as exc:
            self._log("warning", "matrix-command-apply-failed", action=command.action, error_type=exc.__class__.__name__, **context)
            self._send_reply(event, f"Command failed: {redact_diagnostic_text(exc.__class__.__name__)}")
            return "error"
        self._send_reply(event, reply)
        self._log("info", "matrix-command-applied", action=command.action, **context)
        return "processed"

    def _apply_command(self, command: MatrixCommand, *, event: MatrixTextEvent) -> str:
        metadata = {"matrix_event_id": event.event_id, "matrix_sender": event.sender, "matrix_room_id": event.room_id}
        if self._correction_already_seen(event.event_id):
            return "Command already applied; acknowledgement repeated."
        if command.action == "rename_profile":
            assert command.profile_id is not None and command.label is not None
            applied = self.archive.rename_profile(command.profile_id, command.label, **metadata)
            return f"Profile {command.profile_id} renamed to {command.label}. Correction {applied.correction_id} recorded."
        if command.action == "merge_profiles":
            assert command.source_profile_id is not None and command.target_profile_id is not None
            applied = self.archive.merge_profiles(command.source_profile_id, command.target_profile_id, **metadata)
            return f"Profile {command.source_profile_id} merged into {command.target_profile_id}. Correction {applied.correction_id} recorded."
        if command.action == "wrong_match":
            assert command.subject_id is not None
            session_id = self._resolve_wrong_match_subject(command.subject_id)
            applied = self.archive.mark_wrong_match(session_id, matrix_event_id=event.event_id, matrix_sender=event.sender, matrix_room_id=event.room_id)
            return f"Wrong match recorded for session {session_id}. Correction {applied.correction_id} recorded."
        if command.action == "profile_summary":
            assert command.profile_id is not None
            summary = self._profile_summary(command.profile_id, event=event)
            return _format_profile_summary_reply(summary)
        raise MatrixCommandParseError("unknown command")

    def _profile_summary(self, profile_id: str, *, event: MatrixTextEvent) -> Mapping[str, Any]:
        try:
            return self.archive.profile_summary(profile_id, matrix_event_id=event.event_id, matrix_sender=event.sender, matrix_room_id=event.room_id)
        except TypeError:
            return self.archive.profile_summary(profile_id)

    def _resolve_wrong_match_subject(self, subject_id: str) -> str:
        for record in [*self.archive.load_active_sessions(), *self.archive.list_closed_sessions()]:
            if getattr(record, "session_id", None) == subject_id:
                return subject_id
        matches = [record for record in [*self.archive.load_active_sessions(), *self.archive.list_closed_sessions()] if getattr(record, "spot_id", None) == subject_id]
        if not matches:
            return subject_id
        matches.sort(key=lambda record: str(getattr(record, "ended_at", None) or getattr(record, "started_at", "")))
        return str(getattr(matches[-1], "session_id"))

    def _correction_already_seen(self, event_id: str) -> bool:
        load = getattr(self.archive, "load_corrections", None)
        if not callable(load):
            return False
        try:
            return any(getattr(correction, "matrix_event_id", None) == event_id for correction in load())
        except Exception:
            return False

    def _send_reply(self, event: MatrixTextEvent, body: str) -> None:
        self.client.send_text(room_id=self.room_id, txn_id=f"command:{event.event_id}", body=body)

    def _log(self, level: str, event_name: str, **fields: Any) -> None:
        if self.logger is None:
            return
        safe_fields = _sanitize_diagnostics(fields)
        log = getattr(self.logger, level)
        log(event_name, **safe_fields)


def parse_matrix_command(body: str, *, command_prefix: str = "!parking") -> MatrixCommand:
    if not isinstance(body, str):
        raise MatrixCommandParseError("body must be text")
    if len(body.encode("utf-8")) > 512:
        raise MatrixCommandParseError("body is too large")
    text = " ".join(body.strip().split())
    if not text:
        raise MatrixCommandParseError("body is blank")
    prefix = _require_non_empty("command_prefix", command_prefix)
    if text != prefix and not text.startswith(prefix + " "):
        raise MatrixCommandParseError("command prefix is required")
    parts = text.split(" ")
    if len(parts) < 2:
        raise MatrixCommandParseError("command action is required")
    if parts[1:3] == ["profile", "rename"]:
        if len(parts) < 5:
            raise MatrixCommandParseError("usage: !parking profile rename <profile_id> <label>")
        profile_id = _validate_profile_id(parts[3], "profile_id")
        label = _validate_label(" ".join(parts[4:]))
        return MatrixCommand(action="rename_profile", profile_id=profile_id, label=label)
    if parts[1:3] == ["profile", "merge"]:
        if len(parts) != 5:
            raise MatrixCommandParseError("usage: !parking profile merge <source_profile_id> <target_profile_id>")
        source = _validate_profile_id(parts[3], "source_profile_id")
        target = _validate_profile_id(parts[4], "target_profile_id")
        if source == target:
            raise MatrixCommandParseError("source and target profiles must differ")
        return MatrixCommand(action="merge_profiles", source_profile_id=source, target_profile_id=target)
    if parts[1:3] == ["profile", "summary"]:
        if len(parts) != 4:
            raise MatrixCommandParseError("usage: !parking profile summary <profile_id>")
        return MatrixCommand(action="profile_summary", profile_id=_validate_profile_id(parts[3], "profile_id"))
    if parts[1] == "wrong":
        if len(parts) != 3:
            raise MatrixCommandParseError("usage: !parking wrong <spot_id|session_id>")
        return MatrixCommand(action="wrong_match", subject_id=_validate_subject_id(parts[2]))
    raise MatrixCommandParseError("unknown command")


def _parse_sync_response(payload: Any, *, room_id: str, operation: str, status_code: int) -> MatrixSyncResult:
    if not isinstance(payload, dict):
        raise MatrixError("Matrix sync response was malformed", error_type="malformed_response", operation=operation, status_code=status_code, missing_key="next_batch")
    next_batch = payload.get("next_batch")
    if not isinstance(next_batch, str) or not next_batch:
        raise MatrixError("Matrix sync response was missing a required field", error_type="malformed_response", operation=operation, status_code=status_code, missing_key="next_batch")
    events_payload = (((payload.get("rooms") or {}).get("join") or {}).get(room_id) or {}).get("timeline", {}).get("events", [])
    if not isinstance(events_payload, list):
        raise MatrixError("Matrix sync response room timeline was malformed", error_type="malformed_response", operation=operation, status_code=status_code, missing_key="rooms.join.timeline.events")
    events: list[MatrixTextEvent] = []
    for item in events_payload:
        if not isinstance(item, Mapping) or item.get("type") != "m.room.message":
            continue
        content = item.get("content")
        if not isinstance(content, Mapping) or content.get("msgtype") != "m.text":
            continue
        body = content.get("body")
        event_id = item.get("event_id")
        sender = item.get("sender")
        if isinstance(body, str) and isinstance(event_id, str) and isinstance(sender, str):
            events.append(MatrixTextEvent(event_id=event_id, sender=sender, room_id=room_id, body=body[:512]))
    return MatrixSyncResult(next_batch=next_batch, events=tuple(events))


def _validate_profile_id(value: str, name: str) -> str:
    if not re.fullmatch(r"prof_[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", value):
        raise MatrixCommandParseError(f"invalid {name}")
    return value


def _validate_subject_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,219}", value):
        raise MatrixCommandParseError("invalid subject id")
    return value


def _validate_label(value: str) -> str:
    label = " ".join(value.strip().split())
    if not label:
        raise MatrixCommandParseError("label is required")
    if len(label) > 160:
        raise MatrixCommandParseError("label is too long")
    if re.search(r"[\x00-\x1f\x7f]", label):
        raise MatrixCommandParseError("label contains control characters")
    return label


def _format_profile_summary_reply(summary: Mapping[str, Any]) -> str:
    profile_id = _safe_text(summary.get("profile_id"), default="unknown")
    label = _safe_text(summary.get("label"), default="unlabeled")
    closed = _int_field(summary, "closed_session_count", default=0)
    active = _int_field(summary, "active_session_count", default=0)
    excluded = _int_field(summary, "wrong_match_excluded_session_count", default=0)
    estimate_status = _safe_text(summary.get("estimate_status"), default="unknown")
    estimate_samples = _int_field(summary, "estimate_sample_count", default=0)
    return (
        f"Profile {profile_id}: {label}\n"
        f"Sessions: {closed} closed, {active} active, {excluded} wrong-match excluded\n"
        f"Estimate: {estimate_status} from {estimate_samples} samples"
    )

def prepare_event_snapshot(
    *,
    source_path: str | Path,
    data_dir: str | Path,
    snapshots_dir: str | Path | None,
    event_type: str,
    event_id: str,
    spot_id: str | None,
    observed_at: object,
    snapshot_retention_count: int | None = None,
    logger: StructuredLogger | None = None,
    retention_trigger: str = "matrix-event",
) -> MatrixSnapshot:
    """Copy a raw full-frame snapshot into a stable event-specific Matrix evidence file.

    The helper intentionally rejects local debug overlays and validates JPEG
    metadata before callers can upload bytes as an ``m.image`` message.
    """

    source = Path(source_path)
    event_type_text = _require_non_empty("event_type", event_type)
    event_id_text = _require_non_empty("event_id", event_id)
    observed_text = _format_observed_at(observed_at)
    snapshot_root = Path(snapshots_dir) if snapshots_dir is not None else Path(data_dir) / "snapshots"
    filename = _snapshot_filename(
        event_type=event_type_text,
        stable_id=spot_id or event_id_text,
        observed_at=observed_text,
    )
    destination = snapshot_root / filename

    if source.name == "debug_latest.jpg":
        raise MatrixError(
            "Matrix snapshot source cannot be the local debug overlay",
            error_type="snapshot_invalid_source",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
        )

    try:
        snapshot_root.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    except OSError as exc:
        raise MatrixError(
            "Matrix snapshot copy failed",
            error_type="snapshot_copy_failed",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
            exception_type=exc.__class__.__name__,
        ) from exc

    byte_size = destination.stat().st_size
    try:
        width, height = _jpeg_dimensions(destination)
    except (OSError, UnidentifiedImageError) as exc:
        raise MatrixError(
            "Matrix snapshot metadata could not be read as JPEG",
            error_type="snapshot_metadata_failed",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
            byte_size=byte_size,
            exception_type=exc.__class__.__name__,
        ) from exc

    info: dict[str, int | str] = {"mimetype": JPEG_MIMETYPE, "size": byte_size, "w": width, "h": height}
    log_context = _sanitize_diagnostics(
        {
            "event_type": event_type_text,
            "event_id": event_id_text,
            "spot_id": spot_id,
            "source_path": str(source),
            "snapshot_path": str(destination),
            "byte_size": byte_size,
            "mimetype": JPEG_MIMETYPE,
            "width": width,
            "height": height,
        }
    )
    if snapshot_retention_count is not None:
        prune_event_snapshots(
            snapshot_root,
            retention_count=snapshot_retention_count,
            logger=logger,
            current_snapshot=destination,
            trigger=retention_trigger,
        )
    return MatrixSnapshot(
        path=destination,
        filename=filename,
        txn_id=f"snapshot-{Path(filename).stem}",
        body=_snapshot_body(spot_id=spot_id, observed_at=observed_text),
        info=info,
        log_context=log_context,
    )


def prune_event_snapshots(
    snapshot_root: str | Path,
    retention_count: int,
    logger: StructuredLogger | None,
    *,
    current_snapshot: str | Path | None = None,
    trigger: str = "manual",
) -> SnapshotRetentionResult:
    """Prune oldest Matrix event snapshot files while preserving unrelated runtime files.

    Only JPEG names following the event snapshot contract generated by
    ``prepare_event_snapshot`` are considered. Missing directories are empty,
    malformed filenames are ignored, and deletion failures are logged without
    raising so Matrix delivery and monitor startup can continue.
    """

    root = Path(snapshot_root)
    if retention_count < 1:
        _log_retention_failure(
            logger,
            root=root,
            trigger=trigger,
            error_type="ValueError",
            message="snapshot retention count must be positive",
        )
        return SnapshotRetentionResult(failed_count=1)
    if not root.exists():
        return SnapshotRetentionResult()
    try:
        candidates = [path for path in root.iterdir() if path.is_file() and _is_event_snapshot_file(path)]
    except OSError as exc:
        _log_retention_failure(logger, root=root, trigger=trigger, error_type=type(exc).__name__, message=str(exc))
        return SnapshotRetentionResult(failed_count=1)

    candidates.sort(key=lambda path: (_safe_mtime_ns(path), path.name))
    retained_count = min(len(candidates), retention_count)
    if len(candidates) <= retention_count:
        return SnapshotRetentionResult(retained_count=len(candidates))

    current = Path(current_snapshot).resolve() if current_snapshot is not None else None
    to_delete = candidates[: len(candidates) - retention_count]
    pruned_count = 0
    pruned_bytes = 0
    failed_count = 0
    for path in to_delete:
        if current is not None and _same_path(path, current):
            failed_count += 1
            _log_retention_failure(
                logger,
                root=root,
                trigger=trigger,
                error_type="RetentionInvariantError",
                message="retention attempted to delete current snapshot",
            )
            continue
        try:
            byte_size = path.stat().st_size
            path.unlink()
        except OSError as exc:
            failed_count += 1
            _log_retention_failure(logger, root=root, trigger=trigger, error_type=type(exc).__name__, message=str(exc))
            continue
        pruned_count += 1
        pruned_bytes += byte_size

    if pruned_count:
        _log_retention_pruned(
            logger,
            root=root,
            trigger=trigger,
            pruned_count=pruned_count,
            pruned_bytes=pruned_bytes,
            retained_count=len(candidates) - pruned_count,
        )
    return SnapshotRetentionResult(
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
        retained_count=len(candidates) - pruned_count,
        failed_count=failed_count,
    )


def open_spot_event_id(event: Mapping[str, Any]) -> str:
    """Return the stable Matrix transaction base for a confirmed open event."""

    event_type = _require_non_empty("event_type", str(event.get("event_type", OPEN_SPOT_EVENT_TYPE)))
    spot_id = _require_non_empty("spot_id", str(event.get("spot_id", "")))
    observed_at = _format_observed_at(event.get("observed_at"))
    return f"{event_type}:{spot_id}:{observed_at}"


def occupied_spot_event_id(event: Mapping[str, Any]) -> str:
    """Return the stable Matrix transaction base for a confirmed occupied event."""

    event_type = _require_non_empty("event_type", str(event.get("event_type", OCCUPIED_SPOT_EVENT_TYPE)))
    spot_id = _require_non_empty("spot_id", str(event.get("spot_id", "")))
    observed_at = _format_observed_at(event.get("observed_at"))
    return f"{event_type}:{spot_id}:{observed_at}"


_EVENT_SNAPSHOT_PATTERN = re.compile(
    r"^[a-z0-9]+(?:-[a-z0-9]+)*-.+-\d{4}-\d{2}-\d{2}t\d{2}-\d{2}-\d{2}z\.jpg$"
)


def _is_event_snapshot_file(path: Path) -> bool:
    return bool(_EVENT_SNAPSHOT_PATTERN.match(path.name))


def _safe_mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return 0


def _same_path(path: Path, current: Path) -> bool:
    try:
        return path.resolve() == current
    except OSError:
        return False


def _log_retention_pruned(
    logger: StructuredLogger | None,
    *,
    root: Path,
    trigger: str,
    pruned_count: int,
    pruned_bytes: int,
    retained_count: int,
) -> None:
    if logger is None:
        return
    logger.info(
        "snapshot-retention-pruned",
        root=str(root),
        trigger=trigger,
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
        retained_count=retained_count,
    )


def _log_retention_failure(
    logger: StructuredLogger | None,
    *,
    root: Path,
    trigger: str,
    error_type: str,
    message: str,
    failed_count: int = 1,
    pruned_count: int = 0,
    pruned_bytes: int = 0,
) -> None:
    if logger is None:
        return
    logger.warning(
        "snapshot-retention-failed",
        root=str(root),
        trigger=trigger,
        error_type=error_type,
        message=message,
        failed_count=failed_count,
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
    )


def _display_observed_at(value: object) -> str:
    observed_at = _parse_observed_at(value)
    if observed_at is None:
        return _format_observed_at(value).replace("Z", "+00:00")
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        return observed_at.isoformat()
    return observed_at.astimezone(DISPLAY_TIMEZONE).isoformat()


def _parse_observed_at(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = _require_non_empty("observed_at", value)
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def format_open_spot_alert(event: Mapping[str, Any]) -> str:
    """Return deterministic Matrix text for a confirmed parking-open event."""

    spot_id = _require_non_empty("spot_id", redact_diagnostic_text(event.get("spot_id", "")))
    observed_at = _display_observed_at(event.get("observed_at"))
    return f"Parking spot open: {spot_id} at {observed_at}"




def format_occupied_spot_alert(event: Mapping[str, Any]) -> str:
    """Return deterministic Matrix text for a confirmed parking-occupied event.

    The formatter is intentionally metadata-only: it never opens snapshot files
    and only reads an allowlist of alert-safe fields from the provided mapping.
    """

    spot_id = _require_non_empty("spot_id", redact_diagnostic_text(event.get("spot_id", "")))
    observed_at = _display_observed_at(event.get("observed_at"))
    vehicle = _mapping_field(event, "likely_vehicle")
    estimate = _mapping_field(event, "vehicle_history_estimate") or _mapping_field(event, "history_estimate")

    label = _safe_text(_first_present(vehicle, "label", "vehicle_label", "display_label"), default="unknown vehicle")
    profile_id = _safe_text(
        _first_present(vehicle, "profile_id") or _first_present(estimate, "profile_id") or event.get("profile_id"),
        default="unknown",
    )
    match_status = _safe_text(
        _first_present(vehicle, "match_status", "status") or event.get("match_status"),
        default="unknown",
    )
    match_confidence = _safe_text(
        _first_present(vehicle, "confidence", "profile_confidence") or event.get("profile_confidence"),
        default="unknown",
    )

    lines = [f"Parking spot occupied: {spot_id} at {observed_at}"]

    estimate_status = _safe_text(_first_present(estimate, "status"), default="insufficient_history")
    sample_count = _int_field(estimate, "sample_count", default=0)
    estimate_confidence = _safe_text(_first_present(estimate, "confidence"), default="unknown")
    has_useful_vehicle_context = label != "unknown vehicle" or sample_count > 0 or estimate_status == "estimated" or match_status != "new_profile"
    if not has_useful_vehicle_context:
        return "\n".join(lines)

    lines.extend(
        [
            f"Likely vehicle: {label} (profile {profile_id})",
            f"Match: {match_status}, confidence {match_confidence}",
        ]
    )

    if estimate_status == "estimated":
        dwell_range = _mapping_field(estimate, "dwell_range")
        leave_window = _mapping_field(estimate, "leave_time_window")
        lines.append(f"Estimated dwell: {_format_dwell_range(dwell_range)}")
        lines.append(f"Usual leave window: {_format_leave_window(leave_window)}")
    else:
        reason = _safe_text(_first_present(estimate, "reason"), default="insufficient-history")
        lines.append(f"Estimate unavailable: {reason}")
    lines.append(f"History: {sample_count} {_plural('sample', sample_count)}, estimate confidence {estimate_confidence}")
    return "\n".join(lines)


def _mapping_field(source: object, name: str) -> Mapping[str, Any]:
    if isinstance(source, Mapping):
        value = source.get(name)
        if isinstance(value, Mapping):
            return value
        value = getattr(value, "__dict__", None)
        if isinstance(value, Mapping):
            return value
    return {}


def _first_present(source: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = source.get(name)
        if value is not None:
            return value
    return None


def _safe_text(value: object, *, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")
    text = redact_diagnostic_text(value).strip()
    return text or default


def _int_field(source: Mapping[str, Any], name: str, *, default: int) -> int:
    value = source.get(name)
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


def _format_dwell_range(dwell_range: Mapping[str, Any]) -> str:
    lower = _int_field(dwell_range, "lower_seconds", default=0)
    upper = _int_field(dwell_range, "upper_seconds", default=0)
    typical = _int_field(dwell_range, "typical_seconds", default=0)
    return f"{_format_duration(lower)}–{_format_duration(upper)} (typical {_format_duration(typical)})"


def _format_duration(seconds: int) -> str:
    seconds = max(0, seconds)
    total_minutes = int(round(seconds / 60))
    if total_minutes < 60:
        return f"{total_minutes} {_plural('min', total_minutes)}"
    hours, minutes = divmod(total_minutes, 60)
    hour_text = f"{hours} {_plural('hr', hours)}"
    if minutes == 0:
        return hour_text
    return f"{hour_text} {minutes} {_plural('min', minutes)}"


def _format_leave_window(leave_window: Mapping[str, Any]) -> str:
    start = _int_field(leave_window, "start_minute", default=0)
    end = _int_field(leave_window, "end_minute", default=0)
    typical = _int_field(leave_window, "typical_minute", default=0)
    crosses_midnight = bool(leave_window.get("crosses_midnight"))
    suffix = "; crosses midnight" if crosses_midnight else ""
    return f"{_format_minute_of_day(start)}–{_format_minute_of_day(end)} (typical {_format_minute_of_day(typical)}{suffix})"


def _format_minute_of_day(value: int) -> str:
    minute = value % (24 * 60)
    return f"{minute // 60:02d}:{minute % 60:02d}"


def _plural(word: str, count: int) -> str:
    if word == "min":
        return word
    return word if count == 1 else f"{word}s"


def _occupied_snapshot_body(*, spot_id: str, observed_at: object) -> str:
    return f"Raw occupied full-frame snapshot for {redact_diagnostic_text(spot_id)} at {_display_observed_at(observed_at)}"

def format_quiet_window_notice(event: Mapping[str, Any]) -> str:
    """Return deterministic Matrix text for a street-sweeping start/end notice."""

    event_type = _require_non_empty("event_type", str(event.get("event_type", "")))
    window_id = _require_non_empty("window_id", str(event.get("window_id", "")))
    if event_type == "quiet-window-upcoming":
        minutes_before = _int_field(event, "reminder_minutes_before", default=0)
        lead_time = _format_lead_time(minutes_before)
        return f"Street sweeping starts in {lead_time}: {window_id}"
    if event_type == "quiet-window-started":
        verb = "started"
    elif event_type == "quiet-window-ended":
        verb = "ended"
    else:
        verb = event_type
    return f"Street sweeping {verb}: {window_id}"


def _format_lead_time(minutes: int) -> str:
    if minutes == 60:
        return "1 hour"
    if minutes > 0 and minutes % 60 == 0:
        return f"{minutes // 60} hours"
    return f"{minutes} minutes"


def format_live_proof_text(*, observed_at: object, selected_mode: object) -> str:
    observed_text = _display_observed_at(observed_at)
    mode_text = str(getattr(selected_mode, "value", selected_mode))
    return f"LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at {observed_text} (decode mode: {mode_text})."


def format_live_proof_image_body(*, observed_at: object) -> str:
    observed_text = _display_observed_at(observed_at)
    return f"LIVE PROOF / TEST IMAGE: raw full-frame camera snapshot captured at {observed_text}."


def live_proof_event_id(observed_at: object) -> str:
    return f"live-proof:{_format_observed_at(observed_at)}"


def _jpeg_dimensions(path: Path) -> tuple[int, int]:
    with Image.open(path) as image:
        if image.format != "JPEG":
            raise OSError("snapshot is not a JPEG image")
        width, height = image.size
        image.verify()
    return width, height


def _snapshot_filename(*, event_type: str, stable_id: str, observed_at: str) -> str:
    return f"{_path_token(event_type)}-{_path_token(stable_id)}-{_path_token(observed_at)}.jpg"


def _path_token(value: object) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "-", redact_diagnostic_text(value).strip().lower()).strip("-")
    return token or "unknown"


def _format_observed_at(value: object) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return _require_non_empty("observed_at", str(value))


def _snapshot_body(*, spot_id: str | None, observed_at: str) -> str:
    subject = redact_diagnostic_text(spot_id) if spot_id else "parking spot"
    return f"Raw full-frame snapshot for {subject} at {observed_at.replace('Z', '+00:00')}"


def _room_message_path(room_id: str, txn_id: str) -> str:
    room_segment = quote(_require_non_empty("room_id", room_id), safe="")
    txn_segment = quote(_require_non_empty("txn_id", txn_id), safe="")
    return f"{CLIENT_API_PREFIX}/rooms/{room_segment}/send/m.room.message/{txn_segment}"


def _require_non_empty(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty")
    return value


def _require_response_key(response: httpx.Response, key: str, *, operation: str, attempt: int | None = None) -> str:
    try:
        payload = response.json()
    except ValueError as exc:
        raise MatrixError(
            "Matrix response was not valid JSON",
            error_type="malformed_response",
            operation=operation,
            status_code=response.status_code,
            missing_key=key,
            attempt=attempt,
        ) from exc
    value = payload.get(key) if isinstance(payload, dict) else None
    if not isinstance(value, str) or not value:
        raise MatrixError(
            "Matrix response was missing a required field",
            error_type="malformed_response",
            operation=operation,
            status_code=response.status_code,
            missing_key=key,
            attempt=attempt,
        )
    return value


def _http_status_error(response: httpx.Response, *, method: str, path: str, attempt: int) -> MatrixError:
    errcode = None
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("errcode"), str):
        errcode = payload["errcode"]
    return MatrixError(
        "Matrix request returned an error status",
        error_type="http_status",
        operation=method,
        path=path,
        attempt=attempt,
        status_code=response.status_code,
        errcode=errcode,
    )


_UNSAFE_DIAGNOSTIC_KEYS = {"raw_body", "response_body", "body", "headers", "authorization"}


def _sanitize_diagnostics(diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in diagnostics.items():
        if value is None or str(key).lower() in _UNSAFE_DIAGNOSTIC_KEYS:
            continue
        if isinstance(value, str):
            sanitized[key] = redact_diagnostic_text(value)
        elif isinstance(value, Mapping):
            sanitized[key] = _sanitize_diagnostics(value)
        else:
            sanitized[key] = value
    return sanitized
