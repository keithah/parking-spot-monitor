from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Protocol, TypeAlias

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.matrix_client import MatrixClient
from parking_spot_monitor.matrix_cockpit import MatrixOperatorCockpitContext
from parking_spot_monitor.matrix_command_catalog import (
    AppliedMatrixCommand,
    format_command_help_reply,
    parse_applied_matrix_command,
    parse_matrix_command,
)
from parking_spot_monitor.matrix_command_runtime import MatrixCommandArchive, MatrixCommandRuntime, MatrixFeedbackLabeler
from parking_spot_monitor.matrix_models import (
    MatrixCommand,
    MatrixCommandParseError,
    MatrixCommandPollResult,
    MatrixCommandResponse,
    MatrixSyncResult,
    MatrixTextEvent,
)
from parking_spot_monitor.matrix_snapshots import JPEG_MIMETYPE, MatrixSnapshot, _matrix_snapshot_upload
from parking_spot_monitor.matrix_support import _require_non_empty, _sanitize_diagnostics

_CockpitProvider: TypeAlias = Callable[..., str | MatrixCommandResponse]


class MatrixCommandServiceArchive(MatrixCommandArchive, Protocol):
    def read_matrix_cursor(self) -> Mapping[str, Any] | None: ...
    def write_matrix_cursor(self, state: Mapping[str, Any]) -> None: ...


class MatrixCommandService:
    """Poll Matrix commands, authorize them, and apply archive corrections."""

    def __init__(
        self,
        *,
        client: MatrixClient,
        archive: MatrixCommandServiceArchive,
        room_id: str,
        authorized_senders: list[str] | tuple[str, ...],
        command_prefix: str = "!parking",
        bot_user_id: str | None = None,
        logger: StructuredLogger | None = None,
        sync_timeout_ms: int = 0,
        sync_limit: int = 20,
        cockpit_provider: _CockpitProvider | None = None,
        who_snapshot_provider: Callable[[str], str | MatrixCommandResponse] | None = None,
        cockpit_context: MatrixOperatorCockpitContext | None = None,
        feedback_labeler: MatrixFeedbackLabeler | None = None,
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
        self.cockpit_provider = cockpit_provider
        self.who_snapshot_provider = who_snapshot_provider
        self.cockpit_context = cockpit_context
        self.feedback_labeler = feedback_labeler

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if callable(close):
            close()

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
            command = parse_applied_matrix_command(event.body, command_prefix=self.command_prefix)
        except MatrixCommandParseError as exc:
            self._log("warning", "matrix-command-parse-failed", reason=str(exc), **context)
            self._send_reply(event, f"Command rejected: {exc}")
            return "error"
        try:
            response = self._apply_command(command, event=event)
            self._send_command_response(event, response)
        except Exception as exc:
            self._log("warning", "matrix-command-apply-failed", action=command.action, error_type=exc.__class__.__name__, **context)
            try:
                self._send_reply(event, f"Command failed: {redact_diagnostic_text(exc.__class__.__name__)}")
            except Exception as reply_exc:
                self._log("warning", "matrix-command-failure-reply-failed", action=command.action, error_type=reply_exc.__class__.__name__, **context)
            return "error"
        self._log("info", "matrix-command-applied", action=command.action, **context)
        return "processed"

    def _apply_command(self, typed_command: AppliedMatrixCommand, *, event: MatrixTextEvent) -> str | MatrixCommandResponse:
        runtime = MatrixCommandRuntime(
            archive=self.archive,
            command_prefix=self.command_prefix,
            help_formatter=format_command_help_reply,
            logger=self.logger,
            cockpit_provider=self.cockpit_provider,
            who_snapshot_provider=self.who_snapshot_provider,
            cockpit_context=self.cockpit_context,
            feedback_labeler=self.feedback_labeler,
        )
        return typed_command.apply(runtime, event)

    def _send_command_response(self, event: MatrixTextEvent, response: str | MatrixCommandResponse) -> None:
        command_response = _coerce_command_response(response)
        if command_response.image_path is None:
            self._send_reply(event, command_response.text)
            return
        image_info = _validate_command_image_info(command_response.image_info)
        image_path = Path(command_response.image_path)
        self.client.send_text(room_id=self.room_id, txn_id=f"command:{event.event_id}:text", body=command_response.text)
        upload = _matrix_snapshot_upload(
            MatrixSnapshot(
                path=image_path,
                filename=image_path.name,
                txn_id=f"command:{event.event_id}:image",
                body=f"Raw full-frame {image_path.name} evidence",
                info=image_info,
                log_context={"snapshot_path": str(image_path), "event_id": event.event_id},
            ),
            logger=self.logger,
        )
        content_uri = self.client.upload_image(
            filename=image_path.name,
            data=upload["data"],
            content_type=JPEG_MIMETYPE,
        )
        self.client.send_image(
            room_id=self.room_id,
            txn_id=f"command:{event.event_id}:image",
            body=f"Raw full-frame {image_path.name} evidence",
            content_uri=content_uri,
            info=upload["info"],
        )

    def _send_reply(self, event: MatrixTextEvent, body: str) -> None:
        self.client.send_text(room_id=self.room_id, txn_id=f"command:{event.event_id}", body=body)

    def _log(self, level: str, event_name: str, **fields: Any) -> None:
        if self.logger is None:
            return
        safe_fields = _sanitize_diagnostics(fields)
        log = getattr(self.logger, level)
        log(event_name, **safe_fields)

def _coerce_command_response(response: str | MatrixCommandResponse) -> MatrixCommandResponse:
    if isinstance(response, MatrixCommandResponse):
        return response
    if isinstance(response, str):
        return MatrixCommandResponse(text=response)
    raise MatrixCommandParseError("operator cockpit response was malformed")

def _validate_command_image_info(info: Mapping[str, Any] | None) -> dict[str, int | str]:
    if not isinstance(info, Mapping):
        raise MatrixCommandParseError("operator cockpit image metadata was malformed")
    mimetype = info.get("mimetype")
    size = info.get("size")
    width = info.get("w")
    height = info.get("h")
    if mimetype != JPEG_MIMETYPE or not all(isinstance(value, int) and value > 0 for value in (size, width, height)):
        raise MatrixCommandParseError("operator cockpit image metadata was malformed")
    return {"mimetype": JPEG_MIMETYPE, "size": int(size), "w": int(width), "h": int(height)}
