from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, TypeAlias

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.matrix_client import MatrixClient
from parking_spot_monitor.matrix_alerts import _int_field, _safe_text
from parking_spot_monitor.matrix_cockpit import (
    MatrixOperatorCockpitContext,
    _active_spot_assignments_with_runtime_status,
    _format_active_spot_assignments_reply,
    build_incident_review_response,
    format_detection_lab_run_reply,
    format_detection_lab_status_reply,
    format_operator_analytics_reply,
    format_operator_confidence_reply,
    format_operator_config_reply,
    format_operator_recent_reply,
    format_operator_status_reply,
    format_operator_why_reply,
)
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

_CockpitRenderer: TypeAlias = Callable[[MatrixOperatorCockpitContext, StructuredLogger | None], MatrixCommandResponse]


@dataclass(frozen=True)
class _CockpitReplyCommand:
    action: str
    render: _CockpitRenderer = field(repr=False, compare=False)
    arguments: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class _CorrectSpotStateCommand:
    spot_id: str
    actual_state: str
    action: str = field(default="correct_spot_state", init=False)


@dataclass(frozen=True)
class _LearnLabelCommand:
    spot_id: str
    actual_state: str
    requested_time: str
    action: str = field(default="learn_label", init=False)


@dataclass(frozen=True)
class _RenameProfileCommand:
    profile_id: str
    label: str
    action: str = field(default="rename_profile", init=False)


@dataclass(frozen=True)
class _MergeProfilesCommand:
    source_profile_id: str
    target_profile_id: str
    action: str = field(default="merge_profiles", init=False)


@dataclass(frozen=True)
class _WrongMatchCommand:
    subject_id: str
    action: str = field(default="wrong_match", init=False)


@dataclass(frozen=True)
class _AssignOwnerCommand:
    spot_id: str
    action: str = field(default="assign_owner", init=False)


@dataclass(frozen=True)
class _ActiveSpotAssignmentsCommand:
    action: str = field(default="active_spot_assignments", init=False)


@dataclass(frozen=True)
class _HelpCommand:
    action: str = field(default="help", init=False)


@dataclass(frozen=True)
class _ProfileSummaryCommand:
    profile_id: str
    action: str = field(default="profile_summary", init=False)


_AppliedMatrixCommand: TypeAlias = (
    _CockpitReplyCommand
    | _CorrectSpotStateCommand
    | _LearnLabelCommand
    | _RenameProfileCommand
    | _MergeProfilesCommand
    | _WrongMatchCommand
    | _AssignOwnerCommand
    | _ActiveSpotAssignmentsCommand
    | _HelpCommand
    | _ProfileSummaryCommand
)


class _CockpitProvider(Protocol):
    def __call__(self, action: str, **kwargs: str) -> str | MatrixCommandResponse: ...


_CommandParser: TypeAlias = Callable[[list[str], str], _AppliedMatrixCommand]


@dataclass(frozen=True)
class _CommandSpec:
    triggers: tuple[tuple[str, ...], ...]
    help_text: str
    parse: _CommandParser


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
        cockpit_provider: _CockpitProvider | None = None,
        who_snapshot_provider: Callable[[str], str | MatrixCommandResponse] | None = None,
        cockpit_context: MatrixOperatorCockpitContext | None = None,
        feedback_labeler: Any | None = None,
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
            command = _parse_applied_matrix_command(event.body, command_prefix=self.command_prefix)
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

    def _apply_command(self, typed_command: _AppliedMatrixCommand, *, event: MatrixTextEvent) -> str | MatrixCommandResponse:
        metadata = {"matrix_event_id": event.event_id, "matrix_sender": event.sender, "matrix_room_id": event.room_id}
        if isinstance(typed_command, _CockpitReplyCommand):
            return self._format_cockpit_reply(typed_command)
        if isinstance(typed_command, _CorrectSpotStateCommand):
            if self.feedback_labeler is None:
                raise RuntimeError("operator feedback labeler is not configured")
            result = self.feedback_labeler.record_correction(
                spot_id=typed_command.spot_id,
                actual_state=typed_command.actual_state,
                matrix_event_id=event.event_id,
                matrix_sender=event.sender,
                matrix_room_id=event.room_id,
            )
            return result.reply_text
        if isinstance(typed_command, _LearnLabelCommand):
            if self.feedback_labeler is None:
                raise RuntimeError("operator feedback labeler is not configured")
            result = self.feedback_labeler.record_learn_label(
                spot_id=typed_command.spot_id,
                target_state=typed_command.actual_state,
                requested_time=typed_command.requested_time,
                settings=None if self.cockpit_context is None else self.cockpit_context.settings,
                state_path=None if self.cockpit_context is None else self.cockpit_context.state_path,
                detector=None if self.cockpit_context is None else self.cockpit_context.incident_detector,
                matrix_event_id=event.event_id,
                matrix_sender=event.sender,
                matrix_room_id=event.room_id,
            )
            return result.reply_text
        if isinstance(typed_command, (_RenameProfileCommand, _MergeProfilesCommand, _WrongMatchCommand)) and self._correction_already_seen(event.event_id):
            return "Command already applied; acknowledgement repeated."
        if isinstance(typed_command, _RenameProfileCommand):
            applied = self.archive.rename_profile(typed_command.profile_id, typed_command.label, **metadata)
            return f"Profile {typed_command.profile_id} renamed to {typed_command.label}. Correction {applied.correction_id} recorded."
        if isinstance(typed_command, _MergeProfilesCommand):
            applied = self.archive.merge_profiles(typed_command.source_profile_id, typed_command.target_profile_id, **metadata)
            return f"Profile {typed_command.source_profile_id} merged into {typed_command.target_profile_id}. Correction {applied.correction_id} recorded."
        if isinstance(typed_command, _WrongMatchCommand):
            session_id = self._resolve_wrong_match_subject(typed_command.subject_id)
            applied = self.archive.mark_wrong_match(session_id, matrix_event_id=event.event_id, matrix_sender=event.sender, matrix_room_id=event.room_id)
            return f"Wrong match recorded for session {session_id}. Correction {applied.correction_id} recorded."
        if isinstance(typed_command, _AssignOwnerCommand):
            assignment = self.archive.assign_owner_profile_to_active_spot(typed_command.spot_id)
            profile_id = _safe_text(getattr(assignment, "profile_id", None), default="unknown")
            session_id = _safe_text(getattr(assignment, "session_id", None), default="unknown")
            confidence = getattr(assignment, "profile_confidence", None)
            confidence_text = _confidence_text(confidence)
            return f"Owner vehicle assigned to {typed_command.spot_id}: session {session_id}, profile {profile_id}, confidence {confidence_text}."
        if isinstance(typed_command, _ActiveSpotAssignmentsCommand):
            assignments = _active_spot_assignments_with_runtime_status(self.archive.active_spot_assignments(), cockpit_context=self.cockpit_context, logger=self.logger)
            base_reply = _format_active_spot_assignments_reply(assignments)
            if self.who_snapshot_provider is not None:
                return self.who_snapshot_provider(base_reply)
            return base_reply
        if isinstance(typed_command, _HelpCommand):
            return _format_command_help_reply(self.command_prefix)
        if isinstance(typed_command, _ProfileSummaryCommand):
            summary = self._profile_summary(typed_command.profile_id, event=event)
            return _format_profile_summary_reply(summary)
        raise MatrixCommandParseError("unknown command")

    def _format_cockpit_reply(
        self,
        command: _CockpitReplyCommand,
    ) -> str | MatrixCommandResponse:
        if self.cockpit_provider is not None:
            return self.cockpit_provider(command.action, **dict(command.arguments))
        if self.cockpit_context is not None:
            return command.render(self.cockpit_context, self.logger)
        raise RuntimeError("operator cockpit provider is not configured")

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


def parse_matrix_command(body: str, *, command_prefix: str = "!parking") -> MatrixCommand:
    return _matrix_command_from_applied(_parse_applied_matrix_command(body, command_prefix=command_prefix))


def _parse_applied_matrix_command(body: str, *, command_prefix: str = "!parking") -> _AppliedMatrixCommand:
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
    spec = _command_spec_for(parts)
    if spec is not None:
        return spec.parse(parts, prefix)
    if parts[1] == "lab":
        raise MatrixCommandParseError("unknown lab command")
    raise MatrixCommandParseError("unknown command")


def _command_spec_for(parts: Sequence[str]) -> _CommandSpec | None:
    for spec in MATRIX_COMMAND_SPECS:
        for trigger in spec.triggers:
            if tuple(parts[1 : 1 + len(trigger)]) == trigger:
                return spec
    return None


def _usage(prefix: str, suffix: str) -> str:
    return f"usage: {prefix} {suffix}"


_EXACT_COCKPIT_RENDERERS: dict[str, _CockpitRenderer] = {
    "status": lambda context, logger: context.status_reply(logger=logger),
    "config": lambda context, logger: context.config_reply(logger=logger),
    "latest": lambda context, logger: context.latest_reply(logger=logger),
    "recent": lambda context, logger: context.recent_reply(logger=logger),
    "confidence": lambda context, logger: context.confidence_reply(logger=logger),
}


def _exact_cockpit_command(action: str) -> _CockpitReplyCommand:
    return _CockpitReplyCommand(action=action, render=_EXACT_COCKPIT_RENDERERS[action])


def _spot_cockpit_command(action: str, spot_id: str) -> _CockpitReplyCommand:
    return _CockpitReplyCommand(
        action=action,
        render=lambda context, logger: context.why_reply(spot_id, logger=logger),
        arguments={"spot_id": spot_id},
    )


def _analytics_command(window: str) -> _CockpitReplyCommand:
    return _CockpitReplyCommand(
        action="analytics",
        render=lambda context, logger: context.analytics_reply(window, logger=logger),
        arguments={"analytics_window": window},
    )


def _incident_review_command(*, incident_time: str, spot_id: str) -> _CockpitReplyCommand:
    return _CockpitReplyCommand(
        action="incident_review",
        render=lambda context, logger: context.incident_review_reply(
            spot_id=spot_id,
            incident_time=incident_time,
            logger=logger,
        ),
        arguments={"incident_time": incident_time, "spot_id": spot_id},
    )


def _lab_run_command(kind: str) -> _CockpitReplyCommand:
    return _CockpitReplyCommand(
        action="lab_run",
        render=lambda context, logger: context.lab_run_reply(kind, logger=logger),
        arguments={"lab_kind": kind},
    )


def _lab_status_command(job_id: str) -> _CockpitReplyCommand:
    return _CockpitReplyCommand(
        action="lab_status",
        render=lambda context, logger: context.lab_status_reply(job_id, logger=logger),
        arguments={"lab_job_id": job_id},
    )


def _exact_cockpit(action: str, usage_suffix: str) -> _CommandParser:
    def parse(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
        if len(parts) != 2:
            raise MatrixCommandParseError(_usage(prefix, usage_suffix))
        return _exact_cockpit_command(action)

    return parse


def _parse_spot_cockpit(action: str, usage_suffix: str) -> _CommandParser:
    def parse(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
        if len(parts) != 3:
            raise MatrixCommandParseError(_usage(prefix, usage_suffix))
        return _spot_cockpit_command(action, _validate_spot_id(parts[2]))

    return parse


def _parse_correction(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    usage = _usage(prefix, "correct <spot_id> <open|occupied>" if parts[1] == "correct" else "false-alert <spot_id> <open|occupied>")
    if len(parts) != 4:
        raise MatrixCommandParseError(usage)
    return _CorrectSpotStateCommand(spot_id=_validate_spot_id(parts[2]), actual_state=_validate_actual_state(parts[3]))


def _parse_learn_label(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    usage = _usage(prefix, "learn <spot_id> <open|occupied> at <time>" if parts[1] == "learn" else "missed-alert <spot_id> <open|occupied> at <time>")
    if len(parts) != 6 or parts[4] != "at":
        raise MatrixCommandParseError(usage)
    learn_time = redact_diagnostic_text(parts[5])[:80]
    if not learn_time:
        raise MatrixCommandParseError(usage)
    return _LearnLabelCommand(
        spot_id=_validate_spot_id(parts[2]),
        actual_state=_validate_actual_state(parts[3]),
        requested_time=learn_time,
    )


def _parse_analytics(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) == 2:
        return _analytics_command("7d")
    if len(parts) == 3 and parts[2] in {"today", "7d", "30d", "all"}:
        return _analytics_command(parts[2])
    raise MatrixCommandParseError(_usage(prefix, "analytics [today|7d|30d|all]"))


def _parse_incident_review(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) != 4:
        raise MatrixCommandParseError(_usage(prefix, "at <time> <spot_id>"))
    incident_time = redact_diagnostic_text(parts[2])[:80]
    if not incident_time:
        raise MatrixCommandParseError(_usage(prefix, "at <time> <spot_id>"))
    return _incident_review_command(incident_time=incident_time, spot_id=_validate_spot_id(parts[3]))


def _parse_lab_run(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) != 4:
        raise MatrixCommandParseError(_usage(prefix, "lab run <replay|tuning>"))
    return _lab_run_command(_validate_lab_kind(parts[3]))


def _parse_lab_status(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) == 3:
        return _lab_status_command("latest")
    if len(parts) == 4:
        return _lab_status_command(_validate_lab_job_id(parts[3]))
    raise MatrixCommandParseError(_usage(prefix, "lab status [job_id|latest]"))


def _parse_profile_rename(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) < 5:
        raise MatrixCommandParseError(_usage(prefix, "profile rename <profile_id> <label>"))
    return _RenameProfileCommand(
        profile_id=_validate_profile_id(parts[3], "profile_id"),
        label=_validate_label(" ".join(parts[4:])),
    )


def _parse_profile_merge(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) != 5:
        raise MatrixCommandParseError(_usage(prefix, "profile merge <source_profile_id> <target_profile_id>"))
    source = _validate_profile_id(parts[3], "source_profile_id")
    target = _validate_profile_id(parts[4], "target_profile_id")
    if source == target:
        raise MatrixCommandParseError("source and target profiles must differ")
    return _MergeProfilesCommand(source_profile_id=source, target_profile_id=target)


def _parse_profile_summary(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) != 4:
        raise MatrixCommandParseError(_usage(prefix, "profile summary <profile_id>"))
    return _ProfileSummaryCommand(profile_id=_validate_profile_id(parts[3], "profile_id"))


def _parse_wrong_match(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) != 3:
        raise MatrixCommandParseError(_usage(prefix, "wrong <spot_id|session_id>"))
    return _WrongMatchCommand(subject_id=_validate_subject_id(parts[2]))


def _parse_assign_owner(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) != 3:
        raise MatrixCommandParseError(_usage(prefix, "owner <spot_id>"))
    return _AssignOwnerCommand(spot_id=_validate_subject_id(parts[2]))


def _parse_who(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) != 2:
        raise MatrixCommandParseError(_usage(prefix, "who"))
    return _ActiveSpotAssignmentsCommand()


def _parse_help(parts: list[str], prefix: str) -> _AppliedMatrixCommand:
    if len(parts) != 2:
        raise MatrixCommandParseError(_usage(prefix, "help"))
    return _HelpCommand()


MATRIX_COMMAND_SPECS: tuple[_CommandSpec, ...] = (
    _CommandSpec((("help",),), "help — show this help text", _parse_help),
    _CommandSpec((("status",),), "status — show runtime health and spot status", _exact_cockpit("status", "status")),
    _CommandSpec((("config",),), "config — show safe monitor configuration", _exact_cockpit("config", "config")),
    _CommandSpec((("latest",),), "latest — show latest runtime summary and raw full-frame image evidence", _exact_cockpit("latest", "latest")),
    _CommandSpec((("why",),), "why <spot_id> — explain recent parking decisions for one spot from bounded local memory", _parse_spot_cockpit("why", "why <spot_id>")),
    _CommandSpec((("explain",),), "explain <spot_id> — alias for why with the same bounded local-memory explanation", _parse_spot_cockpit("explain", "explain <spot_id>")),
    _CommandSpec((("correct",),), "correct <spot_id> <open|occupied> — record the actual spot state for a wrong alert", _parse_correction),
    _CommandSpec((("false-alert",),), "false-alert <spot_id> <open|occupied> — explicit alias for correcting a false alert", _parse_correction),
    _CommandSpec((("learn",),), "learn <spot_id> <open|occupied> at <time> — record a retained-timeline calibration label for review", _parse_learn_label),
    _CommandSpec((("missed-alert",),), "missed-alert <spot_id> <open|occupied> at <time> — explicit alias for recording missed timeline evidence", _parse_learn_label),
    _CommandSpec((("recent",),), "recent — show recent decision, alert, suppression, command, and lab records from bounded local memory", _exact_cockpit("recent", "recent")),
    _CommandSpec((("confidence",),), "confidence — show artifact-derived spot stability, weak evidence, timeline health, and Matrix delivery status", _exact_cockpit("confidence", "confidence")),
    _CommandSpec((("analytics",),), "analytics [today|7d|30d|all] — show spot-level historical occupancy metrics from local vehicle-history sessions", _parse_analytics),
    _CommandSpec((("at",),), "at <time> <spot_id> — review the nearest retained timeline frame and local decision memory for an incident", _parse_incident_review),
    _CommandSpec((("lab", "run"),), "lab run replay — start a bounded local replay lab job using fixed inputs", _parse_lab_run),
    _CommandSpec((("lab", "run"),), "lab run tuning — start a bounded local tuning lab job using fixed inputs", _parse_lab_run),
    _CommandSpec((("lab", "status"),), "lab status [job_id|latest] — show the latest or selected redacted lab job status", _parse_lab_status),
    _CommandSpec((("who",),), "who — list active parking sessions by spot and attach a fresh current snapshot when configured", _parse_who),
    _CommandSpec((("owner",),), "owner <spot_id> — mark the active vehicle in a spot as the configured owner vehicle", _parse_assign_owner),
    _CommandSpec((("wrong",),), "wrong <spot_id|session_id> — mark a vehicle profile match as wrong", _parse_wrong_match),
    _CommandSpec((("profile", "summary"),), "profile summary <profile_id> — show a safe vehicle profile summary", _parse_profile_summary),
    _CommandSpec((("profile", "rename"),), "profile rename <profile_id> <label> — set a human label for a profile", _parse_profile_rename),
    _CommandSpec((("profile", "merge"),), "profile merge <source_profile_id> <target_profile_id> — merge one profile into another", _parse_profile_merge),
)


def _matrix_command_from_applied(command: _AppliedMatrixCommand) -> MatrixCommand:
    if isinstance(command, _CockpitReplyCommand):
        return MatrixCommand(
            action=command.action,
            spot_id=command.arguments.get("spot_id"),
            subject_id=command.arguments.get("incident_time") or command.arguments.get("analytics_window"),
            lab_kind=command.arguments.get("lab_kind"),
            lab_job_id=command.arguments.get("lab_job_id"),
        )
    if isinstance(command, _CorrectSpotStateCommand):
        return MatrixCommand(action=command.action, spot_id=command.spot_id, actual_state=command.actual_state)
    if isinstance(command, _LearnLabelCommand):
        return MatrixCommand(action=command.action, spot_id=command.spot_id, actual_state=command.actual_state, subject_id=command.requested_time)
    if isinstance(command, _RenameProfileCommand):
        return MatrixCommand(action=command.action, profile_id=command.profile_id, label=command.label)
    if isinstance(command, _MergeProfilesCommand):
        return MatrixCommand(action=command.action, source_profile_id=command.source_profile_id, target_profile_id=command.target_profile_id)
    if isinstance(command, _WrongMatchCommand):
        return MatrixCommand(action=command.action, subject_id=command.subject_id)
    if isinstance(command, _AssignOwnerCommand):
        return MatrixCommand(action=command.action, subject_id=command.spot_id)
    if isinstance(command, _ProfileSummaryCommand):
        return MatrixCommand(action=command.action, profile_id=command.profile_id)
    return MatrixCommand(action=command.action)


def _validate_profile_id(value: str, name: str) -> str:
    if not re.fullmatch(r"prof_[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", value):
        raise MatrixCommandParseError(f"invalid {name}")
    return value

def _validate_subject_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,219}", value):
        raise MatrixCommandParseError("invalid subject id")
    return value

def _validate_spot_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", value):
        raise MatrixCommandParseError("invalid spot id")
    return value

def _validate_actual_state(value: str) -> str:
    if value not in {"open", "occupied"}:
        raise MatrixCommandParseError("invalid actual state")
    return value

def _validate_lab_kind(value: str) -> str:
    if value not in {"replay", "tuning"}:
        raise MatrixCommandParseError("invalid lab job kind")
    return value

def _validate_lab_job_id(value: str) -> str:
    if value == "latest":
        return value
    if not re.fullmatch(r"lab-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", value):
        raise MatrixCommandParseError("invalid lab job id")
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

def _format_command_help_reply(command_prefix: str) -> str:
    lines = ["Parking monitor commands:"]
    lines.extend(f"{command_prefix} {spec.help_text}" for spec in MATRIX_COMMAND_SPECS)
    return "\n".join(lines)

def _confidence_text(value: object) -> str:
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return f"{float(value):.2f}"
    return "unknown"

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
