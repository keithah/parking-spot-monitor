from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
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
from parking_spot_monitor.matrix_snapshots import JPEG_MIMETYPE
from parking_spot_monitor.matrix_support import _require_non_empty, _sanitize_diagnostics


@dataclass(frozen=True)
class _CockpitCommand:
    action: str
    spot_id: str | None = None
    lab_kind: str | None = None
    lab_job_id: str | None = None
    incident_time: str | None = None
    analytics_window: str | None = None


@dataclass(frozen=True)
class _CorrectSpotStateCommand:
    spot_id: str
    actual_state: str


@dataclass(frozen=True)
class _LearnLabelCommand:
    spot_id: str
    actual_state: str
    requested_time: str


@dataclass(frozen=True)
class _RenameProfileCommand:
    profile_id: str
    label: str


@dataclass(frozen=True)
class _MergeProfilesCommand:
    source_profile_id: str
    target_profile_id: str


@dataclass(frozen=True)
class _WrongMatchCommand:
    subject_id: str


@dataclass(frozen=True)
class _AssignOwnerCommand:
    spot_id: str


@dataclass(frozen=True)
class _ActiveSpotAssignmentsCommand:
    pass


@dataclass(frozen=True)
class _HelpCommand:
    pass


@dataclass(frozen=True)
class _ProfileSummaryCommand:
    profile_id: str


_AppliedMatrixCommand: TypeAlias = (
    _CockpitCommand
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
        cockpit_provider: Callable[[str], str | MatrixCommandResponse] | None = None,
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

    def _apply_command(self, command: MatrixCommand, *, event: MatrixTextEvent) -> str | MatrixCommandResponse:
        typed_command = _typed_command(command)
        metadata = {"matrix_event_id": event.event_id, "matrix_sender": event.sender, "matrix_room_id": event.room_id}
        if isinstance(typed_command, _CockpitCommand):
            return self._format_cockpit_reply(
                typed_command.action,
                spot_id=typed_command.spot_id,
                lab_kind=typed_command.lab_kind,
                lab_job_id=typed_command.lab_job_id,
                incident_time=typed_command.incident_time,
                analytics_window=typed_command.analytics_window,
            )
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
        action: str,
        *,
        spot_id: str | None = None,
        lab_kind: str | None = None,
        lab_job_id: str | None = None,
        incident_time: str | None = None,
        analytics_window: str | None = None,
    ) -> str | MatrixCommandResponse:
        if self.cockpit_provider is not None:
            kwargs: dict[str, str] = {}
            if spot_id is not None:
                kwargs["spot_id"] = spot_id
            if lab_kind is not None:
                kwargs["lab_kind"] = lab_kind
            if lab_job_id is not None:
                kwargs["lab_job_id"] = lab_job_id
            if incident_time is not None:
                kwargs["incident_time"] = incident_time
            if analytics_window is not None:
                kwargs["analytics_window"] = analytics_window
            return self.cockpit_provider(action, **kwargs)  # type: ignore[call-arg]
        if self.cockpit_context is not None:
            return self.cockpit_context.format_reply(
                action,
                spot_id=spot_id,
                lab_kind=lab_kind,
                lab_job_id=lab_job_id,
                incident_time=incident_time,
                analytics_window=analytics_window,
                logger=self.logger,
            )
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
        content_uri = self.client.upload_image(
            filename=image_path.name,
            data=image_path.read_bytes(),
            content_type=JPEG_MIMETYPE,
        )
        self.client.send_image(
            room_id=self.room_id,
            txn_id=f"command:{event.event_id}:image",
            body=f"Raw full-frame {image_path.name} evidence",
            content_uri=content_uri,
            info=image_info,
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


def _typed_command(command: MatrixCommand) -> _AppliedMatrixCommand:
    if command.action in {"status", "config", "latest", "recent", "confidence"}:
        return _CockpitCommand(command.action)
    if command.action == "analytics":
        return _CockpitCommand(command.action, analytics_window=command.subject_id or "7d")
    if command.action == "lab_run":
        return _CockpitCommand(command.action, lab_kind=_require_command_field(command.lab_kind, "lab_kind"))
    if command.action == "lab_status":
        return _CockpitCommand(command.action, lab_job_id=command.lab_job_id or "latest")
    if command.action in {"why", "explain"}:
        return _CockpitCommand(command.action, spot_id=_require_command_field(command.spot_id, "spot_id"))
    if command.action == "incident_review":
        return _CockpitCommand(
            command.action,
            spot_id=_require_command_field(command.spot_id, "spot_id"),
            incident_time=_require_command_field(command.subject_id, "incident_time"),
        )
    if command.action == "correct_spot_state":
        return _CorrectSpotStateCommand(
            spot_id=_require_command_field(command.spot_id, "spot_id"),
            actual_state=_require_command_field(command.actual_state, "actual_state"),
        )
    if command.action == "learn_label":
        return _LearnLabelCommand(
            spot_id=_require_command_field(command.spot_id, "spot_id"),
            actual_state=_require_command_field(command.actual_state, "actual_state"),
            requested_time=_require_command_field(command.subject_id, "requested_time"),
        )
    if command.action == "rename_profile":
        return _RenameProfileCommand(
            profile_id=_require_command_field(command.profile_id, "profile_id"),
            label=_require_command_field(command.label, "label"),
        )
    if command.action == "merge_profiles":
        return _MergeProfilesCommand(
            source_profile_id=_require_command_field(command.source_profile_id, "source_profile_id"),
            target_profile_id=_require_command_field(command.target_profile_id, "target_profile_id"),
        )
    if command.action == "wrong_match":
        return _WrongMatchCommand(subject_id=_require_command_field(command.subject_id, "subject_id"))
    if command.action == "assign_owner":
        return _AssignOwnerCommand(spot_id=_require_command_field(command.subject_id, "spot_id"))
    if command.action == "active_spot_assignments":
        return _ActiveSpotAssignmentsCommand()
    if command.action == "help":
        return _HelpCommand()
    if command.action == "profile_summary":
        return _ProfileSummaryCommand(profile_id=_require_command_field(command.profile_id, "profile_id"))
    raise MatrixCommandParseError("unknown command")


def _require_command_field(value: str | None, name: str) -> str:
    if value is None:
        raise MatrixCommandParseError(f"missing {name}")
    return value


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
    if parts[1] == "status":
        if len(parts) != 2:
            raise MatrixCommandParseError("usage: !parking status")
        return MatrixCommand(action="status")
    if parts[1] == "config":
        if len(parts) != 2:
            raise MatrixCommandParseError("usage: !parking config")
        return MatrixCommand(action="config")
    if parts[1] == "latest":
        if len(parts) != 2:
            raise MatrixCommandParseError("usage: !parking latest")
        return MatrixCommand(action="latest")
    if parts[1] == "why":
        if len(parts) != 3:
            raise MatrixCommandParseError("usage: !parking why <spot_id>")
        return MatrixCommand(action="why", spot_id=_validate_spot_id(parts[2]))
    if parts[1] == "explain":
        if len(parts) != 3:
            raise MatrixCommandParseError("usage: !parking explain <spot_id>")
        return MatrixCommand(action="explain", spot_id=_validate_spot_id(parts[2]))
    if parts[1] in {"correct", "false-alert"}:
        usage = "usage: !parking correct <spot_id> <open|occupied>" if parts[1] == "correct" else f"usage: {prefix} false-alert <spot_id> <open|occupied>"
        if len(parts) != 4:
            raise MatrixCommandParseError(usage)
        return MatrixCommand(
            action="correct_spot_state",
            spot_id=_validate_spot_id(parts[2]),
            actual_state=_validate_actual_state(parts[3]),
        )
    if parts[1] in {"learn", "missed-alert"}:
        usage = "usage: !parking learn <spot_id> <open|occupied> at <time>" if parts[1] == "learn" else f"usage: {prefix} missed-alert <spot_id> <open|occupied> at <time>"
        if len(parts) != 6 or parts[4] != "at":
            raise MatrixCommandParseError(usage)
        learn_time = redact_diagnostic_text(parts[5])[:80]
        if not learn_time:
            raise MatrixCommandParseError(usage)
        return MatrixCommand(
            action="learn_label",
            spot_id=_validate_spot_id(parts[2]),
            actual_state=_validate_actual_state(parts[3]),
            subject_id=learn_time,
        )
    if parts[1] == "recent":
        if len(parts) != 2:
            raise MatrixCommandParseError("usage: !parking recent")
        return MatrixCommand(action="recent")
    if parts[1] == "confidence":
        if len(parts) != 2:
            raise MatrixCommandParseError("usage: !parking confidence")
        return MatrixCommand(action="confidence")
    if parts[1] == "analytics":
        if len(parts) == 2:
            return MatrixCommand(action="analytics", subject_id="7d")
        if len(parts) == 3 and parts[2] in {"today", "7d", "30d", "all"}:
            return MatrixCommand(action="analytics", subject_id=parts[2])
        raise MatrixCommandParseError("usage: !parking analytics [today|7d|30d|all]")
    if parts[1] == "at":
        if len(parts) != 4:
            raise MatrixCommandParseError("usage: !parking at <time> <spot_id>")
        incident_time = redact_diagnostic_text(parts[2])[:80]
        if not incident_time:
            raise MatrixCommandParseError("usage: !parking at <time> <spot_id>")
        return MatrixCommand(action="incident_review", subject_id=incident_time, spot_id=_validate_spot_id(parts[3]))
    if parts[1] == "lab":
        if parts[1:3] == ["lab", "run"]:
            if len(parts) != 4:
                raise MatrixCommandParseError("usage: !parking lab run <replay|tuning>")
            return MatrixCommand(action="lab_run", lab_kind=_validate_lab_kind(parts[3]))
        if parts[1:3] == ["lab", "status"]:
            if len(parts) == 3:
                return MatrixCommand(action="lab_status", lab_job_id="latest")
            if len(parts) == 4:
                return MatrixCommand(action="lab_status", lab_job_id=_validate_lab_job_id(parts[3]))
            raise MatrixCommandParseError("usage: !parking lab status [job_id|latest]")
        raise MatrixCommandParseError("unknown lab command")
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
    if parts[1] == "owner":
        if len(parts) != 3:
            raise MatrixCommandParseError("usage: !parking owner <spot_id>")
        return MatrixCommand(action="assign_owner", subject_id=_validate_subject_id(parts[2]))
    if parts[1] == "who":
        if len(parts) != 2:
            raise MatrixCommandParseError("usage: !parking who")
        return MatrixCommand(action="active_spot_assignments")
    if parts[1] == "help":
        if len(parts) != 2:
            raise MatrixCommandParseError("usage: !parking help")
        return MatrixCommand(action="help")
    raise MatrixCommandParseError("unknown command")

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
    return (
        "Parking monitor commands:\n"
        f"{command_prefix} help — show this help text\n"
        f"{command_prefix} status — show runtime health and spot status\n"
        f"{command_prefix} config — show safe monitor configuration\n"
        f"{command_prefix} latest — show latest runtime summary and raw full-frame image evidence\n"
        f"{command_prefix} why <spot_id> — explain recent parking decisions for one spot from bounded local memory\n"
        f"{command_prefix} explain <spot_id> — alias for why with the same bounded local-memory explanation\n"
        f"{command_prefix} correct <spot_id> <open|occupied> — record the actual spot state for a wrong alert\n"
        f"{command_prefix} false-alert <spot_id> <open|occupied> — explicit alias for correcting a false alert\n"
        f"{command_prefix} learn <spot_id> <open|occupied> at <time> — record a retained-timeline calibration label for review\n"
        f"{command_prefix} missed-alert <spot_id> <open|occupied> at <time> — explicit alias for recording missed timeline evidence\n"
        f"{command_prefix} recent — show recent decision, alert, suppression, command, and lab records from bounded local memory\n"
        f"{command_prefix} confidence — show artifact-derived spot stability, weak evidence, timeline health, and Matrix delivery status\n"
        f"{command_prefix} analytics [today|7d|30d|all] — show spot-level historical occupancy metrics from local vehicle-history sessions\n"
        f"{command_prefix} at <time> <spot_id> — review the nearest retained timeline frame and local decision memory for an incident\n"
        f"{command_prefix} lab run replay — start a bounded local replay lab job using fixed inputs\n"
        f"{command_prefix} lab run tuning — start a bounded local tuning lab job using fixed inputs\n"
        f"{command_prefix} lab status [job_id|latest] — show the latest or selected redacted lab job status\n"
        f"{command_prefix} who — list active parking sessions by spot and attach a fresh current snapshot when configured\n"
        f"{command_prefix} owner <spot_id> — mark the active vehicle in a spot as the configured owner vehicle\n"
        f"{command_prefix} wrong <spot_id|session_id> — mark a vehicle profile match as wrong\n"
        f"{command_prefix} profile summary <profile_id> — show a safe vehicle profile summary\n"
        f"{command_prefix} profile rename <profile_id> <label> — set a human label for a profile\n"
        f"{command_prefix} profile merge <source_profile_id> <target_profile_id> — merge one profile into another"
    )

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
