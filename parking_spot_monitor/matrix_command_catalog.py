from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from parking_spot_monitor.logging import redact_diagnostic_text
from parking_spot_monitor.matrix_alerts import _int_field, _safe_text
from parking_spot_monitor.matrix_cockpit import (
    MatrixOperatorCockpitContext,
    _active_spot_assignments_with_runtime_status,
    _format_active_spot_assignments_reply,
)
from parking_spot_monitor.matrix_command_runtime import MatrixCommandRuntime
from parking_spot_monitor.matrix_models import (
    MatrixCommand,
    MatrixCommandParseError,
    MatrixCommandResponse,
    MatrixTextEvent,
)
from parking_spot_monitor.matrix_support import _require_non_empty


MatrixCommandApplyResult: TypeAlias = str | MatrixCommandResponse


@dataclass(frozen=True)
class StatusCommand:
    action: str = field(default="status", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(runtime, self.action, {}, lambda context: context.status_reply(logger=runtime.logger))

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action)


@dataclass(frozen=True)
class ConfigCommand:
    action: str = field(default="config", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(runtime, self.action, {}, lambda context: context.config_reply(logger=runtime.logger))

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action)


@dataclass(frozen=True)
class LatestCommand:
    action: str = field(default="latest", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(runtime, self.action, {}, lambda context: context.latest_reply(logger=runtime.logger))

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action)


@dataclass(frozen=True)
class RecentCommand:
    action: str = field(default="recent", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(runtime, self.action, {}, lambda context: context.recent_reply(logger=runtime.logger))

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action)


@dataclass(frozen=True)
class ConfidenceCommand:
    action: str = field(default="confidence", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(runtime, self.action, {}, lambda context: context.confidence_reply(logger=runtime.logger))

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action)


@dataclass(frozen=True)
class SpotCockpitCommand:
    action: str
    spot_id: str

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(
            runtime,
            self.action,
            {"spot_id": self.spot_id},
            lambda context: context.why_reply(self.spot_id, logger=runtime.logger),
        )

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, spot_id=self.spot_id)


@dataclass(frozen=True)
class AnalyticsCommand:
    window: str
    action: str = field(default="analytics", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(
            runtime,
            self.action,
            {"analytics_window": self.window},
            lambda context: context.analytics_reply(self.window, logger=runtime.logger),
        )

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, subject_id=self.window)


@dataclass(frozen=True)
class IncidentReviewCommand:
    incident_time: str
    spot_id: str
    action: str = field(default="incident_review", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(
            runtime,
            self.action,
            {"incident_time": self.incident_time, "spot_id": self.spot_id},
            lambda context: context.incident_review_reply(
                spot_id=self.spot_id,
                incident_time=self.incident_time,
                logger=runtime.logger,
            ),
        )

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, spot_id=self.spot_id, subject_id=self.incident_time)


@dataclass(frozen=True)
class LabRunCommand:
    kind: str
    action: str = field(default="lab_run", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(
            runtime,
            self.action,
            {"lab_kind": self.kind},
            lambda context: context.lab_run_reply(self.kind, logger=runtime.logger),
        )

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, lab_kind=self.kind)


@dataclass(frozen=True)
class LabStatusCommand:
    job_id: str
    action: str = field(default="lab_status", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return _cockpit_reply(
            runtime,
            self.action,
            {"lab_job_id": self.job_id},
            lambda context: context.lab_status_reply(self.job_id, logger=runtime.logger),
        )

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, lab_job_id=self.job_id)


@dataclass(frozen=True)
class CorrectSpotStateCommand:
    spot_id: str
    actual_state: str
    action: str = field(default="correct_spot_state", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        if runtime.feedback_labeler is None:
            raise RuntimeError("operator feedback labeler is not configured")
        result = runtime.feedback_labeler.record_correction(
            spot_id=self.spot_id,
            actual_state=self.actual_state,
            **runtime.event_metadata(event),
        )
        return result.reply_text

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, spot_id=self.spot_id, actual_state=self.actual_state)


@dataclass(frozen=True)
class LearnLabelCommand:
    spot_id: str
    actual_state: str
    requested_time: str
    action: str = field(default="learn_label", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        if runtime.feedback_labeler is None:
            raise RuntimeError("operator feedback labeler is not configured")
        result = runtime.feedback_labeler.record_learn_label(
            spot_id=self.spot_id,
            target_state=self.actual_state,
            requested_time=self.requested_time,
            settings=None if runtime.cockpit_context is None else runtime.cockpit_context.settings,
            state_path=None if runtime.cockpit_context is None else runtime.cockpit_context.state_path,
            detector=None if runtime.cockpit_context is None else runtime.cockpit_context.incident_detector,
            **runtime.event_metadata(event),
        )
        return result.reply_text

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, spot_id=self.spot_id, actual_state=self.actual_state, subject_id=self.requested_time)


@dataclass(frozen=True)
class RenameProfileCommand:
    profile_id: str
    label: str
    action: str = field(default="rename_profile", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        if runtime.correction_already_seen(event.event_id):
            return "Command already applied; acknowledgement repeated."
        applied = runtime.archive.rename_profile(self.profile_id, self.label, **runtime.event_metadata(event))
        return f"Profile {self.profile_id} renamed to {self.label}. Correction {applied.correction_id} recorded."

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, profile_id=self.profile_id, label=self.label)


@dataclass(frozen=True)
class MergeProfilesCommand:
    source_profile_id: str
    target_profile_id: str
    action: str = field(default="merge_profiles", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        if runtime.correction_already_seen(event.event_id):
            return "Command already applied; acknowledgement repeated."
        applied = runtime.archive.merge_profiles(self.source_profile_id, self.target_profile_id, **runtime.event_metadata(event))
        return f"Profile {self.source_profile_id} merged into {self.target_profile_id}. Correction {applied.correction_id} recorded."

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, source_profile_id=self.source_profile_id, target_profile_id=self.target_profile_id)


@dataclass(frozen=True)
class WrongMatchCommand:
    subject_id: str
    action: str = field(default="wrong_match", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        if runtime.correction_already_seen(event.event_id):
            return "Command already applied; acknowledgement repeated."
        session_id = runtime.archive.resolve_wrong_match_subject(self.subject_id)
        applied = runtime.archive.mark_wrong_match(session_id, **runtime.event_metadata(event))
        return f"Wrong match recorded for session {session_id}. Correction {applied.correction_id} recorded."

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, subject_id=self.subject_id)


@dataclass(frozen=True)
class AssignOwnerCommand:
    spot_id: str
    action: str = field(default="assign_owner", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        assignment = runtime.archive.assign_owner_profile_to_active_spot(self.spot_id)
        profile_id = _safe_text(assignment.profile_id, default="unknown")
        session_id = _safe_text(assignment.session_id, default="unknown")
        confidence_text = _confidence_text(assignment.profile_confidence)
        return f"Owner vehicle assigned to {self.spot_id}: session {session_id}, profile {profile_id}, confidence {confidence_text}."

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, subject_id=self.spot_id)


@dataclass(frozen=True)
class ActiveSpotAssignmentsCommand:
    action: str = field(default="active_spot_assignments", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        assignments = _active_spot_assignments_with_runtime_status(
            runtime.archive.active_spot_assignments(),
            cockpit_context=runtime.cockpit_context,
            logger=runtime.logger,
        )
        base_reply = _format_active_spot_assignments_reply(assignments)
        if runtime.who_snapshot_provider is not None:
            return runtime.who_snapshot_provider(base_reply)
        return base_reply

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action)


@dataclass(frozen=True)
class HelpCommand:
    action: str = field(default="help", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        del event
        return runtime.help_formatter(runtime.command_prefix)

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action)


@dataclass(frozen=True)
class ProfileSummaryCommand:
    profile_id: str
    action: str = field(default="profile_summary", init=False)

    def apply(self, runtime: MatrixCommandRuntime, event: MatrixTextEvent) -> MatrixCommandApplyResult:
        return _format_profile_summary_reply(runtime.profile_summary(self.profile_id, event=event))

    def to_matrix_command(self) -> MatrixCommand:
        return MatrixCommand(action=self.action, profile_id=self.profile_id)


AppliedMatrixCommand: TypeAlias = (
    StatusCommand
    | ConfigCommand
    | LatestCommand
    | RecentCommand
    | ConfidenceCommand
    | SpotCockpitCommand
    | AnalyticsCommand
    | IncidentReviewCommand
    | LabRunCommand
    | LabStatusCommand
    | CorrectSpotStateCommand
    | LearnLabelCommand
    | RenameProfileCommand
    | MergeProfilesCommand
    | WrongMatchCommand
    | AssignOwnerCommand
    | ActiveSpotAssignmentsCommand
    | HelpCommand
    | ProfileSummaryCommand
)


_CommandParser: TypeAlias = Callable[[list[str], str], AppliedMatrixCommand]


@dataclass(frozen=True)
class MatrixCommandSpec:
    triggers: tuple[tuple[str, ...], ...]
    help_text: str
    parse: _CommandParser


def parse_matrix_command(body: str, *, command_prefix: str = "!parking") -> MatrixCommand:
    return parse_applied_matrix_command(body, command_prefix=command_prefix).to_matrix_command()


def parse_applied_matrix_command(body: str, *, command_prefix: str = "!parking") -> AppliedMatrixCommand:
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


def format_command_help_reply(command_prefix: str) -> str:
    lines = ["Parking monitor commands:"]
    lines.extend(f"{command_prefix} {spec.help_text}" for spec in MATRIX_COMMAND_SPECS)
    return "\n".join(lines)


def _command_spec_for(parts: list[str]) -> MatrixCommandSpec | None:
    for spec in MATRIX_COMMAND_SPECS:
        for trigger in spec.triggers:
            if tuple(parts[1 : 1 + len(trigger)]) == trigger:
                return spec
    return None


def _usage(prefix: str, suffix: str) -> str:
    return f"usage: {prefix} {suffix}"


def _exact_cockpit_command(action: str) -> AppliedMatrixCommand:
    commands: dict[str, AppliedMatrixCommand] = {
        "status": StatusCommand(),
        "config": ConfigCommand(),
        "latest": LatestCommand(),
        "recent": RecentCommand(),
        "confidence": ConfidenceCommand(),
    }
    try:
        return commands[action]
    except KeyError as exc:
        raise MatrixCommandParseError("unknown command") from exc


def _exact_cockpit(action: str, usage_suffix: str) -> _CommandParser:
    def parse(parts: list[str], prefix: str) -> AppliedMatrixCommand:
        if len(parts) != 2:
            raise MatrixCommandParseError(_usage(prefix, usage_suffix))
        return _exact_cockpit_command(action)

    return parse


def _parse_spot_cockpit(action: str, usage_suffix: str) -> _CommandParser:
    def parse(parts: list[str], prefix: str) -> AppliedMatrixCommand:
        if len(parts) != 3:
            raise MatrixCommandParseError(_usage(prefix, usage_suffix))
        return SpotCockpitCommand(action=action, spot_id=_validate_spot_id(parts[2]))

    return parse


def _parse_correction(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    usage = _usage(prefix, "correct <spot_id> <open|occupied>" if parts[1] == "correct" else "false-alert <spot_id> <open|occupied>")
    if len(parts) != 4:
        raise MatrixCommandParseError(usage)
    return CorrectSpotStateCommand(spot_id=_validate_spot_id(parts[2]), actual_state=_validate_actual_state(parts[3]))


def _parse_learn_label(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    usage = _usage(prefix, "learn <spot_id> <open|occupied> at <time>" if parts[1] == "learn" else "missed-alert <spot_id> <open|occupied> at <time>")
    if len(parts) != 6 or parts[4] != "at":
        raise MatrixCommandParseError(usage)
    learn_time = redact_diagnostic_text(parts[5])[:80]
    if not learn_time:
        raise MatrixCommandParseError(usage)
    return LearnLabelCommand(
        spot_id=_validate_spot_id(parts[2]),
        actual_state=_validate_actual_state(parts[3]),
        requested_time=learn_time,
    )


def _parse_analytics(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) == 2:
        return AnalyticsCommand(window="7d")
    if len(parts) == 3 and parts[2] in {"today", "7d", "30d", "all"}:
        return AnalyticsCommand(window=parts[2])
    raise MatrixCommandParseError(_usage(prefix, "analytics [today|7d|30d|all]"))


def _parse_incident_review(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) != 4:
        raise MatrixCommandParseError(_usage(prefix, "at <time> <spot_id>"))
    incident_time = redact_diagnostic_text(parts[2])[:80]
    if not incident_time:
        raise MatrixCommandParseError(_usage(prefix, "at <time> <spot_id>"))
    return IncidentReviewCommand(incident_time=incident_time, spot_id=_validate_spot_id(parts[3]))


def _parse_lab_run(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) != 4:
        raise MatrixCommandParseError(_usage(prefix, "lab run <replay|tuning>"))
    return LabRunCommand(kind=_validate_lab_kind(parts[3]))


def _parse_lab_status(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) == 3:
        return LabStatusCommand(job_id="latest")
    if len(parts) == 4:
        return LabStatusCommand(job_id=_validate_lab_job_id(parts[3]))
    raise MatrixCommandParseError(_usage(prefix, "lab status [job_id|latest]"))


def _parse_profile_rename(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) < 5:
        raise MatrixCommandParseError(_usage(prefix, "profile rename <profile_id> <label>"))
    return RenameProfileCommand(
        profile_id=_validate_profile_id(parts[3], "profile_id"),
        label=_validate_label(" ".join(parts[4:])),
    )


def _parse_profile_merge(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) != 5:
        raise MatrixCommandParseError(_usage(prefix, "profile merge <source_profile_id> <target_profile_id>"))
    source = _validate_profile_id(parts[3], "source_profile_id")
    target = _validate_profile_id(parts[4], "target_profile_id")
    if source == target:
        raise MatrixCommandParseError("source and target profiles must differ")
    return MergeProfilesCommand(source_profile_id=source, target_profile_id=target)


def _parse_profile_summary(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) != 4:
        raise MatrixCommandParseError(_usage(prefix, "profile summary <profile_id>"))
    return ProfileSummaryCommand(profile_id=_validate_profile_id(parts[3], "profile_id"))


def _parse_wrong_match(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) != 3:
        raise MatrixCommandParseError(_usage(prefix, "wrong <spot_id|session_id>"))
    return WrongMatchCommand(subject_id=_validate_subject_id(parts[2]))


def _parse_assign_owner(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) != 3:
        raise MatrixCommandParseError(_usage(prefix, "owner <spot_id>"))
    return AssignOwnerCommand(spot_id=_validate_subject_id(parts[2]))


def _parse_who(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) != 2:
        raise MatrixCommandParseError(_usage(prefix, "who"))
    return ActiveSpotAssignmentsCommand()


def _parse_help(parts: list[str], prefix: str) -> AppliedMatrixCommand:
    if len(parts) != 2:
        raise MatrixCommandParseError(_usage(prefix, "help"))
    return HelpCommand()


MATRIX_COMMAND_SPECS: tuple[MatrixCommandSpec, ...] = (
    MatrixCommandSpec((("help",),), "help — show this help text", _parse_help),
    MatrixCommandSpec((("status",),), "status — show runtime health and spot status", _exact_cockpit("status", "status")),
    MatrixCommandSpec((("config",),), "config — show safe monitor configuration", _exact_cockpit("config", "config")),
    MatrixCommandSpec((("latest",),), "latest — show latest runtime summary and raw full-frame image evidence", _exact_cockpit("latest", "latest")),
    MatrixCommandSpec((("why",),), "why <spot_id> — explain recent parking decisions for one spot from bounded local memory", _parse_spot_cockpit("why", "why <spot_id>")),
    MatrixCommandSpec((("explain",),), "explain <spot_id> — alias for why with the same bounded local-memory explanation", _parse_spot_cockpit("explain", "explain <spot_id>")),
    MatrixCommandSpec((("correct",),), "correct <spot_id> <open|occupied> — record the actual spot state for a wrong alert", _parse_correction),
    MatrixCommandSpec((("false-alert",),), "false-alert <spot_id> <open|occupied> — explicit alias for correcting a false alert", _parse_correction),
    MatrixCommandSpec((("learn",),), "learn <spot_id> <open|occupied> at <time> — record a retained-timeline calibration label for review", _parse_learn_label),
    MatrixCommandSpec((("missed-alert",),), "missed-alert <spot_id> <open|occupied> at <time> — explicit alias for recording missed timeline evidence", _parse_learn_label),
    MatrixCommandSpec((("recent",),), "recent — show recent decision, alert, suppression, command, and lab records from bounded local memory", _exact_cockpit("recent", "recent")),
    MatrixCommandSpec((("confidence",),), "confidence — show artifact-derived spot stability, weak evidence, timeline health, and Matrix delivery status", _exact_cockpit("confidence", "confidence")),
    MatrixCommandSpec((("analytics",),), "analytics [today|7d|30d|all] — show spot-level historical occupancy metrics from local vehicle-history sessions", _parse_analytics),
    MatrixCommandSpec((("at",),), "at <time> <spot_id> — review the nearest retained timeline frame and local decision memory for an incident", _parse_incident_review),
    MatrixCommandSpec((("lab", "run"),), "lab run replay — start a bounded local replay lab job using fixed inputs", _parse_lab_run),
    MatrixCommandSpec((("lab", "run"),), "lab run tuning — start a bounded local tuning lab job using fixed inputs", _parse_lab_run),
    MatrixCommandSpec((("lab", "status"),), "lab status [job_id|latest] — show the latest or selected redacted lab job status", _parse_lab_status),
    MatrixCommandSpec((("who",),), "who — list active parking sessions by spot and attach a fresh current snapshot when configured", _parse_who),
    MatrixCommandSpec((("owner",),), "owner <spot_id> — mark the active vehicle in a spot as the configured owner vehicle", _parse_assign_owner),
    MatrixCommandSpec((("wrong",),), "wrong <spot_id|session_id> — mark a vehicle profile match as wrong", _parse_wrong_match),
    MatrixCommandSpec((("profile", "summary"),), "profile summary <profile_id> — show a safe vehicle profile summary", _parse_profile_summary),
    MatrixCommandSpec((("profile", "rename"),), "profile rename <profile_id> <label> — set a human label for a profile", _parse_profile_rename),
    MatrixCommandSpec((("profile", "merge"),), "profile merge <source_profile_id> <target_profile_id> — merge one profile into another", _parse_profile_merge),
)


def _cockpit_reply(
    runtime: MatrixCommandRuntime,
    action: str,
    arguments: Mapping[str, str],
    context_reply: Callable[[MatrixOperatorCockpitContext], MatrixCommandResponse],
) -> str | MatrixCommandResponse:
    if runtime.cockpit_provider is not None:
        return runtime.cockpit_provider(action, **dict(arguments))
    if runtime.cockpit_context is not None:
        return context_reply(runtime.cockpit_context)
    raise RuntimeError("operator cockpit provider is not configured")

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
