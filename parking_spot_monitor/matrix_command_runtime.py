from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_cockpit import MatrixOperatorCockpitContext
from parking_spot_monitor.matrix_models import MatrixCommandResponse, MatrixTextEvent
from parking_spot_monitor.vehicle_history_models import ProfileAssignment, ProfileCorrectionEvent, SessionRecord


class MatrixCommandArchive(Protocol):
    def load_corrections(self) -> Sequence[ProfileCorrectionEvent]: ...
    def load_active_sessions(self) -> Sequence[SessionRecord]: ...
    def list_closed_sessions(self) -> Sequence[SessionRecord]: ...
    def resolve_wrong_match_subject(self, subject_id: str) -> str: ...
    def rename_profile(self, profile_id: str, label: str, **metadata: str) -> ProfileCorrectionEvent: ...
    def merge_profiles(self, source_profile_id: str, target_profile_id: str, **metadata: str) -> ProfileCorrectionEvent: ...
    def mark_wrong_match(self, session_id: str, **metadata: str) -> ProfileCorrectionEvent: ...
    def assign_owner_profile_to_active_spot(self, spot_id: str) -> ProfileAssignment: ...
    def active_spot_assignments(self) -> Sequence[Mapping[str, Any]]: ...
    def profile_summary(self, profile_id: str, **metadata: str) -> Mapping[str, Any]: ...


class MatrixFeedbackResult(Protocol):
    reply_text: str


class MatrixFeedbackLabeler(Protocol):
    def record_correction(self, *, spot_id: str, actual_state: str, matrix_event_id: str, matrix_sender: str, matrix_room_id: str) -> MatrixFeedbackResult: ...

    def record_learn_label(
        self,
        *,
        spot_id: str,
        target_state: str,
        requested_time: str,
        settings: object | None,
        state_path: object | None,
        detector: object | None,
        matrix_event_id: str,
        matrix_sender: str,
        matrix_room_id: str,
    ) -> MatrixFeedbackResult: ...


@dataclass(frozen=True)
class MatrixCommandRuntime:
    archive: MatrixCommandArchive
    command_prefix: str
    help_formatter: Callable[[str], str]
    logger: StructuredLogger | None = None
    cockpit_provider: Callable[..., str | MatrixCommandResponse] | None = None
    who_snapshot_provider: Callable[[str], str | MatrixCommandResponse] | None = None
    cockpit_context: MatrixOperatorCockpitContext | None = None
    feedback_labeler: MatrixFeedbackLabeler | None = None

    def event_metadata(self, event: MatrixTextEvent) -> dict[str, str]:
        return {"matrix_event_id": event.event_id, "matrix_sender": event.sender, "matrix_room_id": event.room_id}

    def correction_already_seen(self, event_id: str) -> bool:
        return any(correction.matrix_event_id == event_id for correction in self.archive.load_corrections())

    def resolve_wrong_match_subject(self, subject_id: str) -> str:
        return self.archive.resolve_wrong_match_subject(subject_id)

    def profile_summary(self, profile_id: str, *, event: MatrixTextEvent) -> Mapping[str, Any]:
        return self.archive.profile_summary(profile_id, **self.event_metadata(event))
