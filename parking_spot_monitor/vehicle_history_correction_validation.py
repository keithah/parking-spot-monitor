"""Archive-bound validation for operator correction events."""

from __future__ import annotations

from itertools import chain
from typing import Any, Sequence

from parking_spot_monitor.vehicle_history_models import (
    CORRECTION_ACTION_MERGE_PROFILES,
    CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED,
    CORRECTION_ACTION_RENAME_PROFILE,
    CORRECTION_ACTION_WRONG_MATCH,
    ArchiveSchemaError,
    CorrectionReplayState,
    ProfileCorrectionEvent,
    SessionRecord,
)

ValidationRecords = tuple[CorrectionReplayState, Sequence[SessionRecord], Sequence[SessionRecord]]


def validate_correction(owner: Any, event: ProfileCorrectionEvent, records: ValidationRecords | None) -> None:
    if records is None:
        state = owner.correction_replay_state()
        active_records, closed_records = owner.load_active_sessions(), owner.list_closed_sessions()
    else:
        state, active_records, closed_records = records
    session_by_id = {record.session_id: record for record in chain(active_records, closed_records)}
    profile_ids = owner._known_profile_ids(
        state=state,
        active_records=active_records,
        closed_records=closed_records,
        active_profiles=owner.load_active_profiles(),
    )
    if event.action == CORRECTION_ACTION_RENAME_PROFILE:
        profile_id = _required(event.profile_id, "profile_id")
        if owner.resolve_profile_id(profile_id, merges=state.merges) not in profile_ids:
            raise ArchiveSchemaError("unknown profile_id")
    elif event.action == CORRECTION_ACTION_MERGE_PROFILES:
        source = owner.resolve_profile_id(_required(event.source_profile_id, "source_profile_id"), merges=state.merges)
        target = owner.resolve_profile_id(_required(event.target_profile_id, "target_profile_id"), merges=state.merges)
        if source not in profile_ids or target not in profile_ids:
            raise ArchiveSchemaError("unknown profile_id")
        if source == target:
            raise ArchiveSchemaError("profile merge cycle detected")
    elif event.action == CORRECTION_ACTION_WRONG_MATCH:
        session_id = _required(event.session_id, "session_id")
        session = session_by_id.get(session_id)
        if session is None:
            raise ArchiveSchemaError("unknown session_id")
        if event.profile_id is not None and owner.resolve_profile_id(
            session.profile_id, merges=state.merges
        ) != owner.resolve_profile_id(event.profile_id, merges=state.merges):
            raise ArchiveSchemaError("wrong_match profile_id does not match session profile")
    elif event.action == CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED:
        profile_id = _required(event.profile_id, "profile_id")
        if owner.resolve_profile_id(profile_id, merges=state.merges) not in profile_ids:
            raise ArchiveSchemaError("unknown profile_id")


def _required(value: str | None, field_name: str) -> str:
    if value is None:
        raise ArchiveSchemaError(f"correction missing {field_name}")
    return value
