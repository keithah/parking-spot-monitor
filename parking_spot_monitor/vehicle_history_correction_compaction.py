"""Fail-closed correction replay and legacy read-audit compaction."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from parking_spot_monitor.vehicle_history_correction_cache import build_correction_replay_state
from parking_spot_monitor.vehicle_history_correction_io import (
    CorrectionReplayLoadResult,
    compact_correction_events,
)
from parking_spot_monitor.vehicle_history_models import (
    CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED,
    MAX_CORRECTION_FILE_BYTES,
    ArchiveSchemaError,
    ArchiveWriteError,
    CorrectionReplayState,
)


class CorrectionReplayOwner(Protocol):
    corrections_path: Path

    def _load_correction_replay(self) -> CorrectionReplayLoadResult: ...

    def _record_failure(self, **fields: Any) -> None: ...

    def _bump_revision(self) -> None: ...

    def _bump_correction_revision(self) -> None: ...


def load_complete_correction_state(owner: CorrectionReplayOwner) -> CorrectionReplayState:
    loaded = owner._load_correction_replay()
    if not loaded.succeeded:
        raise ArchiveSchemaError("correction replay unavailable")
    actionable = tuple(
        event
        for event in loaded.events
        if event.action != CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED
    )
    if len(actionable) != len(loaded.events):
        if not compact_correction_events(
            owner.corrections_path,
            actionable,
            max_file_bytes=MAX_CORRECTION_FILE_BYTES,
            record_failure=owner._record_failure,
        ):
            raise ArchiveWriteError("legacy summary audit could not be compacted")
        owner._bump_revision()
        owner._bump_correction_revision()
    return build_correction_replay_state(
        actionable,
        quarantine_count=loaded.quarantine_count,
    )
