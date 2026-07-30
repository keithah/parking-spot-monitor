from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from parking_spot_monitor.vehicle_history_models import (
    CORRECTION_ACTION_MERGE_PROFILES,
    CORRECTION_ACTION_RENAME_PROFILE,
    CORRECTION_ACTION_WRONG_MATCH,
    ArchiveSchemaError,
    CorrectionReplaySignature,
    CorrectionReplayState,
    ProfileCorrectionEvent,
)


class CorrectionReplayCache:
    """One-entry replay cache guarded by revision and source-file signatures."""

    def __init__(self) -> None:
        self._entry: tuple[CorrectionReplaySignature, CorrectionReplayState] | None = None

    def lookup(
        self,
        *,
        revision: int,
        corrections_path: Path,
        quarantine_path: Path,
    ) -> tuple[CorrectionReplayState | None, CorrectionReplaySourceSnapshot]:
        snapshot = self.snapshot(
            revision=revision,
            corrections_path=corrections_path,
            quarantine_path=quarantine_path,
        )
        entry = self._entry
        if snapshot.available and entry is not None and snapshot.signature == entry[0]:
            return entry[1], snapshot
        return None, snapshot

    def snapshot(
        self,
        *,
        revision: int,
        corrections_path: Path,
        quarantine_path: Path,
    ) -> CorrectionReplaySourceSnapshot:
        return _replay_snapshot(
            revision=revision,
            corrections_path=corrections_path,
            quarantine_path=quarantine_path,
        )

    def store_if_stable(
        self,
        *,
        before: CorrectionReplaySourceSnapshot,
        after_load: CorrectionReplaySourceSnapshot,
        after_count: CorrectionReplaySourceSnapshot,
        quarantine_writes: int,
        safe: bool,
        value: CorrectionReplayState,
    ) -> bool:
        corrections_stat = before.signature.corrections_stat
        stable = (
            safe
            and before.available
            and after_load.available
            and after_count.available
            and corrections_stat == after_load.signature.corrections_stat == after_count.signature.corrections_stat
            and after_load.signature == after_count.signature
            and after_load.signature.revision == before.signature.revision + quarantine_writes
        )
        if not stable:
            self._entry = None
            return False
        self._entry = (after_count.signature, value)
        return True


@dataclass(frozen=True)
class CorrectionReplaySourceSnapshot:
    signature: CorrectionReplaySignature
    available: bool


def build_correction_replay_state(
    events: Iterable[ProfileCorrectionEvent],
    *,
    quarantine_count: int,
) -> CorrectionReplayState:
    labels: dict[str, str] = {}
    merges: dict[str, str] = {}
    wrong_matches: set[str] = set()
    matrix_event_ids: set[str] = set()
    valid_count = 0
    last_action: str | None = None
    last_created_at: str | None = None
    for event in events:
        valid_count += 1
        last_action = event.action
        last_created_at = event.created_at
        if event.matrix_event_id:
            matrix_event_ids.add(event.matrix_event_id)
        if event.action == CORRECTION_ACTION_RENAME_PROFILE and event.profile_id is not None and event.label is not None:
            labels[_resolve_profile_id(event.profile_id, merges)] = event.label
        elif event.action == CORRECTION_ACTION_MERGE_PROFILES and event.source_profile_id is not None and event.target_profile_id is not None:
            merges[event.source_profile_id] = _resolve_profile_id(event.target_profile_id, merges)
        elif event.action == CORRECTION_ACTION_WRONG_MATCH and event.session_id is not None:
            wrong_matches.add(event.session_id)
    return CorrectionReplayState(
        labels=MappingProxyType(labels),
        merges=MappingProxyType(merges),
        wrong_match_session_ids=frozenset(wrong_matches),
        valid_count=valid_count,
        invalid_count=quarantine_count,
        quarantine_count=quarantine_count,
        last_action=last_action,
        last_created_at=last_created_at,
        canonical_profile_ids=MappingProxyType(_canonical_profile_map(merges)),
        matrix_event_ids=frozenset(matrix_event_ids),
    )


def _resolve_profile_id(profile_id: str, merges: Mapping[str, str]) -> str:
    seen: set[str] = set()
    current = profile_id
    while current in merges:
        if current in seen:
            raise ArchiveSchemaError("profile merge cycle detected")
        seen.add(current)
        current = merges[current]
    return current


def _canonical_profile_map(merges: Mapping[str, str]) -> dict[str, str]:
    canonical: dict[str, str] = {}
    for profile_id in set(merges) | set(merges.values()):
        trail: list[str] = []
        current = profile_id
        while current in merges and current not in canonical:
            if current in trail:
                raise ArchiveSchemaError("profile merge cycle detected")
            trail.append(current)
            current = merges[current]
        resolved = canonical.get(current, current)
        canonical[current] = resolved
        for item in trail:
            canonical[item] = resolved
    return canonical


def _replay_snapshot(
    *,
    revision: int,
    corrections_path: Path,
    quarantine_path: Path,
) -> CorrectionReplaySourceSnapshot:
    corrections_stat, corrections_available = _file_stat_signature(corrections_path)
    quarantine_stat, quarantine_available = _file_stat_signature(quarantine_path)
    return CorrectionReplaySourceSnapshot(
        signature=CorrectionReplaySignature(
            revision=revision,
            corrections_stat=corrections_stat,
            quarantine_stat=quarantine_stat,
        ),
        available=corrections_available and quarantine_available,
    )


def _file_stat_signature(path: Path) -> tuple[tuple[int, int] | None, bool]:
    try:
        stat_result = path.stat()
    except FileNotFoundError:
        return None, True
    except OSError:
        return None, False
    return (stat_result.st_mtime_ns, stat_result.st_size), True
