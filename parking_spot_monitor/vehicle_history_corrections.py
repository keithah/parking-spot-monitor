from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from itertools import chain
from pathlib import Path
from typing import Any

from parking_spot_monitor.logging import redact_diagnostic_text
from parking_spot_monitor.vehicle_history_correction_cache import _canonical_profile_map, build_correction_replay_state
from parking_spot_monitor.vehicle_history_correction_io import append_bounded_correction_event, load_correction_events, quarantine_correction_line
from parking_spot_monitor.vehicle_history_models import (
    CORRECTION_ACTION_MERGE_PROFILES,
    CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED,
    CORRECTION_ACTION_RENAME_PROFILE,
    CORRECTION_ACTION_WRONG_MATCH,
    MAX_CORRECTION_LINE_BYTES,
    MAX_CORRECTION_FILE_BYTES,
    MAX_CORRECTION_COMPACT_BYTES,
    MAX_CORRECTION_EVENTS,
    MAX_CORRECTION_INVALID_LINES,
    MAX_CORRECTION_TEXT_LENGTH,
    SCHEMA_VERSION,
    ArchiveSchemaError,
    ArchiveWriteError,
    CorrectionReplayState,
    ProfileCorrectionEvent,
    SessionRecord,
    _bounded_string,
    _correction_id,
    _optional_bounded_string,
    _optional_profile_id,
    _safe_error_message,
    _utc_now,
)
from parking_spot_monitor.vehicle_history_profile_utils import _session_with_profile
from parking_spot_monitor.vehicle_history_correction_validation import validate_correction

_CorrectionValidationRecords = tuple[CorrectionReplayState, Sequence[SessionRecord], Sequence[SessionRecord]]


class VehicleHistoryCorrectionMixin:
    def append_correction(self, event: ProfileCorrectionEvent) -> ProfileCorrectionEvent:
        return self._append_correction(event)

    def _append_correction(
        self, event: ProfileCorrectionEvent, *, validation_records: _CorrectionValidationRecords | None = None
    ) -> ProfileCorrectionEvent:
        """Persist a validated correction event without rewriting archive records."""

        event = ProfileCorrectionEvent.from_json_dict(event.to_json_dict())
        complete_state = self._complete_correction_replay_state()
        if validation_records is None:
            validation_records = (
                complete_state,
                self.load_active_sessions(),
                self.list_closed_sessions(),
            )
        else:
            _state, active_records, closed_records = validation_records
            validation_records = (complete_state, active_records, closed_records)
        self._validate_correction_against_archive(event, records=validation_records)
        line = json.dumps(event.to_json_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(line.encode("utf-8")) > MAX_CORRECTION_LINE_BYTES:
            raise ArchiveSchemaError("correction event exceeds maximum size")
        try:
            compacted = append_bounded_correction_event(
                self.corrections_path,
                line,
                current_count=complete_state.valid_count,
                max_events=MAX_CORRECTION_EVENTS,
                max_file_bytes=MAX_CORRECTION_FILE_BYTES,
                compact_at_bytes=MAX_CORRECTION_COMPACT_BYTES,
                load_events=self._load_correction_replay,
                record_failure=self._record_failure,
            )
            if compacted:
                self._bump_correction_revision()
        except ArchiveWriteError:
            raise
        except OSError as exc:
            self._record_failure(phase="correction-append", path_name=self.corrections_path.name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc
        self._bump_revision()
        self._bump_correction_revision()
        self._log(
            "info",
            "vehicle-profile-correction-appended",
            phase="correction-append",
            action=event.action,
            correction_id=event.correction_id,
            matrix_event_id=event.matrix_event_id,
            matrix_sender=event.matrix_sender,
            matrix_room_id=event.matrix_room_id,
        )
        return event

    def rename_profile(
        self,
        profile_id: str,
        label: str,
        *,
        matrix_event_id: str | None = None,
        matrix_sender: str | None = None,
        matrix_room_id: str | None = None,
    ) -> ProfileCorrectionEvent:
        return self.append_correction(
            ProfileCorrectionEvent(
                schema_version=SCHEMA_VERSION,
                correction_id=_correction_id(CORRECTION_ACTION_RENAME_PROFILE),
                action=CORRECTION_ACTION_RENAME_PROFILE,
                created_at=_utc_now(),
                matrix_event_id=_optional_bounded_string(matrix_event_id, "matrix_event_id", max_length=160),
                matrix_sender=_optional_bounded_string(matrix_sender, "matrix_sender", max_length=160),
                matrix_room_id=_optional_bounded_string(matrix_room_id, "matrix_room_id", max_length=160),
                profile_id=_optional_profile_id(profile_id, "profile_id"),
                label=_bounded_string(label, "label", max_length=MAX_CORRECTION_TEXT_LENGTH),
            )
        )

    def merge_profiles(
        self,
        source_profile_id: str,
        target_profile_id: str,
        *,
        matrix_event_id: str | None = None,
        matrix_sender: str | None = None,
        matrix_room_id: str | None = None,
    ) -> ProfileCorrectionEvent:
        return self.append_correction(
            ProfileCorrectionEvent(
                schema_version=SCHEMA_VERSION,
                correction_id=_correction_id(CORRECTION_ACTION_MERGE_PROFILES),
                action=CORRECTION_ACTION_MERGE_PROFILES,
                created_at=_utc_now(),
                matrix_event_id=_optional_bounded_string(matrix_event_id, "matrix_event_id", max_length=160),
                matrix_sender=_optional_bounded_string(matrix_sender, "matrix_sender", max_length=160),
                matrix_room_id=_optional_bounded_string(matrix_room_id, "matrix_room_id", max_length=160),
                source_profile_id=_optional_profile_id(source_profile_id, "source_profile_id"),
                target_profile_id=_optional_profile_id(target_profile_id, "target_profile_id"),
            )
        )

    def mark_wrong_match(
        self,
        session_id: str,
        *,
        profile_id: str | None = None,
        matrix_event_id: str | None = None,
        matrix_sender: str | None = None,
        matrix_room_id: str | None = None,
    ) -> ProfileCorrectionEvent:
        return self.append_correction(
            ProfileCorrectionEvent(
                schema_version=SCHEMA_VERSION,
                correction_id=_correction_id(CORRECTION_ACTION_WRONG_MATCH),
                action=CORRECTION_ACTION_WRONG_MATCH,
                created_at=_utc_now(),
                matrix_event_id=_optional_bounded_string(matrix_event_id, "matrix_event_id", max_length=160),
                matrix_sender=_optional_bounded_string(matrix_sender, "matrix_sender", max_length=160),
                matrix_room_id=_optional_bounded_string(matrix_room_id, "matrix_room_id", max_length=160),
                session_id=_bounded_string(session_id, "session_id", max_length=220),
                profile_id=_optional_profile_id(profile_id, "profile_id"),
            )
        )

    def load_corrections(self) -> list[ProfileCorrectionEvent]:
        self.corrections_dir.mkdir(parents=True, exist_ok=True)
        return list(self._load_correction_replay().events)

    def correction_event_seen(self, event_id: str) -> bool:
        return event_id in self.correction_replay_state().matrix_event_ids

    def _load_correction_replay(self):
        return load_correction_events(
            self.corrections_path,
            max_line_bytes=MAX_CORRECTION_LINE_BYTES,
            max_file_bytes=MAX_CORRECTION_FILE_BYTES,
            max_events=MAX_CORRECTION_EVENTS,
            max_invalid_lines=MAX_CORRECTION_INVALID_LINES,
            quarantine_path=self.corrections_quarantine_path,
            quarantine_line=self._quarantine_correction_line,
            record_failure=self._record_failure,
        )

    def correction_replay_state(self) -> CorrectionReplayState:
        cached, before = self._correction_replay_cache.lookup(
            revision=self.correction_revision(),
            corrections_path=self.corrections_path,
            quarantine_path=self.corrections_quarantine_path,
        )
        if cached is not None:
            return cached
        loaded = self._load_correction_replay()
        after_load = self._correction_replay_cache.snapshot(
            revision=self.correction_revision(),
            corrections_path=self.corrections_path,
            quarantine_path=self.corrections_quarantine_path,
        )
        after_count = self._correction_replay_cache.snapshot(
            revision=self.correction_revision(),
            corrections_path=self.corrections_path,
            quarantine_path=self.corrections_quarantine_path,
        )
        state = build_correction_replay_state(loaded.events, quarantine_count=loaded.quarantine_count)
        self._correction_replay_cache.store_if_stable(
            before=before,
            after_load=after_load,
            after_count=after_count,
            quarantine_writes=loaded.quarantine_writes,
            safe=loaded.succeeded,
            value=state,
        )
        return state

    def _complete_correction_replay_state(self) -> CorrectionReplayState:
        loaded = self._load_correction_replay()
        if not loaded.succeeded:
            raise ArchiveSchemaError("correction replay unavailable")
        return build_correction_replay_state(loaded.events, quarantine_count=loaded.quarantine_count)

    def resolve_profile_id(self, profile_id: str | None, *, merges: Mapping[str, str] | None = None) -> str | None:
        normalized = _optional_profile_id(profile_id, "profile_id")
        if normalized is None:
            return None
        if merges is None:
            state = self.correction_replay_state()
            return state.canonical_profile_ids.get(normalized, normalized)
        mapping = merges
        seen: set[str] = set()
        current = normalized
        while current in mapping:
            if current in seen:
                raise ArchiveSchemaError("profile merge cycle detected")
            seen.add(current)
            current = mapping[current]
        return current

    def effective_label(self, profile_id: str | None) -> str | None:
        canonical = self.resolve_profile_id(profile_id)
        if canonical is None:
            return None
        state = self.correction_replay_state()
        if canonical in state.labels:
            return state.labels[canonical]
        for profile in self.load_active_profiles():
            if self.resolve_profile_id(profile.profile_id, merges=state.merges) == canonical and profile.label is not None:
                return profile.label
        return None

    def profile_summary(
        self,
        profile_id: str,
        *,
        matrix_event_id: str | None = None,
        matrix_sender: str | None = None,
        matrix_room_id: str | None = None,
    ) -> dict[str, Any]:
        canonical = self.resolve_profile_id(profile_id)
        if canonical is None:
            raise ArchiveSchemaError("profile_id is required")
        state = self.correction_replay_state()
        closed = self._effective_sessions(self.list_closed_sessions(), state=state, exclude_wrong_matches=False)
        active = self._effective_sessions(self.load_active_sessions(), state=state, exclude_wrong_matches=False)
        closed_session_count, closed_excluded = _profile_session_counts(closed, profile_id=canonical, wrong_matches=state.wrong_match_session_ids)
        active_session_count, active_excluded = _profile_session_counts(active, profile_id=canonical, wrong_matches=state.wrong_match_session_ids)
        excluded_count = closed_excluded + active_excluded
        estimate = self._estimate_for_profile_records(
            canonical,
            self._effective_sessions(closed, state=state),
            state=state,
            min_samples=2,
            min_profile_confidence=0.76,
        )
        self._append_correction(
            ProfileCorrectionEvent(
                schema_version=SCHEMA_VERSION,
                correction_id=_correction_id(CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED),
                action=CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED,
                created_at=_utc_now(),
                matrix_event_id=_optional_bounded_string(matrix_event_id, "matrix_event_id", max_length=160),
                matrix_sender=_optional_bounded_string(matrix_sender, "matrix_sender", max_length=160),
                matrix_room_id=_optional_bounded_string(matrix_room_id, "matrix_room_id", max_length=160),
                profile_id=canonical,
            ),
            validation_records=(state, active, closed),
        )
        return {
            "profile_id": canonical,
            "requested_profile_id": profile_id,
            "label": self.effective_label(canonical),
            "closed_session_count": closed_session_count,
            "active_session_count": active_session_count,
            "wrong_match_excluded_session_count": excluded_count,
            "merged_profile_ids": sorted(source for source, target in state.merges.items() if target == canonical),
            "estimate_status": estimate.status,
            "estimate_reason": estimate.reason,
            "estimate_sample_count": estimate.sample_count,
            "estimate_confidence": estimate.confidence,
        }

    def read_matrix_cursor(self) -> dict[str, Any] | None:
        if not self.matrix_state_path.exists():
            return None
        try:
            if self.matrix_state_path.stat().st_size > MAX_CORRECTION_LINE_BYTES:
                raise ArchiveSchemaError("matrix state exceeds maximum size")
            payload = json.loads(self.matrix_state_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ArchiveSchemaError("matrix state must be an object")
            return {str(key): _optional_bounded_string(value, str(key), max_length=MAX_CORRECTION_TEXT_LENGTH) for key, value in payload.items()}
        except (OSError, json.JSONDecodeError, ArchiveSchemaError, ValueError) as exc:
            self._record_failure(phase="matrix-state-load", path_name=self.matrix_state_path.name, error=exc)
            return None

    def write_matrix_cursor(self, state: Mapping[str, Any]) -> None:
        payload = {str(key): _optional_bounded_string(value, str(key), max_length=MAX_CORRECTION_TEXT_LENGTH) for key, value in state.items()}
        self.corrections_dir.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=self.corrections_dir, prefix=".matrix-state.", suffix=".tmp") as handle:
                temp_path = Path(handle.name)
                json.dump(payload, handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_path, 0o644)
            os.replace(temp_path, self.matrix_state_path)
        except Exception as exc:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            self._record_failure(phase="matrix-state-write", path_name=self.matrix_state_path.name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc

    def _effective_sessions(
        self,
        records: Sequence[SessionRecord],
        *,
        state: CorrectionReplayState | None = None,
        exclude_wrong_matches: bool = True,
    ) -> list[SessionRecord]:
        state = state if state is not None else self.correction_replay_state()
        canonical_profile_ids = state.canonical_profile_ids or _canonical_profile_map(state.merges)
        effective: list[SessionRecord] = []
        for record in records:
            if exclude_wrong_matches and record.session_id in state.wrong_match_session_ids:
                continue
            canonical = canonical_profile_ids.get(record.profile_id, record.profile_id) if record.profile_id is not None else None
            if canonical is None or canonical == record.profile_id:
                effective.append(record)
            else:
                effective.append(_session_with_profile(record, profile_id=canonical, confidence=record.profile_confidence or 0.0))
        return effective

    def _validate_correction_against_archive(
        self, event: ProfileCorrectionEvent, *, records: _CorrectionValidationRecords | None = None
    ) -> None:
        validate_correction(self, event, records)

    def _known_profile_ids(self, *, state: CorrectionReplayState | None = None, active_records: Sequence[SessionRecord] | None = None, closed_records: Sequence[SessionRecord] | None = None, active_profiles: Sequence[Any] | None = None) -> set[str]:
        state = state if state is not None else self.correction_replay_state()
        profiles = active_profiles if active_profiles is not None else self.load_active_profiles()
        active = active_records if active_records is not None else self.load_active_sessions()
        closed = closed_records if closed_records is not None else self.list_closed_sessions()
        profile_ids = {self.resolve_profile_id(profile.profile_id, merges=state.merges) for profile in profiles}
        for record in chain(active, closed):
            resolved = self.resolve_profile_id(record.profile_id, merges=state.merges)
            if resolved is not None:
                profile_ids.add(resolved)
        return {profile_id for profile_id in profile_ids if profile_id is not None}

    def _quarantine_correction_line(
        self,
        *,
        line_number: int,
        reason: str,
        known_keys: set[tuple[int, str]] | None = None,
    ):
        self.corrections_dir.mkdir(parents=True, exist_ok=True)
        safe_reason = redact_diagnostic_text(reason)
        outcome = quarantine_correction_line(
            self.corrections_quarantine_path,
            line_number=line_number,
            reason=safe_reason,
            quarantined_at=_utc_now(),
            record_failure=self._record_failure,
            bump_revision=self._bump_correction_quarantine_revisions,
            known_keys=known_keys,
        )
        self._log("warning", "vehicle-profile-correction-quarantined", phase="correction-load", line_number=line_number, reason=reason)
        return outcome

    def _bump_correction_quarantine_revisions(self) -> None:
        self._bump_revision()
        self._bump_correction_revision()


def _profile_session_counts(records: Sequence[SessionRecord], *, profile_id: str, wrong_matches: set[str] | frozenset[str]) -> tuple[int, int]:
    kept = excluded = 0
    for record in records:
        if record.profile_id == profile_id:
            excluded += int(record.session_id in wrong_matches)
            kept += int(record.session_id not in wrong_matches)
    return kept, excluded
