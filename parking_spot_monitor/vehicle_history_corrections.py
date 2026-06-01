from __future__ import annotations

import json
import os
import tempfile

from parking_spot_monitor.vehicle_history_models import *


class VehicleHistoryCorrectionMixin:
    def append_correction(self, event: ProfileCorrectionEvent) -> ProfileCorrectionEvent:
        """Persist a validated correction event without rewriting archive records."""

        event = ProfileCorrectionEvent.from_json_dict(event.to_json_dict())
        self._validate_correction_against_archive(event)
        line = json.dumps(event.to_json_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(line.encode("utf-8")) > MAX_CORRECTION_LINE_BYTES:
            raise ArchiveSchemaError("correction event exceeds maximum size")
        self.corrections_dir.mkdir(parents=True, exist_ok=True)
        try:
            with self.corrections_path.open("a", encoding="utf-8") as handle:
                handle.write(line)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            self._record_failure(phase="correction-append", path_name=self.corrections_path.name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc
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
        if not self.corrections_path.exists():
            return []
        corrections: list[ProfileCorrectionEvent] = []
        try:
            with self.corrections_path.open("rb") as handle:
                for line_number, raw_line in enumerate(handle, start=1):
                    if len(raw_line) > MAX_CORRECTION_LINE_BYTES:
                        self._quarantine_correction_line(line_number=line_number, reason="line-too-large")
                        continue
                    try:
                        text = raw_line.decode("utf-8")
                        if not text.strip():
                            continue
                        payload = json.loads(text)
                        corrections.append(ProfileCorrectionEvent.from_json_dict(payload))
                    except (UnicodeDecodeError, json.JSONDecodeError, ArchiveSchemaError, ValueError) as exc:
                        self._quarantine_correction_line(line_number=line_number, reason=type(exc).__name__)
        except OSError as exc:
            self._record_failure(phase="correction-load", path_name=self.corrections_path.name, error=exc)
            return corrections
        return corrections

    def correction_replay_state(self) -> CorrectionReplayState:
        labels: dict[str, str] = {}
        merges: dict[str, str] = {}
        wrong_matches: set[str] = set()
        valid_count = 0
        last_action: str | None = None
        last_created_at: str | None = None
        for event in self.load_corrections():
            valid_count += 1
            last_action = event.action
            last_created_at = event.created_at
            if event.action == CORRECTION_ACTION_RENAME_PROFILE and event.profile_id is not None and event.label is not None:
                labels[self.resolve_profile_id(event.profile_id, merges=merges)] = event.label
            elif event.action == CORRECTION_ACTION_MERGE_PROFILES and event.source_profile_id is not None and event.target_profile_id is not None:
                merges[event.source_profile_id] = self.resolve_profile_id(event.target_profile_id, merges=merges)
            elif event.action == CORRECTION_ACTION_WRONG_MATCH and event.session_id is not None:
                wrong_matches.add(event.session_id)
        return CorrectionReplayState(
            labels=labels,
            merges=merges,
            wrong_match_session_ids=frozenset(wrong_matches),
            valid_count=valid_count,
            invalid_count=self._correction_quarantine_count(),
            quarantine_count=self._correction_quarantine_count(),
            last_action=last_action,
            last_created_at=last_created_at,
        )

    def resolve_profile_id(self, profile_id: str | None, *, merges: Mapping[str, str] | None = None) -> str | None:
        normalized = _optional_profile_id(profile_id, "profile_id")
        if normalized is None:
            return None
        mapping = dict(merges) if merges is not None else dict(self.correction_replay_state().merges)
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
        relevant = [record for record in [*closed, *active] if record.profile_id == canonical]
        excluded = [record for record in relevant if record.session_id in state.wrong_match_session_ids]
        estimate = self.estimate_for_profile(canonical)
        self.append_correction(
            ProfileCorrectionEvent(
                schema_version=SCHEMA_VERSION,
                correction_id=_correction_id(CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED),
                action=CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED,
                created_at=_utc_now(),
                matrix_event_id=_optional_bounded_string(matrix_event_id, "matrix_event_id", max_length=160),
                matrix_sender=_optional_bounded_string(matrix_sender, "matrix_sender", max_length=160),
                matrix_room_id=_optional_bounded_string(matrix_room_id, "matrix_room_id", max_length=160),
                profile_id=canonical,
            )
        )
        return {
            "profile_id": canonical,
            "requested_profile_id": profile_id,
            "label": self.effective_label(canonical),
            "closed_session_count": sum(1 for record in closed if record.profile_id == canonical and record.session_id not in state.wrong_match_session_ids),
            "active_session_count": sum(1 for record in active if record.profile_id == canonical and record.session_id not in state.wrong_match_session_ids),
            "wrong_match_excluded_session_count": len(excluded),
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
        effective: list[SessionRecord] = []
        for record in records:
            if exclude_wrong_matches and record.session_id in state.wrong_match_session_ids:
                continue
            canonical = self.resolve_profile_id(record.profile_id, merges=state.merges)
            if canonical is None or canonical == record.profile_id:
                effective.append(record)
            else:
                effective.append(_session_with_profile(record, profile_id=canonical, confidence=record.profile_confidence or 0.0))
        return effective

    def _validate_correction_against_archive(self, event: ProfileCorrectionEvent) -> None:
        state = self.correction_replay_state()
        profile_ids = self._known_profile_ids(state=state)
        session_ids = {record.session_id for record in [*self.load_active_sessions(), *self.list_closed_sessions()]}
        if event.action == CORRECTION_ACTION_RENAME_PROFILE:
            assert event.profile_id is not None
            if self.resolve_profile_id(event.profile_id, merges=state.merges) not in profile_ids:
                raise ArchiveSchemaError("unknown profile_id")
        elif event.action == CORRECTION_ACTION_MERGE_PROFILES:
            assert event.source_profile_id is not None and event.target_profile_id is not None
            source = self.resolve_profile_id(event.source_profile_id, merges=state.merges)
            target = self.resolve_profile_id(event.target_profile_id, merges=state.merges)
            if source not in profile_ids or target not in profile_ids:
                raise ArchiveSchemaError("unknown profile_id")
            if source == target:
                raise ArchiveSchemaError("profile merge cycle detected")
        elif event.action == CORRECTION_ACTION_WRONG_MATCH:
            assert event.session_id is not None
            if event.session_id not in session_ids:
                raise ArchiveSchemaError("unknown session_id")
            if event.profile_id is not None:
                session = next(record for record in [*self.load_active_sessions(), *self.list_closed_sessions()] if record.session_id == event.session_id)
                if self.resolve_profile_id(session.profile_id, merges=state.merges) != self.resolve_profile_id(event.profile_id, merges=state.merges):
                    raise ArchiveSchemaError("wrong_match profile_id does not match session profile")
        elif event.action == CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED:
            assert event.profile_id is not None
            if self.resolve_profile_id(event.profile_id, merges=state.merges) not in profile_ids:
                raise ArchiveSchemaError("unknown profile_id")

    def _known_profile_ids(self, *, state: CorrectionReplayState | None = None) -> set[str]:
        state = state if state is not None else self.correction_replay_state()
        profile_ids = {self.resolve_profile_id(profile.profile_id, merges=state.merges) for profile in self.load_active_profiles()}
        for record in [*self.load_active_sessions(), *self.list_closed_sessions()]:
            resolved = self.resolve_profile_id(record.profile_id, merges=state.merges)
            if resolved is not None:
                profile_ids.add(resolved)
        return {profile_id for profile_id in profile_ids if profile_id is not None}

    def _quarantine_correction_line(self, *, line_number: int, reason: str) -> None:
        self.corrections_dir.mkdir(parents=True, exist_ok=True)
        entry = {"line_number": line_number, "reason": redact_diagnostic_text(reason), "quarantined_at": _utc_now()}
        try:
            with self.corrections_quarantine_path.open("a", encoding="utf-8") as handle:
                json.dump(entry, handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
                handle.write("\n")
        except OSError as exc:
            self._record_failure(phase="correction-quarantine", path_name=self.corrections_quarantine_path.name, error=exc)
        self._log("warning", "vehicle-profile-correction-quarantined", phase="correction-load", line_number=line_number, reason=reason)

    def _correction_quarantine_count(self) -> int:
        if not self.corrections_quarantine_path.exists():
            return 0
        try:
            with self.corrections_quarantine_path.open("r", encoding="utf-8") as handle:
                return sum(1 for _ in handle)
        except OSError as exc:
            self._record_failure(phase="correction-quarantine-count", path_name=self.corrections_quarantine_path.name, error=exc)
            return 0
