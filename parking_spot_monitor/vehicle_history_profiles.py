from __future__ import annotations

from pathlib import Path

from parking_spot_monitor.owner_vehicles import load_owner_vehicle_registry
from parking_spot_monitor.vehicle_estimates import VehicleHistoryEstimate, estimate_vehicle_history
from parking_spot_monitor.vehicle_profiles import (
    MatchStatus,
    VehicleProfileDescriptorError,
    extract_vehicle_descriptor,
    match_vehicle_profile,
)
from parking_spot_monitor.vehicle_history_models import (
    OWNER_PROFILE_MIN_ASSIGNMENT_CONFIDENCE,
    PROFILE_STATUS_ACTIVE,
    SCHEMA_VERSION,
    ArchiveSchemaError,
    ArchiveWriteError,
    ProfileAssignment,
    StoredVehicleProfile,
    _bounded_string,
    _safe_error_message,
    _slug,
    _utc_now,
)
from parking_spot_monitor.vehicle_history_profile_utils import (
    _is_owner_profile_low_confidence_match,
    _profile_with_sample,
    _session_with_profile,
)


class VehicleHistoryProfileMixin:
    def match_or_create_profile(self, *, session_id: str) -> ProfileAssignment:
        """Assign a stable visual profile id to an active session when confidence permits."""

        active_path = self.active_dir / f"{session_id}.json"
        if not active_path.exists():
            error = ArchiveSchemaError("active session is missing")
            self._record_failure(phase="profile-match", path_name=active_path.name, error=error, session_id=session_id)
            raise ArchiveWriteError(_safe_error_message(error)) from error
        record = self._load_record(active_path)
        if record is None or record.session_id != session_id:
            error = ArchiveSchemaError("active session is missing")
            self._record_failure(phase="profile-match", path_name=active_path.name, error=error, session_id=session_id)
            raise ArchiveWriteError(_safe_error_message(error)) from error

        if record.profile_id is not None and record.profile_confidence is not None:
            self._log(
                "info",
                "vehicle-session-profile-noop",
                spot_id=record.spot_id,
                session_id=record.session_id,
                profile_id=record.profile_id,
                reason="already-assigned",
            )
            return ProfileAssignment(
                session_id=record.session_id,
                status=MatchStatus.MATCHED.value,
                profile_id=record.profile_id,
                profile_confidence=record.profile_confidence,
                reason="already-assigned",
            )

        if record.occupied_crop_path is None:
            error = ArchiveSchemaError("active session is missing occupied_crop_path")
            self._record_failure(phase="profile-match", path_name=active_path.name, error=error, session_id=record.session_id)
            raise ArchiveWriteError(_safe_error_message(error)) from error

        try:
            descriptor = extract_vehicle_descriptor(record.occupied_crop_path)
            profiles = self.load_active_profiles()
            result = _match_vehicle_profile(descriptor, [profile.as_match_record() for profile in profiles])
        except (VehicleProfileDescriptorError, ValueError, OSError) as exc:
            self._record_failure(phase="profile-match", path_name=Path(record.occupied_crop_path).name, error=exc, session_id=record.session_id)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc

        if result.status is MatchStatus.MATCHED and result.profile_id is not None:
            matched = next(profile for profile in profiles if profile.profile_id == result.profile_id)
            if _is_owner_profile_low_confidence_match(self.root, matched.profile_id, result.confidence):
                self._log(
                    "info",
                    "vehicle-session-profile-owner-match-skipped",
                    spot_id=record.spot_id,
                    session_id=record.session_id,
                    profile_id=matched.profile_id,
                    profile_confidence=result.confidence,
                    min_profile_confidence=OWNER_PROFILE_MIN_ASSIGNMENT_CONFIDENCE,
                    reason="owner-profile-confidence-too-low",
                )
                return ProfileAssignment(record.session_id, MatchStatus.UNKNOWN.value, None, None, "owner-profile-confidence-too-low")
            updated_profile = _profile_with_sample(matched, descriptor=descriptor, session_id=record.session_id, crop_path=record.occupied_crop_path)
            self._write_profile(self.active_profiles_dir / f"{updated_profile.profile_id}.json", updated_profile, phase="profile-match")
            updated_record = _session_with_profile(record, profile_id=result.profile_id, confidence=result.confidence)
            self._write_record(active_path, updated_record, phase="profile-match")
            self._log(
                "info",
                "vehicle-session-profile-matched",
                spot_id=record.spot_id,
                session_id=record.session_id,
                profile_id=result.profile_id,
                profile_confidence=result.confidence,
                reason=result.reason,
            )
            return ProfileAssignment(record.session_id, result.status.value, result.profile_id, result.confidence, result.reason)

        if result.status is MatchStatus.NEW_PROFILE:
            profile_id = self._new_profile_id(record.session_id)
            now = _utc_now()
            profile = StoredVehicleProfile(
                schema_version=SCHEMA_VERSION,
                profile_id=profile_id,
                label=None,
                status=PROFILE_STATUS_ACTIVE,
                descriptor=descriptor,
                sample_count=1,
                sample_session_ids=(record.session_id,),
                exemplar_crop_path=Path(record.occupied_crop_path).name,
                created_at=now,
                updated_at=now,
            )
            self._write_profile(self.active_profiles_dir / f"{profile_id}.json", profile, phase="profile-match")
            updated_record = _session_with_profile(record, profile_id=profile_id, confidence=1.0)
            self._write_record(active_path, updated_record, phase="profile-match")
            self._log(
                "info",
                "vehicle-session-profile-created",
                spot_id=record.spot_id,
                session_id=record.session_id,
                profile_id=profile_id,
                reason=result.reason,
            )
            return ProfileAssignment(record.session_id, result.status.value, profile_id, 1.0, result.reason)

        self._log(
            "info",
            "vehicle-session-profile-unknown",
            spot_id=record.spot_id,
            session_id=record.session_id,
            reason=result.reason,
            profile_status=result.status.value,
        )
        return ProfileAssignment(record.session_id, result.status.value, None, None, result.reason)

    def assign_owner_profile_to_active_spot(self, spot_id: str) -> ProfileAssignment:
        """Mark the active session in a spot as the configured owner vehicle."""

        normalized_spot_id = _bounded_string(spot_id, "spot_id", max_length=220)
        active_sessions = [record for record in self.load_active_sessions() if record.spot_id == normalized_spot_id]
        if not active_sessions:
            raise ArchiveSchemaError(f"no active session for spot {normalized_spot_id}")
        active_sessions.sort(key=lambda record: record.started_at)
        record = active_sessions[-1]
        if record.occupied_crop_path is None:
            raise ArchiveSchemaError("active session is missing occupied_crop_path")

        owner_registry = load_owner_vehicle_registry(self.root / "owner-vehicles.json")
        owner_profile_ids = sorted(owner_registry.vehicles_by_profile_id.keys())
        if len(owner_profile_ids) != 1:
            raise ArchiveSchemaError("exactly one owner vehicle profile is required")
        owner_profile_id = owner_profile_ids[0]
        profiles = {profile.profile_id: profile for profile in self.load_active_profiles()}
        profile = profiles.get(owner_profile_id)
        if profile is None:
            raise ArchiveSchemaError("owner vehicle profile is missing")

        try:
            descriptor = extract_vehicle_descriptor(record.occupied_crop_path)
        except (VehicleProfileDescriptorError, ValueError, OSError) as exc:
            self._record_failure(phase="profile-match", path_name=Path(record.occupied_crop_path).name, error=exc, session_id=record.session_id)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc

        updated_profile = _profile_with_sample(profile, descriptor=descriptor, session_id=record.session_id, crop_path=record.occupied_crop_path)
        self._write_profile(self.active_profiles_dir / f"{updated_profile.profile_id}.json", updated_profile, phase="profile-match")
        updated_record = _session_with_profile(record, profile_id=owner_profile_id, confidence=1.0)
        self._write_record(self.active_dir / f"{record.session_id}.json", updated_record, phase="profile-match")
        self._log(
            "info",
            "vehicle-session-owner-profile-assigned",
            spot_id=record.spot_id,
            session_id=record.session_id,
            profile_id=owner_profile_id,
            profile_confidence=1.0,
        )
        return ProfileAssignment(record.session_id, "owner_assigned", owner_profile_id, 1.0, "operator-confirmed-owner")

    def active_spot_assignments(self) -> list[dict[str, Any]]:
        """Return a safe operator summary of active sessions by spot."""

        owner_registry = load_owner_vehicle_registry(self.root / "owner-vehicles.json")
        profiles = {profile.profile_id: profile for profile in self.load_active_profiles()}
        assignments: list[dict[str, Any]] = []
        for record in self.load_active_sessions():
            owner = owner_registry.owner_for_profile(record.profile_id)
            profile = profiles.get(record.profile_id) if record.profile_id is not None else None
            label = owner.label if owner is not None else self.effective_label(record.profile_id)
            assignments.append(
                {
                    "spot_id": record.spot_id,
                    "session_id": record.session_id,
                    "profile_id": record.profile_id,
                    "profile_label": label,
                    "profile_confidence": record.profile_confidence,
                    "is_owner": owner is not None,
                    "owner_label": None if owner is None else owner.label,
                    "profile_sample_count": None if profile is None else profile.sample_count,
                    "started_at": record.started_at,
                }
            )
        assignments.sort(key=lambda item: str(item["spot_id"]))
        return assignments

    def load_active_profiles(self) -> list[StoredVehicleProfile]:
        self.active_profiles_dir.mkdir(parents=True, exist_ok=True)
        profiles: list[StoredVehicleProfile] = []
        for path in sorted(self.active_profiles_dir.glob("*.json")):
            profile = self._load_profile(path)
            if profile is not None:
                profiles.append(profile)
        self._log("info", "vehicle-profile-registry-loaded", profile_count=len(profiles))
        return profiles

    def _new_profile_id(self, session_id: str) -> str:
        base = f"prof_{_slug(session_id)}"[:170]
        candidate = base
        index = 1
        while (self.active_profiles_dir / f"{candidate}.json").exists():
            index += 1
            candidate = f"{base}-{index}"[:180]
        return candidate

    def estimate_for_profile(
        self,
        profile_id: str | None,
        *,
        min_samples: int = 2,
        min_profile_confidence: float = 0.76,
    ) -> VehicleHistoryEstimate:
        """Estimate repeat-vehicle history from closed sessions for a profile id."""

        state = self.correction_replay_state()
        canonical_profile_id = self.resolve_profile_id(profile_id, merges=state.merges)
        return estimate_vehicle_history(
            canonical_profile_id,
            self._effective_sessions(self.list_closed_sessions(), state=state),
            min_samples=min_samples,
            min_profile_confidence=min_profile_confidence,
        )

    def estimate_for_session(
        self,
        session_id: str,
        *,
        min_samples: int = 2,
        min_profile_confidence: float = 0.76,
    ) -> VehicleHistoryEstimate:
        """Estimate repeat-vehicle history for an active session's assigned profile.

        The active session is used only to discover the current profile id; dwell
        and leave-time evidence comes exclusively from already-closed sessions.
        """

        active_path = self.active_dir / f"{session_id}.json"
        active_record = self._load_record(active_path) if active_path.exists() else None
        state = self.correction_replay_state()
        profile_id = self.resolve_profile_id(active_record.profile_id, merges=state.merges) if active_record is not None else None
        return self.estimate_for_profile(
            profile_id,
            min_samples=min_samples,
            min_profile_confidence=min_profile_confidence,
        )


def _match_vehicle_profile(descriptor: object, profiles: object) -> object:
    return match_vehicle_profile(descriptor, profiles)
