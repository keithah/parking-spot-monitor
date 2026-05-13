from __future__ import annotations

import json
import math
import os
import re
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus
from parking_spot_monitor.vehicle_estimates import VehicleHistoryEstimate, estimate_vehicle_history
from parking_spot_monitor.vehicle_history_images import VehicleHistoryImageError, capture_occupied_images
from parking_spot_monitor.vehicle_profiles import (
    MatchStatus,
    VehicleDescriptor,
    VehicleProfileDescriptorError,
    VehicleProfileRecord,
    extract_vehicle_descriptor,
    match_vehicle_profile,
)

SCHEMA_VERSION = 1
MAX_SESSION_FILE_BYTES = 1_000_000
MAX_PROFILE_FILE_BYTES = 500_000
MAX_CORRECTION_LINE_BYTES = 16_000
MAX_CORRECTION_TEXT_LENGTH = 160
MAX_CORRECTION_INVALID_LINES = 200
PROFILE_STATUS_ACTIVE = "active"

CORRECTION_ACTION_RENAME_PROFILE = "rename_profile"
CORRECTION_ACTION_MERGE_PROFILES = "merge_profiles"
CORRECTION_ACTION_WRONG_MATCH = "wrong_match"
CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED = "profile_summary_requested"
CORRECTION_ACTIONS = frozenset(
    {
        CORRECTION_ACTION_RENAME_PROFILE,
        CORRECTION_ACTION_MERGE_PROFILES,
        CORRECTION_ACTION_WRONG_MATCH,
        CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED,
    }
)


@dataclass(frozen=True)
class ProfileCorrectionEvent:
    """Append-only operator correction event for effective vehicle-history views."""

    schema_version: int
    correction_id: str
    action: str
    created_at: str
    matrix_event_id: str | None
    matrix_sender: str | None
    matrix_room_id: str | None
    profile_id: str | None = None
    label: str | None = None
    source_profile_id: str | None = None
    target_profile_id: str | None = None
    session_id: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "correction_id": self.correction_id,
            "action": self.action,
            "created_at": self.created_at,
            "matrix_event_id": self.matrix_event_id,
            "matrix_sender": self.matrix_sender,
            "matrix_room_id": self.matrix_room_id,
            "profile_id": self.profile_id,
            "label": self.label,
            "source_profile_id": self.source_profile_id,
            "target_profile_id": self.target_profile_id,
            "session_id": self.session_id,
        }

    @classmethod
    def from_json_dict(cls, payload: Any) -> ProfileCorrectionEvent:
        if not isinstance(payload, dict):
            raise ArchiveSchemaError("correction payload must be an object")
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ArchiveSchemaError("unsupported correction schema_version")
        required = ("correction_id", "action", "created_at", "matrix_event_id", "matrix_sender", "matrix_room_id")
        missing = [field for field in required if field not in payload]
        if missing:
            raise ArchiveSchemaError(f"correction payload missing required fields: {', '.join(missing)}")
        action = _string(payload["action"], "action")
        if action not in CORRECTION_ACTIONS:
            raise ArchiveSchemaError("unsupported correction action")
        event = cls(
            schema_version=SCHEMA_VERSION,
            correction_id=_bounded_string(payload["correction_id"], "correction_id", max_length=80),
            action=action,
            created_at=_bounded_string(payload["created_at"], "created_at", max_length=80),
            matrix_event_id=_optional_bounded_string(payload["matrix_event_id"], "matrix_event_id", max_length=160),
            matrix_sender=_optional_bounded_string(payload["matrix_sender"], "matrix_sender", max_length=160),
            matrix_room_id=_optional_bounded_string(payload["matrix_room_id"], "matrix_room_id", max_length=160),
            profile_id=_optional_profile_id(payload.get("profile_id"), "profile_id"),
            label=_optional_bounded_string(payload.get("label"), "label", max_length=MAX_CORRECTION_TEXT_LENGTH),
            source_profile_id=_optional_profile_id(payload.get("source_profile_id"), "source_profile_id"),
            target_profile_id=_optional_profile_id(payload.get("target_profile_id"), "target_profile_id"),
            session_id=_optional_bounded_string(payload.get("session_id"), "session_id", max_length=220),
        )
        event._validate_action_fields()
        _validate_json_safe(event.to_json_dict(), "correction")
        return event

    def _validate_action_fields(self) -> None:
        if self.action == CORRECTION_ACTION_RENAME_PROFILE:
            if self.profile_id is None or self.label is None:
                raise ArchiveSchemaError("rename_profile correction requires profile_id and label")
            if not self.label.strip():
                raise ArchiveSchemaError("rename_profile label cannot be blank")
        elif self.action == CORRECTION_ACTION_MERGE_PROFILES:
            if self.source_profile_id is None or self.target_profile_id is None:
                raise ArchiveSchemaError("merge_profiles correction requires source_profile_id and target_profile_id")
            if self.source_profile_id == self.target_profile_id:
                raise ArchiveSchemaError("merge_profiles source and target must differ")
        elif self.action == CORRECTION_ACTION_WRONG_MATCH:
            if self.session_id is None:
                raise ArchiveSchemaError("wrong_match correction requires session_id")
        elif self.action == CORRECTION_ACTION_PROFILE_SUMMARY_REQUESTED and self.profile_id is None:
            raise ArchiveSchemaError("profile_summary_requested correction requires profile_id")


@dataclass(frozen=True)
class CorrectionReplayState:
    labels: Mapping[str, str]
    merges: Mapping[str, str]
    wrong_match_session_ids: frozenset[str]
    valid_count: int
    invalid_count: int
    quarantine_count: int
    last_action: str | None
    last_created_at: str | None


@dataclass(frozen=True)
class ProfileAssignment:
    """Result of applying the local visual profile registry to one session."""

    session_id: str
    status: str
    profile_id: str | None
    profile_confidence: float | None
    reason: str


@dataclass(frozen=True)
class StoredVehicleProfile:
    """Durable JSON contract for one active visual vehicle profile."""

    schema_version: int
    profile_id: str
    label: str | None
    status: str
    descriptor: VehicleDescriptor
    sample_count: int
    sample_session_ids: tuple[str, ...]
    exemplar_crop_path: str | None
    created_at: str
    updated_at: str

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "profile_id": self.profile_id,
            "label": self.label,
            "status": self.status,
            "descriptor": _descriptor_to_json(self.descriptor),
            "sample_count": self.sample_count,
            "sample_session_ids": list(self.sample_session_ids),
            "exemplar_crop_path": self.exemplar_crop_path,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_json_dict(cls, payload: Any) -> StoredVehicleProfile:
        if not isinstance(payload, dict):
            raise ArchiveSchemaError("profile payload must be an object")
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ArchiveSchemaError("unsupported profile schema_version")
        required = (
            "profile_id",
            "label",
            "status",
            "descriptor",
            "sample_count",
            "sample_session_ids",
            "exemplar_crop_path",
            "created_at",
            "updated_at",
        )
        missing = [field for field in required if field not in payload]
        if missing:
            raise ArchiveSchemaError(f"profile payload missing required fields: {', '.join(missing)}")
        status = _string(payload["status"], "status")
        if status != PROFILE_STATUS_ACTIVE:
            raise ArchiveSchemaError("profile status must be active")
        sample_count = _positive_int(payload["sample_count"], "sample_count")
        sample_session_ids = _string_tuple(payload["sample_session_ids"], "sample_session_ids")
        if len(sample_session_ids) > sample_count:
            raise ArchiveSchemaError("profile sample_session_ids cannot exceed sample_count")
        profile_id = _string(payload["profile_id"], "profile_id")
        if not profile_id.startswith("prof_"):
            raise ArchiveSchemaError("profile_id must start with prof_")
        record = cls(
            schema_version=SCHEMA_VERSION,
            profile_id=profile_id,
            label=_optional_string(payload["label"], "label"),
            status=status,
            descriptor=_descriptor_from_json(payload["descriptor"]),
            sample_count=sample_count,
            sample_session_ids=sample_session_ids,
            exemplar_crop_path=_optional_string(payload["exemplar_crop_path"], "exemplar_crop_path"),
            created_at=_string(payload["created_at"], "created_at"),
            updated_at=_string(payload["updated_at"], "updated_at"),
        )
        _validate_json_safe(record.to_json_dict(), "profile")
        return record

    def as_match_record(self) -> VehicleProfileRecord:
        return VehicleProfileRecord(
            profile_id=self.profile_id,
            descriptor=self.descriptor,
            sample_count=self.sample_count,
            quarantined=False,
        )


class ArchiveSchemaError(ValueError):
    """Raised when a vehicle-history record or event violates the supported schema."""


class ArchiveWriteError(RuntimeError):
    """Raised when the archive cannot safely persist a session record."""


@dataclass(frozen=True)
class VehicleHistoryExportResult:
    """Metadata-only summary of an operator-owned archive export bundle."""

    operation: str
    status: str
    started_at: str
    completed_at: str
    output_path: str
    manifest_path: str
    retention_policy: str
    archive_schema_version: int
    bundle_format: str
    member_count: int
    archive_file_count: int
    archive_bytes: int
    export_bytes: int
    session_count: int
    active_session_count: int
    closed_session_count: int
    profile_count: int
    correction_count: int
    member_names: tuple[str, ...]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "output_path": self.output_path,
            "manifest_path": self.manifest_path,
            "retention_policy": self.retention_policy,
            "archive_schema_version": self.archive_schema_version,
            "bundle_format": self.bundle_format,
            "member_count": self.member_count,
            "archive_file_count": self.archive_file_count,
            "archive_bytes": self.archive_bytes,
            "export_bytes": self.export_bytes,
            "session_count": self.session_count,
            "active_session_count": self.active_session_count,
            "closed_session_count": self.closed_session_count,
            "profile_count": self.profile_count,
            "correction_count": self.correction_count,
            "member_names": list(self.member_names),
        }


@dataclass(frozen=True)
class VehicleHistoryPruneResult:
    """Metadata-only summary of an explicit closed-session prune pass."""

    operation: str
    status: str
    started_at: str
    completed_at: str
    dry_run: bool
    cutoff: str
    retention_policy: str
    archive_schema_version: int
    candidate_session_count: int
    pruned_session_count: int
    pruned_file_count: int
    pruned_bytes: int
    missing_file_count: int
    skipped_active_session_count: int
    skipped_retained_image_count: int
    retained_session_count: int
    manifest_path: str

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "dry_run": self.dry_run,
            "cutoff": self.cutoff,
            "retention_policy": self.retention_policy,
            "archive_schema_version": self.archive_schema_version,
            "candidate_session_count": self.candidate_session_count,
            "pruned_session_count": self.pruned_session_count,
            "pruned_file_count": self.pruned_file_count,
            "pruned_bytes": self.pruned_bytes,
            "missing_file_count": self.missing_file_count,
            "skipped_active_session_count": self.skipped_active_session_count,
            "skipped_retained_image_count": self.skipped_retained_image_count,
            "retained_session_count": self.retained_session_count,
            "manifest_path": self.manifest_path,
        }


@dataclass(frozen=True)
class SessionRecord:
    """Durable JSON contract for one confirmed vehicle occupancy session.

    S01 owns the lifecycle shell only: confirmed occupied transitions create an
    active record, open/empty transitions close it, and downstream slices may
    later fill optional image/profile fields without changing the archive
    layout. Optional fields are serialized as explicit ``null`` values until
    populated so older records remain schema-compatible as slices add data.
    """

    schema_version: int
    session_id: str
    spot_id: str
    started_at: str
    ended_at: str | None
    duration_seconds: int | None
    start_event: dict[str, Any]
    close_event: dict[str, Any] | None
    source_snapshot_path: str | None
    candidate_summary: dict[str, Any] | None
    occupied_snapshot_path: str | None
    occupied_crop_path: str | None
    profile_id: str | None
    profile_confidence: float | None
    created_at: str
    updated_at: str

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "spot_id": self.spot_id,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_seconds": self.duration_seconds,
            "start_event": self.start_event,
            "close_event": self.close_event,
            "source_snapshot_path": self.source_snapshot_path,
            "candidate_summary": self.candidate_summary,
            "occupied_snapshot_path": self.occupied_snapshot_path,
            "occupied_crop_path": self.occupied_crop_path,
            "profile_id": self.profile_id,
            "profile_confidence": self.profile_confidence,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_json_dict(cls, payload: Any) -> SessionRecord:
        if not isinstance(payload, dict):
            raise ArchiveSchemaError("session payload must be an object")
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ArchiveSchemaError("unsupported session schema_version")
        required = (
            "session_id",
            "spot_id",
            "started_at",
            "ended_at",
            "duration_seconds",
            "start_event",
            "close_event",
            "source_snapshot_path",
            "candidate_summary",
            "occupied_snapshot_path",
            "occupied_crop_path",
            "profile_id",
            "profile_confidence",
            "created_at",
            "updated_at",
        )
        missing = [field for field in required if field not in payload]
        if missing:
            raise ArchiveSchemaError(f"session payload missing required fields: {', '.join(missing)}")

        record = cls(
            schema_version=SCHEMA_VERSION,
            session_id=_string(payload["session_id"], "session_id"),
            spot_id=_string(payload["spot_id"], "spot_id"),
            started_at=_string(payload["started_at"], "started_at"),
            ended_at=_optional_string(payload["ended_at"], "ended_at"),
            duration_seconds=_optional_non_negative_int(payload["duration_seconds"], "duration_seconds"),
            start_event=_dict(payload["start_event"], "start_event"),
            close_event=_optional_dict(payload["close_event"], "close_event"),
            source_snapshot_path=_optional_string(payload["source_snapshot_path"], "source_snapshot_path"),
            candidate_summary=_optional_dict(payload["candidate_summary"], "candidate_summary"),
            occupied_snapshot_path=_optional_string(payload["occupied_snapshot_path"], "occupied_snapshot_path"),
            occupied_crop_path=_optional_string(payload["occupied_crop_path"], "occupied_crop_path"),
            profile_id=_optional_string(payload["profile_id"], "profile_id"),
            profile_confidence=_optional_finite_float(payload["profile_confidence"], "profile_confidence"),
            created_at=_string(payload["created_at"], "created_at"),
            updated_at=_string(payload["updated_at"], "updated_at"),
        )
        _validate_json_safe(record.to_json_dict(), "session")
        return record


class VehicleHistoryArchive:
    """File-backed vehicle session archive separate from runtime state.json.

    Records live under ``vehicle-history/sessions/active`` while occupied and
    move to ``vehicle-history/sessions/closed`` when an open/empty event closes
    the session; malformed records are moved to ``sessions/quarantine``. Archive
    failures are non-blocking to the runtime loop but remain observable through
    safe structured logs plus ``vehicle_history_failure_count`` and
    ``last_vehicle_history_error`` health fields.
    """

    def __init__(self, root: str | os.PathLike[str], logger: StructuredLogger | None = None) -> None:
        root_path = Path(root)
        self.root = root_path if root_path.name == "vehicle-history" else root_path / "vehicle-history"
        self.sessions_dir = self.root / "sessions"
        self.active_dir = self.sessions_dir / "active"
        self.closed_dir = self.sessions_dir / "closed"
        self.quarantine_dir = self.sessions_dir / "quarantine"
        self.profiles_dir = self.root / "profiles"
        self.active_profiles_dir = self.profiles_dir / "active"
        self.profile_quarantine_dir = self.profiles_dir / "quarantine"
        self.corrections_dir = self.root / "corrections"
        self.corrections_path = self.corrections_dir / "events.jsonl"
        self.corrections_quarantine_path = self.corrections_dir / "quarantine.jsonl"
        self.matrix_state_path = self.corrections_dir / "matrix-state.json"
        self.logger = logger
        self._failure_count = 0
        self._last_error: dict[str, Any] | None = None

    def start_session(self, event: OccupancyEvent) -> SessionRecord:
        _validate_start_event(event)
        for record in self.load_active_sessions():
            if record.spot_id == event.spot_id:
                self._log(
                    "warning",
                    "vehicle-session-start-noop",
                    spot_id=event.spot_id,
                    session_id=record.session_id,
                    reason="active-session-exists",
                )
                return record

        now = _utc_now()
        session_id = _session_id(event.spot_id, event.observed_at)
        record = SessionRecord(
            schema_version=SCHEMA_VERSION,
            session_id=session_id,
            spot_id=event.spot_id,
            started_at=_event_time(event),
            ended_at=None,
            duration_seconds=None,
            start_event=_event_payload(event),
            close_event=None,
            source_snapshot_path=_optional_event_snapshot(event),
            candidate_summary=dict(event.candidate_summary) if event.candidate_summary is not None else None,
            occupied_snapshot_path=None,
            occupied_crop_path=None,
            profile_id=None,
            profile_confidence=None,
            created_at=now,
            updated_at=now,
        )
        self._write_record(self.active_dir / f"{record.session_id}.json", record, phase="start")
        self._log("info", "vehicle-session-started", spot_id=record.spot_id, session_id=record.session_id)
        return record

    def close_session(self, event: OccupancyEvent) -> SessionRecord | None:
        _validate_close_event(event)
        active_records = self.load_active_sessions()
        record = next((item for item in active_records if item.spot_id == event.spot_id), None)
        if record is None:
            self._log("warning", "vehicle-session-close-noop", spot_id=event.spot_id, reason="active-session-missing")
            return None

        ended_at = _event_time(event)
        closed = SessionRecord(
            schema_version=SCHEMA_VERSION,
            session_id=record.session_id,
            spot_id=record.spot_id,
            started_at=record.started_at,
            ended_at=ended_at,
            duration_seconds=_duration_seconds(record.started_at, ended_at),
            start_event=record.start_event,
            close_event=_event_payload(event),
            source_snapshot_path=record.source_snapshot_path,
            candidate_summary=record.candidate_summary,
            occupied_snapshot_path=record.occupied_snapshot_path,
            occupied_crop_path=record.occupied_crop_path,
            profile_id=record.profile_id,
            profile_confidence=record.profile_confidence,
            created_at=record.created_at,
            updated_at=_utc_now(),
        )
        closed_path = self.closed_dir / f"{closed.session_id}.json"
        self._write_record(closed_path, closed, phase="close")
        try:
            (self.active_dir / f"{record.session_id}.json").unlink(missing_ok=True)
        except OSError as exc:
            self._record_failure(phase="active-unlink", path_name=f"{record.session_id}.json", error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc
        self._log("info", "vehicle-session-closed", spot_id=closed.spot_id, session_id=closed.session_id)
        return closed


    def attach_occupied_images(
        self,
        *,
        session_id: str,
        source_frame_path: str | os.PathLike[str],
        bbox: Sequence[float],
    ) -> SessionRecord:
        """Attach archive-owned occupied JPEG artifacts to an active session.

        Images are written under ``vehicle-history/images`` and referenced from
        the session JSON only; Matrix alert uploads continue to use their own
        delivery-time retention path and are not coupled to these artifacts.
        """
        active_path = self.active_dir / f"{session_id}.json"
        if not active_path.exists():
            error = ArchiveSchemaError("active session is missing")
            self._record_failure(phase="image-attach", path_name=active_path.name, error=error, session_id=session_id)
            raise ArchiveWriteError(_safe_error_message(error)) from error
        record = self._load_record(active_path)
        if record is None or record.session_id != session_id:
            error = ArchiveSchemaError("active session is missing")
            self._record_failure(phase="image-attach", path_name=active_path.name, error=error, session_id=session_id)
            raise ArchiveWriteError(_safe_error_message(error)) from error

        if record.occupied_snapshot_path is not None and record.occupied_crop_path is not None:
            self._log(
                "info",
                "vehicle-session-images-noop",
                spot_id=record.spot_id,
                session_id=record.session_id,
                full_path_name=Path(record.occupied_snapshot_path).name,
                crop_path_name=Path(record.occupied_crop_path).name,
                reason="already-attached",
            )
            return record

        try:
            captured = capture_occupied_images(
                archive_root=self.root,
                session_id=record.session_id,
                source_frame_path=source_frame_path,
                bbox=bbox,
            )
        except VehicleHistoryImageError as exc:
            self._record_failure(phase="image-capture", path_name=active_path.name, error=exc, session_id=record.session_id)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc

        updated = SessionRecord(
            schema_version=SCHEMA_VERSION,
            session_id=record.session_id,
            spot_id=record.spot_id,
            started_at=record.started_at,
            ended_at=record.ended_at,
            duration_seconds=record.duration_seconds,
            start_event=record.start_event,
            close_event=record.close_event,
            source_snapshot_path=record.source_snapshot_path,
            candidate_summary=record.candidate_summary,
            occupied_snapshot_path=str(captured.full_frame_path),
            occupied_crop_path=str(captured.crop_path),
            profile_id=record.profile_id,
            profile_confidence=record.profile_confidence,
            created_at=record.created_at,
            updated_at=_utc_now(),
        )
        self._write_record(active_path, updated, phase="image-attach")
        self._log(
            "info",
            "vehicle-session-images-captured",
            spot_id=updated.spot_id,
            session_id=updated.session_id,
            full_path_name=captured.full_frame_path.name,
            crop_path_name=captured.crop_path.name,
        )
        return updated

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
            result = match_vehicle_profile(descriptor, [profile.as_match_record() for profile in profiles])
        except (VehicleProfileDescriptorError, ValueError, OSError) as exc:
            self._record_failure(phase="profile-match", path_name=Path(record.occupied_crop_path).name, error=exc, session_id=record.session_id)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc

        if result.status is MatchStatus.MATCHED and result.profile_id is not None:
            matched = next(profile for profile in profiles if profile.profile_id == result.profile_id)
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

    def load_active_sessions(self) -> list[SessionRecord]:
        return self._load_records(self.active_dir)

    def list_closed_sessions(self) -> list[SessionRecord]:
        return self._load_records(self.closed_dir)

    def export_archive(self, output_path: str | os.PathLike[str]) -> VehicleHistoryExportResult:
        """Create an operator-owned tar.gz bundle and safe maintenance manifest."""

        started_at = _utc_now()
        output = Path(output_path)
        if output.exists() and output.is_dir():
            error = ArchiveWriteError("export output must be a file path")
            self._record_failure(phase="maintenance-export", path_name=output.name, error=error)
            raise error
        output.parent.mkdir(parents=True, exist_ok=True)
        self.root.mkdir(parents=True, exist_ok=True)

        active_records = self.load_active_sessions()
        closed_records = self.list_closed_sessions()
        profiles = self.load_active_profiles()
        corrections = self.load_corrections()
        archive_files = _archive_files_for_export(self.root, output)
        archive_file_count = len(archive_files)
        archive_bytes = sum(_safe_file_size(path) for path in archive_files)
        completed_at = _utc_now()
        manifest_name = f"export-{_maintenance_stamp(completed_at)}.json"
        manifest_rel = f"vehicle-history/metadata/maintenance/{manifest_name}"
        member_names = tuple([_archive_member_name(self.root, path) for path in archive_files] + [manifest_rel])
        manifest = {
            "operation": "export",
            "status": "ok",
            "started_at": started_at,
            "completed_at": completed_at,
            "retention_policy": "indefinite",
            "archive_schema_version": SCHEMA_VERSION,
            "bundle_format": "tar.gz",
            "member_count": len(member_names),
            "archive_file_count": archive_file_count,
            "archive_bytes": archive_bytes,
            "session_count": len(active_records) + len(closed_records),
            "active_session_count": len(active_records),
            "closed_session_count": len(closed_records),
            "profile_count": len(profiles),
            "correction_count": len(corrections),
            "member_names": list(member_names),
        }
        _validate_json_safe(manifest, "export manifest")

        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile("wb", delete=False, dir=output.parent, prefix=f".{output.name}.", suffix=".tmp") as handle:
                temp_path = Path(handle.name)
            with tarfile.open(temp_path, "w:gz") as bundle:
                for path in archive_files:
                    bundle.add(path, arcname=_archive_member_name(self.root, path), recursive=False)
                manifest_bytes = (json.dumps(manifest, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
                info = tarfile.TarInfo(manifest_rel)
                info.size = len(manifest_bytes)
                info.mtime = int(datetime.now(timezone.utc).timestamp())
                info.mode = 0o644
                import io

                bundle.addfile(info, io.BytesIO(manifest_bytes))
            os.chmod(temp_path, 0o644)
            os.replace(temp_path, output)
        except Exception as exc:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            self._record_failure(phase="maintenance-export", path_name=output.name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc

        export_bytes = _safe_file_size(output)
        manifest["export_bytes"] = export_bytes
        persisted_manifest_path = self._write_maintenance_manifest(manifest_name, manifest, phase="maintenance-export")
        result = VehicleHistoryExportResult(
            operation="export",
            status="ok",
            started_at=started_at,
            completed_at=completed_at,
            output_path=str(output),
            manifest_path=str(persisted_manifest_path),
            retention_policy="indefinite",
            archive_schema_version=SCHEMA_VERSION,
            bundle_format="tar.gz",
            member_count=len(member_names),
            archive_file_count=archive_file_count,
            archive_bytes=archive_bytes,
            export_bytes=export_bytes,
            session_count=len(active_records) + len(closed_records),
            active_session_count=len(active_records),
            closed_session_count=len(closed_records),
            profile_count=len(profiles),
            correction_count=len(corrections),
            member_names=member_names,
        )
        self._log("info", "vehicle-history-exported", **_maintenance_log_fields(result.to_json_dict()))
        return result

    def prune_closed_sessions(
        self,
        *,
        older_than: str | datetime,
        dry_run: bool = True,
    ) -> VehicleHistoryPruneResult:
        """Prune closed sessions older than a cutoff while preserving active references."""

        cutoff = _coerce_cutoff_datetime(older_than)
        cutoff_text = cutoff.isoformat().replace("+00:00", "Z")
        started_at = _utc_now()
        active_records = self.load_active_sessions()
        closed_records = self.list_closed_sessions()
        candidates = [record for record in closed_records if _record_closed_before(record, cutoff)]
        candidate_ids = {record.session_id for record in candidates}
        retained_records = [record for record in [*active_records, *closed_records] if record.session_id not in candidate_ids]
        retained_refs = _referenced_archive_paths(self.root, retained_records)

        session_paths = [self.closed_dir / f"{record.session_id}.json" for record in candidates]
        image_paths: list[Path] = []
        missing_file_count = 0
        skipped_retained_image_count = 0
        for record in candidates:
            for image_path in _record_archive_image_paths(self.root, record):
                if image_path in retained_refs:
                    skipped_retained_image_count += 1
                    continue
                if image_path not in image_paths:
                    image_paths.append(image_path)
        prune_paths = [*session_paths, *image_paths]
        existing_paths: list[Path] = []
        pruned_bytes = 0
        for path in prune_paths:
            try:
                stat_result = path.stat()
            except FileNotFoundError:
                missing_file_count += 1
                continue
            except OSError:
                missing_file_count += 1
                continue
            if path.is_file():
                existing_paths.append(path)
                pruned_bytes += stat_result.st_size

        if not dry_run:
            for path in existing_paths:
                try:
                    path.unlink(missing_ok=True)
                except OSError as exc:
                    self._record_failure(phase="maintenance-prune", path_name=path.name, error=exc)
                    raise ArchiveWriteError(_safe_error_message(exc)) from exc

        completed_at = _utc_now()
        status = "dry_run" if dry_run else "ok"
        manifest_name = f"prune-{_maintenance_stamp(completed_at)}.json"
        manifest = {
            "operation": "prune",
            "status": status,
            "started_at": started_at,
            "completed_at": completed_at,
            "dry_run": dry_run,
            "cutoff": cutoff_text,
            "retention_policy": "indefinite",
            "archive_schema_version": SCHEMA_VERSION,
            "candidate_session_count": len(candidates),
            "pruned_session_count": len(candidates),
            "pruned_file_count": len(existing_paths),
            "pruned_bytes": pruned_bytes,
            "missing_file_count": missing_file_count,
            "skipped_active_session_count": len(active_records),
            "skipped_retained_image_count": skipped_retained_image_count,
            "retained_session_count": len(retained_records),
        }
        manifest_path = self._write_maintenance_manifest(manifest_name, manifest, phase="maintenance-prune")
        result = VehicleHistoryPruneResult(
            operation="prune",
            status=status,
            started_at=started_at,
            completed_at=completed_at,
            dry_run=dry_run,
            cutoff=cutoff_text,
            retention_policy="indefinite",
            archive_schema_version=SCHEMA_VERSION,
            candidate_session_count=len(candidates),
            pruned_session_count=len(candidates),
            pruned_file_count=len(existing_paths),
            pruned_bytes=pruned_bytes,
            missing_file_count=missing_file_count,
            skipped_active_session_count=len(active_records),
            skipped_retained_image_count=skipped_retained_image_count,
            retained_session_count=len(retained_records),
            manifest_path=str(manifest_path),
        )
        self._log("info", "vehicle-history-pruned", **_maintenance_log_fields(result.to_json_dict()))
        return result

    def _write_maintenance_manifest(self, name: str, payload: Mapping[str, Any], *, phase: str) -> Path:
        directory = self.root / "metadata" / "maintenance"
        path = directory / name
        directory.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=directory, prefix=f".{name}.", suffix=".tmp") as handle:
                temp_path = Path(handle.name)
                json.dump(dict(payload), handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_path, 0o644)
            os.replace(temp_path, path)
        except Exception as exc:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            self._record_failure(phase=phase, path_name=name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc
        return path

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

    def health_snapshot(self) -> dict[str, Any]:
        active_records = self.load_active_sessions()
        closed_records = self.list_closed_sessions()
        profiles = self.load_active_profiles()
        full_stats = self._image_directory_stats(self.root / "images" / "occupied-full", phase="image-scan")
        crop_stats = self._image_directory_stats(self.root / "images" / "occupied-crops", phase="image-scan")
        missing_refs = _missing_occupied_image_reference_count([*active_records, *closed_records])
        all_records = [*active_records, *closed_records]
        correction_state = self.correction_replay_state()
        archive_stats = self._archive_directory_stats()
        maintenance_metadata = self._last_maintenance_metadata()
        return {
            "active_session_count": len(active_records),
            "closed_session_count": len(closed_records),
            "retention_policy": "indefinite",
            "management_capabilities": ["export", "prune"],
            "oldest_retained_session_started_at": _oldest_retained_session_started_at([*active_records, *closed_records]),
            "archive_file_count": archive_stats[0],
            "archive_bytes": archive_stats[1],
            "last_maintenance_metadata": maintenance_metadata,
            "occupied_snapshot_count": full_stats[0],
            "occupied_crop_count": crop_stats[0],
            "image_file_count": full_stats[0] + crop_stats[0],
            "image_bytes": full_stats[1] + crop_stats[1],
            "missing_occupied_image_reference_count": missing_refs,
            "profile_count": len(profiles),
            "profile_sample_count": sum(profile.sample_count for profile in profiles),
            "profile_unknown_session_count": sum(1 for record in all_records if record.occupied_crop_path is not None and record.profile_id is None),
            "profile_quarantine_count": _profile_quarantine_count(self.profile_quarantine_dir),
            "correction_count": correction_state.valid_count,
            "correction_invalid_count": correction_state.invalid_count,
            "correction_quarantine_count": correction_state.quarantine_count,
            "last_correction_action": correction_state.last_action,
            "last_correction_created_at": correction_state.last_created_at,
            "matrix_command_cursor_present": self.matrix_state_path.exists(),
            "vehicle_history_failure_count": self._failure_count,
            "last_vehicle_history_error": dict(self._last_error) if self._last_error is not None else None,
        }

    summarize = health_snapshot

    def _image_directory_stats(self, directory: Path, *, phase: str) -> tuple[int, int]:
        try:
            return _image_directory_stats(directory)
        except OSError as exc:
            self._record_failure(phase=phase, path_name=directory.name, error=exc)
            return (0, 0)

    def _archive_directory_stats(self) -> tuple[int, int]:
        try:
            return _archive_directory_stats(self.root)
        except OSError as exc:
            self._record_failure(phase="archive-scan", path_name=self.root.name, error=exc)
            return (0, 0)

    def _last_maintenance_metadata(self) -> dict[str, Any] | None:
        directory = self.root / "metadata" / "maintenance"
        try:
            candidates = [path for path in directory.glob("*.json") if path.is_file()]
        except OSError as exc:
            self._record_failure(phase="maintenance-scan", path_name=directory.name, error=exc)
            return None
        if not candidates:
            return None
        try:
            latest = max(candidates, key=lambda path: path.stat().st_mtime)
        except OSError as exc:
            self._record_failure(phase="maintenance-scan", path_name=directory.name, error=exc)
            return None
        try:
            if latest.stat().st_size > MAX_PROFILE_FILE_BYTES:
                raise ArchiveSchemaError(f"maintenance metadata exceeds maximum size of {MAX_PROFILE_FILE_BYTES} bytes")
            with latest.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError, ArchiveSchemaError) as exc:
            self._record_failure(phase="maintenance-load", path_name=latest.name, error=exc)
            return {"manifest_name": latest.name, "status": "unreadable"}
        if not isinstance(payload, Mapping):
            self._record_failure(
                phase="maintenance-load",
                path_name=latest.name,
                error=ArchiveSchemaError("maintenance metadata must be an object"),
            )
            return {"manifest_name": latest.name, "status": "invalid"}
        metadata = _safe_maintenance_metadata(payload)
        metadata["manifest_name"] = latest.name
        return metadata

    def _load_records(self, directory: Path) -> list[SessionRecord]:
        directory.mkdir(parents=True, exist_ok=True)
        records: list[SessionRecord] = []
        for path in sorted(directory.glob("*.json")):
            record = self._load_record(path)
            if record is not None:
                records.append(record)
        self._log(
            "info",
            "vehicle-archive-loaded",
            archive_state=directory.name,
            session_count=len(records),
        )
        return records

    def _load_record(self, path: Path) -> SessionRecord | None:
        try:
            size = path.stat().st_size
        except OSError as exc:
            self._quarantine(path, phase="stat", error=exc)
            return None
        if size > MAX_SESSION_FILE_BYTES:
            self._quarantine(
                path,
                phase="size-validate",
                error=ArchiveSchemaError(f"session file exceeds maximum size of {MAX_SESSION_FILE_BYTES} bytes"),
            )
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            self._quarantine(path, phase="json-load", error=exc)
            return None
        try:
            return SessionRecord.from_json_dict(payload)
        except ArchiveSchemaError as exc:
            self._quarantine(path, phase="schema-validate", error=exc)
            return None

    def _load_profile(self, path: Path) -> StoredVehicleProfile | None:
        try:
            size = path.stat().st_size
        except OSError as exc:
            self._quarantine_profile(path, phase="profile-load", error=exc)
            return None
        if size > MAX_PROFILE_FILE_BYTES:
            self._quarantine_profile(
                path,
                phase="profile-load",
                error=ArchiveSchemaError(f"profile file exceeds maximum size of {MAX_PROFILE_FILE_BYTES} bytes"),
            )
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            self._quarantine_profile(path, phase="profile-load", error=exc)
            return None
        try:
            return StoredVehicleProfile.from_json_dict(payload)
        except (ArchiveSchemaError, ValueError) as exc:
            self._quarantine_profile(path, phase="profile-scan", error=exc)
            return None

    def _write_profile(self, path: Path, profile: StoredVehicleProfile, *, phase: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                delete=False,
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
            ) as handle:
                temp_path = Path(handle.name)
                json.dump(profile.to_json_dict(), handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_path, 0o644)
            os.replace(temp_path, path)
        except Exception as exc:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            self._record_failure(phase=phase, path_name=path.name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc

    def _write_record(self, path: Path, record: SessionRecord, *, phase: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                delete=False,
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
            ) as handle:
                temp_path = Path(handle.name)
                json.dump(record.to_json_dict(), handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_path, 0o644)
            os.replace(temp_path, path)
        except Exception as exc:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            self._record_failure(phase=phase, path_name=path.name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc

    def _quarantine(self, path: Path, *, phase: str, error: BaseException) -> None:
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        candidate = self.quarantine_dir / f"{path.name}.corrupt-{timestamp}"
        index = 1
        while candidate.exists():
            candidate = self.quarantine_dir / f"{path.name}.corrupt-{timestamp}-{index}"
            index += 1
        try:
            os.replace(path, candidate)
        except OSError as exc:
            self._record_failure(phase="quarantine", path_name=path.name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc
        self._record_failure(phase=phase, path_name=path.name, error=error)
        self._log(
            "warning",
            "vehicle-session-quarantined",
            path_name=path.name,
            quarantine_name=candidate.name,
            phase=phase,
            error_type=type(error).__name__,
            error_message=_safe_error_message(error),
        )

    def _quarantine_profile(self, path: Path, *, phase: str, error: BaseException) -> None:
        self.profile_quarantine_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        candidate = self.profile_quarantine_dir / f"{path.name}.corrupt-{timestamp}"
        index = 1
        while candidate.exists():
            candidate = self.profile_quarantine_dir / f"{path.name}.corrupt-{timestamp}-{index}"
            index += 1
        try:
            os.replace(path, candidate)
        except OSError as exc:
            self._record_failure(phase="profile-quarantine", path_name=path.name, error=exc)
            raise ArchiveWriteError(_safe_error_message(exc)) from exc
        self._record_failure(phase=phase, path_name=path.name, error=error)
        self._log(
            "warning",
            "vehicle-profile-quarantined",
            path_name=path.name,
            quarantine_name=candidate.name,
            phase=phase,
            error_type=type(error).__name__,
            error_message=_safe_error_message(error),
        )

    def _record_failure(
        self,
        *,
        phase: str,
        path_name: str,
        error: BaseException,
        session_id: str | None = None,
    ) -> None:
        self._failure_count += 1
        self._last_error = {
            "phase": phase,
            "path_name": path_name,
            "error_type": type(error).__name__,
            "error_message": _safe_error_message(error),
        }
        if session_id is not None:
            self._last_error["session_id"] = session_id
        profile_phases = {"profile-load", "profile-scan", "profile-match", "profile-quarantine"}
        correction_phases = {
            "correction-append",
            "correction-load",
            "correction-quarantine",
            "correction-quarantine-count",
            "matrix-state-load",
            "matrix-state-write",
        }
        if phase in {
            "start",
            "close",
            "active-unlink",
            "quarantine",
            "image-capture",
            "image-attach",
            "image-scan",
            "archive-scan",
            "maintenance-scan",
            "maintenance-load",
            "maintenance-export",
            "maintenance-prune",
            *profile_phases,
            *correction_phases,
        }:
            if phase.startswith("image-"):
                event = "vehicle-session-images-failed"
            elif phase.startswith("profile-"):
                event = "vehicle-session-profile-failed"
            elif phase.startswith("correction-") or phase.startswith("matrix-state"):
                event = "vehicle-profile-correction-failed"
            elif phase.startswith("archive-") or phase.startswith("maintenance-"):
                event = "vehicle-archive-health-failed"
            else:
                event = "vehicle-session-write-failed"
            fields = {
                "path_name": path_name,
                "phase": phase,
                "error_type": type(error).__name__,
                "error_message": _safe_error_message(error),
            }
            if session_id is not None:
                fields["session_id"] = session_id
            self._log("error", event, **fields)

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        getattr(self.logger, level)(event, **fields)


def start_session(root: str | os.PathLike[str], event: OccupancyEvent, logger: StructuredLogger | None = None) -> SessionRecord:
    return VehicleHistoryArchive(root, logger=logger).start_session(event)


def close_session(root: str | os.PathLike[str], event: OccupancyEvent, logger: StructuredLogger | None = None) -> SessionRecord | None:
    return VehicleHistoryArchive(root, logger=logger).close_session(event)



def attach_occupied_images(
    root: str | os.PathLike[str],
    *,
    session_id: str,
    source_frame_path: str | os.PathLike[str],
    bbox: Sequence[float],
    logger: StructuredLogger | None = None,
) -> SessionRecord:
    return VehicleHistoryArchive(root, logger=logger).attach_occupied_images(
        session_id=session_id,
        source_frame_path=source_frame_path,
        bbox=bbox,
    )


def match_or_create_profile(
    root: str | os.PathLike[str],
    *,
    session_id: str,
    logger: StructuredLogger | None = None,
) -> ProfileAssignment:
    return VehicleHistoryArchive(root, logger=logger).match_or_create_profile(session_id=session_id)


def rename_profile(
    root: str | os.PathLike[str],
    *,
    profile_id: str,
    label: str,
    matrix_event_id: str | None = None,
    matrix_sender: str | None = None,
    matrix_room_id: str | None = None,
    logger: StructuredLogger | None = None,
) -> ProfileCorrectionEvent:
    return VehicleHistoryArchive(root, logger=logger).rename_profile(
        profile_id,
        label,
        matrix_event_id=matrix_event_id,
        matrix_sender=matrix_sender,
        matrix_room_id=matrix_room_id,
    )


def merge_profiles(
    root: str | os.PathLike[str],
    *,
    source_profile_id: str,
    target_profile_id: str,
    matrix_event_id: str | None = None,
    matrix_sender: str | None = None,
    matrix_room_id: str | None = None,
    logger: StructuredLogger | None = None,
) -> ProfileCorrectionEvent:
    return VehicleHistoryArchive(root, logger=logger).merge_profiles(
        source_profile_id,
        target_profile_id,
        matrix_event_id=matrix_event_id,
        matrix_sender=matrix_sender,
        matrix_room_id=matrix_room_id,
    )


def mark_wrong_match(
    root: str | os.PathLike[str],
    *,
    session_id: str,
    profile_id: str | None = None,
    matrix_event_id: str | None = None,
    matrix_sender: str | None = None,
    matrix_room_id: str | None = None,
    logger: StructuredLogger | None = None,
) -> ProfileCorrectionEvent:
    return VehicleHistoryArchive(root, logger=logger).mark_wrong_match(
        session_id,
        profile_id=profile_id,
        matrix_event_id=matrix_event_id,
        matrix_sender=matrix_sender,
        matrix_room_id=matrix_room_id,
    )


def profile_summary(
    root: str | os.PathLike[str],
    *,
    profile_id: str,
    matrix_event_id: str | None = None,
    matrix_sender: str | None = None,
    matrix_room_id: str | None = None,
    logger: StructuredLogger | None = None,
) -> dict[str, Any]:
    return VehicleHistoryArchive(root, logger=logger).profile_summary(
        profile_id, matrix_event_id=matrix_event_id, matrix_sender=matrix_sender, matrix_room_id=matrix_room_id
    )


def estimate_profile_history(
    root: str | os.PathLike[str],
    *,
    profile_id: str | None,
    logger: StructuredLogger | None = None,
    min_samples: int = 2,
    min_profile_confidence: float = 0.76,
) -> VehicleHistoryEstimate:
    return VehicleHistoryArchive(root, logger=logger).estimate_for_profile(
        profile_id,
        min_samples=min_samples,
        min_profile_confidence=min_profile_confidence,
    )


def estimate_session_history(
    root: str | os.PathLike[str],
    *,
    session_id: str,
    logger: StructuredLogger | None = None,
    min_samples: int = 2,
    min_profile_confidence: float = 0.76,
) -> VehicleHistoryEstimate:
    return VehicleHistoryArchive(root, logger=logger).estimate_for_session(
        session_id,
        min_samples=min_samples,
        min_profile_confidence=min_profile_confidence,
    )


def load_active_sessions(root: str | os.PathLike[str], logger: StructuredLogger | None = None) -> list[SessionRecord]:
    return VehicleHistoryArchive(root, logger=logger).load_active_sessions()


def list_closed_sessions(root: str | os.PathLike[str], logger: StructuredLogger | None = None) -> list[SessionRecord]:
    return VehicleHistoryArchive(root, logger=logger).list_closed_sessions()


def health_snapshot(root: str | os.PathLike[str], logger: StructuredLogger | None = None) -> dict[str, Any]:
    """Return archive counters, including occupied image and profile registry totals."""
    return VehicleHistoryArchive(root, logger=logger).health_snapshot()


def export_archive(
    root: str | os.PathLike[str],
    *,
    output_path: str | os.PathLike[str],
    logger: StructuredLogger | None = None,
) -> VehicleHistoryExportResult:
    return VehicleHistoryArchive(root, logger=logger).export_archive(output_path)


def prune_closed_sessions(
    root: str | os.PathLike[str],
    *,
    older_than: str | datetime,
    dry_run: bool = True,
    logger: StructuredLogger | None = None,
) -> VehicleHistoryPruneResult:
    return VehicleHistoryArchive(root, logger=logger).prune_closed_sessions(older_than=older_than, dry_run=dry_run)


def _session_with_profile(record: SessionRecord, *, profile_id: str, confidence: float) -> SessionRecord:
    return SessionRecord(
        schema_version=record.schema_version,
        session_id=record.session_id,
        spot_id=record.spot_id,
        started_at=record.started_at,
        ended_at=record.ended_at,
        duration_seconds=record.duration_seconds,
        start_event=record.start_event,
        close_event=record.close_event,
        source_snapshot_path=record.source_snapshot_path,
        candidate_summary=record.candidate_summary,
        occupied_snapshot_path=record.occupied_snapshot_path,
        occupied_crop_path=record.occupied_crop_path,
        profile_id=profile_id,
        profile_confidence=confidence,
        created_at=record.created_at,
        updated_at=_utc_now(),
    )


def _profile_with_sample(
    profile: StoredVehicleProfile,
    *,
    descriptor: VehicleDescriptor,
    session_id: str,
    crop_path: str,
) -> StoredVehicleProfile:
    if session_id in profile.sample_session_ids:
        return profile
    sample_count = profile.sample_count + 1
    return StoredVehicleProfile(
        schema_version=profile.schema_version,
        profile_id=profile.profile_id,
        label=profile.label,
        status=profile.status,
        descriptor=_blend_descriptor(profile.descriptor, descriptor, previous_count=profile.sample_count),
        sample_count=sample_count,
        sample_session_ids=(*profile.sample_session_ids, session_id)[-20:],
        exemplar_crop_path=profile.exemplar_crop_path or Path(crop_path).name,
        created_at=profile.created_at,
        updated_at=_utc_now(),
    )


def _blend_descriptor(previous: VehicleDescriptor, latest: VehicleDescriptor, *, previous_count: int) -> VehicleDescriptor:
    sample_count = max(1, previous_count)
    next_count = sample_count + 1
    histogram = tuple(((value * sample_count) + new_value) / next_count for value, new_value in zip(previous.rgb_histogram, latest.rgb_histogram, strict=True))
    return VehicleDescriptor(
        width=round(((previous.width * sample_count) + latest.width) / next_count),
        height=round(((previous.height * sample_count) + latest.height) / next_count),
        aspect_ratio=((previous.aspect_ratio * sample_count) + latest.aspect_ratio) / next_count,
        rgb_histogram=histogram,
        average_hash=latest.average_hash,
        hash_bits=latest.hash_bits,
    )


def _descriptor_to_json(descriptor: VehicleDescriptor) -> dict[str, Any]:
    return {
        "width": descriptor.width,
        "height": descriptor.height,
        "aspect_ratio": descriptor.aspect_ratio,
        "rgb_histogram": list(descriptor.rgb_histogram),
        "average_hash": descriptor.average_hash,
        "hash_bits": descriptor.hash_bits,
    }


def _descriptor_from_json(payload: Any) -> VehicleDescriptor:
    if not isinstance(payload, dict):
        raise ArchiveSchemaError("profile descriptor must be an object")
    required = ("width", "height", "aspect_ratio", "rgb_histogram", "average_hash", "hash_bits")
    missing = [field for field in required if field not in payload]
    if missing:
        raise ArchiveSchemaError(f"profile descriptor missing required fields: {', '.join(missing)}")
    histogram_value = payload["rgb_histogram"]
    if not isinstance(histogram_value, list):
        raise ArchiveSchemaError("profile descriptor rgb_histogram must be an array")
    descriptor = VehicleDescriptor(
        width=_positive_int(payload["width"], "descriptor.width"),
        height=_positive_int(payload["height"], "descriptor.height"),
        aspect_ratio=_finite_float(payload["aspect_ratio"], "descriptor.aspect_ratio"),
        rgb_histogram=tuple(_finite_float(value, "descriptor.rgb_histogram") for value in histogram_value),
        average_hash=_non_negative_int(payload["average_hash"], "descriptor.average_hash"),
        hash_bits=_positive_int(payload["hash_bits"], "descriptor.hash_bits"),
    )
    # Reuse the matcher's validation through a zero-distance self comparison.
    from parking_spot_monitor.vehicle_profiles import descriptor_distance

    descriptor_distance(descriptor, descriptor)
    return descriptor


def _profile_quarantine_count(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(1 for path in directory.glob("*.corrupt-*") if path.is_file())


def _archive_files_for_export(root: Path, output: Path) -> list[Path]:
    if not root.exists():
        return []
    resolved_output = _safe_resolve(output)
    files: list[Path] = []
    stack = [root]
    while stack:
        current = stack.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                path = Path(entry.path)
                if entry.is_dir(follow_symlinks=False):
                    stack.append(path)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                if _safe_resolve(path) == resolved_output:
                    continue
                files.append(path)
    return sorted(files, key=lambda path: _archive_member_name(root, path))


def _archive_member_name(root: Path, path: Path) -> str:
    return f"vehicle-history/{path.relative_to(root).as_posix()}"


def _safe_file_size(path: Path) -> int:
    try:
        return path.stat().st_size if path.is_file() else 0
    except OSError:
        return 0


def _maintenance_stamp(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "-", value).strip("-").lower() or "unknown"


def _coerce_cutoff_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = _parse_timestamp(value)
        if parsed is None:
            raise ArchiveSchemaError("cutoff must be an ISO timestamp")
    else:
        raise ArchiveSchemaError("cutoff must be an ISO timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def cutoff_older_than_days(days: int, *, now: datetime | None = None) -> datetime:
    if isinstance(days, bool) or not isinstance(days, int) or days < 0:
        raise ArchiveSchemaError("older-than-days must be a non-negative integer")
    reference = now if now is not None else datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return reference.astimezone(timezone.utc) - timedelta(days=days)


def _record_closed_before(record: SessionRecord, cutoff: datetime) -> bool:
    if record.ended_at is None:
        return False
    ended_at = _parse_timestamp(record.ended_at)
    if ended_at is None:
        return False
    return ended_at.astimezone(timezone.utc) < cutoff


def _referenced_archive_paths(root: Path, records: Sequence[SessionRecord]) -> set[Path]:
    paths: set[Path] = set()
    for record in records:
        paths.update(_record_archive_image_paths(root, record))
    return paths


def _record_archive_image_paths(root: Path, record: SessionRecord) -> set[Path]:
    paths: set[Path] = set()
    for value in (record.occupied_snapshot_path, record.occupied_crop_path):
        path = _archive_local_path(root, value)
        if path is not None:
            paths.add(path)
    return paths


def _archive_local_path(root: Path, value: str | None) -> Path | None:
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    resolved_root = _safe_resolve(root)
    resolved_path = _safe_resolve(path)
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError:
        return None
    return resolved_path


def _safe_resolve(path: Path) -> Path:
    try:
        return path.resolve(strict=False)
    except OSError:
        return path.absolute()


def _maintenance_log_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    blocked = {"member_names", "output_path", "manifest_path"}
    safe: dict[str, Any] = {}
    for key, value in payload.items():
        if key in blocked:
            continue
        safe[key] = _json_scalar_or_collection(redact_diagnostic_value(value))
    return safe


def _archive_directory_stats(directory: Path) -> tuple[int, int]:
    if not directory.exists():
        return (0, 0)
    count = 0
    total_bytes = 0
    stack = [directory]
    while stack:
        current = stack.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                    continue
                if entry.is_file(follow_symlinks=False):
                    stat_result = entry.stat(follow_symlinks=False)
                    count += 1
                    total_bytes += stat_result.st_size
    return (count, total_bytes)


def _oldest_retained_session_started_at(records: Sequence[SessionRecord]) -> str | None:
    oldest_record: SessionRecord | None = None
    oldest_timestamp: datetime | None = None
    for record in records:
        parsed = _parse_timestamp(record.started_at)
        if parsed is None:
            continue
        if oldest_timestamp is None or parsed < oldest_timestamp:
            oldest_timestamp = parsed
            oldest_record = record
    return None if oldest_record is None else oldest_record.started_at


def _safe_maintenance_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    allowed_keys = {
        "operation",
        "action",
        "status",
        "result",
        "started_at",
        "completed_at",
        "created_at",
        "updated_at",
        "retention_policy",
        "archive_file_count",
        "archive_bytes",
        "file_count",
        "bytes",
        "pruned_file_count",
        "export_file_count",
        "dry_run",
    }
    safe: dict[str, Any] = {}
    for key in allowed_keys:
        if key in payload:
            safe[key] = _json_scalar_or_collection(redact_diagnostic_value(payload[key]))
    return safe


def _json_scalar_or_collection(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value if not isinstance(value, float) or math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _json_scalar_or_collection(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_scalar_or_collection(item) for item in value]
    return str(value)


def _image_directory_stats(directory: Path) -> tuple[int, int]:
    count = 0
    total_bytes = 0
    for path in directory.glob("*.jpg"):
        try:
            stat_result = path.stat()
        except OSError:
            continue
        if path.is_file():
            count += 1
            total_bytes += stat_result.st_size
    return (count, total_bytes)


def _missing_occupied_image_reference_count(records: Sequence[SessionRecord]) -> int:
    return sum(1 for record in records if record.occupied_snapshot_path is None or record.occupied_crop_path is None)


def _validate_start_event(event: OccupancyEvent) -> None:
    if event.event_type is not OccupancyEventType.STATE_CHANGED or event.new_status is not OccupancyStatus.OCCUPIED:
        raise ArchiveSchemaError("start_session requires a state-changed event to occupied")


def _validate_close_event(event: OccupancyEvent) -> None:
    if event.new_status is not OccupancyStatus.EMPTY:
        raise ArchiveSchemaError("close_session requires an event whose new_status is empty")


def _event_payload(event: OccupancyEvent) -> dict[str, Any]:
    payload = event.to_dict()
    _validate_json_safe(payload, "event")
    return payload


def _event_time(event: OccupancyEvent) -> str:
    return str(event.observed_at)


def _optional_event_snapshot(event: OccupancyEvent) -> str | None:
    return None if event.snapshot_path is None else str(event.snapshot_path)


def _session_id(spot_id: str, observed_at: Any) -> str:
    spot_slug = _slug(spot_id)
    time_slug = _slug(str(observed_at))
    return f"sess_{spot_slug}_{time_slug}"[:180]


def _slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "unknown"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _duration_seconds(started_at: str, ended_at: str) -> int | None:
    start = _parse_timestamp(started_at)
    end = _parse_timestamp(ended_at)
    if start is None or end is None:
        return None
    duration = (end - start).total_seconds()
    if duration < 0 or not math.isfinite(duration):
        return None
    return int(duration)


def _parse_timestamp(value: str) -> datetime | None:
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _correction_id(action: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"corr_{_slug(action)}_{stamp}"


def _string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or value == "":
        raise ArchiveSchemaError(f"{field_name} must be a non-empty string")
    return value


def _bounded_string(value: Any, field_name: str, *, max_length: int) -> str:
    text = _string(value, field_name)
    if len(text) > max_length:
        raise ArchiveSchemaError(f"{field_name} exceeds maximum length of {max_length}")
    return text


def _optional_bounded_string(value: Any, field_name: str, *, max_length: int) -> str | None:
    if value is None:
        return None
    return _bounded_string(value, field_name, max_length=max_length)


def _optional_profile_id(value: Any, field_name: str) -> str | None:
    text = _optional_bounded_string(value, field_name, max_length=220)
    if text is None:
        return None
    if not text.startswith("prof"):
        raise ArchiveSchemaError(f"{field_name} must start with prof")
    return text


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _string(value, field_name)


def _dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ArchiveSchemaError(f"{field_name} must be an object")
    _validate_json_safe(value, field_name)
    return dict(value)


def _optional_dict(value: Any, field_name: str) -> dict[str, Any] | None:
    if value is None:
        return None
    return _dict(value, field_name)


def _optional_non_negative_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    return _non_negative_int(value, field_name)


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ArchiveSchemaError(f"{field_name} must be a positive integer")
    return value


def _non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ArchiveSchemaError(f"{field_name} must be a non-negative integer")
    return value


def _finite_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ArchiveSchemaError(f"{field_name} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise ArchiveSchemaError(f"{field_name} must be a finite number")
    return result


def _string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ArchiveSchemaError(f"{field_name} must be an array")
    return tuple(_string(item, field_name) for item in value)


def _optional_finite_float(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    return _finite_float(value, field_name)


def _validate_json_safe(value: Any, field_name: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ArchiveSchemaError(f"{field_name} contains a non-finite number")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ArchiveSchemaError(f"{field_name} contains a non-string object key")
            _validate_json_safe(item, f"{field_name}.{key}")
    elif isinstance(value, list | tuple):
        for index, item in enumerate(value):
            _validate_json_safe(item, f"{field_name}[{index}]")


def _safe_error_message(error: BaseException) -> str:
    message = redact_diagnostic_text(error)
    return message.replace("raw_image_bytes", "<redacted>")
