from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from parking_spot_monitor.logging import redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus
from parking_spot_monitor.owner_vehicles import load_owner_vehicle_registry
from parking_spot_monitor.vehicle_profiles import VehicleDescriptor, VehicleProfileRecord

SCHEMA_VERSION = 1
MAX_SESSION_FILE_BYTES = 1_000_000
MAX_PROFILE_FILE_BYTES = 500_000
MAX_CORRECTION_LINE_BYTES = 16_000
MAX_CORRECTION_TEXT_LENGTH = 160
MAX_CORRECTION_INVALID_LINES = 200
PROFILE_STATUS_ACTIVE = "active"
OWNER_PROFILE_MIN_ASSIGNMENT_CONFIDENCE = 0.95

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

def _is_owner_profile_low_confidence_match(root: Path, profile_id: str, confidence: float) -> bool:
    owner = load_owner_vehicle_registry(root / "owner-vehicles.json").owner_for_profile(profile_id)
    return owner is not None and confidence < OWNER_PROFILE_MIN_ASSIGNMENT_CONFIDENCE

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


__all__ = [name for name in globals() if not name.startswith("__")]
