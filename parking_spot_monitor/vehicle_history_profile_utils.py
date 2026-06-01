from __future__ import annotations

from pathlib import Path

from parking_spot_monitor.owner_vehicles import load_owner_vehicle_registry
from parking_spot_monitor.vehicle_history_models import (
    OWNER_PROFILE_MIN_ASSIGNMENT_CONFIDENCE,
    SessionRecord,
    StoredVehicleProfile,
    _utc_now,
)
from parking_spot_monitor.vehicle_profiles import VehicleDescriptor


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
    histogram = tuple(
        ((value * sample_count) + new_value) / next_count
        for value, new_value in zip(previous.rgb_histogram, latest.rgb_histogram, strict=True)
    )
    return VehicleDescriptor(
        width=round(((previous.width * sample_count) + latest.width) / next_count),
        height=round(((previous.height * sample_count) + latest.height) / next_count),
        aspect_ratio=((previous.aspect_ratio * sample_count) + latest.aspect_ratio) / next_count,
        rgb_histogram=histogram,
        average_hash=latest.average_hash,
        hash_bits=latest.hash_bits,
    )
