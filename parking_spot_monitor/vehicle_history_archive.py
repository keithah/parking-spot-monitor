from __future__ import annotations

from parking_spot_monitor.vehicle_history_corrections import VehicleHistoryCorrectionMixin
from parking_spot_monitor.vehicle_history_maintenance import VehicleHistoryMaintenanceMixin
from parking_spot_monitor.vehicle_history_profiles import VehicleHistoryProfileMixin
from parking_spot_monitor.vehicle_history_sessions import VehicleHistorySessionMixin
from parking_spot_monitor.vehicle_history_storage import VehicleHistoryStorageMixin
from parking_spot_monitor.vehicle_history_models import *


class VehicleHistoryArchive(
    VehicleHistorySessionMixin,
    VehicleHistoryProfileMixin,
    VehicleHistoryCorrectionMixin,
    VehicleHistoryMaintenanceMixin,
    VehicleHistoryStorageMixin,
):
    """File-backed vehicle session archive separate from runtime state.json."""


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
