from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

from parking_spot_monitor.occupancy import OccupancyEvent
from parking_spot_monitor.vehicle_history_images import VehicleHistoryImageError, capture_occupied_images
from parking_spot_monitor.vehicle_history_models import (
    SCHEMA_VERSION,
    ArchiveSchemaError,
    ArchiveWriteError,
    SessionRecord,
    _duration_seconds,
    _event_payload,
    _event_time,
    _optional_event_snapshot,
    _safe_error_message,
    _session_id,
    _utc_now,
    _validate_close_event,
    _validate_start_event,
)
from parking_spot_monitor.vehicle_history_storage_utils import _session_file_signature


class VehicleHistorySessionMixin:
    def active_session_signature(self) -> tuple[tuple[str, int, int], ...]:
        return _session_file_signature(self.active_dir)

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
        self._bump_revision()
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
