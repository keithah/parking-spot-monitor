from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value
from parking_spot_monitor.vehicle_history_models import (
    MAX_PROFILE_FILE_BYTES,
    MAX_SESSION_FILE_BYTES,
    ArchiveSchemaError,
    ArchiveWriteError,
    SessionRecord,
    StoredVehicleProfile,
    _safe_error_message,
)

class VehicleHistoryStorageMixin:
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
        self._mutation_revision = 0

    def mutation_revision(self) -> int:
        """Monotonic counter bumped on every archive write that affects health_snapshot."""
        return self._mutation_revision

    def _bump_revision(self) -> None:
        self._mutation_revision += 1

    def load_active_sessions(self) -> list[SessionRecord]:
        return self._load_records(self.active_dir)

    def list_closed_sessions(self) -> list[SessionRecord]:
        return self._load_records(self.closed_dir)

    def resolve_wrong_match_subject(self, subject_id: str) -> str:
        for directory in (self.active_dir, self.closed_dir):
            path = directory / f"{subject_id}.json"
            if path.exists():
                record = self._load_record(path)
                if record is not None and record.session_id == subject_id:
                    return subject_id

        latest_match: SessionRecord | None = None
        for record in self._iter_records(self.active_dir):
            if record.spot_id == subject_id:
                latest_match = _latest_session_record(latest_match, record)
        for record in self._iter_records(self.closed_dir):
            if record.spot_id == subject_id:
                latest_match = _latest_session_record(latest_match, record)
        return subject_id if latest_match is None else latest_match.session_id

    def _load_records(self, directory: Path) -> list[SessionRecord]:
        directory.mkdir(parents=True, exist_ok=True)
        records: list[SessionRecord] = []
        for record in self._iter_records(directory):
            records.append(record)
        self._log(
            "info",
            "vehicle-archive-loaded",
            archive_state=directory.name,
            session_count=len(records),
        )
        return records

    def _iter_records(self, directory: Path) -> Iterator[SessionRecord]:
        directory.mkdir(parents=True, exist_ok=True)
        for path in sorted(directory.glob("*.json")):
            record = self._load_record(path)
            if record is not None:
                yield record

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
        self._bump_revision()

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
        self._bump_revision()

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
        self._bump_revision()
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
        self._bump_revision()
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


def _latest_session_record(current: SessionRecord | None, candidate: SessionRecord) -> SessionRecord:
    if current is None:
        return candidate
    current_time = str(current.ended_at or current.started_at)
    candidate_time = str(candidate.ended_at or candidate.started_at)
    return candidate if candidate_time >= current_time else current
