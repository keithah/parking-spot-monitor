from __future__ import annotations

import io
import json
import os
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor.vehicle_estimates import VehicleHistoryEstimate, estimate_vehicle_history
from parking_spot_monitor.vehicle_history_models import *


class VehicleHistoryMaintenanceMixin:
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

    def _image_directory_stats(self, directory: Path, *, phase: str) -> tuple[int, int]:
        try:
            return _image_directory_stats(directory)
        except OSError as exc:
            self._record_failure(phase=phase, path_name=directory.name, error=exc)
            return (0, 0)

    def _archive_directory_stats(self) -> tuple[int, int]:
        try:
            from parking_spot_monitor import vehicle_history as legacy_vehicle_history

            return legacy_vehicle_history._archive_directory_stats(self.root)
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
