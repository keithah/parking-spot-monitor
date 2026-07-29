from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from parking_spot_monitor.owner_vehicles import OwnerVehicleRegistry, load_owner_vehicle_registry
from parking_spot_monitor.vehicle_history_models import SessionRecord

MAX_SNAPSHOT_ATTEMPTS = 2
ActiveSessionSignature = tuple[tuple[str, int, int], ...]
OwnerVehicleCacheKey = tuple[tuple[int, int] | None, int, ActiveSessionSignature | None]


class OwnerVehicleSnapshotUnstableError(RuntimeError):
    """The owner inputs changed throughout a bounded snapshot attempt."""


class OwnerVehicleArchive(Protocol):
    def mutation_revision(self) -> int: ...

    def active_session_signature(self) -> ActiveSessionSignature: ...

    def load_active_sessions(self) -> list[SessionRecord]: ...


@dataclass(frozen=True, slots=True)
class OwnerVehicleSnapshot:
    registry: OwnerVehicleRegistry
    active_sessions: tuple[SessionRecord, ...]


class OwnerVehicleRuntimeCache:
    """Keep the current owner registry and active sessions as one snapshot."""

    def __init__(self, registry_path: str | Path) -> None:
        self.registry_path = Path(registry_path)
        self._entry: tuple[OwnerVehicleCacheKey, OwnerVehicleSnapshot] | None = None

    def snapshot(self, archive: OwnerVehicleArchive) -> OwnerVehicleSnapshot:
        for _attempt in range(MAX_SNAPSHOT_ATTEMPTS):
            before = _snapshot_key(self.registry_path, archive)
            if self._entry is not None and self._entry[0] == before:
                return self._entry[1]
            value = OwnerVehicleSnapshot(
                registry=load_owner_vehicle_registry(self.registry_path, raise_io_errors=True),
                active_sessions=tuple(archive.load_active_sessions()),
            )
            after = _snapshot_key(self.registry_path, archive)
            if after == before:
                self._entry = (after, value)
                return value
        raise OwnerVehicleSnapshotUnstableError("owner vehicle inputs changed during snapshot")


def _file_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat_result = path.stat()
    except FileNotFoundError:
        return None
    return stat_result.st_mtime_ns, stat_result.st_size


def _snapshot_key(
    registry_path: Path,
    archive: OwnerVehicleArchive,
) -> OwnerVehicleCacheKey:
    signature_reader = getattr(archive, "active_session_signature", None)
    active_signature = signature_reader() if callable(signature_reader) else None
    return _file_signature(registry_path), archive.mutation_revision(), active_signature
