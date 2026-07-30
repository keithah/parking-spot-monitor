from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.owner_vehicles import (
    OwnerVehicleRegistry,
    OwnerVehicleRegistryError,
    load_owner_vehicle_registry,
)
from parking_spot_monitor.vehicle_history_models import SessionRecord

MAX_SNAPSHOT_ATTEMPTS = 2
ActiveSessionSignature = tuple[tuple[str, int, int, int, int, int], ...]
OwnerVehicleFileSignature = tuple[int, int, int, int, int]
OwnerVehicleCacheKey = tuple[OwnerVehicleFileSignature | None, int, ActiveSessionSignature | None]


class OwnerVehicleSnapshotUnstableError(RuntimeError):
    """The owner inputs changed throughout a bounded snapshot attempt."""


class OwnerVehicleSnapshotUnavailableError(RuntimeError):
    """No validated owner registry is available for alert evaluation."""


class OwnerVehicleArchive(Protocol):
    def mutation_revision(self) -> int: ...

    def active_session_signature(self) -> ActiveSessionSignature: ...

    def load_active_sessions(self) -> list[SessionRecord]: ...


@dataclass(frozen=True, slots=True)
class OwnerVehicleSnapshot:
    registry: OwnerVehicleRegistry
    active_sessions: tuple[SessionRecord, ...]


class OwnerVehicleSnapshotProvider(Protocol):
    def snapshot(self, archive: OwnerVehicleArchive) -> OwnerVehicleSnapshot: ...


class OwnerVehicleRuntimeCache:
    """Keep the current owner registry and active sessions as one snapshot."""

    def __init__(self, registry_path: str | Path, *, logger: StructuredLogger) -> None:
        self.registry_path = Path(registry_path)
        self.logger = logger
        self._entry: tuple[OwnerVehicleCacheKey, OwnerVehicleSnapshot] | None = None
        self._last_good_registry: OwnerVehicleRegistry | None = None
        self._last_good_signature: OwnerVehicleFileSignature | None | object = _UNSET
        self._last_failed_signature: OwnerVehicleFileSignature | None | object = _UNSET

    def snapshot(self, archive: OwnerVehicleArchive) -> OwnerVehicleSnapshot:
        for _attempt in range(MAX_SNAPSHOT_ATTEMPTS):
            before = _snapshot_key(self.registry_path, archive)
            if (
                self._last_failed_signature is _UNSET
                and self._entry is not None
                and self._entry[0] == before
            ):
                return self._entry[1]
            registry_signature = before[0]
            loaded_registry: OwnerVehicleRegistry | None = None
            load_failed = False
            if (
                self._last_failed_signature is _UNSET
                and self._last_good_registry is not None
                and registry_signature == self._last_good_signature
            ):
                registry = self._last_good_registry
            elif (
                self._last_good_registry is not None
                and registry_signature == self._last_failed_signature
            ):
                registry = self._last_good_registry
                load_failed = True
            else:
                try:
                    loaded_registry = load_owner_vehicle_registry(self.registry_path, strict=True)
                    registry = loaded_registry
                except OwnerVehicleRegistryError as exc:
                    load_failed = True
                    if registry_signature != self._last_failed_signature:
                        self.logger.warning(
                            "owner-vehicle-registry-invalid",
                            phase="owner-vehicle",
                            action="load-owner-registry",
                            error_code=exc.code,
                            error_message=exc.safe_message,
                        )
                        if exc.code != "read_failed":
                            self._last_failed_signature = registry_signature
                    if self._last_good_registry is None:
                        raise OwnerVehicleSnapshotUnavailableError(
                            "no valid owner vehicle registry snapshot is available"
                        ) from exc
                    registry = self._last_good_registry
            value = OwnerVehicleSnapshot(
                registry=registry,
                active_sessions=tuple(archive.load_active_sessions()),
            )
            after = _snapshot_key(self.registry_path, archive)
            if after == before:
                if loaded_registry is not None:
                    self._last_good_registry = loaded_registry
                    self._last_good_signature = registry_signature
                    self._last_failed_signature = _UNSET
                if not load_failed:
                    self._entry = (after, value)
                return value
        raise OwnerVehicleSnapshotUnstableError("owner vehicle inputs changed during snapshot")


def _file_signature(path: Path) -> OwnerVehicleFileSignature | None:
    try:
        stat_result = path.stat()
    except FileNotFoundError:
        return None
    return (
        stat_result.st_dev,
        stat_result.st_ino,
        stat_result.st_size,
        stat_result.st_mtime_ns,
        stat_result.st_ctime_ns,
    )


def _snapshot_key(
    registry_path: Path,
    archive: OwnerVehicleArchive,
) -> OwnerVehicleCacheKey:
    return _file_signature(registry_path), archive.mutation_revision(), archive.active_session_signature()


_UNSET = object()
