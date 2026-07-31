from __future__ import annotations

import math
import time
from collections.abc import Callable
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
DEFAULT_ACTIVE_SESSION_POLL_SECONDS = 5.0
OwnerVehicleFileSignature = tuple[int, int, int, int, int]
OwnerVehicleCacheKey = tuple[OwnerVehicleFileSignature | None, int]
_UNSET = object()


class OwnerVehicleSnapshotUnstableError(RuntimeError):
    """The owner inputs changed throughout a bounded snapshot attempt."""


class OwnerVehicleSnapshotUnavailableError(RuntimeError):
    """No validated owner registry is available for alert evaluation."""


class OwnerVehicleArchive(Protocol):
    def mutation_revision(self) -> int: ...

    def load_active_sessions(self) -> list[SessionRecord]: ...


@dataclass(frozen=True, slots=True)
class OwnerVehicleSnapshot:
    registry: OwnerVehicleRegistry
    active_sessions: tuple[SessionRecord, ...]


class OwnerVehicleSnapshotProvider(Protocol):
    def snapshot(self, archive: OwnerVehicleArchive) -> OwnerVehicleSnapshot: ...


class OwnerVehicleRuntimeCache:
    """Keep the current owner registry and active sessions as one snapshot."""

    def __init__(
        self,
        registry_path: str | Path,
        *,
        logger: StructuredLogger,
        now: Callable[[], float] = time.monotonic,
        active_session_poll_seconds: float = DEFAULT_ACTIVE_SESSION_POLL_SECONDS,
    ) -> None:
        if not math.isfinite(active_session_poll_seconds) or active_session_poll_seconds <= 0:
            raise ValueError("active_session_poll_seconds must be finite and positive")
        self.registry_path = Path(registry_path)
        self.logger = logger
        self._now = now
        self._active_session_poll_seconds = active_session_poll_seconds
        self._active_sessions_checked_at: float | None = None
        self._entry: tuple[OwnerVehicleCacheKey, OwnerVehicleSnapshot] | None = None
        self._last_good_registry: OwnerVehicleRegistry | None = None
        self._last_good_signature: OwnerVehicleFileSignature | None | object = _UNSET
        self._last_failed_signature: OwnerVehicleFileSignature | None | object = _UNSET

    def snapshot(self, archive: OwnerVehicleArchive) -> OwnerVehicleSnapshot:
        checked_at = self._now()
        registry_signature = _file_signature(self.registry_path)
        revision = archive.mutation_revision()
        if self._entry_is_fresh(registry_signature, revision, checked_at):
            entry = self._entry
            assert entry is not None
            return entry[1]
        for _attempt in range(MAX_SNAPSHOT_ATTEMPTS):
            before = (registry_signature, revision)
            loaded_registry: OwnerVehicleRegistry | None = None
            load_failed = False
            if (
                self._last_failed_signature is _UNSET
                and self._last_good_registry is not None
                and registry_signature == self._last_good_signature
            ):
                registry = self._last_good_registry
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
                        self._last_failed_signature = registry_signature
                    if self._last_good_registry is None:
                        raise OwnerVehicleSnapshotUnavailableError(
                            "no valid owner vehicle registry snapshot is available"
                        ) from exc
                    registry = self._last_good_registry
            active_sessions = tuple(archive.load_active_sessions())
            after = _snapshot_key(self.registry_path, archive)
            if after == before:
                if (
                    not load_failed
                    and self._last_failed_signature is _UNSET
                    and self._entry is not None
                    and self._entry[0] == after
                    and self._entry[1].active_sessions == active_sessions
                ):
                    self._active_sessions_checked_at = checked_at
                    return self._entry[1]
                if loaded_registry is not None:
                    self._last_good_registry = loaded_registry
                    self._last_good_signature = registry_signature
                    self._last_failed_signature = _UNSET
                value = OwnerVehicleSnapshot(registry=registry, active_sessions=active_sessions)
                if not load_failed or registry_signature == self._last_failed_signature:
                    self._entry = (after, value)
                    self._active_sessions_checked_at = checked_at
                return value
            registry_signature = after[0]
            revision = after[1]
        raise OwnerVehicleSnapshotUnstableError("owner vehicle inputs changed during snapshot")

    def _entry_is_fresh(
        self,
        registry_signature: OwnerVehicleFileSignature | None,
        revision: int,
        checked_at: float,
    ) -> bool:
        if self._entry is None:
            return False
        key, _snapshot = self._entry
        if key[0] != registry_signature or key[1] != revision:
            return False
        last_checked_at = self._active_sessions_checked_at
        return (
            last_checked_at is not None
            and checked_at - last_checked_at < self._active_session_poll_seconds
        )


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
    return (
        _file_signature(registry_path),
        archive.mutation_revision(),
    )
