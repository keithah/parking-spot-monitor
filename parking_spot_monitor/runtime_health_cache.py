from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any, Protocol


class VehicleHistoryHealthArchive(Protocol):
    def health_snapshot(self) -> Mapping[str, Any]: ...

    def mutation_revision(self) -> int: ...


class VehicleHistoryHealthSnapshotCache:
    """Memoize the expensive archive health snapshot.

    The snapshot is recomputed when the archive's mutation revision advances
    (any write that affects the snapshot bumps it), or when the time-to-live
    elapses as a backstop for state the revision does not track.
    """

    def __init__(
        self,
        archive: VehicleHistoryHealthArchive,
        *,
        now: Callable[[], datetime],
        ttl_seconds: int,
    ) -> None:
        self._archive = archive
        self._now = now
        self._ttl = timedelta(seconds=ttl_seconds)
        self._cached: dict[str, Any] | None = None
        self._cached_at: datetime | None = None
        self._cached_revision: int | None = None

    def snapshot(self, *, force: bool = False) -> Mapping[str, Any]:
        current = self._now()
        revision = self._archive.mutation_revision()
        if (
            not force
            and self._cached is not None
            and self._cached_at is not None
            and self._cached_revision == revision
            and current - self._cached_at < self._ttl
        ):
            return dict(self._cached)
        snapshot = dict(self._archive.health_snapshot())
        self._cached = snapshot
        self._cached_at = current
        self._cached_revision = revision
        return snapshot
