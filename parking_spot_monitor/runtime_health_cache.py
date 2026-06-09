from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any, Protocol


class VehicleHistoryHealthArchive(Protocol):
    def health_snapshot(self) -> Mapping[str, Any]: ...


class VehicleHistoryHealthSnapshotCache:
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

    def snapshot(self, *, force: bool = False) -> Mapping[str, Any]:
        current = self._now()
        if not force and self._cached is not None and self._cached_at is not None and current - self._cached_at < self._ttl:
            return dict(self._cached)
        snapshot = dict(self._archive.health_snapshot())
        self._cached = snapshot
        self._cached_at = current
        return snapshot

    def invalidate(self) -> None:
        self._cached = None
        self._cached_at = None
