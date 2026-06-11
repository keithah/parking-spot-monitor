from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from parking_spot_monitor.runtime_health_cache import VehicleHistoryHealthSnapshotCache


class FakeArchive:
    def __init__(self) -> None:
        self.snapshot_calls = 0
        self.revision = 0

    def health_snapshot(self) -> Mapping[str, Any]:
        self.snapshot_calls += 1
        return {"calls": self.snapshot_calls}

    def mutation_revision(self) -> int:
        return self.revision


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


def _cache(archive: FakeArchive, clock: FakeClock, ttl_seconds: int = 300) -> VehicleHistoryHealthSnapshotCache:
    return VehicleHistoryHealthSnapshotCache(archive, now=clock, ttl_seconds=ttl_seconds)


def test_snapshot_is_memoized_within_ttl_and_revision() -> None:
    archive = FakeArchive()
    clock = FakeClock(datetime(2026, 6, 10, tzinfo=timezone.utc))
    cache = _cache(archive, clock)

    first = cache.snapshot()
    clock.advance(10)
    second = cache.snapshot()

    assert first == second
    assert archive.snapshot_calls == 1


def test_snapshot_recomputes_when_revision_advances() -> None:
    archive = FakeArchive()
    clock = FakeClock(datetime(2026, 6, 10, tzinfo=timezone.utc))
    cache = _cache(archive, clock)

    cache.snapshot()
    archive.revision += 1
    cache.snapshot()

    assert archive.snapshot_calls == 2


def test_snapshot_recomputes_after_ttl_even_without_writes() -> None:
    archive = FakeArchive()
    clock = FakeClock(datetime(2026, 6, 10, tzinfo=timezone.utc))
    cache = _cache(archive, clock, ttl_seconds=300)

    cache.snapshot()
    clock.advance(301)
    cache.snapshot()

    assert archive.snapshot_calls == 2


def test_force_bypasses_the_cache() -> None:
    archive = FakeArchive()
    clock = FakeClock(datetime(2026, 6, 10, tzinfo=timezone.utc))
    cache = _cache(archive, clock)

    cache.snapshot()
    cache.snapshot(force=True)

    assert archive.snapshot_calls == 2
