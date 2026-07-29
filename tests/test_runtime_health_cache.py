from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor.runtime_health_cache import VehicleHistoryHealthSnapshotCache
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive


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


def test_successful_correction_quarantine_invalidates_a_primed_health_snapshot(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    clock = FakeClock(datetime(2026, 6, 10, tzinfo=timezone.utc))
    cache = VehicleHistoryHealthSnapshotCache(archive, now=clock, ttl_seconds=300)
    first = cache.snapshot()
    revision = archive.mutation_revision()
    archive.corrections_dir.mkdir(parents=True, exist_ok=True)
    archive.corrections_path.write_text("{not-json\n", encoding="utf-8")

    replay = archive.correction_replay_state()
    second = cache.snapshot()

    assert first["correction_quarantine_count"] == 0
    assert replay.quarantine_count == 1
    assert archive.mutation_revision() == revision + 1
    assert second["correction_quarantine_count"] == 1
