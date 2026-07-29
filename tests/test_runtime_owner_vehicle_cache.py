from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from parking_spot_monitor import runtime_owner_vehicle_cache
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.owner_vehicles import OwnerVehicle, OwnerVehicleRegistry
from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleRuntimeCache, OwnerVehicleSnapshot
from parking_spot_monitor.runtime_vehicle_events import _owner_vehicle_quiet_window_alerts
from parking_spot_monitor.scheduler import QuietWindowStatus


class FakeArchive:
    def __init__(self) -> None:
        self.revision = 1
        self.active_loads = 0

    def mutation_revision(self) -> int:
        return self.revision

    def load_active_sessions(self) -> list[object]:
        self.active_loads += 1
        return []


def write_registry(path: Path, profile_id: str | None = None) -> None:
    vehicles = [] if profile_id is None else [{"profile_id": profile_id, "label": "Owner car"}]
    path.write_text(json.dumps({"schema_version": 1, "owner_vehicles": vehicles}), encoding="utf-8")


def test_owner_snapshot_reuses_registry_and_active_sessions(tmp_path: Path) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    registry_path.write_text('{"schema_version":1,"owner_vehicles":[]}', encoding="utf-8")
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)

    first = cache.snapshot(archive)
    second = cache.snapshot(archive)

    assert second is first
    assert archive.active_loads == 1


def test_owner_snapshot_copies_active_sessions_into_a_tuple(tmp_path: Path) -> None:
    marker = object()

    class MutableArchive(FakeArchive):
        def __init__(self) -> None:
            super().__init__()
            self.sessions = [marker]

        def load_active_sessions(self) -> list[object]:
            self.active_loads += 1
            return self.sessions

    registry_path = tmp_path / "owner-vehicles.json"
    write_registry(registry_path)
    archive = MutableArchive()

    snapshot = OwnerVehicleRuntimeCache(registry_path).snapshot(archive)
    archive.sessions.clear()

    assert snapshot.active_sessions == (marker,)


def test_owner_registry_defensively_freezes_its_profile_mapping() -> None:
    owner = OwnerVehicle(profile_id="profile-a", label="Car")
    source = {"profile-a": owner}
    registry = OwnerVehicleRegistry(source)

    source.clear()

    assert registry.owner_for_profile("profile-a") is owner
    with pytest.raises(TypeError):
        registry.vehicles_by_profile_id["profile-b"] = owner  # type: ignore[index]


def test_owner_snapshot_invalidates_on_registry_replace(tmp_path: Path) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    registry_path.write_text('{"schema_version":1,"owner_vehicles":[]}', encoding="utf-8")
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)
    first = cache.snapshot(archive)

    replacement = tmp_path / "owner-vehicles.next.json"
    replacement.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_vehicles": [{"profile_id": "profile-a", "label": "Owner car"}],
            }
        ),
        encoding="utf-8",
    )
    replacement.replace(registry_path)

    second = cache.snapshot(archive)

    assert second is not first
    assert second.registry.owner_for_profile("profile-a") is not None
    assert archive.active_loads == 2


def test_owner_snapshot_invalidates_on_archive_mutation(tmp_path: Path) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    registry_path.write_text('{"schema_version":1,"owner_vehicles":[]}', encoding="utf-8")
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)
    first = cache.snapshot(archive)

    archive.revision += 1
    second = cache.snapshot(archive)

    assert second is not first
    assert archive.active_loads == 2


def test_missing_owner_registry_has_a_stable_cache_signature(tmp_path: Path) -> None:
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(tmp_path / "missing-owner-vehicles.json")

    first = cache.snapshot(archive)
    second = cache.snapshot(archive)

    assert second is first
    assert archive.active_loads == 1
    assert dict(first.registry.vehicles_by_profile_id) == {}


def test_registry_replacement_during_rebuild_does_not_claim_a_stable_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    write_registry(registry_path)
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)
    real_load = runtime_owner_vehicle_cache.load_owner_vehicle_registry
    registry_loads = 0

    def replace_after_load(path: str | Path, *, raise_io_errors: bool = False) -> OwnerVehicleRegistry:
        nonlocal registry_loads
        registry_loads += 1
        registry = real_load(path, raise_io_errors=raise_io_errors)
        if registry_loads == 1:
            Path(path).write_text(
                '{"schema_version":1,"owner_vehicles":[{"profile_id":"profile-b","label":"Car"}]}',
                encoding="utf-8",
            )
        return registry

    monkeypatch.setattr(runtime_owner_vehicle_cache, "load_owner_vehicle_registry", replace_after_load)

    snapshot = cache.snapshot(archive)

    assert snapshot.registry.owner_for_profile("profile-b") is not None
    assert registry_loads == 2
    assert archive.active_loads == 2
    assert cache.snapshot(archive) is snapshot


def test_archive_mutation_during_rebuild_is_cached_only_after_a_stable_retry(tmp_path: Path) -> None:
    stale_session = object()
    current_session = object()

    class MutatingArchive(FakeArchive):
        def __init__(self) -> None:
            super().__init__()
            self.revision_reads = 0
            self.sessions = [stale_session]

        def mutation_revision(self) -> int:
            self.revision_reads += 1
            return self.revision

        def load_active_sessions(self) -> list[object]:
            self.active_loads += 1
            sessions = list(self.sessions)
            if self.active_loads == 1:
                self.revision += 1
                self.sessions = [current_session]
            return sessions

    registry_path = tmp_path / "owner-vehicles.json"
    write_registry(registry_path)
    archive = MutatingArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)

    snapshot = cache.snapshot(archive)
    cached = cache.snapshot(archive)

    assert snapshot.active_sessions == (current_session,)
    assert cached is snapshot
    assert archive.active_loads == 2
    assert archive.revision_reads == 5


def test_registry_load_failure_retries_without_claiming_the_new_signature(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    write_registry(registry_path)
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)
    original = cache.snapshot(archive)
    write_registry(registry_path, "profile-b")
    real_read_text = Path.read_text
    attempts = 0

    def fail_once(path: Path, *args: object, **kwargs: object) -> str:
        nonlocal attempts
        if path == registry_path:
            attempts += 1
        if path == registry_path and attempts == 1:
            raise OSError("transient registry read failure")
        return real_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", fail_once)

    with pytest.raises(OSError, match="transient registry read failure"):
        cache.snapshot(archive)
    recovered = cache.snapshot(archive)

    assert recovered is not original
    assert recovered.registry.owner_for_profile("profile-b") is not None
    assert cache.snapshot(archive) is recovered
    assert attempts == 2


def test_registry_stat_failure_retries_without_overwriting_the_prior_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    write_registry(registry_path)
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)
    original = cache.snapshot(archive)
    write_registry(registry_path, "profile-c")
    real_stat = Path.stat
    fail_next = True

    def fail_once(path: Path, *args: object, **kwargs: object) -> os.stat_result:
        nonlocal fail_next
        if path == registry_path and fail_next:
            fail_next = False
            raise OSError("transient registry stat failure")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", fail_once)

    with pytest.raises(OSError, match="transient registry stat failure"):
        cache.snapshot(archive)
    recovered = cache.snapshot(archive)

    assert recovered is not original
    assert recovered.registry.owner_for_profile("profile-c") is not None
    assert cache.snapshot(archive) is recovered


def test_no_quiet_window_avoids_owner_snapshot_loading() -> None:
    class ForbiddenCache:
        def snapshot(self, _archive: object) -> object:
            raise AssertionError("inactive frames must not load owner inputs")

    alerts = _owner_vehicle_quiet_window_alerts(
        FakeArchive(),  # type: ignore[arg-type]
        quiet_status=QuietWindowStatus(active=False),
        observed_at=datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
        emitted_alert_ids=set(),
        configured_spot_ids=("left_spot",),
        logger=StructuredLogger(),
        owner_vehicle_cache=ForbiddenCache(),  # type: ignore[arg-type]
    )

    assert alerts == []


def test_owner_snapshot_failure_preserves_the_alert_scan_warning(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FailingCache:
        def snapshot(self, _archive: object) -> OwnerVehicleSnapshot:
            raise PermissionError("registry denied token=private-value")

    alerts = _owner_vehicle_quiet_window_alerts(
        FakeArchive(),  # type: ignore[arg-type]
        quiet_status=QuietWindowStatus(active=True, active_window_id="window-a"),
        observed_at=datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
        emitted_alert_ids=set(),
        configured_spot_ids=("left_spot",),
        logger=StructuredLogger(),
        owner_vehicle_cache=FailingCache(),  # type: ignore[arg-type]
    )

    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert alerts == []
    assert '"event":"owner-vehicle-alert-scan-failed"' in output
    assert '"action":"load-owner-registry"' in output
    assert "private-value" not in output


def test_repeated_snapshot_instability_is_bounded_and_warns_without_alerting(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class AlwaysMutatingArchive(FakeArchive):
        def load_active_sessions(self) -> list[object]:
            self.active_loads += 1
            self.revision += 1
            return []

    registry_path = tmp_path / "owner-vehicles.json"
    write_registry(registry_path, "profile-a")
    archive = AlwaysMutatingArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)

    alerts = _owner_vehicle_quiet_window_alerts(
        archive,  # type: ignore[arg-type]
        quiet_status=QuietWindowStatus(active=True, active_window_id="window-a"),
        observed_at=datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
        emitted_alert_ids=set(),
        configured_spot_ids=("left_spot",),
        logger=StructuredLogger(),
        owner_vehicle_cache=cache,
    )

    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert alerts == []
    assert archive.active_loads == 2
    assert '"event":"owner-vehicle-alert-scan-failed"' in output
    assert '"error_type":"OwnerVehicleSnapshotUnstableError"' in output
    assert '"error_message":"owner vehicle inputs changed during snapshot"' in output
