from __future__ import annotations

import json
import inspect
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from parking_spot_monitor import runtime_owner_vehicle_cache
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus
from parking_spot_monitor.owner_vehicles import OwnerVehicle, OwnerVehicleRegistry
from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleRuntimeCache, OwnerVehicleSnapshot
from parking_spot_monitor.runtime_vehicle_events import _owner_vehicle_quiet_window_alerts
from parking_spot_monitor.scheduler import QuietWindowStatus
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive


class FakeArchive:
    def __init__(self) -> None:
        self.revision = 1
        self.active_loads = 0

    def mutation_revision(self) -> int:
        return self.revision

    def active_session_signature(self) -> tuple[tuple[str, int, int], ...]:
        return ()

    def load_active_sessions(self) -> list[object]:
        self.active_loads += 1
        return []


def write_registry(path: Path, profile_id: str | None = None) -> None:
    vehicles = [] if profile_id is None else [{"profile_id": profile_id, "label": "Owner car"}]
    path.write_text(json.dumps({"schema_version": 1, "owner_vehicles": vehicles}), encoding="utf-8")


def occupied_event(*, spot_id: str = "left", observed_at: str = "2026-05-18T13:00:00Z") -> OccupancyEvent:
    return OccupancyEvent(
        event_type=OccupancyEventType.STATE_CHANGED,
        spot_id=spot_id,
        previous_status=OccupancyStatus.EMPTY,
        new_status=OccupancyStatus.OCCUPIED,
        observed_at=observed_at,
        source_timestamp=None,
        snapshot_path="/data/snapshots/start.jpg",
        candidate_summary={"score": 0.97, "bbox": [1, 2, 3, 4]},
    )


def replace_active_profile(
    archive: VehicleHistoryArchive,
    *,
    session_id: str,
    profile_id: str,
    mtime_ns: int | None = None,
) -> None:
    active_path = archive.active_dir / f"{session_id}.json"
    payload = json.loads(active_path.read_text(encoding="utf-8"))
    payload["profile_id"] = profile_id
    payload["profile_confidence"] = 0.99
    replacement = archive.active_dir / f".{session_id}.replacement"
    replacement.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    if mtime_ns is not None:
        os.utime(replacement, ns=(mtime_ns, mtime_ns))
    os.replace(replacement, active_path)


def test_owner_snapshot_reuses_registry_and_active_sessions(tmp_path: Path) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    registry_path.write_text('{"schema_version":1,"owner_vehicles":[]}', encoding="utf-8")
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())

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

    snapshot = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger()).snapshot(archive)
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
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())
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
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())
    first = cache.snapshot(archive)

    archive.revision += 1
    second = cache.snapshot(archive)

    assert second is not first
    assert archive.active_loads == 2


def test_owner_snapshot_invalidates_after_external_active_record_replacement(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event())
    cache = OwnerVehicleRuntimeCache(archive.root / "owner-vehicles.json", logger=StructuredLogger())
    first = cache.snapshot(archive)
    revision = archive.mutation_revision()
    old_stat = (archive.active_dir / f"{active.session_id}.json").stat()

    replace_active_profile(
        archive,
        session_id=active.session_id,
        profile_id="profile-external",
        mtime_ns=old_stat.st_mtime_ns + 1_000_000,
    )
    second = cache.snapshot(archive)

    assert archive.mutation_revision() == revision
    assert second is not first
    assert first.active_sessions[0].profile_id is None
    assert second.active_sessions[0].profile_id == "profile-external"


def test_external_active_record_replacement_during_rebuild_retries_to_a_stable_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event())
    cache = OwnerVehicleRuntimeCache(archive.root / "owner-vehicles.json", logger=StructuredLogger())
    real_load = archive.load_active_sessions
    active_loads = 0

    def replace_after_first_load() -> list[object]:
        nonlocal active_loads
        active_loads += 1
        sessions = real_load()
        if active_loads == 1:
            old_stat = (archive.active_dir / f"{active.session_id}.json").stat()
            replace_active_profile(
                archive,
                session_id=active.session_id,
                profile_id="profile-current",
                mtime_ns=old_stat.st_mtime_ns + 1_000_000,
            )
        return sessions  # type: ignore[return-value]

    monkeypatch.setattr(archive, "load_active_sessions", replace_after_first_load)

    snapshot = cache.snapshot(archive)

    assert snapshot.active_sessions[0].profile_id == "profile-current"
    assert active_loads == 2
    assert cache.snapshot(archive) is snapshot


def test_active_session_signature_stat_failure_warns_and_does_not_admit_a_cache_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    active = archive.start_session(occupied_event())
    active_path = archive.active_dir / f"{active.session_id}.json"
    cache = OwnerVehicleRuntimeCache(archive.root / "owner-vehicles.json", logger=StructuredLogger())
    real_stat = Path.stat
    real_load = archive.load_active_sessions
    active_stat_calls = 0
    active_loads = 0

    def fail_active_stat_once(path: Path, *args: object, **kwargs: object) -> os.stat_result:
        nonlocal active_stat_calls
        if path == active_path:
            active_stat_calls += 1
        if path == active_path and active_stat_calls == 3:
            raise PermissionError("active session signature denied")
        return real_stat(path, *args, **kwargs)

    def counted_load_active_sessions():
        nonlocal active_loads
        active_loads += 1
        return real_load()

    monkeypatch.setattr(Path, "stat", fail_active_stat_once)
    monkeypatch.setattr(archive, "load_active_sessions", counted_load_active_sessions)

    alerts = _owner_vehicle_quiet_window_alerts(
        archive,
        quiet_status=QuietWindowStatus(active=True, active_window_id="window-a"),
        observed_at=datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
        emitted_alert_ids=set(),
        configured_spot_ids=("left",),
        logger=StructuredLogger(),
        owner_vehicle_snapshot_provider=cache,
    )
    recovered = cache.snapshot(archive)
    cached = cache.snapshot(archive)

    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert alerts == []
    assert '"event":"owner-vehicle-alert-scan-failed"' in output
    assert '"error_type":"PermissionError"' in output
    assert recovered.active_sessions[0].session_id == active.session_id
    assert cached is recovered
    assert active_loads == 2


def test_missing_owner_registry_has_a_stable_cache_signature(tmp_path: Path) -> None:
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(tmp_path / "missing-owner-vehicles.json", logger=StructuredLogger())

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
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())
    real_load = runtime_owner_vehicle_cache.load_owner_vehicle_registry
    registry_loads = 0

    def replace_after_load(path: str | Path, *, strict: bool = False) -> OwnerVehicleRegistry:
        nonlocal registry_loads
        registry_loads += 1
        registry = real_load(path, strict=strict)
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
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())

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
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())
    original = cache.snapshot(archive)
    write_registry(registry_path, "profile-b")
    real_open = Path.open
    attempts = 0

    def fail_once(path: Path, *args: object, **kwargs: object):
        nonlocal attempts
        if path == registry_path:
            attempts += 1
        if path == registry_path and attempts == 1:
            raise OSError("transient registry read failure")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_once)

    stale = cache.snapshot(archive)
    recovered = cache.snapshot(archive)

    assert stale.registry is original.registry
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
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())
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
        owner_vehicle_snapshot_provider=ForbiddenCache(),  # type: ignore[arg-type]
    )

    assert alerts == []


def test_owner_snapshot_provider_is_required_at_each_runtime_boundary() -> None:
    from parking_spot_monitor.runtime_frame_plan import build_runtime_frame_plan
    from parking_spot_monitor.runtime_state_update import _update_runtime_state_for_frame

    for boundary in (
        build_runtime_frame_plan,
        _update_runtime_state_for_frame,
        _owner_vehicle_quiet_window_alerts,
    ):
        parameter = inspect.signature(boundary).parameters["owner_vehicle_snapshot_provider"]
        assert parameter.default is inspect.Parameter.empty
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


def test_active_quiet_window_calls_required_owner_snapshot_provider_once() -> None:
    calls: list[object] = []

    class RecordingProvider:
        def snapshot(self, archive: object) -> OwnerVehicleSnapshot:
            calls.append(archive)
            return OwnerVehicleSnapshot(registry=OwnerVehicleRegistry.empty(), active_sessions=())

    archive = FakeArchive()
    alerts = _owner_vehicle_quiet_window_alerts(
        archive,  # type: ignore[arg-type]
        quiet_status=QuietWindowStatus(active=True, active_window_id="window-a"),
        observed_at=datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
        emitted_alert_ids=set(),
        configured_spot_ids=("left_spot",),
        logger=StructuredLogger(),
        owner_vehicle_snapshot_provider=RecordingProvider(),  # type: ignore[arg-type]
    )

    assert alerts == []
    assert calls == [archive]


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
        owner_vehicle_snapshot_provider=FailingCache(),  # type: ignore[arg-type]
    )

    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert alerts == []
    assert '"event":"owner-vehicle-alert-scan-failed"' in output
    assert '"action":"load-owner-registry"' in output
    assert "private-value" not in output


def test_runtime_cache_keeps_valid_registry_across_invalid_replace(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    write_registry(registry_path, "profile-a")
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())
    first = cache.snapshot(archive)
    registry_path.write_text("{broken", encoding="utf-8")

    second = cache.snapshot(archive)
    cache.snapshot(archive)

    assert second.registry is first.registry
    assert second.registry.owner_for_profile("profile-a") is not None
    output = capsys.readouterr().err
    assert output.count('"event":"owner-vehicle-registry-invalid"') == 1
    assert "{broken" not in output


def test_runtime_cache_initial_invalid_registry_fails_closed(tmp_path: Path) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    registry_path.write_text("{broken", encoding="utf-8")
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())

    with pytest.raises(runtime_owner_vehicle_cache.OwnerVehicleSnapshotUnavailableError):
        cache.snapshot(FakeArchive())


def test_repeated_active_scan_of_initial_invalid_registry_logs_only_once(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    registry_path.write_text("{broken", encoding="utf-8")
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())
    archive = FakeArchive()

    for _ in range(2):
        alerts = _owner_vehicle_quiet_window_alerts(
            archive,  # type: ignore[arg-type]
            quiet_status=QuietWindowStatus(active=True, active_window_id="window-a"),
            observed_at=datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
            emitted_alert_ids=set(),
            configured_spot_ids=("left_spot",),
            logger=StructuredLogger(),
            owner_vehicle_snapshot_provider=cache,
        )
        assert alerts == []

    output = capsys.readouterr().err
    assert output.count('"event":"owner-vehicle-registry-invalid"') == 1
    assert '"event":"owner-vehicle-alert-scan-failed"' not in output


def test_runtime_cache_recovers_after_invalid_registry_is_repaired(tmp_path: Path) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    write_registry(registry_path, "profile-a")
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())
    cache.snapshot(archive)
    registry_path.write_text("{broken", encoding="utf-8")
    stale = cache.snapshot(archive)
    write_registry(registry_path, "profile-b")

    repaired = cache.snapshot(archive)

    assert stale.registry.owner_for_profile("profile-a") is not None
    assert repaired.registry.owner_for_profile("profile-a") is None
    assert repaired.registry.owner_for_profile("profile-b") is not None


def test_owner_snapshot_requires_archive_signature_protocol(tmp_path: Path) -> None:
    class IncompleteArchive:
        def mutation_revision(self) -> int:
            return 1

        def load_active_sessions(self) -> list[object]:
            return []

    cache = OwnerVehicleRuntimeCache(tmp_path / "missing.json", logger=StructuredLogger())

    with pytest.raises(AttributeError, match="active_session_signature"):
        cache.snapshot(IncompleteArchive())  # type: ignore[arg-type]


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
    cache = OwnerVehicleRuntimeCache(registry_path, logger=StructuredLogger())

    alerts = _owner_vehicle_quiet_window_alerts(
        archive,  # type: ignore[arg-type]
        quiet_status=QuietWindowStatus(active=True, active_window_id="window-a"),
        observed_at=datetime(2026, 5, 18, 20, 5, 6, tzinfo=timezone.utc),
        emitted_alert_ids=set(),
        configured_spot_ids=("left_spot",),
        logger=StructuredLogger(),
        owner_vehicle_snapshot_provider=cache,
    )

    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert alerts == []
    assert archive.active_loads == 2
    assert '"event":"owner-vehicle-alert-scan-failed"' in output
    assert '"error_type":"OwnerVehicleSnapshotUnstableError"' in output
    assert '"error_message":"owner vehicle inputs changed during snapshot"' in output
