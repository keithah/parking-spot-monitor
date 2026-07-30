from __future__ import annotations

import json
import os
from pathlib import Path

import parking_spot_monitor.runtime_owner_vehicle_cache as owner_cache_module
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.runtime_owner_vehicle_cache import OwnerVehicleRuntimeCache
from parking_spot_monitor.vehicle_history_correction_cache import _file_stat_signature
from parking_spot_monitor.vehicle_history_storage_utils import _session_file_signature


def _replace_preserving_size_and_mtime(path: Path, payload: bytes) -> None:
    original = path.stat()
    assert len(payload) == original.st_size
    replacement = path.with_name(f".{path.name}.replacement")
    replacement.write_bytes(payload)
    os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
    os.replace(replacement, path)


def test_correction_signature_detects_same_size_preserved_mtime_replacement(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    path.write_bytes(b'{"label":"old"}\n')
    before, available = _file_stat_signature(path)
    assert available

    _replace_preserving_size_and_mtime(path, b'{"label":"new"}\n')

    after, available = _file_stat_signature(path)
    assert available
    assert after != before


def test_active_session_signature_detects_same_size_preserved_mtime_replacement(
    tmp_path: Path,
) -> None:
    active = tmp_path / "active"
    active.mkdir()
    path = active / "session.json"
    path.write_bytes(b'{"profile":"old"}')
    before = _session_file_signature(active)

    _replace_preserving_size_and_mtime(path, b'{"profile":"new"}')

    assert _session_file_signature(active) != before


def test_unchanged_invalid_owner_registry_reuses_last_good_without_reparse(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "owner-vehicles.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_vehicles": [{"profile_id": "profile-a", "label": "Owner"}],
            }
        ),
        encoding="utf-8",
    )

    class Archive:
        def mutation_revision(self) -> int:
            return 0

        def active_session_signature(self):
            return ()

        def load_active_sessions(self):
            return []

    cache = OwnerVehicleRuntimeCache(path, logger=StructuredLogger())
    assert cache.snapshot(Archive()).registry.owner_for_profile("profile-a") is not None
    path.write_text("{broken", encoding="utf-8")
    real_load = owner_cache_module.load_owner_vehicle_registry
    calls = 0

    def counted_load(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real_load(*args, **kwargs)

    monkeypatch.setattr(owner_cache_module, "load_owner_vehicle_registry", counted_load)

    assert cache.snapshot(Archive()).registry.owner_for_profile("profile-a") is not None
    assert cache.snapshot(Archive()).registry.owner_for_profile("profile-a") is not None
    assert calls == 1
