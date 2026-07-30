from __future__ import annotations

from tests.support._vehicle_history import *  # noqa: F403


def test_export_archive_writes_tar_bundle_and_safe_maintenance_manifest(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    old = archive.start_session(occupied_event(spot_id="export-old", observed_at="2026-05-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="export-old", observed_at="2026-05-01T09:00:00Z"))
    active = archive.start_session(occupied_event(spot_id="export-active", observed_at="2026-05-02T08:00:00Z"))
    image_dir = tmp_path / "vehicle-history" / "images" / "occupied-full"
    image_dir.mkdir(parents=True)
    image_path = image_dir / f"{old.session_id}.jpg"
    image_path.write_bytes(b"explicit operator bundle may contain image bytes")

    output = tmp_path / "vehicle-history-export.tar.gz"
    result = archive.export_archive(output)

    assert output.exists()
    assert result.status == "ok"
    assert result.retention_policy == "indefinite"
    assert result.active_session_count == 1
    assert result.closed_session_count == 1
    assert result.member_count == len(result.member_names)
    assert "vehicle-history/sessions/closed/" + old.session_id + ".json" in result.member_names
    assert "vehicle-history/sessions/active/" + active.session_id + ".json" in result.member_names
    assert "vehicle-history/images/occupied-full/" + image_path.name in result.member_names
    assert any(name.startswith("vehicle-history/metadata/maintenance/export-") for name in result.member_names)
    with tarfile.open(output, "r:gz") as bundle:
        names = bundle.getnames()
        assert sorted(names) == sorted(result.member_names)
        manifest_name = next(name for name in names if name.startswith("vehicle-history/metadata/maintenance/export-"))
        manifest_file = bundle.extractfile(manifest_name)
        assert manifest_file is not None
        bundle_manifest = json.loads(manifest_file.read().decode("utf-8"))
    disk_manifest = json.loads(Path(result.manifest_path).read_text())
    assert disk_manifest["operation"] == "export"
    assert disk_manifest["member_names"] == list(result.member_names)
    assert bundle_manifest["member_names"] == list(result.member_names)
    rendered = json.dumps(result.to_json_dict()) + json.dumps(disk_manifest)
    assert "explicit operator bundle may contain image bytes" not in rendered
    assert "raw_image_bytes" not in rendered
    assert archive.health_snapshot()["last_maintenance_metadata"]["operation"] == "export"


def test_prune_closed_sessions_dry_run_apply_and_reference_safety(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    old = archive.start_session(occupied_event(spot_id="old", observed_at="2026-01-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="old", observed_at="2026-01-01T09:00:00Z"))
    retained = archive.start_session(occupied_event(spot_id="retained", observed_at="2026-05-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="retained", observed_at="2026-05-01T09:00:00Z"))
    active = archive.start_session(occupied_event(spot_id="active", observed_at="2026-05-02T08:00:00Z"))
    full_dir = tmp_path / "vehicle-history" / "images" / "occupied-full"
    crop_dir = tmp_path / "vehicle-history" / "images" / "occupied-crops"
    full_dir.mkdir(parents=True)
    crop_dir.mkdir(parents=True)
    old_image = full_dir / "old-shared.jpg"
    old_crop = crop_dir / "old-only.jpg"
    retained_image = full_dir / "retained-shared.jpg"
    old_image.write_bytes(b"old shared")
    old_crop.write_bytes(b"old crop")
    retained_image.write_bytes(b"retained shared")

    for archive_state, session_id, full_path, crop_path in [
        ("closed", old.session_id, old_image, old_crop),
        ("closed", retained.session_id, retained_image, retained_image),
        ("active", active.session_id, old_image, retained_image),
    ]:
        path = tmp_path / "vehicle-history" / "sessions" / archive_state / f"{session_id}.json"
        payload = json.loads(path.read_text())
        payload["occupied_snapshot_path"] = str(full_path)
        payload["occupied_crop_path"] = str(crop_path)
        path.write_text(json.dumps(payload, allow_nan=False))

    cutoff = "2026-02-01T00:00:00Z"
    dry = archive.prune_closed_sessions(older_than=cutoff, dry_run=True)

    assert dry.status == "dry_run"
    assert dry.candidate_session_count == 1
    assert dry.pruned_file_count == 2  # old session JSON + unshared crop only
    assert dry.skipped_active_session_count == 1
    assert dry.skipped_retained_image_count == 1
    assert (tmp_path / "vehicle-history" / "sessions" / "closed" / f"{old.session_id}.json").exists()
    assert old_crop.exists()

    applied = archive.prune_closed_sessions(older_than=cutoff, dry_run=False)

    assert applied.status == "ok"
    assert applied.candidate_session_count == 1
    assert not (tmp_path / "vehicle-history" / "sessions" / "closed" / f"{old.session_id}.json").exists()
    assert not old_crop.exists()
    assert old_image.exists()  # still referenced by active session
    assert retained_image.exists()
    assert (tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json").exists()
    assert (tmp_path / "vehicle-history" / "sessions" / "closed" / f"{retained.session_id}.json").exists()
    assert archive.health_snapshot()["last_maintenance_metadata"]["operation"] == "prune"


def test_prune_closed_sessions_deduplicates_image_refs_without_list_membership_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import parking_spot_monitor.vehicle_history_maintenance as maintenance

    class HashOnlyPath:
        def __init__(self, path: Path) -> None:
            self.path = path

        def __fspath__(self) -> str:
            return os.fspath(self.path)

        def __hash__(self) -> int:
            return hash(self.path)

        def __eq__(self, other: object) -> bool:
            raise AssertionError("deduplication should use a hash set, not repeated list membership")

        def stat(self) -> os.stat_result:
            return self.path.stat()

        def is_file(self) -> bool:
            return self.path.is_file()

        def unlink(self, *, missing_ok: bool = False) -> None:
            self.path.unlink(missing_ok=missing_ok)

    archive = VehicleHistoryArchive(tmp_path)
    old = archive.start_session(occupied_event(spot_id="old", observed_at="2026-01-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="old", observed_at="2026-01-01T09:00:00Z"))
    image_path = tmp_path / "vehicle-history" / "images" / "occupied-full" / "old.jpg"
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"old image")
    monkeypatch.setattr(maintenance, "_record_archive_image_paths", lambda root, record: [HashOnlyPath(image_path), HashOnlyPath(image_path)])

    result = archive.prune_closed_sessions(older_than="2026-02-01T00:00:00Z", dry_run=True)

    assert result.pruned_file_count == 2


def test_resolve_profile_id_uses_provided_merge_mapping_without_copying(tmp_path: Path) -> None:
    class LookupOnlyMerges:
        def __contains__(self, key: object) -> bool:
            return key == "prof_source"

        def __getitem__(self, key: str) -> str:
            if key == "prof_source":
                return "prof_target"
            raise KeyError(key)

        def __iter__(self) -> Any:
            raise AssertionError("provided merge mapping should not be copied")

        def __len__(self) -> int:
            return 1

    archive = VehicleHistoryArchive(tmp_path)

    assert archive.resolve_profile_id("prof_source", merges=LookupOnlyMerges()) == "prof_target"


def test_prune_counts_missing_image_refs_and_rejects_invalid_cutoff(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    old = archive.start_session(occupied_event(spot_id="missing", observed_at="2026-01-01T08:00:00Z"))
    archive.close_session(open_event(spot_id="missing", observed_at="2026-01-01T09:00:00Z"))
    path = tmp_path / "vehicle-history" / "sessions" / "closed" / f"{old.session_id}.json"
    payload = json.loads(path.read_text())
    payload["occupied_snapshot_path"] = str(tmp_path / "vehicle-history" / "images" / "occupied-full" / "missing.jpg")
    path.write_text(json.dumps(payload, allow_nan=False))

    result = archive.prune_closed_sessions(older_than="2026-02-01T00:00:00Z", dry_run=True)

    assert result.missing_file_count == 1
    with pytest.raises(ArchiveSchemaError, match="ISO timestamp"):
        archive.prune_closed_sessions(older_than="not a date", dry_run=True)
    with pytest.raises(ArchiveSchemaError, match="non-negative"):
        cutoff_older_than_days(-1)


def test_clamp_crop_box_uses_floor_ceil_and_image_bounds() -> None:
    clamped = clamp_crop_box((-1.9, 2.1, 5.2, 8.8), (6, 7))

    assert clamped.as_pillow_box == (0, 2, 6, 7)
