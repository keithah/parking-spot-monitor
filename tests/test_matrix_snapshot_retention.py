from __future__ import annotations

from tests.support._matrix import *  # noqa: F403


def test_prepare_event_snapshot_copies_raw_latest_jpeg_with_metadata_and_stable_alert_payload(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(source, size=(8, 6))
    observed_at = datetime(2026, 5, 18, 20, 1, 2, tzinfo=timezone.utc)

    snapshot = prepare_event_snapshot(
        source_path=source,
        data_dir=tmp_path / "data",
        snapshots_dir=tmp_path / "matrix-snapshots",
        event_type="occupancy-open-event",
        event_id="open:left spot/../A?token=secret",
        spot_id="left spot/../A?token=secret",
        observed_at=observed_at,
    )

    assert snapshot.path.parent == tmp_path / "matrix-snapshots"
    assert snapshot.path.read_bytes() == raw_bytes
    assert snapshot.filename == "occupancy-open-event-left-spot-a-token-redacted-2026-05-18t20-01-02z.jpg"
    assert snapshot.txn_id == "snapshot-occupancy-open-event-left-spot-a-token-redacted-2026-05-18t20-01-02z"
    assert snapshot.info == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 8, "h": 6}
    assert snapshot.body == "Raw full-frame snapshot for left spot/../A?token=<redacted> at 2026-05-18T20:01:02+00:00"
    assert snapshot.log_context == {
        "event_type": "occupancy-open-event",
        "event_id": "open:left spot/../A?token=<redacted>",
        "spot_id": "left spot/../A?token=<redacted>",
        "source_path": str(source),
        "snapshot_path": str(snapshot.path),
        "byte_size": len(raw_bytes),
        "mimetype": "image/jpeg",
        "width": 8,
        "height": 6,
    }

    assert format_open_spot_alert(
        {"spot_id": "left_spot", "observed_at": observed_at, "snapshot_path": str(snapshot.path)}
    ) == "Parking spot open: left_spot at 2026-05-18 1:01:02 PM PDT"


def test_format_open_spot_alert_displays_12_hour_string_in_los_angeles_time() -> None:
    assert format_open_spot_alert({"spot_id": "right_spot", "observed_at": "2026-05-12T16:04:08.223073+00:00"}) == (
        "Parking spot open: right_spot at 2026-05-12 9:04:08 AM PDT"
    )


def test_monitor_lifecycle_event_uses_canonical_observed_at_format() -> None:
    observed_at = datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc)
    event = monitor_lifecycle_event(MONITOR_STARTED_EVENT_TYPE, observed_at)

    assert event["observed_at"] == "2026-05-18T18:00:00Z"
    assert event["event_id"] == "parking-monitor-started:2026-05-18T18:00:00Z"


def test_monitor_lifecycle_event_id_includes_signal_when_present() -> None:
    assert (
        monitor_lifecycle_event_id(
            event_type=MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
            observed_at="2026-05-18T18:01:00Z",
            signal="SIGTERM",
        )
        == "parking-monitor-shutdown-requested:SIGTERM:2026-05-18T18:01:00Z"
    )


def test_format_lifecycle_notice_formats_started_and_shutdown_events() -> None:
    assert (
        format_lifecycle_notice(
            {
                "event_type": MONITOR_STARTED_EVENT_TYPE,
                "observed_at": "2026-05-18T18:00:00Z",
            }
        )
        == "Parking monitor started at 2026-05-18 11:00:00 AM PDT."
    )
    assert (
        format_lifecycle_notice(
            {
                "event_type": MONITOR_SHUTDOWN_REQUESTED_EVENT_TYPE,
                "observed_at": "2026-05-18T18:01:00Z",
                "signal": "SIGTERM",
            }
        )
        == "Parking monitor shutdown requested by SIGTERM at 2026-05-18 11:01:00 AM PDT."
    )


def test_format_lifecycle_notice_rejects_unsupported_event_type() -> None:
    with pytest.raises(MatrixError) as exc_info:
        format_lifecycle_notice({"event_type": "parking-monitor-unknown", "observed_at": "2026-05-18T18:00:00Z"})

    assert exc_info.value.diagnostics["error_type"] == "unsupported_lifecycle_event"


def test_prepare_event_snapshot_uses_data_dir_snapshots_fallback_and_sanitizes_ids(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)

    snapshot = prepare_event_snapshot(
        source_path=source,
        data_dir=tmp_path / "data-root",
        snapshots_dir=None,
        event_type="occupancy/open event",
        event_id="spot#1 / event",
        spot_id=None,
        observed_at="2026-05-18T20:01:02Z",
    )

    assert snapshot.path == tmp_path / "data-root" / "snapshots" / "occupancy-open-event-spot-1-event-2026-05-18t20-01-02z.jpg"
    assert "/" not in snapshot.filename
    assert "#" not in snapshot.txn_id
    assert snapshot.info["mimetype"] == "image/jpeg"


def test_prune_event_snapshots_removes_oldest_matching_files_only_and_logs_counts(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.logging import StructuredLogger

    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    oldest = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    middle = snapshot_root / "quiet-window-started-street-sweeping-2026-05-18t21-00-00z.jpg"
    newest = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t22-00-00z.jpg"
    unrelated = snapshot_root / "latest.jpg"
    malformed = snapshot_root / "occupancy-open-event-left-spot-not-a-time.jpg"
    for index, path in enumerate([oldest, middle, newest, unrelated, malformed], start=1):
        path.write_bytes(b"x" * index)
        # Deliberately force mtime ordering to differ from lexical names only slightly.
        path.touch()

    result = prune_event_snapshots(snapshot_root, retention_count=2, logger=StructuredLogger())

    output = capsys.readouterr().err
    assert result.pruned_count == 1
    assert result.pruned_bytes == 1
    assert not oldest.exists()
    assert middle.exists()
    assert newest.exists()
    assert unrelated.exists()
    assert malformed.exists()
    assert '"event":"snapshot-retention-pruned"' in output
    assert '"pruned_count":1' in output
    assert '"retained_count":2' in output


@pytest.mark.parametrize("count", [0, 1, 2])
def test_prune_event_snapshots_keeps_files_when_at_or_under_limit(tmp_path: Path, count: int, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.logging import StructuredLogger

    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    files = [snapshot_root / f"occupancy-open-event-left-spot-2026-05-18t20-0{index}-00z.jpg" for index in range(count)]
    for path in files:
        path.write_bytes(b"jpeg")

    result = prune_event_snapshots(snapshot_root, retention_count=2, logger=StructuredLogger())

    assert result.pruned_count == 0
    assert result.pruned_bytes == 0
    assert all(path.exists() for path in files)
    assert "snapshot-retention-pruned" not in capsys.readouterr().err


def test_prune_event_snapshots_treats_missing_directory_as_empty(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from parking_spot_monitor.logging import StructuredLogger

    result = prune_event_snapshots(tmp_path / "missing", retention_count=2, logger=StructuredLogger())

    assert result.pruned_count == 0
    assert result.pruned_bytes == 0
    assert "snapshot-retention" not in capsys.readouterr().err


def test_prune_event_snapshots_logs_safe_failure_without_raising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from parking_spot_monitor.logging import StructuredLogger

    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    oldest = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    newest = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t21-00-00z.jpg"
    oldest.write_bytes(b"old")
    newest.write_bytes(b"new")

    def fail_unlink(_root: Path, _directory: str | None, _filename: str) -> int:
        raise PermissionError("permission denied token=secret raw_image_bytes abc")

    monkeypatch.setattr(matrix_snapshots, "delete_owned_artifact", fail_unlink)

    result = prune_event_snapshots(snapshot_root, retention_count=1, logger=StructuredLogger())

    output = capsys.readouterr().err
    assert result.pruned_count == 0
    assert oldest.exists()
    assert newest.exists()
    assert '"event":"snapshot-retention-failed"' in output
    assert '"error_type":"PermissionError"' in output
    assert "secret" not in output
    assert "raw_image_bytes abc" not in output


def test_delete_owned_artifact_reports_uncertain_durability_after_successful_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "snapshots"
    root.mkdir()
    target = root / "owned.jpg"
    target.write_bytes(b"owned")
    real_fsync = matrix_snapshot_storage.os.fsync
    real_unlink = owned_file_disposal.os.unlink
    directory_identity = (root.stat().st_dev, root.stat().st_ino)
    failed = False
    unlinked = False

    def track_disposal_unlink(name: object, *args: object, **kwargs: object) -> None:
        nonlocal unlinked
        real_unlink(name, *args, **kwargs)
        if str(name).endswith(".dispose"):
            unlinked = True

    def fail_post_unlink_directory_sync(descriptor: int) -> None:
        nonlocal failed
        value = os.fstat(descriptor)
        if not failed and unlinked and (value.st_dev, value.st_ino) == directory_identity:
            failed = True
            raise OSError("directory sync failed")
        real_fsync(descriptor)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", track_disposal_unlink)
    monkeypatch.setattr(matrix_snapshot_storage.os, "fsync", fail_post_unlink_directory_sync)
    result = matrix_snapshot_storage.delete_owned_artifact(root, None, target.name)

    assert result.status == "deleted"
    assert result.durable is False
    assert not target.exists()
    assert not any(path.name.endswith((".dispose", ".quarantine")) for path in root.iterdir())


def test_derivative_directory_sync_failure_is_retried_before_child_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "snapshots"
    child = root / ".upload-derivatives"
    child.mkdir(parents=True)
    with owned_directory_durability.IDENTITY_LOCK:
        owned_directory_durability.DURABLE_IDENTITIES.clear()
    root_identity = (root.stat().st_dev, root.stat().st_ino)
    real_fsync = matrix_snapshot_storage.os.fsync
    root_syncs = 0

    def fail_first_root_sync(descriptor: int) -> None:
        nonlocal root_syncs
        value = os.fstat(descriptor)
        if (value.st_dev, value.st_ino) == root_identity:
            root_syncs += 1
            if root_syncs == 1:
                raise OSError("root sync failed")
        real_fsync(descriptor)

    monkeypatch.setattr(matrix_snapshot_storage.os, "fsync", fail_first_root_sync)
    with pytest.raises(OSError):
        matrix_snapshot_storage.publish_owned_bytes(root, ".upload-derivatives", "first.jpg", b"x", mode=0o600)
    published = matrix_snapshot_storage.publish_owned_bytes(
        root, ".upload-derivatives", "second.jpg", b"x", mode=0o600
    )

    assert root_syncs == 2
    assert published.read_bytes() == b"x"
    with owned_directory_durability.IDENTITY_LOCK:
        owned_directory_durability.DURABLE_IDENTITIES.clear()
    matrix_snapshot_storage.publish_owned_bytes(
        root, ".upload-derivatives", "after-restart.jpg", b"x", mode=0o600
    )
    assert root_syncs == 3


def test_retention_treats_uncertain_durability_as_deleted_and_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "snapshots"
    root.mkdir()
    oldest = root / "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    newest = root / "occupancy-open-event-left-spot-2026-05-18t21-00-00z.jpg"
    oldest.write_bytes(b"old")
    newest.write_bytes(b"new")
    real_delete = matrix_snapshots.delete_owned_artifact

    def uncertain_delete(
        snapshot_root: Path, directory: str | None, filename: str
    ) -> matrix_snapshot_storage.OwnedArtifactDeleteResult:
        result = real_delete(snapshot_root, directory, filename)
        if directory is None and result.status == "deleted":
            return matrix_snapshot_storage.OwnedArtifactDeleteResult(
                result.status, result.bytes_deleted, durable=False
            )
        return result

    monkeypatch.setattr(matrix_snapshots, "delete_owned_artifact", uncertain_delete)
    result = prune_event_snapshots(root, retention_count=1, logger=StructuredLogger())

    output = capsys.readouterr().err
    assert result.pruned_count == 1
    assert result.failed_count == 0
    assert result.durability_uncertain_count == 1
    assert '"event":"snapshot-retention-durability-uncertain"' in output


def test_retention_recovers_manifested_raw_and_derivative_work_behind_decoys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "snapshots"
    derivative_dir = root / ".upload-derivatives"
    derivative_dir.mkdir(parents=True)
    oldest_name = "occupancy-open-event-left-spot-2026-05-18t20-00-00z.jpg"
    newest_name = "occupancy-open-event-left-spot-2026-05-18t21-00-00z.jpg"
    for directory in (root, derivative_dir):
        for index in range(300):
            (directory / f"decoy-{index:03d}").write_bytes(b"x")
    (root / oldest_name).write_bytes(b"old")
    (root / newest_name).write_bytes(b"new")
    (derivative_dir / oldest_name).write_bytes(b"derived")
    real_unlink = owned_file_disposal.os.unlink

    def interrupt_disposal(name: object, *args: object, **kwargs: object) -> None:
        if str(name).endswith(".dispose"):
            raise OSError("simulated crash")
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", interrupt_disposal)
    warmup = prune_event_snapshots(root, retention_count=1, logger=StructuredLogger())
    assert warmup.failed_count > 0
    first = prune_event_snapshots(root, retention_count=1, logger=StructuredLogger())
    assert first.failed_count > 0

    monkeypatch.setattr(owned_file_disposal.os, "unlink", real_unlink)
    pruned_count = warmup.pruned_count + first.pruned_count
    for _attempt in range(8):
        second = prune_event_snapshots(root, retention_count=1, logger=StructuredLogger())
        pruned_count += second.pruned_count
        if second.failed_count == 0:
            break

    assert second.failed_count == 0
    assert pruned_count == 1
    assert not (root / oldest_name).exists()
    assert not (derivative_dir / oldest_name).exists()


def test_prepare_event_snapshot_prunes_after_copy_without_removing_current_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    raw_bytes = write_jpeg(source)
    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    old = snapshot_root / "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"
    old.write_bytes(b"old")

    snapshot = prepare_event_snapshot(
        source_path=source,
        data_dir=tmp_path,
        snapshots_dir=snapshot_root,
        event_type="occupancy-open-event",
        event_id="event-1",
        spot_id="left_spot",
        observed_at="2026-05-18T20:01:02Z",
        snapshot_retention_count=1,
    )

    assert not old.exists()
    assert snapshot.path.exists()
    assert snapshot.path.read_bytes() == raw_bytes


def test_prepare_event_snapshot_rejects_debug_latest_as_matrix_evidence(tmp_path: Path) -> None:
    source = tmp_path / "debug_latest.jpg"
    write_jpeg(source)

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=tmp_path / "snapshots",
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_invalid_source"
    assert exc_info.value.diagnostics["source_path"] == str(source)
    assert not any((tmp_path / "snapshots").glob("*.jpg"))


def test_prepare_event_snapshot_reports_missing_source_without_deleting_raw_source(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=None,
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_copy_failed"
    assert exc_info.value.diagnostics["source_path"] == str(source)
    assert exc_info.value.diagnostics["snapshot_path"].endswith("left-spot-2026-05-18t20-01-02z.jpg")


def test_prepare_event_snapshot_rejects_non_image_bytes_without_claiming_jpeg_metadata(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    source.write_bytes(b"not a jpeg")

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=tmp_path / "snapshots",
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert source.read_bytes() == b"not a jpeg"
    assert exc_info.value.diagnostics["error_type"] == "snapshot_metadata_failed"
    assert exc_info.value.diagnostics["source_path"] == str(source)
    assert not (tmp_path / "snapshots" / "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg").exists()
    assert "mimetype" not in exc_info.value.diagnostics


def test_prepare_event_snapshot_failure_preserves_replacement_of_published_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    destination = tmp_path / "snapshots" / "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg"
    replacement = tmp_path / "replacement.txt"
    replacement_bytes = b"unrelated retained replacement"
    replacement.write_bytes(replacement_bytes)

    def replace_then_fail(*args: object, **kwargs: object) -> object:
        os.replace(replacement, destination)
        raise JpegDecodeError("read_failed")

    monkeypatch.setattr(matrix_snapshots, "read_owned_jpeg_evidence", replace_then_fail)

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=tmp_path / "snapshots",
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_metadata_failed"
    assert destination.read_bytes() == replacement_bytes


def test_prepare_event_snapshot_rejects_final_replacement_before_metadata_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source, size=(8, 6), color=(10, 20, 30))
    destination = tmp_path / "snapshots" / "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg"
    replacement = tmp_path / "replacement.jpg"
    write_jpeg(replacement, size=(3, 2), color=(200, 10, 5))
    replacement_bytes = replacement.read_bytes()
    real_read = matrix_snapshot_storage.read_owned_jpeg_evidence

    def swap_then_read(root: Path, filename: str, **kwargs: object) -> object:
        os.replace(replacement, destination)
        return real_read(root, filename, **kwargs)

    monkeypatch.setattr(matrix_snapshots, "read_owned_jpeg_evidence", swap_then_read)

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=tmp_path / "snapshots",
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_metadata_failed"
    assert destination.read_bytes() == replacement_bytes


def test_read_owned_jpeg_evidence_rejects_snapshot_root_swap_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    snapshot_root = tmp_path / "snapshots"
    publication = matrix_retained_publication.publish_retained_snapshot(source, snapshot_root, "event.jpg")
    moved_root = tmp_path / "snapshots-held"
    real_read_exact = matrix_snapshot_storage.read_descriptor_exact

    def swap_root(descriptor: int, signature: object, **kwargs: object) -> object:
        os.replace(snapshot_root, moved_root)
        snapshot_root.mkdir()
        return real_read_exact(descriptor, signature, **kwargs)

    monkeypatch.setattr(matrix_snapshot_storage, "read_descriptor_exact", swap_root)

    with pytest.raises(OSError):
        matrix_snapshot_storage.read_owned_jpeg_evidence(snapshot_root, "event.jpg")

    assert publication.path.name == "event.jpg"
    assert not (snapshot_root / "event.jpg").exists()
    assert (moved_root / "event.jpg").exists()


def test_matrix_metadata_cleanup_preserves_swap_at_delete_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    destination = tmp_path / "snapshots" / "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg"
    replacement = tmp_path / "replacement.txt"
    replacement_bytes = b"unrelated Matrix replacement"
    replacement.write_bytes(replacement_bytes)
    real_unlink, real_rename, real_replace = os.unlink, os.rename, os.replace
    swapped = False

    def swap_target() -> None:
        nonlocal swapped
        if not swapped:
            swapped = True
            real_replace(replacement, destination)

    def swapping_unlink(path: object, *args: object, **kwargs: object) -> None:
        if path == destination.name and kwargs.get("dir_fd") is not None:
            swap_target()
        real_unlink(path, *args, **kwargs)

    def swapping_rename(source_name: object, target: object, *args: object, **kwargs: object) -> None:
        if source_name == destination.name and kwargs.get("src_dir_fd") is not None:
            swap_target()
        real_rename(source_name, target, *args, **kwargs)

    def fail_metadata(*args: object, **kwargs: object) -> object:
        raise JpegDecodeError("read_failed")

    monkeypatch.setattr(matrix_snapshot_storage.os, "unlink", swapping_unlink)
    monkeypatch.setattr(matrix_snapshot_storage.os, "rename", swapping_rename)
    monkeypatch.setattr(matrix_snapshots, "read_owned_jpeg_evidence", fail_metadata)

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=tmp_path / "snapshots",
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_metadata_failed"
    assert swapped is True
    assert destination.read_bytes() == replacement_bytes
    assert list(destination.parent.glob(".*.quarantine")) == []


def test_retained_snapshot_rejects_oversized_source_before_creating_storage(tmp_path: Path) -> None:
    source = tmp_path / "padded.jpg"
    write_jpeg(source)
    with source.open("r+b") as handle:
        handle.seek(matrix_snapshot_storage.MAX_RETAINED_JPEG_BYTES)
        handle.write(b"x")
    snapshot_root = tmp_path / "snapshots"

    with pytest.raises(OSError, match="bounded regular file"):
        matrix_retained_publication.publish_retained_snapshot(source, snapshot_root, "event.jpg")

    assert not snapshot_root.exists()


def test_prepare_event_snapshot_maps_oversized_source_to_copy_failure(tmp_path: Path) -> None:
    source = tmp_path / "padded.jpg"
    write_jpeg(source)
    with source.open("r+b") as handle:
        handle.seek(matrix_snapshot_storage.MAX_RETAINED_JPEG_BYTES)
        handle.write(b"x")
    snapshot_root = tmp_path / "snapshots"

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=snapshot_root,
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_copy_failed"
    assert list(snapshot_root.glob("*.jpg")) == []
    assert list(snapshot_root.glob(".*.tmp")) == []


def test_retained_snapshot_growth_never_writes_beyond_preflight_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    original_size = source.stat().st_size
    source_identity = (source.stat().st_dev, source.stat().st_ino)
    snapshot_root = tmp_path / "snapshots"
    real_read = os.read
    real_unlink = file_descriptor_binding.RootedDirectoryOwner.unlink_if_matches
    appended = False
    discarded_sizes: list[int] = []

    def append_after_first_source_read(descriptor: int, size: int) -> bytes:
        nonlocal appended
        chunk = real_read(descriptor, size)
        value = os.fstat(descriptor)
        if chunk and not appended and (value.st_dev, value.st_ino) == source_identity:
            appended = True
            with source.open("ab") as handle:
                handle.write(b"x" * 4096)
        return chunk

    def record_discarded_size(owner: object, name: str, identity: object) -> bool:
        directory_fd = owner.fd
        discarded_sizes.append(os.stat(name, dir_fd=directory_fd, follow_symlinks=False).st_size)
        return real_unlink(owner, name, identity)

    monkeypatch.setattr(matrix_snapshot_storage.os, "read", append_after_first_source_read)
    monkeypatch.setattr(file_descriptor_binding.RootedDirectoryOwner, "unlink_if_matches", record_discarded_size)

    with pytest.raises(OSError, match="changed while copying"):
        matrix_retained_publication.publish_retained_snapshot(source, snapshot_root, "event.jpg")

    assert appended is True
    assert discarded_sizes == [original_size]
    assert list(snapshot_root.glob(".*.tmp")) == []


@pytest.mark.parametrize(
    "bomb",
    [
        Image.DecompressionBombError("oversized image"),
        Image.DecompressionBombWarning("oversized image warning"),
    ],
)
def test_prepare_event_snapshot_maps_decompression_bombs_and_deletes_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bomb: BaseException,
) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    destination = tmp_path / "snapshots" / "occupancy-open-event-left-spot-2026-05-18t20-01-02z.jpg"

    def raise_bomb(*args: object, **kwargs: object) -> None:
        raise bomb

    monkeypatch.setattr(jpeg_artifacts.Image, "open", raise_bomb)

    with pytest.raises(MatrixError) as exc_info:
        prepare_event_snapshot(
            source_path=source,
            data_dir=tmp_path,
            snapshots_dir=tmp_path / "snapshots",
            event_type="occupancy-open-event",
            event_id="event-1",
            spot_id="left_spot",
            observed_at="2026-05-18T20:01:02Z",
        )

    assert exc_info.value.diagnostics["error_type"] == "snapshot_metadata_failed"
    assert source.exists()
    assert not destination.exists()
