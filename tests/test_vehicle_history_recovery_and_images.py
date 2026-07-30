from __future__ import annotations

from tests.support._vehicle_history import *  # noqa: F403


@pytest.mark.parametrize(
    "token",
    [
        "0123456789abcde",
        "0123456789abcdef0",
        "0123456789abcdeF",
        "0123456789abcdeg",
        "0123456789abcdef.manual",
    ],
)
def test_owned_cleanup_recovery_ignores_noncanonical_quarantine_names(
    tmp_path: Path, token: str
) -> None:
    target = tmp_path / "owned[1].jpg"
    candidate = tmp_path / f".{target.name}.{token}.quarantine"
    candidate.write_bytes(b"unrelated")

    assert file_descriptor_binding.recover_quarantined_path(target) == 0

    assert not target.exists()
    assert candidate.read_bytes() == b"unrelated"


def test_owned_cleanup_recovery_unlinks_prior_hardlink_idempotently(tmp_path: Path) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    quarantine = tmp_path / ".owned.jpg.0123456789abcdef.quarantine"
    os.link(target, quarantine)

    assert file_descriptor_binding.recover_quarantined_path(target) == 1

    assert target.read_bytes() == b"owned"
    assert not quarantine.exists()


def test_owned_cleanup_recovery_retries_transient_hardlink_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    quarantine = tmp_path / ".owned.jpg.0123456789abcdef.quarantine"
    os.link(target, quarantine)
    real_unlink = os.unlink
    failed = False

    def fail_once(path: object, *args: object, **kwargs: object) -> None:
        nonlocal failed
        if str(path).endswith(".dispose") and kwargs.get("dir_fd") is not None and not failed:
            failed = True
            raise OSError("transient unlink failure")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(owned_file_cleanup.os, "unlink", fail_once)

    assert file_descriptor_binding.recover_quarantined_path(target) == 0
    assert not quarantine.exists()
    assert len(list(tmp_path.glob(".*.dispose"))) == 1
    assert file_descriptor_binding.recover_quarantined_path(target) == 1
    assert not list(tmp_path.glob(".*.dispose"))


def test_owned_cleanup_recovery_selects_exact_candidates_deterministically(tmp_path: Path) -> None:
    target = tmp_path / "owned[1].jpg"
    later = tmp_path / f".{target.name}.ffffffffffffffff.quarantine"
    first = tmp_path / f".{target.name}.0000000000000000.quarantine"
    unrelated = tmp_path / f".{target.name}.AAAAAAAAAAAAAAAA.quarantine"
    later.write_bytes(b"later")
    first.write_bytes(b"first")
    unrelated.write_bytes(b"unrelated")

    assert file_descriptor_binding.recover_quarantined_path(target) == 1

    assert target.read_bytes() == b"first"
    assert later.read_bytes() == b"later"
    assert unrelated.read_bytes() == b"unrelated"


def test_vehicle_failure_cleanup_preserves_swap_at_delete_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))
    full_path = tmp_path / "archive" / "images" / "occupied-full" / "session.jpg"
    replacement = tmp_path / "replacement.txt"
    replacement_bytes = b"unrelated vehicle replacement"
    replacement.write_bytes(replacement_bytes)
    real_unlink, real_rename, real_replace = os.unlink, os.rename, os.replace
    swapped = False

    def swap_target() -> None:
        nonlocal swapped
        if not swapped:
            swapped = True
            real_replace(replacement, full_path)

    def swapping_unlink(path: object, *args: object, **kwargs: object) -> None:
        if path == full_path.name and kwargs.get("dir_fd") is not None:
            swap_target()
        real_unlink(path, *args, **kwargs)

    def swapping_rename(source_name: object, destination: object, *args: object, **kwargs: object) -> None:
        if source_name == full_path.name and kwargs.get("src_dir_fd") is not None:
            swap_target()
        real_rename(source_name, destination, *args, **kwargs)

    def fail_crop(*args: object) -> ClampedCropBox:
        raise VehicleHistoryImageError("crop failed")

    monkeypatch.setattr(file_descriptor_binding.os, "unlink", swapping_unlink)
    monkeypatch.setattr(file_descriptor_binding.os, "rename", swapping_rename)
    monkeypatch.setattr(vehicle_history_images, "clamp_crop_box", fail_crop)

    with pytest.raises(VehicleHistoryImageError, match="crop failed"):
        capture_occupied_images(
            archive_root=tmp_path / "archive",
            session_id="session",
            source_frame_path=source,
            bbox=(0, 0, 8, 6),
        )

    assert swapped is True
    assert full_path.read_bytes() == replacement_bytes
    assert list(full_path.parent.glob(".*.quarantine")) == []


def test_vehicle_transaction_rejects_archive_root_swap_without_path_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))
    archive = tmp_path / "archive"
    moved_archive = tmp_path / "archive-held"
    real_write = vehicle_history_images._write_jpeg_atomic

    def swap_root_then_write(path: Path, image: Image.Image, **kwargs: object) -> object:
        os.replace(archive, moved_archive)
        archive.mkdir()
        return real_write(path, image, **kwargs)

    monkeypatch.setattr(vehicle_history_images, "_write_jpeg_atomic", swap_root_then_write)

    with pytest.raises(VehicleHistoryImageError):
        capture_occupied_images(
            archive_root=archive,
            session_id="session",
            source_frame_path=source,
            bbox=(0, 0, 8, 6),
        )

    assert list(archive.rglob("*")) == []
    assert not list(moved_archive.rglob("*.jpg"))
    assert not list(moved_archive.rglob("*.tmp"))
    assert not list(moved_archive.rglob("*.quarantine"))


def test_vehicle_transaction_rejects_crop_replacement_and_preserves_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))
    archive = tmp_path / "archive"
    full_path = archive / "images" / "occupied-full" / "session.jpg"
    crop_path = archive / "images" / "occupied-crops" / "session.jpg"
    replacement = tmp_path / "replacement.txt"
    replacement_bytes = b"unrelated crop replacement"
    replacement.write_bytes(replacement_bytes)
    real_write = vehicle_history_images._write_jpeg_atomic

    def write_then_swap(path: Path, image: Image.Image, **kwargs: object) -> object:
        result = real_write(path, image, **kwargs)
        os.replace(replacement, crop_path)
        return result

    monkeypatch.setattr(vehicle_history_images, "_write_jpeg_atomic", write_then_swap)

    with pytest.raises(VehicleHistoryImageError):
        capture_occupied_images(
            archive_root=archive,
            session_id="session",
            source_frame_path=source,
            bbox=(0, 0, 8, 6),
        )

    assert not full_path.exists()
    assert crop_path.read_bytes() == replacement_bytes
    assert not list(archive.rglob("*.tmp"))
    assert not list(archive.rglob("*.quarantine"))


def test_canonical_jpeg_prefers_reflink_without_attempting_hardlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    calls: list[str] = []

    def reflink(source_fd: int, owner: Any, temporary_name: str, source_mode: int) -> None:
        calls.append("reflink")
        write_owned_temporary(owner, temporary_name, source_fd, source_mode)

    monkeypatch.setattr(
        "parking_spot_monitor.jpeg_artifacts.os.link",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("hardlink must not be attempted")),
    )
    monkeypatch.setattr("parking_spot_monitor.jpeg_artifacts._reflink", reflink)

    publication = publish_canonical_jpeg(source, tmp_path / "archive" / "full.jpg")

    assert publication.strategy == "reflink"
    assert publication.path.read_bytes() == source.read_bytes()
    assert calls == ["reflink"]


def test_canonical_jpeg_falls_back_to_bounded_copy_and_cleans_failed_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    destination = tmp_path / "archive" / "full.jpg"
    calls: list[str] = []

    def unsupported(*args: object, **kwargs: object) -> None:
        calls.append("reflink")
        raise OSError(errno.EOPNOTSUPP, "unsupported")

    monkeypatch.setattr(
        "parking_spot_monitor.jpeg_artifacts.os.link",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("hardlink must not be attempted")),
    )
    monkeypatch.setattr("parking_spot_monitor.jpeg_artifacts._reflink", unsupported)

    publication = publish_canonical_jpeg(source, destination)

    assert publication.strategy == "copy"
    assert publication.path.read_bytes() == source.read_bytes()
    assert calls == ["reflink"]
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_jpeg_post_replace_directory_sync_failure_keeps_committed_file_without_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    destination = tmp_path / "archive" / "full.jpg"
    monkeypatch.setattr(
        "parking_spot_monitor.file_descriptor_binding.RootedDirectoryOwner.fsync",
        lambda owner: (_ for _ in ()).throw(OSError("directory sync failed")),
    )

    with pytest.raises(OSError, match="directory sync failed"):
        publish_canonical_jpeg(source, destination)

    assert destination.read_bytes() == source.read_bytes()
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_jpeg_never_publishes_source_path_replacement_after_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    expected = source.read_bytes()
    replacement = tmp_path / "replacement.png"
    Image.new("RGB", (8, 6), (200, 20, 20)).save(replacement, "PNG")
    destination = tmp_path / "archive" / "full.jpg"
    def swap_then_reflink(source_fd: int, owner: Any, temporary_name: str, source_mode: int) -> None:
        os.replace(replacement, source)
        write_owned_temporary(owner, temporary_name, source_fd, source_mode)

    monkeypatch.setattr(jpeg_artifacts, "_reflink", swap_then_reflink)

    try:
        publication = publish_canonical_jpeg(source, destination)
    except JpegDecodeError as exc:
        assert str(exc) == "read_failed"
        assert not destination.exists()
    else:
        assert publication.strategy == "reflink"
        assert destination.read_bytes() == expected
        assert destination.read_bytes() != source.read_bytes()
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_copy_fallback_never_reopens_replaced_source_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    expected = source.read_bytes()
    replacement = tmp_path / "replacement.png"
    Image.new("RGB", (8, 6), (200, 20, 20)).save(replacement, "PNG")
    original_away = tmp_path / "validated-source-away.jpg"
    destination = tmp_path / "archive" / "full.jpg"
    real_copy = jpeg_artifacts._copy_file

    def unavailable(*args: object, **kwargs: object) -> None:
        raise OSError(errno.EXDEV, "fallback required")

    def replace_path_during_copy(
        source_handle: int, owner: Any, temporary_name: str, source_mode: int, source_size: int
    ) -> None:
        os.replace(source, original_away)
        os.replace(replacement, source)
        try:
            real_copy(source_handle, owner, temporary_name, source_mode, source_size)
        finally:
            os.replace(source, replacement)
            os.replace(original_away, source)

    monkeypatch.setattr(jpeg_artifacts, "_reflink", unavailable)
    monkeypatch.setattr(jpeg_artifacts, "_copy_file", replace_path_during_copy)

    try:
        publication = publish_canonical_jpeg(source, destination)
    except JpegDecodeError as exc:
        assert str(exc) == "read_failed"
        assert not destination.exists()
    else:
        assert publication.strategy == "copy"
        assert destination.read_bytes() == expected
        assert destination.read_bytes() != replacement.read_bytes()
    assert source.read_bytes() == expected
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_jpeg_rejects_in_place_mutation_during_descriptor_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    expected = source.read_bytes()
    destination = tmp_path / "archive" / "full.jpg"
    real_validate = jpeg_artifacts._validate_jpeg_bytes

    def mutate_during_validation(payload: bytes) -> None:
        real_validate(payload)
        source.write_bytes(b"not validated")

    monkeypatch.setattr(jpeg_artifacts, "_validate_jpeg_bytes", mutate_during_validation)

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert source.read_bytes() != expected
    assert not destination.exists()


@pytest.mark.parametrize("strategy", ["reflink", "copy"])
def test_canonical_jpeg_rejects_in_place_mutation_during_fallback_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    strategy: str,
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    expected = source.read_bytes()
    destination = tmp_path / "archive" / "full.jpg"
    real_copy = jpeg_artifacts._copy_file

    def unavailable(*args: object, **kwargs: object) -> None:
        raise OSError(errno.EXDEV, "fallback required")

    def publish_then_mutate(
        source_fd: int,
        owner: Any,
        temporary_name: str,
        source_mode: int,
        source_signature: tuple[int, int, int, int, int] | None = None,
    ) -> None:
        if strategy == "reflink":
            write_owned_temporary(owner, temporary_name, source_fd, source_mode)
        else:
            assert source_signature is not None
            real_copy(source_fd, owner, temporary_name, source_mode, source_signature)
        source.write_bytes(b"changed during publication")

    monkeypatch.setattr(jpeg_artifacts, "_reflink", publish_then_mutate if strategy == "reflink" else unavailable)
    if strategy == "copy":
        monkeypatch.setattr(jpeg_artifacts, "_copy_file", publish_then_mutate)

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert source.read_bytes() != expected
    assert not destination.exists()
    assert list(destination.parent.glob(".*.tmp")) == []


def test_attach_occupied_images_writes_full_frame_and_clamped_crop_then_close_preserves_refs(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="image spot"))
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))
    source_bytes = source.read_bytes()

    updated = archive.attach_occupied_images(
        session_id=active.session_id,
        source_frame_path=source,
        bbox=(-2.2, 1.2, 5.1, 20.9),
    )

    assert updated.occupied_snapshot_path is not None
    assert updated.occupied_crop_path is not None
    full_path = Path(updated.occupied_snapshot_path)
    crop_path = Path(updated.occupied_crop_path)
    assert full_path.exists()
    assert crop_path.exists()
    assert full_path != crop_path
    assert full_path.name == f"{active.session_id}.jpg"
    assert crop_path.name == f"{active.session_id}.jpg"
    assert stat.S_IMODE(full_path.stat().st_mode) == stat.S_IMODE(source.stat().st_mode)
    assert stat.S_IMODE(crop_path.stat().st_mode) == 0o644
    assert full_path.read_bytes() == source_bytes
    assert full_path.stat().st_ino != source.stat().st_ino
    with Image.open(full_path) as full_frame:
        assert full_frame.size == (8, 6)
        assert full_frame.format == "JPEG"
    with Image.open(crop_path) as crop:
        assert crop.size == (6, 5)
        assert crop.format == "JPEG"
        assert all(abs(actual - expected) <= 3 for actual, expected in zip(crop.getpixel((2, 2)), (10, 80, 140)))

    active_path = tmp_path / "vehicle-history" / "sessions" / "active" / f"{active.session_id}.json"
    raw_active = json.loads(active_path.read_text())
    assert raw_active["occupied_snapshot_path"] == str(full_path)
    assert raw_active["occupied_crop_path"] == str(crop_path)

    snapshot = archive.health_snapshot()
    assert snapshot["occupied_snapshot_count"] == 1
    assert snapshot["occupied_crop_count"] == 1
    assert snapshot["image_file_count"] == 2
    assert snapshot["image_bytes"] == full_path.stat().st_size + crop_path.stat().st_size
    assert snapshot["missing_occupied_image_reference_count"] == 0

    closed = archive.close_session(open_event(spot_id="image spot"))

    assert closed is not None
    assert closed.occupied_snapshot_path == str(full_path)
    assert closed.occupied_crop_path == str(crop_path)
    raw_closed = json.loads((tmp_path / "vehicle-history" / "sessions" / "closed" / f"{active.session_id}.json").read_text())
    assert raw_closed["occupied_snapshot_path"] == str(full_path)
    assert raw_closed["occupied_crop_path"] == str(crop_path)
    records = logger_records(stream)
    image_records = [record for record in records if record["event"].startswith("vehicle-session-images")]
    assert image_records == [
        {
            "crop_path_name": f"{active.session_id}.jpg",
            "event": "vehicle-session-images-captured",
            "full_path_name": f"{active.session_id}.jpg",
            "level": "INFO",
            "session_id": active.session_id,
            "spot_id": "image spot",
        }
    ]


def test_attach_occupied_images_is_idempotent_and_does_not_overwrite_existing_files(tmp_path: Path) -> None:
    stream = StringIO()
    archive = VehicleHistoryArchive(tmp_path, logger=setup_logging(stream=stream))
    active = archive.start_session(occupied_event(spot_id="duplicate image"))
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6), color=(20, 30, 40))
    first = archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=(1, 1, 6, 5))
    assert first.occupied_snapshot_path is not None
    assert first.occupied_crop_path is not None
    full_path = Path(first.occupied_snapshot_path)
    crop_path = Path(first.occupied_crop_path)
    full_before = full_path.read_bytes()
    crop_before = crop_path.read_bytes()
    source.unlink()

    second = archive.attach_occupied_images(session_id=active.session_id, source_frame_path=source, bbox=(0, 0, 8, 6))

    assert second == first
    assert full_path.read_bytes() == full_before
    assert crop_path.read_bytes() == crop_before
    records = logger_records(stream)
    noop = [record for record in records if record["event"] == "vehicle-session-images-noop"]
    assert noop == [
        {
            "crop_path_name": crop_path.name,
            "event": "vehicle-session-images-noop",
            "full_path_name": full_path.name,
            "level": "INFO",
            "reason": "already-attached",
            "session_id": active.session_id,
            "spot_id": "duplicate image",
        }
    ]
