from __future__ import annotations

from tests.support._vehicle_history import *  # noqa: F403


def test_canonical_jpeg_prefers_reflink_without_reencoding_or_source_mode_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    source.chmod(0o600)
    source_inode = source.stat().st_ino
    original_chmod = os.chmod
    original_fchmod = os.fchmod

    def deterministic_reflink(source_fd: int, owner: Any, temporary_name: str, source_mode: int) -> None:
        write_owned_temporary(owner, temporary_name, source_fd, source_mode)

    def reject_shared_chmod(path: str | os.PathLike[str], mode: int, *args: object, **kwargs: object) -> None:
        if Path(path).exists() and Path(path).stat().st_ino == source_inode:
            raise AssertionError("must not chmod the source inode")
        original_chmod(path, mode, *args, **kwargs)

    def reject_shared_fchmod(fd: int, mode: int) -> None:
        if os.fstat(fd).st_ino == source_inode:
            raise AssertionError("must not fchmod the source inode")
        original_fchmod(fd, mode)

    monkeypatch.setattr(os, "chmod", reject_shared_chmod)
    monkeypatch.setattr(os, "fchmod", reject_shared_fchmod)
    monkeypatch.setattr(jpeg_artifacts, "_reflink", deterministic_reflink)

    publication = publish_canonical_jpeg(source, tmp_path / "archive" / "full.jpg")

    assert publication.strategy == "reflink"
    assert publication.path.read_bytes() == source.read_bytes()
    assert publication.path.stat().st_ino != source_inode
    assert stat.S_IMODE(source.stat().st_mode) == 0o600
    assert stat.S_IMODE(publication.path.stat().st_mode) == 0o600


def test_canonical_jpeg_default_publication_is_independent_from_writable_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    validated = source.read_bytes()
    destination = tmp_path / "archive" / "full.jpg"

    def deterministic_reflink(source_fd: int, owner: Any, temporary_name: str, source_mode: int) -> None:
        write_owned_temporary(owner, temporary_name, source_fd, source_mode)

    monkeypatch.setattr(jpeg_artifacts, "_reflink", deterministic_reflink)

    publication = publish_canonical_jpeg(source, destination)
    source.write_bytes(b"changed after successful publication")

    assert publication.strategy == "reflink"
    assert destination.read_bytes() == validated
    assert destination.stat().st_ino != source.stat().st_ino


def test_canonical_jpeg_detects_same_length_restore_with_spoofed_mtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    expected = source.read_bytes()
    original_mtime = source.stat().st_mtime_ns
    destination = tmp_path / "archive" / "full.jpg"
    real_signature = jpeg_artifacts._descriptor_signature
    source_descriptor: int | None = None
    mutation_observed = False

    def mutate_restore_and_spoof(source_fd: int, owner: Any, temporary_name: str, source_mode: int) -> None:
        nonlocal mutation_observed, source_descriptor
        source_descriptor = source_fd
        write_owned_temporary(owner, temporary_name, source_fd, source_mode)
        source.write_bytes(b"x" * len(expected))
        source.write_bytes(expected)
        os.utime(source, ns=(original_mtime, original_mtime))
        mutation_observed = True

    def signature_with_observed_ctime(descriptor: int) -> tuple[int, int, int, int, int]:
        signature = real_signature(descriptor)
        assert len(signature) == 5
        assert signature[-1] == os.fstat(descriptor).st_ctime_ns
        if mutation_observed and descriptor == source_descriptor:
            return (*signature[:-1], signature[-1] + 1)
        return signature

    monkeypatch.setattr(jpeg_artifacts, "_reflink", mutate_restore_and_spoof)
    monkeypatch.setattr(jpeg_artifacts, "_descriptor_signature", signature_with_observed_ctime)

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert not destination.exists()
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_jpeg_rechecks_integrity_after_temporary_fsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    expected_size = source.stat().st_size
    destination = tmp_path / "archive" / "full.jpg"
    real_fsync = os.fsync

    def mutate_after_fsync(descriptor: int) -> None:
        real_fsync(descriptor)
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            Path(f"/proc/self/fd/{descriptor}").write_bytes(b"z" * expected_size)

    monkeypatch.setattr(jpeg_artifacts.os, "fsync", mutate_after_fsync)

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert not destination.exists()
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_jpeg_rejects_oversized_source_before_creating_destination(tmp_path: Path) -> None:
    source = write_test_jpeg(tmp_path / "padded.jpg")
    with source.open("r+b") as handle:
        handle.seek(32 * 1024 * 1024)
        handle.write(b"x")
    destination = tmp_path / "archive" / "full.jpg"

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert not destination.parent.exists()


def test_canonical_jpeg_accepts_exact_32_mib_valid_source(tmp_path: Path) -> None:
    source = write_test_jpeg(tmp_path / "exact-limit.jpg")
    with source.open("ab") as handle:
        handle.truncate(32 * 1024 * 1024)
    destination = tmp_path / "archive" / "full.jpg"

    publication = publish_canonical_jpeg(source, destination)

    assert publication.path.stat().st_size == 32 * 1024 * 1024


def test_canonical_validation_gives_pillow_only_captured_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "source.jpg")
    preflight_size = source.stat().st_size
    destination = tmp_path / "archive" / "full.jpg"
    real_open = jpeg_artifacts.Image.open
    opened_buffers = 0

    def bounded_open(payload: object, *args: object, **kwargs: object) -> Image.Image:
        nonlocal opened_buffers
        assert isinstance(payload, BytesIO)
        offset = payload.tell()
        payload.seek(0, os.SEEK_END)
        assert payload.tell() == preflight_size
        payload.seek(offset)
        opened_buffers += 1
        return real_open(payload, *args, **kwargs)

    monkeypatch.setattr(jpeg_artifacts.Image, "open", bounded_open)

    publish_canonical_jpeg(source, destination)

    assert opened_buffers >= 1


def test_canonical_growth_rejection_reads_at_most_preflight_size_plus_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "growing.jpg")
    preflight_size = source.stat().st_size
    source_identity = (source.stat().st_dev, source.stat().st_ino)
    destination = tmp_path / "archive" / "full.jpg"
    growth_limit = 40 * 1024 * 1024
    real_read = os.read
    consumed = 0

    def inject_40_mib_growth(descriptor: int, size: int) -> bytes:
        nonlocal consumed
        chunk = real_read(descriptor, size)
        value = os.fstat(descriptor)
        if (value.st_dev, value.st_ino) == source_identity:
            consumed += len(chunk)
            if chunk and source.stat().st_size < growth_limit:
                with source.open("r+b") as handle:
                    handle.truncate(growth_limit)
        return chunk

    monkeypatch.setattr(jpeg_artifacts.os, "read", inject_40_mib_growth)

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert consumed <= preflight_size + 1
    assert source.stat().st_size == growth_limit
    assert not destination.exists()


def test_canonical_copy_growth_never_writes_beyond_preflight_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    source_size = source.stat().st_size
    source_identity = (source.stat().st_dev, source.stat().st_ino)
    destination = tmp_path / "archive" / "full.jpg"
    real_copy = jpeg_artifacts._copy_file
    real_read = os.read
    real_unlink = file_descriptor_binding.RootedDirectoryOwner.unlink_if_matches
    discarded_sizes: list[int] = []
    appended = False

    def unsupported_reflink(*args: object) -> None:
        raise OSError(errno.EOPNOTSUPP, "unsupported")

    def append_during_copy(*args: object, **kwargs: object) -> None:
        def growing_read(descriptor: int, size: int) -> bytes:
            nonlocal appended
            chunk = real_read(descriptor, size)
            value = os.fstat(descriptor)
            if chunk and not appended and (value.st_dev, value.st_ino) == source_identity:
                appended = True
                with source.open("ab") as handle:
                    handle.write(b"x" * 4096)
            return chunk

        monkeypatch.setattr(jpeg_artifacts.os, "read", growing_read)
        real_copy(*args, **kwargs)

    def record_discarded_size(
        owner: file_descriptor_binding.RootedDirectoryOwner,
        name: str,
        identity: file_descriptor_binding.FileIdentity,
    ) -> bool:
        if name.endswith(".tmp"):
            discarded_sizes.append(os.stat(name, dir_fd=owner.fd, follow_symlinks=False).st_size)
        return real_unlink(owner, name, identity)

    monkeypatch.setattr(jpeg_artifacts, "_reflink", unsupported_reflink)
    monkeypatch.setattr(jpeg_artifacts, "_copy_file", append_during_copy)
    monkeypatch.setattr(file_descriptor_binding.RootedDirectoryOwner, "unlink_if_matches", record_discarded_size)

    publication = publish_canonical_jpeg(source, destination)

    assert publication.strategy == "copy"
    assert appended is False
    assert discarded_sizes == []
    assert destination.stat().st_size == source_size
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_jpeg_rejects_validated_temporary_swap_before_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    destination = tmp_path / "archive" / "full.jpg"
    validated_away = tmp_path / "validated-away.jpg"
    unvalidated = b"arbitrary unvalidated bytes"
    real_replace = os.replace
    swapped = False

    def swap_before_replace(
        source_path: str | bytes | os.PathLike[str],
        destination_path: str | bytes | os.PathLike[str],
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal swapped
        if destination_path == destination.name and kwargs.get("dst_dir_fd") is not None and not swapped:
            swapped = True
            directory_fd = int(kwargs["src_dir_fd"])
            real_replace(source_path, validated_away, src_dir_fd=directory_fd)
            descriptor = os.open(source_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=directory_fd)
            os.write(descriptor, unvalidated)
            os.close(descriptor)
        real_replace(source_path, destination_path, *args, **kwargs)

    monkeypatch.setattr(jpeg_artifacts.os, "replace", swap_before_replace)

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert swapped is True
    assert destination.read_bytes() == unvalidated
    assert validated_away.read_bytes() == source.read_bytes()
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_jpeg_preserves_swapped_temporary_symlink_without_touching_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    destination = tmp_path / "archive" / "full.jpg"
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"unrelated target")
    real_replace = os.replace
    swapped = False

    def swap_to_symlink(
        source_path: str | bytes | os.PathLike[str],
        destination_path: str | bytes | os.PathLike[str],
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal swapped
        if destination_path == destination.name and kwargs.get("dst_dir_fd") is not None and not swapped:
            swapped = True
            directory_fd = int(kwargs["src_dir_fd"])
            real_replace(source_path, tmp_path / "validated-away.jpg", src_dir_fd=directory_fd)
            os.symlink(outside, source_path, dir_fd=directory_fd)
        real_replace(source_path, destination_path, *args, **kwargs)

    monkeypatch.setattr(jpeg_artifacts.os, "replace", swap_to_symlink)

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert swapped is True
    assert os.path.islink(destination)
    assert outside.read_bytes() == b"unrelated target"


def test_canonical_jpeg_mismatch_cleanup_preserves_concurrent_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    destination = tmp_path / "archive" / "full.jpg"
    unrelated = tmp_path / "unrelated.txt"
    unrelated_bytes = b"concurrent unrelated destination"
    unrelated.write_bytes(unrelated_bytes)
    real_validate = jpeg_artifacts._validate_artifact_descriptor
    validations = 0

    def replace_destination_before_failure(descriptor: int, source_fd: int, evidence: object) -> None:
        nonlocal validations
        validations += 1
        real_validate(descriptor, source_fd, evidence)
        if validations == 1:
            os.replace(unrelated, destination)
            raise JpegDecodeError("read_failed")

    monkeypatch.setattr(jpeg_artifacts, "_validate_artifact_descriptor", replace_destination_before_failure)

    with pytest.raises(JpegDecodeError, match="read_failed"):
        publish_canonical_jpeg(source, destination)

    assert destination.read_bytes() == unrelated_bytes
    assert list(destination.parent.glob(".*.tmp")) == []


def test_canonical_jpeg_rejects_symlinked_destination_parent_before_writing(
    tmp_path: Path,
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    outside = tmp_path / "outside"
    outside.mkdir()
    archive_link = tmp_path / "archive"
    archive_link.symlink_to(outside, target_is_directory=True)

    with pytest.raises(OSError):
        publish_canonical_jpeg(source, archive_link / "full.jpg")

    assert not (outside / "full.jpg").exists()
    assert list(outside.glob(".*.tmp")) == []


def test_canonical_jpeg_rejects_intermediate_symlink_ancestor_before_writing(tmp_path: Path) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "archive-link"
    linked.symlink_to(outside, target_is_directory=True)

    with pytest.raises(OSError):
        publish_canonical_jpeg(source, linked / "nested" / "full.jpg")

    assert not (outside / "nested").exists()


def test_canonical_jpeg_parent_swap_cleans_held_directory_without_touching_replacement_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    destination = tmp_path / "archive" / "nested" / "full.jpg"
    outside = tmp_path / "outside"
    (outside / "nested").mkdir(parents=True)
    moved_parent = tmp_path / "archive-held"
    real_reflink = jpeg_artifacts._reflink

    def swap_parent(source_fd: int, owner: object, temporary_name: str, source_mode: int) -> None:
        os.replace(tmp_path / "archive", moved_parent)
        (tmp_path / "archive").symlink_to(outside, target_is_directory=True)
        real_reflink(source_fd, owner, temporary_name, source_mode)

    monkeypatch.setattr(jpeg_artifacts, "_reflink", swap_parent)

    with pytest.raises((JpegDecodeError, OSError)):
        publish_canonical_jpeg(source, destination)

    assert not (outside / "nested" / "full.jpg").exists()
    assert not (moved_parent / "nested" / "full.jpg").exists()
    assert list((moved_parent / "nested").glob(".*.tmp")) == []


@pytest.mark.parametrize("relative", [False, True])
def test_canonical_jpeg_nested_publication_reports_committed_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, relative: bool
) -> None:
    source = write_test_jpeg(tmp_path / "latest.jpg")
    absolute_destination = tmp_path / "archive" / "nested" / "full.jpg"
    absolute_destination.parent.mkdir(parents=True)
    absolute_destination.write_bytes(b"preexisting destination")
    if relative:
        monkeypatch.chdir(tmp_path)
        source_argument: Path = Path("latest.jpg")
        destination_argument = Path("archive/nested/full.jpg")
    else:
        source_argument = source
        destination_argument = absolute_destination

    publication = publish_canonical_jpeg(source_argument, destination_argument)
    committed = absolute_destination.stat()

    assert publication.path == destination_argument
    assert (publication.identity.dev, publication.identity.ino) == (committed.st_dev, committed.st_ino)
    assert absolute_destination.read_bytes() == source.read_bytes()


def test_vehicle_image_failure_preserves_replacement_after_canonical_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6))
    full_path = tmp_path / "archive" / "images" / "occupied-full" / "session.jpg"
    replacement = tmp_path / "replacement.txt"
    replacement_bytes = b"unrelated replacement"
    replacement.write_bytes(replacement_bytes)

    def replace_then_fail(*args: object, **kwargs: object) -> ClampedCropBox:
        os.replace(replacement, full_path)
        raise VehicleHistoryImageError("crop failed")

    monkeypatch.setattr("parking_spot_monitor.vehicle_history_images.clamp_crop_box", replace_then_fail)

    with pytest.raises(VehicleHistoryImageError, match="crop failed"):
        capture_occupied_images(
            archive_root=tmp_path / "archive",
            session_id="session",
            source_frame_path=source,
            bbox=(0, 0, 8, 6),
        )

    assert full_path.read_bytes() == replacement_bytes


def test_vehicle_crop_rejects_replacement_of_published_full_frame(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = write_test_jpeg(tmp_path / "source.jpg", size=(8, 6), color=(10, 20, 30))
    replacement = write_test_jpeg(tmp_path / "replacement.jpg", size=(8, 6), color=(200, 10, 5))
    replacement_bytes = replacement.read_bytes()
    full_path = tmp_path / "archive" / "images" / "occupied-full" / "session.jpg"
    crop_path = tmp_path / "archive" / "images" / "occupied-crops" / "session.jpg"
    real_open = vehicle_history_images.open_owned_at

    def swap_then_open(owner: object, name: str, identity: object) -> object:
        os.replace(replacement, full_path)
        return real_open(owner, name, identity)

    monkeypatch.setattr(vehicle_history_images, "open_owned_at", swap_then_open)

    with pytest.raises(VehicleHistoryImageError):
        capture_occupied_images(
            archive_root=tmp_path / "archive",
            session_id="session",
            source_frame_path=source,
            bbox=(0, 0, 8, 6),
        )

    assert full_path.read_bytes() == replacement_bytes
    assert not crop_path.exists()
