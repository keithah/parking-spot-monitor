from __future__ import annotations

from tests.support._vehicle_history import *  # noqa: F403


def test_owned_path_cleanup_quarantines_before_identity_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owned = tmp_path / "owned.jpg"
    owned.write_bytes(b"owned bytes")
    identity = file_descriptor_binding.FileIdentity.from_stat(owned.stat())
    replacement = tmp_path / "replacement.txt"
    replacement_bytes = b"unrelated replacement"
    replacement.write_bytes(replacement_bytes)
    real_unlink, real_rename, real_replace = os.unlink, os.rename, os.replace
    swapped = False

    def swap_target() -> None:
        nonlocal swapped
        if not swapped:
            swapped = True
            real_replace(replacement, owned)

    def swapping_unlink(path: object, *args: object, **kwargs: object) -> None:
        if path == owned.name and kwargs.get("dir_fd") is not None:
            swap_target()
        real_unlink(path, *args, **kwargs)

    def swapping_rename(source: object, destination: object, *args: object, **kwargs: object) -> None:
        if source == owned.name and kwargs.get("src_dir_fd") is not None:
            swap_target()
        real_rename(source, destination, *args, **kwargs)

    monkeypatch.setattr(file_descriptor_binding.os, "unlink", swapping_unlink)
    monkeypatch.setattr(file_descriptor_binding.os, "rename", swapping_rename)

    assert file_descriptor_binding.unlink_owned_path(owned, identity) is False
    assert swapped is True
    assert owned.read_bytes() == replacement_bytes
    assert list(tmp_path.glob(".*.quarantine")) == []


def test_owned_cleanup_leaves_stable_mismatched_directory_untouched(tmp_path: Path) -> None:
    expected_file = tmp_path / "expected.jpg"
    expected_file.write_bytes(b"expected")
    expected = file_descriptor_binding.FileIdentity.from_stat(expected_file.stat())
    target = tmp_path / "target.jpg"
    target.mkdir()

    assert file_descriptor_binding.unlink_owned_path(target, expected) is False

    assert target.is_dir()
    assert list(tmp_path.glob(".*.quarantine")) == []


def test_owned_cleanup_recovers_quarantined_mismatch_after_name_blocker_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    expected = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    replacement = tmp_path / "replacement.txt"
    replacement_bytes = b"replacement caught by quarantine"
    replacement.write_bytes(replacement_bytes)
    blocker_bytes = b"new original-name blocker"
    real_rename, real_replace = os.rename, os.replace
    injected = False

    def swap_then_block(source: object, quarantine: object, *args: object, **kwargs: object) -> None:
        nonlocal injected
        if source == target.name and kwargs.get("src_dir_fd") is not None and not injected:
            injected = True
            real_replace(replacement, target)
            real_rename(source, quarantine, *args, **kwargs)
            target.write_bytes(blocker_bytes)
            return
        real_rename(source, quarantine, *args, **kwargs)

    monkeypatch.setattr(file_descriptor_binding.os, "rename", swap_then_block)

    assert file_descriptor_binding.unlink_owned_path(target, expected) is False
    assert target.read_bytes() == blocker_bytes
    quarantines = list(tmp_path.glob(".*.quarantine"))
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == replacement_bytes
    target.unlink()
    assert file_descriptor_binding.unlink_owned_path(target, expected) is False
    assert target.read_bytes() == replacement_bytes
    assert list(tmp_path.glob(".*.quarantine")) == []


def test_owned_cleanup_never_unlinks_the_checked_quarantine_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    expected = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    unrelated = tmp_path / "unrelated.txt"
    unrelated.write_bytes(b"unrelated")
    owned_away = tmp_path / "owned-away.txt"
    real_unlink, real_replace = os.unlink, os.replace
    swapped = False

    def swap_at_old_unlink(path: object, *args: object, **kwargs: object) -> None:
        nonlocal swapped
        if str(path).endswith(".quarantine") and kwargs.get("dir_fd") is not None:
            swapped = True
            directory_fd = int(kwargs["dir_fd"])
            quarantine_path = Path(f"/proc/self/fd/{directory_fd}") / str(path)
            os.replace(quarantine_path, owned_away)
            real_replace(unrelated, quarantine_path)
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(owned_file_cleanup.os, "unlink", swap_at_old_unlink)

    assert file_descriptor_binding.unlink_owned_path(target, expected) is True

    assert swapped is False
    assert unrelated.read_bytes() == b"unrelated"
    assert not target.exists()


def test_owned_cleanup_preserves_disposal_transition_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    expected = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    unrelated = tmp_path / "unrelated.txt"
    unrelated.write_bytes(b"unrelated")
    owned_away = tmp_path / "owned-away.txt"
    real_rename, real_replace = os.rename, os.replace
    transitioned = False

    def swap_disposal(source: object, destination: object, *args: object, **kwargs: object) -> None:
        nonlocal transitioned
        real_rename(source, destination, *args, **kwargs)
        if str(destination).endswith(".dispose") and not transitioned:
            transitioned = True
            directory_fd = int(kwargs["dst_dir_fd"])
            disposal_path = Path(f"/proc/self/fd/{directory_fd}") / str(destination)
            os.replace(disposal_path, owned_away)
            real_replace(unrelated, disposal_path)

    monkeypatch.setattr(owned_file_cleanup.os, "rename", swap_disposal)

    assert file_descriptor_binding.unlink_owned_path(target, expected) is False

    assert transitioned is True
    assert owned_away.read_bytes() == b"owned"
    assert target.read_bytes() == b"unrelated"
    assert not list(tmp_path.glob("*.dispose.*"))


@pytest.mark.parametrize("max_entries", [0, 1, 256])
def test_owned_cleanup_recovery_consumes_at_most_scan_cap_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, max_entries: int
) -> None:
    for index in range(300):
        (tmp_path / f"unrelated-{index:03d}").write_bytes(b"x")
    directory_fd = os.open(tmp_path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    real_scandir = os.scandir
    next_calls = 0

    class CountingScandir:
        def __init__(self, descriptor: int) -> None:
            self._entries = real_scandir(descriptor)

        def __enter__(self) -> CountingScandir:
            return self

        def __exit__(self, *args: object) -> None:
            self._entries.close()

        def __iter__(self) -> CountingScandir:
            return self

        def __next__(self) -> os.DirEntry[str]:
            nonlocal next_calls
            next_calls += 1
            return next(self._entries)

    monkeypatch.setattr(owned_file_cleanup.os, "scandir", CountingScandir)
    try:
        assert owned_file_cleanup.recover_quarantined_at(
            directory_fd, "owned.jpg", max_entries=max_entries
        ) == 0
    finally:
        os.close(directory_fd)

    assert next_calls == max_entries


def test_owned_cleanup_recovers_interrupted_exact_disposal(tmp_path: Path) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    disposal = tmp_path / ".owned.jpg.0123456789abcdef.dispose"
    os.rename(target, disposal)

    assert file_descriptor_binding.recover_quarantined_path(target) == 1

    assert target.read_bytes() == b"owned"
    assert not disposal.exists()


def test_owned_cleanup_persistent_disposal_failure_stays_bounded_then_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    expected = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    real_unlink = os.unlink
    fail_disposal = True

    def fail_disposal_unlink(path: object, *args: object, **kwargs: object) -> None:
        if fail_disposal and (str(path).endswith(".dispose") or ".dispose." in str(path)):
            raise OSError("persistent disposal unlink failure")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", fail_disposal_unlink)

    for _ in range(3):
        assert file_descriptor_binding.unlink_owned_path(target, expected) is False
        quarantines = list(tmp_path.glob(".*.quarantine"))
        disposals = [path for path in tmp_path.iterdir() if path.name.endswith(".dispose") or ".dispose." in path.name]
        assert quarantines == []
        assert len(disposals) == 1
        matching = [path for path in (target, disposals[0]) if path.exists() and path.stat().st_ino == expected.ino]
        assert 1 <= len(matching) <= 2
        assert all(path.read_bytes() == b"owned" for path in matching)

    fail_disposal = False
    assert file_descriptor_binding.recover_quarantined_path(target) == 1

    assert target.read_bytes() == b"owned"
    assert not list(tmp_path.glob(".*.quarantine"))
    assert not [path for path in tmp_path.iterdir() if path.name.endswith(".dispose") or ".dispose." in path.name]


def test_owned_directory_manifest_recovers_pending_artifact_behind_decoys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    for index in range(300):
        (tmp_path / f"decoy-{index:03d}").write_bytes(b"x")
    real_unlink = owned_file_disposal.os.unlink

    def interrupt_disposal(name: object, *args: object, **kwargs: object) -> None:
        if str(name).endswith(".dispose"):
            raise OSError("simulated crash")
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", interrupt_disposal)
    assert file_descriptor_binding.unlink_owned_path(target, identity) is False
    assert not target.exists()
    assert (tmp_path / ".owned-disposals.json").exists()

    monkeypatch.setattr(owned_file_disposal.os, "unlink", real_unlink)
    with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as restarted_owner:
        result = restarted_owner.recover_owned()

    assert result.recovered == 1
    assert result.pending is False
    assert target.read_bytes() == b"owned"
    assert not list(tmp_path.glob("*.dispose"))


def test_owned_directory_manifest_rejects_replaced_disposal_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    real_unlink = owned_file_disposal.os.unlink

    def interrupt_disposal(name: object, *args: object, **kwargs: object) -> None:
        if str(name).endswith(".dispose"):
            raise OSError("simulated crash")
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", interrupt_disposal)
    assert file_descriptor_binding.unlink_owned_path(target, identity) is False
    disposal = next(path for path in tmp_path.iterdir() if path.name.endswith(".dispose"))
    moved_owned = tmp_path / "moved-owned.jpg"
    disposal.replace(moved_owned)
    disposal.write_bytes(b"unrelated")

    monkeypatch.setattr(owned_file_disposal.os, "unlink", real_unlink)
    with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as restarted_owner:
        result = restarted_owner.recover_owned()

    assert result.pending is True
    assert not target.exists()
    assert moved_owned.read_bytes() == b"owned"
    assert disposal.read_bytes() == b"unrelated"


def test_owned_disposal_manifest_serializes_record_rename_and_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    recorded = threading.Event()
    release_producer = threading.Event()
    recovery_started = threading.Event()
    recovery_finished = threading.Event()
    producer_result: list[bool] = []
    recovery_result: list[owned_file_recovery.RecoveryResult] = []
    real_record = owned_file_disposal.record_disposal_at
    real_unlink = owned_file_disposal.os.unlink

    def pause_after_record(*args: object, **kwargs: object) -> bool:
        result = real_record(*args, **kwargs)
        if threading.current_thread().name == "disposal-producer":
            recorded.set()
            assert release_producer.wait(2)
        return result

    def interrupt_producer_unlink(name: object, *args: object, **kwargs: object) -> None:
        if threading.current_thread().name == "disposal-producer" and str(name).endswith(".dispose"):
            raise OSError("simulated crash")
        real_unlink(name, *args, **kwargs)

    def produce() -> None:
        producer_result.append(file_descriptor_binding.unlink_owned_path(target, identity))

    def recover() -> None:
        recovery_started.set()
        with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as owner:
            recovery_result.append(owner.recover_owned())
        recovery_finished.set()

    monkeypatch.setattr(owned_file_disposal, "record_disposal_at", pause_after_record)
    monkeypatch.setattr(owned_file_disposal.os, "unlink", interrupt_producer_unlink)
    producer = threading.Thread(target=produce, name="disposal-producer", daemon=True)
    producer.start()
    assert recorded.wait(2)
    recovery = threading.Thread(target=recover, name="disposal-recovery", daemon=True)
    recovery.start()
    assert recovery_started.wait(2)
    assert not recovery_finished.wait(0.1)
    release_producer.set()
    producer.join(2)
    recovery.join(2)

    assert not producer.is_alive()
    assert not recovery.is_alive()
    assert producer_result == [False]
    assert len(recovery_result) == 1
    assert recovery_result[0].recovered == 1
    assert target.read_bytes() == b"owned"
    with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as owner:
        assert owned_disposal_manifest.manifest_entries_at(owner.fd) == []


def test_owned_disposal_manifest_transient_stat_error_preserves_pending_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    real_unlink = owned_file_disposal.os.unlink

    def interrupt_disposal(name: object, *args: object, **kwargs: object) -> None:
        if str(name).endswith(".dispose"):
            raise OSError("simulated crash")
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", interrupt_disposal)
    assert file_descriptor_binding.unlink_owned_path(target, identity) is False
    monkeypatch.setattr(owned_file_disposal.os, "unlink", real_unlink)
    disposal = next(path.name for path in tmp_path.iterdir() if path.name.endswith(".dispose"))
    real_stat = owned_file_recovery.os.stat

    def fail_disposal_stat(name: object, *args: object, **kwargs: object) -> os.stat_result:
        if name == disposal:
            raise OSError(errno.EIO, "simulated storage error")
        return real_stat(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_recovery.os, "stat", fail_disposal_stat)
    with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as owner:
        result = owner.recover_owned()
        entries = owned_disposal_manifest.manifest_entries_at(owner.fd)

    assert result.pending is True
    assert [entry.disposal for entry in entries] == [disposal]


def test_owned_disposal_first_bind_eio_retains_manifest_until_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    for index in range(300):
        (tmp_path / f"decoy-{index:03d}").write_bytes(b"x")
    real_open = owned_file_disposal.os.open
    fail_bind = True

    def fail_disposal_bind(name: object, *args: object, **kwargs: object) -> int:
        if fail_bind and str(name).endswith(".dispose"):
            raise OSError(errno.EIO, "simulated bind error")
        return real_open(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "open", fail_disposal_bind)
    assert file_descriptor_binding.unlink_owned_path(target, identity) is False
    disposal = next(path for path in tmp_path.iterdir() if path.name.endswith(".dispose"))
    with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as owner:
        entries = owned_disposal_manifest.manifest_entries_at(owner.fd)
    assert not target.exists()
    assert [entry.disposal for entry in entries] == [disposal.name]

    fail_bind = False
    with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as restarted_owner:
        result = restarted_owner.recover_owned()
        entries = owned_disposal_manifest.manifest_entries_at(restarted_owner.fd)

    assert result.recovered == 1
    assert result.pending is False
    assert target.read_bytes() == b"owned"
    assert not disposal.exists()
    assert entries == []


def test_owned_disposal_same_identity_eio_retains_manifest_until_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    for index in range(300):
        (tmp_path / f"decoy-{index:03d}").write_bytes(b"x")
    real_stat = owned_file_disposal.os.stat
    fail_identity_check = True

    def fail_existing_disposal_stat(name: object, *args: object, **kwargs: object) -> os.stat_result:
        value = real_stat(name, *args, **kwargs)
        if fail_identity_check and str(name).endswith(".dispose"):
            raise OSError(errno.EIO, "simulated identity check error")
        return value

    monkeypatch.setattr(owned_file_disposal.os, "stat", fail_existing_disposal_stat)
    assert file_descriptor_binding.unlink_owned_path(target, identity) is False
    disposal = next(path for path in tmp_path.iterdir() if path.name.endswith(".dispose"))
    with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as owner:
        entries = owned_disposal_manifest.manifest_entries_at(owner.fd)
    assert not target.exists()
    assert [entry.disposal for entry in entries] == [disposal.name]

    fail_identity_check = False
    with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as restarted_owner:
        result = restarted_owner.recover_owned()
        entries = owned_disposal_manifest.manifest_entries_at(restarted_owner.fd)

    assert result.recovered == 1
    assert result.pending is False
    assert target.read_bytes() == b"owned"
    assert not disposal.exists()
    assert entries == []


def test_owned_disposal_manifest_transaction_is_reentrant(tmp_path: Path) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
    entry = owned_disposal_manifest.DisposalManifestEntry(
        ".owned.jpg.0123456789abcdef.dispose", target.name, identity.dev, identity.ino
    )
    finished = threading.Event()
    failures: list[BaseException] = []

    def nested_transaction() -> None:
        try:
            with file_descriptor_binding.RootedDirectoryOwner(tmp_path, create=False) as owner:
                with owned_disposal_manifest.disposal_manifest_transaction(owner.fd):
                    assert owned_disposal_manifest.record_disposal_at(owner.fd, entry)
                    assert owned_disposal_manifest.manifest_entries_at(owner.fd) == [entry]
                    assert owned_disposal_manifest.forget_disposal_at(owner.fd, entry.disposal)
        except Exception as exc:
            failures.append(exc)
        finally:
            finished.set()

    worker = threading.Thread(target=nested_transaction, daemon=True)
    worker.start()
    assert finished.wait(2), "nested manifest transaction deadlocked"
    assert failures == []


def test_owned_disposal_transactions_do_not_block_unrelated_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    first = first_dir / "first.jpg"
    second = second_dir / "second.jpg"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    first_identity = file_descriptor_binding.FileIdentity.from_stat(first.stat())
    second_identity = file_descriptor_binding.FileIdentity.from_stat(second.stat())
    first_recorded = threading.Event()
    release_first = threading.Event()
    second_finished = threading.Event()
    real_record = owned_file_disposal.record_disposal_at

    def pause_first(*args: object, **kwargs: object) -> bool:
        result = real_record(*args, **kwargs)
        if threading.current_thread().name == "first-disposal":
            first_recorded.set()
            assert release_first.wait(2)
        return result

    monkeypatch.setattr(owned_file_disposal, "record_disposal_at", pause_first)
    first_thread = threading.Thread(
        target=lambda: file_descriptor_binding.unlink_owned_path(first, first_identity),
        name="first-disposal",
        daemon=True,
    )
    second_thread = threading.Thread(
        target=lambda: (
            file_descriptor_binding.unlink_owned_path(second, second_identity),
            second_finished.set(),
        ),
        name="second-disposal",
        daemon=True,
    )
    first_thread.start()
    assert first_recorded.wait(2)
    second_thread.start()
    assert second_finished.wait(2)
    release_first.set()
    first_thread.join(2)
    second_thread.join(2)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert not first.exists()
    assert not second.exists()


def test_vehicle_capture_recovers_owned_full_and_crop_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "archive"
    full_dir = archive / "images" / "occupied-full"
    crop_dir = archive / "images" / "occupied-crops"
    full_dir.mkdir(parents=True)
    crop_dir.mkdir(parents=True)
    pending_paths = [full_dir / "old-full.jpg", crop_dir / "old-crop.jpg"]
    real_unlink = owned_file_disposal.os.unlink

    def interrupt_disposal(name: object, *args: object, **kwargs: object) -> None:
        if str(name).endswith(".dispose"):
            raise OSError("simulated crash")
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(owned_file_disposal.os, "unlink", interrupt_disposal)
    for directory, target in zip((full_dir, crop_dir), pending_paths, strict=True):
        for index in range(300):
            (directory / f"decoy-{index:03d}").write_bytes(b"x")
        target.write_bytes(b"pending")
        identity = file_descriptor_binding.FileIdentity.from_stat(target.stat())
        assert file_descriptor_binding.unlink_owned_path(target, identity) is False
        assert not target.exists()

    source = tmp_path / "source.jpg"
    Image.new("RGB", (16, 16), "blue").save(source, format="JPEG")
    monkeypatch.setattr(owned_file_disposal.os, "unlink", real_unlink)
    capture_occupied_images(
        archive_root=archive,
        session_id="new-session",
        source_frame_path=source,
        bbox=(0, 0, 8, 8),
    )

    assert [path.read_bytes() for path in pending_paths] == [b"pending", b"pending"]
