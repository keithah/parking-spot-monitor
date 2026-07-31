from __future__ import annotations

import os
from pathlib import Path

import pytest

from parking_spot_monitor.file_descriptor_binding import RootedDirectoryOwner
from parking_spot_monitor import owned_file_cleanup, owned_file_recovery
from parking_spot_monitor.owned_disposal_manifest import DisposalManifestEntry, manifest_entries_at
from parking_spot_monitor.owned_file_disposal import DisposalResult, FileIdentity
from parking_spot_monitor.owned_file_recovery import recover_owned_directory_at
from parking_spot_monitor.owned_recovery_scan import TransitionScanBatch


def test_recovery_finds_legacy_quarantine_after_many_ordinary_entries(tmp_path: Path) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    for index in range(300):
        (tmp_path / f"ordinary-{index:04d}").write_bytes(b"ordinary")
    quarantine = tmp_path / ".owned.jpg.0123456789abcdef.quarantine"
    os.link(target, quarantine)

    with RootedDirectoryOwner(tmp_path, create=False) as owner:
        result = recover_owned_directory_at(owner.fd, max_entries=1)
        assert result.recovered == 0
        assert result.pending is True
        assert result.blocking is True
        recovered = result.recovered
        for _ in range(303):
            if not result.pending:
                break
            result = recover_owned_directory_at(owner.fd, max_entries=1)
            recovered += result.recovered

    assert recovered == 1
    assert result.pending is False
    assert target.read_bytes() == b"owned"
    assert not quarantine.exists()


def test_interrupted_quarantine_is_indexed_before_followup_disposal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    identity = FileIdentity.from_stat(target.stat())
    monkeypatch.setattr(owned_file_cleanup.secrets, "token_hex", lambda _size: "0123456789abcdef")
    monkeypatch.setattr(
        owned_file_cleanup,
        "dispose_owned_name_at",
        lambda *_args, **_kwargs: DisposalResult("restored"),
    )
    monkeypatch.setattr(owned_file_cleanup, "restore_quarantined_at", lambda *_args: False)

    with RootedDirectoryOwner(tmp_path, create=False) as owner:
        result = owned_file_cleanup.unlink_owned_at_result(owner.fd, target.name, identity)
        entries = manifest_entries_at(owner.fd)

    assert result.deleted is False
    assert entries == [
        DisposalManifestEntry(
            ".owned.jpg.0123456789abcdef.quarantine",
            "owned.jpg",
            identity.dev,
            identity.ino,
        )
    ]


def test_unfinished_legacy_directory_scan_blocks_conflicting_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"current publication")
    monkeypatch.setattr(
        owned_file_recovery,
        "scan_transition_batch",
        lambda *_args, **_kwargs: TransitionScanBatch((), exhausted=False),
    )

    with RootedDirectoryOwner(tmp_path, create=False) as owner:
        result = recover_owned_directory_at(owner.fd)

    assert result.blocking is True
    assert target.read_bytes() == b"current publication"
    assert list(tmp_path.glob(".*.quarantine")) == []


def test_unfinished_selected_scan_prevents_later_stale_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"stale publication")
    stale = tmp_path / ".owned.jpg.0123456789abcdef.quarantine"
    os.link(target, stale)
    target.unlink()
    target.write_bytes(b"current publication")
    current_identity = FileIdentity.from_stat(target.stat())
    batches = iter(
        (
            TransitionScanBatch((), exhausted=False),
            TransitionScanBatch(((stale.name, target.name, "quarantine"),), exhausted=True),
        )
    )
    monkeypatch.setattr(
        owned_file_recovery,
        "scan_transition_batch",
        lambda *_args, **_kwargs: next(batches),
    )

    with RootedDirectoryOwner(tmp_path, create=False) as owner:
        deleted = owned_file_cleanup.unlink_owned_at_result(owner.fd, target.name, current_identity)
        recovery = owned_file_recovery.recover_owned_at(owner.fd, target.name)

    assert deleted.deleted is False
    assert recovery.blocking is True
    assert target.read_bytes() == b"current publication"
    assert stale.read_bytes() == b"stale publication"
