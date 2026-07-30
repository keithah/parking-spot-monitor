from __future__ import annotations

import os
from pathlib import Path

import pytest

from parking_spot_monitor.file_descriptor_binding import RootedDirectoryOwner
from parking_spot_monitor import owned_file_cleanup
from parking_spot_monitor.owned_disposal_manifest import DisposalManifestEntry, manifest_entries_at
from parking_spot_monitor.owned_file_disposal import DisposalResult, FileIdentity
from parking_spot_monitor.owned_file_recovery import recover_owned_directory_at


def test_recovery_finds_legacy_quarantine_after_many_ordinary_entries(tmp_path: Path) -> None:
    target = tmp_path / "owned.jpg"
    target.write_bytes(b"owned")
    quarantine = tmp_path / ".owned.jpg.0123456789abcdef.quarantine"
    os.link(target, quarantine)
    for index in range(300):
        (tmp_path / f"ordinary-{index:04d}").write_bytes(b"ordinary")

    with RootedDirectoryOwner(tmp_path, create=False) as owner:
        result = recover_owned_directory_at(owner.fd, max_entries=1)
        for _ in range(302):
            if result.recovered:
                break
            result = recover_owned_directory_at(owner.fd, max_entries=1)

    assert result.recovered == 1
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
