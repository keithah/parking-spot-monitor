from __future__ import annotations

import errno
import os
from pathlib import Path

from PIL import Image

from parking_spot_monitor import jpeg_artifacts


def test_canonical_publication_reads_each_boundary_once_and_parses_source_once(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "source.jpg"
    Image.new("RGB", (32, 24), (20, 40, 60)).save(source, "JPEG")
    destination = tmp_path / "archive" / "published.jpg"
    source_identity = (source.stat().st_dev, source.stat().st_ino)
    bytes_read: dict[tuple[int, int], int] = {}
    opens = 0
    real_read = jpeg_artifacts.os.read
    real_open = jpeg_artifacts.Image.open

    def unsupported_reflink(*_args, **_kwargs) -> None:
        raise OSError(errno.EOPNOTSUPP, "unsupported")

    def counted_read(descriptor: int, size: int) -> bytes:
        payload = real_read(descriptor, size)
        value = os.fstat(descriptor)
        key = (value.st_dev, value.st_ino)
        bytes_read[key] = bytes_read.get(key, 0) + len(payload)
        return payload

    def counted_open(*args, **kwargs):
        nonlocal opens
        opens += 1
        return real_open(*args, **kwargs)

    monkeypatch.setattr(jpeg_artifacts, "_reflink", unsupported_reflink)
    monkeypatch.setattr(jpeg_artifacts.os, "read", counted_read)
    monkeypatch.setattr(jpeg_artifacts.Image, "open", counted_open)

    publication = jpeg_artifacts.publish_canonical_jpeg(source, destination)

    destination_identity = (destination.stat().st_dev, destination.stat().st_ino)
    assert publication.strategy == "copy"
    assert bytes_read[source_identity] == source.stat().st_size
    assert bytes_read[destination_identity] == destination.stat().st_size
    assert opens == 1
    assert destination.read_bytes() == source.read_bytes()
