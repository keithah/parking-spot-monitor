from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from PIL import Image


def _write_jpeg(path: Path, *, color: tuple[int, int, int] = (20, 40, 60)) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (8, 6), color=color).save(path, format="JPEG")
    return path.stat().st_size


def test_record_timeline_frame_keeps_one_frame_per_minute_and_prunes_older_than_retention(tmp_path: Path) -> None:
    from parking_spot_monitor.timeline_buffer import record_timeline_frame

    source = tmp_path / "latest.jpg"
    first_size = _write_jpeg(source, color=(10, 20, 30))
    frames_dir = tmp_path / "timeline" / "frames"
    old_frame = frames_dir / "20260516T064200Z.jpg"
    old_frame_size = _write_jpeg(old_frame, color=(1, 2, 3))

    first = record_timeline_frame(
        source,
        data_dir=tmp_path,
        observed_at=datetime(2026, 5, 16, 18, 42, 39, tzinfo=timezone.utc),
    )
    assert first.saved is True
    assert first.path == frames_dir / "20260516T184200Z.jpg"
    assert first.byte_size == first_size
    assert first.pruned_count == 1
    assert first.pruned_bytes == old_frame_size
    assert not old_frame.exists()

    _write_jpeg(source, color=(200, 10, 20))
    duplicate = record_timeline_frame(
        source,
        data_dir=tmp_path,
        observed_at=datetime(2026, 5, 16, 18, 42, 59, tzinfo=timezone.utc),
    )
    assert duplicate.saved is False
    assert duplicate.reason == "already-sampled"
    assert duplicate.path == frames_dir / "20260516T184200Z.jpg"
    assert first.path.read_bytes() != source.read_bytes()

    next_minute = record_timeline_frame(
        source,
        data_dir=tmp_path,
        observed_at=datetime(2026, 5, 16, 18, 43, 0, tzinfo=timezone.utc),
    )
    assert next_minute.saved is True
    assert next_minute.path == frames_dir / "20260516T184300Z.jpg"
    assert sorted(path.name for path in frames_dir.glob("*.jpg")) == ["20260516T184200Z.jpg", "20260516T184300Z.jpg"]


def test_record_timeline_frame_does_not_prune_on_duplicate_minute_sample(tmp_path: Path) -> None:
    from parking_spot_monitor.timeline_buffer import record_timeline_frame

    source = tmp_path / "latest.jpg"
    _write_jpeg(source)
    frames_dir = tmp_path / "timeline" / "frames"
    existing = frames_dir / "20260516T184200Z.jpg"
    stale = frames_dir / "20260516T064200Z.jpg"
    _write_jpeg(existing)
    _write_jpeg(stale)

    duplicate = record_timeline_frame(
        source,
        data_dir=tmp_path,
        observed_at=datetime(2026, 5, 16, 18, 42, 59, tzinfo=timezone.utc),
    )

    assert duplicate.saved is False
    assert duplicate.reason == "already-sampled"
    assert duplicate.pruned_count == 0
    assert stale.exists()


def test_record_timeline_frame_fails_safely_when_source_missing(tmp_path: Path) -> None:
    from parking_spot_monitor.timeline_buffer import record_timeline_frame

    result = record_timeline_frame(
        tmp_path / "missing.jpg",
        data_dir=tmp_path,
        observed_at=datetime(2026, 5, 16, 18, 42, 39, tzinfo=timezone.utc),
    )

    assert result.saved is False
    assert result.reason == "source-missing"
    assert result.path is None
    assert not (tmp_path / "timeline").exists()


def test_record_timeline_frame_fails_safely_when_timeline_path_is_unavailable(tmp_path: Path) -> None:
    from parking_spot_monitor.timeline_buffer import record_timeline_frame

    source = tmp_path / "latest.jpg"
    _write_jpeg(source)
    (tmp_path / "timeline").write_text("not a directory", encoding="utf-8")

    result = record_timeline_frame(
        source,
        data_dir=tmp_path,
        observed_at=datetime(2026, 5, 16, 18, 42, 39, tzinfo=timezone.utc),
    )

    assert result.saved is False
    assert result.reason == "timeline-unavailable"
    assert result.path == tmp_path / "timeline" / "frames" / "20260516T184200Z.jpg"
