from __future__ import annotations

from tests.support._matrix_operator_cockpit import *  # noqa: F403


def test_who_resize_uses_shared_budget_encoder_and_preserves_validation_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    seen: dict[str, object] = {}

    def fake_encoder(image: Image.Image, **kwargs: object) -> JpegBudgetResult:
        seen["image_mode"] = image.mode
        seen.update(kwargs)
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 5)

    monkeypatch.setattr(operator_cockpit_snapshots, "encode_jpeg_under_budget", fake_encoder, raising=False)
    result = operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=destination,
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert result.state == "available"
    assert result.path == destination
    assert result.info == {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360}
    assert result.freshness == "fresh"
    assert result.age == "0s ago"
    assert destination.read_bytes() == b"jpeg"
    assert seen == {
        "image_mode": "RGB",
        "max_bytes": operator_cockpit_snapshots.MAX_WHO_MATRIX_IMAGE_BYTES,
        "initial_max_dimension": operator_cockpit_snapshots.WHO_MATRIX_INITIAL_MAX_DIMENSION,
        "min_dimension": operator_cockpit_snapshots.WHO_MATRIX_MIN_DIMENSION,
        "dimension_scale": 0.85,
        "qualities": operator_cockpit_snapshots.WHO_MATRIX_JPEG_QUALITIES,
        "resampling": Image.Resampling.LANCZOS,
    }


def test_who_resize_publishes_temp_sibling_with_atomic_replace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    source_bytes = _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    destination.write_bytes(b"previous-valid-jpeg")
    real_replace = os.replace
    replacements: list[tuple[Path, Path, bytes, bytes]] = []

    def tracking_replace(source_path: str | os.PathLike[str], destination_path: str | os.PathLike[str]) -> None:
        temporary = Path(source_path)
        target = Path(destination_path)
        replacements.append((temporary, target, temporary.read_bytes(), target.read_bytes()))
        real_replace(temporary, target)

    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"new-jpeg", 640, 360, 65, 2),
        raising=False,
    )
    monkeypatch.setattr(operator_cockpit_snapshots.os, "replace", tracking_replace, raising=False)

    result = operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=destination,
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert result.path == destination
    assert destination.read_bytes() == b"new-jpeg"
    assert source.read_bytes() == source_bytes
    assert len(replacements) == 1
    temporary, target, staged_bytes, prior_bytes = replacements[0]
    assert temporary.parent == destination.parent
    assert temporary.name.startswith(f".{destination.name}.")
    assert target == destination
    assert staged_bytes == b"new-jpeg"
    assert prior_bytes == b"previous-valid-jpeg"
    assert not temporary.exists()


@pytest.mark.parametrize("failure", ["encode", "write", "fsync", "replace", "stat"])
def test_who_resize_publish_failure_preserves_previous_destination_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    old_bytes = b"previous-valid-jpeg"
    destination.write_bytes(old_bytes)
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"replacement-jpeg", 640, 360, 65, 2),
        raising=False,
    )

    if failure == "encode":
        monkeypatch.setattr(
            operator_cockpit_snapshots,
            "encode_jpeg_under_budget",
            lambda image, **kwargs: (_ for _ in ()).throw(ImageBudgetError("encode failed")),
        )
    elif failure == "write":
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "write",
            lambda fd, data: (_ for _ in ()).throw(OSError("write failed")),
            raising=False,
        )
    elif failure == "fsync":
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "fsync",
            lambda fd: (_ for _ in ()).throw(OSError("fsync failed")),
            raising=False,
        )
    elif failure == "replace":
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "replace",
            lambda source_path, destination_path: (_ for _ in ()).throw(OSError("replace failed")),
            raising=False,
        )
    else:
        real_stat = Path.stat

        def fail_temp_stat(path: Path, *args: object, **kwargs: object) -> os.stat_result:
            if path.name.startswith(f".{destination.name}."):
                raise OSError("stat failed")
            return real_stat(path, *args, **kwargs)

        monkeypatch.setattr(Path, "stat", fail_temp_stat)

    expected_error = ImageBudgetError if failure == "encode" else OSError
    with pytest.raises(expected_error, match=failure):
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=destination,
            now=datetime.now(timezone.utc),
            logger=None,
        )

    assert destination.read_bytes() == old_bytes
    assert list(tmp_path.glob(f".{destination.name}.*")) == []


def test_who_resize_drafts_before_load_reuses_rgb_source_and_closes_it(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[object] = []

    class TrackingImage:
        format = "JPEG"
        size = (1280, 720)
        mode = "RGB"

        def draft(self, mode: str, size: tuple[int, int]) -> None:
            events.append(("draft", mode, size))

        def load(self) -> None:
            events.append("load")

        def copy(self) -> Image.Image:
            raise AssertionError("RGB operator source must not be copied")

        def convert(self, mode: str) -> Image.Image:
            raise AssertionError(f"RGB operator source must not be converted to {mode}")

        def close(self) -> None:
            events.append("source-close")

    source_image = TrackingImage()
    monkeypatch.setattr(operator_cockpit_snapshots.Image, "open", lambda path: source_image)

    def fake_encoder(image: object, **kwargs: object) -> JpegBudgetResult:
        assert image is source_image
        events.append("encode")
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 3)

    monkeypatch.setattr(operator_cockpit_snapshots, "encode_jpeg_under_budget", fake_encoder, raising=False)
    operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        tmp_path / "latest.jpg",
        destination=tmp_path / "who_latest.jpg",
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert events == [("draft", "RGB", (960, 540)), "load", "encode", "source-close"]


def test_who_resize_converts_non_rgb_once_and_closes_both_images(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[object] = []

    class ConvertedImage:
        size = (1280, 720)
        mode = "RGB"

        def close(self) -> None:
            events.append("converted-close")

    converted = ConvertedImage()

    class SourceImage:
        format = "JPEG"
        size = (1280, 720)
        mode = "CMYK"

        def draft(self, mode: str, size: tuple[int, int]) -> None:
            events.append(("draft", mode, size))

        def load(self) -> None:
            events.append("load")

        def convert(self, mode: str) -> ConvertedImage:
            events.append(("convert", mode))
            return converted

        def close(self) -> None:
            events.append("source-close")

    monkeypatch.setattr(operator_cockpit_snapshots.Image, "open", lambda path: SourceImage())

    def fake_encoder(image: object, **kwargs: object) -> JpegBudgetResult:
        assert image is converted
        events.append("encode")
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 3)

    monkeypatch.setattr(operator_cockpit_snapshots, "encode_jpeg_under_budget", fake_encoder, raising=False)
    operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        tmp_path / "latest.jpg",
        destination=tmp_path / "who_latest.jpg",
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert events == [
        ("draft", "RGB", (960, 540)),
        "load",
        ("convert", "RGB"),
        "encode",
        "converted-close",
        "source-close",
    ]


def test_who_resize_logs_quality_and_attempts_without_expanding_media_info(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 7),
        raising=False,
    )
    stream = StringIO()

    result = operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=destination,
        now=datetime.now(timezone.utc),
        logger=StructuredLogger(stream=stream),
    )

    assert result.info == {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360}
    assert json.loads(stream.getvalue()) == {
        "event": "operator-who-snapshot-resized",
        "level": "INFO",
        "source_path": str(source),
        "destination_path": str(destination),
        "source_width": 1280,
        "source_height": 720,
        "output_width": 640,
        "output_height": 360,
        "byte_size": 4,
        "quality": 65,
        "attempts": 7,
    }


@pytest.mark.parametrize(
    "operational_failure",
    [
        ImageBudgetError("access_token=budget-secret"),
        OSError("Authorization: Bearer io-secret"),
        operator_cockpit_snapshots.UnidentifiedImageError("secret=unidentified"),
        Image.DecompressionBombError("password=bomb-secret"),
        Image.DecompressionBombWarning("token=warning-secret"),
    ],
)
def test_who_resize_expected_operational_failure_returns_safe_unavailable_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operational_failure: BaseException,
) -> None:
    source = tmp_path / "latest.jpg"
    source.write_bytes(b"oversized")
    stream = StringIO()
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "_validate_latest_snapshot",
        lambda path, **kwargs: operator_cockpit_snapshots.LatestSnapshotValidation(state="error", error_type="too large"),
    )
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "_resize_who_snapshot_for_matrix",
        lambda *args, **kwargs: (_ for _ in ()).throw(operational_failure),
    )

    result = operator_cockpit_snapshots._prepare_who_snapshot_for_matrix(
        source,
        data_dir=tmp_path,
        now=datetime.now(timezone.utc),
        logger=StructuredLogger(stream=stream),
    )

    assert result == operator_cockpit_snapshots.LatestSnapshotValidation(state="error", error_type="resize failed")
    rendered = stream.getvalue()
    assert "operator-who-snapshot-unavailable" in rendered
    assert "resize_failed" in rendered
    assert "secret" not in rendered


@pytest.mark.parametrize(
    "unexpected",
    [AssertionError("encoder invariant failed"), MemoryError("allocation invariant failed")],
)
def test_who_snapshot_availability_boundary_does_not_hide_unexpected_resize_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    unexpected: BaseException,
) -> None:
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "_prepare_who_snapshot_for_matrix",
        lambda *args, **kwargs: (_ for _ in ()).throw(unexpected),
    )

    with pytest.raises(type(unexpected)) as exc_info:
        operator_cockpit_snapshots.build_who_snapshot_response(
            settings=object(),
            data_dir=tmp_path,
            base_text="Parking monitor who",
            capture_func=lambda *args, **kwargs: SimpleNamespace(
                latest_path=tmp_path / "latest.jpg",
                timestamp="2026-05-16T17:42:39Z",
            ),
        )

    assert exc_info.value is unexpected


@pytest.mark.parametrize(
    ("capture_result", "expected_reason"),
    [
        (OSError("Authorization: Bearer capture-secret"), "OSError"),
        (SimpleNamespace(timestamp="2026-05-16T17:42:39Z"), "AttributeError"),
    ],
)
def test_who_snapshot_unexpected_capture_or_result_failure_keeps_safe_text_fallback(
    tmp_path: Path,
    capture_result: object,
    expected_reason: str,
) -> None:
    stream = StringIO()

    def capture_func(*args: object, **kwargs: object) -> object:
        if isinstance(capture_result, BaseException):
            raise capture_result
        return capture_result

    response = operator_cockpit_snapshots.build_who_snapshot_response(
        settings=object(),
        data_dir=tmp_path,
        base_text="Parking monitor who\n- left_spot: occupied",
        capture_func=capture_func,
        logger=StructuredLogger(stream=stream),
    )

    assert response.image_path is None
    assert response.image_info is None
    assert f"Snapshot: fresh capture unavailable ({expected_reason}); no live state was changed." in response.text
    assert "left_spot: occupied" in response.text
    rendered = response.text + stream.getvalue()
    assert "capture-secret" not in rendered
    assert "operator-who-snapshot-unavailable" in rendered


def test_who_resize_promotes_real_pillow_decompression_warning_at_availability_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(12, 12))
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 100)
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "_validate_latest_snapshot",
        lambda path, **kwargs: operator_cockpit_snapshots.LatestSnapshotValidation(state="error", error_type="too large"),
    )
    stream = StringIO()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = operator_cockpit_snapshots._prepare_who_snapshot_for_matrix(
            source,
            data_dir=tmp_path,
            now=datetime.now(timezone.utc),
            logger=StructuredLogger(stream=stream),
        )

    assert result == operator_cockpit_snapshots.LatestSnapshotValidation(state="error", error_type="resize failed")
    assert caught == []
    rendered = stream.getvalue()
    assert "resize_failed" in rendered
    assert "DecompressionBombWarning" in rendered


def test_who_resize_logs_temp_cleanup_failure_without_masking_primary_publish_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    old_bytes = b"previous-valid-jpeg"
    destination.write_bytes(old_bytes)
    primary = OSError("fsync primary failure")
    real_unlink = Path.unlink
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"replacement-jpeg", 640, 360, 65, 2),
    )
    monkeypatch.setattr(
        operator_cockpit_snapshots.os,
        "fsync",
        lambda fd: (_ for _ in ()).throw(primary),
    )

    def fail_temp_unlink(path: Path, *args: object, **kwargs: object) -> None:
        if path.name.startswith(f".{destination.name}."):
            raise OSError("token=cleanup-secret")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_temp_unlink)
    stream = StringIO()

    with pytest.raises(OSError) as exc_info:
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=destination,
            now=datetime.now(timezone.utc),
            logger=StructuredLogger(stream=stream),
        )

    assert exc_info.value is primary
    assert destination.read_bytes() == old_bytes
    temporary_files = list(tmp_path.glob(f".{destination.name}.*"))
    assert len(temporary_files) == 1
    rendered = stream.getvalue()
    assert "temp_cleanup_failed" in rendered
    assert "OSError" in rendered
    assert "cleanup-secret" not in rendered
    real_unlink(temporary_files[0])


def test_who_resize_preserves_existing_destination_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 2),
    )
    existing = tmp_path / "who_latest.jpg"
    existing.write_bytes(b"old")
    existing.chmod(0o640)

    operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=existing,
        now=datetime.now(timezone.utc),
        logger=None,
    )
    assert existing.stat().st_mode & 0o777 == 0o640


@pytest.mark.parametrize(("process_umask", "expected_mode"), [(0o077, 0o600), (0o022, 0o644)])
def test_who_resize_new_destination_mode_matches_path_write_bytes_umask_semantics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    process_umask: int,
    expected_mode: int,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 2),
    )
    new_destination = tmp_path / "new" / "who_latest.jpg"
    previous_umask = os.umask(process_umask)
    try:
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=new_destination,
            now=datetime.now(timezone.utc),
            logger=None,
        )
    finally:
        os.umask(previous_umask)

    assert new_destination.stat().st_mode & 0o777 == expected_mode


@pytest.mark.parametrize("failure", ["destination-stat", "fchmod"])
def test_who_resize_mode_preparation_failure_preserves_old_destination_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    old_bytes = b"previous-valid-jpeg"
    destination.write_bytes(old_bytes)
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"replacement-jpeg", 640, 360, 65, 2),
    )
    if failure == "destination-stat":
        real_stat = Path.stat

        def fail_destination_stat(path: Path, *args: object, **kwargs: object) -> os.stat_result:
            if path == destination:
                raise OSError("destination-stat failed")
            return real_stat(path, *args, **kwargs)

        monkeypatch.setattr(Path, "stat", fail_destination_stat)
    else:
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "fchmod",
            lambda fd, mode: (_ for _ in ()).throw(OSError("fchmod failed")),
            raising=False,
        )

    with pytest.raises(OSError, match=failure):
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=destination,
            now=datetime.now(timezone.utc),
            logger=None,
        )

    assert destination.read_bytes() == old_bytes
    assert list(tmp_path.glob(f".{destination.name}.*")) == []


def test_who_resize_retries_partial_writes_until_complete(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    payload = b"partial-write-jpeg"
    real_write = os.write
    write_sizes: list[int] = []
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(payload, 640, 360, 65, 2),
    )

    def partial_write(file_descriptor: int, data: object) -> int:
        chunk = bytes(data)[:3]
        write_sizes.append(len(chunk))
        return real_write(file_descriptor, chunk)

    monkeypatch.setattr(operator_cockpit_snapshots.os, "write", partial_write)

    operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=destination,
        now=datetime.now(timezone.utc),
        logger=None,
    )

    assert destination.read_bytes() == payload
    assert write_sizes == [3, 3, 3, 3, 3, 3]


@pytest.mark.parametrize("write_failure", ["zero-progress", "interrupted"])
def test_who_resize_zero_progress_or_interrupted_write_preserves_old_destination_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    write_failure: str,
) -> None:
    source = tmp_path / "latest.jpg"
    _write_test_jpeg(source, size=(1280, 720))
    destination = tmp_path / "who_latest.jpg"
    old_bytes = b"previous-valid-jpeg"
    destination.write_bytes(old_bytes)
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"replacement-jpeg", 640, 360, 65, 2),
    )
    if write_failure == "zero-progress":
        monkeypatch.setattr(operator_cockpit_snapshots.os, "write", lambda fd, data: 0)
        expected = "no progress"
    else:
        monkeypatch.setattr(
            operator_cockpit_snapshots.os,
            "write",
            lambda fd, data: (_ for _ in ()).throw(InterruptedError("interrupted")),
        )
        expected = "interrupted"

    with pytest.raises(OSError, match=expected):
        operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
            source,
            destination=destination,
            now=datetime.now(timezone.utc),
            logger=None,
        )

    assert destination.read_bytes() == old_bytes
    assert list(tmp_path.glob(f".{destination.name}.*")) == []


def test_latest_snapshot_summary_contract_returns_text_and_raw_image_path(tmp_path: Path) -> None:
    from parking_spot_monitor.operator_cockpit import build_latest_snapshot_response

    settings = _settings(tmp_path)
    health_path, state_path = _write_runtime_files(tmp_path)
    latest_path = tmp_path / "latest.jpg"
    raw_bytes = _write_test_jpeg(latest_path, size=(16, 9))
    log_stream = StringIO()

    response = build_latest_snapshot_response(
        settings=settings,
        latest_path=latest_path,
        health_path=health_path,
        state_path=state_path,
        now=datetime(2026, 5, 18, 19, 0, 20, tzinfo=timezone.utc),
        logger=StructuredLogger(stream=log_stream),
    )

    assert response.image_path == latest_path
    assert response.image_info == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 16, "h": 9}
    assert "Parking monitor latest" in response.text
    assert "Snapshot: fresh raw latest.jpg" in response.text
    assert "16x9" in response.text
    assert "Health: degraded" in response.text
    assert "last frame 30s ago" in response.text
    assert "detection failures 2" in response.text
    assert "left_spot" in response.text and "right_spot" in response.text
    assert len(response.text.encode("utf-8")) <= 4096
    _assert_no_sensitive_text(response.text + log_stream.getvalue())
