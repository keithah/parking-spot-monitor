from __future__ import annotations

from tests.support._matrix import *  # noqa: F403


def test_occupied_spot_event_id_uses_event_type_spot_and_normalized_observed_at() -> None:
    event = occupied_event()

    assert occupied_spot_event_id(event) == "occupancy-occupied-event:left_spot:2026-05-18T20:01:02Z"


def test_format_occupied_spot_alert_includes_vehicle_and_estimate_context_without_unsafe_fields(tmp_path: Path) -> None:
    event = occupied_event(tmp_path / "latest.jpg") | {
        "occupied_crop_path": "/tmp/crop.jpg",
        "descriptor": {"histogram": [1, 2, 3]},
        "raw_bytes": b"jpeg",
        "rtsp_url": "rtsp://user:pass@example/camera",
        "ocr_text": "ABC1234",
        "matrix_token": ACCESS_TOKEN,
    }

    body = format_occupied_spot_alert(event)

    assert body == (
        "Parking spot occupied: left_spot at 2026-05-18 1:01:02 PM PDT\n"
        "Likely vehicle: silver hatchback (profile prof_repeat)\n"
        "Match: matched, confidence 0.92\n"
        "Estimated dwell: 1 hr–1 hr 30 min (typical 1 hr 15 min)\n"
        "Usual leave window: 11:45 PM–12:15 AM (typical 12:00 AM; crosses midnight)\n"
        "History: 4 samples, estimate confidence medium"
    )
    rendered = body.lower()
    assert "crop" not in rendered
    assert "descriptor" not in rendered
    assert "histogram" not in rendered
    assert "rtsp" not in rendered
    assert "abc1234" not in rendered
    assert ACCESS_TOKEN not in body


def test_format_occupied_spot_alert_is_honest_about_insufficient_history() -> None:
    event = occupied_event()
    event["likely_vehicle"] = {"profile_id": "prof_repeat", "match_status": "new_profile", "confidence": None}
    event["vehicle_history_estimate"] = {
        "status": "insufficient_history",
        "reason": "insufficient-samples",
        "profile_id": "prof_repeat",
        "sample_count": 1,
        "confidence": "low",
        "dwell_range": None,
        "leave_time_window": None,
    }

    assert format_occupied_spot_alert(event) == "Parking spot occupied: left_spot at 2026-05-18 1:01:02 PM PDT"


def test_format_occupied_spot_alert_omits_unavailable_new_profile_history_noise() -> None:
    event = occupied_event()
    event["likely_vehicle"] = {
        "profile_id": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "match_status": "new_profile",
        "confidence": 1,
    }
    event["vehicle_history_estimate"] = {
        "status": "insufficient_history",
        "reason": "insufficient-samples",
        "profile_id": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "sample_count": 0,
        "confidence": "low",
        "dwell_range": None,
        "leave_time_window": None,
    }

    assert format_occupied_spot_alert(event) == "Parking spot occupied: left_spot at 2026-05-18 1:01:02 PM PDT"


def test_format_occupied_spot_alert_omits_low_confidence_profile_only_estimate_noise() -> None:
    event = occupied_event()
    event["spot_id"] = "right_spot"
    event["observed_at"] = "2026-05-12T17:16:48.322925-07:00"
    event["likely_vehicle"] = {
        "label": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "profile_id": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "match_status": "matched",
        "confidence": 0.82,
    }
    event["vehicle_history_estimate"] = {
        "status": "estimated",
        "profile_id": "prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00",
        "sample_count": 2,
        "confidence": "low",
        "dwell_range": {"lower_seconds": 8700, "upper_seconds": 18600, "typical_seconds": 13500},
        "leave_time_window": {
            "start_minute": 21 * 60 + 15,
            "end_minute": 0,
            "typical_minute": 22 * 60 + 45,
            "crosses_midnight": True,
        },
    }

    assert format_occupied_spot_alert(event) == "Parking spot occupied: right_spot at 2026-05-12 5:16:48 PM PDT"


def test_matrix_delivery_open_alert_uploads_resized_image_without_mutating_retained_raw_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    noisy = Image.effect_noise((1280, 720), 80).convert("RGB")
    noisy.save(source, format="JPEG", quality=95)
    raw_bytes = source.read_bytes()
    assert len(raw_bytes) > 300_000
    seen: list[dict[str, Any]] = []

    class FakeClient:
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            seen.append({"kind": "text", "room_id": room_id, "txn_id": txn_id, "body": body})
            return "$text:example.org"

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            seen.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
            return "mxc://example.org/open"

        def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
            seen.append({
                "kind": "image",
                "room_id": room_id,
                "txn_id": txn_id,
                "body": body,
                "content_uri": content_uri,
                "info": dict(info),
            })
            return "$image:example.org"

    from parking_spot_monitor.logging import StructuredLogger

    delivery = MatrixDelivery(
        client=FakeClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=StructuredLogger(),
    )

    snapshot = delivery.send_open_spot_alert(open_event(source))

    assert [item["kind"] for item in seen] == ["text", "upload", "image"]
    texts = [item for item in seen if item["kind"] == "text"]
    uploads = [item for item in seen if item["kind"] == "upload"]
    images = [item for item in seen if item["kind"] == "image"]
    assert texts[0]["body"] == "Parking spot open: left_spot at 2026-05-18 1:01:02 PM PDT"
    assert len(uploads) == 1
    assert len(images) == 1
    assert snapshot.path.read_bytes() == raw_bytes
    assert uploads[0]["filename"] == snapshot.filename
    assert uploads[0]["content_type"] == "image/jpeg"
    assert uploads[0]["data"] != raw_bytes
    assert len(uploads[0]["data"]) <= 300_000
    assert images[0]["info"]["size"] == len(uploads[0]["data"])
    assert images[0]["info"]["w"] < 1280
    assert images[0]["info"]["h"] < 720
    assert images[0]["body"] == "Parking spot open: left_spot at 2026-05-18 1:01:02 PM PDT"


def test_matrix_snapshot_resize_uses_shared_encoder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source = tmp_path / "large.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    seen: dict[str, object] = {}

    def fake_encoder(image: Image.Image, **kwargs: object) -> JpegBudgetResult:
        seen["image_mode"] = image.mode
        seen.update(kwargs)
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 6)

    monkeypatch.setattr(matrix_snapshots, "encode_jpeg_under_budget", fake_encoder)

    data, info = matrix_snapshots._resize_jpeg_for_matrix_upload(source)

    assert data == b"jpeg"
    assert info == {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360}
    assert seen == {
        "image_mode": "RGB",
        "max_bytes": matrix_snapshots.MAX_MATRIX_UPLOAD_IMAGE_BYTES,
        "initial_max_dimension": matrix_snapshots.MATRIX_UPLOAD_INITIAL_MAX_DIMENSION,
        "min_dimension": matrix_snapshots.MATRIX_UPLOAD_MIN_DIMENSION,
        "dimension_scale": 0.85,
        "qualities": matrix_snapshots.MATRIX_UPLOAD_JPEG_QUALITIES,
        "resampling": Image.Resampling.LANCZOS,
    }


def test_matrix_snapshot_resize_drafts_before_load_and_reuses_rgb_source(
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
            raise AssertionError("RGB Matrix source must not be copied")

        def convert(self, mode: str) -> Image.Image:
            raise AssertionError(f"RGB Matrix source must not be converted to {mode}")

        def close(self) -> None:
            events.append("source-close")

    source_image = TrackingImage()
    monkeypatch.setattr(matrix_snapshots.Image, "open", lambda _path: source_image)

    def fake_encoder(image: object, **_kwargs: object) -> JpegBudgetResult:
        assert image is source_image
        events.append("encode")
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 3)

    monkeypatch.setattr(matrix_snapshots, "encode_jpeg_under_budget", fake_encoder)

    matrix_snapshots._resize_jpeg_bytes_for_matrix_upload_result(
        b"captured-jpeg",
        snapshot_path=tmp_path / "oversized.jpg",
    )

    assert events == [("draft", "RGB", (960, 540)), "load", "encode", "source-close"]


def test_matrix_snapshot_resize_log_has_diagnostics_without_expanding_media_info(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "large.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    source_size = source.stat().st_size
    assert source_size > matrix_snapshots.MAX_MATRIX_UPLOAD_IMAGE_BYTES
    monkeypatch.setattr(
        matrix_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 6),
    )
    snapshot = matrix_snapshots.MatrixSnapshot(
        path=source,
        filename=source.name,
        txn_id="snapshot-large",
        body="Raw full-frame snapshot",
        info={"mimetype": "image/jpeg", "size": source_size, "w": 1280, "h": 720},
        log_context={},
    )
    stream = StringIO()

    upload = matrix_snapshots._matrix_snapshot_upload(snapshot, logger=StructuredLogger(stream=stream))

    assert upload == {
        "data": b"jpeg",
        "info": {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360},
    }
    assert json.loads(stream.getvalue()) == {
        "event": "matrix-snapshot-upload-resized",
        "level": "INFO",
        "snapshot_path": str(source),
        "source_size": source_size,
        "upload_size": 4,
        "width": 640,
        "height": 360,
        "quality": 65,
        "attempts": 6,
    }


def test_matrix_snapshot_resize_translates_shared_encoder_failure_without_leaking_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "large.jpg"
    Image.new("RGB", (1280, 720)).save(source, "JPEG")
    opened: list[Image.Image] = []
    real_open = Image.open

    def tracking_open(path: Path) -> Image.Image:
        image = real_open(path)
        opened.append(image)
        return image

    monkeypatch.setattr(matrix_snapshots.Image, "open", tracking_open)
    monkeypatch.setattr(
        matrix_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: (_ for _ in ()).throw(ImageBudgetError("access_token=encoder-secret")),
    )

    with pytest.raises(MatrixError) as exc_info:
        matrix_snapshots._resize_jpeg_bytes_for_matrix_upload_result(source.read_bytes(), snapshot_path=source)

    assert str(exc_info.value) == "Matrix snapshot could not be resized under upload budget"
    assert exc_info.value.diagnostics == {
        "error_type": "snapshot_resize_failed",
        "snapshot_path": str(source),
    }
    assert exc_info.value.__cause__ is None
    assert "encoder-secret" not in str(exc_info.value) + repr(exc_info.value.diagnostics)
    assert len(opened) == 1
    with pytest.raises(ValueError, match="closed image"):
        opened[0].getpixel((0, 0))


def test_matrix_snapshot_resize_does_not_translate_unexpected_encoder_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "large.jpg"
    Image.new("RGB", (1280, 720)).save(source, "JPEG")
    unexpected = AssertionError("encoder invariant failed")

    def fail_unexpectedly(image: Image.Image, **kwargs: object) -> JpegBudgetResult:
        raise unexpected

    monkeypatch.setattr(matrix_snapshots, "encode_jpeg_under_budget", fail_unexpectedly)

    with pytest.raises(AssertionError) as exc_info:
        matrix_snapshots._resize_jpeg_for_matrix_upload(source)

    assert exc_info.value is unexpected
    assert exc_info.traceback[-1].name == "fail_unexpectedly"


def test_matrix_snapshot_resize_rejects_invalid_dimensions_with_safe_matrix_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class InvalidImage:
        format = "JPEG"
        size = (0, 720)

        def close(self) -> None:
            events.append("source-close")

    source = tmp_path / "invalid-dimensions.jpg"
    monkeypatch.setattr(matrix_snapshots.Image, "open", lambda _path: InvalidImage())

    with pytest.raises(MatrixError) as exc_info:
        matrix_snapshots._resize_jpeg_bytes_for_matrix_upload_result(b"captured-jpeg", snapshot_path=source)

    assert str(exc_info.value) == "Matrix snapshot dimensions are invalid"
    assert exc_info.value.diagnostics == {
        "error_type": "snapshot_resize_failed",
        "snapshot_path": str(source),
    }
    assert events == ["source-close"]


def test_matrix_snapshot_resize_converts_non_rgb_once_and_closes_both_images(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[object] = []

    class ConvertedImage:
        size = (1280, 720)
        mode = "RGB"
        closed = False

        def close(self) -> None:
            self.closed = True
            events.append("converted-close")

    converted = ConvertedImage()

    class SourceImage:
        format = "JPEG"
        size = (1280, 720)
        mode = "CMYK"

        def close(self) -> None:
            events.append("source-close")

        def draft(self, mode: str, size: tuple[int, int]) -> None:
            events.append(("draft", mode, size))

        def load(self) -> None:
            events.append("load")

        def convert(self, mode: str) -> ConvertedImage:
            events.append(("convert", mode))
            return converted

    monkeypatch.setattr(matrix_snapshots.Image, "open", lambda _path: SourceImage())

    def fake_encoder(image: object, **_kwargs: object) -> JpegBudgetResult:
        assert image is converted
        assert converted.closed is False
        events.append("encode")
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 3)

    monkeypatch.setattr(matrix_snapshots, "encode_jpeg_under_budget", fake_encoder)

    matrix_snapshots._resize_jpeg_bytes_for_matrix_upload_result(
        b"captured-jpeg",
        snapshot_path=tmp_path / "oversized-cmyk.jpg",
    )

    assert events == [
        ("draft", "RGB", (960, 540)),
        "load",
        ("convert", "RGB"),
        "encode",
        "converted-close",
        "source-close",
    ]


def test_matrix_snapshot_resize_translates_pillow_open_failure_safely(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "broken.jpg"
    monkeypatch.setattr(
        matrix_snapshots.Image,
        "open",
        lambda _path: (_ for _ in ()).throw(OSError("Authorization: Bearer pillow-secret")),
    )

    with pytest.raises(MatrixError) as exc_info:
        matrix_snapshots._resize_jpeg_for_matrix_upload(source)

    assert str(exc_info.value) == "Matrix snapshot could not be resized under upload budget"
    assert exc_info.value.diagnostics == {
        "error_type": "snapshot_resize_failed",
        "snapshot_path": str(source),
    }
    assert exc_info.value.__cause__ is None
    assert "pillow-secret" not in str(exc_info.value) + repr(exc_info.value.diagnostics)


def test_matrix_snapshot_resize_translates_decompression_bomb_warning_safely(
    tmp_path: Path,
) -> None:
    source = tmp_path / "decompression-bomb.jpg"
    Image.new("RGB", (40, 30)).save(source, "JPEG")
    prior_max_image_pixels = Image.MAX_IMAGE_PIXELS
    try:
        Image.MAX_IMAGE_PIXELS = 1_000
        with pytest.raises(MatrixError) as exc_info:
            matrix_snapshots._resize_jpeg_for_matrix_upload(source)
    finally:
        Image.MAX_IMAGE_PIXELS = prior_max_image_pixels

    assert str(exc_info.value) == "Matrix snapshot could not be resized under upload budget"
    assert exc_info.value.diagnostics == {
        "error_type": "snapshot_resize_failed",
        "snapshot_path": str(source),
    }
    assert exc_info.value.__cause__ is None


def test_matrix_snapshot_upload_under_budget_returns_raw_bytes_without_encoding(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "small.jpg"
    raw = write_jpeg(source, size=(8, 6))
    info = {"mimetype": "image/jpeg", "size": len(raw), "w": 8, "h": 6}
    snapshot = matrix_snapshots.MatrixSnapshot(
        path=source,
        filename=source.name,
        txn_id="snapshot-small",
        body="Raw full-frame snapshot",
        info=info,
        log_context={},
    )
    monkeypatch.setattr(
        matrix_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: (_ for _ in ()).throw(AssertionError("raw upload called encoder")),
    )

    upload = matrix_snapshots._matrix_snapshot_upload(snapshot, logger=None)

    assert upload == {"data": raw, "info": info}
    assert upload["info"] is not info


def test_matrix_snapshot_upload_rejects_retained_path_swapped_to_symlink(tmp_path: Path) -> None:
    source = tmp_path / "retained.jpg"
    raw = write_jpeg(source, size=(8, 6))
    outside = tmp_path / "operator-secret.txt"
    secret = b"arbitrary operator secret"
    outside.write_bytes(secret)
    snapshot = matrix_snapshots.MatrixSnapshot(
        path=source,
        filename=source.name,
        txn_id="snapshot-swapped",
        body="Raw full-frame snapshot",
        info={"mimetype": "image/jpeg", "size": len(raw), "w": 8, "h": 6},
        log_context={},
    )
    source.unlink()
    source.symlink_to(outside)

    with pytest.raises(MatrixError) as exc_info:
        matrix_snapshots._matrix_snapshot_upload(snapshot, logger=None)

    assert exc_info.value.diagnostics["error_type"] == "snapshot_resize_failed"
    assert secret.decode() not in str(exc_info.value) + repr(exc_info.value.diagnostics)


def test_matrix_snapshot_upload_derives_info_from_the_uploaded_bytes(tmp_path: Path) -> None:
    source = tmp_path / "retained.jpg"
    raw = write_jpeg(source, size=(11, 7))
    snapshot = matrix_snapshots.MatrixSnapshot(
        path=source,
        filename=source.name,
        txn_id="snapshot-evidence",
        body="Raw full-frame snapshot",
        info={"mimetype": "image/jpeg", "size": 1, "w": 1, "h": 1},
        log_context={},
    )

    upload = matrix_snapshots._matrix_snapshot_upload(snapshot, logger=None)

    assert upload == {
        "data": raw,
        "info": {"mimetype": "image/jpeg", "size": len(raw), "w": 11, "h": 7},
    }


def test_rooted_jpeg_evidence_rejects_mutation_during_descriptor_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retained = tmp_path / "retained.jpg"
    original = write_jpeg(retained, size=(80, 60))
    replacement_path = tmp_path / "replacement.jpg"
    replacement = write_jpeg(replacement_path, size=(80, 60), color=(200, 10, 10))
    padded_size = max(len(original), len(replacement))
    original = original.ljust(padded_size, b"\0")
    replacement = replacement.ljust(padded_size, b"\0")
    retained.write_bytes(original)
    assert len(replacement) == len(original)
    assert replacement != original
    real_read = matrix_snapshot_storage.os.read
    mutated = False

    def mutating_read(descriptor: int, size: int) -> bytes:
        nonlocal mutated
        chunk = real_read(descriptor, size)
        if chunk and not mutated:
            mutated = True
            retained.write_bytes(replacement)
        return chunk

    monkeypatch.setattr(matrix_snapshot_storage.os, "read", mutating_read)

    with pytest.raises(OSError, match="changed while reading"):
        matrix_snapshot_storage.read_owned_jpeg_evidence(
            tmp_path,
            retained.name,
            max_bytes=2 * 1024 * 1024,
        )


def test_rooted_jpeg_growth_rejection_reads_at_most_preflight_size_plus_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    retained = tmp_path / "growing.jpg"
    write_jpeg(retained)
    preflight_size = retained.stat().st_size
    retained_identity = (retained.stat().st_dev, retained.stat().st_ino)
    growth_limit = 40 * 1024 * 1024
    real_read = os.read
    consumed = 0
    read_calls = 0

    def continuously_growing_read(descriptor: int, size: int) -> bytes:
        nonlocal consumed, read_calls
        value = os.fstat(descriptor)
        if (value.st_dev, value.st_ino) == retained_identity:
            read_calls += 1
            if read_calls >= 2 and retained.stat().st_size < growth_limit:
                with retained.open("ab") as handle:
                    handle.write(b"x" * min(1024 * 1024, growth_limit - retained.stat().st_size))
        chunk = real_read(descriptor, size)
        if (value.st_dev, value.st_ino) == retained_identity:
            consumed += len(chunk)
        return chunk

    monkeypatch.setattr(matrix_snapshot_storage.os, "read", continuously_growing_read)

    with pytest.raises(OSError, match="changed while reading"):
        matrix_snapshot_storage.read_owned_jpeg_evidence(tmp_path, retained.name)

    assert consumed <= preflight_size + 1


@pytest.mark.parametrize("unsafe_name", [".", ".."])
def test_rooted_storage_rejects_dot_artifact_names(
    tmp_path: Path, unsafe_name: str
) -> None:
    snapshot_root = tmp_path / "snapshots"
    parent_artifact = tmp_path / "outside.jpg"
    parent_artifact.write_bytes(b"parent-owned")

    with pytest.raises(OSError, match="basename"):
        matrix_snapshot_storage.safe_artifact_name(unsafe_name)
    with pytest.raises(OSError, match="basename"):
        matrix_snapshot_storage.publish_owned_bytes(
            snapshot_root,
            unsafe_name,
            parent_artifact.name,
            b"replacement",
            mode=0o600,
        )
    with pytest.raises(OSError, match="basename"):
        matrix_snapshot_storage.delete_owned_artifact(
            snapshot_root,
            unsafe_name,
            parent_artifact.name,
        )

    assert parent_artifact.read_bytes() == b"parent-owned"


def test_owned_artifact_delete_result_distinguishes_deleted_missing_and_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "snapshots"
    root.mkdir()

    missing = matrix_snapshot_storage.delete_owned_artifact(root, None, "missing.jpg")
    assert (missing.status, missing.bytes_deleted) == ("missing", 0)

    failed_path = root / "failed.jpg"
    failed_path.write_bytes(b"failed")
    monkeypatch.setattr(matrix_snapshot_storage, "unlink_owned_at", lambda *_args: False)
    failed = matrix_snapshot_storage.delete_owned_artifact(root, None, failed_path.name)
    assert (failed.status, failed.bytes_deleted) == ("failed", 0)
    assert failed_path.read_bytes() == b"failed"

    monkeypatch.undo()
    deleted_path = root / "deleted.jpg"
    deleted_path.write_bytes(b"deleted")
    deleted = matrix_snapshot_storage.delete_owned_artifact(root, None, deleted_path.name)
    assert (deleted.status, deleted.bytes_deleted) == ("deleted", len(b"deleted"))
    assert not deleted_path.exists()


def test_rooted_jpeg_evidence_exposes_only_upload_bytes_and_info(tmp_path: Path) -> None:
    retained = tmp_path / "retained.jpg"
    payload = write_jpeg(retained, size=(8, 6))

    evidence = matrix_snapshot_storage.read_owned_jpeg_evidence(tmp_path, retained.name)

    assert evidence.data == payload
    assert dict(evidence.info) == {"mimetype": "image/jpeg", "size": len(payload), "w": 8, "h": 6}
    assert not hasattr(evidence, "sha256")


def test_matrix_snapshot_real_resize_honors_budget_dimensions_and_payload_metadata(tmp_path: Path) -> None:
    source = tmp_path / "large-real.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    assert source.stat().st_size > matrix_snapshots.MAX_MATRIX_UPLOAD_IMAGE_BYTES

    data, info = matrix_snapshots._resize_jpeg_for_matrix_upload(source)

    assert len(data) <= matrix_snapshots.MAX_MATRIX_UPLOAD_IMAGE_BYTES
    assert info == {"mimetype": "image/jpeg", "size": len(data), "w": 960, "h": 540}
    with Image.open(BytesIO(data)) as encoded:
        assert encoded.format == "JPEG"
        assert encoded.size == (960, 540)


def test_matrix_delivery_occupied_alert_sends_upload_and_image_with_alert_body(tmp_path: Path) -> None:
    source = tmp_path / "occupied.jpg"
    raw_bytes = write_jpeg(source, size=(9, 7))
    seen: list[dict[str, Any]] = []

    class FakeClient:
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            seen.append({"kind": "text", "room_id": room_id, "txn_id": txn_id, "body": body})
            return "$text:example.org"

        def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
            seen.append({"kind": "upload", "filename": filename, "data": data, "content_type": content_type})
            return "mxc://example.org/occupied"

        def send_image(self, *, room_id: str, txn_id: str, body: str, content_uri: str, info: dict[str, Any]) -> str:
            seen.append(
                {
                    "kind": "image",
                    "room_id": room_id,
                    "txn_id": txn_id,
                    "body": body,
                    "content_uri": content_uri,
                    "info": dict(info),
                }
            )
            return "$image:example.org"

    delivery = MatrixDelivery(
        client=FakeClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=None,  # type: ignore[arg-type]
    )

    snapshot = delivery.send_occupied_spot_alert(occupied_event(source))

    event_id = "occupancy-occupied-event:left_spot:2026-05-18T20:01:02Z"
    assert [item["kind"] for item in seen] == ["text", "upload", "image"]
    assert seen[0]["body"].startswith("Parking spot occupied: left_spot at 2026-05-18 1:01:02 PM PDT")
    assert seen[1]["content_type"] == "image/jpeg"
    assert seen[1]["data"] == raw_bytes
    assert seen[1]["filename"] == "occupancy-occupied-event-left-spot-2026-05-18t20-01-02z.jpg"
    assert snapshot.path == tmp_path / "snapshots" / "occupancy-occupied-event-left-spot-2026-05-18t20-01-02z.jpg"
    assert snapshot.filename == "occupancy-occupied-event-left-spot-2026-05-18t20-01-02z.jpg"
    assert seen[2]["txn_id"] == f"{event_id}:image"
    assert seen[2]["body"].startswith("Parking spot occupied: left_spot at 2026-05-18 1:01:02 PM PDT")
    assert "Likely vehicle: silver hatchback (profile prof_repeat)" in seen[2]["body"]
    assert seen[2]["content_uri"] == "mxc://example.org/occupied"
    assert seen[2]["info"] == {"mimetype": "image/jpeg", "size": len(raw_bytes), "w": 9, "h": 7}


def test_matrix_delivery_occupied_alert_rejects_invalid_snapshot_source(tmp_path: Path) -> None:
    source = tmp_path / "debug_latest.jpg"
    write_jpeg(source)

    class TextOnlyClient:
        def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
            return "$text:example.org"

    delivery = MatrixDelivery(
        client=TextOnlyClient(),  # type: ignore[arg-type]
        room_id=ROOM_ID,
        data_dir=tmp_path,
        snapshots_dir=tmp_path / "snapshots",
        logger=None,  # type: ignore[arg-type]
    )

    with pytest.raises(MatrixError) as exc_info:
        delivery.send_occupied_spot_alert(occupied_event(source))

    assert exc_info.value.diagnostics["error_type"] == "snapshot_invalid_source"
    assert exc_info.value.diagnostics["event_type"] == OCCUPIED_SPOT_EVENT_TYPE
