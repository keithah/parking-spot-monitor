from __future__ import annotations

from tests.support._startup import *  # noqa: F403


def test_process_detection_uses_spot_crop_inference_to_recover_full_frame_miss(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example").read_text(encoding="utf-8").replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    class FullMissCropDetector:
        def __init__(self) -> None:
            self.calls: list[tuple[Path, tuple[int, int]]] = []

        def detect(
            self,
            frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            path = Path(frame_path)
            with Image.open(path) as image:
                size = image.size
            self.calls.append((path, size))
            assert confidence_threshold == 0.1
            assert inference_image_size == 1280
            if size == (1458, 806):
                return []
            if size == (531, 296):
                return [VehicleDetection(class_name="car", confidence=0.88, bbox=(98, 93, 483, 233))]
            return []

    from parking_spot_monitor.runtime_detection import _process_detection_for_capture

    settings = load_settings(config_path, environ=fake_environ())
    detector = FullMissCropDetector()

    result = _process_detection_for_capture(
        settings,
        adapt_detector(detector),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    right = result.by_spot["right_spot"].accepted
    assert right is not None
    assert right.bbox == pytest.approx((1010, 215, 1395, 355))
    assert [size for _path, size in detector.calls] == [(1458, 806), (526, 276), (531, 296)]
    assert all(not path.exists() for path, _size in detector.calls[1:])
    output = combined_output(capsys)
    assert '"spot_crop_inference_enabled":true' in output
    assert '"spot_crop_detection_count":1' in output


def test_process_detection_uses_no_temporary_files_for_in_memory_crop_detector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")
    real_image_open = Image.open
    image_open_calls = 0

    def counting_image_open(*args: object, **kwargs: object) -> Image.Image:
        nonlocal image_open_calls
        image_open_calls += 1
        return real_image_open(*args, **kwargs)

    monkeypatch.setattr(Image, "open", counting_image_open)

    class RecordingInMemoryDetector:
        def __init__(self) -> None:
            self.crop_images: list[Image.Image] = []
            self.crop_calls: list[dict[str, object]] = []

        def detect(self, frame_path: str | Path, **kwargs: object) -> list[VehicleDetection]:
            return []

        def detect_image(self, image: Image.Image, **kwargs: object) -> list[VehicleDetection]:
            self.crop_images.append(image)
            self.crop_calls.append({"size": image.size, **kwargs})
            if image.size == (531, 296):
                return [VehicleDetection(class_name="car", confidence=0.88, bbox=(98, 93, 483, 233))]
            return []

    import parking_spot_monitor.runtime_detection as runtime_detection

    def fail_temporary_directory(*args: object, **kwargs: object) -> object:
        pytest.fail("in-memory crop inference allocated a temporary directory")

    monkeypatch.setattr(runtime_detection.tempfile, "TemporaryDirectory", fail_temporary_directory)
    detector = RecordingInMemoryDetector()
    settings = load_settings(config_path, environ=fake_environ())

    result = runtime_detection._process_detection_for_capture(
        settings,
        adapt_detector(detector),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert detector.crop_calls == [
        {"size": (526, 276), "confidence_threshold": 0.1, "inference_image_size": 1280},
        {"size": (531, 296), "confidence_threshold": 0.1, "inference_image_size": 1280},
    ]
    assert result.by_spot["right_spot"].accepted is not None
    assert result.by_spot["right_spot"].accepted.bbox == pytest.approx((1010, 215, 1395, 355))
    assert image_open_calls == 1
    for crop in detector.crop_images:
        with pytest.raises(ValueError, match="closed"):
            crop.getpixel((0, 0))


def test_incompatible_detect_image_uses_temporary_jpeg_fallback(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    class IncidentalDetectImageDetector:
        def __init__(self) -> None:
            self.path_calls: list[tuple[Path, tuple[int, int]]] = []
            self.image_calls = 0

        def detect(
            self,
            frame_path: str | Path,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            path = Path(frame_path)
            with Image.open(path) as image:
                size = image.size
            self.path_calls.append((path, size))
            if size == (531, 296):
                return [VehicleDetection(class_name="car", confidence=0.88, bbox=(98, 93, 483, 233))]
            return []

        def detect_image(self, image: Image.Image) -> list[VehicleDetection]:
            self.image_calls += 1
            return []

    import parking_spot_monitor.runtime_detection as runtime_detection

    detector = IncidentalDetectImageDetector()
    result = runtime_detection._process_detection_for_capture(
        load_settings(config_path, environ=fake_environ()),
        adapt_detector(detector),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert detector.image_calls == 0
    assert [size for _path, size in detector.path_calls] == [(1458, 806), (526, 276), (531, 296)]
    assert all(not path.exists() for path, _size in detector.path_calls[1:])
    assert result.by_spot["right_spot"].accepted is not None


def test_compatible_detect_image_internal_type_error_propagates(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")
    sentinel = TypeError("detector inference failed internally")

    class FailingInMemoryDetector:
        def detect(self, frame_path: str | Path, **kwargs: object) -> list[VehicleDetection]:
            return []

        def detect_image(
            self,
            image: Image.Image,
            *,
            confidence_threshold: float | None = None,
            inference_image_size: int | None = None,
        ) -> list[VehicleDetection]:
            raise sentinel

    import parking_spot_monitor.runtime_detection as runtime_detection

    with pytest.raises(TypeError) as exc_info:
        runtime_detection._process_detection_for_capture(
            load_settings(config_path, environ=fake_environ()),
            adapt_detector(FailingInMemoryDetector()),
            frame,
            logger=StructuredLogger(),
            mode="test",
            frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
        )

    assert exc_info.value is sentinel


def test_spot_crop_image_size_failure_closes_open_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example")
        .read_text(encoding="utf-8")
        .replace("spot_crop_inference: false", "spot_crop_inference: true"),
        encoding="utf-8",
    )

    class FailingSizeImage:
        def __init__(self) -> None:
            self.closed = False

        @property
        def size(self) -> tuple[int, int]:
            raise OSError("unreadable dimensions")

        def close(self) -> None:
            self.closed = True

    class EmptyDetector:
        def detect(self, frame_path: str | Path, **kwargs: object) -> list[VehicleDetection]:
            return []

    source = FailingSizeImage()
    monkeypatch.setattr(Image, "open", lambda *args, **kwargs: source)

    import parking_spot_monitor.runtime_detection as runtime_detection

    runtime_detection._process_detection_for_capture(
        load_settings(config_path, environ=fake_environ()),
        adapt_detector(EmptyDetector()),
        tmp_path / "latest.jpg",
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert source.closed is True


def test_process_detection_scales_configured_polygons_to_actual_frame_size(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    frame = tmp_path / "low-res-latest.jpg"
    Image.new("RGB", (640, 360), (20, 30, 40)).save(frame, format="JPEG")

    class LowResDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return [VehicleDetection(class_name="car", confidence=0.9, bbox=(142.0, 91.0, 265.0, 151.0))]

    from parking_spot_monitor.runtime_detection import _process_detection_for_capture

    settings = load_settings("config.yaml.example", environ=fake_environ())
    result = _process_detection_for_capture(
        settings,
        adapt_detector(LowResDetector()),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="low_resolution", expected_size=(640, 360)),
    )

    output = combined_output(capsys)
    [record] = json_records(output)
    assert result.by_spot["left_spot"].accepted is not None
    assert result.by_spot["right_spot"].accepted is None
    assert record["frame_size_mismatch"] is True
    assert record["configured_frame_size"] == {"height": 806, "width": 1458}
    assert record["actual_frame_size"] == {"height": 360, "width": 640}
    assert record["accepted_by_spot"] == {"left_spot": True, "right_spot": False}


def test_process_detection_skips_candidate_summaries_when_info_is_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import parking_spot_monitor.runtime_detection as runtime_detection

    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    def forbidden_candidate_summaries(_result: DetectionFilterResult) -> list[dict[str, Any]]:
        pytest.fail("candidate summaries were computed for a suppressed INFO record")

    monkeypatch.setattr(runtime_detection, "_candidate_summaries", forbidden_candidate_summaries)

    result = runtime_detection._process_detection_for_capture(
        load_settings("config.yaml.example", environ=fake_environ()),
        adapt_detector(NoopDetector()),
        frame,
        logger=StructuredLogger(level="WARNING"),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert set(result.by_spot) == {"left_spot", "right_spot"}


def test_runtime_detection_does_not_build_candidate_arrays_for_info_logging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import parking_spot_monitor.runtime_detection as runtime_detection

    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")

    def forbidden_candidate_summaries(_result: DetectionFilterResult) -> list[dict[str, Any]]:
        pytest.fail("routine runtime INFO must not build candidate arrays")

    monkeypatch.setattr(runtime_detection, "_candidate_summaries", forbidden_candidate_summaries)
    runtime_detection._process_detection_for_capture(
        load_settings("config.yaml.example", environ=fake_environ()),
        adapt_detector(NoopDetector()),
        frame,
        logger=StructuredLogger(),
        mode="runtime-loop",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    assert '"event":"detection-frame-processed"' not in combined_output(capsys)


def test_process_detection_keeps_candidate_summary_schema_when_info_is_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import parking_spot_monitor.runtime_detection as runtime_detection

    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, format="JPEG")
    calls = 0
    original = runtime_detection._candidate_summaries

    def candidate_summary_spy(result: DetectionFilterResult) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        return original(result)

    monkeypatch.setattr(runtime_detection, "_candidate_summaries", candidate_summary_spy)

    runtime_detection._process_detection_for_capture(
        load_settings("config.yaml.example", environ=fake_environ()),
        adapt_detector(NoopDetector()),
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )

    [record] = json_records(combined_output(capsys))
    assert calls == 1
    assert record["candidate_summaries"] == []


def test_runtime_loop_matrix_state_change_skip_log_explains_policy(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    detections = [[left_spot_vehicle()], [left_spot_vehicle()], [left_spot_vehicle()]]
    delivery = FakeMatrixDelivery()

    def fake_capture(_settings: object, _data_dir: str | Path, *, stream_profile: str | None = None) -> FrameCaptureResult:
        return captured_frame(tmp_path, timestamp="2026-05-18T19:00:00Z")

    class SequencedDetector:
        def detect(self, frame_path: str | Path, *, confidence_threshold: float | None = None) -> list[VehicleDetection]:
            return next_detection(detections, allow_exhausted=True)

    exit_code = _main(
        ["--config", "config.yaml.example", "--data-dir", str(tmp_path)],
        environ=fake_environ(),
        capture=fake_capture,
        overlay=noop_overlay,
        detector_factory=lambda _settings: SequencedDetector(),
        matrix_delivery_factory=lambda _settings, _data_dir, _logger: delivery,
        sleep=lambda _seconds: None,
        max_iterations=3,
        now=lambda: datetime(2026, 5, 18, 19, 0, tzinfo=timezone.utc),
    )

    output = combined_output(capsys)
    records = json_records(output)
    skipped = [
        record
        for record in records
        if record.get("event") == "matrix-delivery-skipped"
        and record.get("event_type") == "occupancy-state-changed"
        and record.get("spot_id") == "left_spot"
    ]
    assert exit_code == 0
    assert delivery.open_alerts == []
    assert len(skipped) == 1
    assert skipped[0]["reason"] == "state-change-not-alert"
    assert skipped[0]["matrix_dispatch_policy"] == "open-events-only"
    assert skipped[0]["next_expected_event"] == "occupancy-open-event"
    assert_no_secret_leak(output)
