from __future__ import annotations

import builtins
import math

import pytest
from pydantic import ValidationError

from parking_spot_monitor.config import DetectionConfig
from parking_spot_monitor.detection import (
    DetectionError,
    RejectionReason,
    UltralyticsVehicleDetector,
    VehicleDetection,
    filter_spot_detections,
)


LEFT_SPOT = [(0, 0), (100, 0), (100, 100), (0, 100)]
RIGHT_SPOT = [(200, 0), (300, 0), (300, 100), (200, 100)]
SPOTS = {"left_spot": LEFT_SPOT, "right_spot": RIGHT_SPOT}
CONFIGURED_SPOTS = {
    "left_spot": [(300, 180), (610, 160), (690, 285), (420, 360), (260, 300)],
    "right_spot": [(1010, 155), (1395, 170), (1395, 355), (1040, 370), (960, 250)],
}


def detection(
    bbox: tuple[float, float, float, float],
    *,
    confidence: float = 0.9,
    class_name: str = "car",
) -> VehicleDetection:
    return VehicleDetection(class_name=class_name, confidence=confidence, bbox=bbox)


def test_filter_spot_detections_accepts_left_and_right_candidates() -> None:
    result = filter_spot_detections(
        [detection((10, 10, 90, 90)), detection((210, 10, 290, 90), confidence=0.8)],
        spots=SPOTS,
        allowed_classes={"car", "truck"},
        confidence_threshold=0.35,
        min_bbox_area_px=100,
        min_polygon_overlap_ratio=0.5,
    )

    assert result.by_spot["left_spot"].accepted is not None
    assert result.by_spot["left_spot"].accepted.class_name == "car"
    assert result.by_spot["left_spot"].accepted.overlap_ratio == pytest.approx(1.0)
    assert result.by_spot["right_spot"].accepted is not None
    assert result.rejection_counts == {RejectionReason.CENTROID_OUTSIDE: 2}


def test_filter_spot_detections_rejects_driveway_bbox_by_centroid() -> None:
    result = filter_spot_detections(
        [detection((90, 120, 220, 220))],
        spots=SPOTS,
        allowed_classes={"car"},
        confidence_threshold=0.35,
        min_bbox_area_px=100,
        min_polygon_overlap_ratio=0.1,
    )

    assert result.by_spot["left_spot"].accepted is None
    assert result.by_spot["right_spot"].accepted is None
    assert result.rejection_counts == {RejectionReason.CENTROID_OUTSIDE: 2}


def test_filter_spot_detections_rejects_s03_driveway_bbox_for_configured_spots() -> None:
    result = filter_spot_detections(
        [detection((260, 330, 940, 806))],
        spots=CONFIGURED_SPOTS,
        allowed_classes={"car"},
        confidence_threshold=0.35,
        min_bbox_area_px=1200,
        min_polygon_overlap_ratio=0.2,
    )

    assert result.by_spot["left_spot"].accepted is None
    assert result.by_spot["right_spot"].accepted is None
    assert result.rejection_counts == {RejectionReason.CENTROID_OUTSIDE: 2}


@pytest.mark.parametrize("confidence", [math.nan, math.inf, -math.inf, -0.01, 1.01])
def test_vehicle_detection_rejects_non_finite_or_out_of_range_confidence(confidence: float) -> None:
    with pytest.raises(ValueError, match="confidence"):
        detection((10, 10, 90, 90), confidence=confidence)


def test_filter_spot_detections_propagates_source_frame_metadata_to_candidate() -> None:
    result = filter_spot_detections(
        [detection((10, 10, 90, 90))],
        spots={"left_spot": LEFT_SPOT},
        allowed_classes={"car"},
        confidence_threshold=0.35,
        min_bbox_area_px=100,
        min_polygon_overlap_ratio=0.5,
        source_frame_path="/data/latest.jpg",
        source_timestamp="2025-01-01T00:00:00Z",
    )

    candidate = result.by_spot["left_spot"].accepted
    assert candidate is not None
    assert candidate.source_frame_path == "/data/latest.jpg"
    assert candidate.source_timestamp == "2025-01-01T00:00:00Z"


@pytest.mark.parametrize(
    "vehicle,expected_reason",
    [
        (detection((10, 10, 90, 90), class_name="person"), RejectionReason.CLASS_NOT_ALLOWED),
        (detection((10, 10, 90, 90), confidence=0.34), RejectionReason.CONFIDENCE_TOO_LOW),
        (detection((10, 10, 15, 15)), RejectionReason.AREA_TOO_SMALL),
        (detection((110, 10, 190, 90)), RejectionReason.CENTROID_OUTSIDE),
        (detection((-75, 10, 75, 90)), RejectionReason.OVERLAP_TOO_LOW),
    ],
)
def test_filter_spot_detections_reports_each_rejection_reason(
    vehicle: VehicleDetection,
    expected_reason: RejectionReason,
) -> None:
    result = filter_spot_detections(
        [vehicle],
        spots={"left_spot": LEFT_SPOT},
        allowed_classes={"car"},
        confidence_threshold=0.35,
        min_bbox_area_px=100,
        min_polygon_overlap_ratio=0.75,
    )

    assert result.by_spot["left_spot"].accepted is None
    assert [rejection.reason for rejection in result.by_spot["left_spot"].rejected] == [expected_reason]
    assert result.rejection_counts == {expected_reason: 1}


def test_filter_spot_detections_selects_deterministically_by_confidence_overlap_and_stable_ordering() -> None:
    result = filter_spot_detections(
        [
            detection((10, 10, 90, 90), confidence=0.8, class_name="truck"),
            detection((20, 20, 80, 80), confidence=0.9, class_name="truck"),
            detection((10, 10, 90, 90), confidence=0.9, class_name="car"),
            detection((10, 10, 80, 80), confidence=0.9, class_name="car"),
        ],
        spots={"left_spot": LEFT_SPOT},
        allowed_classes={"car", "truck"},
        confidence_threshold=0.35,
        min_bbox_area_px=100,
        min_polygon_overlap_ratio=0.1,
    )

    assert result.by_spot["left_spot"].accepted is not None
    assert result.by_spot["left_spot"].accepted.class_name == "car"
    assert result.by_spot["left_spot"].accepted.bbox == (10.0, 10.0, 80.0, 80.0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"confidence_threshold": -0.1},
        {"confidence_threshold": 1.1},
        {"min_bbox_area_px": 0},
        {"min_polygon_overlap_ratio": -0.1},
        {"min_polygon_overlap_ratio": 1.1},
    ],
)
def test_detection_config_rejects_invalid_thresholds(kwargs: dict[str, float]) -> None:
    values = {
        "model": "yolov8n.pt",
        "confidence_threshold": 0.35,
        "vehicle_classes": ["car"],
        "min_bbox_area_px": 500,
        "min_polygon_overlap_ratio": 0.25,
    }
    values.update(kwargs)

    with pytest.raises(ValidationError):
        DetectionConfig.model_validate(values)


@pytest.mark.parametrize("bbox", [(0, 0, 10), (10, 0, 0, 10)])
def test_vehicle_detection_rejects_invalid_bbox_shape_or_order(bbox: tuple[int, ...]) -> None:
    with pytest.raises(ValueError, match="bbox"):
        VehicleDetection(class_name="car", confidence=0.9, bbox=bbox)  # type: ignore[arg-type]


def test_filter_spot_detections_accepts_boundary_thresholds_and_empty_input() -> None:
    empty = filter_spot_detections(
        [],
        spots={"left_spot": LEFT_SPOT},
        allowed_classes={"car"},
        confidence_threshold=0.35,
        min_bbox_area_px=100,
        min_polygon_overlap_ratio=0.5,
    )
    assert empty.by_spot["left_spot"].accepted is None
    assert empty.by_spot["left_spot"].rejected == []

    boundary = filter_spot_detections(
        [detection((-50, 0, 50, 100), confidence=0.35)],
        spots={"left_spot": LEFT_SPOT},
        allowed_classes={"car"},
        confidence_threshold=0.35,
        min_bbox_area_px=10000,
        min_polygon_overlap_ratio=0.5,
    )
    assert boundary.by_spot["left_spot"].accepted is not None
    assert boundary.by_spot["left_spot"].accepted.overlap_ratio == pytest.approx(0.5)


class FakeBoxes:
    def __init__(self, xyxy: object, conf: object, cls: object) -> None:
        self.xyxy = xyxy
        self.conf = conf
        self.cls = cls


class FakeResult:
    def __init__(self, *, boxes: FakeBoxes | None, names: object) -> None:
        self.boxes = boxes
        self.names = names


class FakeYOLO:
    constructed_with: list[str] = []

    def __init__(self, model_path: str) -> None:
        self.constructed_with.append(model_path)
        self.predict_calls: list[dict[str, object]] = []
        self.results: list[FakeResult] = []

    def predict(self, **kwargs: object) -> list[FakeResult]:
        self.predict_calls.append(kwargs)
        return self.results


class FakeTensor:
    def __init__(self, value: object) -> None:
        self.value = value

    def detach(self) -> "FakeTensor":
        return self

    def cpu(self) -> "FakeTensor":
        return self

    def numpy(self) -> "FakeTensor":
        return self

    def tolist(self) -> object:
        return self.value


def test_ultralytics_detector_normalizes_fake_results_and_forwards_confidence(tmp_path) -> None:
    FakeYOLO.constructed_with = []
    detector = UltralyticsVehicleDetector("yolov8n.pt", yolo_class=FakeYOLO)
    detector._model.results = [  # type: ignore[attr-defined]
        FakeResult(
            boxes=FakeBoxes(
                xyxy=FakeTensor([[1, 2, 11, 22], [30.5, 40.5, 70.5, 90.5]]),
                conf=FakeTensor([0.91, 0.42]),
                cls=FakeTensor([2, 7]),
            ),
            names={2: "car", 7: "truck"},
        )
    ]

    detections = detector.detect(tmp_path / "frame.jpg", confidence_threshold=0.35)

    assert detections == [
        VehicleDetection(class_name="car", confidence=0.91, bbox=(1, 2, 11, 22)),
        VehicleDetection(class_name="truck", confidence=0.42, bbox=(30.5, 40.5, 70.5, 90.5)),
    ]
    assert detector._model.predict_calls == [  # type: ignore[attr-defined]
        {"source": str(tmp_path / "frame.jpg"), "conf": 0.35, "verbose": False}
    ]
    assert FakeYOLO.constructed_with == ["yolov8n.pt"]


def test_ultralytics_detector_supports_empty_and_multiple_result_batches(tmp_path) -> None:
    detector = UltralyticsVehicleDetector("yolov8n.pt", yolo_class=FakeYOLO)
    detector._model.results = [  # type: ignore[attr-defined]
        FakeResult(boxes=None, names={}),
        FakeResult(boxes=FakeBoxes(xyxy=[], conf=[], cls=[]), names={}),
        FakeResult(boxes=FakeBoxes(xyxy=[[1, 1, 5, 5]], conf=[0.6], cls=[0]), names=["car"]),
        FakeResult(boxes=FakeBoxes(xyxy=[[10, 10, 20, 20]], conf=[0.7], cls=[1]), names={"1": "truck"}),
    ]

    assert detector.detect(tmp_path / "frame.jpg") == [
        VehicleDetection(class_name="car", confidence=0.6, bbox=(1, 1, 5, 5)),
        VehicleDetection(class_name="truck", confidence=0.7, bbox=(10, 10, 20, 20)),
    ]
    assert detector._model.predict_calls == [{"source": str(tmp_path / "frame.jpg"), "verbose": False}]  # type: ignore[attr-defined]


def test_ultralytics_detector_uses_stable_unknown_class_name(tmp_path) -> None:
    detector = UltralyticsVehicleDetector("yolov8n.pt", yolo_class=FakeYOLO)
    detector._model.results = [  # type: ignore[attr-defined]
        FakeResult(boxes=FakeBoxes(xyxy=[[1, 1, 5, 5]], conf=[0.6], cls=[99]), names={})
    ]

    assert detector.detect(tmp_path / "frame.jpg") == [
        VehicleDetection(class_name="unknown_99", confidence=0.6, bbox=(1, 1, 5, 5))
    ]


@pytest.mark.parametrize(
    "boxes",
    [
        object(),
        FakeBoxes(xyxy=[[1, 1, 5, 5]], conf=[0.6], cls=[]),
        FakeBoxes(xyxy=[[1, 1, 5, 5]], conf=["not-a-number"], cls=[0]),
        FakeBoxes(xyxy=[[1, 1, "wide", 5]], conf=[0.6], cls=[0]),
    ],
)
def test_ultralytics_detector_raises_safe_error_for_malformed_results(tmp_path, boxes: object) -> None:
    detector = UltralyticsVehicleDetector("yolov8n.pt", yolo_class=FakeYOLO)
    detector._model.results = [FakeResult(boxes=boxes, names={0: "car"})]  # type: ignore[attr-defined]

    with pytest.raises(DetectionError) as exc_info:
        detector.detect(tmp_path / "frame.jpg")

    diagnostic = exc_info.value.diagnostics()
    assert diagnostic["phase"] == "predict"
    assert diagnostic["model_path"] == "yolov8n.pt"
    assert diagnostic["frame_path"] == str(tmp_path / "frame.jpg")
    assert "Traceback" not in str(exc_info.value)


def test_ultralytics_detector_raises_safe_error_for_import_failure(monkeypatch) -> None:
    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "ultralytics":
            raise ImportError("cannot load rtsp://user:pass@camera token=abc Traceback noisy")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(DetectionError) as exc_info:
        UltralyticsVehicleDetector("yolov8n.pt")

    diagnostic = exc_info.value.diagnostics()
    assert diagnostic == {
        "phase": "model_load",
        "model_path": "yolov8n.pt",
        "error_type": "ImportError",
        "message": "cannot load rtsp://<redacted> token=<redacted> noisy",
    }
    assert "user:pass" not in str(exc_info.value)
    assert "abc" not in str(exc_info.value)
    assert "Traceback" not in str(exc_info.value)


def test_ultralytics_detector_raises_safe_error_for_construction_failure() -> None:
    class FailingYOLO:
        def __init__(self, model_path: str) -> None:
            raise RuntimeError(f"bad model {model_path}")

    with pytest.raises(DetectionError) as exc_info:
        UltralyticsVehicleDetector("bad.pt", yolo_class=FailingYOLO)

    assert exc_info.value.diagnostics() == {
        "phase": "model_load",
        "model_path": "bad.pt",
        "error_type": "RuntimeError",
        "message": "bad model bad.pt",
    }


def test_ultralytics_detector_raises_safe_error_for_prediction_failure(tmp_path) -> None:
    class PredictFailYOLO:
        def __init__(self, model_path: str) -> None:
            self.model_path = model_path

        def predict(self, **kwargs: object) -> list[FakeResult]:
            raise RuntimeError("predict failed access_token=secret")

    detector = UltralyticsVehicleDetector("yolov8n.pt", yolo_class=PredictFailYOLO)

    with pytest.raises(DetectionError) as exc_info:
        detector.detect(tmp_path / "frame.jpg")

    assert exc_info.value.diagnostics() == {
        "phase": "predict",
        "model_path": "yolov8n.pt",
        "frame_path": str(tmp_path / "frame.jpg"),
        "error_type": "RuntimeError",
        "message": "predict failed access_token=<redacted>",
    }


def test_detection_error_redacts_secret_bearing_model_and_frame_paths() -> None:
    exc = DetectionError(
        "failed",
        model_path="rtsp://user:pass@camera/model.pt?access_token=model-secret",
        frame_path="/data/latest.jpg?matrix_token=frame-secret",
        phase="predict",
        error_type="RuntimeError",
    )

    diagnostic = exc.diagnostics()
    rendered = str(exc)
    assert diagnostic["model_path"] == "rtsp://<redacted>"
    assert diagnostic["frame_path"] == "/data/latest.jpg?matrix_token=<redacted>"
    assert "user:pass" not in rendered
    assert "model-secret" not in rendered
    assert "frame-secret" not in rendered
