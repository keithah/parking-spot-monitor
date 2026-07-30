from __future__ import annotations

import hashlib
import json
import math
from typing import Any

from parking_spot_monitor.geometry import bbox_iou


PARITY_MINIMUM_IOU = 0.99
PARITY_MAXIMUM_CONFIDENCE_DELTA = 0.02
RESOURCE_IMPROVEMENT_RATIO = 0.15
MAX_DETECTIONS_PER_FRAME = 512

_WORKER_EVIDENCE_KEYS = {
    "backend",
    "worker_pid",
    "load_seconds",
    "timings",
    "peak_rss_bytes",
    "frames",
    "normalized_frames_sha256",
    "measured_iteration_count",
    "deterministic",
    "mismatch_iteration_count",
    "first_mismatch_iteration",
}


def normalized_frames_hash(frames: list[list[dict[str, Any]]]) -> str:
    encoded = json.dumps(
        frames,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def validated_detection_payload(detection: Any) -> dict[str, Any]:
    payload = {
        "class_name": detection.class_name,
        "confidence": detection.confidence,
        "bbox": list(detection.bbox),
    }
    _validate_detection(payload)
    return payload


def validate_worker_evidence(
    backend: str,
    evidence: object,
    *,
    expected_frame_count: int,
    expected_iterations: int,
) -> dict[str, Any]:
    try:
        if not isinstance(evidence, dict) or set(evidence) != _WORKER_EVIDENCE_KEYS:
            raise ValueError
        if evidence["backend"] != backend:
            raise ValueError
        if not _positive_int(evidence["worker_pid"]):
            raise ValueError
        if not _finite_number(evidence["load_seconds"], minimum=0.0):
            raise ValueError
        timings = evidence["timings"]
        if (
            not isinstance(timings, list)
            or len(timings) != expected_frame_count * expected_iterations
            or not all(_finite_number(value, minimum=0.0) for value in timings)
        ):
            raise ValueError
        if not _positive_int(evidence["peak_rss_bytes"]):
            raise ValueError
        frames = evidence["frames"]
        if not isinstance(frames, list) or len(frames) != expected_frame_count:
            raise ValueError
        for detections in frames:
            if not isinstance(detections, list) or len(detections) > MAX_DETECTIONS_PER_FRAME:
                raise ValueError
            for detection in detections:
                _validate_detection(detection)
        digest = evidence["normalized_frames_sha256"]
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or digest != normalized_frames_hash(frames)
        ):
            raise ValueError
        if evidence["measured_iteration_count"] != expected_iterations:
            raise ValueError
        deterministic = evidence["deterministic"]
        mismatch_count = evidence["mismatch_iteration_count"]
        first_mismatch = evidence["first_mismatch_iteration"]
        if not isinstance(deterministic, bool):
            raise ValueError
        if not _nonnegative_int(mismatch_count) or mismatch_count >= expected_iterations:
            raise ValueError
        if deterministic:
            if mismatch_count != 0 or first_mismatch is not None:
                raise ValueError
        elif (
            mismatch_count == 0
            or not _positive_int(first_mismatch)
            or not 2 <= first_mismatch <= expected_iterations
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{backend} worker evidence is malformed") from exc
    return evidence


def parity_metrics(
    baseline_frames: list[list[dict[str, Any]]],
    candidate_frames: list[list[dict[str, Any]]],
) -> dict[str, Any]:
    exact_frame_count = len(baseline_frames) == len(candidate_frames)
    exact_ordered_classes_and_counts = exact_frame_count and all(
        [item["class_name"] for item in baseline]
        == [item["class_name"] for item in candidate]
        for baseline, candidate in zip(baseline_frames, candidate_frames, strict=True)
    )
    no_new_or_omitted = exact_frame_count and all(
        len(baseline) == len(candidate)
        for baseline, candidate in zip(baseline_frames, candidate_frames, strict=True)
    )
    confidence_deltas: list[float] = []
    bbox_ious: list[float] = []
    for baseline, candidate in zip(baseline_frames, candidate_frames):
        for expected, actual in zip(baseline, candidate):
            confidence_deltas.append(
                abs(float(expected["confidence"]) - float(actual["confidence"]))
            )
            bbox_ious.append(bbox_iou(expected["bbox"], actual["bbox"]))
    maximum_confidence_delta = max(confidence_deltas, default=0.0)
    minimum_bbox_iou = min(bbox_ious, default=1.0)
    if not no_new_or_omitted:
        minimum_bbox_iou = 0.0
    parity_passed = (
        exact_frame_count
        and exact_ordered_classes_and_counts
        and no_new_or_omitted
        and minimum_bbox_iou >= PARITY_MINIMUM_IOU
        and maximum_confidence_delta <= PARITY_MAXIMUM_CONFIDENCE_DELTA
    )
    return {
        "exact_frame_count": exact_frame_count,
        "exact_ordered_classes_and_counts": exact_ordered_classes_and_counts,
        "no_new_or_omitted_detections": no_new_or_omitted,
        "maximum_confidence_delta": maximum_confidence_delta,
        "minimum_bbox_iou": minimum_bbox_iou,
        "parity_passed": parity_passed,
    }


def improved_by_resource_gate(baseline: float | int, candidate: float | int) -> bool:
    return baseline > 0 and candidate <= baseline * (1.0 - RESOURCE_IMPROVEMENT_RATIO)


def _validate_detection(detection: object) -> None:
    if not isinstance(detection, dict) or set(detection) != {
        "class_name",
        "confidence",
        "bbox",
    }:
        raise ValueError
    class_name = detection["class_name"]
    if not isinstance(class_name, str) or not class_name or len(class_name) > 256:
        raise ValueError
    if not _finite_number(detection["confidence"], minimum=0.0, maximum=1.0):
        raise ValueError
    bbox = detection["bbox"]
    if (
        not isinstance(bbox, list)
        or len(bbox) != 4
        or not all(_finite_number(value) for value in bbox)
    ):
        raise ValueError
    bbox_iou(bbox, bbox)


def _finite_number(
    value: object,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    number = float(value)
    return (
        math.isfinite(number)
        and (minimum is None or number >= minimum)
        and (maximum is None or number <= maximum)
    )


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _nonnegative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0
