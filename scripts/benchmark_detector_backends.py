#!/usr/bin/env python3
"""Benchmark detector exports offline without changing the production backend."""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing
import os
import resource
import statistics
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parking_spot_monitor.detection import _normalize_ultralytics_results


PARITY_MINIMUM_IOU = 0.99
PARITY_MAXIMUM_CONFIDENCE_DELTA = 0.02
RESOURCE_IMPROVEMENT_RATIO = 0.15


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Serial, subprocess-isolated benchmark for YOLO detector exports."
    )
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--pt-model", required=True, type=Path)
    parser.add_argument("--onnx-model", required=True, type=Path)
    parser.add_argument("--torchscript-model", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=20)
    return parser


def _load_manifest(path: Path) -> list[Path]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("manifest is missing or is not valid JSON") from exc
    frames = payload.get("frames") if isinstance(payload, dict) else None
    if not isinstance(frames, list) or not frames:
        raise ValueError("manifest must contain a non-empty frames array")
    if not all(isinstance(frame, str) and frame for frame in frames):
        raise ValueError("every manifest frame must be a non-empty path string")
    resolved = [
        (Path(frame) if Path(frame).is_absolute() else path.parent / frame)
        for frame in frames
    ]
    if any(not frame.is_file() for frame in resolved):
        raise ValueError("every manifest frame must exist as a regular file")
    return resolved


def _peak_rss_bytes() -> int:
    peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return peak if sys.platform == "darwin" else peak * 1024


def _detection_payload(detection: Any) -> dict[str, Any]:
    return {
        "class_name": detection.class_name,
        "confidence": detection.confidence,
        "bbox": list(detection.bbox),
    }


def _run_backend_worker(
    sender: Any,
    backend: str,
    model_path: str,
    frame_paths: list[str],
    warmup: int,
    iterations: int,
) -> None:
    try:
        from ultralytics import YOLO

        load_started = time.perf_counter()
        model = YOLO(model_path)
        load_seconds = time.perf_counter() - load_started
        measured_seconds: list[float] = []
        normalized_frames: list[list[dict[str, Any]]] = []
        for cycle in range(warmup + iterations):
            cycle_frames: list[list[dict[str, Any]]] = []
            for frame_path in frame_paths:
                started = time.perf_counter()
                raw_results = model.predict(source=frame_path, verbose=False)
                elapsed = time.perf_counter() - started
                normalized = _normalize_ultralytics_results(raw_results)
                cycle_frames.append(
                    [_detection_payload(detection) for detection in normalized]
                )
                if cycle >= warmup:
                    measured_seconds.append(elapsed)
            if cycle >= warmup:
                normalized_frames = cycle_frames
        sender.send(
            {
                "backend": backend,
                "worker_pid": os.getpid(),
                "load_seconds": load_seconds,
                "timings": measured_seconds,
                "peak_rss_bytes": _peak_rss_bytes(),
                "frames": normalized_frames,
            }
        )
    except Exception as exc:
        sender.send(
            {
                "error": "backend benchmark failed",
                "error_type": type(exc).__name__,
            }
        )
    finally:
        sender.close()


def _run_isolated_backend(
    backend: str,
    model_path: Path,
    frames: list[Path],
    *,
    warmup: int,
    iterations: int,
) -> dict[str, Any]:
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_run_backend_worker,
        args=(
            sender,
            backend,
            str(model_path),
            [str(frame) for frame in frames],
            warmup,
            iterations,
        ),
    )
    process.start()
    sender.close()
    try:
        evidence = receiver.recv()
    except EOFError as exc:
        raise ValueError(f"{backend} worker produced no evidence") from exc
    finally:
        receiver.close()
        process.join()
    if process.exitcode != 0 or not isinstance(evidence, dict) or "error" in evidence:
        raise ValueError(f"{backend} worker failed to produce valid evidence")
    timings = evidence.pop("timings", None)
    if not isinstance(timings, list) or not timings:
        raise ValueError(f"{backend} worker produced no timing evidence")
    evidence["p50_seconds"] = statistics.median(timings)
    evidence["p95_seconds"] = _percentile(timings, 0.95)
    evidence["frame_count"] = len(frames)
    evidence["detection_count"] = sum(len(items) for items in evidence["frames"])
    return evidence


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def _bbox_iou(left: list[float], right: list[float]) -> float:
    intersection_width = max(0.0, min(left[2], right[2]) - max(left[0], right[0]))
    intersection_height = max(0.0, min(left[3], right[3]) - max(left[1], right[1]))
    intersection = intersection_width * intersection_height
    left_area = max(0.0, left[2] - left[0]) * max(0.0, left[3] - left[1])
    right_area = max(0.0, right[2] - right[0]) * max(0.0, right[3] - right[1])
    union = left_area + right_area - intersection
    return intersection / union if union > 0 else 0.0


def _parity_metrics(
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
            bbox_ious.append(_bbox_iou(expected["bbox"], actual["bbox"]))
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


def _improved_by_gate(baseline: float | int, candidate: float | int) -> bool:
    return baseline > 0 and candidate <= baseline * (1.0 - RESOURCE_IMPROVEMENT_RATIO)


def _build_report(
    results: dict[str, dict[str, Any]],
    *,
    warmup: int,
    iterations: int,
) -> dict[str, Any]:
    baseline = results["pt"]
    baseline.update(
        {
            "exact_frame_count": True,
            "exact_ordered_classes_and_counts": True,
            "no_new_or_omitted_detections": True,
            "maximum_confidence_delta": 0.0,
            "minimum_bbox_iou": 1.0,
            "parity_passed": True,
            "resource_improvement_passed": False,
        }
    )
    for backend in ("onnx", "torchscript"):
        result = results[backend]
        result.update(_parity_metrics(baseline["frames"], result["frames"]))
        result["resource_improvement_passed"] = _improved_by_gate(
            baseline["p95_seconds"], result["p95_seconds"]
        ) or _improved_by_gate(
            baseline["peak_rss_bytes"], result["peak_rss_bytes"]
        )
    alternatives = [results["onnx"], results["torchscript"]]
    eligible = all(item["parity_passed"] for item in alternatives) and any(
        item["resource_improvement_passed"] for item in alternatives
    )
    return {
        "schema_version": 1,
        "execution": "serial-spawned-subprocesses",
        "warmup": warmup,
        "iterations": iterations,
        "thresholds": {
            "minimum_bbox_iou": PARITY_MINIMUM_IOU,
            "maximum_confidence_delta": PARITY_MAXIMUM_CONFIDENCE_DELTA,
            "minimum_resource_improvement_ratio": RESOURCE_IMPROVEMENT_RATIO,
        },
        "backends": results,
        "production_switch_eligible": eligible,
    }


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.warmup < 0 or args.iterations < 1:
            raise ValueError("warmup must be non-negative and iterations must be positive")
        models = {
            "pt": args.pt_model,
            "onnx": args.onnx_model,
            "torchscript": args.torchscript_model,
        }
        if any(not path.is_file() for path in models.values()):
            raise ValueError("every model must exist as a regular file")
        frames = _load_manifest(args.manifest)
        results = {
            backend: _run_isolated_backend(
                backend,
                model,
                frames,
                warmup=args.warmup,
                iterations=args.iterations,
            )
            for backend, model in models.items()
        }
        _write_report(
            args.output,
            _build_report(results, warmup=args.warmup, iterations=args.iterations),
        )
    except ValueError as exc:
        print(f"benchmark input/evidence error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
