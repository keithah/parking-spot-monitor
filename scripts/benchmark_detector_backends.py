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
from parking_spot_monitor.detector_benchmark_evidence import (
    MAX_DETECTIONS_PER_FRAME,
    PARITY_MAXIMUM_CONFIDENCE_DELTA,
    PARITY_MINIMUM_IOU,
    RESOURCE_IMPROVEMENT_RATIO,
    improved_by_resource_gate,
    normalized_frames_hash,
    parity_metrics,
    validated_detection_payload,
    validate_worker_evidence,
)
from parking_spot_monitor.detector_benchmark_models import (
    require_unchanged_models,
    validated_model_identities,
)
from parking_spot_monitor.detector_benchmark_output import (
    validate_benchmark_output,
    write_guarded_report,
)


MAX_MANIFEST_FRAMES = 256
MAX_WARMUP = 20
MAX_ITERATIONS = 100
DEFAULT_WORKER_TIMEOUT_SECONDS = 1800.0
MAX_WORKER_TIMEOUT_SECONDS = 3600.0


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
    parser.add_argument(
        "--worker-timeout-seconds",
        type=float,
        default=DEFAULT_WORKER_TIMEOUT_SECONDS,
    )
    return parser


def _load_manifest(path: Path) -> list[Path]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("manifest is missing or is not valid JSON") from exc
    frames = payload.get("frames") if isinstance(payload, dict) else None
    if not isinstance(frames, list) or not frames:
        raise ValueError("manifest must contain a non-empty frames array")
    if len(frames) > MAX_MANIFEST_FRAMES:
        raise ValueError("manifest exceeds the supported frame bound")
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
        readiness_results = model.predict(source=frame_paths[0], verbose=False)
        readiness_detections = _normalize_ultralytics_results(readiness_results)
        if len(readiness_detections) > MAX_DETECTIONS_PER_FRAME:
            raise ValueError("backend exceeded the per-frame detection bound")
        for detection in readiness_detections:
            validated_detection_payload(detection)
        load_seconds = time.perf_counter() - load_started
        measured_seconds: list[float] = []
        normalized_frames: list[list[dict[str, Any]]] | None = None
        normalized_frames_sha256: str | None = None
        mismatch_iteration_count = 0
        first_mismatch_iteration: int | None = None
        for cycle in range(warmup + iterations):
            cycle_frames: list[list[dict[str, Any]]] = []
            for frame_path in frame_paths:
                started = time.perf_counter()
                raw_results = model.predict(source=frame_path, verbose=False)
                elapsed = time.perf_counter() - started
                normalized = _normalize_ultralytics_results(raw_results)
                if len(normalized) > MAX_DETECTIONS_PER_FRAME:
                    raise ValueError("backend exceeded the per-frame detection bound")
                cycle_frames.append(
                    [validated_detection_payload(detection) for detection in normalized]
                )
                if cycle >= warmup:
                    measured_seconds.append(elapsed)
            if cycle >= warmup:
                iteration = cycle - warmup + 1
                cycle_hash = normalized_frames_hash(cycle_frames)
                if normalized_frames is None:
                    normalized_frames = cycle_frames
                    normalized_frames_sha256 = cycle_hash
                elif cycle_hash != normalized_frames_sha256 or cycle_frames != normalized_frames:
                    mismatch_iteration_count += 1
                    if first_mismatch_iteration is None:
                        first_mismatch_iteration = iteration
        if normalized_frames is None or normalized_frames_sha256 is None:
            raise ValueError("backend produced no measured normalized evidence")
        sender.send(
            {
                "backend": backend,
                "worker_pid": os.getpid(),
                "load_seconds": load_seconds,
                "timings": measured_seconds,
                "peak_rss_bytes": _peak_rss_bytes(),
                "frames": normalized_frames,
                "normalized_frames_sha256": normalized_frames_sha256,
                "measured_iteration_count": iterations,
                "deterministic": mismatch_iteration_count == 0,
                "mismatch_iteration_count": mismatch_iteration_count,
                "first_mismatch_iteration": first_mismatch_iteration,
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
    worker_timeout_seconds: float,
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
    deadline = time.monotonic() + worker_timeout_seconds
    try:
        if not receiver.poll(max(0.0, deadline - time.monotonic())):
            raise TimeoutError
        evidence = receiver.recv()
    except EOFError as exc:
        _terminate_worker(process)
        process.close()
        raise ValueError(f"{backend} worker produced no evidence") from exc
    except TimeoutError as exc:
        _terminate_worker(process)
        process.close()
        raise ValueError(f"{backend} worker exceeded timeout") from exc
    except BaseException:
        _terminate_worker(process)
        process.close()
        raise
    finally:
        receiver.close()
    process.join(max(0.0, deadline - time.monotonic()))
    if process.is_alive():
        _terminate_worker(process)
        process.close()
        raise ValueError(f"{backend} worker exceeded timeout")
    exit_code = process.exitcode
    process.close()
    if exit_code != 0 or not isinstance(evidence, dict) or "error" in evidence:
        raise ValueError(f"{backend} worker failed to produce valid evidence")
    evidence = validate_worker_evidence(
        backend,
        evidence,
        expected_frame_count=len(frames),
        expected_iterations=iterations,
    )
    timings = evidence.pop("timings", None)
    if not isinstance(timings, list) or not timings:
        raise ValueError(f"{backend} worker produced no timing evidence")
    evidence["p50_seconds"] = statistics.median(timings)
    evidence["p95_seconds"] = _percentile(timings, 0.95)
    evidence["frame_count"] = len(frames)
    evidence["detection_count"] = sum(len(items) for items in evidence["frames"])
    return evidence


def _terminate_worker(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(1.0)
    if process.is_alive():
        process.kill()
        process.join(1.0)


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def _build_report(
    results: dict[str, dict[str, Any]],
    *,
    warmup: int,
    iterations: int,
    worker_timeout_seconds: float,
) -> dict[str, Any]:
    baseline = results["pt"]
    baseline.update(
        {
            "exact_frame_count": True,
            "exact_ordered_classes_and_counts": True,
            "no_new_or_omitted_detections": True,
            "maximum_confidence_delta": 0.0,
            "minimum_bbox_iou": 1.0,
            "parity_passed": baseline["deterministic"],
            "resource_improvement_passed": False,
        }
    )
    for backend in ("onnx", "torchscript"):
        result = results[backend]
        result.update(parity_metrics(baseline["frames"], result["frames"]))
        result["parity_passed"] = (
            result["parity_passed"]
            and result["deterministic"]
            and baseline["deterministic"]
        )
        result["resource_improvement_passed"] = improved_by_resource_gate(
            baseline["p95_seconds"], result["p95_seconds"]
        ) or improved_by_resource_gate(
            baseline["peak_rss_bytes"], result["peak_rss_bytes"]
        )
    alternatives = [results["onnx"], results["torchscript"]]
    eligible = baseline["parity_passed"] and all(
        item["parity_passed"] for item in alternatives
    ) and any(
        item["resource_improvement_passed"] for item in alternatives
    )
    return {
        "schema_version": 1,
        "execution": "serial-spawned-subprocesses",
        "load_seconds_definition": "constructor-through-first-normalized-prediction",
        "warmup": warmup,
        "iterations": iterations,
        "worker_timeout_seconds": worker_timeout_seconds,
        "thresholds": {
            "minimum_bbox_iou": PARITY_MINIMUM_IOU,
            "maximum_confidence_delta": PARITY_MAXIMUM_CONFIDENCE_DELTA,
            "minimum_resource_improvement_ratio": RESOURCE_IMPROVEMENT_RATIO,
        },
        "backends": results,
        "production_switch_eligible": eligible,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if (
            args.warmup < 0
            or args.warmup > MAX_WARMUP
            or args.iterations < 1
            or args.iterations > MAX_ITERATIONS
            or not math.isfinite(args.worker_timeout_seconds)
            or args.worker_timeout_seconds <= 0
            or args.worker_timeout_seconds > MAX_WORKER_TIMEOUT_SECONDS
        ):
            raise ValueError("benchmark limits are outside the supported bounds")
        models = {
            "pt": args.pt_model,
            "onnx": args.onnx_model,
            "torchscript": args.torchscript_model,
        }
        frames = _load_manifest(args.manifest)
        identities = validated_model_identities(models)
        output_guard = validate_benchmark_output(
            args.output,
            protected_paths=[*models.values(), args.manifest, *frames],
        )
        results: dict[str, dict[str, Any]] = {}
        for backend, model in models.items():
            require_unchanged_models(identities)
            result = _run_isolated_backend(
                backend,
                model,
                frames,
                warmup=args.warmup,
                iterations=args.iterations,
                worker_timeout_seconds=args.worker_timeout_seconds,
            )
            require_unchanged_models(identities)
            result["model_sha256"] = identities[backend].sha256
            result["model_size_bytes"] = identities[backend].size_bytes
            results[backend] = result
        write_guarded_report(
            output_guard,
            _build_report(
                results,
                warmup=args.warmup,
                iterations=args.iterations,
                worker_timeout_seconds=args.worker_timeout_seconds,
            ),
            before_publish=lambda: require_unchanged_models(identities),
        )
    except ValueError as exc:
        print(f"benchmark input/evidence error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
