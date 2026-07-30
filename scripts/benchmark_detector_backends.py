#!/usr/bin/env python3
"""Benchmark detector exports offline without changing the production backend."""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parking_spot_monitor.detector_benchmark_corpus import (
    CorpusSnapshot,
    prepare_corpus,
)
from parking_spot_monitor.detector_benchmark_evidence import (
    PARITY_MAXIMUM_CONFIDENCE_DELTA,
    PARITY_MINIMUM_IOU,
    RESOURCE_IMPROVEMENT_RATIO,
    improved_by_resource_gate,
    parity_metrics,
)
from parking_spot_monitor.detector_benchmark_models import (
    require_unchanged_models,
    validated_model_identities,
)
from parking_spot_monitor.detector_benchmark_output import (
    validate_benchmark_output,
    write_guarded_report,
)
from parking_spot_monitor.detector_benchmark_worker import run_isolated_backend


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


def _build_report(
    results: dict[str, dict[str, Any]],
    *,
    corpus: dict[str, Any],
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
        "corpus": corpus,
        "thresholds": {
            "minimum_bbox_iou": PARITY_MINIMUM_IOU,
            "maximum_confidence_delta": PARITY_MAXIMUM_CONFIDENCE_DELTA,
            "minimum_resource_improvement_ratio": RESOURCE_IMPROVEMENT_RATIO,
        },
        "backends": results,
        "production_switch_eligible": eligible,
    }


def _require_unchanged_inputs(
    identities: dict[str, Any],
    corpus: CorpusSnapshot,
) -> None:
    require_unchanged_models(identities)
    corpus.require_unchanged()


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
        corpus = prepare_corpus(
            args.manifest,
            warmup=args.warmup,
            iterations=args.iterations,
        )
        try:
            identities = validated_model_identities(models)
            output_guard = validate_benchmark_output(
                args.output,
                protected_paths=[*models.values(), *corpus.protected_paths],
            )
            try:
                results: dict[str, dict[str, Any]] = {}
                for backend, model in models.items():
                    _require_unchanged_inputs(identities, corpus)
                    result = run_isolated_backend(
                        backend,
                        model,
                        list(corpus.snapshot_paths),
                        warmup=args.warmup,
                        iterations=args.iterations,
                        worker_timeout_seconds=args.worker_timeout_seconds,
                    )
                    _require_unchanged_inputs(identities, corpus)
                    result["model_sha256"] = identities[backend].sha256
                    result["model_size_bytes"] = identities[backend].size_bytes
                    results[backend] = result
                write_guarded_report(
                    output_guard,
                    _build_report(
                        results,
                        corpus=corpus.evidence,
                        warmup=args.warmup,
                        iterations=args.iterations,
                        worker_timeout_seconds=args.worker_timeout_seconds,
                    ),
                    before_publish=lambda: _require_unchanged_inputs(
                        identities, corpus
                    ),
                )
            finally:
                output_guard.close()
        finally:
            corpus.close()
    except ValueError as exc:
        print(f"benchmark input/evidence error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
