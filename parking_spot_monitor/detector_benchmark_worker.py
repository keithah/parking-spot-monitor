from __future__ import annotations

import json
import math
import multiprocessing
import os
import resource
import secrets
import stat
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from parking_spot_monitor.detection import _normalize_ultralytics_results
from parking_spot_monitor.detector_benchmark_evidence import (
    MAX_DETECTIONS_PER_FRAME,
    normalized_frames_hash,
    validated_detection_payload,
    validate_worker_evidence,
)


MAX_WORKER_EVIDENCE_BYTES = 64 * 1024 * 1024
_TERMINATION_GRACE_SECONDS = 0.2


def run_isolated_backend(
    backend: str,
    model_path: Path,
    frames: list[Path],
    *,
    warmup: int,
    iterations: int,
    worker_timeout_seconds: float,
) -> dict[str, Any]:
    context = multiprocessing.get_context("spawn")
    with tempfile.TemporaryDirectory(
        prefix=f"detector-benchmark-{backend}-evidence-"
    ) as evidence_directory:
        os.chmod(evidence_directory, 0o700)
        evidence_path = Path(evidence_directory) / "evidence.json"
        process = context.Process(
            target=_run_backend_worker,
            args=(
                evidence_directory,
                evidence_path.name,
                backend,
                str(model_path),
                [str(frame) for frame in frames],
                warmup,
                iterations,
            ),
        )
        deadline = time.monotonic() + worker_timeout_seconds
        try:
            process.start()
        except BaseException:
            process.close()
            raise
        try:
            process.join(max(0.0, deadline - time.monotonic()))
            if process.is_alive() or time.monotonic() > deadline:
                _terminate_worker(process)
                raise ValueError(f"{backend} worker exceeded timeout")
            exit_code = process.exitcode
            if exit_code != 0:
                raise ValueError(f"{backend} worker failed to produce valid evidence")
            evidence = _read_worker_evidence(evidence_path, backend)
        except BaseException:
            _terminate_worker(process)
            raise
        finally:
            process.close()
    if not isinstance(evidence, dict) or "error" in evidence:
        raise ValueError(f"{backend} worker failed to produce valid evidence")
    validated = validate_worker_evidence(
        backend,
        evidence,
        expected_frame_count=len(frames),
        expected_iterations=iterations,
    )
    timings = validated.pop("timings", None)
    if not isinstance(timings, list) or not timings:
        raise ValueError(f"{backend} worker produced no timing evidence")
    validated["p50_seconds"] = statistics.median(timings)
    validated["p95_seconds"] = _percentile(timings, 0.95)
    validated["frame_count"] = len(frames)
    validated["detection_count"] = sum(len(items) for items in validated["frames"])
    return validated


def _run_backend_worker(
    evidence_directory: str,
    evidence_name: str,
    backend: str,
    model_path: str,
    frame_paths: list[str],
    warmup: int,
    iterations: int,
) -> None:
    evidence_path = str(Path(evidence_directory) / evidence_name)
    os.environ["PARKING_BENCHMARK_EVIDENCE_PATH"] = evidence_path
    try:
        evidence = _benchmark_backend(
            backend, model_path, frame_paths, warmup, iterations
        )
    except Exception as exc:
        evidence = {
            "error": "backend benchmark failed",
            "error_type": type(exc).__name__,
        }
    _write_worker_evidence(evidence_directory, evidence_name, evidence)


def _benchmark_backend(
    backend: str,
    model_path: str,
    frame_paths: list[str],
    warmup: int,
    iterations: int,
) -> dict[str, Any]:
    from ultralytics import YOLO

    load_started = time.perf_counter()
    model = YOLO(model_path)
    readiness = _normalize_ultralytics_results(
        model.predict(source=frame_paths[0], verbose=False)
    )
    _validated_detections(readiness)
    load_seconds = time.perf_counter() - load_started
    measured_seconds: list[float] = []
    normalized_frames: list[list[dict[str, Any]]] | None = None
    normalized_frames_sha256: str | None = None
    mismatch_count = 0
    first_mismatch: int | None = None
    for cycle in range(warmup + iterations):
        cycle_frames: list[list[dict[str, Any]]] = []
        for frame_path in frame_paths:
            started = time.perf_counter()
            normalized = _normalize_ultralytics_results(
                model.predict(source=frame_path, verbose=False)
            )
            elapsed = time.perf_counter() - started
            cycle_frames.append(_validated_detections(normalized))
            if cycle >= warmup:
                measured_seconds.append(elapsed)
        if cycle >= warmup:
            iteration = cycle - warmup + 1
            cycle_hash = normalized_frames_hash(cycle_frames)
            if normalized_frames is None:
                normalized_frames = cycle_frames
                normalized_frames_sha256 = cycle_hash
            elif (
                cycle_hash != normalized_frames_sha256
                or cycle_frames != normalized_frames
            ):
                mismatch_count += 1
                if first_mismatch is None:
                    first_mismatch = iteration
    if normalized_frames is None or normalized_frames_sha256 is None:
        raise ValueError("backend produced no measured normalized evidence")
    return {
        "backend": backend,
        "worker_pid": os.getpid(),
        "load_seconds": load_seconds,
        "timings": measured_seconds,
        "peak_rss_bytes": _peak_rss_bytes(),
        "frames": normalized_frames,
        "normalized_frames_sha256": normalized_frames_sha256,
        "measured_iteration_count": iterations,
        "deterministic": mismatch_count == 0,
        "mismatch_iteration_count": mismatch_count,
        "first_mismatch_iteration": first_mismatch,
    }


def _validated_detections(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(items) > MAX_DETECTIONS_PER_FRAME:
        raise ValueError("backend exceeded the per-frame detection bound")
    return [validated_detection_payload(item) for item in items]


def _write_worker_evidence(directory: str, name: str, evidence: dict[str, Any]) -> None:
    encoded = json.dumps(evidence, allow_nan=False, separators=(",", ":")).encode()
    if len(encoded) > MAX_WORKER_EVIDENCE_BYTES:
        raise ValueError("worker evidence exceeds the supported bound")
    directory_descriptor = os.open(
        directory,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    temporary_name = f".evidence-{secrets.token_hex(16)}"
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_descriptor,
        )
        try:
            _write_all(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(
            temporary_name,
            name,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
        )
        os.fsync(directory_descriptor)
    finally:
        try:
            os.unlink(temporary_name, dir_fd=directory_descriptor)
        except FileNotFoundError:
            pass
        os.close(directory_descriptor)


def _read_worker_evidence(path: Path, backend: str) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError(f"{backend} worker produced no evidence") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size > MAX_WORKER_EVIDENCE_BYTES
        ):
            raise ValueError(
                f"{backend} worker evidence is outside the supported bound"
            )
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(
            descriptor,
            min(1024 * 1024, MAX_WORKER_EVIDENCE_BYTES + 1 - total),
        ):
            total += len(chunk)
            if total > MAX_WORKER_EVIDENCE_BYTES:
                raise ValueError(f"{backend} worker evidence is outside the supported bound")
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if _stable_fields(before) != _stable_fields(after) or total != after.st_size:
        raise ValueError(f"{backend} worker evidence changed while reading")
    try:
        payload = json.loads(b"".join(chunks))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{backend} worker produced malformed evidence") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{backend} worker produced malformed evidence")
    return payload


def _terminate_worker(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(_TERMINATION_GRACE_SECONDS)
    if process.is_alive():
        process.kill()
        process.join()


def _peak_rss_bytes() -> int:
    peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return peak if sys.platform == "darwin" else peak * 1024


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def _stable_fields(item: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        item.st_dev,
        item.st_ino,
        item.st_size,
        item.st_mtime_ns,
        item.st_ctime_ns,
    )


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        offset += os.write(descriptor, payload[offset:])
