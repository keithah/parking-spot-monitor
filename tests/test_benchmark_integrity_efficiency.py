from __future__ import annotations

import json
from pathlib import Path

from parking_spot_monitor import detector_benchmark_corpus, detector_benchmark_model_snapshot
from parking_spot_monitor.detector_benchmark_corpus import prepare_corpus
from parking_spot_monitor.detector_benchmark_model_snapshot import prepare_model_snapshots


def test_unchanged_corpus_uses_metadata_checks_until_final_validation(
    tmp_path: Path, monkeypatch
) -> None:
    frame = tmp_path / "frame.jpg"
    frame.write_bytes(b"frame-payload")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"frames": [frame.name]}), encoding="utf-8")
    corpus = prepare_corpus(manifest, warmup=0, iterations=1)
    reads = 0
    real_read = detector_benchmark_corpus.read_identity

    def counted(*args, **kwargs):
        nonlocal reads
        reads += 1
        return real_read(*args, **kwargs)

    monkeypatch.setattr(detector_benchmark_corpus, "read_identity", counted)
    try:
        corpus.require_unchanged()
        assert reads == 0
        corpus.require_unchanged(comprehensive=True)
        assert reads == 4
    finally:
        corpus.close()


def test_changed_corpus_identity_is_hashed_once_before_rejection(
    tmp_path: Path, monkeypatch
) -> None:
    frame = tmp_path / "frame.jpg"
    frame.write_bytes(b"frame-payload")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"frames": [frame.name]}), encoding="utf-8")
    corpus = prepare_corpus(manifest, warmup=0, iterations=1)
    reads = 0
    real_read = detector_benchmark_corpus.read_identity
    frame.write_bytes(b"changed-frame")

    def counted(*args, **kwargs):
        nonlocal reads
        reads += 1
        return real_read(*args, **kwargs)

    monkeypatch.setattr(detector_benchmark_corpus, "read_identity", counted)
    try:
        try:
            corpus.require_unchanged()
        except ValueError as exc:
            assert "frame changed" in str(exc)
        else:
            raise AssertionError("changed frame was accepted")
        assert reads == 1
    finally:
        corpus.close()


def test_unchanged_models_defer_full_hashing_until_final_validation(
    tmp_path: Path, monkeypatch
) -> None:
    models = {
        "pt": tmp_path / "model.pt",
        "onnx": tmp_path / "model.onnx",
        "torchscript": tmp_path / "model.torchscript",
    }
    for index, path in enumerate(models.values()):
        path.write_bytes(f"model-{index}".encode())
    snapshots = prepare_model_snapshots(models)
    reads = 0
    real_read = detector_benchmark_model_snapshot.read_model_identity

    def counted(*args, **kwargs):
        nonlocal reads
        reads += 1
        return real_read(*args, **kwargs)

    monkeypatch.setattr(detector_benchmark_model_snapshot, "read_model_identity", counted)
    try:
        snapshots.require_unchanged()
        assert reads == 0
        snapshots.require_unchanged(comprehensive=True)
        assert reads == 6
    finally:
        snapshots.close()
