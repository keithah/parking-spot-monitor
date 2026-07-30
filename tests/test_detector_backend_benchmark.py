from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def _detection(*, bbox: list[float] | None = None) -> dict[str, Any]:
    return {
        "class_name": "car",
        "confidence": 0.8,
        "bbox": [10.0, 20.0, 110.0, 120.0] if bbox is None else bbox,
    }


def _run_fake_benchmark(
    tmp_path: Path,
    *,
    onnx_detections: list[dict[str, Any]],
) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
    fake_modules = tmp_path / "fake-modules"
    fake_modules.mkdir()
    (fake_modules / "torch.py").write_text(
        'raise AssertionError("standard benchmark tests must not import Torch")\n',
        encoding="utf-8",
    )
    (fake_modules / "ultralytics.py").write_text(
        """\
import json
import os
import time
from pathlib import Path

class _Boxes:
    def __init__(self, detections):
        self.xyxy = [item["bbox"] for item in detections]
        self.conf = [item["confidence"] for item in detections]
        self.cls = list(range(len(detections)))

class _Result:
    def __init__(self, detections):
        self.boxes = _Boxes(detections)
        self.names = {index: item["class_name"] for index, item in enumerate(detections)}

class YOLO:
    def __init__(self, model_path):
        self.backend = Path(model_path).suffix.removeprefix(".")
        self.evidence = json.loads(Path(os.environ["FAKE_BACKEND_EVIDENCE"]).read_text())

    def predict(self, *, source, verbose=False):
        time.sleep(float(self.evidence["delays"].get(self.backend, 0.0)))
        return [_Result(self.evidence["detections"][self.backend][Path(source).name])]
""",
        encoding="utf-8",
    )
    frame = tmp_path / "frame-a.jpg"
    frame.write_bytes(b"fake-frame")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"frames": [frame.name]}), encoding="utf-8")
    models = {
        "pt": tmp_path / "model.pt",
        "onnx": tmp_path / "model.onnx",
        "torchscript": tmp_path / "model.torchscript",
    }
    for model in models.values():
        model.write_bytes(b"fake-model")
    evidence = tmp_path / "fake-evidence.json"
    evidence.write_text(
        json.dumps(
            {
                "delays": {"pt": 0.002, "onnx": 0.0, "torchscript": 0.0},
                "detections": {
                    "pt": {frame.name: [_detection()]},
                    "onnx": {frame.name: onnx_detections},
                    "torchscript": {frame.name: [_detection()]},
                },
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "report.json"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(fake_modules), str(Path.cwd()), environment.get("PYTHONPATH", "")]
    )
    environment["FAKE_BACKEND_EVIDENCE"] = str(evidence)
    result = subprocess.run(
        [
            sys.executable,
            "scripts/benchmark_detector_backends.py",
            "--manifest",
            str(manifest),
            "--pt-model",
            str(models["pt"]),
            "--onnx-model",
            str(models["onnx"]),
            "--torchscript-model",
            str(models["torchscript"]),
            "--output",
            str(output),
            "--warmup",
            "0",
            "--iterations",
            "2",
        ],
        cwd=Path.cwd(),
        env=environment,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    report = json.loads(output.read_text(encoding="utf-8")) if output.exists() else {}
    return result, report


def test_backend_benchmark_marks_accuracy_mismatch_ineligible(tmp_path: Path) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection(bbox=[10.0, 20.0, 80.0, 90.0])],
    )

    assert result.returncode == 0, result.stderr
    assert report["backends"]["onnx"]["parity_passed"] is False
    assert report["backends"]["onnx"]["minimum_bbox_iou"] < 0.99
    assert report["production_switch_eligible"] is False


def test_backend_benchmark_isolates_each_backend_in_a_spawned_process(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
    )

    assert result.returncode == 0, result.stderr
    worker_pids = {
        backend["worker_pid"] for backend in report["backends"].values()
    }
    assert len(worker_pids) == 3
    assert os.getpid() not in worker_pids


def test_backend_benchmark_requires_all_parity_and_resource_improvement(
    tmp_path: Path,
) -> None:
    torch_was_loaded = "torch" in sys.modules
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
    )

    assert result.returncode == 0, result.stderr
    assert report["execution"] == "serial-spawned-subprocesses"
    assert report["warmup"] == 0
    assert report["iterations"] == 2
    assert report["thresholds"] == {
        "maximum_confidence_delta": 0.02,
        "minimum_bbox_iou": 0.99,
        "minimum_resource_improvement_ratio": 0.15,
    }
    assert all(
        report["backends"][name]["parity_passed"]
        for name in ("pt", "onnx", "torchscript")
    )
    assert report["backends"]["onnx"]["resource_improvement_passed"]
    assert report["production_switch_eligible"] is True
    assert ("torch" in sys.modules) is torch_was_loaded


def test_backend_benchmark_returns_two_for_missing_inputs(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/benchmark_detector_backends.py",
            "--manifest",
            str(tmp_path / "missing-manifest.json"),
            "--pt-model",
            str(tmp_path / "missing.pt"),
            "--onnx-model",
            str(tmp_path / "missing.onnx"),
            "--torchscript-model",
            str(tmp_path / "missing.torchscript"),
            "--output",
            str(tmp_path / "report.json"),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10,
    )

    assert result.returncode == 2
    assert "benchmark input/evidence error" in result.stderr
    assert not (tmp_path / "report.json").exists()


def test_backend_benchmark_returns_two_for_malformed_worker_evidence(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection(bbox=[10.0, 20.0, 80.0])],
    )

    assert result.returncode == 2
    assert "benchmark input/evidence error" in result.stderr
    assert report == {}
