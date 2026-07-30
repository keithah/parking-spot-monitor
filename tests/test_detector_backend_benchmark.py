from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from parking_spot_monitor.detector_benchmark_evidence import validate_worker_evidence


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
    symlink_backend: str | None = None,
    onnx_sequence: list[list[dict[str, Any]]] | None = None,
    hang_backend: str | None = None,
    worker_timeout_seconds: float | None = None,
    duplicate_model_content: bool = False,
    crosswire_models: bool = False,
    mutate_onnx_from_pt: bool = False,
    mutate_target: str | None = None,
    mutate_from_backend: str | None = None,
    output_alias: str | None = None,
    output_hardlink: str | None = None,
    swap_output_to: str | None = None,
    output_symlink: str | None = None,
    output_directory: bool = False,
    output_parent_symlink: bool = False,
    output_lexical_alias: str | None = None,
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
        self.calls = {}
        Path(os.environ["FAKE_BACKEND_EVIDENCE"]).with_name(f"started-{self.backend}").touch()
        if self.backend == "torchscript" and self.evidence.get("swap_output_to"):
            output = Path(self.evidence["output_path"])
            output.unlink(missing_ok=True)
            os.link(self.evidence["swap_output_to"], output)

    def predict(self, *, source, verbose=False):
        if self.backend == self.evidence.get("mutate_from_backend") and self.evidence.get("mutate_model"):
            Path(self.evidence["mutate_model"]).write_bytes(b"mutated-model")
        if self.backend == self.evidence.get("hang_backend"):
            time.sleep(5)
        time.sleep(float(self.evidence["delays"].get(self.backend, 0.0)))
        frame_name = Path(source).name
        call_index = self.calls.get(frame_name, 0)
        self.calls[frame_name] = call_index + 1
        sequence = self.evidence.get("sequences", {}).get(self.backend)
        detections = (
            sequence[min(call_index, len(sequence) - 1)]
            if sequence
            else self.evidence["detections"][self.backend][frame_name]
        )
        return [_Result(detections)]
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
    for backend, model in models.items():
        model.write_bytes(f"fake-{backend}-model".encode())
    if duplicate_model_content:
        models["onnx"].write_bytes(models["pt"].read_bytes())
    if symlink_backend is not None:
        model = models[symlink_backend]
        target = model.with_name(f"real-{model.name}")
        model.replace(target)
        model.symlink_to(target.name)
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
                "sequences": (
                    {"onnx": onnx_sequence} if onnx_sequence is not None else {}
                ),
                "hang_backend": hang_backend,
                "mutate_model": (
                    str(models[mutate_target or "onnx"])
                    if mutate_onnx_from_pt or mutate_target
                    else None
                ),
                "mutate_from_backend": (
                    mutate_from_backend or ("pt" if mutate_onnx_from_pt else None)
                ),
                "output_path": str(tmp_path / "report.json"),
                "swap_output_to": (
                    str(models[swap_output_to]) if swap_output_to else None
                ),
            }
        ),
        encoding="utf-8",
    )
    aliases = {
        "pt": models["pt"],
        "onnx": models["onnx"],
        "torchscript": models["torchscript"],
        "manifest": manifest,
        "frame": frame,
    }
    if output_parent_symlink:
        real_parent = tmp_path / "real-output-parent"
        real_parent.mkdir()
        linked_parent = tmp_path / "linked-output-parent"
        linked_parent.symlink_to(real_parent.name, target_is_directory=True)
        output = linked_parent / "report.json"
    elif output_lexical_alias:
        output = tmp_path / "missing-parent" / ".." / aliases[output_lexical_alias].name
    else:
        output = aliases[output_alias] if output_alias else tmp_path / "report.json"
    if output_hardlink is not None:
        os.link(aliases[output_hardlink], output)
    if output_symlink is not None:
        output.symlink_to(aliases[output_symlink])
    if output_directory:
        output.mkdir()
    cli_models = dict(models)
    if crosswire_models:
        cli_models["pt"], cli_models["onnx"] = cli_models["onnx"], cli_models["pt"]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(fake_modules), str(Path.cwd()), environment.get("PYTHONPATH", "")]
    )
    environment["FAKE_BACKEND_EVIDENCE"] = str(evidence)
    command = [
            sys.executable,
            "scripts/benchmark_detector_backends.py",
            "--manifest",
            str(manifest),
            "--pt-model",
            str(cli_models["pt"]),
            "--onnx-model",
            str(cli_models["onnx"]),
            "--torchscript-model",
            str(models["torchscript"]),
            "--output",
            str(output),
            "--warmup",
            "0",
            "--iterations",
            "2",
        ]
    if worker_timeout_seconds is not None:
        command.extend(["--worker-timeout-seconds", str(worker_timeout_seconds)])
    result = subprocess.run(
        command,
        cwd=Path.cwd(),
        env=environment,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    report = (
        json.loads(output.read_text(encoding="utf-8"))
        if result.returncode == 0 and output.exists()
        else {}
    )
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
    assert report["load_seconds_definition"] == (
        "constructor-through-first-normalized-prediction"
    )
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
    assert report["backends"]["pt"]["load_seconds"] >= 0.0015
    model_hashes = {
        report["backends"][name]["model_sha256"]
        for name in ("pt", "onnx", "torchscript")
    }
    assert len(model_hashes) == 3
    assert all(len(digest) == 64 for digest in model_hashes)
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


def test_backend_benchmark_returns_two_for_nonfinite_worker_evidence(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[
            _detection(bbox=[10.0, 20.0, float("nan"), 90.0])
        ],
    )

    assert result.returncode == 2
    assert "benchmark input/evidence error" in result.stderr
    assert report == {}


def test_backend_benchmark_rejects_symlinked_model_before_spawning(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        symlink_backend="onnx",
    )

    assert result.returncode == 2
    assert "benchmark input/evidence error" in result.stderr
    assert report == {}


def test_backend_benchmark_rejects_late_matching_nondeterministic_results(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        onnx_sequence=[
            [_detection()],
            [_detection(bbox=[10.0, 20.0, 80.0, 90.0])],
            [_detection()],
        ],
    )

    assert result.returncode == 0, result.stderr
    assert report["backends"]["onnx"]["deterministic"] is False
    assert report["backends"]["onnx"]["mismatch_iteration_count"] == 1
    assert report["backends"]["onnx"]["first_mismatch_iteration"] == 2
    assert report["production_switch_eligible"] is False


def test_backend_benchmark_terminates_a_hanging_worker_before_next_backend(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        hang_backend="onnx",
        worker_timeout_seconds=0.2,
    )

    assert result.returncode == 2
    assert "onnx worker exceeded timeout" in result.stderr
    assert report == {}
    assert (tmp_path / "started-pt").is_file()
    assert (tmp_path / "started-onnx").is_file()
    assert not (tmp_path / "started-torchscript").exists()


def test_worker_evidence_validation_rejects_malformed_maps() -> None:
    with pytest.raises(ValueError, match="worker evidence"):
        validate_worker_evidence(
            "onnx",
            {"backend": "onnx", "timings": [0.1]},
            expected_frame_count=1,
            expected_iterations=2,
        )


@pytest.mark.parametrize("case", ["duplicate-content", "cross-wired"])
def test_backend_benchmark_rejects_non_distinct_or_cross_wired_models(
    tmp_path: Path,
    case: str,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        duplicate_model_content=case == "duplicate-content",
        crosswire_models=case == "cross-wired",
    )

    assert result.returncode == 2
    assert "benchmark input/evidence error" in result.stderr
    assert report == {}
    assert not (tmp_path / "started-pt").exists()


def test_backend_benchmark_rejects_model_mutation_before_its_worker(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        mutate_onnx_from_pt=True,
    )

    assert result.returncode == 2
    assert "onnx model changed after preflight validation" in result.stderr
    assert report == {}
    assert (tmp_path / "started-pt").is_file()
    assert not (tmp_path / "started-onnx").exists()


def test_backend_benchmark_never_uses_a_model_as_its_output(tmp_path: Path) -> None:
    original = b"fake-pt-model"
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        output_alias="pt",
    )

    assert result.returncode == 2
    assert "benchmark output" in result.stderr
    assert report == {}
    assert (tmp_path / "model.pt").read_bytes() == original
    assert not (tmp_path / "started-pt").exists()


@pytest.mark.parametrize(
    ("output_alias", "output_hardlink"),
    [
        ("manifest", None),
        ("frame", None),
        (None, "manifest"),
        (None, "frame"),
        (None, "pt"),
    ],
)
def test_backend_benchmark_rejects_output_aliases_to_nonmodel_inputs(
    tmp_path: Path,
    output_alias: str | None,
    output_hardlink: str | None,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        output_alias=output_alias,
        output_hardlink=output_hardlink,
    )

    assert result.returncode == 2
    assert "benchmark output" in result.stderr
    assert report == {}
    assert not (tmp_path / "started-pt").exists()


def test_backend_benchmark_rechecks_output_alias_before_atomic_publication(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        swap_output_to="pt",
    )

    assert result.returncode == 2
    assert "benchmark output changed after preflight" in result.stderr
    assert report == {}
    assert (tmp_path / "model.pt").read_bytes() == b"fake-pt-model"


def test_backend_benchmark_revalidates_earlier_models_after_later_workers(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        mutate_target="pt",
        mutate_from_backend="onnx",
    )

    assert result.returncode == 2
    assert "pt model changed after preflight validation" in result.stderr
    assert report == {}
    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "started-torchscript").exists()


@pytest.mark.parametrize(
    "unsafe_output",
    ["symlink", "directory", "parent-symlink", "lexical-alias"],
)
def test_backend_benchmark_rejects_unsafe_output_paths(
    tmp_path: Path,
    unsafe_output: str,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        output_symlink="manifest" if unsafe_output == "symlink" else None,
        output_directory=unsafe_output == "directory",
        output_parent_symlink=unsafe_output == "parent-symlink",
        output_lexical_alias="pt" if unsafe_output == "lexical-alias" else None,
    )

    assert result.returncode == 2
    assert "benchmark output" in result.stderr
    assert report == {}
    assert not (tmp_path / "started-pt").exists()
