from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from parking_spot_monitor.detector_benchmark_evidence import validate_worker_evidence
from parking_spot_monitor.detector_benchmark_output import (
    validate_benchmark_output,
    write_guarded_report,
)


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
    mutate_frame_from_backend: str | None = None,
    restore_frame_bytes: bool = False,
    mutate_manifest_order_from_backend: str | None = None,
    second_frame: bool = False,
    symlink_frame: bool = False,
    oversize_frame: bool = False,
    partial_stall_backend: str | None = None,
    late_backend: str | None = None,
    restore_model_bytes: bool = False,
    fifo_input: str | None = None,
    mutate_private_snapshot: str | None = None,
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
        Path(os.environ["FAKE_BACKEND_EVIDENCE"]).with_name(f"started-{self.backend}").write_text(str(os.getpid()))
        Path(os.environ["FAKE_BACKEND_EVIDENCE"]).with_name(f"loaded-{self.backend}.json").write_text(json.dumps({
            "path": str(model_path),
            "bytes": Path(model_path).read_bytes().hex(),
        }))
        if self.backend == "pt" and self.evidence.get("mutate_private_snapshot") in {"own-model", "sibling-model"}:
            target = Path(model_path)
            if self.evidence["mutate_private_snapshot"] == "sibling-model":
                target = target.with_name("onnx.onnx")
            original = target.read_bytes()
            target.chmod(0o600)
            target.write_bytes(b"s" * len(original))
        if self.backend == "torchscript" and self.evidence.get("swap_output_to"):
            output = Path(self.evidence["output_path"])
            output.unlink(missing_ok=True)
            os.link(self.evidence["swap_output_to"], output)

    def predict(self, *, source, verbose=False):
        if self.backend == "pt" and self.evidence.get("mutate_private_snapshot") == "frame":
            target = Path(source)
            original = target.read_bytes()
            target.chmod(0o600)
            target.write_bytes(b"f" * len(original))
        if self.backend == self.evidence.get("partial_stall_backend"):
            Path(os.environ["PARKING_BENCHMARK_EVIDENCE_PATH"]).write_text('{"backend":')
            time.sleep(5)
        if self.backend == self.evidence.get("late_backend"):
            time.sleep(0.8)
        if self.backend == self.evidence.get("mutate_from_backend") and self.evidence.get("mutate_model"):
            target = Path(self.evidence["mutate_model"])
            original = target.read_bytes()
            original_stat = target.stat()
            target.write_bytes(b"x" * len(original))
            if self.evidence.get("restore_model_bytes"):
                target.write_bytes(original)
                os.utime(target, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
        if self.backend == self.evidence.get("mutate_frame_from_backend"):
            target = Path(self.evidence["original_frame"])
            original = target.read_bytes()
            target.write_bytes(b"mutated-frame")
            if self.evidence.get("restore_frame_bytes"):
                target.write_bytes(original)
        if self.backend == self.evidence.get("mutate_manifest_order_from_backend"):
            manifest = Path(self.evidence["manifest_path"])
            frames = json.loads(manifest.read_text())["frames"]
            manifest.write_text(json.dumps({"frames": list(reversed(frames))}))
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
    if oversize_frame:
        frame.write_bytes(b"")
        with frame.open("r+b") as handle:
            handle.truncate(32 * 1024 * 1024 + 1)
    if symlink_frame:
        target = tmp_path / "real-frame-a.jpg"
        frame.replace(target)
        frame.symlink_to(target.name)
    frames = [frame]
    if second_frame:
        frame_b = tmp_path / "frame-b.jpg"
        frame_b.write_bytes(b"fake-frame-b")
        frames.append(frame_b)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"frames": [item.name for item in frames]}),
        encoding="utf-8",
    )
    if fifo_input == "manifest":
        manifest.unlink()
        os.mkfifo(manifest)
    models = {
        "pt": tmp_path / "model.pt",
        "onnx": tmp_path / "model.onnx",
        "torchscript": tmp_path / "model.torchscript",
    }
    for backend, model in models.items():
        model.write_bytes(f"fake-{backend}-model".encode())
    if fifo_input == "frame":
        frame.unlink()
        os.mkfifo(frame)
    if fifo_input == "model":
        models["pt"].unlink()
        os.mkfifo(models["pt"])
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
                "delays": {"pt": 0.01, "onnx": 0.0, "torchscript": 0.0},
                "detections": {
                    "pt": {item.name: [_detection()] for item in frames},
                    "onnx": {item.name: onnx_detections for item in frames},
                    "torchscript": {item.name: [_detection()] for item in frames},
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
                "mutate_frame_from_backend": mutate_frame_from_backend,
                "restore_frame_bytes": restore_frame_bytes,
                "original_frame": str(frame),
                "mutate_manifest_order_from_backend": (
                    mutate_manifest_order_from_backend
                ),
                "manifest_path": str(manifest),
                "partial_stall_backend": partial_stall_backend,
                "late_backend": late_backend,
                "restore_model_bytes": restore_model_bytes,
                "mutate_private_snapshot": mutate_private_snapshot,
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
    benchmark_temp = tmp_path / "benchmark-temp"
    benchmark_temp.mkdir()
    environment["TMPDIR"] = str(benchmark_temp)
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
    try:
        result = subprocess.run(
            command,
            cwd=Path.cwd(),
            env=environment,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=1.5 if fifo_input else 30,
        )
    except subprocess.TimeoutExpired:
        result = subprocess.CompletedProcess(
            command,
            -999,
            stdout="",
            stderr="benchmark blocked on special input",
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
    assert report["backends"]["pt"]["load_seconds"] >= 0.009
    model_hashes = {
        report["backends"][name]["model_sha256"]
        for name in ("pt", "onnx", "torchscript")
    }
    assert len(model_hashes) == 3
    assert all(len(digest) == 64 for digest in model_hashes)
    assert all(
        report["backends"][name]["model_snapshot_sha256"]
        == report["backends"][name]["model_sha256"]
        for name in ("pt", "onnx", "torchscript")
    )
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
        worker_timeout_seconds=0.5,
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
    assert "benchmark input/evidence error" in result.stderr
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


def test_backend_benchmark_snapshots_models_and_rejects_transient_restore(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        mutate_target="pt",
        mutate_from_backend="pt",
        restore_model_bytes=True,
    )

    loaded = json.loads((tmp_path / "loaded-pt.json").read_text())
    assert result.returncode == 2
    assert "pt model changed after preflight validation" in result.stderr
    assert report == {}
    assert loaded["path"] != str(tmp_path / "model.pt")
    assert loaded["path"].endswith(".pt")
    assert bytes.fromhex(loaded["bytes"]) == b"fake-pt-model"
    assert list((tmp_path / "benchmark-temp").iterdir()) == []
    assert not (tmp_path / "started-onnx").exists()


@pytest.mark.parametrize(
    "snapshot_target",
    ["own-model", "sibling-model", "frame"],
)
def test_backend_benchmark_rejects_any_private_snapshot_mutation(
    tmp_path: Path,
    snapshot_target: str,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        mutate_private_snapshot=snapshot_target,
    )

    assert result.returncode == 2
    assert "snapshot changed after preflight validation" in result.stderr
    assert report == {}
    assert (tmp_path / "started-pt").is_file()
    assert not (tmp_path / "started-onnx").exists()
    assert not (tmp_path / "report.json").exists()
    assert list((tmp_path / "benchmark-temp").iterdir()) == []


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


def test_backend_benchmark_records_bound_ordered_corpus_provenance(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        second_frame=True,
    )

    assert result.returncode == 0, result.stderr
    assert report["corpus"]["frame_count"] == 2
    assert len(report["corpus"]["manifest_sha256"]) == 64
    assert len(report["corpus"]["ordered_frame_sha256"]) == 2
    assert all(
        len(digest) == 64
        for digest in report["corpus"]["ordered_frame_sha256"]
    )
    assert (
        report["corpus"]["manifest_snapshot_sha256"]
        == report["corpus"]["manifest_sha256"]
    )
    assert (
        report["corpus"]["ordered_frame_snapshot_sha256"]
        == report["corpus"]["ordered_frame_sha256"]
    )
    assert len(report["corpus"]["corpus_sha256"]) == 64


@pytest.mark.parametrize("restore_frame_bytes", [False, True])
def test_backend_benchmark_rejects_later_worker_frame_mutation(
    tmp_path: Path,
    restore_frame_bytes: bool,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        mutate_frame_from_backend="onnx",
        restore_frame_bytes=restore_frame_bytes,
    )

    assert result.returncode == 2
    assert "frame changed after corpus preflight" in result.stderr
    assert report == {}
    assert not (tmp_path / "started-torchscript").exists()


def test_backend_benchmark_rejects_manifest_order_mutation(
    tmp_path: Path,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        second_frame=True,
        mutate_manifest_order_from_backend="onnx",
    )

    assert result.returncode == 2
    assert "manifest changed after corpus preflight" in result.stderr
    assert report == {}
    assert not (tmp_path / "started-torchscript").exists()


@pytest.mark.parametrize("unsafe_frame", ["symlink", "oversize"])
def test_backend_benchmark_rejects_unsafe_corpus_frames_before_workers(
    tmp_path: Path,
    unsafe_frame: str,
) -> None:
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        symlink_frame=unsafe_frame == "symlink",
        oversize_frame=unsafe_frame == "oversize",
    )

    assert result.returncode == 2
    assert "frame" in result.stderr
    assert report == {}
    assert not (tmp_path / "started-pt").exists()


@pytest.mark.parametrize("worker_case", ["partial-stall", "late-completion"])
def test_backend_benchmark_enforces_absolute_deadline_and_cleans_worker_state(
    tmp_path: Path,
    worker_case: str,
) -> None:
    started = time.monotonic()
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        partial_stall_backend="onnx" if worker_case == "partial-stall" else None,
        late_backend="onnx" if worker_case == "late-completion" else None,
        worker_timeout_seconds=0.3,
    )
    elapsed = time.monotonic() - started

    assert result.returncode == 2
    assert "onnx worker exceeded timeout" in result.stderr
    assert elapsed < 2.0
    assert report == {}
    worker_pid = int((tmp_path / "started-onnx").read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(worker_pid, 0)
    assert list((tmp_path / "benchmark-temp").iterdir()) == []
    assert not (tmp_path / "started-torchscript").exists()


@pytest.mark.parametrize(
    "replacement_phase",
    ["before-temp", "before-replace", "after-replace-fsync"],
)
def test_guarded_report_rejects_parent_replacement_at_every_publication_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_phase: str,
) -> None:
    from parking_spot_monitor import detector_benchmark_output as output_module

    parent = tmp_path / "evidence"
    parent.mkdir()
    protected = tmp_path / "manifest.json"
    protected.write_text('{"frames":["frame.jpg"]}', encoding="utf-8")
    output = parent / "report.json"
    moved_parent = tmp_path / "evidence-moved"
    guard = validate_benchmark_output(output, protected_paths=[protected])
    replaced = False

    def replace_parent() -> None:
        nonlocal replaced
        if replaced:
            return
        parent.rename(moved_parent)
        parent.mkdir()
        replaced = True

    if replacement_phase == "before-temp":
        replace_parent()
    before_publish = (
        replace_parent if replacement_phase == "before-replace" else lambda: None
    )
    if replacement_phase == "after-replace-fsync":
        real_fsync = output_module.os.fsync

        def replace_after_directory_fsync(descriptor: int) -> None:
            real_fsync(descriptor)
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                replace_parent()

        monkeypatch.setattr(output_module.os, "fsync", replace_after_directory_fsync)

    with pytest.raises(ValueError, match="output parent changed"):
        write_guarded_report(
            guard,
            {"schema_version": 1},
            before_publish=before_publish,
        )

    assert not output.exists()
    assert not list(parent.glob(".report.json.tmp-*"))
    assert not list(moved_parent.glob(".report.json.tmp-*"))
    assert not (moved_parent / "report.json").exists()
    guard.close()


def test_guarded_report_removes_owned_temporary_when_file_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parking_spot_monitor import detector_benchmark_output as output_module

    parent = tmp_path / "evidence"
    parent.mkdir()
    protected = tmp_path / "manifest.json"
    protected.write_text('{"frames":["frame.jpg"]}', encoding="utf-8")
    output = parent / "report.json"
    guard = validate_benchmark_output(output, protected_paths=[protected])
    real_fsync = output_module.os.fsync

    def fail_file_fsync(descriptor: int) -> None:
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("injected file fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(output_module.os, "fsync", fail_file_fsync)

    with pytest.raises(OSError, match="injected file fsync failure"):
        write_guarded_report(
            guard,
            {"schema_version": 1},
            before_publish=lambda: None,
        )

    assert not output.exists()
    assert not list(parent.glob(".report.json.tmp-*"))
    guard.close()


@pytest.mark.parametrize("fifo_input", ["manifest", "frame", "model"])
def test_backend_benchmark_rejects_fifo_inputs_without_blocking(
    tmp_path: Path,
    fifo_input: str,
) -> None:
    started = time.monotonic()
    result, report = _run_fake_benchmark(
        tmp_path,
        onnx_detections=[_detection()],
        fifo_input=fifo_input,
    )
    elapsed = time.monotonic() - started

    assert result.returncode == 2, result.stderr
    assert elapsed < 1.5
    assert "benchmark input/evidence error" in result.stderr
    assert report == {}
    assert not (tmp_path / "started-pt").exists()
    assert not (tmp_path / "report.json").exists()
    assert list((tmp_path / "benchmark-temp").iterdir()) == []
