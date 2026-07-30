from __future__ import annotations

import ast
import inspect
from pathlib import Path

from parking_spot_monitor import detector_benchmark_evidence
from parking_spot_monitor import diagnostic_bounding
from parking_spot_monitor import decision_memory_runtime


ROOT = Path(__file__).resolve().parents[1]


def test_parity_extrema_are_accumulated_without_per_detection_arrays() -> None:
    tree = ast.parse(inspect.getsource(detector_benchmark_evidence.parity_metrics))
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "append"
        for node in ast.walk(tree)
    )


def test_decision_memory_factory_uses_a_typed_settings_boundary_without_reflection() -> None:
    source = inspect.getsource(decision_memory_runtime.runtime_decision_memory_store)
    annotation = inspect.signature(
        decision_memory_runtime.runtime_decision_memory_store
    ).parameters["runtime_settings"].annotation
    assert annotation not in {object, "object"}
    assert "getattr" not in source


def test_diagnostic_bounding_has_a_public_api_and_callers_use_it() -> None:
    assert diagnostic_bounding.take_bounded(range(4), 2) == ([0, 1], True)
    for relative in (
        "parking_spot_monitor/operator_decision_memory.py",
        "parking_spot_monitor/operator_feedback_models.py",
        "parking_spot_monitor/detection_lab.py",
    ):
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "import _take_bounded" not in source
