from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _line_count(relative_path: str) -> int:
    return len((ROOT / relative_path).read_text(encoding="utf-8").splitlines())


def _imported_names(relative_path: str, module_name: str) -> list[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == module_name
        for alias in node.names
    ]


def _function_names(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}


def _string_constants(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)}


def _imports_module(relative_path: str, module_name: str) -> bool:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return any(isinstance(node, ast.ImportFrom) and node.module == module_name for node in ast.walk(tree))


def _imported_module_names(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            names.add(node.module)
    return names


def _lambda_calls_function(relative_path: str, function_name: str) -> bool:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Lambda):
            continue
        if any(
            isinstance(child, ast.Call) and isinstance(child.func, ast.Name) and child.func.id == function_name
            for child in ast.walk(node.body)
        ):
            return True
    return False


def _lambda_call_targets(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    targets: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Lambda):
            continue
        for child in ast.walk(node.body):
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                targets.add(child.func.attr)
    return targets


def test_matrix_module_is_a_small_compatibility_shim() -> None:
    assert _line_count("parking_spot_monitor/matrix.py") <= 220
    module_caps = {
        "parking_spot_monitor/matrix_client.py": 260,
        "parking_spot_monitor/matrix_commands.py": 740,
        "parking_spot_monitor/matrix_cockpit.py": 430,
        "parking_spot_monitor/matrix_delivery.py": 280,
        "parking_spot_monitor/matrix_models.py": 100,
        "parking_spot_monitor/matrix_snapshots.py": 430,
        "parking_spot_monitor/matrix_dispatch.py": 410,
    }
    for path, max_lines in module_caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines


def test_matrix_command_contract_is_not_asserted_from_source_text() -> None:
    docs_test_strings = _string_constants("tests/test_operator_docs.py")
    command_strings = _string_constants("parking_spot_monitor/matrix_commands.py")

    assert "parking_spot_monitor/matrix_commands.py" not in docs_test_strings
    assert "# type: ignore" not in command_strings
    assert "_applied_command_from_compat" not in command_strings
    assert "MatrixCommand | _AppliedMatrixCommand" not in command_strings


def test_matrix_compat_shim_does_not_reexport_private_helpers() -> None:
    tree = ast.parse((ROOT / "parking_spot_monitor/matrix.py").read_text(encoding="utf-8"))
    private_imports = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
        if alias.name.startswith("_")
    ]

    assert private_imports == []


def test_matrix_cockpit_context_has_explicit_reply_methods_instead_of_action_dispatch() -> None:
    cockpit_functions = _function_names("parking_spot_monitor/matrix_cockpit.py")
    cockpit_strings = _string_constants("parking_spot_monitor/matrix_cockpit.py")
    command_functions = _function_names("parking_spot_monitor/matrix_commands.py")

    assert "format_reply" not in cockpit_functions
    assert "unknown cockpit command" not in cockpit_strings
    assert "_format_cockpit_context_reply" not in command_functions


def test_runtime_vehicle_history_events_use_transition_helpers() -> None:
    runtime_vehicle_functions = _function_names("parking_spot_monitor/runtime_vehicle_events.py")

    assert "_record_vehicle_history_start" in runtime_vehicle_functions
    assert "_record_vehicle_history_close" in runtime_vehicle_functions
    assert _line_count("parking_spot_monitor/runtime_vehicle_events.py") <= 500


def test_vehicle_history_module_is_a_small_compatibility_shim() -> None:
    assert _line_count("parking_spot_monitor/vehicle_history.py") <= 220
    module_caps = {
        "parking_spot_monitor/vehicle_history_archive.py": 220,
        "parking_spot_monitor/vehicle_history_corrections.py": 410,
        "parking_spot_monitor/vehicle_history_maintenance.py": 390,
        "parking_spot_monitor/vehicle_history_maintenance_utils.py": 260,
        "parking_spot_monitor/vehicle_history_models.py": 640,
        "parking_spot_monitor/vehicle_history_profile_utils.py": 120,
        "parking_spot_monitor/vehicle_history_profiles.py": 320,
        "parking_spot_monitor/vehicle_history_sessions.py": 220,
        "parking_spot_monitor/vehicle_history_storage.py": 320,
    }
    for path, max_lines in module_caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines


def test_runtime_modules_stay_decomposed() -> None:
    assert _line_count("parking_spot_monitor/__main__.py") <= 430
    module_caps = {
        "parking_spot_monitor/capture_loop.py": 280,
        "parking_spot_monitor/runtime_commands.py": 120,
        "parking_spot_monitor/runtime_decision_memory.py": 240,
        "parking_spot_monitor/runtime_detection.py": 250,
        "parking_spot_monitor/runtime_frame_plan.py": 160,
        "parking_spot_monitor/runtime_health.py": 290,
        "parking_spot_monitor/runtime_lifecycle.py": 150,
        "parking_spot_monitor/runtime_overlay.py": 90,
        "parking_spot_monitor/runtime_presence.py": 150,
        "parking_spot_monitor/runtime_state_update.py": 180,
        "parking_spot_monitor/runtime_vehicle_events.py": 500,
    }
    for path, max_lines in module_caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines


def test_operator_modules_stay_decomposed() -> None:
    module_caps = {
        "parking_spot_monitor/operator_cockpit.py": 220,
        "parking_spot_monitor/operator_cockpit_analytics.py": 220,
        "parking_spot_monitor/operator_cockpit_confidence.py": 220,
        "parking_spot_monitor/operator_cockpit_lab.py": 180,
        "parking_spot_monitor/operator_cockpit_memory.py": 80,
        "parking_spot_monitor/operator_cockpit_outbox.py": 180,
        "parking_spot_monitor/operator_cockpit_shared.py": 380,
        "parking_spot_monitor/operator_cockpit_snapshots.py": 460,
        "parking_spot_monitor/operator_feedback.py": 520,
        "parking_spot_monitor/operator_feedback_alerts.py": 80,
        "parking_spot_monitor/operator_feedback_evidence.py": 160,
        "parking_spot_monitor/operator_feedback_labels.py": 160,
        "parking_spot_monitor/operator_feedback_models.py": 450,
        "parking_spot_monitor/operator_feedback_replies.py": 140,
        "parking_spot_monitor/operator_feedback_store.py": 220,
        "parking_spot_monitor/operator_timeline.py": 100,
    }
    for path, max_lines in module_caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines


def test_operator_feedback_does_not_import_model_privates() -> None:
    feedback_source = (ROOT / "parking_spot_monitor/operator_feedback.py").read_text(encoding="utf-8")
    model_source = (ROOT / "parking_spot_monitor/operator_feedback_models.py").read_text(encoding="utf-8")
    tree = ast.parse(feedback_source)

    model_imports = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "parking_spot_monitor.operator_feedback_models"
        for alias in node.names
    ]
    assert model_imports
    assert all(not name.startswith("_") for name in model_imports)
    assert "feedback_label_from_any" not in model_imports

    store_imports = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "parking_spot_monitor.operator_feedback_store"
        for alias in node.names
    ]
    assert {"append_feedback_label", "find_feedback_label_by_matrix_event_id", "load_feedback_labels"} <= set(store_imports)
    assert "write_feedback_labels" not in model_source
    assert "quarantine_feedback_file" not in model_source
    assert "import json" not in model_source
    assert "import tempfile" not in model_source


def test_operator_feedback_compat_surface_stays_thin() -> None:
    imports = _imported_module_names("parking_spot_monitor/operator_feedback.py")
    functions = _function_names("parking_spot_monitor/operator_feedback.py")

    assert "parking_spot_monitor.operator_feedback_alerts" in imports
    assert "parking_spot_monitor.operator_feedback_evidence" in imports
    assert "parking_spot_monitor.operator_feedback_labels" in imports
    assert "parking_spot_monitor.operator_feedback_replies" in imports
    assert "PIL" not in imports
    assert "validate_feedback_evidence" not in functions
    assert "validate_timeline_feedback_evidence" not in functions
    assert "format_learn_reply" not in functions
    assert "make_learn_feedback_label" not in functions
    assert "resolve_latest_alert_candidate" not in functions


def test_operator_cockpit_modules_do_not_import_shared_privates() -> None:
    for relative_path in (
        "parking_spot_monitor/operator_cockpit.py",
        "parking_spot_monitor/operator_cockpit_analytics.py",
        "parking_spot_monitor/operator_cockpit_confidence.py",
        "parking_spot_monitor/operator_cockpit_lab.py",
        "parking_spot_monitor/operator_cockpit_outbox.py",
        "parking_spot_monitor/operator_cockpit_snapshots.py",
    ):
        tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
        shared_imports = [
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("parking_spot_monitor.operator_cockpit")
            for alias in node.names
        ]
        assert shared_imports
        assert all(not name.startswith("_") for name in shared_imports)


def test_operator_cockpit_compat_surface_stays_thin() -> None:
    cockpit_imports = set(_imported_names("parking_spot_monitor/operator_cockpit.py", "parking_spot_monitor.operator_cockpit_analytics"))
    cockpit_imports.update(_imported_names("parking_spot_monitor/operator_cockpit.py", "parking_spot_monitor.operator_cockpit_confidence"))
    cockpit_imports.update(_imported_names("parking_spot_monitor/operator_cockpit.py", "parking_spot_monitor.operator_cockpit_lab"))
    cockpit_imports.update(_imported_names("parking_spot_monitor/operator_cockpit.py", "parking_spot_monitor.operator_cockpit_outbox"))
    cockpit_functions = _function_names("parking_spot_monitor/operator_cockpit.py")

    assert "format_operator_analytics_reply" in cockpit_imports
    assert "format_operator_confidence_reply" in cockpit_imports
    assert "format_detection_lab_run_reply" in cockpit_imports
    assert "matrix_outbox_status_lines" in cockpit_imports
    assert "_load_vehicle_history_session_dicts" not in cockpit_functions
    assert "_summarize_timeline_frames" not in cockpit_functions
    assert "_format_lab_status_lines" not in cockpit_functions
    assert "_outbox_item_lines" not in cockpit_functions


def test_matrix_cockpit_uses_canonical_operator_functions_without_forwarding_wrappers() -> None:
    cockpit_functions = _function_names("parking_spot_monitor/matrix_cockpit.py")

    assert _imports_module("parking_spot_monitor/matrix_cockpit.py", "parking_spot_monitor.operator_cockpit")
    for wrapper_name in [
        "build_latest_snapshot_response",
        "format_operator_status_reply",
        "format_operator_config_reply",
        "format_operator_analytics_reply",
        "format_operator_why_reply",
        "format_operator_recent_reply",
        "format_operator_confidence_reply",
        "build_incident_review_response",
        "format_detection_lab_run_reply",
        "format_detection_lab_status_reply",
    ]:
        assert wrapper_name not in cockpit_functions


def test_matrix_alerts_reuses_shared_time_formatter() -> None:
    assert _imports_module("parking_spot_monitor/matrix_alerts.py", "parking_spot_monitor.matrix_time")
    assert "_format_12_hour_time" not in _function_names("parking_spot_monitor/matrix_alerts.py")


def test_operator_confidence_groups_memory_records_once() -> None:
    confidence_source = ast.parse((ROOT / "parking_spot_monitor/operator_cockpit_confidence.py").read_text(encoding="utf-8"))
    assigned_names = {
        target.id
        for node in ast.walk(confidence_source)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assigned_names.update(
        node.target.id
        for node in ast.walk(confidence_source)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    )

    assert "records_by_spot" in assigned_names
    assert not any(
        isinstance(node, ast.ListComp)
        and isinstance(node.generators[0].iter, ast.Name)
        and node.generators[0].iter.id == "records"
        for node in ast.walk(confidence_source)
    )


def test_matrix_dispatch_send_paths_do_not_copy_event_mappings() -> None:
    lambda_targets = _lambda_call_targets("parking_spot_monitor/matrix_dispatch.py")

    assert {"send_lifecycle_notice", "send_owner_vehicle_quiet_window_alert", "send_quiet_window_notice"} <= lambda_targets
    assert {"send_occupied_spot_alert", "send_open_spot_alert"} <= lambda_targets
    assert not _lambda_calls_function("parking_spot_monitor/matrix_dispatch.py", "dict")


def test_operator_docs_tests_stay_decomposed() -> None:
    module_caps = {
        "tests/operator_docs_helpers.py": 140,
        "tests/test_operator_closeout_docs.py": 320,
        "tests/test_operator_config_docs.py": 180,
        "tests/test_operator_docs.py": 180,
        "tests/test_operator_intelligence_docs.py": 460,
        "tests/test_operator_runtime_docs.py": 360,
    }
    for path, max_lines in module_caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines


def test_vehicle_history_corrections_do_not_use_asserts_for_event_field_narrowing() -> None:
    correction_source = (ROOT / "parking_spot_monitor/vehicle_history_corrections.py").read_text(encoding="utf-8")
    tree = ast.parse(correction_source)

    def accesses_event(value: ast.AST) -> bool:
        if isinstance(value, ast.Attribute):
            return isinstance(value.value, ast.Name) and value.value.id == "event"
        if isinstance(value, ast.Subscript):
            return isinstance(value.value, ast.Name) and value.value.id == "event"
        return False

    assert not any(isinstance(node, ast.Assert) and accesses_event(node.test) for node in ast.walk(tree))
