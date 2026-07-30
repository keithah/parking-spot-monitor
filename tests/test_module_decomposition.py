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


def _function_arg_names(relative_path: str, function_name: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return {arg.arg for arg in node.args.args + node.args.kwonlyargs}
    raise AssertionError(f"{function_name} not found in {relative_path}")


def _function_body_node_types(relative_path: str, function_name: str) -> set[type[ast.AST]]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return {type(child) for child in ast.walk(node)}
    raise AssertionError(f"{function_name} not found in {relative_path}")


def _class_field_annotations(relative_path: str, class_name: str) -> dict[str, str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        fields: dict[str, str] = {}
        for child in node.body:
            if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
                fields[child.target.id] = ast.unparse(child.annotation)
        return fields
    raise AssertionError(f"{class_name} not found in {relative_path}")


def _class_field_annotation_mentions(relative_path: str, class_name: str, field_name: str, expected_name: str) -> bool:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for child in node.body:
            if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name) and child.target.id == field_name:
                return any(isinstance(name, ast.Name) and name.id == expected_name for name in ast.walk(child.annotation))
    raise AssertionError(f"{class_name}.{field_name} not found in {relative_path}")


def _function_arg_annotations(relative_path: str, function_name: str) -> dict[str, str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            args = [*node.args.args, *node.args.kwonlyargs]
            return {arg.arg: ast.unparse(arg.annotation) for arg in args if arg.annotation is not None}
    raise AssertionError(f"{function_name} not found in {relative_path}")


def _function_arg_annotation_mentions(relative_path: str, function_name: str, arg_name: str, expected_name: str) -> bool:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name != function_name:
            continue
        for arg in [*node.args.args, *node.args.kwonlyargs]:
            if arg.arg == arg_name and arg.annotation is not None:
                return any(isinstance(name, ast.Name) and name.id == expected_name for name in ast.walk(arg.annotation))
    raise AssertionError(f"{function_name}.{arg_name} not found in {relative_path}")


def _class_names(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}


def _class_method_names(relative_path: str, class_name: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return {child.name for child in node.body if isinstance(child, ast.FunctionDef)}
    raise AssertionError(f"{class_name} not found in {relative_path}")


def _function_calls(relative_path: str, function_name: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            calls: set[str] = set()
            for child in ast.walk(node):
                if isinstance(child, ast.Call):
                    if isinstance(child.func, ast.Name):
                        calls.add(child.func.id)
                    elif isinstance(child.func, ast.Attribute):
                        calls.add(child.func.attr)
            return calls
    raise AssertionError(f"{function_name} not found in {relative_path}")


def test_dependency_lock_tests_are_split_by_responsibility() -> None:
    assert not (ROOT / "tests" / "test_dependency_locks.py").exists()
    expected = (
        "dependency_lock_helpers.py",
        "test_dependency_lock_contract.py",
        "test_dependency_lock_generation.py",
        "test_dependency_lock_validation.py",
    )
    for name in expected:
        path = ROOT / "tests" / name
        assert path.exists(), name
        assert len(path.read_text(encoding="utf-8").splitlines()) <= 500, name


def test_runtime_decision_memory_does_not_import_runtime_detection() -> None:
    imports = _imported_module_names(
        "parking_spot_monitor/runtime_decision_memory.py"
    )
    assert "parking_spot_monitor.runtime_detection" not in imports
    capture_imports = _imported_names(
        "parking_spot_monitor/capture_loop.py",
        "parking_spot_monitor.runtime_decision_memory",
    )
    assert "build_detection_memory_records" in capture_imports


def test_confirmed_dead_retry_and_replay_declarations_are_absent() -> None:
    assert "ReplayValidationError" not in _class_names(
        "parking_spot_monitor/replay.py"
    )
    assert "_retry_reason" not in _function_names(
        "src/parking_monitor/matrix_outbox_delivery.py"
    )


def test_outbox_is_decomposed_behind_a_small_compatibility_facade() -> None:
    caps = {
        "src/parking_monitor/outbox.py": 450,
        "src/parking_monitor/outbox_derivatives.py": 80,
        "src/parking_monitor/matrix_outbox_delivery.py": 650,
        "src/parking_monitor/matrix_outbox_snapshots.py": 300,
        "src/parking_monitor/outbox_models.py": 500,
        "src/parking_monitor/outbox_storage.py": 500,
    }
    for path, max_lines in caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines, path


def test_matrix_module_is_a_small_compatibility_shim() -> None:
    assert _line_count("parking_spot_monitor/matrix.py") <= 220
    module_caps = {
        "parking_spot_monitor/matrix_command_catalog.py": 720,
        "parking_spot_monitor/matrix_command_runtime.py": 140,
        "parking_spot_monitor/matrix_cancellation.py": 100,
        "parking_spot_monitor/matrix_client.py": 260,
        "parking_spot_monitor/matrix_commands.py": 740,
        "parking_spot_monitor/matrix_cockpit.py": 430,
        "parking_spot_monitor/matrix_delivery.py": 280,
        "parking_spot_monitor/file_descriptor_binding.py": 180,
        "parking_spot_monitor/jpeg_artifacts.py": 350,
        "parking_spot_monitor/matrix_upload_derivatives.py": 400,
        "parking_spot_monitor/matrix_snapshot_storage.py": 320,
        "parking_spot_monitor/matrix_retained_publication.py": 180,
        "parking_spot_monitor/matrix_snapshot_naming.py": 80,
        "parking_spot_monitor/matrix_models.py": 100,
        "parking_spot_monitor/matrix_snapshots.py": 430,
        "parking_spot_monitor/matrix_sync.py": 100,
        "parking_spot_monitor/matrix_dispatch.py": 410,
    }
    for path, max_lines in module_caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines

    derivative_source = (ROOT / "parking_spot_monitor/matrix_upload_derivatives.py").read_text(encoding="utf-8")
    assert "def _jpeg_bytes_dimensions" not in derivative_source
    assert "jpeg_bytes_dimensions" in derivative_source


def test_generic_jpeg_artifacts_do_not_depend_on_matrix_derivative_types() -> None:
    jpeg_source = (ROOT / "parking_spot_monitor/jpeg_artifacts.py").read_text(encoding="utf-8")
    assert "MatrixUploadDerivative" not in jpeg_source
    assert "matrix_snapshots" not in jpeg_source
    derivative_source = (ROOT / "parking_spot_monitor/matrix_upload_derivatives.py").read_text(encoding="utf-8")
    assert "parking_spot_monitor.matrix_snapshots" not in derivative_source
    assert "_matrix_snapshot_upload" not in derivative_source


def test_canonical_jpeg_publication_never_uses_mutable_hardlinks() -> None:
    source = (ROOT / "parking_spot_monitor/jpeg_artifacts.py").read_text(encoding="utf-8")

    assert '"hardlink"' not in source
    assert "os.link(" not in source


def test_matrix_snapshot_artifact_ownership_is_layered() -> None:
    storage_imports = _imported_module_names("parking_spot_monitor/matrix_snapshot_storage.py")
    derivative_imports = _imported_module_names("parking_spot_monitor/matrix_upload_derivatives.py")
    snapshot_imports = _imported_module_names("parking_spot_monitor/matrix_snapshots.py")
    publication_imports = _imported_module_names("parking_spot_monitor/matrix_retained_publication.py")

    assert "parking_spot_monitor.matrix_snapshots" not in storage_imports
    assert "parking_spot_monitor.matrix_upload_derivatives" not in storage_imports
    assert "os" not in derivative_imports
    assert "parking_spot_monitor.matrix_snapshot_storage" in derivative_imports
    assert "parking_spot_monitor.matrix_snapshot_storage" in snapshot_imports
    assert "parking_spot_monitor.matrix_snapshot_storage" not in publication_imports
    assert "parking_spot_monitor.matrix_retained_publication" in storage_imports
    assert "parking_spot_monitor.matrix_snapshot_naming" in snapshot_imports


def test_runtime_resource_policy_is_small_and_has_no_runtime_side_effect_dependencies() -> None:
    relative_path = "parking_spot_monitor/runtime_resource_policy.py"

    assert (ROOT / relative_path).exists()
    assert _line_count(relative_path) <= 220
    imported_modules = _imported_module_names(relative_path)
    forbidden_import_prefixes = (
        "PIL",
        "parking_spot_monitor.capture",
        "parking_spot_monitor.matrix",
        "parking_spot_monitor.vehicle_history",
    )
    assert not any(
        imported.startswith(forbidden)
        for imported in imported_modules
        for forbidden in forbidden_import_prefixes
    )


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


def test_matrix_commands_keep_parse_data_separate_from_application() -> None:
    command_strings = _string_constants("parking_spot_monitor/matrix_commands.py")
    command_functions = _function_names("parking_spot_monitor/matrix_commands.py")
    command_imports = _imported_module_names("parking_spot_monitor/matrix_commands.py")
    command_catalog_imports = set(_imported_names("parking_spot_monitor/matrix_commands.py", "parking_spot_monitor.matrix_command_catalog"))
    command_runtime_imports = set(_imported_names("parking_spot_monitor/matrix_commands.py", "parking_spot_monitor.matrix_command_runtime"))
    catalog_functions = _function_names("parking_spot_monitor/matrix_command_catalog.py")
    catalog_strings = _string_constants("parking_spot_monitor/matrix_command_catalog.py")
    runtime_fields = _class_field_annotations("parking_spot_monitor/matrix_command_runtime.py", "MatrixCommandRuntime")

    assert "_CockpitRenderer" not in command_strings
    assert "_format_cockpit_reply" not in command_functions
    assert ast.If not in _function_body_node_types("parking_spot_monitor/matrix_commands.py", "_apply_command")
    assert "parking_spot_monitor.matrix_command_catalog" in command_imports
    assert "parking_spot_monitor.matrix_command_runtime" in command_imports
    assert command_catalog_imports <= {
        "AppliedMatrixCommand",
        "format_command_help_reply",
        "parse_applied_matrix_command",
        "parse_matrix_command",
    }
    assert command_runtime_imports <= {
        "MatrixCommandArchive",
        "MatrixCommandRuntime",
        "MatrixFeedbackLabeler",
        "WhoSnapshotProvider",
    }
    assert "_parse_applied_matrix_command" not in command_functions
    assert "MATRIX_COMMAND_SPECS" not in command_strings
    assert "render" not in _function_arg_names("parking_spot_monitor/matrix_command_catalog.py", "parse_applied_matrix_command")
    assert "_CockpitRenderer" not in catalog_strings
    assert "Any" not in runtime_fields["archive"]
    assert "Any" not in runtime_fields["feedback_labeler"]
    assert _class_field_annotation_mentions("parking_spot_monitor/matrix_command_runtime.py", "MatrixCommandRuntime", "archive", "MatrixCommandArchive")
    assert _class_field_annotation_mentions("parking_spot_monitor/matrix_command_runtime.py", "MatrixCommandRuntime", "feedback_labeler", "MatrixFeedbackLabeler")
    assert "getattr" not in _function_calls("parking_spot_monitor/matrix_command_runtime.py", "correction_already_seen")
    assert "getattr" not in _function_calls("parking_spot_monitor/matrix_command_runtime.py", "resolve_wrong_match_subject")
    assert "MatrixCommandArchive" not in _class_names("parking_spot_monitor/matrix_command_catalog.py")
    assert "MatrixFeedbackLabeler" not in _class_names("parking_spot_monitor/matrix_command_catalog.py")
    service_archive_fields = _class_names("parking_spot_monitor/matrix_commands.py")
    assert "MatrixCommandServiceArchive" in service_archive_fields
    assert "apply" in catalog_functions
    assert "to_matrix_command" in catalog_functions
    assert "parse_applied_matrix_command" in catalog_functions
    assert "parse_matrix_command" in catalog_functions


def test_runtime_vehicle_history_events_use_transition_helpers() -> None:
    runtime_vehicle_functions = _function_names("parking_spot_monitor/runtime_vehicle_events.py")
    step_result_fields = _class_field_annotations("parking_spot_monitor/runtime_vehicle_events.py", "_VehicleHistoryStepResult")

    assert "_record_vehicle_history_start" in runtime_vehicle_functions
    assert "_record_vehicle_history_close" in runtime_vehicle_functions
    assert "errors" not in _function_arg_names("parking_spot_monitor/runtime_vehicle_events.py", "_attach_occupied_images")
    assert "errors" not in _function_arg_names("parking_spot_monitor/runtime_vehicle_events.py", "_match_vehicle_profile_for_session")
    assert "Any" not in step_result_fields["value"]
    assert _function_arg_annotation_mentions("parking_spot_monitor/runtime_vehicle_events.py", "_match_vehicle_profile_for_session", "image_record", "SessionRecord")
    assert _function_arg_annotation_mentions("parking_spot_monitor/runtime_vehicle_events.py", "_occupied_alert_payload", "image_record", "SessionRecord")
    assert "getattr" not in _function_calls("parking_spot_monitor/runtime_vehicle_events.py", "_occupied_alert_payload")
    runtime_vehicle_imports = _imported_module_names("parking_spot_monitor/runtime_vehicle_events.py")
    assert "parking_spot_monitor.runtime_vehicle_payloads" not in runtime_vehicle_imports
    assert "parking_spot_monitor.vehicle_history_alert_payloads" in runtime_vehicle_imports
    assert "parking_spot_monitor.vehicle_history_models" in runtime_vehicle_imports
    assert "parking_spot_monitor.vehicle_estimates" not in runtime_vehicle_imports
    assert not {"VehicleProfileAssignmentPayload", "VehicleHistoryEstimatePayload"} & _class_names("parking_spot_monitor/runtime_vehicle_events.py")
    assert not {"VehicleHistoryImageAttachResult", "VehicleHistoryProfileMatchResult"} & _class_names("parking_spot_monitor/runtime_vehicle_events.py")
    assert _line_count("parking_spot_monitor/runtime_vehicle_events.py") <= 500


def test_vehicle_history_module_is_a_small_compatibility_shim() -> None:
    assert _line_count("parking_spot_monitor/vehicle_history.py") <= 220
    module_caps = {
        "parking_spot_monitor/vehicle_history_archive.py": 220,
        "parking_spot_monitor/vehicle_history_alert_payloads.py": 120,
        "parking_spot_monitor/vehicle_history_corrections.py": 410,
        "parking_spot_monitor/vehicle_history_maintenance.py": 390,
        "parking_spot_monitor/vehicle_history_maintenance_utils.py": 260,
        "parking_spot_monitor/vehicle_history_models.py": 640,
        "parking_spot_monitor/vehicle_history_profile_utils.py": 120,
        "parking_spot_monitor/vehicle_history_profiles.py": 320,
        "parking_spot_monitor/vehicle_history_sessions.py": 220,
        "parking_spot_monitor/vehicle_history_storage.py": 330,
    }
    for path, max_lines in module_caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines


def test_runtime_modules_stay_decomposed() -> None:
    assert _line_count("parking_spot_monitor/__main__.py") <= 430
    module_caps = {
        "parking_spot_monitor/capture_loop.py": 280,
        "parking_spot_monitor/runtime_commands.py": 120,
        "parking_spot_monitor/runtime_command_worker.py": 190,
        "parking_spot_monitor/runtime_command_results.py": 150,
        "parking_spot_monitor/decision_memory_reconciliation.py": 80,
        "parking_spot_monitor/decision_memory_runtime.py": 60,
        "parking_spot_monitor/decision_memory_store.py": 240,
        "parking_spot_monitor/runtime_decision_memory.py": 240,
        "parking_spot_monitor/runtime_detection.py": 250,
        # Detector adaptation stays a small construction-time boundary.
        "parking_spot_monitor/detector_adapter.py": 180,
        "parking_spot_monitor/runtime_detection_support.py": 110,
        "parking_spot_monitor/runtime_frame.py": 90,
        "parking_spot_monitor/runtime_frame_outcome.py": 90,
        "parking_spot_monitor/runtime_frame_plan.py": 160,
        "parking_spot_monitor/runtime_health.py": 290,
        "parking_spot_monitor/runtime_health_reporting.py": 100,
        "parking_spot_monitor/runtime_lifecycle.py": 150,
        "parking_spot_monitor/runtime_loop_resources.py": 260,
        "parking_spot_monitor/runtime_overlay.py": 90,
        "parking_spot_monitor/runtime_presence.py": 150,
        "parking_spot_monitor/runtime_reconnect.py": 70,
        "parking_spot_monitor/runtime_log_aggregation.py": 120,
        "parking_spot_monitor/runtime_state_update.py": 180,
        "parking_spot_monitor/runtime_stream_escalation.py": 170,
        "parking_spot_monitor/runtime_vehicle_events.py": 500,
    }
    for path, max_lines in module_caps.items():
        assert (ROOT / path).exists()
        assert _line_count(path) <= max_lines

    resource_functions = _function_names(
        "parking_spot_monitor/runtime_loop_resources.py"
    )
    assert "loop_health_writer" not in resource_functions
    assert "write_current_loop_health" not in resource_functions

    retired_module = ROOT / "parking_spot_monitor/runtime_detector_capabilities.py"
    assert not retired_module.exists()
    for source in (ROOT / "parking_spot_monitor").glob("*.py"):
        assert "runtime_detector_capabilities" not in source.read_text(encoding="utf-8")


def test_runtime_matrix_boundaries_use_narrow_protocols() -> None:
    dispatch_classes = _class_names("parking_spot_monitor/matrix_dispatch.py")
    command_classes = _class_names("parking_spot_monitor/runtime_commands.py")

    assert "RuntimeMatrixDelivery" in dispatch_classes
    assert "RuntimeMatrixCommandService" in command_classes
    assert _function_arg_annotation_mentions(
        "parking_spot_monitor/matrix_dispatch.py",
        "dispatch_matrix_event",
        "matrix_delivery",
        "RuntimeMatrixDelivery",
    )
    assert _function_arg_annotation_mentions(
        "parking_spot_monitor/runtime_state_update.py",
        "_update_runtime_state_for_frame",
        "matrix_delivery",
        "RuntimeMatrixDelivery",
    )
    assert _function_arg_annotation_mentions(
        "parking_spot_monitor/runtime_commands.py",
        "_poll_matrix_commands_once",
        "matrix_command_service",
        "RuntimeMatrixCommandService",
    )


def test_promoted_runtime_helpers_are_imported_publicly() -> None:
    loop_resource_overlay_imports = _imported_names(
        "parking_spot_monitor/runtime_loop_resources.py",
        "parking_spot_monitor.runtime_overlay",
    )
    loop_resource_presence_imports = _imported_names(
        "parking_spot_monitor/runtime_loop_resources.py",
        "parking_spot_monitor.runtime_presence",
    )
    frame_plan_presence_imports = _imported_names(
        "parking_spot_monitor/runtime_frame_plan.py",
        "parking_spot_monitor.runtime_presence",
    )
    main_overlay_imports = _imported_names(
        "parking_spot_monitor/__main__.py",
        "parking_spot_monitor.runtime_overlay",
    )

    assert "write_overlay_for_capture" in loop_resource_overlay_imports
    assert "_write_overlay_for_capture" not in loop_resource_overlay_imports
    assert "presence_by_spot" in loop_resource_presence_imports
    assert "_presence_by_spot" not in loop_resource_presence_imports
    assert "presence_by_spot" in frame_plan_presence_imports
    assert "_presence_by_spot" not in frame_plan_presence_imports
    assert "write_overlay_for_capture" in main_overlay_imports
    assert "_write_overlay_for_capture" not in main_overlay_imports


def test_runtime_stream_escalation_stays_pure_orchestration() -> None:
    source = (ROOT / "parking_spot_monitor/runtime_stream_escalation.py").read_text(encoding="utf-8")
    loop_source = (ROOT / "parking_spot_monitor/capture_loop.py").read_text(encoding="utf-8")
    capture_source = (ROOT / "parking_spot_monitor/capture.py").read_text(encoding="utf-8")
    escalation_classes = _class_names("parking_spot_monitor/runtime_stream_escalation.py")
    capture_classes = _class_names("parking_spot_monitor/capture.py")
    capture_fields = _class_field_annotations("parking_spot_monitor/capture.py", "FrameCaptureResult")
    capture_methods = _class_method_names("parking_spot_monitor/capture.py", "FrameCaptureResult")
    main_arg_annotations = _function_arg_annotations("parking_spot_monitor/__main__.py", "_main")

    assert "inspect" not in source
    assert "_append_detection_memory_records" not in source
    assert "StreamEscalationCaptureFailed" not in loop_source
    assert "StreamProfileCapture" not in escalation_classes
    assert "StreamProfileCapture" in capture_classes
    assert "StreamProfileCapture" in main_arg_annotations["capture"]
    assert capture_fields["frame_geometry"] == "FrameGeometry"
    assert "stream_profile" not in capture_fields
    assert "expected_frame_size" not in capture_fields
    assert "__init__" not in capture_methods
    assert "expected_frame_size: tuple[int, int] | None = None" not in capture_source


def test_capture_loop_does_not_own_runtime_frame_outcome_policy() -> None:
    loop_source = (ROOT / "parking_spot_monitor/capture_loop.py").read_text(encoding="utf-8")

    assert "RuntimeFrameCaptureFailed" not in loop_source
    assert "RuntimeFrameCaptureEscalationFailed" not in loop_source
    assert "RuntimeFrameDetectionFailed" not in loop_source
    assert "record_capture_success" not in loop_source


def test_stream_profile_identity_is_not_stored_in_raw_config_model() -> None:
    config_classes = _class_names("parking_spot_monitor/config.py")
    profile_fields = _class_field_annotations("parking_spot_monitor/config.py", "StreamProfileConfig")

    assert "ResolvedStreamProfile" in config_classes
    assert "name" not in profile_fields


def test_matrix_command_history_changes_are_not_inferred_from_action_names() -> None:
    command_source = (ROOT / "parking_spot_monitor/matrix_commands.py").read_text(encoding="utf-8")
    startup_source = (ROOT / "tests/test_startup.py").read_text(encoding="utf-8")
    storage_source = (ROOT / "parking_spot_monitor/vehicle_history_storage.py").read_text(encoding="utf-8")

    assert "_HISTORY_MUTATING_ACTIONS" not in command_source
    assert "command.action in" not in command_source
    assert "_coerce_application_result" not in command_source
    # The dirty signal for the health-snapshot cache is owned by the archive's
    # mutation revision (bumped on writes), not threaded up through command results.
    assert "def mutation_revision" in storage_source
    assert "_bump_revision" in storage_source
    assert "detections.pop(0) if detections else []" not in startup_source
    assert "allow_exhausted=True" in startup_source


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


def test_optimizer_hot_paths_do_not_reintroduce_unbounded_materialization_patterns() -> None:
    snapshot_source = (ROOT / "parking_spot_monitor/operator_cockpit_snapshots.py").read_text(encoding="utf-8")
    analytics_source = (ROOT / "parking_spot_monitor/operator_cockpit_analytics.py").read_text(encoding="utf-8")
    maintenance_source = (ROOT / "parking_spot_monitor/vehicle_history_maintenance.py").read_text(encoding="utf-8")
    correction_source = (ROOT / "parking_spot_monitor/vehicle_history_corrections.py").read_text(encoding="utf-8")

    assert "resized = working.copy()" not in snapshot_source
    assert "data = buffer.getvalue()" not in snapshot_source
    assert "paths = sorted(path for path in directory.glob" not in analytics_source
    assert "retained_records = [record for record in [*active_records, *closed_records]" not in maintenance_source
    assert "prune_paths = [*session_paths, *image_paths]" not in maintenance_source
    assert "record for record in [*closed, *active]" not in correction_source
    assert "record for record in [*active_records, *closed_records]" not in correction_source
    assert "for record in [*active, *closed]" not in correction_source


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

    assert {"enqueue_lifecycle_notice", "enqueue_text_notice"} <= lambda_targets
    assert {"enqueue_occupied_spot_alert", "enqueue_open_spot_alert"} <= lambda_targets
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

    assert not any(
        isinstance(node, ast.Assert) and any(accesses_event(child) for child in ast.walk(node.test))
        for node in ast.walk(tree)
    )
