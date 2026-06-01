from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _line_count(relative_path: str) -> int:
    return len((ROOT / relative_path).read_text(encoding="utf-8").splitlines())


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
    docs_test_source = (ROOT / "tests/test_operator_docs.py").read_text(encoding="utf-8")
    command_source = (ROOT / "parking_spot_monitor/matrix_commands.py").read_text(encoding="utf-8")

    assert 'read_tracked("parking_spot_monitor/matrix_commands.py")' not in docs_test_source
    assert "# type: ignore" not in command_source
    assert "_applied_command_from_compat" not in command_source
    assert "MatrixCommand | _AppliedMatrixCommand" not in command_source


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
