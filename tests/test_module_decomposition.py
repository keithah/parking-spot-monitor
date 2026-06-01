from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _line_count(relative_path: str) -> int:
    return len((ROOT / relative_path).read_text(encoding="utf-8").splitlines())


def test_matrix_module_is_a_small_compatibility_shim() -> None:
    assert _line_count("parking_spot_monitor/matrix.py") <= 220
    for path in [
        "parking_spot_monitor/matrix_client.py",
        "parking_spot_monitor/matrix_commands.py",
        "parking_spot_monitor/matrix_cockpit.py",
        "parking_spot_monitor/matrix_delivery.py",
        "parking_spot_monitor/matrix_models.py",
    ]:
        assert (ROOT / path).exists()


def test_vehicle_history_module_is_a_small_compatibility_shim() -> None:
    assert _line_count("parking_spot_monitor/vehicle_history.py") <= 220
    for path in [
        "parking_spot_monitor/vehicle_history_archive.py",
        "parking_spot_monitor/vehicle_history_facade.py",
        "parking_spot_monitor/vehicle_history_maintenance.py",
        "parking_spot_monitor/vehicle_history_models.py",
        "parking_spot_monitor/vehicle_history_validation.py",
    ]:
        assert (ROOT / path).exists()
