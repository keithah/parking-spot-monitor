from __future__ import annotations

import json
from pathlib import Path

from parking_spot_monitor.owner_vehicles import OwnerVehicleRegistry, load_owner_vehicle_registry


def test_missing_owner_vehicle_registry_is_empty(tmp_path: Path) -> None:
    registry = load_owner_vehicle_registry(tmp_path / "owner-vehicles.json")

    assert registry.owner_for_profile("prof_missing") is None
    assert registry.owner_for_profile(None) is None


def test_owner_vehicle_registry_loads_profiles_and_descriptions(tmp_path: Path) -> None:
    path = tmp_path / "owner-vehicles.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_vehicles": [
                    {
                        "profile_id": "prof_tesla",
                        "label": "Keith's black Tesla",
                        "description": "black Tesla, tinted windows, roof rack",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    registry = load_owner_vehicle_registry(path)
    owner = registry.owner_for_profile("prof_tesla")

    assert owner is not None
    assert owner.profile_id == "prof_tesla"
    assert owner.label == "Keith's black Tesla"
    assert owner.description == "black Tesla, tinted windows, roof rack"
    assert owner.to_alert_payload() == {
        "profile_id": "prof_tesla",
        "label": "Keith's black Tesla",
        "description": "black Tesla, tinted windows, roof rack",
    }


def test_owner_vehicle_registry_rejects_invalid_schema(tmp_path: Path) -> None:
    path = tmp_path / "owner-vehicles.json"
    path.write_text(json.dumps({"schema_version": 1, "owner_vehicles": [{"profile_id": "bad id", "label": "Tesla"}]}), encoding="utf-8")

    registry = load_owner_vehicle_registry(path)

    assert isinstance(registry, OwnerVehicleRegistry)
    assert registry.owner_for_profile("bad id") is None
