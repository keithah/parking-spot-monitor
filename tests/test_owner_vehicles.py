from __future__ import annotations

import json
from pathlib import Path

import pytest

from parking_spot_monitor.owner_vehicles import (
    MAX_OWNER_REGISTRY_BYTES,
    OwnerVehicleRegistry,
    OwnerVehicleRegistryError,
    load_owner_vehicle_registry,
)


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


@pytest.mark.parametrize(
    ("payload", "code"),
    (
        ("{broken token=private-value", "invalid_json"),
        ('{"schema_version":2,"token":"private-value"}', "invalid_schema"),
    ),
)
def test_strict_owner_vehicle_registry_reports_typed_safe_errors(
    tmp_path: Path,
    payload: str,
    code: str,
) -> None:
    path = tmp_path / "owner-vehicles.json"
    path.write_text(payload, encoding="utf-8")

    with pytest.raises(OwnerVehicleRegistryError) as raised:
        load_owner_vehicle_registry(path, strict=True)

    assert raised.value.code == code
    assert str(raised.value) == code
    assert "private-value" not in raised.value.safe_message


def test_strict_owner_registry_rejects_oversize_before_json_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "owner-vehicles.json"
    path.write_bytes(b"{" + b" " * MAX_OWNER_REGISTRY_BYTES + b"}")
    called = False

    def forbidden_loads(_text: str) -> object:
        nonlocal called
        called = True
        raise AssertionError("oversized registry reached json.loads")

    monkeypatch.setattr(json, "loads", forbidden_loads)
    with pytest.raises(OwnerVehicleRegistryError) as raised:
        load_owner_vehicle_registry(path, strict=True)

    assert raised.value.code == "too_large"
    assert called is False


def test_strict_missing_owner_vehicle_registry_is_empty(tmp_path: Path) -> None:
    registry = load_owner_vehicle_registry(tmp_path / "missing.json", strict=True)

    assert dict(registry.vehicles_by_profile_id) == {}
