from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

MAX_OWNER_VEHICLES = 20
MAX_OWNER_TEXT_LENGTH = 160
PROFILE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")


@dataclass(frozen=True)
class OwnerVehicle:
    profile_id: str
    label: str
    description: str | None = None

    def to_alert_payload(self) -> dict[str, str | None]:
        return {"profile_id": self.profile_id, "label": self.label, "description": self.description}


@dataclass(frozen=True)
class OwnerVehicleRegistry:
    vehicles_by_profile_id: Mapping[str, OwnerVehicle]

    @classmethod
    def empty(cls) -> OwnerVehicleRegistry:
        return cls({})

    def owner_for_profile(self, profile_id: object) -> OwnerVehicle | None:
        if not isinstance(profile_id, str) or not profile_id.strip():
            return None
        return self.vehicles_by_profile_id.get(profile_id.strip())


def load_owner_vehicle_registry(path: str | Path) -> OwnerVehicleRegistry:
    registry_path = Path(path)
    if not registry_path.exists():
        return OwnerVehicleRegistry.empty()
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
        return _registry_from_payload(payload)
    except Exception:
        return OwnerVehicleRegistry.empty()


def _registry_from_payload(payload: Any) -> OwnerVehicleRegistry:
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("unsupported owner vehicle registry schema")
    raw_vehicles = payload.get("owner_vehicles")
    if not isinstance(raw_vehicles, list) or len(raw_vehicles) > MAX_OWNER_VEHICLES:
        raise ValueError("owner_vehicles must be a bounded list")
    vehicles: dict[str, OwnerVehicle] = {}
    for raw in raw_vehicles:
        vehicle = _owner_vehicle_from_payload(raw)
        vehicles[vehicle.profile_id] = vehicle
    return OwnerVehicleRegistry(vehicles)


def _owner_vehicle_from_payload(payload: Any) -> OwnerVehicle:
    if not isinstance(payload, dict):
        raise ValueError("owner vehicle entry must be an object")
    profile_id = _required_text(payload.get("profile_id"), "profile_id")
    if not PROFILE_ID_RE.fullmatch(profile_id):
        raise ValueError("profile_id contains unsupported characters")
    label = _required_text(payload.get("label"), "label")
    description = _optional_text(payload.get("description"), "description")
    return OwnerVehicle(profile_id=profile_id, label=label, description=description)


def _required_text(value: object, name: str) -> str:
    text = _optional_text(value, name)
    if text is None:
        raise ValueError(f"{name} is required")
    return text


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    text = " ".join(value.strip().split())
    if not text:
        return None
    if len(text) > MAX_OWNER_TEXT_LENGTH:
        raise ValueError(f"{name} is too long")
    if any(ord(char) < 32 or ord(char) == 127 for char in text):
        raise ValueError(f"{name} contains control characters")
    return text
