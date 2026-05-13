from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from parking_spot_monitor.config import RuntimeSettings


@dataclass(frozen=True)
class RuntimePaths:
    """Effective runtime paths after applying the startup data directory."""

    data_dir: Path
    state_file: Path
    latest_frame: Path
    snapshots_dir: Path
    health_file: Path
    vehicle_history_dir: Path


def resolve_runtime_paths(settings: RuntimeSettings, data_dir: str | Path) -> RuntimePaths:
    """Resolve operator-configured runtime paths under the effective data directory.

    The CLI/container data directory is the anchor for runtime artifacts. Relative
    configured paths are treated as mount-relative values; absolute paths are
    preserved for explicit operator overrides.
    """

    effective_data_dir = Path(data_dir)
    snapshots_dir = _resolve_under_data_dir(settings.storage.snapshots_dir, effective_data_dir, default="snapshots")
    health_file = _resolve_under_data_dir(settings.runtime.health_file, effective_data_dir)
    return RuntimePaths(
        data_dir=effective_data_dir,
        state_file=effective_data_dir / "state.json",
        latest_frame=effective_data_dir / "latest.jpg",
        snapshots_dir=snapshots_dir,
        health_file=health_file,
        vehicle_history_dir=effective_data_dir / "vehicle-history",
    )


def _resolve_under_data_dir(value: Path | None, data_dir: Path, *, default: str | None = None) -> Path:
    selected = Path(default) if value is None else Path(value)
    if selected.is_absolute():
        return selected
    return data_dir / selected
