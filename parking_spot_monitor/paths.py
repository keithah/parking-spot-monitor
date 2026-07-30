from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from parking_spot_monitor.config import RuntimeSettings


_ULTRALYTICS_ENV_LOCK = threading.Lock()
_managed_ultralytics_config_dir: str | None = None


@dataclass(frozen=True)
class RuntimePaths:
    """Effective runtime paths after applying the startup data directory."""

    data_dir: Path
    state_file: Path
    latest_frame: Path
    snapshots_dir: Path
    health_file: Path
    vehicle_history_dir: Path
    decision_memory_file: Path
    detection_lab_dir: Path
    matrix_outbox_file: Path


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
        decision_memory_file=effective_data_dir / "operator-decision-memory.json",
        detection_lab_dir=effective_data_dir / "detection-lab",
        matrix_outbox_file=effective_data_dir / "matrix-outbox.json",
    )


def prepare_ultralytics_config_dir(
    data_dir: str | Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Create the writable Ultralytics state directory without following a leaf symlink."""

    global _managed_ultralytics_config_dir

    with _ULTRALYTICS_ENV_LOCK:
        effective_data_dir = Path(data_dir)
        selected_environ = os.environ if environ is None else environ
        configured = selected_environ.get("YOLO_CONFIG_DIR")
        managed_default = configured is None or (
            environ is None and configured == _managed_ultralytics_config_dir
        )
        config_dir = (
            effective_data_dir / "ultralytics"
            if managed_default
            else Path(configured)
        )
        if config_dir.parent != effective_data_dir or config_dir.name != "ultralytics":
            raise ValueError("YOLO_CONFIG_DIR must be the ultralytics directory under --data-dir")

        effective_data_dir.mkdir(parents=True, exist_ok=True)
        try:
            config_dir.mkdir(mode=0o750)
        except FileExistsError:
            pass
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(config_dir, flags)
        try:
            os.fchmod(descriptor, 0o750)
        finally:
            os.close(descriptor)
        os.environ["YOLO_CONFIG_DIR"] = str(config_dir)
        _managed_ultralytics_config_dir = str(config_dir) if managed_default else None
        return config_dir


def _resolve_under_data_dir(value: Path | None, data_dir: Path, *, default: str | None = None) -> Path:
    selected = Path(default) if value is None else Path(value)
    if selected.is_absolute():
        return selected
    return data_dir / selected
