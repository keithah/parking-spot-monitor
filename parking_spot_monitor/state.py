from __future__ import annotations

import json
import math
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState

SCHEMA_VERSION = 1
MAX_STATE_FILE_BYTES = 1_000_000
MAX_STATE_LIST_ITEMS = 10_000


class StateSchemaError(ValueError):
    """Raised when persisted runtime state does not match the supported schema."""


@dataclass(frozen=True)
class RuntimeState:
    """Minimal restart-safe runtime markers persisted under the data directory."""

    state_by_spot: dict[str, SpotOccupancyState] = field(default_factory=dict)
    active_quiet_window_ids: frozenset[str] = field(default_factory=frozenset)
    quiet_window_notice_ids: frozenset[str] = field(default_factory=frozenset)
    owner_quiet_window_alert_ids: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def default(cls, spot_ids: Iterable[str]) -> RuntimeState:
        return cls(state_by_spot={spot_id: SpotOccupancyState() for spot_id in spot_ids})

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "spots": {spot_id: _spot_state_to_json(state) for spot_id, state in self.state_by_spot.items()},
            "active_quiet_window_ids": sorted(self.active_quiet_window_ids),
            "quiet_window_notice_ids": sorted(self.quiet_window_notice_ids),
            "owner_quiet_window_alert_ids": sorted(self.owner_quiet_window_alert_ids),
        }


def load_runtime_state(path: str | os.PathLike[str], spot_ids: Iterable[str], logger: StructuredLogger | None = None) -> RuntimeState:
    """Load runtime state from JSON, quarantining corrupt input and falling back to defaults."""

    state_path = Path(path)
    configured_spot_ids = list(spot_ids)
    if not state_path.exists():
        state = RuntimeState.default(configured_spot_ids)
        _log_loaded(logger, state_path, state, phase="missing-default")
        return state

    try:
        state_size = state_path.stat().st_size
    except OSError as exc:
        quarantine_path = _quarantine_state_file(state_path)
        state = RuntimeState.default(configured_spot_ids)
        _log_quarantined(logger, state_path, quarantine_path, phase="stat", error=exc)
        _log_loaded(logger, state_path, state, phase="quarantined-default")
        return state
    if state_size > MAX_STATE_FILE_BYTES:
        quarantine_path = _quarantine_state_file(state_path)
        state = RuntimeState.default(configured_spot_ids)
        _log_quarantined(
            logger,
            state_path,
            quarantine_path,
            phase="size-validate",
            error=StateSchemaError(f"state file exceeds maximum size of {MAX_STATE_FILE_BYTES} bytes"),
        )
        _log_loaded(logger, state_path, state, phase="quarantined-default")
        return state

    try:
        with state_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        quarantine_path = _quarantine_state_file(state_path)
        state = RuntimeState.default(configured_spot_ids)
        _log_quarantined(logger, state_path, quarantine_path, phase="json-load", error=exc)
        _log_loaded(logger, state_path, state, phase="quarantined-default")
        return state

    try:
        state = _state_from_json(payload, configured_spot_ids)
    except StateSchemaError as exc:
        quarantine_path = _quarantine_state_file(state_path)
        state = RuntimeState.default(configured_spot_ids)
        _log_quarantined(logger, state_path, quarantine_path, phase="schema-validate", error=exc)
        _log_loaded(logger, state_path, state, phase="quarantined-default")
        return state

    _log_loaded(logger, state_path, state, phase="loaded")
    return state


def save_runtime_state(path: str | os.PathLike[str], state: RuntimeState, logger: StructuredLogger | None = None) -> None:
    """Atomically write runtime state JSON in the target file's parent directory."""

    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            delete=False,
            dir=state_path.parent,
            prefix=f".{state_path.name}.",
            suffix=".tmp",
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(state.to_json_dict(), handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_path, 0o644)
        os.replace(temp_path, state_path)
    except Exception as exc:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        _log_save_failed(logger, state_path, error=exc)
        raise

    _log_saved(logger, state_path, state)


def _state_from_json(payload: Any, configured_spot_ids: list[str]) -> RuntimeState:
    if not isinstance(payload, dict):
        raise StateSchemaError("state payload must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise StateSchemaError("unsupported state schema_version")

    raw_spots = payload.get("spots")
    if not isinstance(raw_spots, dict):
        raise StateSchemaError("spots must be an object")

    state_by_spot: dict[str, SpotOccupancyState] = {}
    for spot_id in configured_spot_ids:
        raw_state = raw_spots.get(spot_id)
        state_by_spot[spot_id] = SpotOccupancyState() if raw_state is None else _spot_state_from_json(spot_id, raw_state)

    return RuntimeState(
        state_by_spot=state_by_spot,
        active_quiet_window_ids=frozenset(_string_list(payload.get("active_quiet_window_ids"), "active_quiet_window_ids")),
        quiet_window_notice_ids=frozenset(_string_list(payload.get("quiet_window_notice_ids"), "quiet_window_notice_ids")),
        owner_quiet_window_alert_ids=frozenset(_string_list(payload.get("owner_quiet_window_alert_ids"), "owner_quiet_window_alert_ids")),
    )


def _spot_state_from_json(spot_id: str, payload: Any) -> SpotOccupancyState:
    if not isinstance(payload, dict):
        raise StateSchemaError(f"spot {spot_id} state must be an object")

    status_value = payload.get("status")
    try:
        status = OccupancyStatus(status_value)
    except ValueError as exc:
        raise StateSchemaError(f"spot {spot_id} status is invalid") from exc

    return SpotOccupancyState(
        status=status,
        hit_streak=_non_negative_int(payload.get("hit_streak"), f"spot {spot_id} hit_streak"),
        miss_streak=_non_negative_int(payload.get("miss_streak"), f"spot {spot_id} miss_streak"),
        last_bbox=_bbox_or_none(payload.get("last_bbox"), f"spot {spot_id} last_bbox"),
        open_event_emitted=_bool(payload.get("open_event_emitted"), f"spot {spot_id} open_event_emitted"),
    )


def _spot_state_to_json(state: SpotOccupancyState) -> dict[str, Any]:
    return {
        "status": OccupancyStatus(state.status).value,
        "hit_streak": int(state.hit_streak),
        "miss_streak": int(state.miss_streak),
        "last_bbox": list(state.last_bbox) if state.last_bbox is not None else None,
        "open_event_emitted": bool(state.open_event_emitted),
    }


def _non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise StateSchemaError(f"{field_name} must be a non-negative integer")
    return value


def _bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise StateSchemaError(f"{field_name} must be a boolean")
    return value


def _bbox_or_none(value: Any, field_name: str) -> tuple[float, float, float, float] | None:
    if value is None:
        return None
    if not isinstance(value, list | tuple) or len(value) != 4:
        raise StateSchemaError(f"{field_name} must be null or four numeric values")
    try:
        bbox = (float(value[0]), float(value[1]), float(value[2]), float(value[3]))
    except (TypeError, ValueError) as exc:
        raise StateSchemaError(f"{field_name} must contain numeric values") from exc
    if not all(math.isfinite(coordinate) for coordinate in bbox):
        raise StateSchemaError(f"{field_name} must contain finite numeric values")
    return bbox


def _string_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise StateSchemaError(f"{field_name} must be a list")
    if len(value) > MAX_STATE_LIST_ITEMS:
        raise StateSchemaError(f"{field_name} exceeds maximum item count of {MAX_STATE_LIST_ITEMS}")
    if not all(isinstance(item, str) for item in value):
        raise StateSchemaError(f"{field_name} must contain only strings")
    return value


def _quarantine_state_file(path: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    candidate = path.with_name(f"{path.name}.corrupt-{timestamp}")
    index = 1
    while candidate.exists():
        candidate = path.with_name(f"{path.name}.corrupt-{timestamp}-{index}")
        index += 1
    os.replace(path, candidate)
    return candidate


def _log_loaded(logger: StructuredLogger | None, path: Path, state: RuntimeState, *, phase: str) -> None:
    if logger is None:
        return
    logger.info("state-loaded", path=str(path), phase=phase, spot_count=len(state.state_by_spot))


def _log_saved(logger: StructuredLogger | None, path: Path, state: RuntimeState) -> None:
    if logger is None:
        return
    logger.info(
        "state-saved",
        path=str(path),
        spot_count=len(state.state_by_spot),
        active_quiet_window_count=len(state.active_quiet_window_ids),
        quiet_window_notice_count=len(state.quiet_window_notice_ids),
    )


def _log_quarantined(
    logger: StructuredLogger | None,
    path: Path,
    quarantine_path: Path,
    *,
    phase: str,
    error: BaseException,
) -> None:
    if logger is None:
        return
    logger.warning(
        "state-corrupt-quarantined",
        path=str(path),
        quarantine_path=str(quarantine_path),
        phase=phase,
        error_type=type(error).__name__,
        error_message=_safe_error_message(error),
    )


def _log_save_failed(logger: StructuredLogger | None, path: Path, *, error: BaseException) -> None:
    if logger is None:
        return
    logger.error(
        "state-save-failed",
        path=str(path),
        phase="atomic-replace",
        error_type=type(error).__name__,
        error_message=_safe_error_message(error),
    )


def _safe_error_message(error: BaseException) -> str:
    message = redact_diagnostic_text(error)
    return message.replace("raw_image_bytes", "<redacted>")
