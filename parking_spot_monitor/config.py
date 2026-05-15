from __future__ import annotations

import os
import re
from os import PathLike
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Mapping, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, SecretStr, ValidationError, field_validator, model_validator

from parking_spot_monitor.errors import ConfigError


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ResolvedSecret(StrictModel):
    """Secret resolved from an environment variable with redacted serialization."""

    env_var: str
    _value: SecretStr = PrivateAttr()

    def __init__(self, *, env_var: str, value: str) -> None:
        super().__init__(env_var=env_var)
        self._value = SecretStr(value)

    @property
    def value(self) -> str:
        return self._value.get_secret_value()

    def __repr__(self) -> str:
        return f"ResolvedSecret(env_var={self.env_var!r}, value='**********')"

    def sanitized_summary(self) -> dict[str, str | bool]:
        return {"env_var": self.env_var, "present": bool(self.value), "value": "**********"}


class Point(StrictModel):
    x: int
    y: int

    @model_validator(mode="before")
    @classmethod
    def from_pair(cls, value: Any) -> Any:
        if isinstance(value, (list, tuple)) and len(value) == 2:
            return {"x": value[0], "y": value[1]}
        return value


class SpotConfig(StrictModel):
    name: str
    polygon: list[Point]

    @field_validator("polygon")
    @classmethod
    def polygon_has_area(cls, value: list[Point]) -> list[Point]:
        if len(value) < 3:
            raise ValueError("polygon must contain at least 3 points")
        return value


class SpotsConfig(StrictModel):
    left_spot: SpotConfig
    right_spot: SpotConfig


class StreamConfig(StrictModel):
    rtsp_url: ResolvedSecret
    frame_width: int = Field(gt=0)
    frame_height: int = Field(gt=0)
    reconnect_seconds: int = Field(default=5, gt=0)


class DetectionConfig(StrictModel):
    model: str = Field(min_length=1)
    confidence_threshold: float = Field(ge=0, le=1)
    inference_image_size: int | None = Field(default=None, gt=0)
    vehicle_classes: list[str] = Field(default_factory=list)
    min_bbox_area_px: float = Field(gt=0)
    min_polygon_overlap_ratio: float = Field(ge=0, le=1)

    @field_validator("model")
    @classmethod
    def model_must_be_local_without_traversal(cls, value: str) -> str:
        if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value):
            raise ValueError("detection.model must be a local model name or mounted file path, not a URL")
        path = PurePosixPath(value)
        if path.is_absolute():
            raise ValueError("detection.model must use a relative local model path, not an absolute path")
        if ".." in path.parts:
            raise ValueError("detection.model must not contain path traversal")
        return value


class OccupancyConfig(StrictModel):
    iou_threshold: float = Field(ge=0, le=1)
    confirm_frames: int = Field(gt=0)
    release_frames: int = Field(default=3, gt=0)


class MatrixConfig(StrictModel):
    homeserver: str
    room_id: str
    access_token: ResolvedSecret
    user_id: str | None = None
    command_prefix: str = Field(default="!parking", min_length=1, max_length=32)
    command_authorized_senders: list[str] = Field(default_factory=list)
    timeout_seconds: float = Field(default=10, gt=0)
    retry_attempts: int = Field(default=3, gt=0)
    retry_backoff_seconds: float = Field(default=1, ge=0)


WEEKDAYS = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
TIME_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


class QuietWindowConfig(StrictModel):
    name: str
    timezone: str
    recurrence: Literal["monthly_weekday"]
    weekdays: list[str]
    ordinals: list[int]
    start: str
    end: str
    reminder_minutes_before: int | None = Field(default=None, gt=0)

    @field_validator("timezone")
    @classmethod
    def timezone_must_exist(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("timezone must be a valid IANA timezone") from exc
        return value

    @field_validator("weekdays")
    @classmethod
    def weekdays_must_be_known_and_non_empty(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("weekdays must contain at least one weekday")
        invalid = [weekday for weekday in value if weekday not in WEEKDAYS]
        if invalid:
            raise ValueError("weekdays must use lowercase weekday names")
        return value

    @field_validator("ordinals")
    @classmethod
    def ordinals_must_be_supported_and_non_empty(cls, value: list[int]) -> list[int]:
        if not value:
            raise ValueError("ordinals must contain at least one month ordinal")
        invalid = [ordinal for ordinal in value if ordinal < 1 or ordinal > 5]
        if invalid:
            raise ValueError("ordinals must be between 1 and 5")
        return value

    @field_validator("start", "end")
    @classmethod
    def times_must_be_hhmm(cls, value: str) -> str:
        if not TIME_PATTERN.match(value):
            raise ValueError("time must use HH:MM 24-hour format")
        return value

    @model_validator(mode="after")
    def end_must_be_after_start(self) -> Self:
        if _minutes_since_midnight(self.end) <= _minutes_since_midnight(self.start):
            raise ValueError("end must be after start for daytime quiet windows")
        return self


class StorageConfig(StrictModel):
    data_dir: Path
    snapshots_dir: Path | None = None
    snapshot_retention_count: int = Field(default=50, gt=0)


class RuntimeConfig(StrictModel):
    health_file: Path
    log_level: str = "INFO"
    startup_timeout_seconds: int = Field(default=30, gt=0)
    frame_interval_seconds: float = Field(default=30, gt=0)


class RuntimeSettings(StrictModel):
    stream: StreamConfig
    spots: SpotsConfig
    detection: DetectionConfig
    occupancy: OccupancyConfig
    matrix: MatrixConfig
    quiet_windows: list[QuietWindowConfig] = Field(default_factory=list)
    storage: StorageConfig
    runtime: RuntimeConfig

    @model_validator(mode="after")
    def validate_polygon_bounds(self) -> Self:
        width = self.stream.frame_width
        height = self.stream.frame_height
        errors: list[str] = []
        for spot_name, spot in (
            ("spots.left_spot.polygon", self.spots.left_spot),
            ("spots.right_spot.polygon", self.spots.right_spot),
        ):
            for index, point in enumerate(spot.polygon):
                if not (0 <= point.x <= width and 0 <= point.y <= height):
                    errors.append(f"{spot_name}.{index}")
        if errors:
            raise ValueError("polygon points out of frame bounds: " + ",".join(errors))
        return self

    def sanitized_summary(self) -> dict[str, Any]:
        return {
            "stream": {
                "rtsp_url": self.stream.rtsp_url.sanitized_summary(),
                "frame_width": self.stream.frame_width,
                "frame_height": self.stream.frame_height,
                "reconnect_seconds": self.stream.reconnect_seconds,
            },
            "spots": {
                "left_spot": {"name": self.spots.left_spot.name, "points": len(self.spots.left_spot.polygon)},
                "right_spot": {"name": self.spots.right_spot.name, "points": len(self.spots.right_spot.polygon)},
            },
            "detection": {
                "model": self.detection.model,
                "confidence_threshold": self.detection.confidence_threshold,
                "inference_image_size": self.detection.inference_image_size,
                "vehicle_classes": list(self.detection.vehicle_classes),
                "min_bbox_area_px": self.detection.min_bbox_area_px,
                "min_polygon_overlap_ratio": self.detection.min_polygon_overlap_ratio,
            },
            "occupancy": {
                "iou_threshold": self.occupancy.iou_threshold,
                "confirm_frames": self.occupancy.confirm_frames,
                "release_frames": self.occupancy.release_frames,
            },
            "matrix": {
                "homeserver": self.matrix.homeserver,
                "room_id": self.matrix.room_id,
                "matrix_token": {
                    "env_var": "Matrix token env key",
                    "present": bool(self.matrix.access_token.value),
                    "value": "**********",
                },
                "user_id": self.matrix.user_id,
                "command_prefix": self.matrix.command_prefix,
                "command_authorized_senders_count": len(self.matrix.command_authorized_senders),
                "timeout_seconds": self.matrix.timeout_seconds,
                "retry_attempts": self.matrix.retry_attempts,
                "retry_backoff_seconds": self.matrix.retry_backoff_seconds,
            },
            "quiet_windows": [window.model_dump() for window in self.quiet_windows],
            "storage": {
                "data_dir": str(self.storage.data_dir),
                "snapshots_dir": str(self.storage.snapshots_dir) if self.storage.snapshots_dir else None,
                "snapshot_retention_count": self.storage.snapshot_retention_count,
            },
            "runtime": {
                "health_file": str(self.runtime.health_file),
                "log_level": self.runtime.log_level,
                "startup_timeout_seconds": self.runtime.startup_timeout_seconds,
                "frame_interval_seconds": self.runtime.frame_interval_seconds,
            },
        }


def load_settings(path: str | PathLike[str], environ: Mapping[str, str] | None = None) -> RuntimeSettings:
    config_path = Path(path)
    source_environ = os.environ if environ is None else environ

    try:
        raw_text = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError("config file could not be read", path=str(config_path), phase="read") from exc

    try:
        raw_config = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigError("config yaml could not be parsed", path=str(config_path), phase="yaml") from exc

    if not isinstance(raw_config, dict):
        raise ConfigError("config yaml root must be a mapping", path=str(config_path), phase="yaml")

    try:
        prepared = _resolve_secret_references(raw_config, source_environ)
    except ConfigError as exc:
        if exc.path is None:
            exc.path = str(config_path)
        raise

    try:
        return RuntimeSettings.model_validate(prepared)
    except ValidationError as exc:
        fields = tuple(_format_validation_error(error) for error in exc.errors(include_input=False))
        raise ConfigError("config schema validation failed", path=str(config_path), phase="schema", fields=fields) from exc
    except ValueError as exc:
        raise ConfigError("config schema validation failed", path=str(config_path), phase="schema", fields=(str(exc),)) from exc


def _minutes_since_midnight(value: str) -> int:
    hours, minutes = value.split(":", 1)
    return int(hours) * 60 + int(minutes)


def _resolve_secret_references(raw_config: dict[str, Any], environ: Mapping[str, str]) -> dict[str, Any]:
    config = _deep_copy(raw_config)
    missing: list[str] = []

    stream = config.get("stream")
    if isinstance(stream, dict):
        stream["rtsp_url"] = _resolve_env_secret(stream.pop("rtsp_url_env", None), environ, missing)

    matrix = config.get("matrix")
    if isinstance(matrix, dict):
        matrix["access_token"] = _resolve_env_secret(matrix.pop("access_token_env", None), environ, missing)

    if missing:
        raise ConfigError("required environment variables are missing or empty", phase="env", missing_env=tuple(sorted(set(missing))))

    return config


def _resolve_env_secret(env_var: Any, environ: Mapping[str, str], missing: list[str]) -> ResolvedSecret | Any:
    if not isinstance(env_var, str) or not env_var:
        return env_var
    value = environ.get(env_var)
    if value is None or value == "":
        missing.append(env_var)
        value = ""
    return ResolvedSecret(env_var=env_var, value=value)


def _deep_copy(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _deep_copy(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_deep_copy(item) for item in value]
    return value


def _format_validation_error(error: dict[str, Any]) -> str:
    location = _format_error_location(tuple(error.get("loc", ())))
    message = str(error.get("msg", "validation failed"))
    return f"{location}:{message}"


def _format_error_location(location: tuple[Any, ...]) -> str:
    return ".".join(str(part) for part in location) if location else "<root>"
