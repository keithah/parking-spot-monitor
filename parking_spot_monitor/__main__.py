from __future__ import annotations

import argparse
import inspect
import math
import os
import sys
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parking_spot_monitor.capture import CaptureError, capture_latest
from parking_spot_monitor.config import RuntimeSettings, SpotConfig, load_settings
from parking_spot_monitor.detection import (
    DetectionError,
    DetectionFilterResult,
    RejectionReason,
    SpotDetectionCandidate,
    UltralyticsVehicleDetector,
    crop_region_for_polygon,
    filter_spot_detections,
    translate_crop_detection,
)
from parking_spot_monitor.errors import ConfigError
from parking_spot_monitor.health import HealthStatus, write_health_status
from parking_spot_monitor.live_proof import run_live_proof_once
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, setup_logging
from parking_spot_monitor.matrix import (
    OCCUPIED_SPOT_EVENT_TYPE,
    MatrixClient,
    MatrixCommandService,
    MatrixDelivery,
    OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
    owner_vehicle_quiet_window_event_id,
    MatrixError,
    occupied_spot_event_id,
    open_spot_event_id,
    prune_event_snapshots,
)
from parking_spot_monitor.occupancy import OccupancyEvent, OccupancyEventType, OccupancyStatus, update_occupancy
from parking_spot_monitor.owner_vehicles import load_owner_vehicle_registry
from parking_spot_monitor.paths import RuntimePaths, resolve_runtime_paths
from parking_spot_monitor.scheduler import QuietWindowEventType, evaluate_quiet_windows, quiet_window_notice_events
from parking_spot_monitor.state import RuntimeState, load_runtime_state, save_runtime_state
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

DEFAULT_CONFIG_PATH = "/config/config.yaml"
DEFAULT_DATA_DIR = "/data"
OWNER_VEHICLE_MIN_PROFILE_CONFIDENCE = 0.95


@dataclass(frozen=True)
class VehicleHistoryEventResult:
    errors: list[dict[str, Any]]
    occupied_alerts: list[dict[str, Any]]


@dataclass(frozen=True)
class FrameUpdateResult:
    runtime_state: RuntimeState
    matrix_errors: list[dict[str, Any]]
    state_save_error: dict[str, Any] | None = None
    history_errors: list[dict[str, Any]] | None = None


class ArgumentParseError(Exception):
    """Raised instead of exiting so tests and callers receive a return code."""

    def __init__(self, message: str, usage: str) -> None:
        super().__init__(message)
        self.message = message
        self.usage = usage


class StartupArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ArgumentParseError(message=message, usage=self.format_usage())


def build_parser() -> argparse.ArgumentParser:
    parser = StartupArgumentParser(description="Parking spot monitor service startup.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to YAML config file.")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="Runtime data directory override.")
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate config and exit without starting the monitor loop.",
    )
    parser.add_argument(
        "--capture-once",
        action="store_true",
        help="Capture one frame to latest.jpg and exit.",
    )
    parser.add_argument(
        "--live-proof-once",
        action="store_true",
        help="Capture one live frame and send labelled Matrix proof text/image evidence, then exit.",
    )
    parser.add_argument("--log-level", default="INFO", help="Startup log level: DEBUG, INFO, WARNING, ERROR, or CRITICAL.")
    return parser


def main(argv: Sequence[str] | None = None, environ: Mapping[str, str] | None = None) -> int:
    return _main(argv=argv, environ=environ)


def _main(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
    *,
    capture: Callable[[RuntimeSettings, str | Path], Any] | None = None,
    overlay: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    max_iterations: int | None = None,
    detector_factory: Callable[[RuntimeSettings], Any] | None = None,
    matrix_delivery_factory: Callable[[RuntimeSettings, Path, StructuredLogger], Any] | None = None,
    matrix_command_service_factory: Callable[[RuntimeSettings, Path, StructuredLogger, VehicleHistoryArchive], Any | None] | None = None,
    now: Callable[[], datetime] | None = None,
) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()

    try:
        args = parser.parse_args(args_list)
    except ArgumentParseError as exc:
        logger = setup_logging(level="INFO")
        logger.error("startup-arguments-invalid", message=exc.message)
        sys.stderr.write(exc.usage)
        sys.stderr.write(f"error: {exc.message}\n")
        return 2

    logger = setup_logging(level=args.log_level)
    config_path = str(args.config)
    data_dir = Path(args.data_dir)
    mode = _startup_mode(validate_config=args.validate_config, capture_once=args.capture_once, live_proof_once=args.live_proof_once)

    logger.info(
        "startup-config-load-start",
        config_path=config_path,
        data_dir=str(data_dir),
        mode=mode,
    )

    try:
        settings = load_settings(config_path, environ=os.environ if environ is None else environ)
        paths = resolve_runtime_paths(settings, data_dir)
        settings = _with_effective_runtime_paths(settings, paths)
    except ConfigError as exc:
        _log_config_error(logger, exc, config_path=config_path)
        return 2
    except Exception as exc:  # pragma: no cover - defensive startup boundary
        logger.error(
            "startup-unexpected-error",
            config_path=config_path,
            error_type=type(exc).__name__,
            message="unexpected startup failure",
        )
        return 1

    summary = _effective_sanitized_summary(settings, paths=paths)
    logger.info("startup-config-loaded", config_path=config_path, config=summary)

    if args.validate_config:
        logger.info("startup-ready", config_path=config_path, data_dir=str(paths.data_dir), mode="validate-config")
        return 0

    retention_result = prune_event_snapshots(
        paths.snapshots_dir,
        retention_count=settings.storage.snapshot_retention_count,
        logger=logger,
        trigger="startup",
    )

    logger.info("startup-ready", config_path=config_path, data_dir=str(paths.data_dir), mode=mode)
    capture_fn = capture if capture is not None else lambda loaded_settings, output_dir: capture_latest(
        loaded_settings,
        output_dir,
        logger=logger,
    )
    overlay_fn = overlay if overlay is not None else _write_debug_overlay
    detector_fn = detector_factory if detector_factory is not None else _default_detector_factory
    matrix_factory = matrix_delivery_factory if matrix_delivery_factory is not None else _default_matrix_delivery_factory
    command_factory = matrix_command_service_factory if matrix_command_service_factory is not None else _default_matrix_command_service_factory

    if args.capture_once:
        return _capture_once(settings, paths.data_dir, logger=logger, capture=capture_fn, overlay=overlay_fn, detector_factory=detector_fn)

    if args.live_proof_once:
        return run_live_proof_once(
            settings,
            paths.data_dir,
            logger=logger,
            capture=capture_fn,
            matrix_delivery=matrix_factory(settings, paths.data_dir, logger),
        )

    history_archive = VehicleHistoryArchive(paths.vehicle_history_dir, logger=logger)
    return _capture_loop(
        settings,
        paths.data_dir,
        logger=logger,
        capture=capture_fn,
        overlay=overlay_fn,
        detector_factory=detector_fn,
        matrix_delivery=matrix_factory(settings, paths.data_dir, logger),
        history_archive=history_archive,
        matrix_command_service=command_factory(settings, paths.data_dir, logger, history_archive),
        sleep=sleep,
        max_iterations=max_iterations,
        now=now,
        startup_retention_failure_count=retention_result.failed_count,
    )


def _capture_once(
    settings: RuntimeSettings,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    capture: Callable[[RuntimeSettings, str | Path], Any],
    overlay: Callable[..., Any],
    detector_factory: Callable[[RuntimeSettings], Any],
) -> int:
    try:
        result = capture(settings, data_dir)
    except CaptureError as exc:
        logger.error("capture-failed", **exc.diagnostics())
        return 1
    if not _write_overlay_for_capture(settings, result.latest_path, data_dir, logger=logger, overlay=overlay):
        return 1
    try:
        detector = detector_factory(settings)
        _process_detection_for_capture(settings, detector, result.latest_path, frame_timestamp=result.timestamp, logger=logger, mode="capture-once")
    except DetectionError as exc:
        logger.error("detection-frame-failed", mode="capture-once", **exc.diagnostics())
        return 1
    logger.info("capture-once-complete", **result.diagnostics())
    return 0


def _capture_loop(
    settings: RuntimeSettings,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    capture: Callable[[RuntimeSettings, str | Path], Any],
    overlay: Callable[..., Any],
    detector_factory: Callable[[RuntimeSettings], Any],
    matrix_delivery: Any | None,
    history_archive: VehicleHistoryArchive | None = None,
    matrix_command_service: Any | None = None,
    sleep: Callable[[float], None],
    max_iterations: int | None = None,
    now: Callable[[], datetime] | None = None,
    startup_retention_failure_count: int = 0,
) -> int:
    iteration = 0
    detector: Any | None = None
    spot_ids = list(_configured_spot_polygons(settings).keys())
    state_path = data_dir / "state.json"
    runtime_state = load_runtime_state(state_path, spot_ids, logger=logger)
    effective_history_archive = history_archive if history_archive is not None else VehicleHistoryArchive(data_dir / "vehicle-history", logger=logger)
    now_fn = now if now is not None else lambda: datetime.now(timezone.utc)
    consecutive_capture_failures = 0
    consecutive_detection_failures = 0
    last_frame_at: str | None = None
    selected_decode_mode: str | None = None
    last_matrix_error: dict[str, Any] | None = None
    last_error: dict[str, Any] | None = None
    state_save_error: dict[str, Any] | None = None
    last_vehicle_history_error: dict[str, Any] | None = None
    vehicle_history_failure_count = 0
    retention_failure_count = startup_retention_failure_count
    _write_loop_health(
        settings,
        logger=logger,
        status="degraded" if retention_failure_count else "starting",
        iteration=iteration,
        last_frame_at=last_frame_at,
        selected_decode_mode=selected_decode_mode,
        consecutive_capture_failures=consecutive_capture_failures,
        consecutive_detection_failures=consecutive_detection_failures,
        last_matrix_error=last_matrix_error,
        last_error=last_error,
        retention_failure_count=retention_failure_count,
        state_save_error=state_save_error,
        vehicle_history_failure_count=vehicle_history_failure_count,
        last_vehicle_history_error=last_vehicle_history_error,
        vehicle_history=effective_history_archive.health_snapshot(),
    )
    while max_iterations is None or iteration < max_iterations:
        iteration += 1
        logger.info("capture-loop-iteration", iteration=iteration, data_dir=str(data_dir))
        try:
            result = capture(settings, data_dir)
            consecutive_capture_failures = 0
            last_frame_at = _format_health_timestamp(result.timestamp)
            selected_decode_mode = str(result.selected_mode.value if hasattr(result.selected_mode, "value") else result.selected_mode)
            _write_overlay_for_capture(settings, result.latest_path, data_dir, logger=logger, overlay=overlay)
            try:
                if detector is None:
                    detector = detector_factory(settings)
                detection_result = _process_detection_for_capture(
                    settings,
                    detector,
                    result.latest_path,
                    frame_timestamp=result.timestamp,
                    logger=logger,
                    mode="runtime-loop",
                    iteration=iteration,
                )
            except DetectionError as exc:
                consecutive_detection_failures += 1
                last_error = _safe_error_context("detection", exc, extra={"iteration": iteration})
                logger.error("detection-frame-failed", mode="runtime-loop", iteration=iteration, **exc.diagnostics())
            else:
                consecutive_detection_failures = 0
                last_error = None
                observed_at = _observed_at(result.timestamp, now_fn)
                frame_update = _update_runtime_state_for_frame(
                    settings=settings,
                    runtime_state=runtime_state,
                    detection_result=detection_result,
                    observed_at=observed_at,
                    snapshot_path=str(result.latest_path),
                    logger=logger,
                    matrix_delivery=matrix_delivery,
                    state_path=state_path,
                    configured_spot_ids=spot_ids,
                    history_archive=effective_history_archive,
                )
                runtime_state = frame_update.runtime_state
                if frame_update.matrix_errors:
                    last_matrix_error = frame_update.matrix_errors[-1]
                    last_error = last_matrix_error
                if frame_update.history_errors:
                    vehicle_history_failure_count += len(frame_update.history_errors)
                    last_vehicle_history_error = frame_update.history_errors[-1]
                    last_error = last_vehicle_history_error
                state_save_error = frame_update.state_save_error
                if state_save_error is not None:
                    last_error = state_save_error
                command_error = _poll_matrix_commands_once(
                    matrix_command_service,
                    logger=logger,
                    iteration=iteration,
                )
                if command_error is not None:
                    vehicle_history_failure_count += 1
                    last_vehicle_history_error = command_error
                    last_error = command_error
            logger.info("capture-loop-frame-written", iteration=iteration, **result.diagnostics())
            status = _health_status_for_loop(
                consecutive_capture_failures=consecutive_capture_failures,
                consecutive_detection_failures=consecutive_detection_failures,
                last_matrix_error=last_matrix_error,
                state_save_error=state_save_error,
                retention_failure_count=retention_failure_count,
                vehicle_history_failure_count=vehicle_history_failure_count,
                last_vehicle_history_error=last_vehicle_history_error,
            )
            _write_loop_health(
                settings,
                logger=logger,
                status=status,
                iteration=iteration,
                last_frame_at=last_frame_at,
                selected_decode_mode=selected_decode_mode,
                consecutive_capture_failures=consecutive_capture_failures,
                consecutive_detection_failures=consecutive_detection_failures,
                last_matrix_error=last_matrix_error,
                last_error=last_error,
                retention_failure_count=retention_failure_count,
                state_save_error=state_save_error,
                vehicle_history_failure_count=vehicle_history_failure_count,
                last_vehicle_history_error=last_vehicle_history_error,
                vehicle_history=effective_history_archive.health_snapshot(),
            )
            logger.info("capture-loop-paced", iteration=iteration, sleep_seconds=settings.runtime.frame_interval_seconds)
            sleep(settings.runtime.frame_interval_seconds)
        except CaptureError as exc:
            consecutive_capture_failures += 1
            last_error = _safe_error_context("capture", exc, extra={"iteration": iteration})
            backoff_seconds = settings.stream.reconnect_seconds
            logger.error("capture-loop-failure", iteration=iteration, backoff_seconds=backoff_seconds, **exc.diagnostics())
            _write_loop_health(
                settings,
                logger=logger,
                status="down",
                iteration=iteration,
                last_frame_at=last_frame_at,
                selected_decode_mode=selected_decode_mode,
                consecutive_capture_failures=consecutive_capture_failures,
                consecutive_detection_failures=consecutive_detection_failures,
                last_matrix_error=last_matrix_error,
                last_error=last_error,
                retention_failure_count=retention_failure_count,
                state_save_error=state_save_error,
                vehicle_history_failure_count=vehicle_history_failure_count,
                last_vehicle_history_error=last_vehicle_history_error,
                vehicle_history=effective_history_archive.health_snapshot(),
            )
            sleep(backoff_seconds)
    return 0


def _default_detector_factory(settings: RuntimeSettings) -> UltralyticsVehicleDetector:
    return UltralyticsVehicleDetector(settings.detection.model)


def _default_matrix_delivery_factory(settings: RuntimeSettings, data_dir: Path, logger: StructuredLogger) -> MatrixDelivery:
    client = MatrixClient(
        homeserver=settings.matrix.homeserver,
        access_token=settings.matrix.access_token.value,
        timeout_seconds=settings.matrix.timeout_seconds,
        retry_attempts=settings.matrix.retry_attempts,
        retry_backoff_seconds=settings.matrix.retry_backoff_seconds,
        logger=logger,
    )
    return MatrixDelivery(
        client=client,
        room_id=settings.matrix.room_id,
        data_dir=data_dir,
        snapshots_dir=settings.storage.snapshots_dir,
        logger=logger,
        snapshot_retention_count=settings.storage.snapshot_retention_count,
    )


def _default_matrix_command_service_factory(
    settings: RuntimeSettings,
    _data_dir: Path,
    logger: StructuredLogger,
    archive: VehicleHistoryArchive,
) -> MatrixCommandService | None:
    if not settings.matrix.command_authorized_senders:
        logger.info(
            "matrix-command-disabled",
            phase="matrix-command",
            action="configure",
            reason="no-authorized-senders",
        )
        return None
    client = MatrixClient(
        homeserver=settings.matrix.homeserver,
        access_token=settings.matrix.access_token.value,
        timeout_seconds=settings.matrix.timeout_seconds,
        retry_attempts=settings.matrix.retry_attempts,
        retry_backoff_seconds=settings.matrix.retry_backoff_seconds,
        logger=logger,
    )
    return MatrixCommandService(
        client=client,
        archive=archive,
        room_id=settings.matrix.room_id,
        authorized_senders=settings.matrix.command_authorized_senders,
        command_prefix=settings.matrix.command_prefix,
        bot_user_id=settings.matrix.user_id,
        logger=logger,
    )


def _poll_matrix_commands_once(
    matrix_command_service: Any | None,
    *,
    logger: StructuredLogger,
    iteration: int,
) -> dict[str, Any] | None:
    if matrix_command_service is None:
        return None
    logger.info(
        "matrix-command-poll-attempt",
        phase="matrix-command",
        action="vehicle-history-correction",
        iteration=iteration,
    )
    try:
        result = matrix_command_service.poll_once()
    except Exception as exc:
        context = _safe_error_context(
            "matrix-command",
            exc,
            extra={
                "action": "vehicle-history-correction",
                "iteration": iteration,
            },
        )
        logger.warning("matrix-command-poll-failed", **context)
        return context
    logger.info(
        "matrix-command-poll-succeeded",
        phase="matrix-command",
        action="vehicle-history-correction",
        iteration=iteration,
        processed_count=getattr(result, "processed_count", None),
        ignored_count=getattr(result, "ignored_count", None),
        error_count=getattr(result, "error_count", None),
        bootstrapped=getattr(result, "bootstrapped", None),
    )
    return None


def _process_detection_for_capture(
    settings: RuntimeSettings,
    detector: Any,
    latest_path: Path,
    *,
    frame_timestamp: Any | None = None,
    logger: StructuredLogger,
    mode: str,
    iteration: int | None = None,
) -> DetectionFilterResult:
    actual_frame_size = _image_size(latest_path)
    configured_frame_size = (settings.stream.frame_width, settings.stream.frame_height)
    frame_size_mismatch = actual_frame_size is not None and actual_frame_size != configured_frame_size
    scale = _frame_scale(configured_frame_size=configured_frame_size, actual_frame_size=actual_frame_size)
    spot_polygons = _configured_spot_polygons(settings, scale=scale)
    full_frame_detections = _detect_vehicles_for_frame(settings, detector, latest_path)
    spot_crop_detections = _detect_spot_crop_vehicles_for_frame(
        settings,
        detector,
        latest_path,
        spot_polygons=spot_polygons,
        actual_frame_size=actual_frame_size,
    )
    detections = [*full_frame_detections, *spot_crop_detections]
    result = filter_spot_detections(
        detections,
        spots=spot_polygons,
        allowed_classes=settings.detection.vehicle_classes,
        confidence_threshold=settings.detection.confidence_threshold,
        min_bbox_area_px=_scaled_min_bbox_area(settings.detection.min_bbox_area_px, scale=scale),
        min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
        source_frame_path=str(latest_path),
        source_timestamp=frame_timestamp,
    )
    fields: dict[str, Any] = {
        "mode": mode,
        "frame_path": str(latest_path),
        "spot_ids": list(result.by_spot.keys()),
        "detection_count": len(detections),
        "full_frame_detection_count": len(full_frame_detections),
        "spot_crop_inference_enabled": settings.detection.spot_crop_inference,
        "spot_crop_detection_count": len(spot_crop_detections),
        "accepted_count": sum(1 for spot in result.by_spot.values() if spot.accepted is not None),
        "accepted_by_spot": _accepted_by_spot(result),
        "rejection_counts": _stringify_rejection_counts(result),
        "thresholds": {
            "confidence_threshold": settings.detection.confidence_threshold,
            "min_bbox_area_px": _scaled_min_bbox_area(settings.detection.min_bbox_area_px, scale=scale),
            "configured_min_bbox_area_px": settings.detection.min_bbox_area_px,
            "min_polygon_overlap_ratio": settings.detection.min_polygon_overlap_ratio,
        },
        "actual_frame_size": _frame_size_dict(actual_frame_size),
        "configured_frame_size": _frame_size_dict(configured_frame_size),
        "frame_size_mismatch": frame_size_mismatch,
        "candidate_summaries": _candidate_summaries(result),
    }
    if iteration is not None:
        fields["iteration"] = iteration
    logger.info("detection-frame-processed", **fields)
    return result


def _detect_spot_crop_vehicles_for_frame(
    settings: RuntimeSettings,
    detector: Any,
    latest_path: Path,
    *,
    spot_polygons: Mapping[str, Sequence[tuple[float, float]]],
    actual_frame_size: tuple[int, int] | None,
) -> list[Any]:
    if not settings.detection.spot_crop_inference or actual_frame_size is None:
        return []

    try:
        from PIL import Image

        translated: list[Any] = []
        with tempfile.TemporaryDirectory(prefix="spot-crops-", dir=str(latest_path.parent)) as temp_dir:
            temp_root = Path(temp_dir)
            with Image.open(latest_path) as image:
                for spot_id, polygon in spot_polygons.items():
                    region = crop_region_for_polygon(
                        polygon,
                        frame_size=actual_frame_size,
                        margin_px=settings.detection.spot_crop_margin_px,
                        spot_id=spot_id,
                    )
                    crop_path = temp_root / f"{spot_id}.jpg"
                    image.crop((region.left, region.top, region.right, region.bottom)).save(crop_path, format="JPEG")
                    translated.extend(
                        translate_crop_detection(detection, offset_x=region.left, offset_y=region.top)
                        for detection in _detect_vehicles_for_frame(settings, detector, crop_path)
                    )
        return translated
    except DetectionError:
        raise


def _detect_vehicles_for_frame(settings: RuntimeSettings, detector: Any, latest_path: Path) -> list[Any]:
    kwargs: dict[str, Any] = {"confidence_threshold": settings.detection.confidence_threshold}
    if _detect_accepts_inference_image_size(detector):
        kwargs["inference_image_size"] = settings.detection.inference_image_size
    return detector.detect(latest_path, **kwargs)


def _detect_accepts_inference_image_size(detector: Any) -> bool:
    detect = getattr(detector, "detect", None)
    try:
        signature = inspect.signature(detect)
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD or name == "inference_image_size"
        for name, parameter in signature.parameters.items()
    )


def _update_runtime_state_for_frame(
    *,
    settings: RuntimeSettings,
    runtime_state: RuntimeState,
    detection_result: DetectionFilterResult,
    observed_at: datetime,
    snapshot_path: str,
    logger: StructuredLogger,
    matrix_delivery: Any | None,
    state_path: Path,
    configured_spot_ids: Sequence[str],
    history_archive: VehicleHistoryArchive | None = None,
) -> FrameUpdateResult:
    matrix_errors: list[dict[str, Any]] = []
    quiet_status = evaluate_quiet_windows(settings.quiet_windows, observed_at)
    notice_events = quiet_window_notice_events(
        previous_active_window_ids=runtime_state.active_quiet_window_ids,
        current=quiet_status,
        emitted_notice_ids=runtime_state.quiet_window_notice_ids,
    )
    emitted_notice_ids = set(runtime_state.quiet_window_notice_ids)
    for notice in notice_events:
        if notice.event_id in emitted_notice_ids:
            continue
        payload = notice.to_dict()
        event_name = str(payload.pop("event_type"))
        logger.info(event_name, **payload)
        matrix_error = _dispatch_matrix_event(matrix_delivery, event_name, payload | {"event_type": event_name}, logger=logger)
        if matrix_error is not None:
            matrix_errors.append(matrix_error)
        emitted_notice_ids.add(notice.event_id)

    occupancy_update = update_occupancy(
        runtime_state.state_by_spot,
        {spot_id: spot_result.accepted for spot_id, spot_result in detection_result.by_spot.items()},
        settings.occupancy,
        observed_at.isoformat(),
        quiet_status,
        snapshot_path,
        configured_spot_ids=configured_spot_ids,
        presence_by_spot=_presence_by_spot(detection_result),
    )
    history_result = _record_vehicle_history_events(
        history_archive,
        occupancy_update.events,
        detection_result=detection_result,
        snapshot_path=snapshot_path,
        logger=logger,
    )
    history_errors = history_result.errors
    owner_alert_ids = set(runtime_state.owner_quiet_window_alert_ids)
    owner_alerts = _owner_vehicle_quiet_window_alerts(
        history_archive,
        quiet_status=quiet_status,
        observed_at=observed_at,
        emitted_alert_ids=owner_alert_ids,
        configured_spot_ids=configured_spot_ids,
        logger=logger,
    )

    for owner_alert in owner_alerts:
        event_name = str(owner_alert.get("event_type", OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE))
        logger.info(event_name, **{key: value for key, value in owner_alert.items() if key != "event_type"})
        matrix_error = _dispatch_matrix_event(matrix_delivery, event_name, owner_alert, logger=logger)
        if matrix_error is not None:
            matrix_errors.append(matrix_error)
        event_id = owner_alert.get("event_id")
        if isinstance(event_id, str) and event_id:
            owner_alert_ids.add(event_id)

    for occupied_alert in history_result.occupied_alerts:
        matrix_error = _dispatch_matrix_event(
            matrix_delivery,
            str(occupied_alert.get("event_type", OCCUPIED_SPOT_EVENT_TYPE)),
            occupied_alert,
            logger=logger,
        )
        if matrix_error is not None:
            matrix_errors.append(matrix_error)

    for event in occupancy_update.events:
        payload = event.to_dict()
        event_name = str(payload.pop("event_type"))
        logger.info(event_name, **payload)
        matrix_error = _dispatch_matrix_event(matrix_delivery, event_name, payload | {"event_type": event_name}, logger=logger)
        if matrix_error is not None:
            matrix_errors.append(matrix_error)

    updated_state = RuntimeState(
        state_by_spot=occupancy_update.state_by_spot,
        active_quiet_window_ids=quiet_status.active_window_ids,
        quiet_window_notice_ids=frozenset(emitted_notice_ids),
        owner_quiet_window_alert_ids=frozenset(owner_alert_ids),
    )
    try:
        save_runtime_state(state_path, updated_state, logger=logger)
    except Exception as exc:
        return FrameUpdateResult(
            runtime_state=runtime_state,
            matrix_errors=matrix_errors,
            state_save_error=_safe_error_context("state-save", exc),
            history_errors=history_errors,
        )
    return FrameUpdateResult(runtime_state=updated_state, matrix_errors=matrix_errors, history_errors=history_errors)





def _owner_vehicle_quiet_window_alerts(
    history_archive: VehicleHistoryArchive | None,
    *,
    quiet_status: Any,
    observed_at: datetime,
    emitted_alert_ids: set[str],
    configured_spot_ids: Sequence[str],
    logger: StructuredLogger,
) -> list[dict[str, Any]]:
    if history_archive is None or not getattr(quiet_status, "active", False):
        return []
    window_id = getattr(quiet_status, "active_window_id", None)
    if not isinstance(window_id, str) or not window_id:
        return []
    registry = load_owner_vehicle_registry(history_archive.root / "owner-vehicles.json")
    configured = set(configured_spot_ids)
    alerts: list[dict[str, Any]] = []
    try:
        sessions = history_archive.load_active_sessions()
    except Exception as exc:
        logger.warning(
            "owner-vehicle-alert-scan-failed",
            phase="owner-vehicle",
            action="scan-active-sessions",
            error_type=type(exc).__name__,
            error_message=redact_diagnostic_text(exc),
        )
        return []
    for session in sessions:
        if session.spot_id not in configured:
            continue
        owner = registry.owner_for_profile(session.profile_id)
        if owner is None:
            continue
        if not _owner_vehicle_profile_confidence_is_high_enough(session.profile_confidence):
            logger.info(
                "owner-vehicle-alert-skipped",
                reason="profile-confidence-too-low",
                spot_id=session.spot_id,
                session_id=session.session_id,
                profile_id=session.profile_id,
                profile_confidence=session.profile_confidence,
                min_profile_confidence=OWNER_VEHICLE_MIN_PROFILE_CONFIDENCE,
            )
            continue
        payload = {
            "event_type": OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE,
            "spot_id": session.spot_id,
            "observed_at": observed_at.isoformat(),
            "window_id": window_id,
            "profile_id": owner.profile_id,
            "session_id": session.session_id,
            "owner_vehicle": owner.to_alert_payload(),
        }
        event_id = owner_vehicle_quiet_window_event_id(payload)
        if event_id in emitted_alert_ids:
            continue
        payload["event_id"] = event_id
        alerts.append(payload)
    return alerts


def _owner_vehicle_profile_confidence_is_high_enough(confidence: float | None) -> bool:
    if confidence is None:
        return False
    try:
        value = float(confidence)
    except (TypeError, ValueError):
        return False
    return math.isfinite(value) and value >= OWNER_VEHICLE_MIN_PROFILE_CONFIDENCE


def _occupancy_history_event_id(event: OccupancyEvent) -> str:
    payload = event.to_dict()
    return str(payload.get("event_id") or f"{event.event_type.value}:{event.spot_id}:{event.observed_at}")


def _record_vehicle_history_events(
    history_archive: VehicleHistoryArchive | None,
    events: Sequence[OccupancyEvent],
    *,
    detection_result: DetectionFilterResult | None = None,
    snapshot_path: str | None = None,
    logger: StructuredLogger,
) -> VehicleHistoryEventResult:
    history_errors: list[dict[str, Any]] = []
    occupied_alerts: list[dict[str, Any]] = []
    if history_archive is None:
        return VehicleHistoryEventResult(errors=history_errors, occupied_alerts=occupied_alerts)
    for event in events:
        if event.event_type is not OccupancyEventType.STATE_CHANGED:
            logger.info(
                "vehicle-session-lifecycle-ignored",
                event_type=event.event_type.value,
                spot_id=event.spot_id,
                reason="not-state-changed",
            )
            continue
        previous_status = event.previous_status
        new_status = event.new_status
        if previous_status is not OccupancyStatus.OCCUPIED and new_status is OccupancyStatus.OCCUPIED:
            logger.info(
                "vehicle-session-lifecycle-attempt",
                action="start",
                spot_id=event.spot_id,
                event_id=_occupancy_history_event_id(event),
            )
            try:
                record = history_archive.start_session(event)
            except Exception as exc:  # preserve Matrix/open-alert delivery when archive recording fails
                context = _safe_error_context(
                    "vehicle-history",
                    exc,
                    extra={
                        "action": "start",
                        "event_type": event.event_type.value,
                        "spot_id": event.spot_id,
                        "event_id": _occupancy_history_event_id(event),
                    },
                )
                history_errors.append(context)
                logger.error("vehicle-history-record-failed", **context)
            else:
                logger.info(
                    "vehicle-session-lifecycle-recorded",
                    action="start",
                    spot_id=event.spot_id,
                    session_id=record.session_id,
                )
                accepted = None
                if detection_result is not None:
                    spot_detection = detection_result.by_spot.get(event.spot_id)
                    if spot_detection is not None:
                        accepted = spot_detection.accepted
                source_frame_path = snapshot_path
                if accepted is None or source_frame_path is None:
                    context = _safe_error_context(
                        "vehicle-history",
                        RuntimeError("accepted occupied candidate or source frame missing"),
                        extra={
                            "action": "attach-images",
                            "image_phase": "image-capture",
                            "event_type": event.event_type.value,
                            "spot_id": event.spot_id,
                            "event_id": _occupancy_history_event_id(event),
                            "session_id": record.session_id,
                        },
                    )
                    history_errors.append(context)
                    logger.error("vehicle-history-record-failed", **context)
                else:
                    try:
                        image_record = history_archive.attach_occupied_images(
                            session_id=record.session_id,
                            source_frame_path=source_frame_path,
                            bbox=accepted.bbox,
                        )
                    except Exception as exc:  # keep the session lifecycle recorded when image capture fails
                        context = _safe_error_context(
                            "vehicle-history",
                            exc,
                            extra={
                                "action": "attach-images",
                                "image_phase": "image-capture",
                                "event_type": event.event_type.value,
                                "spot_id": event.spot_id,
                                "event_id": _occupancy_history_event_id(event),
                                "session_id": record.session_id,
                            },
                        )
                        history_errors.append(context)
                        logger.error("vehicle-history-record-failed", **context)
                    else:
                        logger.info(
                            "vehicle-session-images-attached",
                            action="attach-images",
                            spot_id=event.spot_id,
                            session_id=image_record.session_id,
                            occupied_snapshot_attached=image_record.occupied_snapshot_path is not None,
                            occupied_crop_attached=image_record.occupied_crop_path is not None,
                        )
                        profile_assignment = None
                        if image_record.occupied_crop_path is not None:
                            try:
                                profile_assignment = history_archive.match_or_create_profile(session_id=record.session_id)
                            except Exception as exc:  # keep the session lifecycle and image archive when profile matching fails
                                context = _safe_error_context(
                                    "vehicle-history",
                                    exc,
                                    extra={
                                        "action": "match-profile",
                                        "profile_phase": "profile-match",
                                        "event_type": event.event_type.value,
                                        "spot_id": event.spot_id,
                                        "event_id": _occupancy_history_event_id(event),
                                        "session_id": record.session_id,
                                    },
                                )
                                history_errors.append(context)
                                logger.error("vehicle-history-record-failed", **context)
                            else:
                                logger.info(
                                    "vehicle-session-profile-matched",
                                    action="match-profile",
                                    spot_id=event.spot_id,
                                    session_id=profile_assignment.session_id,
                                    match_status=profile_assignment.status,
                                    profile_id=profile_assignment.profile_id,
                                    profile_confidence=profile_assignment.profile_confidence,
                                )
                        occupied_alert = _occupied_alert_payload(
                            history_archive,
                            event,
                            session_id=record.session_id,
                            image_record=image_record,
                            profile_assignment=profile_assignment,
                            logger=logger,
                        )
                        if occupied_alert is not None:
                            occupied_alerts.append(occupied_alert)
            continue
        if previous_status is OccupancyStatus.OCCUPIED and new_status is OccupancyStatus.EMPTY:
            logger.info(
                "vehicle-session-lifecycle-attempt",
                action="close",
                spot_id=event.spot_id,
                event_id=_occupancy_history_event_id(event),
            )
            try:
                record = history_archive.close_session(event)
            except Exception as exc:  # preserve Matrix/open-alert delivery when archive recording fails
                context = _safe_error_context(
                    "vehicle-history",
                    exc,
                    extra={
                        "action": "close",
                        "event_type": event.event_type.value,
                        "spot_id": event.spot_id,
                        "event_id": _occupancy_history_event_id(event),
                    },
                )
                history_errors.append(context)
                logger.error("vehicle-history-record-failed", **context)
            else:
                logger.info(
                    "vehicle-session-lifecycle-recorded",
                    action="close",
                    spot_id=event.spot_id,
                    session_id=None if record is None else record.session_id,
                    result="noop" if record is None else "closed",
                )
            continue
        logger.info(
            "vehicle-session-lifecycle-ignored",
            event_type=event.event_type.value,
            spot_id=event.spot_id,
            previous_status=None if previous_status is None else previous_status.value,
            new_status=None if new_status is None else new_status.value,
            reason="not-lifecycle-transition",
        )
    return VehicleHistoryEventResult(errors=history_errors, occupied_alerts=occupied_alerts)


def _occupied_alert_payload(
    history_archive: VehicleHistoryArchive,
    event: OccupancyEvent,
    *,
    session_id: str,
    image_record: Any,
    profile_assignment: Any | None,
    logger: StructuredLogger,
) -> dict[str, Any] | None:
    occupied_snapshot_path = getattr(image_record, "occupied_snapshot_path", None)
    if not isinstance(occupied_snapshot_path, str) or not occupied_snapshot_path.strip():
        logger.info(
            "vehicle-history-occupied-alert-skipped",
            event_type=OCCUPIED_SPOT_EVENT_TYPE,
            spot_id=event.spot_id,
            event_id=_occupancy_history_event_id(event),
            session_id=session_id,
            reason="missing-occupied-snapshot",
        )
        return None

    profile_id = getattr(profile_assignment, "profile_id", None)
    profile_confidence = getattr(profile_assignment, "profile_confidence", None)
    match_status = getattr(profile_assignment, "status", None)
    match_reason = getattr(profile_assignment, "reason", None)

    label = _profile_label_for_alert(history_archive, profile_id, logger=logger, spot_id=event.spot_id, session_id=session_id)
    estimate = _estimate_for_alert(history_archive, session_id, logger=logger, spot_id=event.spot_id)

    payload: dict[str, Any] = {
        "event_type": OCCUPIED_SPOT_EVENT_TYPE,
        "spot_id": event.spot_id,
        "observed_at": event.observed_at,
        "source_timestamp": event.source_timestamp,
        "event_id": _occupancy_history_event_id(event),
        "session_id": session_id,
        "profile_id": profile_id,
        "profile_label": label,
        "profile_confidence": profile_confidence,
        "match_status": match_status,
        "match_reason": match_reason,
        "occupied_snapshot_path": occupied_snapshot_path,
        "likely_vehicle": {
            "label": label or profile_id or "unknown vehicle",
            "profile_id": profile_id,
            "profile_confidence": profile_confidence,
            "confidence": profile_confidence,
            "match_status": match_status,
            "match_reason": match_reason,
        },
        "vehicle_history_estimate": estimate,
    }
    return payload


def _profile_label_for_alert(
    history_archive: VehicleHistoryArchive,
    profile_id: object,
    *,
    logger: StructuredLogger,
    spot_id: str,
    session_id: str,
) -> str | None:
    if not isinstance(profile_id, str) or not profile_id.strip():
        return None
    try:
        label = history_archive.effective_label(profile_id)
    except Exception as exc:
        logger.warning(
            "vehicle-history-profile-label-failed",
            phase="vehicle-history",
            action="effective-label",
            spot_id=spot_id,
            session_id=session_id,
            error_type=type(exc).__name__,
            error_message=redact_diagnostic_text(exc),
        )
        return None
    return label if isinstance(label, str) and label.strip() else None


def _estimate_for_alert(
    history_archive: VehicleHistoryArchive,
    session_id: str,
    *,
    logger: StructuredLogger,
    spot_id: str,
) -> dict[str, Any]:
    try:
        estimate = history_archive.estimate_for_session(session_id)
    except Exception as exc:
        logger.warning(
            "vehicle-history-estimate-failed",
            phase="vehicle-history",
            action="estimate-for-session",
            spot_id=spot_id,
            session_id=session_id,
            error_type=type(exc).__name__,
            error_message=redact_diagnostic_text(exc),
        )
        return {
            "status": "insufficient_history",
            "reason": "estimate-error",
            "profile_id": None,
            "sample_count": 0,
            "confidence": "unknown",
            "dwell_range": None,
            "leave_time_window": None,
        }
    return _vehicle_history_estimate_payload(estimate)


def _vehicle_history_estimate_payload(estimate: Any) -> dict[str, Any]:
    return {
        "status": getattr(estimate, "status", "insufficient_history"),
        "reason": getattr(estimate, "reason", None),
        "profile_id": getattr(estimate, "profile_id", None),
        "sample_count": getattr(estimate, "sample_count", 0),
        "confidence": getattr(estimate, "confidence", "unknown"),
        "dwell_range": _dataclass_like_payload(getattr(estimate, "dwell_range", None)),
        "leave_time_window": _dataclass_like_payload(getattr(estimate, "leave_time_window", None)),
    }


def _dataclass_like_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    fields = getattr(value, "__dataclass_fields__", None)
    if isinstance(fields, dict):
        return {name: getattr(value, name) for name in fields}
    if isinstance(value, Mapping):
        return dict(value)
    return None


def _event_mapping_field(source: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = source.get(name)
    return value if isinstance(value, Mapping) else {}


def _presence_by_spot(result: DetectionFilterResult) -> dict[str, bool]:
    """Return weak vehicle-presence evidence that suppresses open alerts.

    Accepted candidates confirm normal occupancy. Rejections that still prove a
    vehicle-like object is inside the spot should prevent release/open alerts,
    but they must not confirm a new occupied state on their own. Centroid-outside
    and class-not-allowed rejections are excluded so pedestrians, driveway cars,
    and passing traffic outside the spot do not keep spots occupied forever.
    """

    suppressing_reasons = {
        RejectionReason.AREA_TOO_SMALL,
        RejectionReason.OVERLAP_TOO_LOW,
        RejectionReason.CONFIDENCE_TOO_LOW,
    }
    presence: dict[str, bool] = {}
    for spot_id, spot_result in result.by_spot.items():
        presence[spot_id] = spot_result.accepted is not None or any(
            rejected.reason in suppressing_reasons for rejected in spot_result.rejected
        )
    return presence


def _dispatch_matrix_event(matrix_delivery: Any | None, event_name: str, event: Mapping[str, Any], *, logger: StructuredLogger) -> dict[str, Any] | None:
    if matrix_delivery is None:
        logger.info("matrix-delivery-skipped", event_type=event_name, reason="not-configured")
        return None

    if event_name == OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE:
        txn_id = str(event.get("event_id") or owner_vehicle_quiet_window_event_id(event))
        logger.info("matrix-delivery-attempt", event_type=event_name, event_id=txn_id, txn_id=txn_id, attempt=1)
        try:
            matrix_delivery.send_owner_vehicle_quiet_window_alert(dict(event))
        except Exception as exc:
            return _log_matrix_delivery_failed(logger, event_name=event_name, event=event, txn_id=txn_id, error=exc)
        logger.info("matrix-delivery-succeeded", event_type=event_name, event_id=txn_id, txn_id=txn_id, attempt=1)
        return None

    if event_name in {QuietWindowEventType.UPCOMING.value, QuietWindowEventType.STARTED.value, QuietWindowEventType.ENDED.value}:
        txn_id = str(event.get("event_id", ""))
        logger.info("matrix-delivery-attempt", event_type=event_name, event_id=txn_id, txn_id=txn_id, attempt=1)
        try:
            matrix_delivery.send_quiet_window_notice(dict(event))
        except Exception as exc:
            return _log_matrix_delivery_failed(logger, event_name=event_name, event=event, txn_id=txn_id, error=exc)
        logger.info("matrix-delivery-succeeded", event_type=event_name, event_id=txn_id, txn_id=txn_id, attempt=1)
        return None

    if event_name == OCCUPIED_SPOT_EVENT_TYPE:
        txn_id = occupied_spot_event_id(event)
        logger.info(
            "matrix-delivery-attempt",
            event_type=event_name,
            spot_id=event.get("spot_id"),
            event_id=event.get("event_id"),
            txn_id=txn_id,
            session_id=event.get("session_id"),
            profile_id=event.get("profile_id"),
            estimate_status=_event_mapping_field(event, "vehicle_history_estimate").get("status"),
            occupied_snapshot_path=event.get("occupied_snapshot_path"),
            attempt=1,
        )
        try:
            matrix_delivery.send_occupied_spot_alert(dict(event))
        except Exception as exc:
            return _log_matrix_delivery_failed(logger, event_name=event_name, event=event, txn_id=txn_id, error=exc)
        logger.info(
            "matrix-delivery-succeeded",
            event_type=event_name,
            spot_id=event.get("spot_id"),
            event_id=event.get("event_id"),
            txn_id=txn_id,
            session_id=event.get("session_id"),
            profile_id=event.get("profile_id"),
            estimate_status=_event_mapping_field(event, "vehicle_history_estimate").get("status"),
            occupied_snapshot_path=event.get("occupied_snapshot_path"),
            attempt=1,
        )
        return None

    if event_name == OccupancyEventType.OPEN_EVENT.value:
        txn_id = open_spot_event_id(event)
        logger.info(
            "matrix-delivery-attempt",
            event_type=event_name,
            spot_id=event.get("spot_id"),
            txn_id=txn_id,
            snapshot_path=event.get("snapshot_path"),
            attempt=1,
        )
        try:
            matrix_delivery.send_open_spot_alert(dict(event))
        except Exception as exc:
            return _log_matrix_delivery_failed(logger, event_name=event_name, event=event, txn_id=txn_id, error=exc)
        logger.info(
            "matrix-delivery-succeeded",
            event_type=event_name,
            spot_id=event.get("spot_id"),
            txn_id=txn_id,
            snapshot_path=event.get("snapshot_path"),
            attempt=1,
        )
        return None

    reason = "suppressed" if event_name == OccupancyEventType.OPEN_SUPPRESSED.value else "unsupported-event-type"
    extra_fields: dict[str, Any] = {}
    if event_name == OccupancyEventType.STATE_CHANGED.value:
        reason = "state-change-not-alert"
        extra_fields = {
            "matrix_dispatch_policy": "open-events-only",
            "next_expected_event": OccupancyEventType.OPEN_EVENT.value,
        }
    logger.info(
        "matrix-delivery-skipped",
        event_type=event_name,
        spot_id=event.get("spot_id"),
        event_id=event.get("event_id"),
        reason=reason,
        **extra_fields,
    )
    return None


def _log_matrix_delivery_failed(
    logger: StructuredLogger,
    *,
    event_name: str,
    event: Mapping[str, Any],
    txn_id: str,
    error: BaseException,
) -> dict[str, Any]:
    diagnostics = dict(error.diagnostics) if isinstance(error, MatrixError) else {"error_type": type(error).__name__}
    attempt = diagnostics.pop("attempt", 1)
    context = {
        "phase": "matrix",
        "event_type": event_name,
        "event_id": event.get("event_id"),
        "spot_id": event.get("spot_id"),
        "txn_id": txn_id,
        "snapshot_path": event.get("snapshot_path") or event.get("occupied_snapshot_path"),
        "session_id": event.get("session_id"),
        "profile_id": event.get("profile_id"),
        "attempt": attempt,
        "final": True,
        **diagnostics,
    }
    logger.error("matrix-delivery-failed", **context)
    return context


def _write_loop_health(
    settings: RuntimeSettings,
    *,
    logger: StructuredLogger,
    status: str,
    iteration: int,
    last_frame_at: str | None,
    selected_decode_mode: str | None,
    consecutive_capture_failures: int,
    consecutive_detection_failures: int,
    last_matrix_error: Mapping[str, Any] | None,
    last_error: Mapping[str, Any] | None,
    retention_failure_count: int,
    state_save_error: Mapping[str, Any] | None,
    vehicle_history_failure_count: int = 0,
    last_vehicle_history_error: Mapping[str, Any] | None = None,
    vehicle_history: Mapping[str, Any] | None = None,
) -> None:
    try:
        write_health_status(
            settings.runtime.health_file,
            HealthStatus(
                status=status,  # type: ignore[arg-type]
                updated_at=datetime.now(timezone.utc).isoformat(),
                iteration=iteration,
                last_frame_at=last_frame_at,
                selected_decode_mode=selected_decode_mode,
                consecutive_capture_failures=consecutive_capture_failures,
                consecutive_detection_failures=consecutive_detection_failures,
                last_matrix_error=last_matrix_error,
                last_error=last_error,
                retention_failure_count=retention_failure_count,
                state_save_error=state_save_error,
                vehicle_history_failure_count=vehicle_history_failure_count,
                last_vehicle_history_error=last_vehicle_history_error,
                vehicle_history=vehicle_history,
            ),
            logger=logger,
        )
    except Exception as exc:
        logger.error(
            "health-write-failed",
            path=str(settings.runtime.health_file),
            error_type=type(exc).__name__,
            error_message=redact_diagnostic_text(exc),
        )


def _health_status_for_loop(
    *,
    consecutive_capture_failures: int,
    consecutive_detection_failures: int,
    last_matrix_error: Mapping[str, Any] | None,
    state_save_error: Mapping[str, Any] | None,
    retention_failure_count: int,
    vehicle_history_failure_count: int = 0,
    last_vehicle_history_error: Mapping[str, Any] | None = None,
) -> str:
    if consecutive_capture_failures:
        return "down"
    if (
        consecutive_detection_failures
        or last_matrix_error is not None
        or state_save_error is not None
        or retention_failure_count
        or vehicle_history_failure_count
        or last_vehicle_history_error is not None
    ):
        return "degraded"
    return "ok"


def _safe_error_context(phase: str, error: BaseException, *, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    context = {
        "phase": phase,
        "error_type": type(error).__name__,
        "error_message": redact_diagnostic_text(error),
    }
    if extra:
        context.update(dict(extra))
    return context


def _format_health_timestamp(value: Any | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _observed_at(frame_timestamp: Any | None, now: Callable[[], datetime]) -> datetime:
    parsed = _parse_frame_timestamp(frame_timestamp)
    observed = parsed if parsed is not None else now()
    if observed.tzinfo is None or observed.utcoffset() is None:
        return observed.replace(tzinfo=timezone.utc)
    return observed


def _parse_frame_timestamp(value: Any | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _configured_spot_polygons(
    settings: RuntimeSettings, *, scale: tuple[float, float] = (1.0, 1.0)
) -> dict[str, list[tuple[float, float]]]:
    return {
        "left_spot": _spot_polygon(settings.spots.left_spot, scale=scale),
        "right_spot": _spot_polygon(settings.spots.right_spot, scale=scale),
    }


def _spot_polygon(spot: SpotConfig, *, scale: tuple[float, float] = (1.0, 1.0)) -> list[tuple[float, float]]:
    scale_x, scale_y = scale
    return [(point.x * scale_x, point.y * scale_y) for point in spot.polygon]


def _image_size(path: Path) -> tuple[int, int] | None:
    try:
        from PIL import Image

        with Image.open(path) as image:
            return image.size
    except Exception:
        return None


def _frame_scale(
    *, configured_frame_size: tuple[int, int], actual_frame_size: tuple[int, int] | None
) -> tuple[float, float]:
    if actual_frame_size is None:
        return (1.0, 1.0)
    configured_width, configured_height = configured_frame_size
    actual_width, actual_height = actual_frame_size
    if configured_width <= 0 or configured_height <= 0:
        return (1.0, 1.0)
    return (actual_width / configured_width, actual_height / configured_height)


def _scaled_min_bbox_area(value: float, *, scale: tuple[float, float]) -> float:
    return float(value) * scale[0] * scale[1]


def _frame_size_dict(size: tuple[int, int] | None) -> dict[str, int] | None:
    if size is None:
        return None
    return {"width": int(size[0]), "height": int(size[1])}


def _stringify_rejection_counts(result: DetectionFilterResult) -> dict[str, int]:
    return {str(reason): count for reason, count in result.rejection_counts.items()}


def _accepted_by_spot(result: DetectionFilterResult) -> dict[str, bool]:
    return {spot_id: spot.accepted is not None for spot_id, spot in result.by_spot.items()}


def _candidate_summaries(result: DetectionFilterResult) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for spot in result.by_spot.values():
        if spot.accepted is not None:
            summaries.append(_candidate_summary(spot.accepted))
    return summaries


def _candidate_summary(candidate: SpotDetectionCandidate) -> dict[str, Any]:
    return {
        "bbox": list(candidate.bbox),
        "bbox_area_px": candidate.bbox_area_px,
        "centroid": list(candidate.centroid),
        "class_name": candidate.class_name,
        "confidence": candidate.confidence,
        "overlap_ratio": candidate.overlap_ratio,
        "source_frame_path": candidate.source_frame_path,
        "source_timestamp": candidate.source_timestamp,
        "spot_id": candidate.spot_id,
    }


def _write_debug_overlay(
    settings: RuntimeSettings,
    source_path: str | Path,
    output_path: str | Path,
    *,
    logger: StructuredLogger,
) -> Any:
    from parking_spot_monitor.debug_overlay import write_debug_overlay

    return write_debug_overlay(settings, source_path, output_path, logger=logger)


def _write_overlay_for_capture(
    settings: RuntimeSettings,
    latest_path: Path,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    overlay: Callable[..., Any],
) -> bool:
    output_path = data_dir / "debug_latest.jpg"
    try:
        overlay(settings, latest_path, output_path, logger=logger)
    except Exception as exc:
        if not _is_expected_debug_overlay_error(exc):
            logger.error(
                "debug-overlay-failed",
                source_path=str(latest_path),
                output_path=str(output_path),
                spot_ids=["left_spot", "right_spot"],
                width=None,
                height=None,
                error_type=type(exc).__name__,
                error_message="debug overlay failed unexpectedly",
            )
        return False
    return True


def _is_expected_debug_overlay_error(exc: Exception) -> bool:
    return type(exc).__name__ == "DebugOverlayError" and hasattr(exc, "diagnostics")


def _startup_mode(*, validate_config: bool, capture_once: bool, live_proof_once: bool) -> str:
    if validate_config:
        return "validate-config"
    if capture_once:
        return "capture-once"
    if live_proof_once:
        return "live-proof-once"
    return "runtime-loop"


def _log_config_error(logger: StructuredLogger, exc: ConfigError, *, config_path: str) -> None:
    logger.error(
        "startup-config-invalid",
        config_path=str(exc.path if exc.path is not None else config_path),
        phase=exc.phase,
        fields=list(exc.fields),
        missing_env=list(exc.missing_env),
        message=str(exc),
    )


def _effective_sanitized_summary(settings: RuntimeSettings, *, paths: RuntimePaths) -> dict[str, Any]:
    summary = settings.sanitized_summary()
    storage = dict(summary.get("storage", {}))
    storage["data_dir"] = str(paths.data_dir)
    storage["state_file"] = str(paths.state_file)
    storage["latest_frame"] = str(paths.latest_frame)
    storage["snapshots_dir"] = str(paths.snapshots_dir)
    storage["vehicle_history_dir"] = str(paths.vehicle_history_dir)
    summary["storage"] = storage
    runtime = dict(summary.get("runtime", {}))
    runtime["health_file"] = str(paths.health_file)
    summary["runtime"] = runtime
    return summary


def _with_effective_runtime_paths(settings: RuntimeSettings, paths: RuntimePaths) -> RuntimeSettings:
    return settings.model_copy(
        update={
            "storage": settings.storage.model_copy(
                update={"data_dir": paths.data_dir, "snapshots_dir": paths.snapshots_dir}
            ),
            "runtime": settings.runtime.model_copy(update={"health_file": paths.health_file}),
        }
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
