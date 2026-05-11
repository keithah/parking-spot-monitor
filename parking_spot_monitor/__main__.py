from __future__ import annotations

import argparse
import os
import sys
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
    filter_spot_detections,
)
from parking_spot_monitor.errors import ConfigError
from parking_spot_monitor.health import HealthStatus, write_health_status
from parking_spot_monitor.live_proof import run_live_proof_once
from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, setup_logging
from parking_spot_monitor.matrix import MatrixClient, MatrixDelivery, MatrixError, open_spot_event_id, prune_event_snapshots
from parking_spot_monitor.occupancy import OccupancyEventType, update_occupancy
from parking_spot_monitor.paths import RuntimePaths, resolve_runtime_paths
from parking_spot_monitor.scheduler import QuietWindowEventType, evaluate_quiet_windows, quiet_window_notice_events
from parking_spot_monitor.state import RuntimeState, load_runtime_state, save_runtime_state

DEFAULT_CONFIG_PATH = "/config/config.yaml"
DEFAULT_DATA_DIR = "/data"


@dataclass(frozen=True)
class FrameUpdateResult:
    runtime_state: RuntimeState
    matrix_errors: list[dict[str, Any]]
    state_save_error: dict[str, Any] | None = None


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

    return _capture_loop(
        settings,
        paths.data_dir,
        logger=logger,
        capture=capture_fn,
        overlay=overlay_fn,
        detector_factory=detector_fn,
        matrix_delivery=matrix_factory(settings, paths.data_dir, logger),
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
    now_fn = now if now is not None else lambda: datetime.now(timezone.utc)
    consecutive_capture_failures = 0
    consecutive_detection_failures = 0
    last_frame_at: str | None = None
    selected_decode_mode: str | None = None
    last_matrix_error: dict[str, Any] | None = None
    last_error: dict[str, Any] | None = None
    state_save_error: dict[str, Any] | None = None
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
                )
                runtime_state = frame_update.runtime_state
                if frame_update.matrix_errors:
                    last_matrix_error = frame_update.matrix_errors[-1]
                    last_error = last_matrix_error
                state_save_error = frame_update.state_save_error
                if state_save_error is not None:
                    last_error = state_save_error
            logger.info("capture-loop-frame-written", iteration=iteration, **result.diagnostics())
            status = _health_status_for_loop(
                consecutive_capture_failures=consecutive_capture_failures,
                consecutive_detection_failures=consecutive_detection_failures,
                last_matrix_error=last_matrix_error,
                state_save_error=state_save_error,
                retention_failure_count=retention_failure_count,
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
    detections = detector.detect(latest_path, confidence_threshold=settings.detection.confidence_threshold)
    result = filter_spot_detections(
        detections,
        spots=_configured_spot_polygons(settings),
        allowed_classes=settings.detection.vehicle_classes,
        confidence_threshold=settings.detection.confidence_threshold,
        min_bbox_area_px=settings.detection.min_bbox_area_px,
        min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
        source_frame_path=str(latest_path),
        source_timestamp=frame_timestamp,
    )
    fields: dict[str, Any] = {
        "mode": mode,
        "frame_path": str(latest_path),
        "spot_ids": list(result.by_spot.keys()),
        "detection_count": len(detections),
        "accepted_count": sum(1 for spot in result.by_spot.values() if spot.accepted is not None),
        "rejection_counts": _stringify_rejection_counts(result),
        "thresholds": {
            "confidence_threshold": settings.detection.confidence_threshold,
            "min_bbox_area_px": settings.detection.min_bbox_area_px,
            "min_polygon_overlap_ratio": settings.detection.min_polygon_overlap_ratio,
        },
        "candidate_summaries": _candidate_summaries(result),
    }
    if iteration is not None:
        fields["iteration"] = iteration
    logger.info("detection-frame-processed", **fields)
    return result


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
) -> FrameUpdateResult:
    matrix_errors: list[dict[str, Any]] = []
    quiet_status = evaluate_quiet_windows(settings.quiet_windows, observed_at)
    notice_events = quiet_window_notice_events(
        previous_active_window_ids=runtime_state.active_quiet_window_ids,
        current=quiet_status,
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
    )
    try:
        save_runtime_state(state_path, updated_state, logger=logger)
    except Exception as exc:
        return FrameUpdateResult(
            runtime_state=runtime_state,
            matrix_errors=matrix_errors,
            state_save_error=_safe_error_context("state-save", exc),
        )
    return FrameUpdateResult(runtime_state=updated_state, matrix_errors=matrix_errors)



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

    if event_name in {QuietWindowEventType.STARTED.value, QuietWindowEventType.ENDED.value}:
        txn_id = str(event.get("event_id", ""))
        logger.info("matrix-delivery-attempt", event_type=event_name, event_id=txn_id, txn_id=txn_id, attempt=1)
        try:
            matrix_delivery.send_quiet_window_notice(dict(event))
        except Exception as exc:
            return _log_matrix_delivery_failed(logger, event_name=event_name, event=event, txn_id=txn_id, error=exc)
        logger.info("matrix-delivery-succeeded", event_type=event_name, event_id=txn_id, txn_id=txn_id, attempt=1)
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
    logger.info(
        "matrix-delivery-skipped",
        event_type=event_name,
        spot_id=event.get("spot_id"),
        event_id=event.get("event_id"),
        reason=reason,
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
        "snapshot_path": event.get("snapshot_path"),
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
) -> str:
    if consecutive_capture_failures:
        return "down"
    if consecutive_detection_failures or last_matrix_error is not None or state_save_error is not None or retention_failure_count:
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


def _configured_spot_polygons(settings: RuntimeSettings) -> dict[str, list[tuple[int, int]]]:
    return {
        "left_spot": _spot_polygon(settings.spots.left_spot),
        "right_spot": _spot_polygon(settings.spots.right_spot),
    }


def _spot_polygon(spot: SpotConfig) -> list[tuple[int, int]]:
    return [(point.x, point.y) for point in spot.polygon]


def _stringify_rejection_counts(result: DetectionFilterResult) -> dict[str, int]:
    return {str(reason): count for reason, count in result.rejection_counts.items()}


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
