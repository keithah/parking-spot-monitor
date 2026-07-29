from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from parking_monitor.matrix_outbox_delivery import MatrixOutboxDelivery
from parking_monitor.outbox import LocalOutbox
from parking_spot_monitor.capture import CaptureError, FrameCaptureResult, StreamProfileCapture, capture_latest
from parking_spot_monitor.detection import (
    DetectionError,
    UltralyticsVehicleDetector,
)
from parking_spot_monitor.detection_lab import DetectionLabManager
from parking_spot_monitor.errors import ConfigError
from parking_spot_monitor.live_proof import run_live_proof_once
from parking_spot_monitor.logging import StructuredLogger, setup_logging
from parking_spot_monitor.matrix_client import MatrixClient
from parking_spot_monitor.matrix_cockpit import MatrixOperatorCockpitContext
from parking_spot_monitor.matrix_commands import MatrixCommandService
from parking_spot_monitor.matrix_snapshots import prune_event_snapshots
from parking_spot_monitor.capture_loop import run_capture_loop
from parking_spot_monitor.config import RuntimeSettings, load_settings
from parking_spot_monitor.operator_cockpit import build_who_snapshot_response
from parking_spot_monitor.operator_feedback import OperatorFeedbackLabeler
from parking_spot_monitor.paths import RuntimePaths, resolve_runtime_paths
from parking_spot_monitor.runtime_health import matrix_outbox_health_payload as _matrix_outbox_health_payload
from parking_spot_monitor.runtime_decision_memory import _append_lab_outcome_memory
from parking_spot_monitor.runtime_detection import _process_detection_for_capture
from parking_spot_monitor.runtime_overlay import _write_debug_overlay, _write_overlay_for_capture
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

DEFAULT_CONFIG_PATH = "/config/config.yaml"
DEFAULT_DATA_DIR = "/data"


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
    capture: StreamProfileCapture | None = None,
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
    def default_capture(
        loaded_settings: RuntimeSettings,
        output_dir: str | Path,
        *,
        stream_profile: str | None = None,
    ) -> FrameCaptureResult:
        return capture_latest(
            loaded_settings,
            output_dir,
            logger=logger,
            stream_profile=stream_profile,
        )

    capture_fn = capture if capture is not None else default_capture
    overlay_fn = overlay if overlay is not None else _write_debug_overlay
    detector_fn = detector_factory if detector_factory is not None else _default_detector_factory
    matrix_factory = matrix_delivery_factory if matrix_delivery_factory is not None else _default_matrix_delivery_factory
    command_factory = matrix_command_service_factory if matrix_command_service_factory is not None else _default_matrix_command_service_factory

    if args.capture_once:
        return _capture_once(settings, paths.data_dir, logger=logger, capture=capture_fn, overlay=overlay_fn, detector_factory=detector_fn)

    if args.live_proof_once:
        matrix_delivery = matrix_factory(settings, paths.data_dir, logger)
        try:
            return run_live_proof_once(
                settings,
                paths.data_dir,
                logger=logger,
                capture=capture_fn,
                matrix_delivery=matrix_delivery,
            )
        finally:
            _close_if_available(matrix_delivery)

    history_archive = VehicleHistoryArchive(paths.vehicle_history_dir, logger=logger)
    matrix_delivery = matrix_factory(settings, paths.data_dir, logger)
    matrix_command_service = command_factory(settings, paths.data_dir, logger, history_archive)
    try:
        return run_capture_loop(
            settings,
            paths.data_dir,
            logger=logger,
            capture=capture_fn,
            overlay=overlay_fn,
            detector_factory=detector_fn,
            matrix_delivery=matrix_delivery,
            history_archive=history_archive,
            matrix_command_service=matrix_command_service,
            sleep=sleep,
            max_iterations=max_iterations,
            now=now,
            startup_retention_failure_count=retention_result.failed_count,
        )
    finally:
        _close_if_available(matrix_command_service)
        _close_if_available(matrix_delivery)


def _capture_once(
    settings: RuntimeSettings,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    capture: StreamProfileCapture,
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
        _process_detection_for_capture(
            settings,
            detector,
            result.latest_path,
            frame_timestamp=result.timestamp,
            logger=logger,
            mode="capture-once",
            frame_geometry=result.frame_geometry,
        )
    except DetectionError as exc:
        logger.error("detection-frame-failed", mode="capture-once", **exc.diagnostics())
        return 1
    logger.info("capture-once-complete", **result.diagnostics())
    return 0


def _close_if_available(resource: Any | None) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        close()


def _default_detector_factory(settings: RuntimeSettings) -> UltralyticsVehicleDetector:
    return UltralyticsVehicleDetector(settings.detection.model)


class _LazyIncidentReplayDetector:
    """Lazy Matrix incident-review detector to avoid model loads at command-service startup."""

    def __init__(self, settings: RuntimeSettings) -> None:
        self._settings = settings
        self._detector: Any | None = None

    def detect(self, frame_path: str | Path, **kwargs: Any) -> Any:
        if self._detector is None:
            self._detector = _default_detector_factory(self._settings)
        return self._detector.detect(frame_path, **kwargs)


def _default_matrix_delivery_factory(settings: RuntimeSettings, data_dir: Path, logger: StructuredLogger) -> MatrixOutboxDelivery:
    paths = resolve_runtime_paths(settings, data_dir)
    client = MatrixClient(
        homeserver=settings.matrix.homeserver,
        access_token=settings.matrix.access_token.value,
        timeout_seconds=settings.matrix.timeout_seconds,
        retry_attempts=settings.matrix.retry_attempts,
        retry_backoff_seconds=settings.matrix.retry_backoff_seconds,
        retry_jitter_ratio=settings.matrix.retry_jitter_ratio,
        logger=logger,
    )
    outbox = LocalOutbox(paths.matrix_outbox_file)
    return MatrixOutboxDelivery(
        client=client,
        room_id=settings.matrix.room_id,
        data_dir=paths.data_dir,
        snapshots_dir=paths.snapshots_dir,
        outbox=outbox,
        logger=logger,
        snapshot_retention_count=settings.storage.snapshot_retention_count,
    )


def _default_matrix_command_service_factory(
    settings: RuntimeSettings,
    data_dir: Path,
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
    paths = resolve_runtime_paths(settings, data_dir)
    feedback_labeler = OperatorFeedbackLabeler(data_dir=paths.data_dir, snapshots_dir=paths.snapshots_dir, logger=logger)

    def who_snapshot_provider(base_text: str) -> Any:
        return build_who_snapshot_response(
            settings=settings,
            data_dir=paths.data_dir,
            base_text=base_text,
            logger=logger,
        )

    client = MatrixClient(
        homeserver=settings.matrix.homeserver,
        access_token=settings.matrix.access_token.value,
        timeout_seconds=settings.matrix.timeout_seconds,
        retry_attempts=settings.matrix.retry_attempts,
        retry_backoff_seconds=settings.matrix.retry_backoff_seconds,
        retry_jitter_ratio=settings.matrix.retry_jitter_ratio,
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
        unauthorized_reply_cooldown_seconds=settings.matrix.unauthorized_reply_cooldown_seconds,
        cockpit_context=MatrixOperatorCockpitContext(
            settings=settings,
            data_dir=paths.data_dir,
            health_path=paths.health_file,
            state_path=paths.state_file,
            matrix_outbox_path=paths.matrix_outbox_file,
            latest_path=paths.latest_frame,
            snapshots_dir=paths.snapshots_dir,
            detection_lab_manager=_default_detection_lab_manager(settings, paths, logger),
            incident_detector=_LazyIncidentReplayDetector(settings),
        ),
        feedback_labeler=feedback_labeler,
        who_snapshot_provider=who_snapshot_provider,
    )


def _default_detection_lab_manager(
    settings: RuntimeSettings,
    paths: RuntimePaths,
    logger: StructuredLogger,
) -> DetectionLabManager:
    del settings

    def record_outcome(status_payload: Mapping[str, Any]) -> None:
        _append_lab_outcome_memory(paths.decision_memory_file, status_payload, data_dir=paths.data_dir, logger=logger)

    return DetectionLabManager(paths.data_dir, logger=logger, outcome_recorder=record_outcome)


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
    storage["decision_memory_file"] = str(paths.decision_memory_file)
    storage["detection_lab_dir"] = str(paths.detection_lab_dir)
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
