from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from parking_spot_monitor.capture import CaptureError, FrameCaptureResult
from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix import MatrixError

LIVE_RTSP_CAPTURE_OK = "LIVE_RTSP_CAPTURE_OK"
LIVE_RTSP_CAPTURE_FAILED = "LIVE_RTSP_CAPTURE_FAILED"
LIVE_MATRIX_TEXT_OK = "LIVE_MATRIX_TEXT_OK"
LIVE_MATRIX_TEXT_FAILED = "LIVE_MATRIX_TEXT_FAILED"
LIVE_MATRIX_IMAGE_OK = "LIVE_MATRIX_IMAGE_OK"
LIVE_MATRIX_IMAGE_FAILED = "LIVE_MATRIX_IMAGE_FAILED"


def run_live_proof_once(
    settings: RuntimeSettings,
    data_dir: Path,
    *,
    logger: StructuredLogger,
    capture: Callable[[RuntimeSettings, str | Path], FrameCaptureResult],
    matrix_delivery: Any,
) -> int:
    """Run one operator-safe live RTSP + Matrix proof without inference or occupancy events."""

    logger.info("live-proof-started", data_dir=str(data_dir))
    try:
        result = capture(settings, data_dir)
    except CaptureError as exc:
        logger.error("live-proof-capture-failed", marker=LIVE_RTSP_CAPTURE_FAILED, **exc.diagnostics())
        return 1

    logger.info("live-proof-capture-ok", marker=LIVE_RTSP_CAPTURE_OK, **result.diagnostics())

    try:
        _send_text(matrix_delivery, latest_path=result.latest_path, observed_at=result.timestamp, selected_mode=result.selected_mode)
    except Exception as exc:
        logger.error("live-proof-matrix-text-failed", marker=LIVE_MATRIX_TEXT_FAILED, **_safe_matrix_error(exc))
        return 1
    logger.info("live-proof-matrix-text-ok", marker=LIVE_MATRIX_TEXT_OK)

    try:
        _send_image(matrix_delivery, latest_path=result.latest_path, observed_at=result.timestamp, selected_mode=result.selected_mode)
    except Exception as exc:
        logger.error("live-proof-matrix-image-failed", marker=LIVE_MATRIX_IMAGE_FAILED, **_safe_matrix_error(exc))
        return 1
    logger.info("live-proof-matrix-image-ok", marker=LIVE_MATRIX_IMAGE_OK)
    return 0


def _send_text(matrix_delivery: Any, *, latest_path: Path, observed_at: object, selected_mode: object) -> None:
    if hasattr(matrix_delivery, "send_live_proof_text"):
        matrix_delivery.send_live_proof_text(observed_at=observed_at, selected_mode=selected_mode)
        return
    if hasattr(matrix_delivery, "send_live_proof"):
        matrix_delivery.send_live_proof(latest_path=latest_path, observed_at=observed_at, selected_mode=selected_mode)
        return
    raise TypeError("matrix delivery does not support live proof text")


def _send_image(matrix_delivery: Any, *, latest_path: Path, observed_at: object, selected_mode: object) -> None:
    if hasattr(matrix_delivery, "send_live_proof_image"):
        matrix_delivery.send_live_proof_image(latest_path=latest_path, observed_at=observed_at, selected_mode=selected_mode)
        return
    if hasattr(matrix_delivery, "send_live_proof"):
        # Composite fakes used by tests record both phases in one call; do not call twice.
        return
    raise TypeError("matrix delivery does not support live proof image")


def _safe_matrix_error(error: BaseException) -> dict[str, Any]:
    if isinstance(error, MatrixError):
        return dict(error.diagnostics)
    return {"error_type": type(error).__name__}
