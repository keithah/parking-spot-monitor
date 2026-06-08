from __future__ import annotations

from pathlib import Path
from typing import Any

from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_value
from parking_spot_monitor.operator_feedback_models import FeedbackEvidence, FeedbackLabelSchemaError, optional_feedback_path_text


def validate_timeline_feedback_evidence(
    *,
    data_dir: str | Path,
    frame_path: str | Path | None,
    logger: StructuredLogger | None = None,
) -> FeedbackEvidence:
    """Validate safe metadata for one retained timeline JPEG frame."""

    if frame_path is None:
        return FeedbackEvidence("timeline_frame", None, False, False, None, None, None, "missing")
    root = Path(data_dir).resolve()
    path = Path(frame_path)
    try:
        resolved = path.resolve()
        relative = resolved.relative_to(root).as_posix()
        safe_relative = optional_feedback_path_text(relative)
    except (OSError, ValueError, FeedbackLabelSchemaError):
        return FeedbackEvidence("timeline_frame", None, False, False, None, None, None, "unsafe_path")
    if safe_relative is None or not safe_relative.startswith("timeline/frames/"):
        return FeedbackEvidence("timeline_frame", safe_relative, False, False, None, None, None, "unsafe_path")
    try:
        byte_size = resolved.stat().st_size
        with Image.open(resolved) as image:
            image.verify()
        with Image.open(resolved) as image:
            width, height = image.size
            if image.format != "JPEG" or width <= 0 or height <= 0:
                return FeedbackEvidence("timeline_frame", safe_relative, False, False, None, None, byte_size, "invalid_jpeg")
    except (OSError, UnidentifiedImageError, ValueError) as exc:
        _log(logger, "warning", "operator-feedback-timeline-evidence-invalid", path=safe_relative, error_type=type(exc).__name__)
        return FeedbackEvidence("timeline_frame", safe_relative, False, False, None, None, None, "invalid_jpeg")
    return FeedbackEvidence("timeline_frame", safe_relative, True, True, width, height, byte_size, None)


def validate_feedback_evidence(
    *,
    data_dir: str | Path,
    snapshot_path: str | None,
    snapshots_dir: str | Path | None = None,
    logger: StructuredLogger | None = None,
) -> FeedbackEvidence:
    """Validate safe, local retained alert snapshot metadata without reading image bytes into labels."""

    if not snapshot_path:
        return FeedbackEvidence("alert_snapshot", None, False, False, None, None, None, "missing")
    try:
        safe_relative = optional_feedback_path_text(snapshot_path)
    except FeedbackLabelSchemaError:
        return FeedbackEvidence("alert_snapshot", None, False, False, None, None, None, "unsafe_path")
    if safe_relative is None:
        return FeedbackEvidence("alert_snapshot", None, False, False, None, None, None, "missing")

    candidate = resolve_feedback_evidence_path(data_dir=Path(data_dir), snapshots_dir=snapshots_dir, safe_relative=safe_relative)
    if candidate is None:
        return FeedbackEvidence("alert_snapshot", safe_relative, False, False, None, None, None, "missing")

    try:
        byte_size = candidate.stat().st_size
        with Image.open(candidate) as image:
            image.verify()
        with Image.open(candidate) as image:
            width, height = image.size
            if image.format != "JPEG" or width <= 0 or height <= 0:
                return FeedbackEvidence("alert_snapshot", safe_relative, False, False, None, None, byte_size, "invalid_jpeg")
    except (OSError, UnidentifiedImageError, ValueError) as exc:
        _log(logger, "warning", "operator-feedback-evidence-invalid", path=safe_relative, error_type=type(exc).__name__)
        return FeedbackEvidence("alert_snapshot", safe_relative, False, False, None, None, None, "invalid_jpeg")

    return FeedbackEvidence("alert_snapshot", safe_relative, True, True, width, height, byte_size, None)


def resolve_feedback_evidence_path(*, data_dir: Path, snapshots_dir: str | Path | None, safe_relative: str) -> Path | None:
    """Resolve a safe relative alert snapshot path under accepted runtime evidence roots."""

    relative = Path(safe_relative)
    roots: list[Path] = [data_dir.resolve()]
    if snapshots_dir is not None:
        root = Path(snapshots_dir).resolve()
        roots.append(root)
        roots.append(root.parent)
    else:
        roots.append((data_dir / "snapshots").resolve())

    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            continue
        if candidate.exists():
            return candidate
    return None


def _log(logger: StructuredLogger | None, level: str, event: str, **fields: Any) -> None:
    if logger is None:
        return
    getattr(logger, level)(event, **redact_diagnostic_value(fields))
