from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, UnidentifiedImageError

from parking_spot_monitor.config import RuntimeSettings, SpotConfig
from parking_spot_monitor.logging import StructuredLogger


@dataclass(frozen=True)
class DebugOverlayResult:
    source_path: str
    output_path: str
    width: int
    height: int
    spot_ids: tuple[str, ...]

    def diagnostics(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["spot_ids"] = list(self.spot_ids)
        return payload


class DebugOverlayError(Exception):
    """Raised when a debug overlay cannot be written with safe diagnostics."""

    def __init__(self, message: str, *, diagnostics: dict[str, Any]) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics


def write_debug_overlay(
    settings: RuntimeSettings,
    source_path: str | Path,
    output_path: str | Path,
    logger: StructuredLogger | None = None,
) -> DebugOverlayResult:
    """Write a local tuning overlay JPEG without modifying the raw source frame."""
    source = Path(source_path)
    output = Path(output_path)
    spot_items = _configured_spots(settings)
    spot_ids = tuple(spot_id for spot_id, _spot in spot_items)
    base_diagnostics: dict[str, Any] = {
        "source_path": str(source),
        "output_path": str(output),
        "spot_ids": list(spot_ids),
    }

    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        with Image.open(source) as opened:
            image = opened.convert("RGB")
        width, height = image.size
        _draw_spot_overlay(image, spot_items)
        image.save(output, format="JPEG", quality=90)
    except FileNotFoundError as exc:
        _raise_overlay_error(
            "debug overlay source frame is missing",
            exc,
            base_diagnostics,
            logger=logger,
        )
    except UnidentifiedImageError as exc:
        _raise_overlay_error(
            "debug overlay source frame could not be decoded",
            exc,
            base_diagnostics,
            logger=logger,
        )
    except OSError as exc:
        _raise_overlay_error(
            "debug overlay could not be written",
            exc,
            base_diagnostics,
            logger=logger,
            width=locals().get("width"),
            height=locals().get("height"),
        )

    result = DebugOverlayResult(
        source_path=str(source),
        output_path=str(output),
        width=width,
        height=height,
        spot_ids=spot_ids,
    )
    if logger is not None:
        logger.info("debug-overlay-written", **result.diagnostics())
    return result


def _configured_spots(settings: RuntimeSettings) -> tuple[tuple[str, SpotConfig], ...]:
    return (
        ("left_spot", settings.spots.left_spot),
        ("right_spot", settings.spots.right_spot),
    )


def _draw_spot_overlay(image: Image.Image, spot_items: tuple[tuple[str, SpotConfig], ...]) -> None:
    canvas = image.convert("RGBA")
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    styles = {
        "left_spot": {"fill": (56, 189, 248, 72), "outline": (14, 165, 233, 255)},
        "right_spot": {"fill": (34, 197, 94, 72), "outline": (22, 163, 74, 255)},
    }
    fallback = {"fill": (250, 204, 21, 72), "outline": (202, 138, 4, 255)}

    for spot_id, spot in spot_items:
        points = [(point.x, point.y) for point in spot.polygon]
        style = styles.get(spot_id, fallback)
        draw.polygon(points, fill=style["fill"], outline=style["outline"])
        draw.line(points + [points[0]], fill=style["outline"], width=5, joint="curve")
        label_x, label_y = points[0]
        draw.text((label_x + 6, label_y + 6), spot_id, fill=style["outline"])

    composed = Image.alpha_composite(canvas, overlay).convert("RGB")
    image.paste(composed)


def _raise_overlay_error(
    message: str,
    exc: BaseException,
    base_diagnostics: dict[str, Any],
    *,
    logger: StructuredLogger | None,
    width: int | None = None,
    height: int | None = None,
) -> None:
    diagnostics = {
        **base_diagnostics,
        "width": width,
        "height": height,
        "error_type": type(exc).__name__,
        "error_message": message,
    }
    if logger is not None:
        logger.error("debug-overlay-failed", **diagnostics)
    raise DebugOverlayError(message, diagnostics=diagnostics) from exc
