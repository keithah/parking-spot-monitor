from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypeVar

from PIL import Image, UnidentifiedImageError

VerificationErrorT = TypeVar("VerificationErrorT", bound=Exception)


def jpeg_check(path: Path) -> dict[str, Any]:
    exists = path.is_file()
    byte_size = path.stat().st_size if exists else 0
    check: dict[str, Any] = {
        "path": str(path),
        "exists": exists,
        "byte_size": byte_size,
        "valid_jpeg": False,
    }
    if not exists:
        check["error_type"] = "missing"
        return check
    try:
        with Image.open(path) as image:
            check["format"] = image.format
            check["width"], check["height"] = image.size
            image.verify()
        check["valid_jpeg"] = check.get("format") == "JPEG"
    except (OSError, UnidentifiedImageError) as exc:
        check["error_type"] = type(exc).__name__
    return check


def load_result_json(path: Path, error_type: type[VerificationErrorT]) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise error_type("result JSON is missing") from exc
    except OSError as exc:
        raise error_type("result JSON cannot be read") from exc
    except json.JSONDecodeError as exc:
        raise error_type("result JSON is malformed") from exc
    if not isinstance(raw, dict):
        raise error_type("result JSON must be an object")
    return raw
