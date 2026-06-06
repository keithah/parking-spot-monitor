from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypeVar

VerificationErrorT = TypeVar("VerificationErrorT", bound=Exception)


def load_result_json(path: Path, error_type: type[VerificationErrorT]) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise error_type("result JSON is missing") from exc
    except json.JSONDecodeError as exc:
        raise error_type("result JSON is malformed") from exc
    if not isinstance(raw, dict):
        raise error_type("result JSON must be an object")
    return raw
