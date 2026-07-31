"""Validation helpers for decision-memory checkpoint configuration."""

from __future__ import annotations

import math


def positive_finite_float(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be a positive finite number")
    resolved = float(value)
    if not math.isfinite(resolved) or resolved <= 0:
        raise ValueError(f"{field_name} must be a positive finite number")
    return resolved


def positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value
