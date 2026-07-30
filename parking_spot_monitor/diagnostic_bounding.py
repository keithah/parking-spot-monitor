from __future__ import annotations

from collections.abc import Iterable
from itertools import islice
from typing import TypeVar

_T = TypeVar("_T")


def take_bounded(values: Iterable[_T], limit: int) -> tuple[list[_T], bool]:
    items = list(islice(values, limit + 1))
    return items[:limit], len(items) > limit
