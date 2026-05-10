from __future__ import annotations

from dataclasses import dataclass
from os import PathLike


@dataclass(slots=True)
class ConfigError(Exception):
    """Safe configuration/startup error suitable for logs and stderr."""

    message: str
    path: str | PathLike[str] | None = None
    phase: str | None = None
    fields: tuple[str, ...] = ()
    missing_env: tuple[str, ...] = ()

    def __str__(self) -> str:
        parts = [self.message]
        if self.path is not None:
            parts.append(f"path={self.path}")
        if self.phase is not None:
            parts.append(f"phase={self.phase}")
        if self.fields:
            parts.append("fields=" + ",".join(self.fields))
        if self.missing_env:
            parts.append("missing_env=" + ",".join(self.missing_env))
        return "; ".join(parts)
