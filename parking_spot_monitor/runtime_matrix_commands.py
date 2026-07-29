from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any


@dataclass(frozen=True, slots=True)
class MatrixCommandSchedule:
    command_poll_interval_seconds: float
    command_failure_cooldown_seconds: float
    command_failure_max_cooldown_seconds: float

    def __post_init__(self) -> None:
        values = (
            self.command_poll_interval_seconds,
            self.command_failure_cooldown_seconds,
            self.command_failure_max_cooldown_seconds,
        )
        if not all(isfinite(value) for value in values):
            raise ValueError("Matrix command schedule timings must be finite")
        if self.command_poll_interval_seconds < 0:
            raise ValueError("Matrix command poll interval must not be negative")
        if self.command_failure_cooldown_seconds <= 0:
            raise ValueError("Matrix command failure cooldown must be positive")
        if (
            self.command_failure_max_cooldown_seconds
            < self.command_failure_cooldown_seconds
        ):
            raise ValueError(
                "Matrix command maximum failure cooldown must cover the initial cooldown"
            )


@dataclass(frozen=True, slots=True)
class MatrixCommandPollState:
    last_attempt_at: float | None = None
    failure_count: int = 0
    retry_at: float | None = None


@dataclass(frozen=True, slots=True)
class RuntimeMatrixCommandPollOutcome:
    transport_failed: bool = False
    health_error: dict[str, Any] | None = None


def command_poll_due(
    settings: MatrixCommandSchedule,
    state: MatrixCommandPollState,
    now_monotonic: float,
) -> bool:
    if state.retry_at is not None:
        return now_monotonic >= state.retry_at
    if settings.command_poll_interval_seconds == 0 or state.last_attempt_at is None:
        return True
    return (
        now_monotonic - state.last_attempt_at
        >= settings.command_poll_interval_seconds
    )


def record_command_poll_result(
    settings: MatrixCommandSchedule,
    state: MatrixCommandPollState,
    now_monotonic: float,
    *,
    failed: bool,
) -> MatrixCommandPollState:
    if not failed:
        return MatrixCommandPollState(last_attempt_at=now_monotonic)
    cooldown = settings.command_failure_cooldown_seconds
    remaining_doublings = state.failure_count
    while (
        remaining_doublings > 0
        and cooldown < settings.command_failure_max_cooldown_seconds
    ):
        cooldown = min(
            cooldown * 2,
            settings.command_failure_max_cooldown_seconds,
        )
        remaining_doublings -= 1
    return MatrixCommandPollState(
        last_attempt_at=now_monotonic,
        failure_count=state.failure_count + 1,
        retry_at=now_monotonic + cooldown,
    )
