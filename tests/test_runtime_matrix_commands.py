from __future__ import annotations

import math

import pytest

from parking_spot_monitor.runtime_matrix_commands import (
    MatrixCommandPollState,
    MatrixCommandSchedule,
    command_poll_due,
    record_command_poll_result,
)


def test_successful_poll_waits_for_configured_interval() -> None:
    config = MatrixCommandSchedule(
        command_poll_interval_seconds=60,
        command_failure_cooldown_seconds=60,
        command_failure_max_cooldown_seconds=900,
    )
    state = MatrixCommandPollState(last_attempt_at=100.0)

    assert command_poll_due(config, state, 159.9) is False
    assert command_poll_due(config, state, 160.0) is True


def test_failed_half_open_probes_double_cooldown_only_to_configured_maximum() -> None:
    config = MatrixCommandSchedule(60, 60, 200)

    first = record_command_poll_result(
        config, MatrixCommandPollState(), 100.0, failed=True
    )
    second = record_command_poll_result(config, first, 160.0, failed=True)
    third = record_command_poll_result(config, second, 280.0, failed=True)
    fourth = record_command_poll_result(config, third, 480.0, failed=True)

    assert first == MatrixCommandPollState(
        last_attempt_at=100.0, failure_count=1, retry_at=160.0
    )
    assert second.retry_at == 280.0
    assert third.retry_at == 480.0
    assert fourth.retry_at == 680.0


def test_successful_half_open_probe_resets_failure_state() -> None:
    config = MatrixCommandSchedule(60, 60, 900)
    failed = MatrixCommandPollState(
        last_attempt_at=160.0, failure_count=2, retry_at=280.0
    )

    recovered = record_command_poll_result(config, failed, 280.0, failed=False)

    assert recovered == MatrixCommandPollState(
        last_attempt_at=280.0, failure_count=0, retry_at=None
    )
    assert command_poll_due(config, recovered, 339.9) is False
    assert command_poll_due(config, recovered, 340.0) is True


def test_prolonged_failure_history_caps_without_exponential_overflow() -> None:
    config = MatrixCommandSchedule(60.0, 60.0, 900.0)
    prolonged_outage = MatrixCommandPollState(
        last_attempt_at=100.0,
        failure_count=10_000,
        retry_at=1000.0,
    )

    failed = record_command_poll_result(
        config, prolonged_outage, 1000.0, failed=True
    )

    assert failed == MatrixCommandPollState(
        last_attempt_at=1000.0,
        failure_count=10_001,
        retry_at=1900.0,
    )


def test_zero_poll_interval_is_due_each_iteration() -> None:
    config = MatrixCommandSchedule(0, 60, 900)

    assert command_poll_due(
        config, MatrixCommandPollState(last_attempt_at=1.0), 1.0
    ) is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("command_poll_interval_seconds", math.nan),
        ("command_poll_interval_seconds", math.inf),
        ("command_failure_cooldown_seconds", math.nan),
        ("command_failure_cooldown_seconds", math.inf),
        ("command_failure_max_cooldown_seconds", math.nan),
        ("command_failure_max_cooldown_seconds", math.inf),
    ],
)
def test_schedule_rejects_nonfinite_timings(field: str, value: float) -> None:
    values = {
        "command_poll_interval_seconds": 60.0,
        "command_failure_cooldown_seconds": 60.0,
        "command_failure_max_cooldown_seconds": 900.0,
    }
    values[field] = value

    with pytest.raises(ValueError):
        MatrixCommandSchedule(**values)


@pytest.mark.parametrize(
    "values",
    [
        (-1.0, 60.0, 900.0),
        (60.0, 0.0, 900.0),
        (60.0, 60.0, 59.0),
    ],
)
def test_schedule_rejects_invalid_timing_bounds(
    values: tuple[float, float, float],
) -> None:
    with pytest.raises(ValueError):
        MatrixCommandSchedule(*values)
