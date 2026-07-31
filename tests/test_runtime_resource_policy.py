from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from parking_spot_monitor.config import load_settings
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
from parking_spot_monitor.runtime_loop_resources import paced_sleep_seconds
from parking_spot_monitor.runtime_resource_policy import (
    RuntimeResourceDecision,
    RuntimeResourcePolicyState,
    artifact_due,
    decide_runtime_interval,
    remaining_sleep_seconds,
    verification_due,
)
from parking_spot_monitor.state import RuntimeState


@pytest.fixture
def settings():
    loaded = load_settings(
        Path("config.yaml.example"),
        environ={
            "RTSP_URL": "rtsp://primary.example/camera",
            "RTSP_URL_4K": "rtsp://high-resolution.example/camera",
            "MATRIX_ACCESS_TOKEN": "test-token",
        },
    )
    return loaded.model_copy(
        update={
            "runtime": loaded.runtime.model_copy(
                update={
                    "frame_interval_seconds": 30.0,
                    "occupied_frame_interval_seconds": 8.0,
                    "stable_frame_interval_seconds": 60.0,
                }
            )
        }
    )


def runtime_state(*states: SpotOccupancyState) -> RuntimeState:
    return RuntimeState(
        state_by_spot={f"spot-{index}": state for index, state in enumerate(states)}
    )


def decide(settings, state: RuntimeState, **overrides) -> RuntimeResourceDecision:
    arguments = {
        "previous_stable_success_count": 2,
        "frame_had_transition": False,
        "frame_has_weak_presence": False,
        "degraded": False,
    }
    arguments.update(overrides)
    return decide_runtime_interval(settings, state, **arguments)


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        (SpotOccupancyState(), "unknown"),
        (
            SpotOccupancyState(
                status=OccupancyStatus.EMPTY,
                hit_streak=1,
            ),
            "partial-streak",
        ),
    ],
)
def test_uncertain_occupancy_uses_active_cadence_and_resets_settling(
    settings, state: SpotOccupancyState, reason: str
) -> None:
    decision = decide(settings, runtime_state(state))

    assert decision == RuntimeResourceDecision(
        interval_seconds=30.0,
        reason=reason,
        stable_success_count=0,
    )


def test_confirmed_occupied_spot_uses_occupied_cadence(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=3)
    )

    assert decide(settings, state) == RuntimeResourceDecision(
        interval_seconds=8.0,
        reason="occupied",
        stable_success_count=0,
    )


def test_partial_release_streak_stays_on_occupied_cadence(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            miss_streak=1,
        )
    )

    assert decide(settings, state) == RuntimeResourceDecision(8.0, "occupied", 0)


def test_all_confirmed_empty_spots_reach_stable_cadence(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(status=OccupancyStatus.EMPTY, miss_streak=3)
    )

    assert decide(settings, state) == RuntimeResourceDecision(60.0, "stable", 3)


@pytest.mark.parametrize(
    ("state", "expected_reason"),
    [
        (
            SpotOccupancyState(status=OccupancyStatus.EMPTY, hit_streak=3),
            "partial-streak",
        ),
        (
            SpotOccupancyState(status=OccupancyStatus.EMPTY, hit_streak=4),
            "stable",
        ),
        (
            SpotOccupancyState(status=OccupancyStatus.EMPTY, miss_streak=4),
            "partial-streak",
        ),
        (
            SpotOccupancyState(status=OccupancyStatus.EMPTY, miss_streak=5),
            "stable",
        ),
    ],
)
def test_configured_confirmation_and_release_thresholds_define_partial_streaks(
    settings, state: SpotOccupancyState, expected_reason: str
) -> None:
    configured = settings.model_copy(
        update={
            "occupancy": settings.occupancy.model_copy(
                update={"confirm_frames": 4, "release_frames": 5}
            )
        }
    )

    assert decide(configured, runtime_state(state)).reason == expected_reason


def test_all_spots_must_be_confirmed_before_stable_cadence(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=3),
        SpotOccupancyState(status=OccupancyStatus.UNKNOWN),
    )

    assert decide(settings, state).reason == "unknown"


def test_stable_observation_remains_active_while_settling(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(status=OccupancyStatus.EMPTY, miss_streak=3)
    )

    decision = decide(settings, state, previous_stable_success_count=0)

    assert decision == RuntimeResourceDecision(30.0, "settling", 1)


def test_stable_success_count_continues_after_settling(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(status=OccupancyStatus.EMPTY, miss_streak=4)
    )

    decision = decide(settings, state, previous_stable_success_count=3)

    assert decision == RuntimeResourceDecision(60.0, "stable", 4)


@pytest.mark.parametrize(
    ("state", "overrides", "reason"),
    [
        (
            SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=3),
            {"degraded": True},
            "degraded",
        ),
        (
            SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=3),
            {"frame_had_transition": True},
            "transition-settle",
        ),
        (
            SpotOccupancyState(status=OccupancyStatus.EMPTY, miss_streak=3),
            {"frame_has_weak_presence": True},
            "weak-presence",
        ),
    ],
)
def test_non_stable_frame_conditions_reset_settling(
    settings,
    state: SpotOccupancyState,
    overrides: dict[str, bool],
    reason: str,
) -> None:
    decision = decide(settings, runtime_state(state), **overrides)

    assert decision == RuntimeResourceDecision(30.0, reason, 0)


def test_disabled_adaptation_always_uses_active_cadence(settings) -> None:
    disabled = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={"adaptive_polling_enabled": False}
            )
        }
    )

    decision = decide(
        disabled,
        runtime_state(SpotOccupancyState()),
        degraded=True,
        frame_had_transition=True,
        frame_has_weak_presence=True,
    )

    assert decision == RuntimeResourceDecision(30.0, "adaptive-disabled", 0)


def test_disabled_adaptation_tracks_stable_successes_for_verification(settings) -> None:
    disabled = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={"adaptive_polling_enabled": False}
            )
        }
    )
    state = runtime_state(
        SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=3)
    )

    decision = decide(
        disabled,
        state,
        previous_stable_success_count=2,
    )

    assert decision == RuntimeResourceDecision(30.0, "adaptive-disabled", 3)


def test_adaptive_occupied_cadence_subtracts_elapsed_work_when_active_and_stable_intervals_match(
    settings,
) -> None:
    equal_intervals = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(
                update={"stable_frame_interval_seconds": 30.0}
            )
        }
    )

    assert (
        paced_sleep_seconds(
            equal_intervals,
            RuntimeResourceDecision(8.0, "occupied", 0),
            iteration_started_at=100.0,
            now_monotonic=102.0,
        )
        == 6.0
    )


def test_resource_policy_state_defaults_and_policy_values_are_immutable() -> None:
    state = RuntimeResourcePolicyState()
    decision = RuntimeResourceDecision(60.0, "stable", 3)

    assert state == RuntimeResourcePolicyState(
        stable_success_count=0,
        last_verification_at=None,
        last_overlay_at=None,
    )
    with pytest.raises(FrozenInstanceError):
        state.stable_success_count = 1  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        decision.reason = "unknown"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("now", "last", "interval", "expected"),
    [
        (100.0, None, 600.0, True),
        (699.999, 100.0, 600.0, False),
        (700.0, 100.0, 600.0, True),
        (1000.0, None, 0.0, False),
        (1000.0, 100.0, 0.0, False),
    ],
)
def test_periodic_verification_uses_a_monotonic_deadline(
    now: float, last: float | None, interval: float, expected: bool
) -> None:
    assert (
        verification_due(
            now_monotonic=now,
            last_verification_at=last,
            interval_seconds=interval,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("now", "last", "interval", "transition", "expected"),
    [
        (100.0, None, 60.0, False, True),
        (159.999, 100.0, 60.0, False, False),
        (160.0, 100.0, 60.0, False, True),
        (101.0, 100.0, 60.0, True, True),
        (101.0, 100.0, 0.0, True, True),
        (101.0, 100.0, 0.0, False, False),
    ],
)
def test_artifact_deadline_preserves_transition_writes(
    now: float,
    last: float | None,
    interval: float,
    transition: bool,
    expected: bool,
) -> None:
    assert (
        artifact_due(
            now_monotonic=now,
            last_written_at=last,
            interval_seconds=interval,
            transition=transition,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("interval", "started", "now", "expected"),
    [
        (60.0, 100.0, 107.5, 52.5),
        (15.0, 100.0, 118.0, 0.0),
        (60.0, 100.0, 99.0, 60.0),
    ],
)
def test_remaining_sleep_subtracts_work_without_returning_negative_or_overlong_values(
    interval: float, started: float, now: float, expected: float
) -> None:
    assert (
        remaining_sleep_seconds(
            interval_seconds=interval,
            iteration_started_at=started,
            now_monotonic=now,
        )
        == expected
    )
