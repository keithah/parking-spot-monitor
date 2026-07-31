from __future__ import annotations

from datetime import UTC, datetime

import pytest

from parking_spot_monitor.occupancy import OccupancyStatus
from parking_spot_monitor.runtime_transition_latency import TransitionEvidenceTracker


def test_confirmed_departure_partitions_observation_and_capture_latency() -> None:
    tracker = TransitionEvidenceTracker()
    tracker.observe(
        spot_id="right_spot",
        observed_at=datetime(2026, 7, 31, 5, 0, 0, tzinfo=UTC),
        evidence_status=OccupancyStatus.OCCUPIED,
    )

    fields = tracker.confirmed_transition_fields(
        spot_id="right_spot",
        previous_status=OccupancyStatus.OCCUPIED,
        new_status=OccupancyStatus.EMPTY,
        confirmed_at=datetime(2026, 7, 31, 5, 0, 16, tzinfo=UTC),
        primary_capture_seconds=0.4,
        verification_capture_seconds=1.2,
        cadence_seconds=8.0,
        cadence_reason="occupied",
    )

    assert fields == {
        "spot_id": "right_spot",
        "transition_direction": "occupied-to-empty",
        "opposite_evidence_to_confirmation_seconds": 16.0,
        "primary_capture_seconds": 0.4,
        "verification_capture_seconds": 1.2,
        "cadence_seconds": 8.0,
        "cadence_reason": "occupied",
    }


def test_confirmed_arrival_partitions_observation_and_capture_latency() -> None:
    tracker = TransitionEvidenceTracker()
    tracker.observe(
        spot_id="left_spot",
        observed_at=datetime(2026, 7, 31, 5, 0, 8, tzinfo=UTC),
        evidence_status=OccupancyStatus.EMPTY,
    )

    fields = tracker.confirmed_transition_fields(
        spot_id="left_spot",
        previous_status=OccupancyStatus.EMPTY,
        new_status=OccupancyStatus.OCCUPIED,
        confirmed_at=datetime(2026, 7, 31, 5, 0, 24, tzinfo=UTC),
        primary_capture_seconds=0.333333333,
        verification_capture_seconds=None,
        cadence_seconds=8.0000004,
        cadence_reason="partial-streak",
    )

    assert fields == {
        "spot_id": "left_spot",
        "transition_direction": "empty-to-occupied",
        "opposite_evidence_to_confirmation_seconds": 16.0,
        "primary_capture_seconds": 0.333333,
        "cadence_seconds": 8.0,
        "cadence_reason": "partial-streak",
    }


def test_transition_without_prior_matching_evidence_returns_none() -> None:
    tracker = TransitionEvidenceTracker()

    assert (
        tracker.confirmed_transition_fields(
            spot_id="right_spot",
            previous_status=OccupancyStatus.OCCUPIED,
            new_status=OccupancyStatus.EMPTY,
            confirmed_at=datetime(2026, 7, 31, 5, 0, 16, tzinfo=UTC),
            primary_capture_seconds=0.4,
            verification_capture_seconds=1.2,
            cadence_seconds=8.0,
            cadence_reason="occupied",
        )
        is None
    )


@pytest.mark.parametrize(
    ("observed_at", "confirmed_at"),
    [
        (
            datetime(2026, 7, 31, 5, 0, 16, tzinfo=UTC),
            datetime(2026, 7, 31, 5, 0, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 7, 31, 5, 0, 0, tzinfo=UTC),
            datetime(2026, 7, 31, 5, 0, 16),
        ),
    ],
)
def test_backward_or_naive_confirmation_returns_none(
    observed_at: datetime,
    confirmed_at: datetime,
) -> None:
    tracker = TransitionEvidenceTracker()
    tracker.observe(
        spot_id="right_spot",
        observed_at=observed_at,
        evidence_status=OccupancyStatus.OCCUPIED,
    )

    assert (
        tracker.confirmed_transition_fields(
            spot_id="right_spot",
            previous_status=OccupancyStatus.OCCUPIED,
            new_status=OccupancyStatus.EMPTY,
            confirmed_at=confirmed_at,
            primary_capture_seconds=0.4,
            verification_capture_seconds=1.2,
            cadence_seconds=8.0,
            cadence_reason="occupied",
        )
        is None
    )


def test_naive_observation_is_not_retained() -> None:
    tracker = TransitionEvidenceTracker()
    tracker.observe(
        spot_id="right_spot",
        observed_at=datetime(2026, 7, 31, 5, 0, 0),
        evidence_status=OccupancyStatus.OCCUPIED,
    )

    assert (
        tracker.confirmed_transition_fields(
            spot_id="right_spot",
            previous_status=OccupancyStatus.OCCUPIED,
            new_status=OccupancyStatus.EMPTY,
            confirmed_at=datetime(2026, 7, 31, 5, 0, 16, tzinfo=UTC),
            primary_capture_seconds=0.4,
            verification_capture_seconds=None,
            cadence_seconds=8.0,
            cadence_reason="occupied",
        )
        is None
    )


def test_same_state_confirmation_returns_none() -> None:
    tracker = TransitionEvidenceTracker()
    tracker.observe(
        spot_id="right_spot",
        observed_at=datetime(2026, 7, 31, 5, 0, 0, tzinfo=UTC),
        evidence_status=OccupancyStatus.OCCUPIED,
    )

    assert (
        tracker.confirmed_transition_fields(
            spot_id="right_spot",
            previous_status=OccupancyStatus.OCCUPIED,
            new_status=OccupancyStatus.OCCUPIED,
            confirmed_at=datetime(2026, 7, 31, 5, 0, 16, tzinfo=UTC),
            primary_capture_seconds=0.4,
            verification_capture_seconds=None,
            cadence_seconds=8.0,
            cadence_reason="occupied",
        )
        is None
    )


def test_non_finite_or_negative_durations_are_bounded_without_throwing() -> None:
    tracker = TransitionEvidenceTracker()
    tracker.observe(
        spot_id="right_spot",
        observed_at=datetime(2026, 7, 31, 5, 0, 0, tzinfo=UTC),
        evidence_status=OccupancyStatus.OCCUPIED,
    )

    fields = tracker.confirmed_transition_fields(
        spot_id="right_spot",
        previous_status=OccupancyStatus.OCCUPIED,
        new_status=OccupancyStatus.EMPTY,
        confirmed_at=datetime(2026, 7, 31, 5, 0, 16, tzinfo=UTC),
        primary_capture_seconds=float("nan"),
        verification_capture_seconds=float("inf"),
        cadence_seconds=-8.0,
        cadence_reason="occupied",
    )

    assert fields == {
        "spot_id": "right_spot",
        "transition_direction": "occupied-to-empty",
        "opposite_evidence_to_confirmation_seconds": 16.0,
        "primary_capture_seconds": 0.0,
        "verification_capture_seconds": 0.0,
        "cadence_seconds": 0.0,
        "cadence_reason": "occupied",
    }
