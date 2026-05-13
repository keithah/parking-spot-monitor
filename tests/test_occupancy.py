from __future__ import annotations

from dataclasses import replace

import pytest

from parking_spot_monitor.config import OccupancyConfig
from parking_spot_monitor.detection import SpotDetectionCandidate
from parking_spot_monitor.occupancy import (
    OccupancyEventType,
    OccupancyStatus,
    QuietWindowStatus,
    SpotOccupancyState,
    update_occupancy,
)


OBSERVED_AT = "2025-01-01T12:00:00Z"
SNAPSHOT_PATH = "/data/latest.jpg"


@pytest.fixture
def occupancy_config() -> OccupancyConfig:
    return OccupancyConfig(iou_threshold=0.7, confirm_frames=2, release_frames=2)


@pytest.fixture
def quiet_inactive() -> QuietWindowStatus:
    return QuietWindowStatus(active=False)


def candidate(
    spot_id: str,
    bbox: tuple[float, float, float, float] = (10.0, 10.0, 90.0, 90.0),
    *,
    confidence: float = 0.91,
    source_timestamp: str = "2025-01-01T11:59:59Z",
) -> SpotDetectionCandidate:
    return SpotDetectionCandidate(
        spot_id=spot_id,
        class_name="car",
        confidence=confidence,
        bbox=bbox,
        bbox_area_px=(bbox[2] - bbox[0]) * (bbox[3] - bbox[1]),
        centroid=((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2),
        overlap_ratio=1.0,
        source_frame_path=SNAPSHOT_PATH,
        source_timestamp=source_timestamp,
    )


def test_stable_hits_confirm_occupied_transition(occupancy_config: OccupancyConfig, quiet_inactive: QuietWindowStatus) -> None:
    first = update_occupancy(
        previous_state={},
        candidates_by_spot={"left_spot": candidate("left_spot")},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    assert first.state_by_spot["left_spot"].status is OccupancyStatus.UNKNOWN
    assert first.state_by_spot["left_spot"].hit_streak == 1
    assert first.events == []

    second = update_occupancy(
        previous_state=first.state_by_spot,
        candidates_by_spot={"left_spot": candidate("left_spot")},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert second.state_by_spot["left_spot"].status is OccupancyStatus.OCCUPIED
    assert second.state_by_spot["left_spot"].hit_streak == 2
    assert [event.event_type for event in second.events] == [OccupancyEventType.STATE_CHANGED]
    assert second.events[0].spot_id == "left_spot"
    assert second.events[0].previous_status is OccupancyStatus.UNKNOWN
    assert second.events[0].new_status is OccupancyStatus.OCCUPIED
    assert second.events[0].observed_at == OBSERVED_AT
    assert second.events[0].source_timestamp == "2025-01-01T11:59:59Z"
    assert second.events[0].snapshot_path == SNAPSHOT_PATH
    assert second.events[0].candidate_summary == {
        "class_name": "car",
        "confidence": 0.91,
        "bbox": (10.0, 10.0, 90.0, 90.0),
        "source_frame_path": SNAPSHOT_PATH,
    }


def test_sustained_misses_release_to_empty_and_emit_single_open_event(
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    occupied_state = {
        "left_spot": SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            hit_streak=2,
            miss_streak=0,
            last_bbox=(10.0, 10.0, 90.0, 90.0),
            open_event_emitted=False,
        )
    }

    first_miss = update_occupancy(
        previous_state=occupied_state,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    assert first_miss.state_by_spot["left_spot"].status is OccupancyStatus.OCCUPIED
    assert first_miss.state_by_spot["left_spot"].miss_streak == 1
    assert first_miss.events == []

    released = update_occupancy(
        previous_state=first_miss.state_by_spot,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert released.state_by_spot["left_spot"].status is OccupancyStatus.EMPTY
    assert released.state_by_spot["left_spot"].open_event_emitted is True
    assert [event.event_type for event in released.events] == [
        OccupancyEventType.STATE_CHANGED,
        OccupancyEventType.OPEN_EVENT,
    ]
    assert released.events[1].previous_status is OccupancyStatus.OCCUPIED
    assert released.events[1].new_status is OccupancyStatus.EMPTY
    assert released.events[1].suppressed_reason is None

    duplicate = update_occupancy(
        previous_state=released.state_by_spot,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    assert duplicate.state_by_spot["left_spot"].status is OccupancyStatus.EMPTY
    assert duplicate.events == []



def test_small_remaining_vehicle_presence_suppresses_open_event(
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    occupied_state = {
        "left_spot": SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            hit_streak=2,
            miss_streak=0,
            last_bbox=(10.0, 10.0, 90.0, 90.0),
            open_event_emitted=False,
        )
    }

    first_small_car = update_occupancy(
        previous_state=occupied_state,
        candidates_by_spot={"left_spot": None},
        presence_by_spot={"left_spot": True},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    second_small_car = update_occupancy(
        previous_state=first_small_car.state_by_spot,
        candidates_by_spot={"left_spot": None},
        presence_by_spot={"left_spot": True},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert second_small_car.state_by_spot["left_spot"].status is OccupancyStatus.OCCUPIED
    assert second_small_car.state_by_spot["left_spot"].miss_streak == 0
    assert second_small_car.events == []

def test_unknown_to_empty_never_emits_open_event(occupancy_config: OccupancyConfig, quiet_inactive: QuietWindowStatus) -> None:
    first = update_occupancy(
        previous_state={},
        candidates_by_spot={"left_spot": None},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    second = update_occupancy(
        previous_state=first.state_by_spot,
        candidates_by_spot={"left_spot": None},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert second.state_by_spot["left_spot"].status is OccupancyStatus.EMPTY
    assert second.state_by_spot["left_spot"].miss_streak == 2
    assert second.state_by_spot["left_spot"].open_event_emitted is False
    assert [event.event_type for event in second.events] == [OccupancyEventType.STATE_CHANGED]
    assert second.events[0].previous_status is OccupancyStatus.UNKNOWN
    assert second.events[0].new_status is OccupancyStatus.EMPTY


def test_flicker_resets_confirmation_streak(occupancy_config: OccupancyConfig, quiet_inactive: QuietWindowStatus) -> None:
    first = update_occupancy(
        previous_state={},
        candidates_by_spot={"left_spot": candidate("left_spot")},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    miss = update_occupancy(
        previous_state=first.state_by_spot,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    hit_again = update_occupancy(
        previous_state=miss.state_by_spot,
        candidates_by_spot={"left_spot": candidate("left_spot")},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert miss.state_by_spot["left_spot"].hit_streak == 0
    assert hit_again.state_by_spot["left_spot"].hit_streak == 1
    assert hit_again.state_by_spot["left_spot"].status is OccupancyStatus.UNKNOWN
    assert hit_again.events == []


def test_moving_bbox_below_iou_threshold_resets_hit_streak(
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    first = update_occupancy(
        previous_state={},
        candidates_by_spot={"left_spot": candidate("left_spot", (10, 10, 90, 90))},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    moved = update_occupancy(
        previous_state=first.state_by_spot,
        candidates_by_spot={"left_spot": candidate("left_spot", (100, 100, 180, 180))},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert moved.state_by_spot["left_spot"].hit_streak == 1
    assert moved.state_by_spot["left_spot"].status is OccupancyStatus.UNKNOWN
    assert moved.events == []


def test_reoccupation_resets_duplicate_open_suppression(
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    empty_state = {
        "left_spot": SpotOccupancyState(
            status=OccupancyStatus.EMPTY,
            hit_streak=0,
            miss_streak=2,
            last_bbox=(10, 10, 90, 90),
            open_event_emitted=True,
        )
    }

    first_hit = update_occupancy(
        previous_state=empty_state,
        candidates_by_spot={"left_spot": candidate("left_spot")},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    reoccupied = update_occupancy(
        previous_state=first_hit.state_by_spot,
        candidates_by_spot={"left_spot": candidate("left_spot")},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert reoccupied.state_by_spot["left_spot"].status is OccupancyStatus.OCCUPIED
    assert reoccupied.state_by_spot["left_spot"].open_event_emitted is False

    miss_one = update_occupancy(
        previous_state=reoccupied.state_by_spot,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )
    empty_again = update_occupancy(
        previous_state=miss_one.state_by_spot,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert [event.event_type for event in empty_again.events] == [
        OccupancyEventType.STATE_CHANGED,
        OccupancyEventType.OPEN_EVENT,
    ]


def test_quiet_window_marks_open_event_suppressed(occupancy_config: OccupancyConfig) -> None:
    quiet_active = QuietWindowStatus(active=True, window_id="tuesday_sweeping")
    occupied_state = {
        "left_spot": SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            hit_streak=2,
            miss_streak=1,
            last_bbox=(10, 10, 90, 90),
            open_event_emitted=False,
        )
    }

    released = update_occupancy(
        previous_state=occupied_state,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_active,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert released.state_by_spot["left_spot"].status is OccupancyStatus.EMPTY
    assert [event.event_type for event in released.events] == [
        OccupancyEventType.STATE_CHANGED,
        OccupancyEventType.OPEN_SUPPRESSED,
    ]
    assert released.events[1].suppressed_reason == "quiet_window:tuesday_sweeping"


def test_per_spot_transitions_are_independent(occupancy_config: OccupancyConfig, quiet_inactive: QuietWindowStatus) -> None:
    previous = {
        "left_spot": SpotOccupancyState(
            status=OccupancyStatus.UNKNOWN,
            hit_streak=1,
            miss_streak=0,
            last_bbox=(10, 10, 90, 90),
        ),
        "right_spot": SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            hit_streak=2,
            miss_streak=0,
            last_bbox=(210, 10, 290, 90),
        ),
    }

    updated = update_occupancy(
        previous_state=previous,
        candidates_by_spot={"left_spot": candidate("left_spot"), "right_spot": None},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert updated.state_by_spot["left_spot"].status is OccupancyStatus.OCCUPIED
    assert updated.state_by_spot["right_spot"].status is OccupancyStatus.OCCUPIED
    assert updated.state_by_spot["right_spot"].miss_streak == 1
    assert [event.spot_id for event in updated.events] == ["left_spot"]
    assert previous["left_spot"].status is OccupancyStatus.UNKNOWN
    assert previous["right_spot"].miss_streak == 0


def test_malformed_candidate_is_ignored_without_crashing_runtime_update(
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    malformed = replace(candidate("left_spot"), bbox=(10.0, 10.0, 5.0, 90.0))

    updated = update_occupancy(
        previous_state={},
        candidates_by_spot={"left_spot": malformed},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert updated.state_by_spot["left_spot"].status is OccupancyStatus.UNKNOWN
    assert updated.state_by_spot["left_spot"].hit_streak == 0
    assert updated.events == []


def test_empty_candidate_map_initializes_configured_spots_without_events(
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    updated = update_occupancy(
        previous_state={},
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
        configured_spot_ids=("left_spot", "right_spot"),
    )

    assert updated.state_by_spot == {
        "left_spot": SpotOccupancyState(miss_streak=1),
        "right_spot": SpotOccupancyState(miss_streak=1),
    }
    assert updated.events == []


def test_events_are_serializable_dicts(occupancy_config: OccupancyConfig, quiet_inactive: QuietWindowStatus) -> None:
    previous = {
        "left_spot": SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            hit_streak=2,
            miss_streak=1,
            last_bbox=(10, 10, 90, 90),
        )
    }

    updated = update_occupancy(
        previous_state=previous,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
    )

    assert updated.events[0].to_dict() == {
        "event_type": "occupancy-state-changed",
        "spot_id": "left_spot",
        "previous_status": "occupied",
        "new_status": "empty",
        "observed_at": OBSERVED_AT,
        "source_timestamp": None,
        "snapshot_path": SNAPSHOT_PATH,
        "candidate": None,
        "suppressed_reason": None,
    }
