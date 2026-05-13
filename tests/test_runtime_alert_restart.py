from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image

from parking_spot_monitor.config import OccupancyConfig
from parking_spot_monitor.detection import SpotDetectionCandidate
from parking_spot_monitor.occupancy import (
    OccupancyEvent,
    OccupancyEventType,
    OccupancyStatus,
    QuietWindowStatus,
    SpotOccupancyState,
    update_occupancy,
)
from parking_spot_monitor.state import RuntimeState, load_runtime_state, save_runtime_state
from parking_spot_monitor.vehicle_history import VehicleHistoryArchive

OBSERVED_AT = "2025-01-01T12:00:00Z"
SNAPSHOT_PATH = "/tmp/restart-soak-frame.jpg"
STREET_SPOTS = ("left_spot", "right_spot")


@pytest.fixture
def occupancy_config() -> OccupancyConfig:
    return OccupancyConfig(iou_threshold=0.7, confirm_frames=2, release_frames=2)


@pytest.fixture
def quiet_inactive() -> QuietWindowStatus:
    return QuietWindowStatus(active=False)


def candidate(
    spot_id: str,
    bbox: tuple[float, float, float, float],
    *,
    source_timestamp: str = "2025-01-01T11:59:59Z",
) -> SpotDetectionCandidate:
    return SpotDetectionCandidate(
        spot_id=spot_id,
        class_name="car",
        confidence=0.91,
        bbox=bbox,
        bbox_area_px=(bbox[2] - bbox[0]) * (bbox[3] - bbox[1]),
        centroid=((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2),
        overlap_ratio=1.0,
        source_frame_path=SNAPSHOT_PATH,
        source_timestamp=source_timestamp,
    )


def empty_frame(
    previous_state: dict[str, SpotOccupancyState],
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
):
    return update_occupancy(
        previous_state=previous_state,
        candidates_by_spot={},
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
        configured_spot_ids=STREET_SPOTS,
    )


def occupied_frame(
    previous_state: dict[str, SpotOccupancyState],
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
    *,
    candidates_by_spot: dict[str, SpotDetectionCandidate],
):
    return update_occupancy(
        previous_state=previous_state,
        candidates_by_spot=candidates_by_spot,
        occupancy_config=occupancy_config,
        observed_at=OBSERVED_AT,
        quiet_window_status=quiet_inactive,
        snapshot_path=SNAPSHOT_PATH,
        configured_spot_ids=STREET_SPOTS,
    )


def save_and_reload(path: Path, state_by_spot: dict[str, SpotOccupancyState]) -> RuntimeState:
    save_runtime_state(path, RuntimeState(state_by_spot=state_by_spot))
    return load_runtime_state(path, STREET_SPOTS)


def open_events(events: list[OccupancyEvent]) -> list[OccupancyEvent]:
    return [event for event in events if event.event_type is OccupancyEventType.OPEN_EVENT]


def test_restart_preserves_open_event_suppression_for_already_open_street_spots(
    tmp_path: Path,
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    state_path = tmp_path / "runtime-state.json"
    first_miss_after_occupied = {
        "left_spot": SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            hit_streak=2,
            miss_streak=1,
            last_bbox=(10.0, 10.0, 90.0, 90.0),
            open_event_emitted=False,
        ),
        "right_spot": SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            hit_streak=2,
            miss_streak=1,
            last_bbox=(210.0, 10.0, 290.0, 90.0),
            open_event_emitted=False,
        ),
    }
    released_before_restart = empty_frame(first_miss_after_occupied, occupancy_config, quiet_inactive)

    assert [event.event_type for event in released_before_restart.events] == [
        OccupancyEventType.STATE_CHANGED,
        OccupancyEventType.OPEN_EVENT,
        OccupancyEventType.STATE_CHANGED,
        OccupancyEventType.OPEN_EVENT,
    ]
    assert [event.spot_id for event in open_events(released_before_restart.events)] == ["left_spot", "right_spot"]
    assert all(state.open_event_emitted for state in released_before_restart.state_by_spot.values())

    reloaded = save_and_reload(state_path, released_before_restart.state_by_spot)

    assert reloaded.state_by_spot == released_before_restart.state_by_spot
    duplicate_check = empty_frame(reloaded.state_by_spot, occupancy_config, quiet_inactive)
    later_duplicate_check = empty_frame(duplicate_check.state_by_spot, occupancy_config, quiet_inactive)

    assert duplicate_check.events == []
    assert later_duplicate_check.events == []
    assert duplicate_check.state_by_spot["left_spot"].open_event_emitted is True
    assert duplicate_check.state_by_spot["right_spot"].open_event_emitted is True


def test_reoccupation_after_restart_resets_suppression_without_spot_bleed(
    tmp_path: Path,
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    state_path = tmp_path / "runtime-state.json"
    reloaded = save_and_reload(
        state_path,
        {
            "left_spot": SpotOccupancyState(
                status=OccupancyStatus.EMPTY,
                hit_streak=0,
                miss_streak=3,
                last_bbox=(10.0, 10.0, 90.0, 90.0),
                open_event_emitted=True,
            ),
            "right_spot": SpotOccupancyState(
                status=OccupancyStatus.EMPTY,
                hit_streak=0,
                miss_streak=3,
                last_bbox=(210.0, 10.0, 290.0, 90.0),
                open_event_emitted=True,
            ),
        },
    )

    first_hit = occupied_frame(
        reloaded.state_by_spot,
        occupancy_config,
        quiet_inactive,
        candidates_by_spot={"left_spot": candidate("left_spot", (10.0, 10.0, 90.0, 90.0))},
    )
    reoccupied = occupied_frame(
        first_hit.state_by_spot,
        occupancy_config,
        quiet_inactive,
        candidates_by_spot={"left_spot": candidate("left_spot", (10.0, 10.0, 90.0, 90.0))},
    )

    assert reoccupied.state_by_spot["left_spot"].status is OccupancyStatus.OCCUPIED
    assert reoccupied.state_by_spot["left_spot"].open_event_emitted is False
    assert reoccupied.state_by_spot["right_spot"].status is OccupancyStatus.EMPTY
    assert reoccupied.state_by_spot["right_spot"].open_event_emitted is True
    assert [event.spot_id for event in reoccupied.events] == ["left_spot"]

    post_reoccupy_reload = save_and_reload(state_path, reoccupied.state_by_spot)
    first_empty = empty_frame(post_reoccupy_reload.state_by_spot, occupancy_config, quiet_inactive)
    empty_again = empty_frame(first_empty.state_by_spot, occupancy_config, quiet_inactive)
    repeated_empty = empty_frame(empty_again.state_by_spot, occupancy_config, quiet_inactive)

    assert [event.event_type for event in empty_again.events] == [
        OccupancyEventType.STATE_CHANGED,
        OccupancyEventType.OPEN_EVENT,
    ]
    assert [event.spot_id for event in empty_again.events] == ["left_spot", "left_spot"]
    assert repeated_empty.events == []
    assert empty_again.state_by_spot["left_spot"].open_event_emitted is True
    assert empty_again.state_by_spot["right_spot"].open_event_emitted is True


def test_vehicle_history_active_session_survives_restart_and_closes_on_later_release(
    tmp_path: Path,
    occupancy_config: OccupancyConfig,
    quiet_inactive: QuietWindowStatus,
) -> None:
    state_path = tmp_path / "runtime-state.json"
    archive = VehicleHistoryArchive(tmp_path / "vehicle-history")
    first_hit = occupied_frame(
        {},
        occupancy_config,
        quiet_inactive,
        candidates_by_spot={"left_spot": candidate("left_spot", (10.0, 10.0, 90.0, 90.0))},
    )
    confirmed = occupied_frame(
        first_hit.state_by_spot,
        occupancy_config,
        quiet_inactive,
        candidates_by_spot={"left_spot": candidate("left_spot", (10.0, 10.0, 90.0, 90.0))},
    )
    start_event = next(event for event in confirmed.events if event.event_type is OccupancyEventType.STATE_CHANGED)
    frame_path = tmp_path / "restart-frame.jpg"
    Image.new("RGB", (320, 240), (10, 20, 30)).save(frame_path, format="JPEG")
    started = archive.start_session(start_event)
    started_with_images = archive.attach_occupied_images(
        session_id=started.session_id,
        source_frame_path=frame_path,
        bbox=(10.0, 10.0, 90.0, 90.0),
    )

    assert started.spot_id == "left_spot"
    assert started_with_images.occupied_snapshot_path is not None
    assert started_with_images.occupied_crop_path is not None
    assert Path(started_with_images.occupied_snapshot_path).exists()
    assert Path(started_with_images.occupied_crop_path).exists()
    assert len(list((tmp_path / "vehicle-history" / "sessions" / "active").glob("*.json"))) == 1

    reloaded = save_and_reload(state_path, confirmed.state_by_spot)
    first_empty = empty_frame(reloaded.state_by_spot, occupancy_config, quiet_inactive)
    released = empty_frame(first_empty.state_by_spot, occupancy_config, quiet_inactive)
    close_event = next(event for event in released.events if event.event_type is OccupancyEventType.STATE_CHANGED)
    closed = archive.close_session(close_event)

    assert closed is not None
    assert closed.session_id == started.session_id
    assert closed.occupied_snapshot_path == started_with_images.occupied_snapshot_path
    assert closed.occupied_crop_path == started_with_images.occupied_crop_path
    assert list((tmp_path / "vehicle-history" / "sessions" / "active").glob("*.json")) == []
    closed_files = list((tmp_path / "vehicle-history" / "sessions" / "closed").glob("*.json"))
    assert len(closed_files) == 1
    closed_payload = json.loads(closed_files[0].read_text(encoding="utf-8"))
    assert closed_payload["occupied_snapshot_path"] == started_with_images.occupied_snapshot_path
    assert closed_payload["occupied_crop_path"] == started_with_images.occupied_crop_path
    assert open_events(released.events)[0].spot_id == "left_spot"
