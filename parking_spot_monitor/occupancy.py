from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Sequence

from parking_spot_monitor.config import OccupancyConfig
from parking_spot_monitor.detection import SpotDetectionCandidate
from parking_spot_monitor.geometry import bbox_area, bbox_iou

BBoxTuple = tuple[float, float, float, float]


class OccupancyStatus(StrEnum):
    """Confirmed occupancy status for one configured parking spot."""

    UNKNOWN = "unknown"
    OCCUPIED = "occupied"
    EMPTY = "empty"


class OccupancyEventType(StrEnum):
    """Stable event names consumed by runtime logging and notification code."""

    STATE_CHANGED = "occupancy-state-changed"
    OPEN_EVENT = "occupancy-open-event"
    OPEN_SUPPRESSED = "occupancy-open-suppressed"


@dataclass(frozen=True)
class QuietWindowStatus:
    """Pure scheduler input for notification suppression decisions."""

    active: bool
    window_id: str | None = None

    @property
    def suppressed_reason(self) -> str | None:
        if not self.active:
            return None
        return f"quiet_window:{self.window_id}" if self.window_id else "quiet_window"


@dataclass(frozen=True)
class SpotOccupancyState:
    """Serializable per-spot runtime state with no filesystem coupling."""

    status: OccupancyStatus = OccupancyStatus.UNKNOWN
    hit_streak: int = 0
    miss_streak: int = 0
    last_bbox: BBoxTuple | None = None
    open_event_emitted: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "hit_streak": self.hit_streak,
            "miss_streak": self.miss_streak,
            "last_bbox": self.last_bbox,
            "open_event_emitted": self.open_event_emitted,
        }


@dataclass(frozen=True)
class OccupancyEvent:
    """Serializable occupancy event payload for downstream S06 delivery."""

    event_type: OccupancyEventType
    spot_id: str
    previous_status: OccupancyStatus
    new_status: OccupancyStatus
    observed_at: Any
    source_timestamp: Any | None
    snapshot_path: str
    candidate_summary: dict[str, Any] | None = None
    suppressed_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "spot_id": self.spot_id,
            "previous_status": self.previous_status.value,
            "new_status": self.new_status.value,
            "observed_at": self.observed_at,
            "source_timestamp": self.source_timestamp,
            "snapshot_path": self.snapshot_path,
            "candidate": self.candidate_summary,
            "suppressed_reason": self.suppressed_reason,
        }


@dataclass(frozen=True)
class OccupancyUpdate:
    """Pure update result containing copied state and transition events."""

    state_by_spot: dict[str, SpotOccupancyState] = field(default_factory=dict)
    events: list[OccupancyEvent] = field(default_factory=list)


def update_occupancy(
    previous_state: Mapping[str, SpotOccupancyState],
    candidates_by_spot: Mapping[str, SpotDetectionCandidate | None],
    occupancy_config: OccupancyConfig,
    observed_at: Any,
    quiet_window_status: QuietWindowStatus,
    snapshot_path: str,
    configured_spot_ids: Sequence[str] | None = None,
) -> OccupancyUpdate:
    """Advance conservative occupancy state for configured spots.

    The function is intentionally detector-neutral and side-effect free. It emits
    events only for confirmed transitions and confirmed open episodes, keeping
    per-frame work and downstream log volume bounded by the number of spots.
    """

    spot_ids = _spot_ids(previous_state, candidates_by_spot, configured_spot_ids)
    next_state: dict[str, SpotOccupancyState] = {}
    events: list[OccupancyEvent] = []

    for spot_id in spot_ids:
        prior = _copy_state(previous_state.get(spot_id, SpotOccupancyState()))
        candidate = _valid_candidate(candidates_by_spot.get(spot_id))

        if candidate is None:
            updated, spot_events = _advance_miss(
                spot_id=spot_id,
                prior=prior,
                occupancy_config=occupancy_config,
                observed_at=observed_at,
                quiet_window_status=quiet_window_status,
                snapshot_path=snapshot_path,
            )
        else:
            updated, spot_events = _advance_hit(
                spot_id=spot_id,
                prior=prior,
                candidate=candidate,
                occupancy_config=occupancy_config,
                observed_at=observed_at,
                snapshot_path=snapshot_path,
            )

        next_state[spot_id] = updated
        events.extend(spot_events)

    return OccupancyUpdate(state_by_spot=next_state, events=events)


def _advance_hit(
    *,
    spot_id: str,
    prior: SpotOccupancyState,
    candidate: SpotDetectionCandidate,
    occupancy_config: OccupancyConfig,
    observed_at: Any,
    snapshot_path: str,
) -> tuple[SpotOccupancyState, list[OccupancyEvent]]:
    hit_streak = prior.hit_streak + 1 if _extends_stable_hit(prior, candidate, occupancy_config.iou_threshold) else 1
    previous_status = prior.status
    new_status = prior.status
    open_event_emitted = prior.open_event_emitted

    if hit_streak >= occupancy_config.confirm_frames and prior.status is not OccupancyStatus.OCCUPIED:
        new_status = OccupancyStatus.OCCUPIED
        open_event_emitted = False

    updated = SpotOccupancyState(
        status=new_status,
        hit_streak=hit_streak,
        miss_streak=0,
        last_bbox=_normalize_bbox(candidate.bbox),
        open_event_emitted=open_event_emitted,
    )

    events: list[OccupancyEvent] = []
    if new_status != previous_status:
        events.append(
            _event(
                event_type=OccupancyEventType.STATE_CHANGED,
                spot_id=spot_id,
                previous_status=previous_status,
                new_status=new_status,
                observed_at=observed_at,
                snapshot_path=snapshot_path,
                candidate=candidate,
            )
        )
    return updated, events


def _advance_miss(
    *,
    spot_id: str,
    prior: SpotOccupancyState,
    occupancy_config: OccupancyConfig,
    observed_at: Any,
    quiet_window_status: QuietWindowStatus,
    snapshot_path: str,
) -> tuple[SpotOccupancyState, list[OccupancyEvent]]:
    miss_streak = prior.miss_streak + 1
    previous_status = prior.status
    new_status = prior.status
    open_event_emitted = prior.open_event_emitted

    if prior.status is OccupancyStatus.UNKNOWN and miss_streak >= occupancy_config.release_frames:
        new_status = OccupancyStatus.EMPTY
    elif prior.status is OccupancyStatus.OCCUPIED and miss_streak >= occupancy_config.release_frames:
        new_status = OccupancyStatus.EMPTY

    events: list[OccupancyEvent] = []
    if new_status != previous_status:
        events.append(
            _event(
                event_type=OccupancyEventType.STATE_CHANGED,
                spot_id=spot_id,
                previous_status=previous_status,
                new_status=new_status,
                observed_at=observed_at,
                snapshot_path=snapshot_path,
            )
        )

    if new_status is OccupancyStatus.EMPTY and previous_status is OccupancyStatus.OCCUPIED and not prior.open_event_emitted:
        suppressed_reason = quiet_window_status.suppressed_reason
        events.append(
            _event(
                event_type=OccupancyEventType.OPEN_SUPPRESSED if suppressed_reason else OccupancyEventType.OPEN_EVENT,
                spot_id=spot_id,
                previous_status=previous_status,
                new_status=new_status,
                observed_at=observed_at,
                snapshot_path=snapshot_path,
                suppressed_reason=suppressed_reason,
            )
        )
        open_event_emitted = True

    return SpotOccupancyState(
        status=new_status,
        hit_streak=0,
        miss_streak=miss_streak,
        last_bbox=prior.last_bbox,
        open_event_emitted=open_event_emitted,
    ), events


def _event(
    *,
    event_type: OccupancyEventType,
    spot_id: str,
    previous_status: OccupancyStatus,
    new_status: OccupancyStatus,
    observed_at: Any,
    snapshot_path: str,
    candidate: SpotDetectionCandidate | None = None,
    suppressed_reason: str | None = None,
) -> OccupancyEvent:
    return OccupancyEvent(
        event_type=event_type,
        spot_id=spot_id,
        previous_status=previous_status,
        new_status=new_status,
        observed_at=observed_at,
        source_timestamp=getattr(candidate, "source_timestamp", None),
        snapshot_path=snapshot_path,
        candidate_summary=_candidate_summary(candidate),
        suppressed_reason=suppressed_reason,
    )


def _spot_ids(
    previous_state: Mapping[str, SpotOccupancyState],
    candidates_by_spot: Mapping[str, SpotDetectionCandidate | None],
    configured_spot_ids: Sequence[str] | None,
) -> list[str]:
    ordered: list[str] = []
    for source in (configured_spot_ids or (), previous_state.keys(), candidates_by_spot.keys()):
        for spot_id in source:
            if spot_id not in ordered:
                ordered.append(spot_id)
    return ordered


def _copy_state(state: SpotOccupancyState) -> SpotOccupancyState:
    return SpotOccupancyState(
        status=OccupancyStatus(state.status),
        hit_streak=state.hit_streak,
        miss_streak=state.miss_streak,
        last_bbox=tuple(state.last_bbox) if state.last_bbox is not None else None,
        open_event_emitted=state.open_event_emitted,
    )


def _extends_stable_hit(prior: SpotOccupancyState, candidate: SpotDetectionCandidate, iou_threshold: float) -> bool:
    if prior.hit_streak <= 0 or prior.last_bbox is None:
        return False
    try:
        return bbox_iou(prior.last_bbox, candidate.bbox) >= iou_threshold
    except (TypeError, ValueError, OverflowError):
        return False


def _valid_candidate(candidate: SpotDetectionCandidate | None) -> SpotDetectionCandidate | None:
    if candidate is None:
        return None
    try:
        _normalize_bbox(candidate.bbox)
    except (TypeError, ValueError, OverflowError):
        return None
    return candidate


def _normalize_bbox(bbox: Sequence[float]) -> BBoxTuple:
    bbox_area(bbox)
    return (float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))


def _candidate_summary(candidate: SpotDetectionCandidate | None) -> dict[str, Any] | None:
    if candidate is None:
        return None
    return {
        "class_name": candidate.class_name,
        "confidence": candidate.confidence,
        "bbox": _normalize_bbox(candidate.bbox),
        "source_frame_path": candidate.source_frame_path,
    }
