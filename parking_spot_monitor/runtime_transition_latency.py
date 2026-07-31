from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from math import isfinite

from parking_spot_monitor.config import RuntimeSettings
from parking_spot_monitor.detection import DetectionFilterResult
from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.occupancy import OccupancyStatus
from parking_spot_monitor.runtime_frame_outcome import RuntimeFrameLoopResult
from parking_spot_monitor.runtime_presence import presence_by_spot
from parking_spot_monitor.runtime_resource_policy import RuntimeResourceDecision
from parking_spot_monitor.state import RuntimeState


def bounded_seconds(value: float) -> float:
    if not isfinite(value) or value < 0:
        return 0.0
    return round(value, 6)


@dataclass
class TransitionEvidenceTracker:
    _last_by_spot_status: dict[tuple[str, OccupancyStatus], datetime] = field(
        default_factory=dict
    )

    def observe(
        self,
        *,
        spot_id: str,
        observed_at: datetime,
        evidence_status: OccupancyStatus,
    ) -> None:
        if observed_at.tzinfo is not None:
            self._last_by_spot_status[(spot_id, evidence_status)] = observed_at

    def confirmed_transition_fields(
        self,
        *,
        spot_id: str,
        previous_status: OccupancyStatus,
        new_status: OccupancyStatus,
        confirmed_at: datetime,
        primary_capture_seconds: float,
        verification_capture_seconds: float | None,
        cadence_seconds: float,
        cadence_reason: str,
    ) -> dict[str, str | float] | None:
        if previous_status is new_status:
            return None
        previous = self._last_by_spot_status.pop(
            (spot_id, previous_status),
            None,
        )
        if previous is None or confirmed_at.tzinfo is None:
            return None
        elapsed = (confirmed_at - previous).total_seconds()
        if not isfinite(elapsed) or elapsed < 0:
            return None
        fields: dict[str, str | float] = {
            "spot_id": spot_id,
            "transition_direction": f"{previous_status.value}-to-{new_status.value}",
            "opposite_evidence_to_confirmation_seconds": round(elapsed, 6),
            "primary_capture_seconds": bounded_seconds(primary_capture_seconds),
            "cadence_seconds": bounded_seconds(cadence_seconds),
            "cadence_reason": cadence_reason,
        }
        if verification_capture_seconds is not None:
            fields["verification_capture_seconds"] = bounded_seconds(
                verification_capture_seconds
            )
        return fields


@dataclass
class RuntimeTransitionTelemetry:
    previous_policy_decision: RuntimeResourceDecision
    evidence: TransitionEvidenceTracker = field(default_factory=TransitionEvidenceTracker)

    @classmethod
    def from_interval(cls, interval_seconds: float) -> RuntimeTransitionTelemetry:
        return cls(
            previous_policy_decision=RuntimeResourceDecision(
                interval_seconds=interval_seconds,
                reason="unknown",
                stable_success_count=0,
            )
        )

    def observe_frame(
        self,
        settings: RuntimeSettings,
        detection_result: DetectionFilterResult,
        observed_at: datetime,
    ) -> None:
        spot_presence = presence_by_spot(
            detection_result,
            open_suppression_classes=settings.detection.open_suppression_classes,
            min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
        )
        for spot_id, spot_result in detection_result.by_spot.items():
            if spot_result.accepted is not None:
                evidence_status = OccupancyStatus.OCCUPIED
            elif not spot_presence.get(spot_id, False):
                evidence_status = OccupancyStatus.EMPTY
            else:
                continue
            self.evidence.observe(
                spot_id=spot_id,
                observed_at=observed_at,
                evidence_status=evidence_status,
            )

    def log_confirmed_transitions(
        self,
        previous_state: RuntimeState,
        next_state: RuntimeState,
        frame_result: RuntimeFrameLoopResult,
        confirmed_at: datetime,
        spot_ids: Sequence[str],
        logger: StructuredLogger,
    ) -> None:
        for spot_id in spot_ids:
            previous_spot_state = previous_state.state_by_spot.get(spot_id)
            new_spot_state = next_state.state_by_spot.get(spot_id)
            if (
                previous_spot_state is None
                or new_spot_state is None
                or previous_spot_state.status is new_spot_state.status
            ):
                continue
            latency_fields = self.evidence.confirmed_transition_fields(
                spot_id=spot_id,
                previous_status=previous_spot_state.status,
                new_status=new_spot_state.status,
                confirmed_at=confirmed_at,
                primary_capture_seconds=frame_result.primary_capture.duration_seconds,
                verification_capture_seconds=(
                    frame_result.capture.duration_seconds
                    if frame_result.escalated
                    else None
                ),
                cadence_seconds=self.previous_policy_decision.interval_seconds,
                cadence_reason=self.previous_policy_decision.reason,
            )
            if latency_fields is not None:
                logger.info("occupancy-transition-latency", **latency_fields)

    def record_policy_decision(
        self,
        decision: RuntimeResourceDecision,
        *,
        iteration: int,
        logger: StructuredLogger,
    ) -> None:
        previous = self.previous_policy_decision
        if (
            decision.interval_seconds != previous.interval_seconds
            or decision.reason != previous.reason
        ):
            logger.info(
                "capture-loop-cadence-changed",
                iteration=iteration,
                previous_interval_seconds=previous.interval_seconds,
                previous_reason=previous.reason,
                interval_seconds=decision.interval_seconds,
                cadence_reason=decision.reason,
            )
        self.previous_policy_decision = decision
