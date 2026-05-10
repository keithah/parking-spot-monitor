from __future__ import annotations

import json

import pytest

from parking_spot_monitor.config import OccupancyConfig
from parking_spot_monitor.replay import (
    ExpectedPresence,
    LabelManifest,
    ReplayDetection,
    ReplayEvaluationConfig,
    ReplayFrame,
    ReplayReportError,
)
from parking_spot_monitor.tuning import (
    TuningDecision,
    build_tuning_comparison_report,
    render_tuning_report_markdown,
)


LEFT_SPOT = [(0, 0), (100, 0), (100, 100), (0, 100)]
RIGHT_SPOT = [(200, 0), (300, 0), (300, 100), (200, 100)]


def replay_config(*, confidence_threshold: float = 0.35, min_polygon_overlap_ratio: float = 0.5) -> ReplayEvaluationConfig:
    return ReplayEvaluationConfig(
        spots={"left_spot": LEFT_SPOT, "right_spot": RIGHT_SPOT},
        allowed_classes=["car", "truck"],
        confidence_threshold=confidence_threshold,
        min_bbox_area_px=100,
        min_polygon_overlap_ratio=min_polygon_overlap_ratio,
        occupancy=OccupancyConfig(iou_threshold=0.7, confirm_frames=2, release_frames=2),
    )


def detection(bbox: tuple[float, float, float, float], *, confidence: float = 0.9, class_name: str = "car") -> ReplayDetection:
    return ReplayDetection(class_name=class_name, confidence=confidence, bbox=bbox)


def manifest(*frames: ReplayFrame, case_id: str = "case-1", scenario_id: str = "scenario") -> LabelManifest:
    return LabelManifest(cases=[{"case_id": case_id, "scenarios": [{"scenario_id": scenario_id, "frames": list(frames)}]}])


def test_improved_proposed_config_applies_shared_tuning_and_serializes_report() -> None:
    report = build_tuning_comparison_report(
        manifest(
            ReplayFrame(
                frame_id="low-confidence-occupied",
                expected={"left_spot": ExpectedPresence.OCCUPIED, "right_spot": ExpectedPresence.EMPTY},
                detections=[detection((10, 10, 90, 90), confidence=0.42)],
            ),
            ReplayFrame(
                frame_id="empty-frame",
                expected={"left_spot": ExpectedPresence.EMPTY, "right_spot": ExpectedPresence.EMPTY},
                detections=[],
            ),
        ),
        baseline_config=replay_config(confidence_threshold=0.55),
        proposed_config=replay_config(confidence_threshold=0.35),
        created_at="2026-05-10T00:00:00Z",
    )

    assert report["schema_version"] == "parking-spot-monitor.tuning-report.v1"
    assert report["decision"] == TuningDecision.APPLY_SHARED_TUNING.value
    assert report["case_ids"] == ["case-1"]
    assert report["baseline"]["metrics_by_spot"]["left_spot"]["fn"] == 1
    assert report["proposed"]["metrics_by_spot"]["left_spot"]["tp"] == 1
    assert report["metric_deltas"]["totals"]["fp"] == 0
    assert report["metric_deltas"]["totals"]["fn"] == -1
    assert report["blocked_reasons"] == []
    assert report["not_covered_reasons"] == []
    assert report["redaction_scan"]["passed"] is True
    json.dumps(report)

    markdown = render_tuning_report_markdown(report)
    assert "# Tuning Comparison Report" in markdown
    assert "Decision: **apply_shared_tuning**" in markdown
    assert "False negatives: -1" in markdown


def test_regressing_proposed_config_keeps_shared_thresholds_with_visible_deltas() -> None:
    report = build_tuning_comparison_report(
        manifest(
            ReplayFrame(
                frame_id="occupied",
                expected={"left_spot": ExpectedPresence.OCCUPIED, "right_spot": ExpectedPresence.EMPTY},
                detections=[detection((10, 10, 90, 90), confidence=0.7)],
            )
        ),
        baseline_config=replay_config(confidence_threshold=0.35),
        proposed_config=replay_config(confidence_threshold=0.85),
    )

    assert report["decision"] == TuningDecision.KEEP_SHARED_THRESHOLDS.value
    assert report["metric_deltas"]["totals"]["fn"] == 1
    assert report["decision_rationale"] == "proposed shared thresholds do not improve false-positive/false-negative evidence"


def test_blocked_and_missing_evidence_force_blocked_decision() -> None:
    report = build_tuning_comparison_report(
        LabelManifest(
            cases=[
                {
                    "case_id": "blocked-case",
                    "scenarios": [{"scenario_id": "scenario", "frames": [{"frame_id": "missing", "expected": {"left_spot": "occupied"}, "detections": None}]}],
                },
                {
                    "case_id": "not-covered-case",
                    "assessed": False,
                    "scenarios": [{"scenario_id": "scenario", "frames": [{"frame_id": "unknown", "expected": {"right_spot": "unknown"}, "detections": []}]}],
                },
            ]
        ),
        baseline_config=replay_config(),
        proposed_config=replay_config(confidence_threshold=0.3),
    )

    assert report["decision"] == TuningDecision.BLOCKED.value
    assert report["status_counts"]["proposed"] == {"passed": 0, "failed": 0, "blocked": 1, "not_covered": 1}
    assert report["blocked_reasons"] == ["missing_detector_data"]
    assert report["not_covered_reasons"] == ["case_not_assessed"]
    assert "blocked or not-covered replay evidence" in report["decision_rationale"]


def test_spot_divergent_residual_false_positives_request_per_spot_thresholds() -> None:
    report = build_tuning_comparison_report(
        manifest(
            ReplayFrame(
                frame_id="right-residual-fp",
                expected={"left_spot": ExpectedPresence.EMPTY, "right_spot": ExpectedPresence.EMPTY},
                detections=[detection((210, 10, 290, 90), confidence=0.95)],
            ),
            ReplayFrame(
                frame_id="left-covered-empty",
                expected={"left_spot": ExpectedPresence.EMPTY, "right_spot": ExpectedPresence.EMPTY},
                detections=[],
            ),
        ),
        baseline_config=replay_config(),
        proposed_config=replay_config(confidence_threshold=0.9),
    )

    assert report["decision"] == TuningDecision.NEEDS_PER_SPOT_THRESHOLDS.value
    assert report["proposed"]["shared_threshold_sufficiency"]["verdict"] == "insufficient"
    assert report["metric_deltas"]["by_spot"]["right_spot"]["fp"] == 0
    assert report["decision_rationale"] == "residual false-positive/false-negative errors diverge by spot under shared proposed thresholds"


def test_unsafe_redaction_content_blocks_report_and_markdown_rendering() -> None:
    report = build_tuning_comparison_report(
        manifest(
            ReplayFrame(
                frame_id="unsafe-source",
                expected={"left_spot": ExpectedPresence.OCCUPIED},
                detections=[detection((10, 10, 90, 90))],
                snapshot_path="rtsp://user:pass@example.test/live.jpg",
            )
        ),
        baseline_config=replay_config(),
        proposed_config=replay_config(confidence_threshold=0.3),
    )

    assert report["decision"] == TuningDecision.BLOCKED.value
    assert report["redaction_scan"]["passed"] is False
    assert "rtsp_url" in report["redaction_scan"]["findings"]
    with pytest.raises(ReplayReportError, match="rendered Markdown contains unsafe content"):
        render_tuning_report_markdown(report)


def test_event_finding_deltas_show_added_and_removed_open_events() -> None:
    report = build_tuning_comparison_report(
        manifest(
            ReplayFrame(frame_id="occupied-1", expected={"left_spot": ExpectedPresence.OCCUPIED}, detections=[detection((10, 10, 90, 90), confidence=0.42)]),
            ReplayFrame(frame_id="occupied-2", expected={"left_spot": ExpectedPresence.OCCUPIED}, detections=[detection((10, 10, 90, 90), confidence=0.42)]),
            ReplayFrame(frame_id="miss-1", expected={"left_spot": ExpectedPresence.EMPTY}, detections=[]),
            ReplayFrame(frame_id="release", expected={"left_spot": ExpectedPresence.EMPTY}, detections=[]),
        ),
        baseline_config=replay_config(confidence_threshold=0.55),
        proposed_config=replay_config(confidence_threshold=0.35),
    )

    assert report["event_deltas"]["added"] == [
        {
            "case_id": "case-1",
            "scenario_id": "scenario",
            "frame_id": "release",
            "spot_id": "left_spot",
            "event_type": "occupancy-open-event",
            "finding": "expected_open_event",
        }
    ]
    assert report["event_deltas"]["removed"] == []
    assert report["event_deltas"]["baseline_count"] == 0
    assert report["event_deltas"]["proposed_count"] == 1
