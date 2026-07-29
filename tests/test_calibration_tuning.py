from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from typing import Any

import pytest

from parking_spot_monitor import tuning
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


def empty_tuning_report() -> dict[str, Any]:
    return build_tuning_comparison_report(
        manifest(
            ReplayFrame(
                frame_id="empty-frame",
                expected={"left_spot": ExpectedPresence.EMPTY, "right_spot": ExpectedPresence.EMPTY},
                detections=[],
            )
        ),
        baseline_config=replay_config(),
        proposed_config=replay_config(),
    )


def duplicated_public_mapping_pair(report: dict[str, Any], pair_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
    pairs = {
        "baseline_thresholds": (report["baseline_thresholds"], report["baseline"]["config_thresholds"]),
        "proposed_thresholds": (report["proposed_thresholds"], report["proposed"]["config_thresholds"]),
        "baseline_status_counts": (report["status_counts"]["baseline"], report["baseline"]["status_counts"]),
        "proposed_status_counts": (report["status_counts"]["proposed"], report["proposed"]["status_counts"]),
    }
    return pairs[pair_name]


DUPLICATED_PUBLIC_MAPPING_PAIRS = (
    "baseline_thresholds",
    "proposed_thresholds",
    "baseline_status_counts",
    "proposed_status_counts",
)


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


@pytest.mark.parametrize("pair_name", DUPLICATED_PUBLIC_MAPPING_PAIRS)
def test_tuning_report_duplicate_mapping_paths_have_independent_identity(pair_name: str) -> None:
    first, second = duplicated_public_mapping_pair(empty_tuning_report(), pair_name)

    assert first == second
    assert first is not second


@pytest.mark.parametrize("pair_name", DUPLICATED_PUBLIC_MAPPING_PAIRS)
def test_mutating_first_duplicate_mapping_path_does_not_change_second(pair_name: str) -> None:
    first, second = duplicated_public_mapping_pair(empty_tuning_report(), pair_name)

    first["first-path-only"] = {"owner": "first"}

    assert "first-path-only" not in second


@pytest.mark.parametrize("pair_name", DUPLICATED_PUBLIC_MAPPING_PAIRS)
def test_mutating_second_duplicate_mapping_path_does_not_change_first(pair_name: str) -> None:
    first, second = duplicated_public_mapping_pair(empty_tuning_report(), pair_name)

    second["second-path-only"] = {"owner": "second"}

    assert "second-path-only" not in first


@pytest.mark.parametrize("side", ("baseline", "proposed"))
def test_mutating_nested_summary_threshold_does_not_change_side_detail(side: str) -> None:
    report = empty_tuning_report()

    report[f"{side}_thresholds"]["occupancy"]["confirm_frames"] = 7

    assert report[side]["config_thresholds"]["occupancy"]["confirm_frames"] == 2


@pytest.mark.parametrize("side", ("baseline", "proposed"))
def test_mutating_nested_side_threshold_does_not_change_summary(side: str) -> None:
    report = empty_tuning_report()

    report[side]["config_thresholds"]["occupancy"]["confirm_frames"] = 7

    assert report[f"{side}_thresholds"]["occupancy"]["confirm_frames"] == 2


def test_rendering_tuning_report_preserves_nested_input_and_exact_markdown() -> None:
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
    before = copy.deepcopy(report)

    markdown = render_tuning_report_markdown(report)

    assert report == before
    assert json.loads(json.dumps(report)) == before
    assert markdown == (
        "# Tuning Comparison Report\n"
        "\n"
        "## Decision\n"
        "- Decision: **apply_shared_tuning**\n"
        "- Rationale: proposed shared thresholds reduce false-positive/false-negative evidence without new safety regressions\n"
        "- Schema version: `parking-spot-monitor.tuning-report.v1`\n"
        "- Case IDs: `case-1`\n"
        "\n"
        "## Thresholds Compared\n"
        '- Baseline: `{"allowed_classes":["car","truck"],"confidence_threshold":0.55,"min_bbox_area_px":100.0,"min_polygon_overlap_ratio":0.5,"occupancy":{"confirm_frames":2,"iou_threshold":0.7,"release_frames":2}}`\n'
        '- Proposed: `{"allowed_classes":["car","truck"],"confidence_threshold":0.35,"min_bbox_area_px":100.0,"min_polygon_overlap_ratio":0.5,"occupancy":{"confirm_frames":2,"iou_threshold":0.7,"release_frames":2}}`\n'
        "\n"
        "## Metric Deltas\n"
        "Deltas are proposed minus baseline; negative FP/FN values are improvements.\n"
        "- True positives: 1\n"
        "- True negatives: 0\n"
        "- False positives: 0\n"
        "- False negatives: -1\n"
        "\n"
        "| Spot | TP | TN | FP | FN | Blocked | Not covered |\n"
        "|---|---:|---:|---:|---:|---:|---:|\n"
        "| `left_spot` | 1 | 0 | 0 | -1 | 0 | 0 |\n"
        "| `right_spot` | 0 | 0 | 0 | 0 | 0 | 0 |\n"
        "\n"
        "## Event Deltas\n"
        "- Baseline findings: 0\n"
        "- Proposed findings: 0\n"
        "- Added findings: 0\n"
        "- Removed findings: 0\n"
        "\n"
        "## Coverage and Safety\n"
        '- Status counts: `{"baseline":{"blocked":0,"failed":1,"not_covered":0,"passed":0},"proposed":{"blocked":0,"failed":0,"not_covered":0,"passed":1}}`\n'
        "- Blocked reasons: None\n"
        "- Not-covered reasons: None\n"
        "- Redaction passed: True\n"
        "- Redaction findings: None\n"
    )


def test_tuning_builder_and_renderer_do_not_decode_reports_from_json(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbid_json_decode(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("tuning reports must not be copied through JSON decoding")

    monkeypatch.setattr(tuning, "json", SimpleNamespace(dumps=json.dumps, loads=forbid_json_decode))

    report = build_tuning_comparison_report(
        manifest(
            ReplayFrame(
                frame_id="empty-frame",
                expected={"left_spot": ExpectedPresence.EMPTY, "right_spot": ExpectedPresence.EMPTY},
                detections=[],
            )
        ),
        baseline_config=replay_config(),
        proposed_config=replay_config(),
    )

    assert render_tuning_report_markdown(report).startswith("# Tuning Comparison Report\n")


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
