from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from parking_spot_monitor.config import OccupancyConfig
from parking_spot_monitor.replay import (
    CaseStatus,
    ExpectedPresence,
    LabelManifest,
    ObservationOutcome,
    ReplayDetection,
    ReplayEvaluationConfig,
    ReplayFrame,
    ReplayReportError,
    ReplayScenario,
    SharedThresholdVerdict,
    build_replay_report,
    render_replay_report_markdown,
    scan_report_redactions,
    evaluate_manifest,
)


LEFT_SPOT = [(0, 0), (100, 0), (100, 100), (0, 100)]
RIGHT_SPOT = [(200, 0), (300, 0), (300, 100), (200, 100)]


def config() -> ReplayEvaluationConfig:
    return ReplayEvaluationConfig(
        spots={"left_spot": LEFT_SPOT, "right_spot": RIGHT_SPOT},
        allowed_classes=["car", "truck"],
        confidence_threshold=0.35,
        min_bbox_area_px=100,
        min_polygon_overlap_ratio=0.5,
        occupancy=OccupancyConfig(iou_threshold=0.7, confirm_frames=2, release_frames=2),
    )


def detection(bbox: tuple[float, float, float, float], *, confidence: float = 0.9, class_name: str = "car") -> ReplayDetection:
    return ReplayDetection(class_name=class_name, confidence=confidence, bbox=bbox)


def manifest_with_frames(*frames: ReplayFrame, scenario_id: str = "scenario") -> LabelManifest:
    return LabelManifest(
        cases=[
            {
                "case_id": "case-1",
                "scenarios": [{"scenario_id": scenario_id, "frames": list(frames)}],
            }
        ]
    )


def outcomes(result) -> list[ObservationOutcome]:
    return [observation.outcome for observation in result.cases[0].scenarios[0].frames[0].observations]


def test_replay_accepts_true_occupied_bbox_and_counts_true_negative_for_empty_spot() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="frame-1",
                expected={"left_spot": ExpectedPresence.OCCUPIED, "right_spot": ExpectedPresence.EMPTY},
                detections=[detection((10, 10, 90, 90))],
            )
        ),
        config(),
    )

    assert result.cases[0].status is CaseStatus.PASSED
    assert result.metrics_by_spot["left_spot"].tp == 1
    assert result.metrics_by_spot["right_spot"].tn == 1
    assert outcomes(result) == [ObservationOutcome.TRUE_POSITIVE, ObservationOutcome.TRUE_NEGATIVE]
    assert result.shared_threshold_sufficiency.verdict is SharedThresholdVerdict.SUFFICIENT
    assert result.to_jsonable()["metrics_by_spot"]["left_spot"]["tp"] == 1
    json.dumps(result.to_jsonable())


def test_replay_rejects_passing_driveway_bbox_as_false_negative_free_true_negative() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="driveway",
                expected={"left_spot": ExpectedPresence.EMPTY, "right_spot": ExpectedPresence.EMPTY},
                detections=[detection((90, 120, 220, 220))],
            )
        ),
        config(),
    )

    frame = result.cases[0].scenarios[0].frames[0]
    assert result.cases[0].status is CaseStatus.PASSED
    assert result.metrics_by_spot["left_spot"].tn == 1
    assert result.metrics_by_spot["right_spot"].tn == 1
    assert frame.rejection_counts == {"centroid_outside": 2}


def test_replay_counts_false_positive_and_marks_shared_thresholds_insufficient() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="fp",
                expected={"left_spot": ExpectedPresence.EMPTY, "right_spot": ExpectedPresence.EMPTY},
                detections=[detection((10, 10, 90, 90))],
            )
        ),
        config(),
    )

    assert result.cases[0].status is CaseStatus.FAILED
    assert result.metrics_by_spot["left_spot"].fp == 1
    assert result.shared_threshold_sufficiency.verdict is SharedThresholdVerdict.INSUFFICIENT


def test_occupied_to_empty_sequence_emits_exactly_one_open_event_after_release_frames() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="occupied-1",
                expected={"left_spot": ExpectedPresence.OCCUPIED},
                detections=[detection((10, 10, 90, 90))],
            ),
            ReplayFrame(
                frame_id="occupied-2",
                expected={"left_spot": ExpectedPresence.OCCUPIED},
                detections=[detection((10, 10, 90, 90))],
            ),
            ReplayFrame(
                frame_id="miss-1",
                expected={"left_spot": ExpectedPresence.EMPTY},
                detections=[],
            ),
            ReplayFrame(
                frame_id="release",
                expected={"left_spot": ExpectedPresence.EMPTY},
                detections=[],
            ),
            ReplayFrame(
                frame_id="duplicate-empty",
                expected={"left_spot": ExpectedPresence.EMPTY},
                detections=[],
            ),
            scenario_id="open-event",
        ),
        config(),
    )

    findings = result.cases[0].scenarios[0].event_findings
    open_findings = [finding for finding in findings if finding.event_type == "occupancy-open-event"]
    assert len(open_findings) == 1
    assert open_findings[0].frame_id == "release"
    assert open_findings[0].finding == "expected_open_event"
    assert [finding.event_type for finding in findings].count("occupancy-open-event") == 1


def test_startup_unknown_to_empty_does_not_emit_open_event() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(frame_id="empty-1", expected={"left_spot": ExpectedPresence.EMPTY}, detections=[]),
            ReplayFrame(frame_id="empty-2", expected={"left_spot": ExpectedPresence.EMPTY}, detections=[]),
            scenario_id="startup-empty",
        ),
        config(),
    )

    findings = result.cases[0].scenarios[0].event_findings
    assert [finding.event_type for finding in findings] == ["occupancy-state-changed"]
    assert all(finding.event_type != "occupancy-open-event" for finding in findings)


def test_quiet_window_release_reports_suppressed_open_event() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(frame_id="occupied-1", expected={"left_spot": ExpectedPresence.OCCUPIED}, detections=[detection((10, 10, 90, 90))]),
            ReplayFrame(frame_id="occupied-2", expected={"left_spot": ExpectedPresence.OCCUPIED}, detections=[detection((10, 10, 90, 90))]),
            ReplayFrame(frame_id="miss-1", expected={"left_spot": ExpectedPresence.EMPTY}, detections=[]),
            ReplayFrame(
                frame_id="quiet-release",
                expected={"left_spot": ExpectedPresence.EMPTY},
                detections=[],
                quiet_window_active=True,
                quiet_window_id="street-cleaning",
            ),
        ),
        config(),
    )

    suppressed = [finding for finding in result.cases[0].scenarios[0].event_findings if finding.event_type == "occupancy-open-suppressed"]
    assert len(suppressed) == 1
    assert suppressed[0].finding == "quiet_window_open_suppressed"
    assert suppressed[0].suppressed_reason == "quiet_window:street-cleaning"


def test_unknown_and_not_visible_expectations_are_not_counted_as_success() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="unknown",
                expected={"left_spot": ExpectedPresence.UNKNOWN, "right_spot": ExpectedPresence.NOT_VISIBLE},
                detections=[detection((10, 10, 90, 90))],
            )
        ),
        config(),
    )

    assert result.cases[0].status is CaseStatus.NOT_ASSESSED
    assert result.metrics_by_spot["left_spot"].not_assessed == 1
    assert result.metrics_by_spot["right_spot"].not_assessed == 1
    assert result.shared_threshold_sufficiency.verdict is SharedThresholdVerdict.NOT_COVERED


def test_missing_detector_data_blocks_frame_and_does_not_count_as_pass() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="missing-detections",
                expected={"left_spot": ExpectedPresence.OCCUPIED},
                detections=None,
            )
        ),
        config(),
    )

    frame = result.cases[0].scenarios[0].frames[0]
    assert result.cases[0].status is CaseStatus.BLOCKED
    assert frame.status is CaseStatus.BLOCKED
    assert result.metrics_by_spot["left_spot"].blocked == 1
    assert result.metrics_by_spot["left_spot"].tp == 0
    assert result.coverage.blocked_reasons == ["missing_detector_data"]
    assert result.shared_threshold_sufficiency.verdict is SharedThresholdVerdict.NOT_COVERED


def test_missing_bundle_manifest_blocks_case_without_pass_counts() -> None:
    result = evaluate_manifest(
        LabelManifest(
            cases=[
                {
                    "case_id": "case-missing-bundle",
                    "bundle_manifest_present": False,
                    "scenarios": [
                        {
                            "scenario_id": "scenario",
                            "frames": [
                                {
                                    "frame_id": "frame-1",
                                    "expected": {"left_spot": "occupied"},
                                    "detections": [
                                        {"class_name": "car", "confidence": 0.9, "bbox": (10, 10, 90, 90)}
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        ),
        config(),
    )

    assert result.cases[0].status is CaseStatus.BLOCKED
    assert result.cases[0].scenarios == []
    assert result.metrics_by_spot["left_spot"].blocked == 1
    assert result.metrics_by_spot["left_spot"].tp == 0
    assert result.coverage.blocked_reasons == ["missing_bundle_manifest"]


def test_unknown_configured_spot_id_blocks_frame() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="bad-spot",
                expected={"left_spot": ExpectedPresence.EMPTY, "curb_spot": ExpectedPresence.OCCUPIED},
                detections=[],
            )
        ),
        config(),
    )

    assert result.cases[0].status is CaseStatus.BLOCKED
    assert result.coverage.blocked_reasons == ["unknown_configured_spot_ids:curb_spot"]
    assert result.metrics_by_spot["left_spot"].blocked == 0


def test_replay_report_payload_and_markdown_include_safe_evidence_sections() -> None:
    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="frame-1",
                expected={"left_spot": ExpectedPresence.OCCUPIED, "right_spot": ExpectedPresence.EMPTY},
                detections=[detection((10, 10, 90, 90), confidence=0.36)],
            )
        ),
        config(),
    )

    report = build_replay_report(result, created_at="2026-05-10T00:00:00Z")
    assert report["schema_version"] == "parking-spot-monitor.replay-report.v1"
    assert report["status_counts"] == {"passed": 1, "failed": 0, "blocked": 0, "not_covered": 0}
    assert report["config_thresholds"]["confidence_threshold"] == 0.35
    assert report["metrics_by_spot"]["left_spot"]["tp"] == 1
    assert report["coverage"]["assessed_frames"] == 1
    assert report["shared_threshold_sufficiency"]["verdict"] == "sufficient"
    assert report["threshold_evidence"]["near_threshold_observations"][0]["spot_id"] == "left_spot"
    assert report["redaction_scan"]["passed"] is True
    json.dumps(report)

    markdown = render_replay_report_markdown(report)
    for section in [
        "## Summary Verdict",
        "## Per-Spot Confusion Metrics",
        "## Coverage Gaps",
        "## Blocked Cases",
        "## Event Findings",
        "## Threshold Evidence",
        "## Redaction Status",
    ]:
        assert section in markdown
    assert "left_spot" in markdown
    assert "Shared-threshold sufficiency: **sufficient**" in markdown


def test_replay_report_distinguishes_blocked_and_not_covered_cases() -> None:
    result = evaluate_manifest(
        LabelManifest(
            cases=[
                {
                    "case_id": "blocked-case",
                    "scenarios": [
                        {
                            "scenario_id": "blocked-scenario",
                            "frames": [{"frame_id": "missing", "expected": {"left_spot": "occupied"}, "detections": None}],
                        }
                    ],
                },
                {
                    "case_id": "not-covered-case",
                    "assessed": False,
                    "scenarios": [
                        {
                            "scenario_id": "not-covered-scenario",
                            "frames": [{"frame_id": "unknown", "expected": {"right_spot": "unknown"}, "detections": []}],
                        }
                    ],
                },
            ]
        ),
        config(),
    )

    report = build_replay_report(result)
    assert report["status_counts"] == {"passed": 0, "failed": 0, "blocked": 1, "not_covered": 1}
    assert report["cases"][0]["status"] == "blocked"
    assert report["cases"][0]["blocked_reasons"] == ["missing_detector_data"]
    assert report["cases"][1]["status"] == "not_covered"
    assert report["cases"][1]["not_covered_reasons"] == ["case_not_assessed"]
    assert report["shared_threshold_sufficiency"]["verdict"] == "inconclusive"


def test_replay_report_redaction_scan_fails_closed_for_private_or_binary_content() -> None:
    unsafe = "rtsp://user:pass@example.test/live Authorization: Bearer syt_secretsecret Traceback (most recent call last) /9j/"
    scan = scan_report_redactions(unsafe)
    assert scan["passed"] is False
    assert set(scan["findings"]) >= {"rtsp_url", "authorization_header", "matrix_token", "traceback", "image_bytes"}

    result = evaluate_manifest(
        manifest_with_frames(
            ReplayFrame(
                frame_id="unsafe-path",
                expected={"left_spot": ExpectedPresence.OCCUPIED},
                detections=[detection((10, 10, 90, 90))],
                snapshot_path="rtsp://camera/private/latest.jpg",
            )
        ),
        config(),
    )
    report = build_replay_report(result)
    assert report["redaction_scan"]["passed"] is False
    assert "rtsp_url" in report["redaction_scan"]["findings"]
    with pytest.raises(ReplayReportError, match="rendered Markdown contains unsafe content"):
        render_replay_report_markdown(report)


def test_replay_report_rejects_non_serializable_malformed_input() -> None:
    class NonSerializable:
        pass

    with pytest.raises(ReplayReportError, match="unsupported non-serializable value") as exc_info:
        build_replay_report({"cases": [], "metrics_by_spot": {"left_spot": NonSerializable()}})

    assert exc_info.value.diagnostics()["phase"] == "render_report"


def test_replay_tags_are_optional_normalized_and_do_not_change_outcomes() -> None:
    manifest = LabelManifest(
        cases=[
            {
                "case_id": "case-tags",
                "tags": ["real_capture", " Bottom Driveway ", "bottom_driveway", ""],
                "scenarios": [
                    {
                        "scenario_id": "passing",
                        "tags": ["passing traffic", "false_positive_probe", "threshold_decision"],
                        "frames": [
                            {
                                "frame_id": "frame-1",
                                "expected": {"left_spot": "occupied", "right_spot": "empty"},
                                "detections": [{"class_name": "car", "confidence": 0.9, "bbox": (10, 10, 90, 90)}],
                            }
                        ],
                    }
                ],
            }
        ]
    )

    result = evaluate_manifest(manifest, config())

    assert result.cases[0].tags == ["real_capture", "bottom_driveway"]
    assert result.cases[0].scenarios[0].tags == ["passing_traffic", "false_positive_probe", "threshold_decision"]
    assert result.metrics_by_spot["left_spot"].tp == 1
    assert result.metrics_by_spot["right_spot"].tn == 1

    report = build_replay_report(result)
    assert report["cases"][0]["tags"] == ["real_capture", "bottom_driveway"]
    assert report["cases"][0]["scenario_tags"] == {"passing": ["passing_traffic", "false_positive_probe", "threshold_decision"]}
    assert report["cases"][0]["frames"][0]["scenario_tags"] == ["passing_traffic", "false_positive_probe", "threshold_decision"]

    markdown = render_replay_report_markdown(report)
    assert "## Semantic Tags" in markdown
    assert "real_capture" in markdown
    assert "passing_traffic" in markdown


def test_replay_labels_without_tags_and_empty_tags_remain_valid() -> None:
    untagged = manifest_with_frames(
        ReplayFrame(frame_id="untagged", expected={"left_spot": ExpectedPresence.EMPTY}, detections=[])
    )
    empty_tagged = LabelManifest(
        cases=[
            {
                "case_id": "case-empty-tags",
                "tags": [],
                "scenarios": [
                    {
                        "scenario_id": "scenario-empty-tags",
                        "tags": [],
                        "frames": [{"frame_id": "frame", "expected": {"left_spot": "empty"}, "detections": []}],
                    }
                ],
            }
        ]
    )

    assert untagged.cases[0].tags == []
    assert untagged.cases[0].scenarios[0].tags == []
    assert empty_tagged.cases[0].tags == []
    assert empty_tagged.cases[0].scenarios[0].tags == []


def test_unknown_fields_still_fail_when_tags_are_supported() -> None:
    payload = {
        "cases": [
            {
                "case_id": "case-1",
                "tags": ["real_capture"],
                "unexpected": "not allowed",
                "scenarios": [
                    {
                        "scenario_id": "scenario",
                        "tags": ["passing_traffic"],
                        "frames": [
                            {
                                "frame_id": "frame",
                                "expected": {"left_spot": "empty"},
                                "detections": [],
                                "unexpected": "not allowed",
                            }
                        ],
                    }
                ],
            }
        ]
    }

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        LabelManifest.model_validate(payload)


@pytest.mark.parametrize(
    "payload,match",
    [
        ({"cases": [{"case_id": "", "scenarios": []}]}, "case_id"),
        (
            {
                "cases": [
                    {
                        "case_id": "case-1",
                        "scenarios": [{"scenario_id": "scenario", "frames": [{"frame_id": "frame", "expected": {}}]}],
                    }
                ]
            },
            "expected spot map",
        ),
        (
            {
                "cases": [
                    {
                        "case_id": "case-1",
                        "scenarios": [
                            {
                                "scenario_id": "scenario",
                                "frames": [
                                    {
                                        "frame_id": "frame",
                                        "expected": {"left_spot": "maybe"},
                                        "detections": [],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
            "Input should",
        ),
        (
            {
                "cases": [
                    {
                        "case_id": "case-1",
                        "scenarios": [
                            {
                                "scenario_id": "scenario",
                                "frames": [
                                    {
                                        "frame_id": "frame",
                                        "expected": {"left_spot": "occupied"},
                                        "detections": [{"class_name": "car", "confidence": 0.9, "bbox": (10, 10, 5, 20)}],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
            "bbox",
        ),
    ],
)
def test_malformed_manifest_validation_fails_without_silent_success(payload: dict, match: str) -> None:
    with pytest.raises(ValidationError, match=match):
        LabelManifest.model_validate(payload)
