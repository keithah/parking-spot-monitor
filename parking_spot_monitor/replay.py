from __future__ import annotations

import json
import re
from enum import StrEnum
from typing import Any, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator

from parking_spot_monitor.config import OccupancyConfig
from parking_spot_monitor.detection import (
    DetectionFilterResult,
    VehicleDetection,
    filter_spot_detections,
)
from parking_spot_monitor.occupancy import (
    OccupancyEventType,
    QuietWindowStatus,
    SpotOccupancyState,
    update_occupancy,
)


class StrictReplayModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=False)


class ExpectedPresence(StrEnum):
    OCCUPIED = "occupied"
    EMPTY = "empty"
    UNKNOWN = "unknown"
    NOT_VISIBLE = "not_visible"
    NOT_ASSESSED = "not_assessed"


class ObservationOutcome(StrEnum):
    TRUE_POSITIVE = "tp"
    TRUE_NEGATIVE = "tn"
    FALSE_POSITIVE = "fp"
    FALSE_NEGATIVE = "fn"
    NOT_ASSESSED = "not_assessed"
    BLOCKED = "blocked"


class CaseStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    BLOCKED = "blocked"
    NOT_ASSESSED = "not_assessed"


class SharedThresholdVerdict(StrEnum):
    SUFFICIENT = "sufficient"
    INSUFFICIENT = "insufficient"
    NOT_COVERED = "inconclusive"
    INCONCLUSIVE = "inconclusive"


class ReplayReportError(ValueError):
    """Safe report-rendering error that names the phase without leaking raw tracebacks."""

    def __init__(self, message: str, *, path: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.path = path

    def diagnostics(self) -> dict[str, str]:
        result = {"phase": "render_report", "message": self.message}
        if self.path is not None:
            result["path"] = self.path
        return result

class ReplayValidationError(ValueError):
    """Typed, safe replay validation error with case/path context."""

    def __init__(self, message: str, *, case_id: str | None = None, path: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.case_id = case_id
        self.path = path

    def diagnostics(self) -> dict[str, str]:
        result = {"phase": "replay_validation", "message": self.message}
        if self.case_id is not None:
            result["case_id"] = self.case_id
        if self.path is not None:
            result["path"] = self.path
        return result


class ReplayDetection(StrictReplayModel):
    class_name: str = Field(min_length=1)
    confidence: float = Field(ge=0, le=1)
    bbox: tuple[float, float, float, float]

    @field_validator("bbox")
    @classmethod
    def bbox_must_be_valid(cls, value: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
        # VehicleDetection reuses the same geometry validation as runtime filtering.
        VehicleDetection(class_name="car", confidence=1.0, bbox=value)
        return tuple(float(item) for item in value)

    def to_vehicle_detection(self) -> VehicleDetection:
        return VehicleDetection(class_name=self.class_name, confidence=self.confidence, bbox=self.bbox)


class ReplayFrame(StrictReplayModel):
    frame_id: str = Field(min_length=1)
    expected: dict[str, ExpectedPresence]
    detections: list[ReplayDetection] | None = None
    observed_at: str | int = Field(default=0)
    source_timestamp: str | None = None
    snapshot_path: str = "replay://snapshot"
    quiet_window_active: bool = False
    quiet_window_id: str | None = None

    @field_validator("expected")
    @classmethod
    def expected_must_not_be_empty(cls, value: dict[str, ExpectedPresence]) -> dict[str, ExpectedPresence]:
        if not value:
            raise ValueError("expected spot map must not be empty")
        return value


class ReplayScenario(StrictReplayModel):
    scenario_id: str = Field(min_length=1)
    tags: list[str] = Field(default_factory=list)
    frames: list[ReplayFrame]

    @field_validator("tags")
    @classmethod
    def tags_must_be_normalized(cls, value: list[str]) -> list[str]:
        return _normalize_tags(value)

    @field_validator("frames")
    @classmethod
    def frames_must_not_be_empty(cls, value: list[ReplayFrame]) -> list[ReplayFrame]:
        if not value:
            raise ValueError("scenario frames must not be empty")
        return value


class ReplayCase(StrictReplayModel):
    case_id: str = Field(min_length=1)
    tags: list[str] = Field(default_factory=list)
    bundle_manifest: str | None = None
    bundle_manifest_present: bool = True
    assessed: bool = True
    scenarios: list[ReplayScenario]

    @field_validator("tags")
    @classmethod
    def tags_must_be_normalized(cls, value: list[str]) -> list[str]:
        return _normalize_tags(value)

    @field_validator("scenarios")
    @classmethod
    def scenarios_must_not_be_empty(cls, value: list[ReplayScenario]) -> list[ReplayScenario]:
        if not value:
            raise ValueError("case scenarios must not be empty")
        return value


class LabelManifest(StrictReplayModel):
    schema_version: str = "parking-spot-monitor.replay.v1"
    cases: list[ReplayCase]


class ReplayEvaluationConfig(StrictReplayModel):
    spots: dict[str, list[tuple[float, float]]]
    allowed_classes: list[str]
    confidence_threshold: float = Field(ge=0, le=1)
    min_bbox_area_px: float = Field(gt=0)
    min_polygon_overlap_ratio: float = Field(ge=0, le=1)
    occupancy: OccupancyConfig

    @field_validator("spots")
    @classmethod
    def spots_must_have_polygons(cls, value: dict[str, list[tuple[float, float]]]) -> dict[str, list[tuple[float, float]]]:
        if not value:
            raise ValueError("at least one configured spot is required")
        for spot_id, polygon in value.items():
            if not spot_id:
                raise ValueError("spot ids must not be empty")
            if len(polygon) < 3:
                raise ValueError(f"spot {spot_id} polygon must contain at least 3 points")
        return value

    @field_validator("allowed_classes")
    @classmethod
    def allowed_classes_must_not_be_empty(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("allowed_classes must not be empty")
        return value


class SpotObservation(StrictReplayModel):
    spot_id: str
    expected: ExpectedPresence
    predicted_occupied: bool | None
    outcome: ObservationOutcome
    reason: str | None = None
    accepted_detection: dict[str, Any] | None = None


class FrameResult(StrictReplayModel):
    frame_id: str
    status: CaseStatus
    observations: list[SpotObservation]
    rejection_counts: dict[str, int]
    blocked_reasons: list[str] = Field(default_factory=list)


class EventFinding(StrictReplayModel):
    scenario_id: str
    frame_id: str
    spot_id: str
    event_type: str
    expected: ExpectedPresence | None = None
    finding: str
    suppressed_reason: str | None = None


class SpotMetrics(StrictReplayModel):
    spot_id: str
    tp: int = 0
    tn: int = 0
    fp: int = 0
    fn: int = 0
    not_assessed: int = 0
    blocked: int = 0


class CoverageSummary(StrictReplayModel):
    assessed_frames: int = 0
    blocked_frames: int = 0
    not_assessed_frames: int = 0
    blocked_reasons: list[str] = Field(default_factory=list)
    not_covered_reasons: list[str] = Field(default_factory=list)


class SharedThresholdSufficiency(StrictReplayModel):
    verdict: SharedThresholdVerdict
    rationale: str
    thresholds: dict[str, Any]


class ScenarioResult(StrictReplayModel):
    scenario_id: str
    tags: list[str] = Field(default_factory=list)
    status: CaseStatus
    frames: list[FrameResult]
    event_findings: list[EventFinding]


class CaseResult(StrictReplayModel):
    case_id: str
    tags: list[str] = Field(default_factory=list)
    scenario_tags: dict[str, list[str]] = Field(default_factory=dict)
    status: CaseStatus
    scenarios: list[ScenarioResult]
    metrics_by_spot: dict[str, SpotMetrics]
    coverage: CoverageSummary


class ReplayResult(StrictReplayModel):
    schema_version: str
    config_thresholds: dict[str, Any]
    cases: list[CaseResult]
    metrics_by_spot: dict[str, SpotMetrics]
    coverage: CoverageSummary
    shared_threshold_sufficiency: SharedThresholdSufficiency
    redaction_scan: dict[str, str | bool] = Field(default_factory=lambda: {"passed": True, "reason": "no_raw_images_or_secret_values_in_replay_result"})

    def to_jsonable(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


_REPORT_SCHEMA_VERSION = "parking-spot-monitor.replay-report.v1"
_REDACTION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("rtsp_url", re.compile(r"rtsp://[^\s)\]}>\"]+", re.IGNORECASE)),
    ("authorization_header", re.compile(r"authorization\s*[:=]\s*(bearer|basic)\s+", re.IGNORECASE)),
    ("matrix_token", re.compile(r"\b(syt_[A-Za-z0-9_\-]{8,}|MATRIX_ACCESS_TOKEN|access_token\b|Bearer\s+[A-Za-z0-9._\-]{12,})", re.IGNORECASE)),
    ("raw_matrix_response", re.compile(r"\b(errcode|event_id|room_id|m\.room\.message|mxc://)\b", re.IGNORECASE)),
    ("traceback", re.compile(r"Traceback \(most recent call last\)|File \".+\", line \d+", re.IGNORECASE)),
    ("image_bytes", re.compile(r"(/9j/|iVBORw0KGgo|data:image/|JFIF|Exif|\xff\xd8\xff)", re.IGNORECASE)),
)


def build_replay_report(result: ReplayResult | Mapping[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    """Build a deterministic, publication-safe replay report payload.

    The builder is pure: it serializes the already-computed replay result, adds
    report-level summaries for downstream tuning, and records a redaction scan
    over the rendered JSON payload. It never opens image paths referenced by the
    result.
    """

    jsonable = _jsonable(result, path="result")
    if not isinstance(jsonable, dict):
        raise ReplayReportError("replay result must serialize to an object", path="result")

    cases = jsonable.get("cases")
    if not isinstance(cases, list):
        raise ReplayReportError("replay result missing cases list", path="result.cases")

    payload: dict[str, Any] = {
        "schema_version": _REPORT_SCHEMA_VERSION,
        "replay_schema_version": jsonable.get("schema_version"),
        "created_at": created_at,
        "config_thresholds": jsonable.get("config_thresholds", {}),
        "spot_ids": sorted((jsonable.get("metrics_by_spot") or {}).keys()),
        "status_counts": _status_counts(cases),
        "cases": [_case_summary(case, index) for index, case in enumerate(cases)],
        "metrics_by_spot": jsonable.get("metrics_by_spot", {}),
        "coverage": _coverage_summary(jsonable.get("coverage", {})),
        "event_findings": _event_findings_summary(cases),
        "threshold_evidence": _threshold_evidence(jsonable),
        "shared_threshold_sufficiency": _normalize_threshold_sufficiency(jsonable.get("shared_threshold_sufficiency", {})),
    }
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    payload["redaction_scan"] = scan_report_redactions(rendered)
    return payload


def render_replay_report_markdown(report: Mapping[str, Any]) -> str:
    """Render a concise human-readable Markdown replay report."""

    jsonable = _jsonable(report, path="report")
    if not isinstance(jsonable, dict):
        raise ReplayReportError("report must serialize to an object", path="report")

    threshold = jsonable.get("shared_threshold_sufficiency", {}) or {}
    coverage = jsonable.get("coverage", {}) or {}
    redaction = jsonable.get("redaction_scan", {}) or {}
    if redaction.get("passed") is False:
        raise ReplayReportError("rendered Markdown contains unsafe content", path=",".join(redaction.get("findings", [])))
    lines = [
        "# Replay Report",
        "",
        "## Summary Verdict",
        f"- Schema version: `{jsonable.get('schema_version', 'unknown')}`",
        f"- Replay schema version: `{jsonable.get('replay_schema_version', 'unknown')}`",
        f"- Shared-threshold sufficiency: **{threshold.get('verdict', 'unknown')}** — {threshold.get('rationale', 'no rationale')}",
        f"- Status counts: {_inline_json(jsonable.get('status_counts', {}))}",
        "",
        "## Config Thresholds",
        f"```json\n{json.dumps(jsonable.get('config_thresholds', {}), indent=2, sort_keys=True)}\n```",
        "",
        "## Per-Spot Confusion Metrics",
        "| Spot | TP | TN | FP | FN | Blocked | Not covered |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    metrics = jsonable.get("metrics_by_spot", {}) or {}
    for spot_id in sorted(metrics):
        metric = metrics[spot_id] or {}
        lines.append(
            f"| `{spot_id}` | {metric.get('tp', 0)} | {metric.get('tn', 0)} | {metric.get('fp', 0)} | {metric.get('fn', 0)} | {metric.get('blocked', 0)} | {metric.get('not_assessed', 0)} |"
        )

    lines.extend(
        [
            "",
            "## Coverage Gaps",
            f"- Assessed frames: {coverage.get('assessed_frames', 0)}",
            f"- Blocked frames: {coverage.get('blocked_frames', 0)}",
            f"- Not-covered frames: {coverage.get('not_assessed_frames', 0)}",
            f"- Blocked reasons: {_comma_list(coverage.get('blocked_reasons', []))}",
            f"- Not-covered reasons: {_comma_list(coverage.get('not_covered_reasons', []))}",
        ]
    )
    lines.extend(["", "## Semantic Tags"])
    tagged_cases = [case for case in jsonable.get("cases", []) if case.get("tags") or any(case.get("scenario_tags", {}).values())]
    if tagged_cases:
        for case in tagged_cases:
            scenario_bits = [
                f"{scenario_id}: {_comma_list(tags)}"
                for scenario_id, tags in sorted((case.get("scenario_tags") or {}).items())
                if tags
            ]
            detail = f"; scenarios: {'; '.join(scenario_bits)}" if scenario_bits else ""
            lines.append(f"- `{case.get('case_id')}`: {_comma_list(case.get('tags', []))}{detail}")
    else:
        lines.append("- None")

    lines.extend(["", "## Blocked Cases"])
    blocked_cases = [case for case in jsonable.get("cases", []) if case.get("status") == "blocked"]
    if blocked_cases:
        for case in blocked_cases:
            lines.append(f"- `{case.get('case_id')}`: {_comma_list(case.get('blocked_reasons', []))}")
    else:
        lines.append("- None")

    lines.extend(["", "## Event Findings"])
    findings = jsonable.get("event_findings", [])
    if findings:
        for finding in findings:
            lines.append(
                f"- `{finding.get('case_id')}/{finding.get('scenario_id')}/{finding.get('frame_id')}` `{finding.get('spot_id')}`: {finding.get('event_type')} — {finding.get('finding')}"
            )
    else:
        lines.append("- None")

    evidence = jsonable.get("threshold_evidence", {}) or {}
    lines.extend(
        [
            "",
            "## Threshold Evidence",
            f"- Spots with counted coverage: {_comma_list(evidence.get('spots_with_counted_coverage', []))}",
            f"- Spots with errors: {_comma_list(evidence.get('spots_with_errors', []))}",
            f"- Near-threshold observations: {_inline_json(evidence.get('near_threshold_observations', []))}",
            "",
            "## Redaction Status",
            f"- Passed: {redaction.get('passed', False)}",
            f"- Findings: {_comma_list(redaction.get('findings', []))}",
        ]
    )
    markdown = "\n".join(lines) + "\n"
    final_scan = scan_report_redactions(markdown)
    if not final_scan["passed"]:
        raise ReplayReportError("rendered Markdown contains unsafe content", path=",".join(final_scan["findings"]))
    return markdown


def scan_report_redactions(rendered_text: str) -> dict[str, Any]:
    """Fail closed when rendered report text contains private or binary-looking content."""

    if not isinstance(rendered_text, str):
        raise ReplayReportError("redaction scan input must be text", path="rendered_text")
    findings = [name for name, pattern in _REDACTION_PATTERNS if pattern.search(rendered_text)]
    return {
        "passed": not findings,
        "findings": findings,
        "reason": "no_forbidden_report_content" if not findings else "forbidden_report_content_detected",
    }


render_replay_report_json = build_replay_report
render_replay_report_markdown_safe = render_replay_report_markdown


_EMPTY_EXPECTATIONS = {ExpectedPresence.UNKNOWN, ExpectedPresence.NOT_VISIBLE, ExpectedPresence.NOT_ASSESSED}


def _normalize_tags(value: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        tag = item.strip().lower().replace(" ", "_")
        if not tag:
            continue
        if tag not in seen:
            normalized.append(tag)
            seen.add(tag)
    return normalized


def _jsonable(value: Any, *, path: str) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item, path=f"{path}.{key}") for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item, path=f"{path}[]") for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise ReplayReportError(f"unsupported non-serializable value type {type(value).__name__}", path=path)


def _status_counts(cases: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"passed": 0, "failed": 0, "blocked": 0, "not_covered": 0}
    for case in cases:
        status = str(case.get("status", "not_covered"))
        if status == "not_assessed":
            status = "not_covered"
        counts[status if status in counts else "not_covered"] += 1
    return counts


def _case_summary(case: Mapping[str, Any], index: int) -> dict[str, Any]:
    if not isinstance(case, Mapping):
        raise ReplayReportError("case result must be an object", path=f"result.cases[{index}]")
    coverage = case.get("coverage", {}) if isinstance(case.get("coverage", {}), Mapping) else {}
    status = case.get("status", "not_covered")
    if status == "not_assessed":
        status = "not_covered"
    return {
        "case_id": case.get("case_id", f"case-{index}"),
        "tags": _normalize_tags(case.get("tags", [])),
        "status": status,
        "scenario_ids": [scenario.get("scenario_id") for scenario in case.get("scenarios", []) if isinstance(scenario, Mapping)],
        "scenario_tags": _scenario_tags_summary(case),
        "frames": _frame_summaries(case.get("scenarios", [])),
        "metrics_by_spot": case.get("metrics_by_spot", {}),
        "coverage": _coverage_summary(coverage),
        "blocked_reasons": sorted(set(coverage.get("blocked_reasons", []))),
        "not_covered_reasons": sorted(set(coverage.get("not_covered_reasons", []))),
    }


def _scenario_tags_summary(case: Mapping[str, Any]) -> dict[str, list[str]]:
    explicit = case.get("scenario_tags")
    if isinstance(explicit, Mapping):
        return {str(scenario_id): _normalize_tags(tags) for scenario_id, tags in explicit.items()}
    return {
        str(scenario.get("scenario_id")): _normalize_tags(scenario.get("tags", []))
        for scenario in case.get("scenarios", [])
        if isinstance(scenario, Mapping) and scenario.get("scenario_id") is not None
    }


def _frame_summaries(scenarios: Any) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    if not isinstance(scenarios, list):
        return summaries
    for scenario in scenarios:
        if not isinstance(scenario, Mapping):
            continue
        for frame in scenario.get("frames", []):
            if not isinstance(frame, Mapping):
                continue
            summaries.append(
                {
                    "scenario_id": scenario.get("scenario_id"),
                    "scenario_tags": _normalize_tags(scenario.get("tags", [])),
                    "frame_id": frame.get("frame_id"),
                    "status": "not_covered" if frame.get("status") == "not_assessed" else frame.get("status"),
                    "blocked_reasons": frame.get("blocked_reasons", []),
                    "rejection_counts": frame.get("rejection_counts", {}),
                    "observations": frame.get("observations", []),
                }
            )
    return summaries


def _coverage_summary(coverage: Any) -> dict[str, Any]:
    if not isinstance(coverage, Mapping):
        return {"assessed_frames": 0, "blocked_frames": 0, "not_assessed_frames": 0, "blocked_reasons": [], "not_covered_reasons": []}
    return {
        "assessed_frames": int(coverage.get("assessed_frames", 0)),
        "blocked_frames": int(coverage.get("blocked_frames", 0)),
        "not_assessed_frames": int(coverage.get("not_assessed_frames", 0)),
        "blocked_reasons": sorted(set(str(reason) for reason in coverage.get("blocked_reasons", []))),
        "not_covered_reasons": sorted(set(str(reason) for reason in coverage.get("not_covered_reasons", []))),
    }


def _event_findings_summary(cases: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for case in cases:
        case_id = case.get("case_id")
        for scenario in case.get("scenarios", []):
            if not isinstance(scenario, Mapping):
                continue
            for finding in scenario.get("event_findings", []):
                if not isinstance(finding, Mapping):
                    continue
                item = dict(finding)
                item["case_id"] = case_id
                findings.append(item)
    return findings


def _threshold_evidence(result: Mapping[str, Any]) -> dict[str, Any]:
    metrics = result.get("metrics_by_spot", {}) if isinstance(result.get("metrics_by_spot", {}), Mapping) else {}
    spots_with_counted = []
    spots_with_errors = []
    for spot_id, metric in metrics.items():
        if not isinstance(metric, Mapping):
            continue
        if any(int(metric.get(key, 0)) for key in ("tp", "tn", "fp", "fn")):
            spots_with_counted.append(spot_id)
        if any(int(metric.get(key, 0)) for key in ("fp", "fn")):
            spots_with_errors.append(spot_id)
    return {
        "spots_with_counted_coverage": sorted(spots_with_counted),
        "spots_with_errors": sorted(spots_with_errors),
        "near_threshold_observations": _near_threshold_observations(result),
    }


def _near_threshold_observations(result: Mapping[str, Any]) -> list[dict[str, Any]]:
    threshold = (result.get("config_thresholds", {}) or {}).get("confidence_threshold")
    if not isinstance(threshold, (int, float)):
        return []
    observations: list[dict[str, Any]] = []
    for case in result.get("cases", []):
        for scenario in case.get("scenarios", []):
            for frame in scenario.get("frames", []):
                for observation in frame.get("observations", []):
                    accepted = observation.get("accepted_detection") if isinstance(observation, Mapping) else None
                    if not isinstance(accepted, Mapping):
                        continue
                    confidence = accepted.get("confidence")
                    if isinstance(confidence, (int, float)) and abs(confidence - threshold) <= 0.05:
                        observations.append(
                            {
                                "case_id": case.get("case_id"),
                                "scenario_id": scenario.get("scenario_id"),
                                "frame_id": frame.get("frame_id"),
                                "spot_id": observation.get("spot_id"),
                                "confidence": confidence,
                                "threshold": threshold,
                            }
                        )
    return observations[:20]


def _normalize_threshold_sufficiency(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {"verdict": "inconclusive", "rationale": "missing threshold sufficiency evidence", "thresholds": {}}
    verdict = value.get("verdict", "inconclusive")
    if verdict == "not_covered":
        verdict = "inconclusive"
    return {
        "verdict": verdict,
        "rationale": value.get("rationale", "missing threshold sufficiency rationale"),
        "thresholds": value.get("thresholds", {}),
    }


def _inline_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _comma_list(values: Any) -> str:
    if not values:
        return "None"
    if isinstance(values, str):
        return values
    return ", ".join(f"`{value}`" for value in values)


def evaluate_manifest(manifest: LabelManifest | Mapping[str, Any], config: ReplayEvaluationConfig | Mapping[str, Any]) -> ReplayResult:
    """Evaluate a publication-safe manifest through runtime detection and occupancy primitives."""

    parsed_manifest = manifest if isinstance(manifest, LabelManifest) else LabelManifest.model_validate(manifest)
    parsed_config = config if isinstance(config, ReplayEvaluationConfig) else ReplayEvaluationConfig.model_validate(config)
    configured_spot_ids = tuple(parsed_config.spots.keys())

    case_results: list[CaseResult] = []
    total_metrics = _new_metrics(configured_spot_ids)
    total_coverage = CoverageSummary()

    for case in parsed_manifest.cases:
        case_result = _evaluate_case(case, parsed_config, configured_spot_ids)
        case_results.append(case_result)
        _merge_metrics(total_metrics, case_result.metrics_by_spot)
        _merge_coverage(total_coverage, case_result.coverage)

    return ReplayResult(
        schema_version=parsed_manifest.schema_version,
        config_thresholds=_threshold_summary(parsed_config),
        cases=case_results,
        metrics_by_spot=total_metrics,
        coverage=total_coverage,
        shared_threshold_sufficiency=_shared_threshold_sufficiency(total_metrics, total_coverage, parsed_config),
    )


def _evaluate_case(case: ReplayCase, config: ReplayEvaluationConfig, configured_spot_ids: tuple[str, ...]) -> CaseResult:
    metrics = _new_metrics(configured_spot_ids)
    coverage = CoverageSummary()
    scenario_results: list[ScenarioResult] = []

    if not case.bundle_manifest_present:
        reason = "missing_bundle_manifest"
        coverage.blocked_reasons.append(reason)
        _block_all_expected_spots(case, metrics, coverage)
        return CaseResult(case_id=case.case_id, tags=case.tags, scenario_tags=_scenario_tags_from_case(case), status=CaseStatus.BLOCKED, scenarios=[], metrics_by_spot=metrics, coverage=coverage)

    if not case.assessed:
        coverage.not_covered_reasons.append("case_not_assessed")
        for scenario in case.scenarios:
            for frame in scenario.frames:
                coverage.not_assessed_frames += 1
                for spot_id in frame.expected:
                    if spot_id in metrics:
                        metrics[spot_id].not_assessed += 1
        return CaseResult(case_id=case.case_id, tags=case.tags, scenario_tags=_scenario_tags_from_case(case), status=CaseStatus.NOT_ASSESSED, scenarios=[], metrics_by_spot=metrics, coverage=coverage)

    for scenario in case.scenarios:
        scenario_results.append(_evaluate_scenario(scenario, config, configured_spot_ids, metrics, coverage))

    status = _status_from_counts(metrics, coverage)
    return CaseResult(case_id=case.case_id, tags=case.tags, scenario_tags=_scenario_tags_from_case(case), status=status, scenarios=scenario_results, metrics_by_spot=metrics, coverage=coverage)


def _scenario_tags_from_case(case: ReplayCase) -> dict[str, list[str]]:
    return {scenario.scenario_id: scenario.tags for scenario in case.scenarios}


def _evaluate_scenario(
    scenario: ReplayScenario,
    config: ReplayEvaluationConfig,
    configured_spot_ids: tuple[str, ...],
    metrics: dict[str, SpotMetrics],
    coverage: CoverageSummary,
) -> ScenarioResult:
    state_by_spot: dict[str, SpotOccupancyState] = {}
    frame_results: list[FrameResult] = []
    event_findings: list[EventFinding] = []

    for frame in scenario.frames:
        unknown_spot_ids = sorted(set(frame.expected) - set(configured_spot_ids))
        if unknown_spot_ids:
            reason = "unknown_configured_spot_ids:" + ",".join(unknown_spot_ids)
            frame_results.append(_blocked_frame(frame, configured_spot_ids, metrics, coverage, reason))
            continue

        if frame.detections is None:
            frame_results.append(_blocked_frame(frame, configured_spot_ids, metrics, coverage, "missing_detector_data"))
            continue

        filter_result = _filter_frame(frame, config)
        candidates_by_spot = {spot_id: filter_result.by_spot[spot_id].accepted for spot_id in configured_spot_ids}
        frame_results.append(_score_frame(frame, candidates_by_spot, filter_result, configured_spot_ids, metrics, coverage))

        occupancy_update = update_occupancy(
            previous_state=state_by_spot,
            candidates_by_spot=candidates_by_spot,
            occupancy_config=config.occupancy,
            observed_at=frame.observed_at,
            quiet_window_status=QuietWindowStatus(active=frame.quiet_window_active, window_id=frame.quiet_window_id),
            snapshot_path=frame.snapshot_path,
            configured_spot_ids=configured_spot_ids,
        )
        state_by_spot = occupancy_update.state_by_spot
        event_findings.extend(_event_findings(scenario.scenario_id, frame, occupancy_update.events))

    return ScenarioResult(
        scenario_id=scenario.scenario_id,
        tags=scenario.tags,
        status=_scenario_status(frame_results),
        frames=frame_results,
        event_findings=event_findings,
    )


def _filter_frame(frame: ReplayFrame, config: ReplayEvaluationConfig) -> DetectionFilterResult:
    return filter_spot_detections(
        [detection.to_vehicle_detection() for detection in frame.detections or []],
        spots=config.spots,
        allowed_classes=config.allowed_classes,
        confidence_threshold=config.confidence_threshold,
        min_bbox_area_px=config.min_bbox_area_px,
        min_polygon_overlap_ratio=config.min_polygon_overlap_ratio,
        source_frame_path=frame.snapshot_path,
        source_timestamp=frame.source_timestamp,
    )


def _score_frame(
    frame: ReplayFrame,
    candidates_by_spot: Mapping[str, Any],
    filter_result: DetectionFilterResult,
    configured_spot_ids: tuple[str, ...],
    metrics: dict[str, SpotMetrics],
    coverage: CoverageSummary,
) -> FrameResult:
    observations: list[SpotObservation] = []
    has_counted_observation = False
    has_failure = False

    for spot_id in configured_spot_ids:
        expected = frame.expected.get(spot_id, ExpectedPresence.NOT_ASSESSED)
        candidate = candidates_by_spot.get(spot_id)
        predicted_occupied = candidate is not None
        outcome = _outcome(expected, predicted_occupied)
        if outcome in {ObservationOutcome.TRUE_POSITIVE, ObservationOutcome.TRUE_NEGATIVE}:
            has_counted_observation = True
        if outcome in {ObservationOutcome.FALSE_POSITIVE, ObservationOutcome.FALSE_NEGATIVE}:
            has_counted_observation = True
            has_failure = True
        _increment_metric(metrics[spot_id], outcome)
        observations.append(
            SpotObservation(
                spot_id=spot_id,
                expected=expected,
                predicted_occupied=None if outcome is ObservationOutcome.NOT_ASSESSED else predicted_occupied,
                outcome=outcome,
                reason=None if outcome is not ObservationOutcome.NOT_ASSESSED else "expected_label_not_assessed",
                accepted_detection=_candidate_summary(candidate),
            )
        )

    if has_counted_observation:
        coverage.assessed_frames += 1
    else:
        coverage.not_assessed_frames += 1

    return FrameResult(
        frame_id=frame.frame_id,
        status=CaseStatus.FAILED if has_failure else (CaseStatus.PASSED if has_counted_observation else CaseStatus.NOT_ASSESSED),
        observations=observations,
        rejection_counts={reason.value: count for reason, count in filter_result.rejection_counts.items()},
    )


def _blocked_frame(
    frame: ReplayFrame,
    configured_spot_ids: tuple[str, ...],
    metrics: dict[str, SpotMetrics],
    coverage: CoverageSummary,
    reason: str,
) -> FrameResult:
    observations: list[SpotObservation] = []
    coverage.blocked_frames += 1
    coverage.blocked_reasons.append(reason)
    for spot_id in configured_spot_ids:
        expected = frame.expected.get(spot_id, ExpectedPresence.NOT_ASSESSED)
        if reason.startswith("unknown_configured_spot_ids:"):
            metrics[spot_id].not_assessed += 1
        elif expected not in _EMPTY_EXPECTATIONS:
            metrics[spot_id].blocked += 1
        else:
            metrics[spot_id].not_assessed += 1
        outcome = ObservationOutcome.NOT_ASSESSED
        if expected not in _EMPTY_EXPECTATIONS:
            outcome = ObservationOutcome.BLOCKED
        if reason.startswith("unknown_configured_spot_ids:"):
            outcome = ObservationOutcome.NOT_ASSESSED
        observations.append(
            SpotObservation(
                spot_id=spot_id,
                expected=expected,
                predicted_occupied=None,
                outcome=outcome,
                reason=reason,
            )
        )
    return FrameResult(frame_id=frame.frame_id, status=CaseStatus.BLOCKED, observations=observations, rejection_counts={}, blocked_reasons=[reason])


def _block_all_expected_spots(case: ReplayCase, metrics: dict[str, SpotMetrics], coverage: CoverageSummary) -> None:
    for scenario in case.scenarios:
        for frame in scenario.frames:
            coverage.blocked_frames += 1
            for spot_id, expected in frame.expected.items():
                if spot_id in metrics and expected not in _EMPTY_EXPECTATIONS:
                    metrics[spot_id].blocked += 1


def _outcome(expected: ExpectedPresence, predicted_occupied: bool) -> ObservationOutcome:
    if expected is ExpectedPresence.OCCUPIED:
        return ObservationOutcome.TRUE_POSITIVE if predicted_occupied else ObservationOutcome.FALSE_NEGATIVE
    if expected is ExpectedPresence.EMPTY:
        return ObservationOutcome.FALSE_POSITIVE if predicted_occupied else ObservationOutcome.TRUE_NEGATIVE
    return ObservationOutcome.NOT_ASSESSED


def _increment_metric(metric: SpotMetrics, outcome: ObservationOutcome) -> None:
    if outcome is ObservationOutcome.TRUE_POSITIVE:
        metric.tp += 1
    elif outcome is ObservationOutcome.TRUE_NEGATIVE:
        metric.tn += 1
    elif outcome is ObservationOutcome.FALSE_POSITIVE:
        metric.fp += 1
    elif outcome is ObservationOutcome.FALSE_NEGATIVE:
        metric.fn += 1
    elif outcome is ObservationOutcome.BLOCKED:
        metric.blocked += 1
    else:
        metric.not_assessed += 1


def _event_findings(scenario_id: str, frame: ReplayFrame, events: Sequence[Any]) -> list[EventFinding]:
    findings: list[EventFinding] = []
    for event in events:
        expected = frame.expected.get(event.spot_id)
        if expected is None:
            continue
        if event.event_type is OccupancyEventType.OPEN_EVENT:
            finding = "expected_open_event" if expected is ExpectedPresence.EMPTY else "unexpected_open_event"
        elif event.event_type is OccupancyEventType.OPEN_SUPPRESSED:
            finding = "quiet_window_open_suppressed"
        else:
            finding = "state_transition"
        findings.append(
            EventFinding(
                scenario_id=scenario_id,
                frame_id=frame.frame_id,
                spot_id=event.spot_id,
                event_type=event.event_type.value,
                expected=expected,
                finding=finding,
                suppressed_reason=event.suppressed_reason,
            )
        )
    return findings


def _scenario_status(frames: Sequence[FrameResult]) -> CaseStatus:
    if any(frame.status is CaseStatus.BLOCKED for frame in frames):
        return CaseStatus.BLOCKED
    if any(frame.status is CaseStatus.FAILED for frame in frames):
        return CaseStatus.FAILED
    if all(frame.status is CaseStatus.NOT_ASSESSED for frame in frames):
        return CaseStatus.NOT_ASSESSED
    return CaseStatus.PASSED


def _status_from_counts(metrics: Mapping[str, SpotMetrics], coverage: CoverageSummary) -> CaseStatus:
    if coverage.blocked_frames or any(metric.blocked for metric in metrics.values()):
        return CaseStatus.BLOCKED
    if any(metric.fp or metric.fn for metric in metrics.values()):
        return CaseStatus.FAILED
    if not any(metric.tp or metric.tn for metric in metrics.values()):
        return CaseStatus.NOT_ASSESSED
    return CaseStatus.PASSED


def _new_metrics(spot_ids: Sequence[str]) -> dict[str, SpotMetrics]:
    return {spot_id: SpotMetrics(spot_id=spot_id) for spot_id in spot_ids}


def _merge_metrics(total: dict[str, SpotMetrics], item: Mapping[str, SpotMetrics]) -> None:
    for spot_id, metric in item.items():
        target = total.setdefault(spot_id, SpotMetrics(spot_id=spot_id))
        target.tp += metric.tp
        target.tn += metric.tn
        target.fp += metric.fp
        target.fn += metric.fn
        target.not_assessed += metric.not_assessed
        target.blocked += metric.blocked


def _merge_coverage(total: CoverageSummary, item: CoverageSummary) -> None:
    total.assessed_frames += item.assessed_frames
    total.blocked_frames += item.blocked_frames
    total.not_assessed_frames += item.not_assessed_frames
    total.blocked_reasons.extend(item.blocked_reasons)
    total.not_covered_reasons.extend(item.not_covered_reasons)


def _shared_threshold_sufficiency(
    metrics: Mapping[str, SpotMetrics],
    coverage: CoverageSummary,
    config: ReplayEvaluationConfig,
) -> SharedThresholdSufficiency:
    counted_by_spot = {spot_id: metric.tp + metric.tn + metric.fp + metric.fn for spot_id, metric in metrics.items()}
    error_by_spot = {spot_id: metric.fp + metric.fn for spot_id, metric in metrics.items()}
    covered_spots = [spot_id for spot_id, count in counted_by_spot.items() if count]
    error_spots = [spot_id for spot_id, count in error_by_spot.items() if count]

    if coverage.blocked_frames:
        verdict = SharedThresholdVerdict.NOT_COVERED
        rationale = "blocked replay frames prevent shared-threshold sufficiency assessment"
    elif len(covered_spots) < len(metrics):
        verdict = SharedThresholdVerdict.NOT_COVERED
        rationale = "one or more configured spots lack counted occupied/empty coverage"
    elif error_spots and len(error_spots) < len(covered_spots):
        verdict = SharedThresholdVerdict.INSUFFICIENT
        rationale = "observed false-positive or false-negative errors diverge by spot under the same global thresholds"
    elif error_spots:
        verdict = SharedThresholdVerdict.NOT_COVERED
        rationale = "shared thresholds produced errors, but evidence does not isolate a spot-specific divergence"
    elif not covered_spots:
        verdict = SharedThresholdVerdict.NOT_COVERED
        rationale = "manifest contains no counted occupied/empty observations"
    else:
        verdict = SharedThresholdVerdict.SUFFICIENT
        rationale = "all counted observations passed with shared detection thresholds"
    return SharedThresholdSufficiency(verdict=verdict, rationale=rationale, thresholds=_threshold_summary(config))


def _threshold_summary(config: ReplayEvaluationConfig) -> dict[str, Any]:
    return {
        "allowed_classes": list(config.allowed_classes),
        "confidence_threshold": config.confidence_threshold,
        "min_bbox_area_px": config.min_bbox_area_px,
        "min_polygon_overlap_ratio": config.min_polygon_overlap_ratio,
        "occupancy": config.occupancy.model_dump(mode="json"),
    }


def _candidate_summary(candidate: Any | None) -> dict[str, Any] | None:
    if candidate is None:
        return None
    return {
        "class_name": candidate.class_name,
        "confidence": candidate.confidence,
        "bbox": tuple(candidate.bbox),
        "bbox_area_px": candidate.bbox_area_px,
        "centroid": tuple(candidate.centroid),
        "overlap_ratio": candidate.overlap_ratio,
        "source_frame_path": candidate.source_frame_path,
    }
