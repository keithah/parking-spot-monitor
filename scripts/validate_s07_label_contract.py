#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parking_spot_monitor.replay import LabelManifest, scan_report_redactions

REQUIRED_STRICT_TAGS = ("real_capture", "bottom_driveway", "passing_traffic", "threshold_decision")
REQUIRED_SPOT_IDS = ("left_spot", "right_spot")
GAP_TAGS = ("insufficient_bbox_detail", "insufficient_real_semantic_coverage")
PRIVATE_ARTIFACT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("raw_latest_frame", re.compile(r"(?:^|[/\\])latest\.jpe?g\b", re.IGNORECASE)),
    ("raw_debug_frame", re.compile(r"(?:^|[/\\])debug_latest\.jpe?g\b", re.IGNORECASE)),
    ("rtsp_env_assignment", re.compile(r"\bRTSP_URL\s*=", re.IGNORECASE)),
    ("matrix_env_assignment", re.compile(r"\bMATRIX_ACCESS_TOKEN\s*=", re.IGNORECASE)),
)


class LabelContractError(Exception):
    """Safe operator-facing contract validation error."""

    def __init__(self, code: str, message: str, *, path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.path = path

    def reason(self) -> str:
        if self.path:
            return f"{self.code}: {self.message} ({self.path})"
        return f"{self.code}: {self.message}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate that S07/S10 replay labels either satisfy the strict real-evidence contract or fail closed safely."
    )
    parser.add_argument("--labels", required=True, help="Path to real-traffic-labels.yaml")
    parser.add_argument(
        "--allow-blocked",
        action="store_true",
        help="Exit 0 when the manifest is schema-valid, publication-safe, and explicitly blocked for missing strict evidence.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = validate_label_contract(Path(args.labels))
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    if report["status"] == "passed":
        return 0
    if args.allow_blocked and report["status"] == "blocked":
        return 0
    return 1


def validate_label_contract(labels_path: Path) -> dict[str, Any]:
    blockers: list[str] = []
    gaps: list[str] = []
    warnings: list[str] = []
    manifest: LabelManifest | None = None

    try:
        _load_yaml_mapping(labels_path)
        _validate_publication_boundary(labels_path.read_text(encoding="utf-8"))
        loaded = yaml.safe_load(labels_path.read_text(encoding="utf-8"))
        manifest = LabelManifest.model_validate(loaded)
    except LabelContractError as exc:
        blockers.append(exc.reason())
    except ValidationError as exc:
        blockers.append("LABEL_SCHEMA_INVALID: " + ", ".join(_format_validation_error(error) for error in exc.errors(include_input=False)))
    except OSError:
        blockers.append("LABELS_NOT_READABLE: label manifest could not be read")

    if manifest is not None:
        all_tags = _collect_tags(manifest)
        missing_tags = [tag for tag in REQUIRED_STRICT_TAGS if tag not in all_tags]
        if missing_tags:
            gaps.append("missing required semantic tags: " + ", ".join(missing_tags))

        strict_frames = list(_strict_real_frames(manifest))
        if missing_tags or not strict_frames:
            if not any(tag in all_tags for tag in GAP_TAGS):
                blockers.append("MISSING_GAP_TAG: blocked manifests must carry insufficient evidence gap tags")
            blockers.append("STRICT_EVIDENCE_BLOCKED: real detector-neutral bbox/per-spot semantic evidence is incomplete")
        else:
            frame_gaps = _validate_strict_frames(strict_frames)
            gaps.extend(frame_gaps)
            if frame_gaps:
                blockers.append("STRICT_FRAME_CONTRACT_INCOMPLETE: strict real frames are not acquisition-complete")

        if not _has_threshold_decision_context(manifest):
            gaps.append("missing threshold_decision scenario/case context for shared-vs-per-spot criteria")

        if _contains_fabrication_marker(manifest):
            blockers.append("FABRICATED_LABEL_MARKER: synthetic/example/fabricated labels cannot satisfy the strict contract")

        warnings.extend(_blocked_case_warnings(manifest))

    return {
        "schema_version": "parking-spot-monitor.s07-label-contract-report.v1",
        "status": _status(blockers, gaps),
        "labels": labels_path.name,
        "required_tags": list(REQUIRED_STRICT_TAGS),
        "required_spot_ids": list(REQUIRED_SPOT_IDS),
        "blocker_reasons": _unique_sorted(blockers),
        "gap_reasons": _unique_sorted(gaps),
        "warnings": _unique_sorted(warnings),
    }


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise LabelContractError("LABELS_NOT_FOUND", "label manifest does not exist", path=path.name)
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise LabelContractError("LABELS_INVALID_YAML", "label manifest could not be parsed", path=path.name) from exc
    if not isinstance(loaded, dict):
        raise LabelContractError("LABELS_INVALID_ROOT", "label manifest root must be a mapping", path=path.name)
    return loaded


def _validate_publication_boundary(text: str) -> None:
    scan = scan_report_redactions(text)
    findings = list(scan.get("findings", [])) if not scan.get("passed") else []
    findings.extend(name for name, pattern in PRIVATE_ARTIFACT_PATTERNS if pattern.search(text))
    if findings:
        raise LabelContractError("PUBLICATION_BOUNDARY_VIOLATION", "unsafe private artifact or secret marker found: " + ", ".join(sorted(set(findings))))


def _collect_tags(manifest: LabelManifest) -> set[str]:
    tags: set[str] = set()
    for case in manifest.cases:
        tags.update(case.tags)
        for scenario in case.scenarios:
            tags.update(scenario.tags)
    return tags


def _strict_real_frames(manifest: LabelManifest) -> list[tuple[str, str, Any]]:
    frames = []
    for case in manifest.cases:
        case_tags = set(case.tags)
        for scenario in case.scenarios:
            combined_tags = case_tags | set(scenario.tags)
            if not {"real_capture", "bottom_driveway", "passing_traffic"}.issubset(combined_tags):
                continue
            for frame in scenario.frames:
                frames.append((case.case_id, scenario.scenario_id, frame))
    return frames


def _validate_strict_frames(frames: Sequence[tuple[str, str, Any]]) -> list[str]:
    gaps: list[str] = []
    for case_id, scenario_id, frame in frames:
        missing_spots = [spot_id for spot_id in REQUIRED_SPOT_IDS if spot_id not in frame.expected]
        if missing_spots:
            gaps.append(f"{case_id}/{scenario_id}/{frame.frame_id} missing per-spot expected labels: " + ", ".join(missing_spots))
        if frame.detections is None:
            gaps.append(f"{case_id}/{scenario_id}/{frame.frame_id} missing detector-neutral detections list")
        elif not frame.detections:
            gaps.append(f"{case_id}/{scenario_id}/{frame.frame_id} has no detector-neutral bbox evidence")
        if not str(frame.snapshot_path).startswith("replay://"):
            gaps.append(f"{case_id}/{scenario_id}/{frame.frame_id} snapshot_path must be a replay:// reference, not a raw frame path")
    return gaps


def _has_threshold_decision_context(manifest: LabelManifest) -> bool:
    for case in manifest.cases:
        if "threshold_decision" in case.tags:
            return True
        if any("threshold_decision" in scenario.tags for scenario in case.scenarios):
            return True
    return False


def _contains_fabrication_marker(manifest: LabelManifest) -> bool:
    forbidden = {"synthetic", "example", "fabricated", "mock", "workflow_smoke"}
    for case in manifest.cases:
        tags = set(case.tags)
        for scenario in case.scenarios:
            tags.update(scenario.tags)
        if tags & forbidden and {"real_capture", "bottom_driveway", "passing_traffic"}.issubset(tags):
            return True
    return False


def _blocked_case_warnings(manifest: LabelManifest) -> list[str]:
    warnings = []
    for case in manifest.cases:
        if not case.assessed or not case.bundle_manifest_present:
            warnings.append(f"{case.case_id} is present for traceability but cannot satisfy strict evidence until assessed with available metadata")
    return warnings


def _format_validation_error(error: Mapping[str, Any]) -> str:
    location = ".".join(str(part) for part in error.get("loc", ())) or "<root>"
    return f"{location}:{error.get('msg', 'validation failed')}"


def _status(blockers: Sequence[str], gaps: Sequence[str]) -> str:
    if blockers:
        return "blocked"
    if gaps:
        return "coverage_gap"
    return "passed"


def _unique_sorted(values: Sequence[str]) -> list[str]:
    return sorted({str(value) for value in values if value})


if __name__ == "__main__":
    raise SystemExit(main())
