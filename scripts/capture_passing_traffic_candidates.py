#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import capture_calibration_bundle

DEFAULT_BUNDLE_ROOT = Path("data/calibration-bundles")
DEFAULT_INDEX = Path("data/s07-replay-evidence/passing-traffic-candidates.json")
DEFAULT_LABELS = Path("data/s07-replay-evidence/real-traffic-labels.yaml")
SCHEMA_VERSION = "parking-spot-monitor.passing-traffic-candidates.v1"
REPLAY_PREFIX = "replay://passing-traffic-candidates"
ALLOWED_STATUSES = {"needs_review", "accepted", "rejected"}
STRICT_TAGS = ["real_capture", "bottom_driveway", "passing_traffic", "threshold_decision"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture and index reviewable passing-traffic evidence candidates.")
    parser.add_argument("--attempts", type=int, default=1, help="Number of calibration captures to run.")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="Delay between capture attempts.")
    parser.add_argument("--candidate-index", default=str(DEFAULT_INDEX), help="Candidate index JSON path.")
    parser.add_argument("--labels", default=str(DEFAULT_LABELS), help="Private real-traffic labels YAML path.")
    parser.add_argument("--bundle-root", default=str(DEFAULT_BUNDLE_ROOT), help="Calibration bundle root to scan.")
    parser.add_argument("--accept-latest", action="store_true", help="Mark the latest captured candidate accepted and promote it to labels.")
    parser.add_argument("--scan-existing", action="store_true", help="Index existing bundle manifests before/without running new captures.")
    parser.add_argument("--docker-timeout-seconds", type=float, default=180.0, help="Per-capture Docker timeout.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    index_path = Path(args.candidate_index)
    labels_path = Path(args.labels)
    bundle_root = Path(args.bundle_root)
    recorded: list[dict[str, Any]] = []
    if args.scan_existing:
        for manifest_path in sorted(_manifest_set(bundle_root), key=lambda p: p.stat().st_mtime):
            recorded.append(record_bundle_candidate(manifest_path, index_path=index_path))

    for attempt in range(max(0, int(args.attempts))):
        before = _manifest_set(bundle_root)
        exit_code = capture_calibration_bundle.main([
            "--bundle-root",
            str(bundle_root),
            "--docker-timeout-seconds",
            str(float(args.docker_timeout_seconds)),
        ])
        after = _manifest_set(bundle_root)
        new_manifests = sorted(after - before, key=lambda p: p.stat().st_mtime)
        for manifest_path in new_manifests:
            record = record_bundle_candidate(manifest_path, index_path=index_path, capture_exit_code=exit_code)
            recorded.append(record)
        if attempt + 1 < int(args.attempts) and float(args.interval_seconds) > 0:
            time.sleep(float(args.interval_seconds))

    if args.accept_latest:
        latest = recorded[-1] if recorded else latest_candidate(index_path)
        if latest is not None:
            mark_candidate_status(index_path, latest["candidate_id"], "accepted", semantic_tags=STRICT_TAGS)
            promote_accepted_candidates(index_path=index_path, labels_path=labels_path)

    print(json.dumps({"recorded": len(recorded), "index": str(index_path), "labels": str(labels_path)}, sort_keys=True))
    return 0


def _manifest_set(bundle_root: Path) -> set[Path]:
    if not bundle_root.exists():
        return set()
    return {path for path in bundle_root.glob("*/manifest.json") if path.is_file()}


def record_bundle_candidate(manifest_path: Path, *, index_path: Path, capture_exit_code: int | None = None) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bundle_dir = Path(str(manifest.get("bundle_dir") or manifest_path.parent))
    candidate_id = bundle_dir.name
    record = {
        "candidate_id": candidate_id,
        "status": "needs_review",
        "semantic_tags": ["real_capture"],
        "snapshot_ref": f"{REPLAY_PREFIX}/{candidate_id}",
        "observed_at": manifest.get("completed_at") or manifest.get("started_at"),
        "capture_status": manifest.get("status"),
        "capture_phase": manifest.get("phase"),
        "capture_exit_code": capture_exit_code,
        "docker_exit_code": manifest.get("docker_exit_code"),
        "raw_frame_valid": bool(((manifest.get("artifacts") or {}).get("raw_frame") or {}).get("valid_jpeg")),
        "debug_overlay_valid": bool(((manifest.get("artifacts") or {}).get("debug_overlay") or {}).get("valid_jpeg")),
        "detections": _safe_detections(manifest.get("detection_summary")),
        "review_notes": "Review raw ignored bundle image locally; promote only if a transient non-spot vehicle is visible.",
    }
    index = _load_index(index_path)
    candidates = [item for item in index["candidates"] if item.get("candidate_id") != candidate_id]
    candidates.append(record)
    index["candidates"] = sorted(candidates, key=lambda item: str(item.get("candidate_id")))
    _write_index(index_path, index)
    return record


def mark_candidate_status(index_path: Path, candidate_id: str, status: str, *, semantic_tags: Sequence[str] | None = None) -> dict[str, Any]:
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"unsupported candidate status: {status}")
    index = _load_index(index_path)
    for item in index["candidates"]:
        if item.get("candidate_id") == candidate_id:
            item["status"] = status
            if semantic_tags is not None:
                item["semantic_tags"] = list(semantic_tags)
            _write_index(index_path, index)
            return item
    raise KeyError(candidate_id)


def latest_candidate(index_path: Path) -> dict[str, Any] | None:
    index = _load_index(index_path)
    if not index["candidates"]:
        return None
    return sorted(index["candidates"], key=lambda item: str(item.get("candidate_id")))[-1]


def promote_accepted_candidates(*, index_path: Path, labels_path: Path) -> int:
    index = _load_index(index_path)
    accepted = [item for item in index["candidates"] if item.get("status") == "accepted" and set(STRICT_TAGS).issubset(set(item.get("semantic_tags") or []))]
    if not accepted:
        return 0
    manifest = _load_labels(labels_path)
    existing_ids = {case.get("case_id") for case in manifest.get("cases", []) if isinstance(case, dict)}
    promoted = 0
    for item in accepted:
        case_id = f"passing-traffic-{item['candidate_id']}"
        if case_id in existing_ids:
            continue
        manifest.setdefault("cases", []).append(_label_case_from_candidate(item))
        promoted += 1
    if promoted:
        labels_path.parent.mkdir(parents=True, exist_ok=True)
        labels_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    return promoted


def _load_index(index_path: Path) -> dict[str, Any]:
    if not index_path.exists():
        return {"schema_version": SCHEMA_VERSION, "candidates": []}
    loaded = json.loads(index_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return {"schema_version": SCHEMA_VERSION, "candidates": []}
    loaded.setdefault("schema_version", SCHEMA_VERSION)
    loaded.setdefault("candidates", [])
    return loaded


def _write_index(index_path: Path, index: dict[str, Any]) -> None:
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _safe_detections(summary: Any) -> list[dict[str, Any]]:
    if not isinstance(summary, dict):
        return []
    raw = summary.get("candidate_summaries") or summary.get("accepted_candidates") or []
    result = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            safe = {key: item[key] for key in ("spot_id", "class_name", "confidence", "bbox") if key in item}
            result.append(safe)
    return result


def _load_labels(labels_path: Path) -> dict[str, Any]:
    if not labels_path.exists():
        return {"schema_version": "parking-spot-monitor.replay.v1", "cases": []}
    loaded = yaml.safe_load(labels_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return {"schema_version": "parking-spot-monitor.replay.v1", "cases": []}
    loaded.setdefault("schema_version", "parking-spot-monitor.replay.v1")
    loaded.setdefault("cases", [])
    return loaded


def _label_case_from_candidate(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "case_id": f"passing-traffic-{item['candidate_id']}",
        "tags": ["real_capture", "bottom_driveway"],
        "assessed": True,
        "scenarios": [
            {
                "scenario_id": "passing-traffic-threshold-check",
                "tags": ["passing_traffic", "threshold_decision"],
                "frames": [
                    {
                        "frame_id": str(item["candidate_id"]),
                        "observed_at": item.get("observed_at") or "1970-01-01T00:00:00Z",
                        "snapshot_path": item["snapshot_ref"],
                        "expected": {"left_spot": "empty", "right_spot": "empty"},
                        "detections": list(item.get("detections") or []),
                    }
                ],
            }
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
