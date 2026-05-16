from __future__ import annotations

import json
import re
import secrets
import tempfile
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text, redact_diagnostic_value

LabJobKind = Literal["replay", "tuning"]
LabJobStatus = Literal["queued", "running", "succeeded", "failed", "blocked"]
LabRunner = Callable[[Mapping[str, Path]], Mapping[str, Any] | Path | str | None]
LabOutcomeRecorder = Callable[[Mapping[str, Any]], None]

LAB_DIR_NAME = "detection-lab"
JOBS_DIR_NAME = "jobs"
STATUS_FILENAME = "status.json"
REPLAY_REPORT_FILENAME = "replay-report.json"
TUNING_REPORT_FILENAME = "tuning-report.json"
REPLAY_LABELS_FILENAME = "labels.json"
REPLAY_CONFIG_FILENAME = "replay-config.json"
TUNING_BASELINE_CONFIG_FILENAME = "baseline-config.json"
TUNING_PROPOSED_CONFIG_FILENAME = "proposed-config.json"
MAX_STATUS_BYTES = 24_000
MAX_TEXT_CHARS = 600
MAX_DETAIL_ITEMS = 24
DEFAULT_MAX_JOBS = 25
_JOB_ID_RE = re.compile(r"^lab-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$")
_ALLOWED_KINDS = {"replay", "tuning"}


class DetectionLabError(ValueError):
    """Safe detection-lab error suitable for persisted operator diagnostics."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class DetectionLabJob:
    job_id: str
    kind: LabJobKind
    job_dir: Path
    status_path: Path


def detection_lab_root(data_dir: str | Path) -> Path:
    return Path(data_dir) / LAB_DIR_NAME


def detection_lab_jobs_root(data_dir: str | Path) -> Path:
    return detection_lab_root(data_dir) / JOBS_DIR_NAME


class DetectionLabManager:
    """Bounded local detection-lab job boundary for Matrix-triggered commands.

    Matrix/runtime callers choose only a job kind. All inputs are fixed files under
    ``<data_dir>/detection-lab`` and all artifacts are contained under
    ``<data_dir>/detection-lab/jobs/<job_id>``.
    """

    def __init__(
        self,
        data_dir: str | Path,
        *,
        replay_runner: LabRunner | None = None,
        tuning_runner: LabRunner | None = None,
        max_jobs: int = DEFAULT_MAX_JOBS,
        logger: StructuredLogger | None = None,
        outcome_recorder: LabOutcomeRecorder | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.lab_root = detection_lab_root(self.data_dir)
        self.jobs_root = detection_lab_jobs_root(self.data_dir)
        self.replay_runner = replay_runner
        self.tuning_runner = tuning_runner
        self.max_jobs = max(1, int(max_jobs))
        self.logger = logger
        self.outcome_recorder = outcome_recorder
        self._lock = threading.Lock()

    def start_replay(self) -> DetectionLabJob:
        return self.start_job("replay")

    def start_tuning(self) -> DetectionLabJob:
        return self.start_job("tuning")

    def start_job(self, kind: str) -> DetectionLabJob:
        safe_kind = self._validate_kind(kind)
        inputs = self._fixed_inputs(safe_kind)
        missing = [name for name, path in inputs.items() if name != "job_dir" and not path.exists()]
        if missing:
            job = self._create_job(safe_kind)
            self._write_status(
                job,
                status="blocked",
                phase="validate_inputs",
                error={"code": "missing_fixed_inputs", "message": f"Missing fixed lab inputs: {', '.join(sorted(missing))}"},
                summary={"missing_inputs": sorted(missing)},
            )
            return job

        runner = self.replay_runner if safe_kind == "replay" else self.tuning_runner
        if runner is None:
            job = self._create_job(safe_kind)
            self._write_status(
                job,
                status="blocked",
                phase="select_runner",
                error={"code": "runner_unavailable", "message": f"No {safe_kind} lab runner is configured"},
            )
            return job

        job = self._create_job(safe_kind)
        self._write_status(job, status="queued", phase="queued", summary={"inputs": sorted(inputs)})
        thread = threading.Thread(target=self._run_job, args=(job, runner), name=f"detection-lab-{job.job_id}", daemon=True)
        thread.start()
        self._log("info", "detection-lab-job-started", job_id=job.job_id, kind=safe_kind)
        return job

    def summarize(self, job_id: str = "latest") -> dict[str, Any]:
        job_dir = self._resolve_job_dir(job_id)
        status_path = job_dir / STATUS_FILENAME
        if not status_path.exists():
            raise DetectionLabError("status_missing", "Detection lab status is missing")
        try:
            payload = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DetectionLabError("status_unreadable", f"Detection lab status is unreadable: {type(exc).__name__}") from exc
        return _sanitize_status(payload)

    def retain_recent_jobs(self) -> list[Path]:
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        job_dirs = sorted((path for path in self.jobs_root.iterdir() if path.is_dir()), key=lambda path: path.stat().st_mtime, reverse=True)
        removed: list[Path] = []
        for job_dir in job_dirs[self.max_jobs :]:
            if not _JOB_ID_RE.match(job_dir.name):
                continue
            _remove_tree(job_dir)
            removed.append(job_dir)
        return removed

    def _run_job(self, job: DetectionLabJob, runner: LabRunner) -> None:
        self._write_status(job, status="running", phase="running")
        inputs = self._fixed_inputs(job.kind)
        inputs = {**inputs, "job_dir": job.job_dir}
        try:
            result = runner(inputs)
            report_path = self._report_path(job, result)
            summary = self._summarize_report(job.kind, report_path)
            self._write_status(job, status="succeeded", phase="complete", report_path=report_path, summary=summary)
            self._log("info", "detection-lab-job-succeeded", job_id=job.job_id, kind=job.kind)
        except DetectionLabError as exc:
            self._write_status(job, status="blocked", phase="run", error={"code": exc.code, "message": exc.message})
            self._log("warning", "detection-lab-job-blocked", job_id=job.job_id, kind=job.kind, error_code=exc.code)
        except Exception as exc:  # pragma: no cover - exact exception type is runner-controlled
            self._write_status(
                job,
                status="failed",
                phase="run",
                error={"code": "runner_exception", "message": f"Runner failed: {type(exc).__name__}: {redact_diagnostic_text(exc)}"},
            )
            self._log("warning", "detection-lab-job-failed", job_id=job.job_id, kind=job.kind, error_type=type(exc).__name__)
        finally:
            self.retain_recent_jobs()

    def _create_job(self, kind: LabJobKind) -> DetectionLabJob:
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        for _ in range(100):
            job_id = f"lab-{_utc_now().strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(4)}"
            job_dir = self.jobs_root / job_id
            try:
                job_dir.mkdir(mode=0o700)
            except FileExistsError:
                continue
            return DetectionLabJob(job_id=job_id, kind=kind, job_dir=job_dir, status_path=job_dir / STATUS_FILENAME)
        raise DetectionLabError("job_id_exhausted", "Could not allocate a unique detection lab job id")

    def _fixed_inputs(self, kind: LabJobKind) -> dict[str, Path]:
        if kind == "replay":
            return {
                "labels": self.lab_root / REPLAY_LABELS_FILENAME,
                "config": self.lab_root / REPLAY_CONFIG_FILENAME,
            }
        return {
            "labels": self.lab_root / REPLAY_LABELS_FILENAME,
            "baseline_config": self.lab_root / TUNING_BASELINE_CONFIG_FILENAME,
            "proposed_config": self.lab_root / TUNING_PROPOSED_CONFIG_FILENAME,
        }

    def _validate_kind(self, kind: str) -> LabJobKind:
        if kind not in _ALLOWED_KINDS:
            raise DetectionLabError("unknown_job_kind", "Detection lab job kind must be replay or tuning")
        return kind  # type: ignore[return-value]

    def _resolve_job_dir(self, job_id: str) -> Path:
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        if job_id == "latest":
            candidates = [path for path in self.jobs_root.iterdir() if path.is_dir() and _JOB_ID_RE.match(path.name)]
            if not candidates:
                raise DetectionLabError("job_not_found", "No detection lab jobs exist")
            return max(candidates, key=lambda path: path.stat().st_mtime)
        if not _JOB_ID_RE.match(job_id):
            raise DetectionLabError("invalid_job_id", "Detection lab job id is invalid")
        return _contained_path(self.jobs_root, job_id)

    def _report_path(self, job: DetectionLabJob, result: Mapping[str, Any] | Path | str | None) -> Path:
        default = job.job_dir / (REPLAY_REPORT_FILENAME if job.kind == "replay" else TUNING_REPORT_FILENAME)
        if result is None:
            report_path = default
        elif isinstance(result, Mapping):
            report_value = result.get("report_path") or result.get("path")
            if report_value is None:
                report_path = default
                _write_json_atomic(report_path, result)
            else:
                report_path = Path(str(report_value))
        else:
            report_path = Path(str(result))
        report_path = _contained_path(job.job_dir, report_path)
        if not report_path.exists():
            raise DetectionLabError("report_missing", "Detection lab runner did not produce a report")
        return report_path

    def _summarize_report(self, kind: LabJobKind, report_path: Path) -> dict[str, Any]:
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise DetectionLabError("malformed_report", f"Detection lab report is malformed JSON: {exc.msg}") from exc
        except OSError as exc:
            raise DetectionLabError("report_unreadable", f"Detection lab report is unreadable: {type(exc).__name__}") from exc
        if not isinstance(report, Mapping):
            raise DetectionLabError("malformed_report", "Detection lab report must be a JSON object")
        return summarize_lab_report(kind, report)

    def _write_status(
        self,
        job: DetectionLabJob,
        *,
        status: LabJobStatus,
        phase: str,
        report_path: Path | None = None,
        summary: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
    ) -> None:
        now = _utc_now_text()
        previous = _read_status(job.status_path)
        payload: dict[str, Any] = {
            "schema_version": "parking-spot-monitor.detection-lab-status.v1",
            "job_id": job.job_id,
            "kind": job.kind,
            "status": status,
            "phase": _clip_text(phase),
            "created_at": previous.get("created_at", now),
            "updated_at": now,
        }
        if report_path is not None:
            payload["report_path"] = report_path.name
        if summary:
            payload["summary"] = _sanitize_value(summary)
        if error:
            payload["error"] = _sanitize_value(error)
        bounded_payload = _bound_status(payload)
        with self._lock:
            _write_json_atomic(job.status_path, bounded_payload)
        if status in {"succeeded", "failed", "blocked"}:
            self._record_outcome(bounded_payload)

    def _record_outcome(self, status_payload: Mapping[str, Any]) -> None:
        if self.outcome_recorder is None:
            return
        try:
            self.outcome_recorder(status_payload)
        except Exception as exc:  # pragma: no cover - recorder is runtime-injected
            self._log("warning", "detection-lab-outcome-record-failed", error_type=type(exc).__name__)

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        getattr(self.logger, level)(event, **fields)


def summarize_lab_report(kind: str, report: Mapping[str, Any]) -> dict[str, Any]:
    """Return a bounded, redacted operator summary for replay/tuning reports."""

    safe_kind = kind if kind in _ALLOWED_KINDS else "unknown"
    status_counts = report.get("status_counts", {})
    coverage = report.get("coverage", {})
    summary: dict[str, Any] = {
        "kind": safe_kind,
        "schema_version": _clip_text(report.get("schema_version", "unknown")),
        "status_counts": _int_mapping(status_counts),
    }
    if safe_kind == "replay":
        summary["coverage"] = {
            "assessed_frames": _safe_int((coverage or {}).get("assessed_frames", 0)) if isinstance(coverage, Mapping) else 0,
            "blocked_frames": _safe_int((coverage or {}).get("blocked_frames", 0)) if isinstance(coverage, Mapping) else 0,
            "not_assessed_frames": _safe_int((coverage or {}).get("not_assessed_frames", 0)) if isinstance(coverage, Mapping) else 0,
        }
        threshold = report.get("shared_threshold_sufficiency", {})
        if isinstance(threshold, Mapping):
            summary["shared_threshold_sufficiency"] = {
                "verdict": _clip_text(threshold.get("verdict", "unknown")),
                "rationale": _clip_text(threshold.get("rationale", "")),
            }
    elif safe_kind == "tuning":
        summary["decision"] = _clip_text(report.get("decision", "unknown"))
        summary["decision_rationale"] = _clip_text(report.get("decision_rationale", ""))
        metric_deltas = report.get("metric_deltas", {})
        if isinstance(metric_deltas, Mapping):
            summary["metric_delta_totals"] = _int_mapping(metric_deltas.get("totals", {}))
    redaction = report.get("redaction_scan", {})
    if isinstance(redaction, Mapping):
        summary["redaction"] = {
            "passed": bool(redaction.get("passed", False)),
            "findings": [_clip_text(item, 80) for item in list(redaction.get("findings", []))[:8]],
        }
    return _sanitize_value(summary)  # type: ignore[return-value]


def _contained_path(root: Path, value: str | Path) -> Path:
    root_resolved = root.resolve()
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise DetectionLabError("path_outside_lab", "Detection lab artifact path escaped the job directory") from exc
    return resolved


def _read_status(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
    except (OSError, json.JSONDecodeError):
        return {}
    return {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, sort_keys=True, indent=2, default=str)
    if len(data.encode("utf-8")) > MAX_STATUS_BYTES:
        payload = _bound_status(payload)
        data = json.dumps(payload, sort_keys=True, indent=2, default=str)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(data)
        handle.write("\n")
        temp_name = handle.name
    Path(temp_name).replace(path)


def _bound_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    bounded = _sanitize_value(payload)
    if not isinstance(bounded, dict):
        bounded = {"status": "failed", "error": {"code": "status_invalid", "message": "invalid status payload"}}
    data = json.dumps(bounded, sort_keys=True, default=str)
    if len(data.encode("utf-8")) <= MAX_STATUS_BYTES:
        return bounded
    if isinstance(bounded.get("error"), dict):
        bounded["error"]["message"] = _clip_text(bounded["error"].get("message", ""), 160)
    if isinstance(bounded.get("summary"), dict):
        bounded["summary"] = {"truncated": True}
    return bounded


def _sanitize_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _bound_status(payload)


def _sanitize_value(value: Any) -> Any:
    redacted = redact_diagnostic_value(value)
    if isinstance(redacted, Mapping):
        result: dict[str, Any] = {}
        for index, (key, item) in enumerate(redacted.items()):
            if index >= MAX_DETAIL_ITEMS:
                result["truncated"] = True
                break
            result[_clip_text(key, 80)] = _sanitize_value(item)
        return result
    if isinstance(redacted, (list, tuple)):
        return [_sanitize_value(item) for item in list(redacted)[:MAX_DETAIL_ITEMS]]
    if isinstance(redacted, str):
        return _clip_text(redacted)
    if isinstance(redacted, bool) or redacted is None:
        return redacted
    if isinstance(redacted, (int, float)):
        return redacted
    return _clip_text(redacted)


def _clip_text(value: object, limit: int = MAX_TEXT_CHARS) -> str:
    text = redact_diagnostic_text(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 14)] + "…<truncated>"


def _int_mapping(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {_clip_text(key, 80): _safe_int(item) for key, item in list(value.items())[:MAX_DETAIL_ITEMS]}


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_text() -> str:
    return _utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def _remove_tree(path: Path) -> None:
    for child in sorted(path.rglob("*"), reverse=True):
        if child.is_file() or child.is_symlink():
            child.unlink(missing_ok=True)
        elif child.is_dir():
            child.rmdir()
    path.rmdir()
