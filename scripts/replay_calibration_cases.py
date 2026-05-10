#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parking_spot_monitor.config import RuntimeSettings, load_settings
from parking_spot_monitor.errors import ConfigError
from parking_spot_monitor.replay import (
    LabelManifest,
    ReplayEvaluationConfig,
    ReplayReportError,
    build_replay_report,
    evaluate_manifest,
    render_replay_report_markdown,
)

REPORT_JSON = "replay-report.json"
REPORT_MARKDOWN = "replay-report.md"
DUMMY_SECRET_VALUE = "replay-local-placeholder"


class ReplayCliError(Exception):
    """Safe CLI error intended for stderr without raw tracebacks."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        phase: str,
        path: str | None = None,
        fields: Sequence[str] = (),
        exit_code: int = 2,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.phase = phase
        self.path = path
        self.fields = tuple(fields)
        self.exit_code = exit_code

    def diagnostic(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "phase": self.phase, "message": self.message}
        if self.path is not None:
            payload["path"] = self.path
        if self.fields:
            payload["fields"] = list(self.fields)
        return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay publication-safe calibration labels and write replay reports.")
    parser.add_argument("--config", required=True, help="Path to parking-spot-monitor config YAML.")
    parser.add_argument("--labels", required=True, help="Path to replay label manifest YAML or JSON.")
    parser.add_argument("--output-dir", required=True, help="Directory where replay-report.json and replay-report.md are written.")
    return parser


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    source_environ = os.environ if environ is None else environ
    try:
        config = _load_replay_config(Path(args.config), source_environ)
        manifest = _load_label_manifest(Path(args.labels))
        result = evaluate_manifest(manifest, config)
        report = build_replay_report(result)
        markdown = render_replay_report_markdown(report)
        json_path, markdown_path = _write_reports(Path(args.output_dir), report, markdown)
    except ReplayCliError as exc:
        _print_diagnostic(exc.diagnostic(), stream=sys.stderr)
        return exc.exit_code
    except ReplayReportError as exc:
        diagnostic = {"code": "REPORT_UNSAFE", **exc.diagnostics()}
        _print_diagnostic(diagnostic, stream=sys.stderr)
        return 2
    except Exception:
        _print_diagnostic(
            {"code": "INTERNAL_ERROR", "phase": "internal", "message": "unexpected replay CLI failure"},
            stream=sys.stderr,
        )
        return 1

    summary = {
        "status": "ok",
        "phase": "complete",
        "outputs": {"json": str(json_path), "markdown": str(markdown_path)},
        "status_counts": report.get("status_counts", {}),
        "redaction_scan": report.get("redaction_scan", {}),
        "shared_threshold_sufficiency": (report.get("shared_threshold_sufficiency") or {}).get("verdict", "unknown"),
    }
    _print_diagnostic(summary, stream=sys.stdout)
    return 0


def _load_replay_config(config_path: Path, environ: Mapping[str, str]) -> ReplayEvaluationConfig:
    try:
        settings = load_settings(config_path, environ=_replay_environ(config_path, environ))
    except ConfigError as exc:
        raise ReplayCliError(
            "CONFIG_INVALID",
            exc.message,
            phase=exc.phase or "config",
            path=str(exc.path or config_path),
            fields=tuple(exc.fields) + tuple(f"missing_env:{name}" for name in exc.missing_env),
        ) from exc
    return _settings_to_replay_config(settings)


def _settings_to_replay_config(settings: RuntimeSettings) -> ReplayEvaluationConfig:
    return ReplayEvaluationConfig(
        spots={
            "left_spot": [(point.x, point.y) for point in settings.spots.left_spot.polygon],
            "right_spot": [(point.x, point.y) for point in settings.spots.right_spot.polygon],
        },
        allowed_classes=list(settings.detection.vehicle_classes),
        confidence_threshold=settings.detection.confidence_threshold,
        min_bbox_area_px=settings.detection.min_bbox_area_px,
        min_polygon_overlap_ratio=settings.detection.min_polygon_overlap_ratio,
        occupancy=settings.occupancy,
    )


def _replay_environ(config_path: Path, environ: Mapping[str, str]) -> dict[str, str]:
    """Provide non-secret placeholders for config secret references during offline replay only."""

    result = dict(environ)
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return result
    if not isinstance(raw, dict):
        return result
    for section_name, key, default in (
        ("stream", "rtsp_url_env", "RTSP_URL"),
        ("matrix", "access_token_env", "MATRIX_ACCESS_TOKEN"),
    ):
        section = raw.get(section_name)
        if not isinstance(section, dict):
            continue
        env_name = section.get(key, default)
        if isinstance(env_name, str) and env_name and not result.get(env_name):
            result[env_name] = DUMMY_SECRET_VALUE
    return result


def _load_label_manifest(labels_path: Path) -> LabelManifest:
    if not labels_path.is_file():
        raise ReplayCliError("LABELS_NOT_FOUND", "label manifest could not be read", phase="labels_read", path=str(labels_path))
    try:
        loaded = yaml.safe_load(labels_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ReplayCliError("LABELS_INVALID", "label manifest could not be parsed", phase="labels_parse", path=str(labels_path)) from exc
    except OSError as exc:
        raise ReplayCliError("LABELS_NOT_FOUND", "label manifest could not be read", phase="labels_read", path=str(labels_path)) from exc
    if not isinstance(loaded, dict):
        raise ReplayCliError("LABELS_INVALID", "label manifest root must be a mapping", phase="labels_parse", path=str(labels_path))
    prepared = _apply_bundle_manifest_presence(loaded, base_dir=labels_path.parent)
    try:
        return LabelManifest.model_validate(prepared)
    except ValidationError as exc:
        fields = tuple(_format_validation_error(error) for error in exc.errors(include_input=False))
        raise ReplayCliError("LABELS_INVALID", "label manifest schema validation failed", phase="labels_schema", path=str(labels_path), fields=fields) from exc


def _apply_bundle_manifest_presence(manifest: dict[str, Any], *, base_dir: Path) -> dict[str, Any]:
    prepared = _deep_copy(manifest)
    cases = prepared.get("cases")
    if not isinstance(cases, list):
        return prepared
    for case in cases:
        if not isinstance(case, dict):
            continue
        bundle_reference = case.get("bundle_manifest")
        if not isinstance(bundle_reference, str) or not bundle_reference.strip():
            continue
        bundle_path = Path(bundle_reference)
        if not bundle_path.is_absolute():
            bundle_path = base_dir / bundle_path
        case["bundle_manifest_present"] = _is_safe_bundle_manifest(bundle_path)
    return prepared


def _is_safe_bundle_manifest(bundle_path: Path) -> bool:
    try:
        loaded = json.loads(bundle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return False
    return isinstance(loaded, dict)


def _write_reports(output_dir: Path, report: Mapping[str, Any], markdown: str) -> tuple[Path, Path]:
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / REPORT_JSON
        markdown_path = output_dir / REPORT_MARKDOWN
        json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(markdown, encoding="utf-8")
    except OSError as exc:
        raise ReplayCliError("OUTPUT_WRITE_FAILED", "replay report files could not be written", phase="output_write", path=str(output_dir)) from exc
    return json_path, markdown_path


def _format_validation_error(error: Mapping[str, Any]) -> str:
    location = ".".join(str(part) for part in error.get("loc", ())) or "<root>"
    return f"{location}:{error.get('msg', 'validation failed')}"


def _deep_copy(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _deep_copy(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_deep_copy(item) for item in value]
    return value


def _print_diagnostic(payload: Mapping[str, Any], *, stream: Any) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
