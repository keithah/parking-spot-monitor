#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parking_spot_monitor.replay import ReplayReportError
from parking_spot_monitor.tuning import build_tuning_comparison_report, render_tuning_report_markdown
from scripts.replay_calibration_cases import ReplayCliError, _load_label_manifest, _load_replay_config, _print_diagnostic

REPORT_JSON = "tuning-report.json"
REPORT_MARKDOWN = "tuning-report.md"


class TuningCliError(Exception):
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
    parser = argparse.ArgumentParser(description="Compare baseline and proposed calibration configs against replay labels.")
    parser.add_argument("--baseline-config", required=True, help="Path to current/baseline parking-spot-monitor config YAML.")
    parser.add_argument("--proposed-config", required=True, help="Path to proposed parking-spot-monitor config YAML.")
    parser.add_argument("--labels", required=True, help="Path to replay label manifest YAML or JSON.")
    parser.add_argument("--output-dir", required=True, help="Directory where tuning-report.json and tuning-report.md are written.")
    return parser


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    source_environ = os.environ if environ is None else environ
    try:
        baseline_config = _load_config_for_side(Path(args.baseline_config), source_environ, side="baseline")
        proposed_config = _load_config_for_side(Path(args.proposed_config), source_environ, side="proposed")
        manifest = _load_label_manifest(Path(args.labels))
        report = build_tuning_comparison_report(
            manifest,
            baseline_config=baseline_config,
            proposed_config=proposed_config,
        )
        markdown = render_tuning_report_markdown(report)
        json_path, markdown_path = _write_reports(Path(args.output_dir), report, markdown)
    except TuningCliError as exc:
        _print_diagnostic(exc.diagnostic(), stream=sys.stderr)
        return exc.exit_code
    except ReplayCliError as exc:
        _print_diagnostic(exc.diagnostic(), stream=sys.stderr)
        return exc.exit_code
    except ReplayReportError as exc:
        diagnostic = {"code": "REPORT_UNSAFE", **exc.diagnostics()}
        _print_diagnostic(diagnostic, stream=sys.stderr)
        return 2
    except Exception:
        _print_diagnostic(
            {"code": "INTERNAL_ERROR", "phase": "internal", "message": "unexpected tuning comparison CLI failure"},
            stream=sys.stderr,
        )
        return 1

    summary = {
        "status": "ok",
        "phase": "complete",
        "outputs": {"json": str(json_path), "markdown": str(markdown_path)},
        "redaction_scan": report.get("redaction_scan", {}),
        "decision": report.get("decision", "unknown"),
        "status_counts": report.get("status_counts", {}),
        "metric_deltas": report.get("metric_deltas", {}),
        "blocked_reasons": report.get("blocked_reasons", []),
        "not_covered_reasons": report.get("not_covered_reasons", []),
    }
    _print_diagnostic(summary, stream=sys.stdout)
    return 0


def _load_config_for_side(config_path: Path, environ: Mapping[str, str], *, side: str) -> Any:
    try:
        return _load_replay_config(config_path, environ)
    except ReplayCliError as exc:
        raise ReplayCliError(
            f"{side.upper()}_{exc.code}",
            exc.message,
            phase=f"{side}_{exc.phase}",
            path=exc.path,
            fields=exc.fields,
            exit_code=exc.exit_code,
        ) from exc


def _write_reports(output_dir: Path, report: Mapping[str, Any], markdown: str) -> tuple[Path, Path]:
    json_path = output_dir / REPORT_JSON
    markdown_path = output_dir / REPORT_MARKDOWN
    json_tmp = output_dir / f".{REPORT_JSON}.tmp"
    markdown_tmp = output_dir / f".{REPORT_MARKDOWN}.tmp"
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        rendered_json = json.dumps(report, indent=2, sort_keys=True) + "\n"
        json_tmp.write_text(rendered_json, encoding="utf-8")
        markdown_tmp.write_text(markdown, encoding="utf-8")
        json_tmp.replace(json_path)
        markdown_tmp.replace(markdown_path)
    except OSError as exc:
        _cleanup_temp(json_tmp, markdown_tmp)
        raise TuningCliError("OUTPUT_WRITE_FAILED", "tuning report files could not be written", phase="output_write", path=str(output_dir)) from exc
    return json_path, markdown_path


def _cleanup_temp(*paths: Path) -> None:
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
