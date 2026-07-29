from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parking_spot_monitor.vehicle_history_models import ArchiveSchemaError, ProfileCorrectionEvent


@dataclass(frozen=True)
class CorrectionQuarantineResult:
    succeeded: bool
    written: bool


@dataclass(frozen=True)
class CorrectionReplayLoadResult:
    events: tuple[ProfileCorrectionEvent, ...]
    succeeded: bool
    quarantine_writes: int


@dataclass(frozen=True)
class CorrectionQuarantineCountResult:
    count: int
    succeeded: bool


def load_correction_events(
    path: Path,
    *,
    max_line_bytes: int,
    quarantine_line: Callable[..., CorrectionQuarantineResult],
    record_failure: Callable[..., None],
) -> CorrectionReplayLoadResult:
    corrections: list[ProfileCorrectionEvent] = []
    succeeded = True
    quarantine_writes = 0
    try:
        with path.open("rb") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                if len(raw_line) > max_line_bytes:
                    outcome = quarantine_line(line_number=line_number, reason="line-too-large")
                else:
                    try:
                        text = raw_line.decode("utf-8")
                        if not text.strip():
                            continue
                        corrections.append(ProfileCorrectionEvent.from_json_dict(json.loads(text)))
                        continue
                    except (UnicodeDecodeError, json.JSONDecodeError, ArchiveSchemaError, ValueError) as exc:
                        outcome = quarantine_line(line_number=line_number, reason=type(exc).__name__)
                succeeded = succeeded and outcome.succeeded
                quarantine_writes += int(outcome.written)
    except FileNotFoundError:
        pass
    except OSError as exc:
        succeeded = False
        record_failure(phase="correction-load", path_name=path.name, error=exc)
    return CorrectionReplayLoadResult(
        events=tuple(corrections),
        succeeded=succeeded,
        quarantine_writes=quarantine_writes,
    )


def quarantine_correction_line(
    path: Path,
    *,
    line_number: int,
    reason: str,
    quarantined_at: str,
    record_failure: Callable[..., None],
    bump_revision: Callable[[], None],
) -> CorrectionQuarantineResult:
    already_quarantined, read_succeeded = _correction_line_already_quarantined(
        path,
        line_number=line_number,
        reason=reason,
        record_failure=record_failure,
    )
    if already_quarantined:
        return CorrectionQuarantineResult(succeeded=read_succeeded, written=False)
    entry = {"line_number": line_number, "reason": reason, "quarantined_at": quarantined_at}
    try:
        with path.open("a", encoding="utf-8") as handle:
            json.dump(entry, handle, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.write("\n")
    except OSError as exc:
        record_failure(phase="correction-quarantine", path_name=path.name, error=exc)
        return CorrectionQuarantineResult(succeeded=False, written=False)
    bump_revision()
    return CorrectionQuarantineResult(succeeded=read_succeeded, written=True)


def count_correction_quarantine(
    path: Path,
    *,
    record_failure: Callable[..., None],
) -> CorrectionQuarantineCountResult:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return CorrectionQuarantineCountResult(count=sum(1 for _ in handle), succeeded=True)
    except FileNotFoundError:
        return CorrectionQuarantineCountResult(count=0, succeeded=True)
    except OSError as exc:
        record_failure(phase="correction-quarantine-count", path_name=path.name, error=exc)
        return CorrectionQuarantineCountResult(count=0, succeeded=False)


def _correction_line_already_quarantined(
    path: Path,
    *,
    line_number: int,
    reason: str,
    record_failure: Callable[..., None],
) -> tuple[bool, bool]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    entry: Any = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(entry, dict) and entry.get("line_number") == line_number and entry.get("reason") == reason:
                    return True, True
    except FileNotFoundError:
        return False, True
    except OSError as exc:
        record_failure(phase="correction-quarantine-read", path_name=path.name, error=exc)
        return False, False
    return False, True
