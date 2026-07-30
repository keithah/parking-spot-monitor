from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
from pathlib import Path
import stat
from typing import Any

from parking_spot_monitor.vehicle_history_models import ArchiveSchemaError, ArchiveWriteError, ProfileCorrectionEvent


@dataclass(frozen=True)
class CorrectionQuarantineResult:
    succeeded: bool
    written: bool


@dataclass(frozen=True)
class CorrectionReplayLoadResult:
    events: tuple[ProfileCorrectionEvent, ...]
    succeeded: bool
    quarantine_writes: int
    quarantine_count: int


@dataclass(frozen=True)
class CorrectionQuarantineCountResult:
    count: int
    succeeded: bool


@contextmanager
def correction_ledger_transaction(path: Path) -> Iterator[None]:
    """Serialize every replay/replace/append sequence on a stable sidecar inode."""

    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f".{path.name}.lock")
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    locked = False
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("correction ledger lock must be a regular file")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        locked = True
        yield
    finally:
        if locked:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def load_correction_events(
    path: Path,
    *,
    max_line_bytes: int,
    max_file_bytes: int,
    max_events: int,
    max_invalid_lines: int,
    quarantine_path: Path,
    quarantine_line: Callable[..., CorrectionQuarantineResult],
    record_failure: Callable[..., None],
) -> CorrectionReplayLoadResult:
    corrections: list[ProfileCorrectionEvent] = []
    succeeded = True
    quarantine_writes = 0
    invalid_lines = 0
    quarantine_keys, quarantine_count, quarantine_succeeded = _load_correction_quarantine_index(
        quarantine_path,
        max_lines=max_invalid_lines,
        max_line_bytes=max_line_bytes,
        record_failure=record_failure,
    )
    succeeded = succeeded and quarantine_succeeded
    try:
        try:
            file_size = path.stat().st_size
        except FileNotFoundError:
            file_size = 0
        if file_size > max_file_bytes:
            raise _CorrectionReplayLimitError("correction ledger exceeds maximum size")
        with path.open("rb") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                if len(corrections) >= max_events:
                    raise _CorrectionReplayLimitError("correction ledger exceeds maximum event count")
                if len(raw_line) > max_line_bytes:
                    reason = "line-too-large"
                else:
                    try:
                        text = raw_line.decode("utf-8")
                        if not text.strip():
                            continue
                        corrections.append(ProfileCorrectionEvent.from_json_dict(json.loads(text)))
                        continue
                    except (UnicodeDecodeError, json.JSONDecodeError, ArchiveSchemaError, ValueError) as exc:
                        reason = type(exc).__name__
                invalid_lines += 1
                if invalid_lines > max_invalid_lines:
                    raise _CorrectionReplayLimitError("correction ledger exceeds maximum invalid-line count")
                key = (line_number, reason)
                if key in quarantine_keys:
                    continue
                outcome = quarantine_line(
                    line_number=line_number,
                    reason=reason,
                    known_keys=quarantine_keys,
                )
                succeeded = succeeded and outcome.succeeded
                quarantine_writes += int(outcome.written)
    except FileNotFoundError:
        pass
    except _CorrectionReplayLimitError as exc:
        succeeded = False
        record_failure(phase="correction-load", path_name=path.name, error=exc)
    except OSError as exc:
        succeeded = False
        record_failure(phase="correction-load", path_name=path.name, error=exc)
    if not quarantine_succeeded and quarantine_writes:
        recounted = count_correction_quarantine(
            quarantine_path,
            max_lines=max_invalid_lines,
            max_bytes=max_invalid_lines * max_line_bytes,
            record_failure=record_failure,
        )
        quarantine_count = recounted.count
    else:
        quarantine_count += quarantine_writes
    return CorrectionReplayLoadResult(
        events=tuple(corrections),
        succeeded=succeeded,
        quarantine_writes=quarantine_writes,
        quarantine_count=quarantine_count,
    )


def quarantine_correction_line(
    path: Path,
    *,
    line_number: int,
    reason: str,
    quarantined_at: str,
    record_failure: Callable[..., None],
    bump_revision: Callable[[], None],
    known_keys: set[tuple[int, str]] | None = None,
) -> CorrectionQuarantineResult:
    if known_keys is None:
        already_quarantined, read_succeeded = _correction_line_already_quarantined(
            path,
            line_number=line_number,
            reason=reason,
            record_failure=record_failure,
        )
    else:
        already_quarantined, read_succeeded = (line_number, reason) in known_keys, True
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
    if known_keys is not None:
        known_keys.add((line_number, reason))
    bump_revision()
    return CorrectionQuarantineResult(succeeded=read_succeeded, written=True)


def count_correction_quarantine(
    path: Path,
    *,
    max_lines: int | None = None,
    max_bytes: int | None = None,
    record_failure: Callable[..., None],
) -> CorrectionQuarantineCountResult:
    try:
        if max_bytes is not None and path.stat().st_size > max_bytes:
            return CorrectionQuarantineCountResult(count=max_lines or 0, succeeded=False)
        with path.open("r", encoding="utf-8") as handle:
            count = 0
            for count, _line in enumerate(handle, start=1):
                if max_lines is not None and count > max_lines:
                    return CorrectionQuarantineCountResult(count=max_lines, succeeded=False)
            return CorrectionQuarantineCountResult(count=count, succeeded=True)
    except FileNotFoundError:
        return CorrectionQuarantineCountResult(count=0, succeeded=True)
    except OSError as exc:
        record_failure(phase="correction-quarantine-count", path_name=path.name, error=exc)
        return CorrectionQuarantineCountResult(count=0, succeeded=False)


def compact_correction_events(
    path: Path,
    events: tuple[ProfileCorrectionEvent, ...],
    *,
    max_file_bytes: int,
    record_failure: Callable[..., None],
) -> bool:
    payload = b"".join(
        json.dumps(
            event.to_json_dict(),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
        for event in events
    )
    if len(payload) > max_file_bytes:
        return False
    temporary: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "wb", delete=False, dir=path.parent, prefix=f".{path.name}.", suffix=".compact"
        ) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        temporary = None
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return True
    except OSError as exc:
        record_failure(phase="correction-compact", path_name=path.name, error=exc)
        return False
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def append_bounded_correction_event(
    path: Path,
    line: str,
    *,
    current_count: int,
    max_events: int,
    max_file_bytes: int,
    compact_at_bytes: int,
    load_events: Callable[[], CorrectionReplayLoadResult],
    record_failure: Callable[..., None],
) -> bool:
    if current_count >= max_events:
        raise ArchiveWriteError("correction ledger reached maximum event count")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        current_size = path.stat().st_size
    except FileNotFoundError:
        current_size = 0
    compacted = False
    if current_size >= compact_at_bytes:
        loaded = load_events()
        if not loaded.succeeded or not compact_correction_events(
            path,
            loaded.events,
            max_file_bytes=max_file_bytes,
            record_failure=record_failure,
        ):
            raise ArchiveWriteError("correction ledger could not be compacted safely")
        current_size = path.stat().st_size
        compacted = True
    encoded_size = len(line.encode("utf-8")) + 1
    if current_size + encoded_size > max_file_bytes:
        raise ArchiveWriteError("correction ledger reached maximum size")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    return compacted


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


class _CorrectionReplayLimitError(OSError):
    pass


def _load_correction_quarantine_index(
    path: Path,
    *,
    max_lines: int,
    max_line_bytes: int,
    record_failure: Callable[..., None],
) -> tuple[set[tuple[int, str]], int, bool]:
    keys: set[tuple[int, str]] = set()
    count = 0
    try:
        if path.stat().st_size > max_lines * max_line_bytes:
            return keys, max_lines, False
        with path.open("r", encoding="utf-8") as handle:
            for count, line in enumerate(handle, start=1):
                if count > max_lines or len(line.encode("utf-8")) > max_line_bytes:
                    return keys, min(count, max_lines), False
                try:
                    entry: Any = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(entry, dict) and isinstance(entry.get("line_number"), int) and isinstance(entry.get("reason"), str):
                    keys.add((entry["line_number"], entry["reason"]))
        return keys, count, True
    except FileNotFoundError:
        return keys, 0, True
    except OSError as exc:
        record_failure(phase="correction-quarantine-read", path_name=path.name, error=exc)
        return keys, 0, False
