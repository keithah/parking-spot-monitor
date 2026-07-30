from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from parking_spot_monitor.logging import StructuredLogger

_MAX_SUMMARY_KEYS = 32
_OTHER_KEY = "other"


@dataclass(frozen=True, slots=True)
class RuntimeLogSummary:
    processed_frames: int
    capture_failures: Mapping[str, int]
    detection_failures: Mapping[str, int]
    suppressed_diagnostics: Mapping[str, int]


@dataclass(slots=True)
class RuntimeLogAggregator:
    interval_seconds: float
    next_summary_at: float
    processed_frames: int = 0
    capture_failures: Counter[str] = field(default_factory=Counter)
    detection_failures: Counter[str] = field(default_factory=Counter)
    suppressed_diagnostics: Counter[str] = field(default_factory=Counter)
    _last_failure_by_kind: dict[str, str] = field(default_factory=dict)

    def record_success(self, kind: str) -> None:
        if kind == "frame":
            self.processed_frames += 1
        self._last_failure_by_kind.pop(kind, None)

    def record_failure(self, kind: str, error_type: str) -> bool:
        key = _bounded_key(error_type)
        counter = self.capture_failures if kind == "capture" else self.detection_failures
        _increment_bounded(counter, key)
        transitioned = self._last_failure_by_kind.get(kind) != key
        self._last_failure_by_kind[kind] = key
        return transitioned

    def record_diagnostic(self, kind: str) -> None:
        _increment_bounded(self.suppressed_diagnostics, _bounded_key(kind))

    def flush_if_due(self, now_monotonic: float) -> RuntimeLogSummary | None:
        if now_monotonic < self.next_summary_at:
            return None
        summary = RuntimeLogSummary(
            processed_frames=self.processed_frames,
            capture_failures=_immutable_counts(self.capture_failures),
            detection_failures=_immutable_counts(self.detection_failures),
            suppressed_diagnostics=_immutable_counts(self.suppressed_diagnostics),
        )
        self.processed_frames = 0
        self.capture_failures.clear()
        self.detection_failures.clear()
        self.suppressed_diagnostics.clear()
        self.next_summary_at = now_monotonic + self.interval_seconds
        return summary


def _bounded_key(value: str) -> str:
    normalized = str(value).strip() or "unknown"
    return normalized[:64]


def _increment_bounded(counter: Counter[str], key: str) -> None:
    if key in counter or len(counter) < _MAX_SUMMARY_KEYS - 1:
        counter[key] += 1
    else:
        counter[_OTHER_KEY] += 1


def _immutable_counts(counter: Counter[str]) -> Mapping[str, int]:
    return MappingProxyType(dict(sorted(counter.items())))


def flush_runtime_log_summary(
    aggregator: RuntimeLogAggregator,
    logger: StructuredLogger,
    now_monotonic: float,
) -> None:
    summary = aggregator.flush_if_due(now_monotonic)
    if summary is None:
        return
    logger.info(
        "runtime-loop-summary",
        processed_frames=summary.processed_frames,
        capture_failures=dict(summary.capture_failures),
        detection_failures=dict(summary.detection_failures),
        suppressed_diagnostics=dict(summary.suppressed_diagnostics),
    )
