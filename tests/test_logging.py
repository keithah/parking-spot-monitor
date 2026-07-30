from __future__ import annotations

from io import StringIO

import pytest

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.runtime_log_aggregation import RuntimeLogAggregator


class FlushCountingStream(StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flush_count = 0

    def flush(self) -> None:
        self.flush_count += 1
        super().flush()


def test_structured_logger_buffers_info_but_flushes_warnings_by_default() -> None:
    stream = FlushCountingStream()
    logger = StructuredLogger(stream=stream)

    logger.info("capture-loop-iteration", iteration=1)
    assert stream.flush_count == 0

    logger.warning("matrix-delivery-degraded")
    assert stream.flush_count == 1


def test_structured_logger_preserves_record_field_order_by_default() -> None:
    stream = FlushCountingStream()
    logger = StructuredLogger(stream=stream)

    logger.info("capture-loop-iteration", zebra=1, alpha=2)

    assert stream.getvalue().splitlines() == ['{"event":"capture-loop-iteration","level":"INFO","zebra":1,"alpha":2}']


def test_runtime_log_aggregator_bounds_repeated_failures_and_diagnostics() -> None:
    aggregator = RuntimeLogAggregator(interval_seconds=900, next_summary_at=900)

    aggregator.record_success("frame")
    aggregator.record_success("frame")
    assert aggregator.record_failure("capture", "timeout") is True
    assert aggregator.record_failure("capture", "timeout") is False
    assert aggregator.record_failure("capture", "decode") is True
    assert aggregator.record_failure("detection", "predict") is True
    aggregator.record_diagnostic("no-rejection")
    aggregator.record_diagnostic("no-rejection")

    assert aggregator.flush_if_due(899.9) is None
    summary = aggregator.flush_if_due(900)
    assert summary is not None
    assert summary.processed_frames == 2
    assert dict(summary.capture_failures) == {"decode": 1, "timeout": 2}
    assert dict(summary.detection_failures) == {"predict": 1}
    assert dict(summary.suppressed_diagnostics) == {"no-rejection": 2}
    assert not hasattr(summary, "candidate_summaries")
    with pytest.raises(TypeError):
        summary.capture_failures["new"] = 1  # type: ignore[index]


def test_runtime_log_aggregator_resets_failure_transition_after_success() -> None:
    aggregator = RuntimeLogAggregator(interval_seconds=1, next_summary_at=1)

    assert aggregator.record_failure("capture", "timeout") is True
    assert aggregator.record_failure("capture", "timeout") is False
    aggregator.record_success("capture")
    assert aggregator.record_failure("capture", "timeout") is True


def test_runtime_log_aggregator_caps_unique_summary_keys() -> None:
    aggregator = RuntimeLogAggregator(interval_seconds=1, next_summary_at=1)

    for index in range(100):
        aggregator.record_failure("capture", f"failure-{index}")
        aggregator.record_diagnostic(f"diagnostic-{index}")

    summary = aggregator.flush_if_due(1)
    assert summary is not None
    assert len(summary.capture_failures) <= 32
    assert len(summary.suppressed_diagnostics) <= 32
    assert summary.capture_failures["other"] == 69
    assert summary.suppressed_diagnostics["other"] == 69
