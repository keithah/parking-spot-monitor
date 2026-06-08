from __future__ import annotations

from io import StringIO

from parking_spot_monitor.logging import StructuredLogger


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
