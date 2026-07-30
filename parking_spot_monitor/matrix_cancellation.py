"""Cooperative cancellation state for synchronous Matrix requests."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from parking_spot_monitor.matrix_support import MatrixError


class MatrixCancellation:
    """Own cancellation waits and idempotent transport close semantics."""

    def __init__(
        self,
        client: Any,
        *,
        owns_client: bool,
        sleep: Callable[[float], None],
    ) -> None:
        self._client = client
        self._owns_client = owns_client
        self._event = threading.Event()
        self._close_lock = threading.Lock()
        self._client_closed = False
        if sleep is time.sleep:
            self._wait = self._event.wait
        else:
            def compatible_wait(timeout_seconds: float) -> bool:
                sleep(timeout_seconds)
                return self._event.is_set()

            self._wait = compatible_wait

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout_seconds: float) -> bool:
        return self._wait(timeout_seconds)

    def request(self, *, force_close: bool) -> None:
        self._event.set()
        with self._close_lock:
            if self._client_closed or (not force_close and not self._owns_client):
                return
            self._client_closed = True
        self._client.close()

    def raise_if_requested(self, *, operation: str, path: str) -> None:
        if self.requested:
            raise self.error(operation=operation, path=path)

    @staticmethod
    def error(*, operation: str, path: str) -> MatrixError:
        return MatrixError(
            "Matrix request cancelled",
            error_type="cancelled",
            operation=operation,
            path=path,
        )
