from __future__ import annotations

import random
import time
from collections.abc import Callable, Mapping
from typing import Any, TypeVar
from urllib.parse import quote

import httpx

from parking_spot_monitor.logging import StructuredLogger
from parking_spot_monitor.matrix_models import MatrixSyncResult, MatrixTextEvent
from parking_spot_monitor.matrix_support import MatrixError, _http_status_error, _require_non_empty, _require_response_key

CLIENT_API_PREFIX = "/_matrix/client/v3"
MEDIA_API_PREFIX = "/_matrix/media/v3"
_T = TypeVar("_T")

def retry_delay(*, attempt: int, backoff_seconds: float, retry_after_seconds: float | None, jitter_ratio: float, random_unit: Callable[[], float]) -> float:
    local_delay = backoff_seconds * (2 ** max(0, attempt - 1))
    return max(retry_after_seconds or 0, local_delay + local_delay * jitter_ratio * random_unit())


def _room_message_path(room_id: str, txn_id: str) -> str:
    room_segment = quote(_require_non_empty("room_id", room_id), safe="")
    txn_segment = quote(_require_non_empty("txn_id", txn_id), safe="")
    return f"{CLIENT_API_PREFIX}/rooms/{room_segment}/send/m.room.message/{txn_segment}"

class MatrixClient:
    """Synchronous Matrix Client-Server API boundary for alerts."""

    def __init__(
        self,
        *,
        homeserver: str,
        access_token: str,
        timeout_seconds: float = 10,
        retry_attempts: int = 1,
        retry_backoff_seconds: float = 0,
        retry_jitter_ratio: float = 0.2,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_unit: Callable[[], float] = random.random,
        logger: StructuredLogger | None = None,
    ) -> None:
        self.homeserver = homeserver.rstrip("/")
        self.access_token = access_token
        self.timeout_seconds = timeout_seconds
        self.retry_attempts = max(1, retry_attempts)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.retry_jitter_ratio = max(0.0, retry_jitter_ratio)
        self._sleep = sleep
        self._random_unit = random_unit
        self._logger = logger
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(base_url=self.homeserver, timeout=timeout_seconds)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> MatrixClient:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def sync(self, *, room_id: str, since: str | None = None, timeout_ms: int = 0, limit: int = 20) -> MatrixSyncResult:
        """Poll Matrix /sync and return only safe text events for one joined room."""

        room_id = _require_non_empty("room_id", room_id)
        params: dict[str, Any] = {"timeout": max(0, int(timeout_ms)), "limit": max(1, min(int(limit), 100))}
        if since is not None and since.strip():
            params["since"] = since
        response = self._request_with_retry(
            operation="sync",
            method="GET",
            path=f"{CLIENT_API_PREFIX}/sync",
            params=params,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise MatrixError(
                "Matrix sync response was not valid JSON",
                error_type="malformed_response",
                operation="sync",
                status_code=response.status_code,
                missing_key="next_batch",
            ) from exc
        return _parse_sync_response(payload, room_id=room_id, operation="sync", status_code=response.status_code)

    def send_text(self, *, room_id: str, txn_id: str, body: str) -> str:
        body = _require_non_empty("body", body)
        path = _room_message_path(room_id, txn_id)
        response_key = self._request_required_key(
            operation="send_text",
            response_key="event_id",
            method="PUT",
            path=path,
            json={"msgtype": "m.text", "body": body},
        )
        return response_key

    def upload_image(self, *, filename: str, data: bytes, content_type: str) -> str:
        filename = _require_non_empty("filename", filename)
        content_type = _require_non_empty("content_type", content_type)
        return self._request_required_key(
            operation="upload_image",
            response_key="content_uri",
            method="POST",
            path=f"{MEDIA_API_PREFIX}/upload",
            params={"filename": filename},
            content=data,
            headers={"Content-Type": content_type},
        )

    def send_image(
        self,
        *,
        room_id: str,
        txn_id: str,
        body: str,
        content_uri: str,
        info: Mapping[str, Any],
    ) -> str:
        body = _require_non_empty("body", body)
        content_uri = _require_non_empty("content_uri", content_uri)
        path = _room_message_path(room_id, txn_id)
        return self._request_required_key(
            operation="send_image",
            response_key="event_id",
            method="PUT",
            path=path,
            json={"msgtype": "m.image", "body": body, "url": content_uri, "info": dict(info)},
        )

    def _request_required_key(self, *, operation: str, response_key: str, method: str, path: str, **kwargs: Any) -> str:
        def request(attempt: int) -> str:
            response = self._request_once(method, path, attempt=attempt, **kwargs)
            return _require_response_key(response, response_key, operation=operation, attempt=attempt)

        return self._retry_request(operation=operation, path=path, request=request)

    def _request_with_retry(self, *, operation: str, method: str, path: str, **kwargs: Any) -> httpx.Response:
        return self._retry_request(
            operation=operation,
            path=path,
            request=lambda attempt: self._request_once(method, path, attempt=attempt, **kwargs),
        )

    def _retry_request(self, *, operation: str, path: str, request: Callable[[int], _T]) -> _T:
        last_error: MatrixError | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                return request(attempt)
            except MatrixError as exc:
                last_error = exc
                if not self._should_retry(exc, attempt):
                    raise
                delay = self._retry_delay_seconds(exc, attempt=attempt)
                self._log_retry_decision(error=exc, operation=operation, path=path, attempt=attempt, delay_seconds=delay)
                if delay > 0:
                    self._sleep(delay)
        if last_error is not None:
            raise last_error
        raise MatrixError("Matrix request failed", error_type="request_error", operation=operation, path=path)

    def _request_once(self, method: str, path: str, *, attempt: int, **kwargs: Any) -> httpx.Response:
        request_kwargs = dict(kwargs)
        headers = {"Authorization": f"Bearer {self.access_token}"}
        headers.update(request_kwargs.pop("headers", {}))
        try:
            response = self._client.request(
                method,
                self.homeserver + path if not path.startswith("http") else path,
                headers=headers,
                timeout=self.timeout_seconds,
                **request_kwargs,
            )
        except httpx.TimeoutException as exc:
            raise MatrixError(
                "Matrix request timed out",
                error_type="timeout",
                operation=method,
                path=path,
                attempt=attempt,
                exception_type=exc.__class__.__name__,
            ) from exc
        except httpx.RequestError as exc:
            raise MatrixError(
                "Matrix request failed",
                error_type="request_error",
                operation=method,
                path=path,
                attempt=attempt,
                exception_type=exc.__class__.__name__,
            ) from exc

        if response.status_code >= 400:
            raise _http_status_error(response, method=method, path=path, attempt=attempt)
        return response

    def _should_retry(self, error: MatrixError, attempt: int) -> bool:
        if attempt >= self.retry_attempts:
            return False
        error_type = error.diagnostics.get("error_type")
        if error_type in {"timeout", "request_error", "malformed_response"}:
            return True
        if error_type == "http_status":
            return error.diagnostics.get("status_code") in {429, 500, 502, 503, 504}
        return False

    def _retry_delay_seconds(self, error: MatrixError, *, attempt: int) -> float:
        retry_after = error.diagnostics.get("retry_after_seconds")
        return retry_delay(
            attempt=attempt, backoff_seconds=self.retry_backoff_seconds,
            retry_after_seconds=float(retry_after) if isinstance(retry_after, (int, float)) else None,
            jitter_ratio=self.retry_jitter_ratio, random_unit=self._random_unit)

    def _log_retry_decision(self, *, error: MatrixError, operation: str, path: str, attempt: int, delay_seconds: float) -> None:
        if self._logger is None:
            return
        diagnostics = dict(error.diagnostics)
        diagnostics.pop("operation", None)
        diagnostics.pop("path", None)
        diagnostics.pop("attempt", None)
        self._logger.info(
            "matrix-request-retry",
            operation=operation,
            path=path,
            attempt=attempt,
            next_attempt=attempt + 1,
            max_attempts=self.retry_attempts,
            backoff_seconds=round(delay_seconds, 6),
            **diagnostics,
        )

def _parse_sync_response(payload: Any, *, room_id: str, operation: str, status_code: int) -> MatrixSyncResult:
    if not isinstance(payload, dict):
        raise MatrixError("Matrix sync response was malformed", error_type="malformed_response", operation=operation, status_code=status_code, missing_key="next_batch")
    next_batch = payload.get("next_batch")
    if not isinstance(next_batch, str) or not next_batch:
        raise MatrixError("Matrix sync response was missing a required field", error_type="malformed_response", operation=operation, status_code=status_code, missing_key="next_batch")
    events_payload = (((payload.get("rooms") or {}).get("join") or {}).get(room_id) or {}).get("timeline", {}).get("events", [])
    if not isinstance(events_payload, list):
        raise MatrixError("Matrix sync response room timeline was malformed", error_type="malformed_response", operation=operation, status_code=status_code, missing_key="rooms.join.timeline.events")
    events: list[MatrixTextEvent] = []
    for item in events_payload:
        if not isinstance(item, Mapping) or item.get("type") != "m.room.message":
            continue
        content = item.get("content")
        if not isinstance(content, Mapping) or content.get("msgtype") != "m.text":
            continue
        body = content.get("body")
        event_id = item.get("event_id")
        sender = item.get("sender")
        if isinstance(body, str) and isinstance(event_id, str) and isinstance(sender, str):
            events.append(MatrixTextEvent(event_id=event_id, sender=sender, room_id=room_id, body=body[:512]))
    return MatrixSyncResult(next_batch=next_batch, events=tuple(events))
