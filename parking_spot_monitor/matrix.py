from __future__ import annotations

import re
import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from PIL import Image, UnidentifiedImageError

from parking_spot_monitor.logging import StructuredLogger, redact_diagnostic_text

CLIENT_API_PREFIX = "/_matrix/client/v3"
MEDIA_API_PREFIX = "/_matrix/media/v3"
JPEG_MIMETYPE = "image/jpeg"


@dataclass(frozen=True)
class MatrixSnapshot:
    """Event-specific raw snapshot prepared for Matrix media upload."""

    path: Path
    filename: str
    txn_id: str
    body: str
    info: dict[str, int | str]
    log_context: dict[str, Any]


@dataclass(frozen=True)
class SnapshotRetentionResult:
    """Safe summary of an event snapshot retention pruning attempt."""

    pruned_count: int = 0
    pruned_bytes: int = 0
    retained_count: int = 0
    failed_count: int = 0


class MatrixError(RuntimeError):
    """Safe Matrix delivery error with structured, redacted diagnostics."""

    def __init__(self, message: str, **diagnostics: Any) -> None:
        self.message = redact_diagnostic_text(message)
        self.diagnostics = _sanitize_diagnostics(diagnostics)
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class MatrixDelivery:
    """Runtime delivery façade for parking events sent to one Matrix room."""

    def __init__(
        self,
        *,
        client: MatrixClient,
        room_id: str,
        data_dir: str | Path,
        snapshots_dir: str | Path | None,
        logger: StructuredLogger,
        snapshot_retention_count: int = 50,
    ) -> None:
        self.client = client
        self.room_id = room_id
        self.data_dir = Path(data_dir)
        self.snapshots_dir = Path(snapshots_dir) if snapshots_dir is not None else None
        self.logger = logger
        self.snapshot_retention_count = snapshot_retention_count

    def send_quiet_window_notice(self, event: Mapping[str, Any]) -> None:
        event_id = _require_non_empty("event_id", str(event.get("event_id", "")))
        self.client.send_text(room_id=self.room_id, txn_id=event_id, body=format_quiet_window_notice(event))

    def send_open_spot_alert(self, event: Mapping[str, Any]) -> None:
        event_id = open_spot_event_id(event)
        self.client.send_text(
            room_id=self.room_id,
            txn_id=f"{event_id}:text",
            body=format_open_spot_alert(event),
        )
        snapshot = prepare_event_snapshot(
            source_path=str(event.get("snapshot_path", "")),
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type="occupancy-open-event",
            event_id=event_id,
            spot_id=str(event.get("spot_id", "")),
            observed_at=event.get("observed_at"),
            snapshot_retention_count=self.snapshot_retention_count,
            logger=self.logger,
            retention_trigger="matrix-event",
        )
        self.logger.info("matrix-snapshot-copied", **snapshot.log_context, txn_id=snapshot.txn_id)
        content_uri = self.client.upload_image(
            filename=snapshot.filename,
            data=snapshot.path.read_bytes(),
            content_type=JPEG_MIMETYPE,
        )
        self.client.send_image(
            room_id=self.room_id,
            txn_id=f"{event_id}:image",
            body=snapshot.body,
            content_uri=content_uri,
            info=snapshot.info,
        )

    def send_live_proof(self, *, latest_path: str | Path, observed_at: object, selected_mode: object) -> None:
        self.send_live_proof_text(observed_at=observed_at, selected_mode=selected_mode)
        self.send_live_proof_image(latest_path=latest_path, observed_at=observed_at, selected_mode=selected_mode)

    def send_live_proof_text(self, *, observed_at: object, selected_mode: object) -> str:
        txn_base = live_proof_event_id(observed_at)
        return self.client.send_text(
            room_id=self.room_id,
            txn_id=f"{txn_base}:text",
            body=format_live_proof_text(observed_at=observed_at, selected_mode=selected_mode),
        )

    def send_live_proof_image(self, *, latest_path: str | Path, observed_at: object, selected_mode: object) -> str:
        txn_base = live_proof_event_id(observed_at)
        snapshot = prepare_event_snapshot(
            source_path=latest_path,
            data_dir=self.data_dir,
            snapshots_dir=self.snapshots_dir,
            event_type="live-proof",
            event_id=txn_base,
            spot_id="camera",
            observed_at=observed_at,
            snapshot_retention_count=self.snapshot_retention_count,
            logger=self.logger,
            retention_trigger="live-proof",
        )
        if self.logger is not None:
            self.logger.info("matrix-live-proof-snapshot-copied", **snapshot.log_context, txn_id=snapshot.txn_id)
        content_uri = self.client.upload_image(
            filename=snapshot.filename,
            data=snapshot.path.read_bytes(),
            content_type=JPEG_MIMETYPE,
        )
        return self.client.send_image(
            room_id=self.room_id,
            txn_id=f"{txn_base}:image",
            body=format_live_proof_image_body(observed_at=observed_at),
            content_uri=content_uri,
            info=snapshot.info,
        )


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
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        logger: StructuredLogger | None = None,
    ) -> None:
        self.homeserver = homeserver.rstrip("/")
        self.access_token = access_token
        self.timeout_seconds = timeout_seconds
        self.retry_attempts = max(1, retry_attempts)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self._sleep = sleep
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
        last_error: MatrixError | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = self._request_once(method, path, attempt=attempt, **kwargs)
                return _require_response_key(response, response_key, operation=operation, attempt=attempt)
            except MatrixError as exc:
                last_error = exc
                if not self._should_retry(exc, attempt):
                    raise
                self._log_retry_decision(error=exc, operation=operation, path=path, attempt=attempt)
                if self.retry_backoff_seconds:
                    self._sleep(self.retry_backoff_seconds)
        if last_error is not None:
            raise last_error
        raise MatrixError("Matrix request failed", error_type="request_error", operation=operation, path=path)

    def _request_once(self, method: str, path: str, *, attempt: int, **kwargs: Any) -> httpx.Response:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        headers.update(kwargs.pop("headers", {}))
        try:
            response = self._client.request(
                method,
                self.homeserver + path if not path.startswith("http") else path,
                headers=headers,
                timeout=self.timeout_seconds,
                **kwargs,
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

    def _log_retry_decision(self, *, error: MatrixError, operation: str, path: str, attempt: int) -> None:
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
            backoff_seconds=self.retry_backoff_seconds,
            **diagnostics,
        )


def prepare_event_snapshot(
    *,
    source_path: str | Path,
    data_dir: str | Path,
    snapshots_dir: str | Path | None,
    event_type: str,
    event_id: str,
    spot_id: str | None,
    observed_at: object,
    snapshot_retention_count: int | None = None,
    logger: StructuredLogger | None = None,
    retention_trigger: str = "matrix-event",
) -> MatrixSnapshot:
    """Copy a raw full-frame snapshot into a stable event-specific Matrix evidence file.

    The helper intentionally rejects local debug overlays and validates JPEG
    metadata before callers can upload bytes as an ``m.image`` message.
    """

    source = Path(source_path)
    event_type_text = _require_non_empty("event_type", event_type)
    event_id_text = _require_non_empty("event_id", event_id)
    observed_text = _format_observed_at(observed_at)
    snapshot_root = Path(snapshots_dir) if snapshots_dir is not None else Path(data_dir) / "snapshots"
    filename = _snapshot_filename(
        event_type=event_type_text,
        stable_id=spot_id or event_id_text,
        observed_at=observed_text,
    )
    destination = snapshot_root / filename

    if source.name == "debug_latest.jpg":
        raise MatrixError(
            "Matrix snapshot source cannot be the local debug overlay",
            error_type="snapshot_invalid_source",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
        )

    try:
        snapshot_root.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    except OSError as exc:
        raise MatrixError(
            "Matrix snapshot copy failed",
            error_type="snapshot_copy_failed",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
            exception_type=exc.__class__.__name__,
        ) from exc

    byte_size = destination.stat().st_size
    try:
        width, height = _jpeg_dimensions(destination)
    except (OSError, UnidentifiedImageError) as exc:
        raise MatrixError(
            "Matrix snapshot metadata could not be read as JPEG",
            error_type="snapshot_metadata_failed",
            source_path=str(source),
            snapshot_path=str(destination),
            event_type=event_type_text,
            event_id=event_id_text,
            spot_id=spot_id,
            byte_size=byte_size,
            exception_type=exc.__class__.__name__,
        ) from exc

    info: dict[str, int | str] = {"mimetype": JPEG_MIMETYPE, "size": byte_size, "w": width, "h": height}
    log_context = _sanitize_diagnostics(
        {
            "event_type": event_type_text,
            "event_id": event_id_text,
            "spot_id": spot_id,
            "source_path": str(source),
            "snapshot_path": str(destination),
            "byte_size": byte_size,
            "mimetype": JPEG_MIMETYPE,
            "width": width,
            "height": height,
        }
    )
    if snapshot_retention_count is not None:
        prune_event_snapshots(
            snapshot_root,
            retention_count=snapshot_retention_count,
            logger=logger,
            current_snapshot=destination,
            trigger=retention_trigger,
        )
    return MatrixSnapshot(
        path=destination,
        filename=filename,
        txn_id=f"snapshot-{Path(filename).stem}",
        body=_snapshot_body(spot_id=spot_id, observed_at=observed_text),
        info=info,
        log_context=log_context,
    )


def prune_event_snapshots(
    snapshot_root: str | Path,
    retention_count: int,
    logger: StructuredLogger | None,
    *,
    current_snapshot: str | Path | None = None,
    trigger: str = "manual",
) -> SnapshotRetentionResult:
    """Prune oldest Matrix event snapshot files while preserving unrelated runtime files.

    Only JPEG names following the event snapshot contract generated by
    ``prepare_event_snapshot`` are considered. Missing directories are empty,
    malformed filenames are ignored, and deletion failures are logged without
    raising so Matrix delivery and monitor startup can continue.
    """

    root = Path(snapshot_root)
    if retention_count < 1:
        _log_retention_failure(
            logger,
            root=root,
            trigger=trigger,
            error_type="ValueError",
            message="snapshot retention count must be positive",
        )
        return SnapshotRetentionResult(failed_count=1)
    if not root.exists():
        return SnapshotRetentionResult()
    try:
        candidates = [path for path in root.iterdir() if path.is_file() and _is_event_snapshot_file(path)]
    except OSError as exc:
        _log_retention_failure(logger, root=root, trigger=trigger, error_type=type(exc).__name__, message=str(exc))
        return SnapshotRetentionResult(failed_count=1)

    candidates.sort(key=lambda path: (_safe_mtime_ns(path), path.name))
    retained_count = min(len(candidates), retention_count)
    if len(candidates) <= retention_count:
        return SnapshotRetentionResult(retained_count=len(candidates))

    current = Path(current_snapshot).resolve() if current_snapshot is not None else None
    to_delete = candidates[: len(candidates) - retention_count]
    pruned_count = 0
    pruned_bytes = 0
    failed_count = 0
    for path in to_delete:
        if current is not None and _same_path(path, current):
            failed_count += 1
            _log_retention_failure(
                logger,
                root=root,
                trigger=trigger,
                error_type="RetentionInvariantError",
                message="retention attempted to delete current snapshot",
            )
            continue
        try:
            byte_size = path.stat().st_size
            path.unlink()
        except OSError as exc:
            failed_count += 1
            _log_retention_failure(logger, root=root, trigger=trigger, error_type=type(exc).__name__, message=str(exc))
            continue
        pruned_count += 1
        pruned_bytes += byte_size

    if pruned_count:
        _log_retention_pruned(
            logger,
            root=root,
            trigger=trigger,
            pruned_count=pruned_count,
            pruned_bytes=pruned_bytes,
            retained_count=len(candidates) - pruned_count,
        )
    return SnapshotRetentionResult(
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
        retained_count=len(candidates) - pruned_count,
        failed_count=failed_count,
    )


def open_spot_event_id(event: Mapping[str, Any]) -> str:
    """Return the stable Matrix transaction base for a confirmed open event."""

    event_type = _require_non_empty("event_type", str(event.get("event_type", "occupancy-open-event")))
    spot_id = _require_non_empty("spot_id", str(event.get("spot_id", "")))
    observed_at = _format_observed_at(event.get("observed_at"))
    return f"{event_type}:{spot_id}:{observed_at}"


_EVENT_SNAPSHOT_PATTERN = re.compile(
    r"^[a-z0-9]+(?:-[a-z0-9]+)*-.+-\d{4}-\d{2}-\d{2}t\d{2}-\d{2}-\d{2}z\.jpg$"
)


def _is_event_snapshot_file(path: Path) -> bool:
    return bool(_EVENT_SNAPSHOT_PATTERN.match(path.name))


def _safe_mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return 0


def _same_path(path: Path, current: Path) -> bool:
    try:
        return path.resolve() == current
    except OSError:
        return False


def _log_retention_pruned(
    logger: StructuredLogger | None,
    *,
    root: Path,
    trigger: str,
    pruned_count: int,
    pruned_bytes: int,
    retained_count: int,
) -> None:
    if logger is None:
        return
    logger.info(
        "snapshot-retention-pruned",
        root=str(root),
        trigger=trigger,
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
        retained_count=retained_count,
    )


def _log_retention_failure(
    logger: StructuredLogger | None,
    *,
    root: Path,
    trigger: str,
    error_type: str,
    message: str,
    failed_count: int = 1,
    pruned_count: int = 0,
    pruned_bytes: int = 0,
) -> None:
    if logger is None:
        return
    logger.warning(
        "snapshot-retention-failed",
        root=str(root),
        trigger=trigger,
        error_type=error_type,
        message=message,
        failed_count=failed_count,
        pruned_count=pruned_count,
        pruned_bytes=pruned_bytes,
    )


def _display_observed_at(value: object) -> str:
    return _format_observed_at(value).replace("Z", "+00:00")


def format_open_spot_alert(event: Mapping[str, Any]) -> str:
    """Return deterministic Matrix text for a confirmed parking-open event."""

    spot_id = _require_non_empty("spot_id", redact_diagnostic_text(event.get("spot_id", "")))
    observed_at = _display_observed_at(event.get("observed_at"))
    return f"Parking spot open: {spot_id} at {observed_at}"


def format_quiet_window_notice(event: Mapping[str, Any]) -> str:
    """Return deterministic Matrix text for a street-sweeping start/end notice."""

    event_type = _require_non_empty("event_type", str(event.get("event_type", "")))
    window_id = _require_non_empty("window_id", str(event.get("window_id", "")))
    if event_type == "quiet-window-started":
        verb = "started"
    elif event_type == "quiet-window-ended":
        verb = "ended"
    else:
        verb = event_type
    return f"Street sweeping {verb}: {window_id}"


def format_live_proof_text(*, observed_at: object, selected_mode: object) -> str:
    observed_text = _display_observed_at(observed_at)
    mode_text = str(getattr(selected_mode, "value", selected_mode))
    return f"LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at {observed_text} (decode mode: {mode_text})."


def format_live_proof_image_body(*, observed_at: object) -> str:
    observed_text = _display_observed_at(observed_at)
    return f"LIVE PROOF / TEST IMAGE: raw full-frame camera snapshot captured at {observed_text}."


def live_proof_event_id(observed_at: object) -> str:
    return f"live-proof:{_format_observed_at(observed_at)}"


def _jpeg_dimensions(path: Path) -> tuple[int, int]:
    with Image.open(path) as image:
        if image.format != "JPEG":
            raise OSError("snapshot is not a JPEG image")
        width, height = image.size
        image.verify()
    return width, height


def _snapshot_filename(*, event_type: str, stable_id: str, observed_at: str) -> str:
    return f"{_path_token(event_type)}-{_path_token(stable_id)}-{_path_token(observed_at)}.jpg"


def _path_token(value: object) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "-", redact_diagnostic_text(value).strip().lower()).strip("-")
    return token or "unknown"


def _format_observed_at(value: object) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return _require_non_empty("observed_at", str(value))


def _snapshot_body(*, spot_id: str | None, observed_at: str) -> str:
    subject = redact_diagnostic_text(spot_id) if spot_id else "parking spot"
    return f"Raw full-frame snapshot for {subject} at {observed_at.replace('Z', '+00:00')}"


def _room_message_path(room_id: str, txn_id: str) -> str:
    room_segment = quote(_require_non_empty("room_id", room_id), safe="")
    txn_segment = quote(_require_non_empty("txn_id", txn_id), safe="")
    return f"{CLIENT_API_PREFIX}/rooms/{room_segment}/send/m.room.message/{txn_segment}"


def _require_non_empty(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty")
    return value


def _require_response_key(response: httpx.Response, key: str, *, operation: str, attempt: int | None = None) -> str:
    try:
        payload = response.json()
    except ValueError as exc:
        raise MatrixError(
            "Matrix response was not valid JSON",
            error_type="malformed_response",
            operation=operation,
            status_code=response.status_code,
            missing_key=key,
            attempt=attempt,
        ) from exc
    value = payload.get(key) if isinstance(payload, dict) else None
    if not isinstance(value, str) or not value:
        raise MatrixError(
            "Matrix response was missing a required field",
            error_type="malformed_response",
            operation=operation,
            status_code=response.status_code,
            missing_key=key,
            attempt=attempt,
        )
    return value


def _http_status_error(response: httpx.Response, *, method: str, path: str, attempt: int) -> MatrixError:
    errcode = None
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("errcode"), str):
        errcode = payload["errcode"]
    return MatrixError(
        "Matrix request returned an error status",
        error_type="http_status",
        operation=method,
        path=path,
        attempt=attempt,
        status_code=response.status_code,
        errcode=errcode,
    )


_UNSAFE_DIAGNOSTIC_KEYS = {"raw_body", "response_body", "body", "headers", "authorization"}


def _sanitize_diagnostics(diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in diagnostics.items():
        if value is None or str(key).lower() in _UNSAFE_DIAGNOSTIC_KEYS:
            continue
        if isinstance(value, str):
            sanitized[key] = redact_diagnostic_text(value)
        elif isinstance(value, Mapping):
            sanitized[key] = _sanitize_diagnostics(value)
        else:
            sanitized[key] = value
    return sanitized
