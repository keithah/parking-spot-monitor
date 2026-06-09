from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import Any, Iterable, Mapping, Sequence


Diagnostic = dict[str, Any]


@dataclass(frozen=True)
class OccupancyWindow:
    label: str
    started_at: str | None
    ended_at: str


@dataclass(frozen=True)
class SpotOccupancyMetrics:
    spot_id: str
    session_count: int
    closed_session_count: int
    active_session_count: int
    currently_occupied: bool
    occupied_duration_seconds: int
    active_duration_seconds: int
    average_dwell_seconds: int | None
    longest_session_seconds: int | None
    first_seen_at: str | None
    last_seen_at: str | None
    diagnostics: tuple[Diagnostic, ...] = ()


@dataclass(frozen=True)
class OccupancyAnalyticsResult:
    window: OccupancyWindow
    spots: dict[str, SpotOccupancyMetrics]
    included_session_ids: tuple[str, ...]
    session_count: int
    closed_session_count: int
    active_session_count: int
    invalid_session_count: int
    current_occupied_spot_count: int
    diagnostics: tuple[Diagnostic, ...] = ()


@dataclass(frozen=True)
class _NormalizedSession:
    session_id: str
    spot_id: str
    started_at: datetime
    ended_at: datetime | None
    duration_seconds: int | None

    @property
    def is_active(self) -> bool:
        return self.ended_at is None


@dataclass(frozen=True)
class _WindowBounds:
    label: str
    started_at: datetime | None
    ended_at: datetime


def analyze_occupancy(
    sessions: Sequence[object] | Iterable[object],
    *,
    active_sessions: Sequence[object] | Iterable[object] = (),
    spot_ids: Sequence[str] | None = None,
    window: str = "7d",
    now: str | datetime | None = None,
    sparse_threshold: int = 3,
) -> OccupancyAnalyticsResult:
    """Return deterministic per-spot historical occupancy metrics.

    The function is pure: callers provide already-loaded vehicle-history records
    and receive dataclass result objects. Closed-session dwell averages are based
    only on closed sessions, while active sessions contribute their current
    duration through ``now`` and occupancy state.
    """

    observed_now = _parse_datetime(now) if now is not None else datetime.now(timezone.utc)
    if observed_now is None:
        raise ValueError("occupancy analytics now must be a timezone-aware ISO timestamp or datetime")
    bounds = _window_bounds(window, observed_now)
    threshold = max(1, int(sparse_threshold)) if not isinstance(sparse_threshold, bool) else 1

    normalized: list[_NormalizedSession] = []
    invalid_count = 0
    for raw in chain(sessions, active_sessions):
        record = _normalize_session(raw)
        if record is None:
            invalid_count += 1
            continue
        if record.started_at > observed_now:
            invalid_count += 1
            continue
        if record.ended_at is not None and record.ended_at < record.started_at:
            invalid_count += 1
            continue
        normalized.append(record)

    included = [record for record in normalized if _overlaps_window(record, bounds)]
    by_spot: dict[str, list[_NormalizedSession]] = {}
    for record in included:
        by_spot.setdefault(record.spot_id, []).append(record)

    if spot_ids is not None:
        for spot_id in spot_ids:
            if spot_id:
                by_spot.setdefault(str(spot_id), [])

    diagnostics: list[Diagnostic] = []
    if invalid_count:
        diagnostics.append(
            {
                "code": "malformed-history",
                "message": f"{invalid_count} vehicle-history sessions were ignored because they were malformed.",
                "invalid_session_count": invalid_count,
            }
        )

    if not included:
        diagnostics.append(
            {
                "code": "no-data-window",
                "message": "No vehicle-history sessions overlap the selected window.",
            }
        )
    elif len(included) < threshold:
        diagnostics.append(
            {
                "code": "sparse-data",
                "message": f"Only {len(included)} qualifying vehicle-history session is available; analytics may be noisy."
                if len(included) == 1
                else f"Only {len(included)} qualifying vehicle-history sessions are available; analytics may be noisy.",
                "qualifying_session_count": len(included),
                "sparse_threshold": threshold,
            }
        )

    spots = {
        spot_id: _spot_metrics(spot_id, records, now=observed_now, window_start=bounds.started_at, sparse_threshold=threshold)
        for spot_id, records in sorted(by_spot.items())
        if records or spot_ids is not None
    }
    active_count = sum(1 for record in included if record.is_active)
    closed_count = len(included) - active_count

    return OccupancyAnalyticsResult(
        window=OccupancyWindow(label=bounds.label, started_at=_format_datetime(bounds.started_at), ended_at=_format_datetime(bounds.ended_at) or ""),
        spots=spots,
        included_session_ids=tuple(sorted(record.session_id for record in included)),
        session_count=len(included),
        closed_session_count=closed_count,
        active_session_count=active_count,
        invalid_session_count=invalid_count,
        current_occupied_spot_count=sum(1 for metric in spots.values() if metric.currently_occupied),
        diagnostics=tuple(diagnostics),
    )


def _spot_metrics(
    spot_id: str,
    records: Sequence[_NormalizedSession],
    *,
    now: datetime,
    window_start: datetime | None,
    sparse_threshold: int,
) -> SpotOccupancyMetrics:
    ordered = sorted(records, key=lambda record: (record.started_at, record.session_id))
    closed = [record for record in ordered if not record.is_active]
    active = [record for record in ordered if record.is_active]
    closed_durations = [_closed_duration_seconds(record) for record in closed]
    active_durations = [_active_duration_seconds(record, now=now, window_start=window_start) for record in active]
    all_durations = closed_durations + active_durations

    diagnostics: list[Diagnostic] = []
    if len(ordered) and len(ordered) < sparse_threshold:
        diagnostics.append(
            {
                "code": "sparse-spot-data",
                "message": f"Only {len(ordered)} qualifying session is available for spot {spot_id}."
                if len(ordered) == 1
                else f"Only {len(ordered)} qualifying sessions are available for spot {spot_id}.",
                "qualifying_session_count": len(ordered),
                "sparse_threshold": sparse_threshold,
            }
        )

    last_seen = None
    if ordered:
        last_seen_dt = max(record.ended_at or now for record in ordered)
        last_seen = _format_datetime(last_seen_dt)

    return SpotOccupancyMetrics(
        spot_id=spot_id,
        session_count=len(ordered),
        closed_session_count=len(closed),
        active_session_count=len(active),
        currently_occupied=bool(active),
        occupied_duration_seconds=sum(all_durations),
        active_duration_seconds=sum(active_durations),
        average_dwell_seconds=(sum(closed_durations) // len(closed_durations)) if closed_durations else None,
        longest_session_seconds=max(all_durations) if all_durations else None,
        first_seen_at=_format_datetime(ordered[0].started_at) if ordered else None,
        last_seen_at=last_seen,
        diagnostics=tuple(diagnostics),
    )


def _normalize_session(raw: object) -> _NormalizedSession | None:
    session_id = _field(raw, "session_id")
    spot_id = _field(raw, "spot_id")
    started_at = _field(raw, "started_at")
    ended_at = _field(raw, "ended_at")
    duration_seconds = _field(raw, "duration_seconds")

    if not session_id or not spot_id or started_at is None:
        return None
    started = _parse_datetime(started_at)
    if started is None:
        return None
    ended = _parse_datetime(ended_at) if ended_at is not None else None
    if ended_at is not None and ended is None:
        return None
    duration = _non_negative_int(duration_seconds) if duration_seconds is not None else None
    if duration_seconds is not None and duration is None:
        return None
    return _NormalizedSession(
        session_id=str(session_id),
        spot_id=str(spot_id),
        started_at=started,
        ended_at=ended,
        duration_seconds=duration,
    )


def _field(raw: object, name: str) -> Any:
    if isinstance(raw, Mapping):
        return raw.get(name)
    return getattr(raw, name, None)


def _non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value.astimezone(timezone.utc).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def _window_bounds(label: str, now: datetime) -> _WindowBounds:
    normalized = str(label).strip().lower()
    if normalized == "today":
        start = now.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    elif normalized == "7d":
        start = now - timedelta(days=7)
    elif normalized == "30d":
        start = now - timedelta(days=30)
    elif normalized == "all":
        start = None
    else:
        raise ValueError(f"unsupported occupancy analytics window: {label}")
    return _WindowBounds(label=normalized, started_at=start, ended_at=now)


def _overlaps_window(record: _NormalizedSession, bounds: _WindowBounds) -> bool:
    if record.started_at > bounds.ended_at:
        return False
    if bounds.started_at is None:
        return True
    effective_end = record.ended_at or bounds.ended_at
    return effective_end >= bounds.started_at


def _closed_duration_seconds(record: _NormalizedSession) -> int:
    if record.duration_seconds is not None:
        return record.duration_seconds
    if record.ended_at is None:
        return 0
    return max(0, int((record.ended_at - record.started_at).total_seconds()))


def _active_duration_seconds(record: _NormalizedSession, *, now: datetime, window_start: datetime | None) -> int:
    start = record.started_at
    if window_start is not None and start < window_start:
        start = window_start
    return max(0, int((now - start).total_seconds()))
