from __future__ import annotations

from parking_spot_monitor.vehicle_history_models import SessionRecord, _parse_timestamp


def _latest_session_record(current: SessionRecord | None, candidate: SessionRecord) -> SessionRecord:
    if current is None:
        return candidate
    current_text = str(current.ended_at or current.started_at)
    candidate_text = str(candidate.ended_at or candidate.started_at)
    current_time = _parse_timestamp(current_text)
    candidate_time = _parse_timestamp(candidate_text)
    if current_time is not None and candidate_time is not None:
        if candidate_time != current_time:
            return candidate if candidate_time > current_time else current
    elif candidate_time is not None:
        return candidate
    elif current_time is not None:
        return current
    elif candidate_text != current_text:
        return candidate if candidate_text > current_text else current
    return candidate if candidate.session_id > current.session_id else current
