from __future__ import annotations

import json
import stat
from pathlib import Path

from parking_monitor.outbox import AlertIntent, LocalOutbox
from parking_spot_monitor.health import HealthStatus, write_health_status


def test_write_health_status_creates_host_readable_json(tmp_path: Path) -> None:
    path = tmp_path / "health.json"

    write_health_status(path, HealthStatus(status="ok", updated_at="2026-05-18T19:00:00Z", iteration=7))

    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "ok"
    assert stat.S_IMODE(path.stat().st_mode) == 0o644


def test_health_status_redacts_vehicle_history_failure_context(tmp_path: Path) -> None:
    path = tmp_path / "health.json"

    write_health_status(
        path,
        HealthStatus(
            status="degraded",
            updated_at="2026-05-18T19:00:00Z",
            iteration=3,
            vehicle_history_failure_count=2,
            last_vehicle_history_error={
                "phase": "vehicle-history",
                "action": "close",
                "spot_id": "left_spot",
                "error_type": "RuntimeError",
                "error_message": "token=vehicle-secret raw_image_bytes should-hide",
                "nested": {"access_token": "nested-secret"},
            },
            vehicle_history={
                "active_session_count": 1,
                "closed_session_count": 1,
                "occupied_snapshot_count": 1,
                "occupied_crop_count": 1,
                "image_file_count": 2,
                "image_bytes": 1234,
                "retention_policy": "indefinite",
                "management_capabilities": ["export", "prune"],
                "oldest_retained_session_started_at": "2026-05-18T13:00:00Z",
                "archive_file_count": 8,
                "archive_bytes": 4567,
                "last_maintenance_metadata": {
                    "operation": "export",
                    "status": "ok",
                    "completed_at": "2026-05-18T20:00:00Z",
                    "token": "maintenance-secret",
                    "nested": {"matrix_token": "nested-maintenance-secret"},
                },
                "profile_count": 1,
                "profile_sample_count": 2,
                "profile_unknown_session_count": 0,
                "profile_quarantine_count": 1,
                "last_vehicle_history_error": {
                    "phase": "image-capture",
                    "path_name": "sess-safe.json",
                    "error_message": "rtsp://camera.local access_token=nested-secret raw_image_bytes",
                },
            },
        ),
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["status"] == "degraded"
    assert payload["vehicle_history_failure_count"] == 2
    assert payload["last_vehicle_history_error"]["phase"] == "vehicle-history"
    assert payload["last_vehicle_history_error"]["action"] == "close"
    assert payload["vehicle_history"]["image_file_count"] == 2
    assert payload["vehicle_history"]["image_bytes"] == 1234
    assert payload["vehicle_history"]["retention_policy"] == "indefinite"
    assert payload["vehicle_history"]["management_capabilities"] == ["export", "prune"]
    assert payload["vehicle_history"]["oldest_retained_session_started_at"] == "2026-05-18T13:00:00Z"
    assert payload["vehicle_history"]["archive_file_count"] == 8
    assert payload["vehicle_history"]["archive_bytes"] == 4567
    assert payload["vehicle_history"]["last_maintenance_metadata"]["operation"] == "export"
    assert payload["vehicle_history"]["last_maintenance_metadata"]["token"] == "<redacted>"
    assert payload["vehicle_history"]["last_maintenance_metadata"]["nested"]["matrix_token"] == "<redacted>"
    assert payload["vehicle_history"]["profile_count"] == 1
    assert payload["vehicle_history"]["profile_sample_count"] == 2
    assert payload["vehicle_history"]["profile_quarantine_count"] == 1
    assert payload["vehicle_history"]["last_vehicle_history_error"]["phase"] == "image-capture"
    assert "vehicle-secret" not in json.dumps(payload)
    assert "nested-secret" not in json.dumps(payload)
    assert "maintenance-secret" not in json.dumps(payload)
    assert "nested-maintenance-secret" not in json.dumps(payload)
    assert "should-hide" not in json.dumps(payload)
    assert "rtsp://camera.local" not in json.dumps(payload)


def test_health_status_includes_redacted_matrix_outbox_summary(tmp_path: Path) -> None:
    path = tmp_path / "health.json"
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    pending = outbox.enqueue(AlertIntent(event_id="evt-pending", phase="text", body="ok"))
    retrying = outbox.enqueue(AlertIntent(event_id="evt-retrying", phase="upload", body="ok"))
    delivered = outbox.enqueue(AlertIntent(event_id="evt-delivered", phase="image", body="ok"))
    failed = outbox.enqueue(AlertIntent(event_id="evt-failed", phase="text", body="ok"))
    dead = outbox.enqueue(AlertIntent(event_id="evt-dead", phase="text", body="ok"))
    outbox.mark_retrying(retrying.id, reason="Authorization: Bearer matrix-secret")
    outbox.mark_delivered(delivered.id)
    outbox.mark_failed(failed.id, reason="matrix_forbidden")
    outbox.mark_dead_lettered(dead.id, reason="rtsp://camera.local/stream")

    write_health_status(
        path,
        HealthStatus(
            status="degraded",
            updated_at="2026-05-18T19:00:00Z",
            iteration=4,
            matrix_outbox=outbox.status_summary()
            | {"diagnostic": {"access_token": "health-secret", "message": "Bearer should-hide"}},
        ),
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    matrix_outbox = payload["matrix_outbox"]
    assert matrix_outbox["counts_by_state"] == {
        "pending": 1,
        "retrying": 1,
        "delivered": 1,
        "failed": 1,
        "dead_lettered": 1,
    }
    assert matrix_outbox["retry_reason_counts"] == {"redacted": 1}
    assert matrix_outbox["dead_letter_reason_counts"] == {"matrix_forbidden": 1, "redacted": 1}
    assert {item["state"] for item in matrix_outbox["items"]} == {
        "pending",
        "retrying",
        "delivered",
        "failed",
        "dead_lettered",
    }
    rendered = json.dumps(payload).lower()
    assert "authorization" not in rendered
    assert "matrix-secret" not in rendered
    assert "health-secret" not in rendered
    assert "should-hide" not in rendered
    assert "rtsp://camera.local" not in rendered
