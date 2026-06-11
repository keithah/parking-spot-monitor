from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from parking_monitor.outbox import AlertIntent, LocalOutbox
from parking_spot_monitor.config import load_settings
from parking_spot_monitor.operator_cockpit import format_operator_confidence_reply, format_operator_status_reply
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
from parking_spot_monitor.state import RuntimeState, save_runtime_state

ACCESS_TOKEN = "secret-token-value"


def load_test_settings(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(Path("config.yaml.example").read_text(encoding="utf-8"), encoding="utf-8")
    rtsp_url = "rtsp://operator:secret@camera/live"
    return load_settings(
        config_path,
        environ={
            "RTSP_URL": rtsp_url,
            "RTSP_URL_4K": f"{rtsp_url}/4k",
            "RTSP_URL_360P": f"{rtsp_url}/360p",
            "MATRIX_ACCESS_TOKEN": ACCESS_TOKEN,
        },
    )


def write_runtime_artifacts(tmp_path: Path) -> tuple[Path, Path]:
    health_path = tmp_path / "health.json"
    health_path.write_text(
        json.dumps(
            {
                "status": "degraded",
                "updated_at": "2026-05-18T19:00:00Z",
                "iteration": 7,
                "last_matrix_error": {"error_type": "timeout", "access_token": ACCESS_TOKEN},
            }
        ),
        encoding="utf-8",
    )
    state_path = tmp_path / "state.json"
    save_runtime_state(
        state_path,
        RuntimeState(
            state_by_spot={
                "left_spot": SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=4, miss_streak=0),
                "right_spot": SpotOccupancyState(status=OccupancyStatus.EMPTY, hit_streak=0, miss_streak=5),
            }
        ),
    )
    return health_path, state_path


def populate_outbox(path: Path) -> None:
    outbox = LocalOutbox(path)
    pending = outbox.enqueue(AlertIntent(event_id="evt-pending", phase="text", body="pending body"))
    retrying = outbox.enqueue(AlertIntent(event_id="evt-retrying", phase="upload", body="retrying body"))
    delivered = outbox.enqueue(AlertIntent(event_id="evt-delivered", phase="image", body="delivered body"))
    failed = outbox.enqueue(AlertIntent(event_id="evt-failed", phase="text", body="failed body"))
    dead = outbox.enqueue(AlertIntent(event_id="evt-dead", phase="upload", body="dead body"))

    assert pending.state == "pending"
    outbox.mark_retrying(retrying.id, reason="Authorization: Bearer matrix-secret")
    outbox.mark_phase_delivered(delivered.id, "image", result={"status": "ok"})
    outbox.mark_failed(failed.id, reason="matrix_forbidden")
    outbox.mark_dead_lettered(dead.id, reason="rtsp://camera.local/stream")


def test_status_reply_includes_bounded_redacted_matrix_outbox_summary(tmp_path: Path) -> None:
    settings = load_test_settings(tmp_path)
    health_path, state_path = write_runtime_artifacts(tmp_path)
    outbox_path = tmp_path / "matrix-outbox.json"
    populate_outbox(outbox_path)

    body = format_operator_status_reply(
        settings=settings,
        health_path=health_path,
        state_path=state_path,
        matrix_outbox_path=outbox_path,
        now=datetime(2026, 5, 18, 19, 1, tzinfo=timezone.utc),
    )

    assert "Parking monitor status" in body
    assert "Matrix outbox:" in body
    assert "outbox total 5: pending 1, retrying 1, delivered 1, failed 1, dead-lettered 1" in body
    assert "phase states:" in body
    assert "retry reasons: redacted 1" in body
    assert "dead-letter reasons: matrix_forbidden 1, redacted 1" in body
    assert "record: state retrying; phase upload; retry redacted" in body
    assert len(body.encode("utf-8")) <= 4096
    rendered = body.lower()
    assert ACCESS_TOKEN not in body
    assert "matrix-secret" not in body
    assert "authorization" not in rendered
    assert "rtsp://" not in rendered
    assert "pending body" not in body
    assert "delivered body" not in body
    assert str(tmp_path) not in body


def test_confidence_reply_includes_outbox_inside_matrix_delivery_section(tmp_path: Path) -> None:
    settings = load_test_settings(tmp_path)
    health_path, state_path = write_runtime_artifacts(tmp_path)
    outbox_path = tmp_path / "matrix-outbox.json"
    populate_outbox(outbox_path)
    frames_dir = tmp_path / "timeline" / "frames"
    frames_dir.mkdir(parents=True)
    (frames_dir / "20260518T190000Z.jpg").write_bytes(b"not opened")

    body = format_operator_confidence_reply(
        settings=settings,
        data_dir=tmp_path,
        health_path=health_path,
        state_path=state_path,
        matrix_outbox_path=outbox_path,
        now=datetime(2026, 5, 18, 19, 1, tzinfo=timezone.utc),
    )

    assert "Parking confidence report" in body
    assert "Matrix delivery:" in body
    assert "last Matrix error: timeout" in body
    assert "outbox total 5: pending 1, retrying 1, delivered 1, failed 1, dead-lettered 1" in body
    assert "Read-only: no detector, camera, media upload, alert emission, or state mutation was run." in body
    assert len(body.encode("utf-8")) <= 4096
    assert ACCESS_TOKEN not in body
    assert "rtsp://" not in body
    assert str(tmp_path) not in body


def test_outbox_missing_empty_and_corrupt_artifacts_are_safe(tmp_path: Path) -> None:
    settings = load_test_settings(tmp_path)
    health_path, state_path = write_runtime_artifacts(tmp_path)
    missing = tmp_path / "missing-outbox.json"
    empty = tmp_path / "empty-outbox.json"
    LocalOutbox(empty)
    corrupt = tmp_path / "corrupt-outbox.json"
    corrupt.write_text("not json Authorization: Bearer matrix-secret rtsp://camera.local", encoding="utf-8")

    missing_body = format_operator_status_reply(settings=settings, health_path=health_path, state_path=state_path, matrix_outbox_path=missing)
    empty_body = format_operator_status_reply(settings=settings, health_path=health_path, state_path=state_path, matrix_outbox_path=empty)
    corrupt_body = format_operator_status_reply(settings=settings, health_path=health_path, state_path=state_path, matrix_outbox_path=corrupt)

    assert "outbox empty" in missing_body
    assert "outbox empty" in empty_body
    assert "recovery: recovered 0; quarantined 1; reasons invalid_json 1" in corrupt_body
    rendered = "\n".join([missing_body, empty_body, corrupt_body]).lower()
    assert "matrix-secret" not in rendered
    assert "authorization" not in rendered
    assert "rtsp://" not in rendered
    assert "traceback" not in rendered
    assert str(tmp_path).lower() not in rendered
