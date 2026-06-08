from __future__ import annotations

from tests.operator_docs_helpers import assert_contains_all, read_matrix_command_contract, read_tracked


def test_m006_incident_intelligence_commands_and_closeout_smoke_are_documented() -> None:
    readme = read_tracked("README.md")
    matrix_contract = read_matrix_command_contract()
    confidence_source = read_tracked("parking_spot_monitor/operator_cockpit_confidence.py")
    cockpit_snapshot_source = read_tracked("parking_spot_monitor/operator_cockpit_snapshots.py")
    feedback_source = read_tracked("parking_spot_monitor/operator_feedback.py")
    smoke_source = read_tracked("scripts/verify_m006_incident_intelligence_closeout.py")

    assert_contains_all(
        readme,
        [
            "`!parking confidence` shows artifact-derived spot stability, weak evidence, timeline health, and Matrix delivery status",
            "`!parking at <time> <spot_id>`",
            "time first, spot second",
            "`!parking at 7:00pm left_spot`",
            "`!parking at 2026-05-18T19:00:00Z left_spot`",
            "local `data/timeline/frames/` buffer",
            "local detector replay only for this incident-review path",
            "copied occupancy-state simulation",
            "bounded recent decision-memory context",
            "Nearest retained frame: unavailable",
            "No retained timeline frames were found",
            "Missing or malformed health/state/memory/timeline artifacts",
            "does not run detector/model inference, camera capture, media upload, alert emission, threshold mutation, cloud work, or live state mutation",
            "never captures a new camera frame",
            "mutates live occupancy state",
            "vehicle-history records",
            "alert history",
            "detector thresholds",
            "Matrix open-spot alert delivery",
            "python scripts/verify_m006_incident_intelligence_closeout.py",
            "placeholder `RTSP_URL` and `MATRIX_ACCESS_TOKEN` values",
            "package/Docker validate-config checks",
            "does not exercise live RTSP capture, live Matrix sync/delivery, media upload, live detector inference, detection-lab jobs, cloud services, or Matrix open-spot alert delivery",
            "M006_CLOSEOUT_START",
            "M006_CLOSEOUT_PASS",
            "M006_CLOSEOUT_FAIL",
            "M006_CLOSEOUT_DATA",
            "M006_CLOSEOUT_RESULT",
            "M006_CLOSEOUT_DATA timeline_frames status=safe-empty",
            "RTSP URL values",
            "Matrix token values",
            "Authorization headers",
            "raw Matrix response bodies",
            "tracebacks",
            "raw image bytes",
        ],
    )
    assert_contains_all(
        matrix_contract,
        [
            "usage: !parking at <time> <spot_id>",
            "usage: !parking confidence",
            "{command_prefix} at <time> <spot_id> — review the nearest retained timeline frame and local decision memory for an incident",
            "{command_prefix} confidence — show artifact-derived spot stability, weak evidence, timeline health, and Matrix delivery status",
        ],
    )
    assert_contains_all(
        cockpit_snapshot_source,
        [
            "Build a local incident review from retained timeline frames and decision memory.",
            "No retained timeline frames were found.",
            "No detector, camera, Matrix send, or state mutation was run.",
        ],
    )
    assert_contains_all(
        confidence_source,
        [
            "Read-only: no detector, camera, media upload, alert emission, or state mutation was run.",
            "filename scan only; image bytes were not opened",
        ],
    )
    assert_contains_all(
        feedback_source,
        [
            "Record a learn-command label from retained timeline evidence and copied-state replay only.",
            "timeline_missing",
            "corrupt_frame",
        ],
    )
    assert_contains_all(
        smoke_source,
        [
            "M006_CLOSEOUT_START",
            "M006_CLOSEOUT_PASS",
            "M006_CLOSEOUT_FAIL",
            "M006_CLOSEOUT_DATA",
            "M006_CLOSEOUT_RESULT",
            "status=safe-empty",
        ],
    )

    forbidden_claims = [
        "at command captures a new camera frame",
        "confidence uploads media",
        "closeout smoke requires live Matrix secrets",
        "closeout smoke runs live RTSP capture",
    ]
    for marker in forbidden_claims:
        assert marker not in readme

def test_m007_matrix_outbox_recovery_docs_and_closeout_smoke_are_documented() -> None:
    readme = read_tracked("README.md")
    outbox_docs = read_tracked("docs/outbox.md")
    smoke_source = read_tracked("scripts/verify_m007_matrix_outbox_closeout.py")

    assert_contains_all(
        outbox_docs,
        [
            "#### S04/S05 operator recovery and closeout evidence",
            "Inspect `<data-dir>/health.json` first",
            "matrix_outbox.available",
            "matrix_outbox.counts_by_state",
            "matrix_outbox.retry_reason_counts",
            "Ask the cockpit for `!parking status`, then `!parking confidence`",
            "Fall back to raw outbox inspection only when health and cockpit summaries are unavailable",
            "`pending` means delivery work is queued",
            "`retrying` means a retryable Matrix phase failed",
            "`failed` and `dead_lettered` are terminal inspection states",
            "dead_letter_reason_counts",
            "A `dead_lettered` record means a permanent Matrix failure",
            "do not expect restart or normal drains to retry it",
            "Corrupt outbox JSON or malformed persisted records are quarantined",
            "reason codes such as `invalid_json`",
            "Retained open-alert snapshots referenced by `pending` or `retrying` outbox records are protected from snapshot pruning",
            "delayed upload should use the original retained event evidence",
            "text=delivered, upload=pending, image=pending",
            "text=delivered, upload=delivered, image=pending",
            "A normal container restart is safe for retryable outbox records",
            "resume from the first non-delivered phase",
            "later drains skip text",
            "later drains reuse the persisted upload result and skip upload",
            "python scripts/verify_m007_matrix_outbox_closeout.py",
            "M007_CLOSEOUT_START",
            "M007_CLOSEOUT_PASS",
            "M007_CLOSEOUT_FAIL",
            "M007_CLOSEOUT_RESULT",
            "M007_OUTBOX_FAILURE_OK",
            "M007_OUTBOX_HEALTH_OK",
            "M007_OUTBOX_RECOVERY_OK",
            "M007_OUTBOX_DEAD_LETTER_OK",
            "M007_OUTBOX_QUARANTINE_OK",
            "M007_OUTBOX_RETENTION_OK",
            "counts_by_state=retrying:1",
            "retry_reason=matrix_upload_timeout",
            "phases=text:delivered,upload:pending,image:pending",
            "state=delivered",
            "skipped=text called=upload,image",
            "state=dead_lettered",
            "dead_letter_reason=matrix_upload_http_403",
            "later_attempted=0",
            "reason=invalid_json",
            "quarantined_count=1",
            "retained_original_upload=true",
            "stale_pruned=true",
            "fake Matrix clients are in-container test doubles only",
            "does not touch live RTSP, live Matrix sync or delivery, detector/model inference, cloud services, real operator credentials, or real retained runtime artifacts",
        ],
    )
    assert_contains_all(
        readme,
        [
            "### 9. Run the secret-free M007 Matrix outbox closeout smoke",
            "python scripts/verify_m007_matrix_outbox_closeout.py",
            "placeholder `RTSP_URL` and `MATRIX_ACCESS_TOKEN` values",
            "parking-spot-monitor:m007-outbox-smoke",
            "Docker Compose config checks",
            "test-only mounted `/data` directory",
            "matrix-outbox.json",
            "health.json",
            "matrix_outbox",
            "retrying",
            "matrix_upload_timeout",
            "text=delivered,upload=pending,image=pending",
            "fresh container against the same mounted `/data`",
            "skips the already-delivered text phase",
            "M007_CLOSEOUT_START",
            "M007_CLOSEOUT_PASS",
            "M007_CLOSEOUT_FAIL",
            "M007_CLOSEOUT_RESULT",
            "M007_OUTBOX_FAILURE_OK",
            "M007_OUTBOX_HEALTH_OK",
            "M007_OUTBOX_RECOVERY_OK",
            "M007_OUTBOX_DEAD_LETTER_OK",
            "M007_OUTBOX_QUARANTINE_OK",
            "M007_OUTBOX_RETENTION_OK",
            "counts_by_state=retrying:1",
            "retry_reason=matrix_upload_timeout",
            "phases=text:delivered,upload:pending,image:pending",
            "state=delivered",
            "skipped=text called=upload,image",
            "state=dead_lettered",
            "dead_letter_reason=matrix_upload_http_403",
            "later_attempted=0",
            "reason=invalid_json",
            "quarantined_count=1",
            "retained_original_upload=true",
            "stale_pruned=true",
            "fake Matrix clients inside the container snippets",
            "Operators should inspect health JSON first, cockpit summaries second, closeout markers and bounded runner output for proof, and raw outbox/quarantine artifacts only as a local fallback",
            "does not exercise live RTSP capture, live Matrix sync/delivery, live media upload, detector/model inference, cloud services, real credentials, or real operator runtime artifacts",
            "Authorization headers",
            "raw Matrix response bodies",
            "tracebacks",
            "raw image bytes",
        ],
    )
    assert_contains_all(
        smoke_source,
        [
            "M007_CLOSEOUT_START",
            "M007_CLOSEOUT_PASS",
            "M007_CLOSEOUT_FAIL",
            "M007_CLOSEOUT_RESULT",
            "M007_OUTBOX_FAILURE_OK",
            "M007_OUTBOX_HEALTH_OK",
            "M007_OUTBOX_RECOVERY_OK",
            "M007_OUTBOX_DEAD_LETTER_OK",
            "M007_OUTBOX_QUARANTINE_OK",
            "M007_OUTBOX_RETENTION_OK",
            "matrix_upload_timeout",
            "counts_by_state=retrying:1",
            "phases=text:delivered,upload:pending,image:pending",
            "state=delivered skipped=text called=upload,image",
            "state=dead_lettered reason=matrix_upload_http_403 later_attempted=0",
            "reason=invalid_json quarantined_count=1",
            "retained_original_upload=true stale_pruned=true state=delivered",
            "PLACEHOLDER_RTSP_URL",
            "PLACEHOLDER_MATRIX_TOKEN",
            "Trace" "back (most recent call last)",
            "BEGIN RAW " "IMAGE BYTES",
            "END RAW " "IMAGE BYTES",
        ],
    )

    forbidden_m007_doc_claims = [
        "M007 closeout smoke requires live Matrix secrets",
        "M007 closeout smoke runs live RTSP capture",
        "M007 closeout smoke runs live detector inference",
        "M007 closeout smoke uses real operator credentials",
        "raw outbox inspection is the first recovery step",
    ]
    for marker in forbidden_m007_doc_claims:
        assert marker not in readme
        assert marker not in outbox_docs
