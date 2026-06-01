from __future__ import annotations

from pathlib import Path
import re

import yaml


ROOT = Path(__file__).resolve().parents[1]


def read_tracked(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def read_yaml(path: str) -> object:
    return yaml.safe_load(read_tracked(path))


def assert_contains_all(text: str, required: list[str]) -> None:
    missing = [token for token in required if token not in text]
    assert not missing, f"missing documented operator tokens: {missing}"


def read_readme_section(heading: str) -> str:
    readme = read_tracked("README.md")
    pattern = re.compile(rf"^## {re.escape(heading)}\s*$", re.MULTILINE)
    match = pattern.search(readme)
    assert match is not None, f"README.md missing section heading: ## {heading}"
    next_heading = re.search(r"^## ", readme[match.end() :], re.MULTILINE)
    section_end = match.end() + next_heading.start() if next_heading else len(readme)
    return readme[match.start() : section_end]


def assert_section_case(section: str, case_name: str, required: list[str]) -> None:
    missing = [token for token in required if token not in section]
    assert not missing, f"README.md troubleshooting case '{case_name}' missing tokens: {missing}"


def test_readme_documents_clean_machine_setup_sequence_and_operator_commands() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "cp config.yaml.example config.yaml",
            "RTSP_URL",
            "MATRIX_ACCESS_TOKEN",
            "python -m parking_spot_monitor --config config.yaml --validate-config",
            "docker build -t parking-spot-monitor:test .",
            "docker compose config --no-interpolate",
            "docker compose up parking-spot-monitor",
            "docker compose logs -f parking-spot-monitor",
            "docker compose restart parking-spot-monitor",
            "docker compose down",
            "!parking help",
            "!parking status",
            "!parking config",
            "!parking latest",
            "!parking why <spot_id>",
            "!parking explain <spot_id>",
            "!parking learn <spot_id> <open|occupied> at <time>",
            "!parking recent",
            "!parking confidence",
            "!parking at <time> <spot_id>",
            "!parking lab run replay",
            "!parking lab run tuning",
            "!parking lab status",
            "!parking lab status <job_id|latest>",
            "!parking who",
            "!parking owner <spot_id>",
            "!parking wrong <spot_id|session_id>",
            "!parking profile summary <profile_id>",
            "matrix.command_authorized_senders",
        ],
    )

    sequence = [
        "cp config.yaml.example config.yaml",
        "python -m parking_spot_monitor --config config.yaml --validate-config",
        "docker build -t parking-spot-monitor:test .",
        "docker compose config --no-interpolate",
        "docker compose up parking-spot-monitor",
        "docker compose logs -f parking-spot-monitor",
        "docker compose restart parking-spot-monitor",
        "docker compose down",
    ]
    positions = [readme.index(token) for token in sequence]
    assert positions == sorted(positions)


def test_operator_cockpit_commands_are_documented_as_authorized_read_only_and_secret_safe() -> None:
    readme = read_tracked("README.md")
    matrix_source = read_tracked("parking_spot_monitor/matrix.py")

    assert_contains_all(
        readme,
        [
            "The read-only cockpit commands are `!parking status`, `!parking config`, `!parking latest`, `!parking why <spot_id>`, `!parking explain <spot_id>`, `!parking recent`, `!parking confidence`, and `!parking at <time> <spot_id>`",
            "Empty `matrix.command_authorized_senders` default-denies all Matrix commands",
            "Missing, corrupt, or unreadable health/state files are reported as `unavailable`",
            "`!parking why <spot_id>` and `!parking explain <spot_id>` explain the bounded recent decision memory",
            "`!parking recent` returns a compact bounded timeline",
            "operator-decision-memory.json",
            "Decision memory unavailable",
            "No recent decision memory for this spot",
            "old health timestamps are called `stale`",
            "safe error classes such as `error_type`",
            "redact resolved camera URLs",
            "Matrix access-token values",
            "raw Matrix response bodies",
            "tracebacks",
            "YAML dumps",
            "image bytes",
            "raw JPEG bytes",
            "raw Matrix event bodies",
            "unbounded detector payloads",
            "do not start capture, detector, model, upload preparation, Matrix-sync, shell, browser, dashboard, detection-lab, or live-proof work",
            "never mutate live spot state or vehicle-history records",
            "owner/wrong/profile commands are the explicit mutation boundary",
            "!parking latest",
        ],
    )
    assert_contains_all(
        matrix_source,
        [
            "{command_prefix} status — show runtime health and spot status",
            "{command_prefix} config — show safe monitor configuration",
            "{command_prefix} latest — show latest runtime summary and raw full-frame image evidence",
            "{command_prefix} why <spot_id> — explain recent parking decisions for one spot from bounded local memory",
            "{command_prefix} explain <spot_id> — alias for why with the same bounded local-memory explanation",
            "{command_prefix} recent — show recent decision, alert, suppression, command, and lab records from bounded local memory",
        ],
    )

    forbidden_promises = [
        "run shell commands",
        "open a dashboard",
        "unredacted secret",
        "mutate live spot state with `!parking status`",
        "mutate live spot state with `!parking config`",
    ]
    for marker in forbidden_promises:
        assert marker not in readme



def test_m006_incident_intelligence_commands_and_closeout_smoke_are_documented() -> None:
    readme = read_tracked("README.md")
    matrix_source = read_tracked("parking_spot_monitor/matrix.py")
    cockpit_source = read_tracked("parking_spot_monitor/operator_cockpit.py")
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
        matrix_source,
        [
            "usage: !parking at <time> <spot_id>",
            "usage: !parking confidence",
            "{command_prefix} at <time> <spot_id> — review the nearest retained timeline frame and local decision memory for an incident",
            "{command_prefix} confidence — show artifact-derived spot stability, weak evidence, timeline health, and Matrix delivery status",
        ],
    )
    assert_contains_all(
        cockpit_source,
        [
            "Build a local incident review from retained timeline frames and decision memory.",
            "No retained timeline frames were found.",
            "No detector, camera, Matrix send, or state mutation was run.",
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


def test_operator_docs_include_feedback_correction_and_who_snapshot_contract() -> None:
    readme = read_tracked("README.md")
    matrix_source = read_tracked("parking_spot_monitor/matrix.py")
    cockpit_source = read_tracked("parking_spot_monitor/operator_cockpit.py")
    feedback_source = read_tracked("parking_spot_monitor/operator_feedback.py")

    assert_contains_all(
        readme,
        [
            "Use `!parking correct <spot_id> <open|occupied>`",
            "Use `!parking learn <spot_id> <open|occupied> at <time>`",
            "records a bounded local label in `data/operator-feedback-labels.json`",
            "does not store image bytes, camera URLs, Matrix tokens, raw Matrix bodies, or tracebacks",
            "Feedback labels are training and replay evidence only",
            "does not mutate live occupancy state",
            "does not automatically change detector thresholds or train a model",
            "Use `!parking who` to list active parking sessions by spot and request a fresh current snapshot",
            "Snapshot: fresh capture unavailable",
            "does not run detector/model inference or mutate live occupancy state",
        ],
    )
    assert_contains_all(
        matrix_source,
        [
            "usage: !parking correct <spot_id> <open|occupied>",
            "usage: !parking learn <spot_id> <open|occupied> at <time>",
            "{command_prefix} correct <spot_id> <open|occupied> — record the actual spot state for a wrong alert",
            "{command_prefix} learn <spot_id> <open|occupied> at <time> — record a retained-timeline calibration label for review",
            "usage: !parking who",
            "{command_prefix} who — list active parking sessions by spot and attach a fresh current snapshot when configured",
        ],
    )
    assert_contains_all(
        cockpit_source,
        [
            "Build a Matrix who reply enriched by one fresh raw capture when available.",
            "not run detector/model inference and does not read or mutate occupancy",
            "Snapshot: fresh capture unavailable",
            "no live state was changed",
        ],
    )
    assert_contains_all(
        feedback_source,
        [
            "FEEDBACK_LABELS_FILENAME = \"operator-feedback-labels.json\"",
            "reported_state",
            "actual_state",
            "operator correction recorded",
            "record_learn_label",
            "operator learn label recorded",
        ],
    )


def test_operator_intelligence_docs_cover_feedback_aliases_analytics_and_live_uat_limits() -> None:
    readme = read_tracked("README.md")
    matrix_source = read_tracked("parking_spot_monitor/matrix.py")
    cockpit_source = read_tracked("parking_spot_monitor/operator_cockpit.py")
    feedback_source = read_tracked("parking_spot_monitor/operator_feedback.py")

    assert_contains_all(
        readme,
        [
            "!parking explain <spot_id>",
            "`!parking analytics` is a read-only historical occupancy summary over local vehicle-history artifacts",
            "Use `!parking analytics` or `!parking analytics 7d`",
            "`!parking analytics today`",
            "`!parking analytics 30d`",
            "`!parking analytics all`",
            "Analytics does not start capture, run detector/model inference, upload media, emit alerts, mutate feedback labels, mutate vehicle-history records, change thresholds, prove a live camera, or prove live Matrix room delivery",
            "Use `!parking correct <spot_id> <open|occupied>` or its explicit false-alert alias `!parking false-alert <spot_id> <open|occupied>`",
            "Use `!parking learn <spot_id> <open|occupied> at <time>` or its explicit missed-alert alias `!parking missed-alert <spot_id> <open|occupied> at <time>`",
            "records a bounded local label in `data/operator-feedback-labels.json`",
            "They append local feedback labels only after validating the bounded alert or timeline evidence they can access",
            "does not mutate live occupancy state",
            "does not provide live-camera proof from repository tests, README examples, or local docs alone",
            "does not provide a live Matrix delivery guarantee from send attempts or docs alone",
            "Room-visible Matrix delivery requires explicit live proof/readback evidence",
        ],
    )
    assert_contains_all(
        matrix_source,
        [
            "usage: !parking explain <spot_id>",
            "usage: !parking analytics [today|7d|30d|all]",
            "{command_prefix} false-alert <spot_id> <open|occupied> — explicit alias for correcting a false alert",
            "{command_prefix} missed-alert <spot_id> <open|occupied> at <time> — explicit alias for recording missed timeline evidence",
            "{command_prefix} analytics [today|7d|30d|all] — show spot-level historical occupancy metrics from local vehicle-history sessions",
            "if parts[1] in {\"correct\", \"false-alert\"}",
            "if parts[1] in {\"learn\", \"missed-alert\"}",
        ],
    )
    assert_contains_all(
        cockpit_source,
        [
            "Format bounded spot-level historical occupancy analytics from local vehicle-history JSON only.",
            "Parking occupancy analytics",
            "Parking occupancy analytics unavailable",
        ],
    )
    assert_contains_all(
        feedback_source,
        [
            "FEEDBACK_LABELS_FILENAME = \"operator-feedback-labels.json\"",
            "feedback_category=\"false_alert\"",
            "feedback_category=\"missed_alert\"",
            "operator-feedback-label-appended",
        ],
    )

    forbidden_operator_intelligence_claims = [
        "analytics proves live Matrix delivery",
        "analytics starts a capture",
        "false-alert mutates live occupancy state",
        "missed-alert changes detector thresholds",
        "local docs prove live Matrix delivery",
    ]
    for marker in forbidden_operator_intelligence_claims:
        assert marker not in readme


def test_operator_docs_include_timeline_frame_buffer_contract() -> None:
    readme = read_tracked("README.md")
    timeline_source = read_tracked("parking_spot_monitor/timeline_buffer.py")
    startup_source = read_tracked("parking_spot_monitor/__main__.py")

    assert_contains_all(
        readme,
        [
            "data/timeline/frames/",
            "one raw full-frame JPEG per UTC minute",
            "12 hours",
            "missed-alert review",
            "about 150–250 MiB",
            "not a continuous video archive",
        ],
    )
    assert_contains_all(
        timeline_source,
        [
            "DEFAULT_TIMELINE_INTERVAL_SECONDS = 60",
            "DEFAULT_TIMELINE_RETENTION_HOURS = 12",
            "already-sampled",
            "timeline-unavailable",
        ],
    )
    capture_loop_source = read_tracked("parking_spot_monitor/capture_loop.py")
    assert_contains_all(
        startup_source + capture_loop_source,
        [
            "record_timeline_frame",
            "timeline-frame-retained",
        ],
    )


def test_detection_lab_command_docs_cover_bounded_authorized_local_artifact_boundary() -> None:
    readme = read_tracked("README.md")
    matrix_source = read_tracked("parking_spot_monitor/matrix.py")
    cockpit_source = read_tracked("parking_spot_monitor/operator_cockpit.py")
    lab_source = read_tracked("parking_spot_monitor/detection_lab.py")
    startup_source = read_tracked("parking_spot_monitor/__main__.py")

    assert_contains_all(
        readme,
        [
            "`!parking lab run replay`",
            "`!parking lab run tuning`",
            "`!parking lab status`",
            "`!parking lab status <job_id|latest>`",
            "authorized cockpit commands",
            "authorization/default-deny boundary",
            "empty `matrix.command_authorized_senders` list denies lab starts",
            "exact lab grammar",
            "malformed job IDs",
            "path traversal strings",
            "shell snippets",
            "Matrix-supplied filesystem paths",
            "asynchronous, non-blocking local replay job",
            "returns immediately with a bounded job ID",
            "data/detection-lab/labels.json",
            "data/detection-lab/replay-config.json",
            "data/detection-lab/baseline-config.json",
            "data/detection-lab/proposed-config.json",
            "data/detection-lab/jobs/<job_id>/",
            "persisted redacted `status.json`",
            "replay-report.json",
            "tuning-report.json",
            "`!parking lab status` is the same as `!parking lab status latest`",
            "Detection lab status unavailable",
            "missing_fixed_inputs",
            "status_unreadable",
            "malformed_report",
            "runner_unavailable",
            "text-only and do not upload media",
            "does not mutate live occupancy",
            "camera capture",
            "live detector/model execution",
            "live Matrix delivery",
            "safe `lab_outcome` records",
            "`!parking recent` may show lab outcomes",
            "RTSP URLs",
            "Matrix tokens",
            "Authorization headers",
            "raw Matrix response bodies",
            "tracebacks",
            "image bytes",
        ],
    )
    assert_contains_all(
        matrix_source,
        [
            "{command_prefix} lab run replay — start a bounded local replay lab job using fixed inputs",
            "{command_prefix} lab run tuning — start a bounded local tuning lab job using fixed inputs",
            "{command_prefix} lab status [job_id|latest] — show the latest or selected redacted lab job status",
            "usage: !parking lab run <replay|tuning>",
            "usage: !parking lab status [job_id|latest]",
            "invalid lab job kind",
            "invalid lab job id",
        ],
    )
    assert_contains_all(
        cockpit_source,
        [
            "Detection lab job started",
            "use !parking lab status latest",
            "Inputs: fixed local detection-lab files under the runtime data directory.",
            "Detection lab status unavailable",
            "No detector, camera, shell, or live occupancy work was run by this reply path.",
            "Report:",
            "missing fixed inputs",
        ],
    )
    assert_contains_all(
        lab_source,
        [
            "LAB_DIR_NAME = \"detection-lab\"",
            "JOBS_DIR_NAME = \"jobs\"",
            "STATUS_FILENAME = \"status.json\"",
            "REPLAY_REPORT_FILENAME = \"replay-report.json\"",
            "TUNING_REPORT_FILENAME = \"tuning-report.json\"",
            "REPLAY_LABELS_FILENAME = \"labels.json\"",
            "REPLAY_CONFIG_FILENAME = \"replay-config.json\"",
            "TUNING_BASELINE_CONFIG_FILENAME = \"baseline-config.json\"",
            "TUNING_PROPOSED_CONFIG_FILENAME = \"proposed-config.json\"",
            "missing_fixed_inputs",
            "runner_unavailable",
            "malformed_report",
            "status_unreadable",
            "path_outside_lab",
            "outcome_recorder",
        ],
    )
    assert "record_outcome" in startup_source
    assert "_append_lab_outcome_memory" in startup_source

    forbidden_lab_doc_claims = [
        "lab command accepts a path",
        "lab command uploads media",
        "lab command mutates live occupancy",
        "lab command runs a live camera",
        "lab command changes production thresholds",
        "lab status reads arbitrary paths",
    ]
    for marker in forbidden_lab_doc_claims:
        assert marker not in readme

def test_why_recent_command_docs_cover_memory_boundaries_and_safe_failures() -> None:
    readme = read_tracked("README.md")
    matrix_source = read_tracked("parking_spot_monitor/matrix.py")
    memory_source = read_tracked("parking_spot_monitor/operator_decision_memory.py")

    assert_contains_all(
        readme,
        [
            "`!parking why <spot_id>` and `!parking explain <spot_id>` explain the bounded recent decision memory",
            "`!parking recent` returns a compact bounded timeline",
            "accepted/rejected evidence",
            "hit/miss streak context",
            "quiet-window or weak-open suppression",
            "alert outcomes",
            "command/lab outcomes",
            "Invalid spot IDs or extra arguments are rejected",
            "Missing, corrupt, oversized, unsupported, or unreadable `operator-decision-memory.json`",
            "Decision memory unavailable",
            "no detector or camera work was run",
            "No recent decision memory for this spot",
            "bounded local `operator-decision-memory.json` under the effective runtime data directory",
            "why/explain/recent memory boundary",
            "Matrix arguments cannot choose arbitrary files",
            "They are text-only commands",
            "do not upload media",
            "mutate archive corrections",
            "start capture",
            "run the detector/model",
            "invoke detection-lab work",
            "raw JPEG bytes",
            "raw Matrix event bodies",
            "unbounded detector payloads",
        ],
    )
    assert_contains_all(
        matrix_source,
        [
            "usage: !parking why <spot_id>",
            "usage: !parking explain <spot_id>",
            "usage: !parking recent",
            "invalid spot id",
            "{command_prefix} why <spot_id> — explain recent parking decisions for one spot from bounded local memory",
            "{command_prefix} recent — show recent decision, alert, suppression, command, and lab records from bounded local memory",
        ],
    )
    assert_contains_all(
        memory_source,
        [
            "operator-decision-memory.json",
            "Decision memory unavailable",
            "No recent decision memory for this spot",
            "no detector or camera work was run",
            "operator-decision-memory-quarantined",
            "MAX_REPLY_BYTES",
        ],
    )

    forbidden_why_recent_claims = [
        "why starts a capture",
        "recent starts a capture",
        "why uploads media",
        "recent uploads media",
        "why runs the detector",
        "recent runs detection-lab",
    ]
    for marker in forbidden_why_recent_claims:
        assert marker not in readme


def test_latest_command_docs_cover_raw_image_failure_and_retention_boundaries() -> None:
    readme = read_tracked("README.md")
    matrix_source = read_tracked("parking_spot_monitor/matrix.py")

    assert_contains_all(
        readme,
        [
            "`!parking latest` sends a concise runtime summary plus one Matrix image",
            "already-existing local `latest.jpg` passes validation",
            "Parking monitor latest unavailable",
            "Snapshot: unavailable",
            "missing",
            "too large",
            "invalid JPEG",
            "health freshness including `stale`",
            "capture/detection failure counts",
            "per-spot decisions",
            "raw full-frame `data/latest.jpg`",
            "must not use `data/debug_latest.jpg`",
            "polygon overlays",
            "not invoke a new capture",
            "detector/model run",
            "Raw full-frame latest.jpg evidence",
            "does not create or prune retained files under `data/snapshots/`",
            "retention boundaries remain for Matrix event/live-proof snapshots",
            "read-only cockpit commands never mutate live spot state",
            "snapshot retention",
            "runtime artifacts",
        ],
    )
    assert_contains_all(
        matrix_source,
        [
            "{command_prefix} latest — show latest runtime summary and raw full-frame image evidence",
            "Raw full-frame {image_path.name} evidence",
            "command:{event.event_id}:image",
        ],
    )

    forbidden_latest_claims = [
        "latest uses debug_latest.jpg",
        "latest creates retained snapshots",
        "latest starts a capture",
        "latest mutates live spot state",
    ]
    for marker in forbidden_latest_claims:
        assert marker not in readme


def test_readme_and_compose_agree_on_service_mount_command_and_device_contract() -> None:
    readme = read_tracked("README.md")
    compose_text = read_tracked("docker-compose.yml")
    compose = read_yaml("docker-compose.yml")
    service = compose["services"]["parking-spot-monitor"]

    assert "env_file" not in service
    assert "env_file" not in compose_text
    assert service["command"] == [
        "python",
        "-m",
        "parking_spot_monitor",
        "--config",
        "/config/config.yaml",
        "--data-dir",
        "/data",
    ]
    assert "./config.yaml:/config/config.yaml:ro" in service["volumes"]
    assert "./data:/data" in service["volumes"]
    assert service["devices"] == ["/dev/dri:/dev/dri"]

    assert_contains_all(
        readme,
        [
            "parking-spot-monitor",
            "/config/config.yaml",
            "/data",
            "./config.yaml:/config/config.yaml:ro",
            "./data:/data",
            "--data-dir",
            "/dev/dri:/dev/dri",
            "No `env_file` contract in `docker-compose.yml`",
        ],
    )


def test_first_check_artifact_guidance_and_structured_events_are_documented() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "/data/latest.jpg",
            "./data/latest.jpg",
            "data/health.json",
            "python -m json.tool data/health.json",
            "find data/snapshots",
        ],
    )
    assert "startup-ready" in readme or "capture-frame-written" in readme


def test_readme_troubleshooting_covers_s04_failure_classes_with_evidence_surfaces() -> None:
    section = read_readme_section("Troubleshooting and cleanup runbook")

    required_cases = {
        "RTSP/capture failures or reconnect symptoms": [
            "RTSP/capture failures",
            "stream.reconnect_seconds",
            "docker compose logs -f parking-spot-monitor",
            "data/latest.jpg",
            "data/health.json",
            "capture-frame-written",
            "capture-all-modes-failed",
        ],
        "hardware decode/device passthrough issues": [
            "hardware decode",
            "/dev/dri:/dev/dri",
            "docker compose ps",
            "data/health.json",
            "selected_decode_mode",
        ],
        "Matrix send/upload failures": [
            "Matrix send/upload failures",
            "docker compose logs -f parking-spot-monitor",
            "data/health.json",
            "last_matrix_error",
            "matrix-send-failed",
            "matrix-delivery-failed",
        ],
        "detector misses/false negatives": [
            "detector misses",
            "false negatives",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "detection-frame-processed",
            "detection-frame-failed",
        ],
        "false positives/passing traffic": [
            "false positives",
            "passing traffic",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "data/state.json",
            "detection-frame-processed",
        ],
        "street-sweeping or quiet-window behavior": [
            "street-sweeping",
            "quiet-window",
            "data/state.json",
            "quiet-window-started",
            "quiet-window-ended",
            "occupancy-open-suppressed",
        ],
        "restart/state corruption recovery": [
            "restart/state corruption recovery",
            "docker compose restart parking-spot-monitor",
            "data/state.json",
            "quarantined",
            "state-corrupt-quarantined",
        ],
        "permissions/disk write failures": [
            "permissions/disk write failures",
            "./data:/data",
            "data/health.json",
            "health-write-failed",
            "state-save-failed",
            "debug-overlay-failed",
        ],
        "snapshot/disk cleanup": [
            "snapshot/disk cleanup",
            "data/snapshots/",
            "storage.snapshot_retention_count",
            "snapshot-retention-pruned",
            "snapshot-retention-failed",
        ],
    }

    for case_name, required in required_cases.items():
        assert_section_case(section, case_name, required)


def test_readme_non_goals_are_explicit_and_distinguished_from_local_docs_validation() -> None:
    section = read_readme_section("Non-goals and deferred capabilities")

    assert_contains_all(
        section,
        [
            "no supported web UI",
            "NVR/video archive",
            "license-plate recognition",
            "cloud AI dependency",
            "encrypted Matrix-room hardening guarantee",
            "driveway-car monitoring",
            "live-camera proof",
            "live Matrix delivery guarantee",
            "local docs alone",
        ],
    )


def test_s04_docs_contract_stays_grounded_in_tracked_source_events() -> None:
    readme = read_tracked("README.md")
    tracked_sources = "\n".join(
        read_tracked(path)
        for path in [
            "docker-compose.yml",
            "config.yaml.example",
            "parking_spot_monitor/__main__.py",
            "parking_spot_monitor/matrix_dispatch.py",
            "parking_spot_monitor/capture_loop.py",
            "parking_spot_monitor/runtime_detection.py",
            "parking_spot_monitor/runtime_frame.py",
            "parking_spot_monitor/runtime_health.py",
            "parking_spot_monitor/runtime_lifecycle.py",
            "parking_spot_monitor/runtime_overlay.py",
            "parking_spot_monitor/runtime_state_update.py",
            "parking_spot_monitor/capture.py",
            "parking_spot_monitor/matrix.py",
            "parking_spot_monitor/matrix_snapshots.py",
            "parking_spot_monitor/matrix_alerts.py",
            "parking_spot_monitor/state.py",
            "parking_spot_monitor/health.py",
            "parking_spot_monitor/debug_overlay.py",
            "parking_spot_monitor/occupancy.py",
            "parking_spot_monitor/operator_decision_memory.py",
        ]
    )

    source_backed_tokens = [
        "startup-ready",
        "capture-frame-written",
        "capture-decode-fallback",
        "capture-all-modes-failed",
        "debug-overlay-written",
        "debug-overlay-failed",
        "detection-frame-processed",
        "matrix-send-failed",
        "matrix-delivery-failed",
        "state-corrupt-quarantined",
        "state-save-failed",
        "health-write-failed",
        "snapshot-retention-pruned",
        "snapshot-retention-failed",
        "quiet-window-started",
        "quiet-window-ended",
        "occupancy-open-suppressed",
        "/dev/dri:/dev/dri",
        "snapshot_retention_count",
        "operator-decision-memory-quarantined",
    ]
    for token in source_backed_tokens:
        assert token in tracked_sources, f"tracked source no longer backs documented token: {token}"
        assert token in readme, f"README.md missing source-backed operator token: {token}"


def test_docs_and_wiring_remain_secret_safe_and_do_not_embed_raw_artifact_spam() -> None:
    scanned_paths = [
        "README.md",
        "Dockerfile",
        "docker-compose.yml",
        "config.yaml.example",
        "tests/test_operator_docs.py",
    ]
    rendered = "\n".join(read_tracked(path) for path in scanned_paths)

    forbidden_live_value_markers = [
        "rt" "sp://",
        "camera-" "secret",
        "matrix-" "secret",
        "should-not-" "leak",
        "mxc" "://",
        "Authorization" ": " "Bearer",
        "Bear" "er " "syt_",
        "Trace" "back (most recent call last)",
        "BEGIN RAW " "IMAGE BYTES",
        "END RAW " "IMAGE BYTES",
        ("raw " "image bytes").upper(),
    ]
    for marker in forbidden_live_value_markers:
        assert marker not in rendered

    forbidden_live_value_patterns = {
        "concrete RTSP URL": r"rt" r"sp://[^\s)>'\"]+",
        "Matrix access token": r"(?:syt|spa|map)_[-A-Za-z0-9._=]{20,}",
        "Authorization/Bear" "er example": r"Authorization\s*:\s*Bearer\s+\S+",
        "Matrix content URI": r"mxc" r"://[^\s)>'\"]+",
        "private Matrix room id": r"![A-Za-z0-9_-]{20,}:[A-Za-z0-9.-]+",
        "traceback spam": r"Traceback \(most recent call last\)",
    }
    for marker_class, pattern in forbidden_live_value_patterns.items():
        assert re.search(pattern, rendered) is None, f"forbidden {marker_class} marker found in docs/config/wiring"


def test_example_config_uses_environment_secret_names_not_live_values() -> None:
    config = read_yaml("config.yaml.example")

    assert config["stream"]["rtsp_url_env"] == "RTSP_URL"
    assert config["matrix"]["access_token_env"] == "MATRIX_ACCESS_TOKEN"
    assert "rtsp_url" not in config["stream"]
    assert "access_token" not in config["matrix"]


def test_example_config_exposes_operator_calibration_and_runtime_fields() -> None:
    config = read_yaml("config.yaml.example")

    required_groups = [
        "stream",
        "spots",
        "detection",
        "occupancy",
        "matrix",
        "quiet_windows",
        "storage",
        "runtime",
    ]
    for group in required_groups:
        assert group in config, f"missing operator config group: {group}"

    required_fields = [
        ("stream", "rtsp_url_env"),
        ("stream", "frame_width"),
        ("stream", "frame_height"),
        ("stream", "reconnect_seconds"),
        ("spots", "left_spot", "polygon"),
        ("spots", "right_spot", "polygon"),
        ("detection", "confidence_threshold"),
        ("detection", "inference_image_size"),
        ("detection", "spot_crop_inference"),
        ("detection", "spot_crop_margin_px"),
        ("detection", "open_suppression_min_confidence"),
        ("detection", "open_suppression_classes"),
        ("detection", "min_bbox_area_px"),
        ("detection", "min_polygon_overlap_ratio"),
        ("detection", "vehicle_classes"),
        ("occupancy", "iou_threshold"),
        ("occupancy", "confirm_frames"),
        ("occupancy", "release_frames"),
        ("matrix", "homeserver"),
        ("matrix", "room_id"),
        ("matrix", "access_token_env"),
        ("storage", "data_dir"),
        ("storage", "snapshots_dir"),
        ("storage", "snapshot_retention_count"),
        ("runtime", "health_file"),
        ("runtime", "frame_interval_seconds"),
    ]
    for path in required_fields:
        value = config
        for key in path:
            assert isinstance(value, dict), f"{'.'.join(path)} parent is not a mapping"
            assert key in value, f"missing operator config field: {'.'.join(path)}"
            value = value[key]

    assert config["stream"]["rtsp_url_env"] == "RTSP_URL"
    assert config["matrix"]["access_token_env"] == "MATRIX_ACCESS_TOKEN"


def test_example_spot_polygons_are_in_frame_and_have_minimum_shape() -> None:
    config = read_yaml("config.yaml.example")
    width = config["stream"]["frame_width"]
    height = config["stream"]["frame_height"]

    for spot_id in ["left_spot", "right_spot"]:
        polygon = config["spots"][spot_id]["polygon"]
        assert len(polygon) >= 3, f"{spot_id} needs at least three polygon points"
        for point in polygon:
            assert isinstance(point, list), f"{spot_id} polygon point must be a YAML [x, y] list"
            assert len(point) == 2, f"{spot_id} polygon point must contain x and y"
            x, y = point
            assert 0 <= x <= width, f"{spot_id} x coordinate out of frame: {x}"
            assert 0 <= y <= height, f"{spot_id} y coordinate out of frame: {y}"


def test_documented_artifact_paths_and_debug_events_stay_wired_to_tracked_code() -> None:
    combined_sources = "\n".join(
        read_tracked(path)
        for path in [
            "parking_spot_monitor/paths.py",
            "parking_spot_monitor/capture.py",
            "parking_spot_monitor/debug_overlay.py",
            "parking_spot_monitor/__main__.py",
            "parking_spot_monitor/runtime_detection.py",
            "parking_spot_monitor/runtime_frame.py",
            "parking_spot_monitor/runtime_overlay.py",
            "parking_spot_monitor/matrix.py",
        ]
    )

    assert_contains_all(
        combined_sources,
        [
            "latest.jpg",
            "debug_latest.jpg",
            "snapshots",
            "capture-frame-written",
            "debug-overlay-written",
            "debug-overlay-failed",
            "detection-frame-processed",
            "detection-frame-failed",
        ],
    )


def test_readme_calibration_artifact_and_safety_contract_is_grounded() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "config.yaml.example",
            "config.yaml",
            "RTSP_URL",
            "MATRIX_ACCESS_TOKEN",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "data/snapshots/",
            "data/health.json",
            "data/state.json",
            "data/operator-decision-memory.json",
            "debug-overlay-written",
            "capture-frame-written",
            "detection-frame-processed",
            "detection-frame-failed",
            "image payload bytes",
            "raw frames, snapshots, health/state, and redacted runtime logs local",
            "does not prove a live camera or Matrix room",
            "per-spot threshold schema",
        ],
    )

    unsupported_claim_markers = [
        "visual calibration UI",
        "validated live camera",
        "validated live Matrix",
    ]
    for marker in unsupported_claim_markers:
        assert marker not in readme
